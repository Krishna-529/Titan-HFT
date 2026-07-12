#pragma once
//
// titan/book/order_book.hpp
// The limit order book: price index -> PriceLevel -> PIN_Node chain, plus an
// O(1) DENSE SLAB (id -> {node, slot}) for cancellation.
//
//   * bids_/asks_  : std::pmr::map (allocations carve from the Arena, never the heap)
//   * locators_    : flat std::vector<SlabEntry> indexed DIRECTLY by OrderId.
//                    Hash-free, single cache miss (replaces std::pmr::unordered_map).
//                    Requires a bounded/dense id space: ids >= id_capacity are
//                    rejected (never crash). Production would recycle ids (ring/slab).
//   * nodes_       : pre-sized PIN_Node pool (one startup allocation, never grows)
//   * free_nodes_  : freelist (stack of node indices) -> O(1) alloc/free, bounded
//
// Every public mutator is noexcept and cannot crash: all node / slot / id accesses
// are bounds-checked; the (allocating) price-map work is wrapped so arena exhaustion
// degrades to a graceful rejection instead of a throw escaping a noexcept function.
//
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <vector>

#include "titan/book/order.hpp"
#include "titan/book/pin_node.hpp"
#include "titan/book/price_level.hpp"
#include "titan/book/rb_price_index.hpp"
#include "titan/domain/types.hpp"
#include "titan/memory/arena.hpp"

namespace titan {

// Dense-slab entry: where a live order physically sits. node == INVALID_INDEX
// means the id is not live (free slab slot). Kept to 8 bytes -> one cache line
// holds 8 entries; price/side are read back from the Order on cancel (we touch
// the node anyway), so they need not be duplicated here.
struct SlabEntry {
    std::uint32_t node;   // PIN_Node pool index
    std::uint32_t slot;   // slot within the node [0, CAPACITY)
};

template <class NodeT,
          class BidMapT = RBPriceIndex<std::greater<PriceTick>>,
          class AskMapT = RBPriceIndex<std::less<PriceTick>>>
class OrderBookT {
public:
    using NodeType = NodeT;

    // `arena` backs the pmr price maps; `max_nodes` bounds the node pool
    // (=> up to max_nodes * NodeT::CAPACITY resting orders). `id_capacity`
    // sizes the id->locator slab; 0 => derive from the pool (max_nodes*CAPACITY).
    OrderBookT(Arena& arena, std::uint32_t max_nodes, std::uint64_t id_capacity = 0)
        : arena_(arena),
          nodes_(max_nodes),
          locators_(id_capacity ? id_capacity
                                 : static_cast<std::uint64_t>(max_nodes) * NodeT::CAPACITY,
                    SlabEntry{INVALID_INDEX, 0}),
          bids_(std::greater<PriceTick>(), arena.pmr()),
          asks_(std::less<PriceTick>(),    arena.pmr()) {
        free_nodes_.reserve(max_nodes);
        for (std::uint32_t i = max_nodes; i-- > 0; ) free_nodes_.push_back(i);
    }

    OrderBookT(const OrderBookT&)            = delete;
    OrderBookT& operator=(const OrderBookT&) = delete;

    // The matching engine (titan/book/matcher.hpp) consumes liquidity by reaching
    // into the node pool / slab; grant it access rather than widen the public API.
    template <class> friend class MatcherT;

    // Add a resting limit order. Returns false if rejected (duplicate id, id out of
    // slab range, or pool/arena exhausted). noexcept: never throws, never crashes.
    bool add(const Order& o) noexcept {
        if (o.id >= locators_.size()) return false;              // id out of slab range
        if (locators_[o.id].node != INVALID_INDEX) return false; // duplicate (already live)
        try {
            return (o.side == Side::BUY) ? add_impl(bids_, o)
                                         : add_impl(asks_, o);
        } catch (...) {
            return false;   // arena exhausted -> reject, don't crash
        }
    }

    // Lazily cancel by id. Returns true if a live order was removed.
    bool cancel(OrderId id) noexcept {
        if (id >= locators_.size()) return false;
        const SlabEntry loc = locators_[id];
        if (loc.node == INVALID_INDEX) return false;                     // not live
        if (loc.node >= nodes_.size()) { clear_slot(id); return false; } // bounds (defensive)

        Order* ord = nodes_[loc.node].at(loc.slot);
        if (ord == nullptr) { clear_slot(id); return false; }            // stale

        const Qty       rem   = ord->remaining;
        const PriceTick price = ord->price;   // read back from the order (no dup in slab)
        const Side      side  = ord->side;

        nodes_[loc.node].remove(loc.slot);    // clear occupancy bit + FIFO unlink (O(1))
        clear_slot(id);

        try {
            if (side == Side::BUY) update_after_cancel(bids_, price, rem);
            else                   update_after_cancel(asks_, price, rem);
        } catch (...) { /* map find/erase here don't allocate; defensive only */ }
        return true;
    }

    // --- Top-of-book / introspection (read-only, noexcept) ---
    const PriceLevel* best_bid() const noexcept {
        return bids_.empty() ? nullptr : &bids_.begin()->second;
    }
    const PriceLevel* best_ask() const noexcept {
        return asks_.empty() ? nullptr : &asks_.begin()->second;
    }
    std::size_t   bid_levels()    const noexcept { return bids_.size(); }
    std::size_t   ask_levels()    const noexcept { return asks_.size(); }
    std::size_t   active_orders() const noexcept { return live_count_; }
    std::uint32_t free_node_count() const noexcept {
        return static_cast<std::uint32_t>(free_nodes_.size());
    }

private:
    using BidMap = BidMapT;
    using AskMap = AskMapT;

    // ---- slab helpers ----
    // Free a slab slot known to be live (caller checked id range + liveness).
    void clear_slot(OrderId id) noexcept {
        locators_[id].node = INVALID_INDEX;
        --live_count_;
    }
    // Matcher calls this on fill to drop a fully-filled maker from the slab.
    void erase_id(OrderId id) noexcept {
        if (id < locators_.size() && locators_[id].node != INVALID_INDEX) {
            locators_[id].node = INVALID_INDEX;
            --live_count_;
        }
    }

    // ---- node pool (freelist) ----
    std::uint32_t alloc_node() noexcept {
        if (free_nodes_.empty()) return INVALID_INDEX;   // pool exhausted (guarded)
        const std::uint32_t idx = free_nodes_.back();
        free_nodes_.pop_back();
        nodes_[idx].reset();
        return idx;
    }
    void free_node(std::uint32_t idx) noexcept {
        if (idx >= nodes_.size()) return;                // bounds
        free_nodes_.push_back(idx);
    }

    template <class Map>
    bool add_impl(Map& book, const Order& o) {
        PriceLevel* lvl = find_or_create_level(book, o.price);
        if (lvl == nullptr) return false;                // pool exhausted
        const std::uint32_t node_idx = ensure_tail_with_space(*lvl);
        if (node_idx == INVALID_INDEX) return false;     // pool exhausted
        const std::uint32_t slot = nodes_[node_idx].insert(o);
        if (slot == INVALID_INDEX) return false;         // defensive (space was ensured)
        lvl->total_qty   += o.remaining;
        lvl->order_count += 1;
        locators_[o.id] = SlabEntry{node_idx, slot};     // O(1) slab write
        ++live_count_;
        return true;
    }

    template <class Map>
    PriceLevel* find_or_create_level(Map& book, PriceTick price) {
        // Single-pass find-or-create: one search, then O(1) splice on a new key.
        auto [it, inserted] = book.try_emplace(price);
        PriceLevel& lvl = it->second;
        if (inserted) {
            const std::uint32_t n = alloc_node();
            if (n == INVALID_INDEX) { book.erase(it); return nullptr; }  // rollback new level
            lvl.price = price;
            lvl.head  = n;
            lvl.tail  = n;
        }
        return &lvl;
    }

    std::uint32_t ensure_tail_with_space(PriceLevel& lvl) noexcept {
        std::uint32_t t = lvl.tail;
        if (t == INVALID_INDEX) {                        // level with no nodes (defensive)
            const std::uint32_t n = alloc_node();
            if (n == INVALID_INDEX) return INVALID_INDEX;
            lvl.head = lvl.tail = n;
            return n;
        }
        if (!nodes_[t].full()) return t;                 // current tail has room

        const std::uint32_t n = alloc_node();            // append a fresh node
        if (n == INVALID_INDEX) return INVALID_INDEX;
        nodes_[n].prev = t;
        nodes_[t].next = n;
        lvl.tail = n;
        return n;
    }

    template <class Map>
    void update_after_cancel(Map& book, PriceTick price, Qty rem) {
        auto lit = book.find(price);
        if (lit == book.end()) return;
        PriceLevel& lvl = lit->second;
        if (lvl.total_qty >= rem) lvl.total_qty -= rem; else lvl.total_qty = 0;
        if (lvl.order_count > 0)  lvl.order_count -= 1;
        if (lvl.order_count == 0) {                      // level emptied -> reclaim
            free_level_nodes(lvl);
            book.erase(lit);
        }
    }

    void free_level_nodes(PriceLevel& lvl) noexcept {
        std::uint32_t n = lvl.head;
        while (n != INVALID_INDEX) {
            const std::uint32_t nxt = (n < nodes_.size()) ? nodes_[n].next : INVALID_INDEX;
            free_node(n);
            n = nxt;
        }
        lvl.head = lvl.tail = INVALID_INDEX;
    }

    Arena&                     arena_;       // backs the pmr price maps
    std::vector<NodeT>         nodes_;       // node pool (one startup allocation)
    std::vector<SlabEntry>     locators_;    // id -> {node, slot}; flat, O(1), hash-free
    std::vector<std::uint32_t> free_nodes_;  // freelist stack of node indices
    BidMap                     bids_;        // highest price first
    AskMap                     asks_;        // lowest price first
    std::size_t                live_count_ = 0;  // active_orders() without scanning the slab
};

// Default book type used everywhere outside the A/B layout experiments.
using OrderBook = OrderBookT<PIN_Node>;

} // namespace titan
