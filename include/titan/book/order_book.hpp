#pragma once
//
// titan/book/order_book.hpp
// The limit order book: price index -> PriceLevel -> PIN_Node chain, plus an
// O(1) id index for lazy cancellation.
//
//   * bids_/asks_  : std::pmr::map (allocations carve from the Arena, never the heap)
//   * id_index_    : std::pmr::unordered_map<OrderId, Locator> for O(1) cancel
//   * nodes_       : pre-sized PIN_Node pool (one startup allocation, never grows)
//   * free_nodes_  : freelist (stack of node indices) -> O(1) alloc/free, bounded
//
// This draft intentionally contains NO matching logic yet — only insert (add a
// resting order) and lazy cancel. Every public mutator is noexcept and wraps its
// (potentially allocating) index work in try/catch, so arena exhaustion degrades
// to a graceful rejection instead of a crash. Nothing here can segfault: all node
// and slot accesses are bounds-checked.
//
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory_resource>
#include <unordered_map>
#include <vector>

#include "titan/book/order.hpp"
#include "titan/book/pin_node.hpp"
#include "titan/book/price_level.hpp"
#include "titan/domain/types.hpp"
#include "titan/memory/arena.hpp"

namespace titan {

// Where a live order physically sits, for O(1) cancellation.
struct Locator {
    std::uint32_t node;   // PIN_Node pool index
    std::uint32_t slot;   // slot within the node [0, CAPACITY)
    PriceTick     price;  // owning price level
    Side          side;   // which book half
};

class OrderBook {
public:
    // `arena` backs the pmr index containers; `max_nodes` bounds the node pool
    // (=> up to max_nodes * PIN_Node::CAPACITY resting orders).
    OrderBook(Arena& arena, std::uint32_t max_nodes)
        : arena_(arena),
          nodes_(max_nodes),
          bids_(std::greater<PriceTick>(), arena.pmr()),
          asks_(std::less<PriceTick>(),    arena.pmr()),
          id_index_(arena.pmr()) {
        free_nodes_.reserve(max_nodes);
        for (std::uint32_t i = max_nodes; i-- > 0; ) free_nodes_.push_back(i);
        // Pre-size id-index buckets so steady-state inserts don't rehash.
        id_index_.reserve(static_cast<std::size_t>(max_nodes) * PIN_Node::CAPACITY / 2);
    }

    OrderBook(const OrderBook&)            = delete;
    OrderBook& operator=(const OrderBook&) = delete;

    // Add a resting limit order. Returns false if rejected (duplicate id or pool
    // exhausted). noexcept: never throws, never corrupts, never crashes.
    bool add(const Order& o) noexcept {
        try {
            if (id_index_.find(o.id) != id_index_.end()) return false;  // dedup
            return (o.side == Side::BUY) ? add_impl(bids_, o)
                                         : add_impl(asks_, o);
        } catch (...) {
            return false;  // arena exhausted / allocation failure -> reject, don't crash
        }
    }

    // Lazily cancel by id. Returns true if a live order was removed.
    bool cancel(OrderId id) noexcept {
        try {
            auto it = id_index_.find(id);
            if (it == id_index_.end()) return false;

            const Locator loc = it->second;
            if (loc.node >= nodes_.size()) { id_index_.erase(it); return false; }  // bounds

            Order* ord = nodes_[loc.node].at(loc.slot);
            if (ord == nullptr) { id_index_.erase(it); return false; }             // stale

            const Qty rem = ord->remaining;
            nodes_[loc.node].remove(loc.slot);  // clear the occupancy bit (lazy)
            id_index_.erase(it);

            if (loc.side == Side::BUY) update_after_cancel(bids_, loc.price, rem);
            else                       update_after_cancel(asks_, loc.price, rem);
            return true;
        } catch (...) {
            return false;
        }
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
    std::size_t   active_orders() const noexcept { return id_index_.size(); }
    std::uint32_t free_node_count() const noexcept {
        return static_cast<std::uint32_t>(free_nodes_.size());
    }

private:
    using BidMap = std::pmr::map<PriceTick, PriceLevel, std::greater<PriceTick>>;
    using AskMap = std::pmr::map<PriceTick, PriceLevel, std::less<PriceTick>>;

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
        id_index_.emplace(o.id, Locator{node_idx, slot, o.price, o.side});
        return true;
    }

    template <class Map>
    PriceLevel* find_or_create_level(Map& book, PriceTick price) {
        auto it = book.find(price);
        if (it != book.end()) return &it->second;

        const std::uint32_t n = alloc_node();
        if (n == INVALID_INDEX) return nullptr;
        PriceLevel lvl;
        lvl.price = price;
        lvl.head  = n;
        lvl.tail  = n;
        auto [ins, ok] = book.try_emplace(price, lvl);
        if (!ok) { free_node(n); }   // key raced in (can't happen single-threaded)
        return &ins->second;
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

    Arena&                     arena_;       // kept for lifetime clarity / future use
    std::vector<PIN_Node>      nodes_;       // node pool (one startup allocation)
    std::vector<std::uint32_t> free_nodes_;  // freelist stack of node indices
    BidMap                     bids_;        // highest price first
    AskMap                     asks_;        // lowest price first
    std::pmr::unordered_map<OrderId, Locator> id_index_;
};

} // namespace titan
