#pragma once
//
// titan/book/matcher.hpp
// The core matching state machine. Crosses an incoming order against the resting
// book under strict price-time priority, emits a TradeEvent per fill, and handles
// the residual by order type:
//     LIMIT  -> rests the leftover on the book
//     MARKET -> discards the leftover instantly
//     IOC    -> discards the leftover instantly
//
// Zero-crash / zero-allocation discipline (matches the rest of the engine):
//   * submit() and every crossing helper are `noexcept`.
//   * No allocation on the matching path. Liquidity is consumed in place; the only
//     structural growth is resting a LIMIT residual, which reuses OrderBook::add()
//     (pool-backed -> still zero OS-heap).
//   * Mock egress: a caller-owned std::vector<TradeEvent>. We push ONLY while
//     size() < capacity(), so it can never reallocate on the hot path -> reserve
//     enough up front. A real Disruptor egress ring replaces this later.
//
// Time-priority: within a PIN_Node we consume from the head of the node's
// intra-node FIFO chain (PIN_Node::head_slot -> next_in_time), which preserves
// strict arrival order even when a cancel frees a low slot that a later insert
// refills. Across nodes, the level's node chain is already oldest-first.
//
#include <cstdint>
#include <type_traits>
#include <vector>

#include "titan/book/order.hpp"
#include "titan/book/order_book.hpp"
#include "titan/book/pin_node.hpp"
#include "titan/book/price_level.hpp"
#include "titan/book/trade_event.hpp"
#include "titan/domain/types.hpp"

namespace titan {

// Outcome of matching one incoming order.
struct MatchResult {
    Qty           filled        = 0;      // quantity of the incoming order that traded
    Qty           residual      = 0;      // quantity left over after crossing
    std::uint32_t trades        = 0;      // number of fills generated
    bool          rested        = false;  // residual added to the book (LIMIT only)
    bool          sink_overflow = false;  // egress vector hit capacity (mock limitation)
};

template <class BookT>
class MatcherT {
public:
    explicit MatcherT(BookT& book) noexcept : book_(book) {}

    // Cross `in` against the book. `out` is the mock egress sink (reserve capacity!).
    // Never throws, never allocates on the OS heap, never crashes.
    MatchResult submit(Order in, std::vector<TradeEvent>& out) noexcept {
        MatchResult r{};
        const Qty original = in.remaining;

        if (in.remaining != 0) {
            if (in.side == Side::BUY) cross(book_.asks_, in, /*taker_buy=*/true,  out, r);
            else                      cross(book_.bids_, in, /*taker_buy=*/false, out, r);
        }

        r.filled   = static_cast<Qty>(original - in.remaining);
        r.residual = in.remaining;

        // Residual handling: LIMIT rests the leftover; MARKET & IOC discard it.
        if (in.remaining != 0 && in.type == OrderType::LIMIT) {
            r.rested = book_.add(in);   // pool-backed rest; false only if pool exhausted
        }
        return r;
    }

private:
    // Walk the opposite book from best price outward, consuming liquidity while the
    // incoming order still crosses. `Map` is deduced as OrderBook's Bid/Ask map.
    template <class Map>
    void cross(Map& opp, Order& in, bool taker_buy,
               std::vector<TradeEvent>& out, MatchResult& r) noexcept {
        while (in.remaining != 0 && !opp.empty()) {
            auto it = opp.begin();                 // best level (ordered by the map comparator)
            PriceLevel& lvl = it->second;

            const bool crosses =
                (in.type == OrderType::MARKET) ? true
                : taker_buy ? (in.price >= lvl.price)
                            : (in.price <= lvl.price);
            if (!crosses) break;                   // best price no longer marketable

            sweep_level(lvl, in, taker_buy, out, r);

            if (lvl.order_count == 0) {            // level fully consumed -> reclaim + erase
                book_.free_level_nodes(lvl);       // defensive (sweep already freed drained nodes)
                opp.erase(it);
            }
        }
    }

    // Fill against the front-of-queue orders at ONE price level in time priority:
    // head node first, along the intrusive chain; lowest occupied slot within a node.
    void sweep_level(PriceLevel& lvl, Order& in, bool taker_buy,
                     std::vector<TradeEvent>& out, MatchResult& r) noexcept {
        while (in.remaining != 0) {
            typename BookT::NodeType* pn = front_node(lvl);  // reclaims empty leading nodes
            if (pn == nullptr) break;

            // Front of the node's intra-node FIFO chain = oldest resting order
            // (strict time priority), independent of physical slot index.
            const std::uint8_t s = pn->head_slot;
            if (s == BookT::NodeType::NIL_SLOT) break;  // defensive: non-empty node has a head
            const std::uint32_t slot = s;
            Order& maker = pn->slots[slot];

            if (maker.remaining == 0) {            // defensive: never emit a zero-qty trade
                book_.erase_id(maker.id);
                pn->remove(slot);
                if (lvl.order_count) --lvl.order_count;
                continue;
            }

            const Qty fill = (in.remaining < maker.remaining) ? in.remaining : maker.remaining;
            in.remaining    = static_cast<Qty>(in.remaining - fill);
            maker.remaining = static_cast<Qty>(maker.remaining - fill);
            lvl.total_qty   = (lvl.total_qty >= fill) ? (lvl.total_qty - fill) : 0;

            emit(out, maker, in, taker_buy, fill, r);

            if (maker.remaining == 0) {            // maker fully filled -> remove from book
                book_.erase_id(maker.id);
                pn->remove(slot);
                if (lvl.order_count) --lvl.order_count;
            }
            // else: maker partially filled => incoming is now exhausted => loop exits
        }
    }

    // Head node holding at least one live order, reclaiming empty leading nodes and
    // repairing the chain as it goes. nullptr once the level has no live orders.
    typename BookT::NodeType* front_node(PriceLevel& lvl) noexcept {
        while (lvl.head != INVALID_INDEX) {
            if (lvl.head >= book_.nodes_.size()) {         // bounds (defensive)
                lvl.head = lvl.tail = INVALID_INDEX;
                return nullptr;
            }
            auto& pn = book_.nodes_[lvl.head];
            if (!pn.empty()) return &pn;

            const std::uint32_t nxt = pn.next;             // drop this now-empty node
            book_.free_node(lvl.head);
            lvl.head = nxt;
            if (nxt != INVALID_INDEX) book_.nodes_[nxt].prev = INVALID_INDEX;
            else                      lvl.tail = INVALID_INDEX;
        }
        return nullptr;
    }

    // Record a fill into the mock egress sink. Bounded push -> never reallocates.
    void emit(std::vector<TradeEvent>& out, const Order& maker, const Order& taker,
              bool taker_buy, Qty qty, MatchResult& r) noexcept {
        ++r.trades;
        if (out.size() < out.capacity()) {
            out.push_back(TradeEvent{
                taker.id, maker.id, maker.price, qty,
                taker_buy ? Side::BUY : Side::SELL, {0, 0, 0}});
        } else {
            r.sink_overflow = true;   // mock sink full; caller should reserve() more
        }
    }

    BookT& book_;
};

// Default matcher type used everywhere outside the A/B layout experiments.
using Matcher = MatcherT<OrderBook>;

} // namespace titan
