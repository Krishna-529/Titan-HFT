//
// tests/tests.cpp
// Foundation-phase unit tests for the PIN limit order book.
// Built with ASan + UBSan (see build.sh) so any memory-safety or UB slip is a
// hard failure -> this doubles as the "zero-crash" proof for the data structures.
//
#include "ut.hpp"

#include "titan/domain/types.hpp"
#include "titan/book/order.hpp"
#include "titan/book/pin_node.hpp"
#include "titan/book/price_level.hpp"
#include "titan/memory/arena.hpp"
#include "titan/book/order_book.hpp"

#include <cstdint>
#include <iterator>
#include <random>
#include <unordered_set>

using namespace titan;

static Order mk(OrderId id, Side s, PriceTick p, Qty q, Seq seq = 0) {
    Order o{};  // value-init -> zeroes padding too (safe init)
    o.id = id; o.seq = seq; o.price = p; o.quantity = q; o.remaining = q;
    o.side = s; o.type = OrderType::LIMIT;
    return o;
}

// ---------------------------------------------------------------- Order (POD)
TEST_CASE(order_is_pod_and_sized) {
    CHECK(sizeof(Order) == 40);
    CHECK(std::is_trivially_copyable_v<Order>);
    Order o = mk(7, Side::BUY, 12345, 50);
    CHECK(o.id == 7);
    CHECK(o.price == 12345);
    CHECK(o.remaining == 50);
}

// ---------------------------------------------------------------- PIN_Node
TEST_CASE(pin_node_insert_remove_reuses_lowest_slot) {
    PIN_Node n; n.reset();
    CHECK(n.empty());
    CHECK(n.insert(mk(1, Side::BUY, 100, 10)) == 0u);
    CHECK(n.insert(mk(2, Side::BUY, 100, 10)) == 1u);
    CHECK(n.count() == 2u);
    CHECK(n.remove(0));                 // free slot 0
    CHECK(n.count() == 1u);
    CHECK(n.insert(mk(3, Side::BUY, 100, 10)) == 0u);  // ctzll picks lowest free = 0
}

TEST_CASE(pin_node_remove_is_bounds_safe) {
    PIN_Node n; n.reset();
    n.insert(mk(1, Side::BUY, 100, 10));
    CHECK(!n.remove(64));               // out of range -> false, no UB
    CHECK(!n.remove(1000000));          // way out of range -> false, no UB
    CHECK(!n.remove(5));                // in range but empty slot -> false
    CHECK(n.at(64) == nullptr);         // safe accessor rejects OOR
    CHECK(n.at(5)  == nullptr);         // safe accessor rejects empty
}

TEST_CASE(pin_node_full_guard_never_crashes) {
    PIN_Node n; n.reset();
    for (std::uint32_t i = 0; i < PIN_Node::CAPACITY; ++i)
        CHECK(n.insert(mk(i + 1, Side::SELL, 100, 1)) == i);
    CHECK(n.full());
    CHECK(n.insert(mk(999, Side::SELL, 100, 1)) == INVALID_INDEX);  // guarded, no ctzll(0)
    CHECK(n.count() == 64u);
}

// ---------------------------------------------------------------- OrderBook
TEST_CASE(book_add_cancel_and_best_of_book) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);

    CHECK(book.add(mk(1, Side::BUY,  100, 10)));
    CHECK(book.add(mk(2, Side::BUY,  101,  5)));   // better bid
    CHECK(book.add(mk(3, Side::SELL, 105,  7)));
    CHECK(book.add(mk(4, Side::SELL, 104,  3)));   // better ask
    CHECK(!book.add(mk(1, Side::BUY, 100, 10)));   // duplicate id rejected

    REQUIRE(book.best_bid() != nullptr);
    REQUIRE(book.best_ask() != nullptr);
    CHECK(book.best_bid()->price == 101);
    CHECK(book.best_ask()->price == 104);
    CHECK(book.active_orders() == 4u);

    CHECK(book.cancel(2));                          // drop best bid
    REQUIRE(book.best_bid() != nullptr);
    CHECK(book.best_bid()->price == 100);
    CHECK(book.active_orders() == 3u);

    CHECK(!book.cancel(999999));                    // unknown id -> safe false
    CHECK(book.cancel(1));                          // empty the bid side
    CHECK(book.best_bid() == nullptr);
    CHECK(book.bid_levels() == 0u);
}

TEST_CASE(book_level_aggregation_tracks_qty_and_count) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    book.add(mk(1, Side::BUY, 100, 10));
    book.add(mk(2, Side::BUY, 100, 15));
    book.add(mk(3, Side::BUY, 100,  5));
    REQUIRE(book.best_bid() != nullptr);
    CHECK(book.best_bid()->order_count == 3u);
    CHECK(book.best_bid()->total_qty   == 30u);
    book.cancel(2);
    CHECK(book.best_bid()->order_count == 2u);
    CHECK(book.best_bid()->total_qty   == 15u);
}

TEST_CASE(book_chains_multiple_pin_nodes_per_level) {
    Arena arena(32u * 1024 * 1024);
    OrderBook book(arena, 2048);
    const int N = 200;                              // > CAPACITY -> forces node chaining
    for (int i = 0; i < N; ++i) CHECK(book.add(mk(i + 1, Side::SELL, 100, 1)));
    REQUIRE(book.best_ask() != nullptr);
    CHECK(book.best_ask()->order_count == static_cast<std::uint32_t>(N));
    CHECK(book.best_ask()->total_qty   == static_cast<std::uint64_t>(N));
    for (int i = 0; i < N; ++i) CHECK(book.cancel(i + 1));
    CHECK(book.best_ask()     == nullptr);
    CHECK(book.active_orders() == 0u);
}

TEST_CASE(book_random_churn_is_safe_and_memory_bounded) {
    Arena arena(64u * 1024 * 1024);
    OrderBook book(arena, 4096);
    std::mt19937_64 rng(0xC0FFEEULL);
    std::unordered_set<OrderId> live;
    OrderId next_id = 1;

    const int ROUNDS = 50000;
    for (int r = 0; r < ROUNDS; ++r) {
        const bool do_add = live.empty() || (rng() & 1u);
        if (do_add) {
            const OrderId id = next_id++;
            const PriceTick px = 90 + static_cast<PriceTick>(rng() % 21);  // 90..110
            const Side sd = (rng() & 1u) ? Side::BUY : Side::SELL;
            if (book.add(mk(id, sd, px, 1 + static_cast<Qty>(rng() % 10)))) live.insert(id);
        } else {
            auto it = live.begin();
            std::advance(it, static_cast<long>(rng() % live.size()));
            const OrderId id = *it;
            CHECK(book.cancel(id));
            live.erase(it);
        }
    }
    // Invariant + bounded memory: freelist never permanently collapses (nodes recycle).
    CHECK(book.active_orders() == live.size());
    CHECK(book.free_node_count() > 0u);
}

int main() { return ut::run_all(); }
