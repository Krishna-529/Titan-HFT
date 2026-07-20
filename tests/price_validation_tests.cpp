//
// tests/price_validation_tests.cpp
// The price-boundary admissibility gate.
//
// PriceTick is a SIGNED 64-bit value memcpy'd straight off the socket, so a client can
// send INT64_MAX, INT64_MIN, or anything between. These tests pin down the contract:
//
//   * an inadmissible LIMIT/IOC price is rejected with exactly one REJECTED TradeEvent,
//   * the order book is never touched on that path (no level created, no order resting),
//   * MARKET orders are EXEMPT -- price == 0 is their legitimate "unused" sentinel.
//
// That last one is the trap this suite exists to guard. A naive `price >= MIN_VALID_PRICE`
// check compiles fine, passes casual inspection, and silently rejects every market order
// in the engine (~250k ops in matcher_bench alone).
//
// Built with ASan+UBSan (see build.sh).
//
#include "ut.hpp"

#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/memory/arena.hpp"

#include <cstdint>
#include <limits>
#include <vector>

using namespace titan;

static Order mk(OrderId id, Side s, PriceTick p, Qty q,
                OrderType t = OrderType::LIMIT, Seq seq = 0) {
    Order o{};  // value-init zeroes padding (safe init)
    o.id = id; o.seq = seq; o.price = p; o.quantity = q; o.remaining = q;
    o.side = s; o.type = t;
    return o;
}

// Collects every published TradeEvent; never full, so the matcher's zero-drop
// busy-spin returns immediately.
struct VecSink {
    std::vector<TradeEvent> trades;
    bool try_publish(const TradeEvent& e) noexcept { trades.push_back(e); return true; }
    void reserve(std::size_t n) { trades.reserve(n); }
    std::size_t size() const noexcept { return trades.size(); }
    bool empty() const noexcept { return trades.empty(); }
    const TradeEvent& operator[](std::size_t i) const noexcept { return trades[i]; }
};

// ------------------------------------------------- the predicate itself (compile-time)
// is_valid_price / is_admissible are constexpr, so the boundary contract is pinned at
// compile time as well as asserted at runtime below.
static_assert(!is_valid_price(MIN_VALID_PRICE - 1), "MIN-1 must be out of range");
static_assert( is_valid_price(MIN_VALID_PRICE),     "MIN must be in range");
static_assert( is_valid_price(MAX_VALID_PRICE),     "MAX must be in range");
static_assert(!is_valid_price(MAX_VALID_PRICE + 1), "MAX+1 must be out of range");
static_assert(!is_valid_price(0),                   "0 is the MARKET sentinel, not a valid price");
static_assert(!is_valid_price(std::numeric_limits<PriceTick>::max()), "INT64_MAX must be rejected");
static_assert(!is_valid_price(std::numeric_limits<PriceTick>::min()), "INT64_MIN must be rejected");

TEST_CASE(price_bounds_predicate_edges) {
    CHECK(!is_valid_price(std::numeric_limits<PriceTick>::min()));
    CHECK(!is_valid_price(-1));
    CHECK(!is_valid_price(0));
    CHECK( is_valid_price(MIN_VALID_PRICE));            // exactly at the floor
    CHECK( is_valid_price(MIN_VALID_PRICE + 1));
    CHECK( is_valid_price(100'128));                    // matcher_bench's ceiling must clear
    CHECK( is_valid_price(MAX_VALID_PRICE - 1));
    CHECK( is_valid_price(MAX_VALID_PRICE));            // exactly at the ceiling
    CHECK(!is_valid_price(MAX_VALID_PRICE + 1));
    CHECK(!is_valid_price(std::numeric_limits<PriceTick>::max()));
}

// ------------------------------------------------- MARKET exemption (the trap)
TEST_CASE(market_order_with_zero_price_is_admissible) {
    // price == 0 would fail is_valid_price(), but MARKET must still be admitted.
    CHECK( is_admissible(mk(1, Side::BUY, 0, 10, OrderType::MARKET)));
    CHECK(!is_admissible(mk(2, Side::BUY, 0, 10, OrderType::LIMIT)));   // LIMIT is NOT exempt
    CHECK(!is_admissible(mk(3, Side::BUY, 0, 10, OrderType::IOC)));     // IOC is NOT exempt

    // A MARKET order is exempt at ANY price, since the matcher ignores its price entirely.
    CHECK(is_admissible(mk(4, Side::SELL, std::numeric_limits<PriceTick>::max(), 5, OrderType::MARKET)));
    CHECK(is_admissible(mk(5, Side::SELL, -12345, 5, OrderType::MARKET)));
}

TEST_CASE(market_order_still_sweeps_after_the_gate) {
    // The regression this whole file exists for: MARKET (price 0) must pass the gate and
    // go on to actually match, exactly as it did before validation was introduced.
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 2));
    book.add(mk(2, Side::SELL, 101, 2));
    MatchResult r = m.submit(mk(9, Side::BUY, 0, 10, OrderType::MARKET), tr);

    CHECK(r.filled == 4u);
    CHECK(r.residual == 6u);
    CHECK(!r.rejected);                                 // NOT rejected by the price gate
    CHECK(r.trades == 2u);
    REQUIRE(tr.size() == 2u);
    CHECK(tr[0].status == TRADE_STATUS_FILL);
    CHECK(tr[1].status == TRADE_STATUS_FILL);
    CHECK(book.active_orders() == 0u);
}

// ------------------------------------------------- rejection: book must stay pristine
TEST_CASE(out_of_range_limit_is_rejected_and_book_untouched) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 5));                // one resting ask for contrast
    const std::size_t asks_before   = book.ask_levels();
    const std::size_t bids_before   = book.bid_levels();
    const std::size_t active_before = book.active_orders();

    MatchResult r = m.submit(mk(2, Side::BUY, MAX_VALID_PRICE + 1, 7), tr);

    CHECK(r.rejected);
    CHECK(!r.rested);
    CHECK(r.filled == 0u);                              // never crossed, despite being "above" the ask
    CHECK(r.trades == 0u);
    CHECK(r.residual == 7u);

    // The whole point: the price index must not have grown a level for a garbage price.
    CHECK(book.ask_levels()    == asks_before);
    CHECK(book.bid_levels()    == bids_before);
    CHECK(book.active_orders() == active_before);
}

TEST_CASE(int64_extremes_are_rejected) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    // The headline hostile case: raw INT64_MAX / INT64_MIN off the wire.
    MatchResult hi = m.submit(mk(1, Side::BUY, std::numeric_limits<PriceTick>::max(), 3), tr);
    MatchResult lo = m.submit(mk(2, Side::SELL, std::numeric_limits<PriceTick>::min(), 3), tr);

    CHECK(hi.rejected);
    CHECK(lo.rejected);
    CHECK(book.bid_levels()    == 0u);
    CHECK(book.ask_levels()    == 0u);
    CHECK(book.active_orders() == 0u);
    CHECK(tr.size() == 2u);                             // one REJECTED event each
}

TEST_CASE(below_floor_prices_are_rejected) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    CHECK(m.submit(mk(1, Side::BUY, 0, 4), tr).rejected);       // 0 = MARKET sentinel, invalid for LIMIT
    CHECK(m.submit(mk(2, Side::BUY, -1, 4), tr).rejected);
    CHECK(m.submit(mk(3, Side::SELL, -99999, 4), tr).rejected);
    CHECK(book.active_orders() == 0u);
    CHECK(tr.size() == 3u);
}

// ------------------------------------------------- boundary values are ADMITTED
TEST_CASE(prices_at_exact_bounds_are_accepted) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    MatchResult lo = m.submit(mk(1, Side::BUY,  MIN_VALID_PRICE, 3), tr);
    MatchResult hi = m.submit(mk(2, Side::SELL, MAX_VALID_PRICE, 3), tr);

    CHECK(!lo.rejected); CHECK(lo.rested);              // inclusive floor -> rests normally
    CHECK(!hi.rejected); CHECK(hi.rested);              // inclusive ceiling -> rests normally
    CHECK(book.bid_levels()    == 1u);
    CHECK(book.ask_levels()    == 1u);
    CHECK(book.active_orders() == 2u);
    CHECK(tr.empty());                                  // no crossing, no events
}

// ------------------------------------------------- the emitted event's exact shape
TEST_CASE(boundary_rejection_emits_one_well_formed_event) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    const PriceTick bad = MAX_VALID_PRICE + 1;
    MatchResult r = m.submit(mk(77, Side::SELL, bad, 9, OrderType::LIMIT, 1234), tr);

    CHECK(r.rejected);
    REQUIRE(tr.size() == 1u);                           // EXACTLY one -- not zero, not two
    CHECK(tr[0].status     == TRADE_STATUS_REJECTED);
    CHECK(tr[0].taker_id   == 77u);
    CHECK(tr[0].maker_id   == 0u);                      // no counterparty on a rejection
    CHECK(tr[0].quantity   == 0u);                      // no liquidity traded
    CHECK(tr[0].price      == bad);                     // echoes back the offending price
    CHECK(tr[0].taker_side == Side::SELL);
}

// ------------------------------------------------- valid traffic is unaffected
TEST_CASE(valid_orders_are_unaffected_by_the_gate) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 10));
    MatchResult r = m.submit(mk(2, Side::BUY, 100, 10), tr);

    CHECK(!r.rejected);
    CHECK(r.filled == 10u);
    CHECK(r.trades == 1u);
    REQUIRE(tr.size() == 1u);
    CHECK(tr[0].status == TRADE_STATUS_FILL);
    CHECK(book.active_orders() == 0u);
}

TEST_CASE(rejected_order_does_not_disturb_a_populated_book) {
    // Interleave a garbage price into a live book and prove the surrounding state --
    // levels, aggregate quantity, top of book -- is bit-for-bit unchanged.
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    VecSink tr; tr.reserve(64);

    book.add(mk(1, Side::BUY,  99,  5));
    book.add(mk(2, Side::BUY,  98,  7));
    book.add(mk(3, Side::SELL, 101, 4));

    REQUIRE(book.best_bid() != nullptr);
    REQUIRE(book.best_ask() != nullptr);
    const PriceTick     bid_px  = book.best_bid()->price;
    const std::uint64_t bid_qty = book.best_bid()->total_qty;
    const PriceTick     ask_px  = book.best_ask()->price;
    const std::uint64_t ask_qty = book.best_ask()->total_qty;

    MatchResult r = m.submit(mk(9, Side::BUY, std::numeric_limits<PriceTick>::max(), 100), tr);
    CHECK(r.rejected);

    CHECK(book.bid_levels()    == 2u);
    CHECK(book.ask_levels()    == 1u);
    CHECK(book.active_orders() == 3u);
    CHECK(book.best_bid()->price     == bid_px);
    CHECK(book.best_bid()->total_qty == bid_qty);
    CHECK(book.best_ask()->price     == ask_px);
    CHECK(book.best_ask()->total_qty == ask_qty);
}
