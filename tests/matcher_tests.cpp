//
// tests/matcher_tests.cpp
// Matching-engine tests: crossing, price-time priority sweeps, partial fills &
// residuals per order type, and mock-egress safety. Built with ASan+UBSan (see
// build.sh) so any memory-safety slip on the matching path is a hard failure.
//
#include "ut.hpp"

#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/memory/arena.hpp"

#include <vector>

using namespace titan;

static Order mk(OrderId id, Side s, PriceTick p, Qty q,
                OrderType t = OrderType::LIMIT, Seq seq = 0) {
    Order o{};  // value-init zeroes padding (safe init)
    o.id = id; o.seq = seq; o.price = p; o.quantity = q; o.remaining = q;
    o.side = s; o.type = t;
    return o;
}

// ------------------------------------------------------------ TradeEvent POD
TEST_CASE(trade_event_is_pod_and_sized) {
    CHECK(sizeof(TradeEvent) == 32);
    CHECK(std::is_trivially_copyable_v<TradeEvent>);
}

// ------------------------------------------------------------ full fill
TEST_CASE(limit_full_fill_clears_maker) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    CHECK(book.add(mk(1, Side::SELL, 100, 10)));      // resting ask
    MatchResult r = m.submit(mk(2, Side::BUY, 100, 10), tr);

    CHECK(r.filled == 10u);
    CHECK(r.residual == 0u);
    CHECK(r.trades == 1u);
    CHECK(!r.rested);
    REQUIRE(tr.size() == 1u);
    CHECK(tr[0].maker_id == 1u);
    CHECK(tr[0].taker_id == 2u);
    CHECK(tr[0].price == 100);                        // trade at resting price
    CHECK(tr[0].quantity == 10u);
    CHECK(tr[0].taker_side == Side::BUY);
    CHECK(book.best_ask() == nullptr);                // book emptied
    CHECK(book.active_orders() == 0u);
}

// ------------------------------------------------------------ partial fill: maker rests
TEST_CASE(limit_partial_fill_leaves_maker_resting) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 10));
    MatchResult r = m.submit(mk(2, Side::BUY, 100, 4), tr);

    CHECK(r.filled == 4u);
    CHECK(r.residual == 0u);
    CHECK(r.trades == 1u);
    REQUIRE(book.best_ask() != nullptr);
    CHECK(book.best_ask()->price == 100);
    CHECK(book.best_ask()->total_qty == 6u);          // 10 - 4 still resting
    CHECK(book.best_ask()->order_count == 1u);
    CHECK(book.active_orders() == 1u);
}

// ------------------------------------------------------------ taker residual rests (LIMIT)
TEST_CASE(limit_taker_residual_rests_on_book) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 5));
    MatchResult r = m.submit(mk(2, Side::BUY, 100, 8), tr);   // buys 5, 3 left over

    CHECK(r.filled == 5u);
    CHECK(r.residual == 3u);
    CHECK(r.rested);
    CHECK(r.trades == 1u);
    CHECK(book.best_ask() == nullptr);                // ask consumed
    REQUIRE(book.best_bid() != nullptr);              // residual now a resting bid
    CHECK(book.best_bid()->price == 100);
    CHECK(book.best_bid()->total_qty == 3u);
}

// ------------------------------------------------------------ price-time priority sweep
TEST_CASE(sweep_respects_price_then_time_priority) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    // Same-level makers (time priority: id1 before id2), plus a worse-priced level.
    book.add(mk(1, Side::SELL, 100, 3, OrderType::LIMIT, 1));
    book.add(mk(2, Side::SELL, 100, 2, OrderType::LIMIT, 2));
    book.add(mk(3, Side::SELL, 101, 5, OrderType::LIMIT, 3));

    MatchResult r = m.submit(mk(9, Side::BUY, 101, 6), tr);   // sweeps 100 then 101

    CHECK(r.filled == 6u);
    CHECK(r.residual == 0u);
    REQUIRE(tr.size() == 3u);
    // id1@100 x3, then id2@100 x2 (time priority), then id3@101 x1 (price priority)
    CHECK(tr[0].maker_id == 1u); CHECK(tr[0].price == 100); CHECK(tr[0].quantity == 3u);
    CHECK(tr[1].maker_id == 2u); CHECK(tr[1].price == 100); CHECK(tr[1].quantity == 2u);
    CHECK(tr[2].maker_id == 3u); CHECK(tr[2].price == 101); CHECK(tr[2].quantity == 1u);
    // id3 partially filled (5 -> 4) still resting at 101
    REQUIRE(book.best_ask() != nullptr);
    CHECK(book.best_ask()->price == 101);
    CHECK(book.best_ask()->total_qty == 4u);
    CHECK(book.bid_levels() == 0u);                   // taker fully filled, nothing rests
}

// ------------------------------------------------------------ MARKET sweeps, discards residual
TEST_CASE(market_order_sweeps_and_discards_residual) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 2));
    book.add(mk(2, Side::SELL, 101, 2));
    // price is ignored for MARKET; ask for 10 but only 4 available
    MatchResult r = m.submit(mk(9, Side::BUY, 0, 10, OrderType::MARKET), tr);

    CHECK(r.filled == 4u);
    CHECK(r.residual == 6u);
    CHECK(!r.rested);                                 // MARKET never rests
    CHECK(r.trades == 2u);
    CHECK(tr[0].price == 100);
    CHECK(tr[1].price == 101);
    CHECK(book.best_ask() == nullptr);
    CHECK(book.active_orders() == 0u);                // nothing left in the book
}

// ------------------------------------------------------------ IOC partial then discard
TEST_CASE(ioc_fills_what_it_can_then_discards) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 100, 3));
    MatchResult r = m.submit(mk(9, Side::BUY, 100, 10, OrderType::IOC), tr);

    CHECK(r.filled == 3u);
    CHECK(r.residual == 7u);
    CHECK(!r.rested);                                 // IOC residual discarded
    CHECK(book.best_bid() == nullptr);                // nothing rested on the bid
    CHECK(book.best_ask() == nullptr);                // ask fully consumed
    CHECK(book.active_orders() == 0u);
}

// ------------------------------------------------------------ non-crossing LIMIT just rests
TEST_CASE(non_crossing_limit_rests_without_trading) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::SELL, 105, 5));              // ask at 105
    MatchResult r = m.submit(mk(2, Side::BUY, 100, 5), tr);   // bid at 100: no cross

    CHECK(r.filled == 0u);
    CHECK(r.trades == 0u);
    CHECK(r.residual == 5u);
    CHECK(r.rested);
    REQUIRE(book.best_bid() != nullptr);
    CHECK(book.best_bid()->price == 100);
    CHECK(book.best_ask()->price == 105);
}

// ------------------------------------------------------------ sell taker crosses bids
TEST_CASE(sell_taker_crosses_bids_high_to_low) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    book.add(mk(1, Side::BUY, 100, 5));
    book.add(mk(2, Side::BUY, 99,  5));
    MatchResult r = m.submit(mk(9, Side::SELL, 99, 8), tr);   // hits 100 first, then 99

    CHECK(r.filled == 8u);
    CHECK(r.residual == 0u);
    REQUIRE(tr.size() == 2u);
    CHECK(tr[0].price == 100); CHECK(tr[0].quantity == 5u);   // best bid first
    CHECK(tr[1].price == 99);  CHECK(tr[1].quantity == 3u);
    CHECK(tr[0].taker_side == Side::SELL);
    REQUIRE(book.best_bid() != nullptr);
    CHECK(book.best_bid()->price == 99);
    CHECK(book.best_bid()->total_qty == 2u);                  // 5 - 3 remains
}

// ------------------------------------------------------------ empty book, no crash
TEST_CASE(market_into_empty_book_is_safe) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    MatchResult r = m.submit(mk(1, Side::BUY, 0, 5, OrderType::MARKET), tr);
    CHECK(r.filled == 0u);
    CHECK(r.residual == 5u);
    CHECK(r.trades == 0u);
    CHECK(tr.empty());
    CHECK(book.active_orders() == 0u);
}

// ------------------------------------------------------------ mock sink overflow: book stays consistent
TEST_CASE(sink_overflow_does_not_corrupt_book) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(1);        // room for only ONE event

    book.add(mk(1, Side::SELL, 100, 1));
    book.add(mk(2, Side::SELL, 100, 1));
    MatchResult r = m.submit(mk(9, Side::BUY, 100, 2), tr);   // 2 trades, sink holds 1

    CHECK(r.filled == 2u);                            // matching completes regardless
    CHECK(r.trades == 2u);
    CHECK(r.sink_overflow);                           // flagged
    CHECK(tr.size() == 1u);                           // never reallocated past capacity
    CHECK(book.best_ask() == nullptr);                // book fully consumed & consistent
    CHECK(book.active_orders() == 0u);
}

// ------------------------------------------------------------ multi-node level sweep
TEST_CASE(sweep_drains_a_multi_node_level) {
    Arena arena(32u * 1024 * 1024);
    OrderBook book(arena, 2048);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(512);

    const int N = 200;                                // > PIN capacity -> node chaining
    for (int i = 0; i < N; ++i)
        CHECK(book.add(mk(static_cast<OrderId>(i + 1), Side::SELL, 100, 1,
                          OrderType::LIMIT, static_cast<Seq>(i + 1))));

    MatchResult r = m.submit(mk(9999, Side::BUY, 100, N, OrderType::LIMIT), tr);

    CHECK(r.filled == static_cast<Qty>(N));
    CHECK(r.trades == static_cast<std::uint32_t>(N));
    CHECK(tr.size() == static_cast<std::size_t>(N));
    CHECK(tr[0].maker_id == 1u);                      // strict FIFO across the chain
    CHECK(tr[N - 1].maker_id == static_cast<OrderId>(N));
    CHECK(book.best_ask() == nullptr);
    CHECK(book.active_orders() == 0u);
    CHECK(book.free_node_count() > 0u);               // all chained nodes returned to pool
}

// ------------------------------------------------------------ intra-node FIFO survives hole refill
// Regression proof for the intra-node next_in_time chain: a cancel frees physical
// slot 0, a later insert refills slot 0, yet that refilled order must trade LAST.
TEST_CASE(intra_node_fifo_survives_cancel_and_refill) {
    Arena arena(16u * 1024 * 1024);
    OrderBook book(arena, 1024);
    Matcher m(book);
    std::vector<TradeEvent> tr; tr.reserve(64);

    // 3 resting sells at one price -> physical slots 0,1,2; FIFO 1 -> 2 -> 3.
    CHECK(book.add(mk(1, Side::SELL, 100, 1, OrderType::LIMIT, 1)));   // slot 0
    CHECK(book.add(mk(2, Side::SELL, 100, 1, OrderType::LIMIT, 2)));   // slot 1
    CHECK(book.add(mk(3, Side::SELL, 100, 1, OrderType::LIMIT, 3)));   // slot 2

    CHECK(book.cancel(1));                            // hole at physical slot 0

    // 4th order refills the LOWEST free slot (0), but is NEWEST in time.
    CHECK(book.add(mk(4, Side::SELL, 100, 1, OrderType::LIMIT, 4)));   // -> slot 0

    // Sweep the whole level (3 live orders x 1).
    MatchResult r = m.submit(mk(9, Side::BUY, 100, 3, OrderType::LIMIT), tr);

    CHECK(r.filled == 3u);
    REQUIRE(tr.size() == 3u);
    // Must execute 2nd and 3rd BEFORE the 4th -- despite the 4th sitting in slot 0.
    CHECK(tr[0].maker_id == 2u);                      // (naive ctzll would put id 4 first)
    CHECK(tr[1].maker_id == 3u);
    CHECK(tr[2].maker_id == 4u);
    CHECK(book.active_orders() == 0u);
}
