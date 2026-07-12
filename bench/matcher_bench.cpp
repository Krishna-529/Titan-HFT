//
// bench/matcher_bench.cpp
// Core throughput baseline for the PIN matching engine.
//
// Workload: 5,000,000 pre-generated operations (RNG excluded from timing) in a
// realistic mix -> 40% limit adds, 55% cancels (O(1) id_index_), 5% market/IOC.
// A deep resting book (SEED_ORDERS) is built first, UNTIMED, so the price map and
// id_index_ are realistically sized (cache-cold lookups, not an optimistic
// L1-resident structure). Cancels target live ids so they actually hit the index.
//
// Dispatch: adds & market/IOC -> Matcher::submit; cancels -> OrderBook::cancel.
//
// Build (RELEASE ONLY, no sanitizers):  see bench.sh
//     g++ -std=c++20 -O3 -march=native -DNDEBUG -Iinclude bench/matcher_bench.cpp
//
#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/memory/arena.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <vector>

using namespace titan;

namespace {

constexpr std::size_t   ARENA_BYTES = 256ull * 1024 * 1024;  // 256 MB monotonic+pool arena
constexpr std::uint32_t MAX_NODES   = 1u << 17;              // 131,072 PIN nodes (pool headroom)
constexpr std::size_t   SEED_ORDERS = 1'000'000;            // pre-seeded resting depth (untimed)
constexpr std::size_t   N_OPS       = 5'000'000;            // timed operations
constexpr std::uint64_t ID_CAPACITY = 4'000'000;           // dense-slab id space (> max id used)
constexpr int           REPS        = 7;
constexpr PriceTick     MID         = 100'000;
constexpr PriceTick     BAND        = 128;                   // +/- ticks around MID
constexpr std::uint64_t SEED_RNG    = 0x9E3779B97F4A7C15ull;

enum OpKind : std::uint8_t { OP_SUBMIT = 0, OP_CANCEL = 1 };

struct Op {
    Order  order;   // SUBMIT: the order.  CANCEL: order.id holds the target id.
    OpKind kind;
};

// A passive limit: bids strictly below MID, asks strictly above -> they never
// cross each other, so limit adds rest and build depth; market/IOC do the trading.
inline Order make_limit(OrderId id, bool buy, std::mt19937_64& rng) noexcept {
    Order o{};
    o.id = id; o.seq = id;
    o.price = buy ? (MID - 1 - static_cast<PriceTick>(rng() % BAND))
                  : (MID + 1 + static_cast<PriceTick>(rng() % BAND));
    o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 10);
    o.side = buy ? Side::BUY : Side::SELL;
    o.type = OrderType::LIMIT;
    return o;
}

}  // namespace

int main() {
    // ---------- Pre-generate the deterministic op stream (excluded from timing) ----------
    std::mt19937_64 rng(SEED_RNG);
    std::vector<Op> ops;
    ops.reserve(N_OPS);

    std::vector<OrderId> live;                 // ids believed to be resting (cancel targets)
    live.reserve(SEED_ORDERS + N_OPS / 2);
    for (OrderId id = 1; id <= SEED_ORDERS; ++id) live.push_back(id);   // seed ids are live

    OrderId next_id = SEED_ORDERS + 1;
    std::size_t n_add = 0, n_cancel = 0, n_mktioc = 0;

    for (std::size_t i = 0; i < N_OPS; ++i) {
        const std::uint32_t roll = static_cast<std::uint32_t>(rng() % 100);
        if (roll < 40 || live.empty()) {                       // 40% limit add
            const OrderId id = next_id++;
            const bool buy = (rng() & 1u);
            ops.push_back(Op{ make_limit(id, buy, rng), OP_SUBMIT });
            live.push_back(id);
            ++n_add;
        } else if (roll < 95) {                                // 55% cancel (targets a live id)
            const std::size_t k = static_cast<std::size_t>(rng() % live.size());
            const OrderId id = live[k];
            live[k] = live.back(); live.pop_back();             // O(1) swap-remove
            Order o{}; o.id = id;
            ops.push_back(Op{ o, OP_CANCEL });
            ++n_cancel;
        } else {                                               // 5% market / IOC
            const OrderId id = next_id++;
            const bool buy = (rng() & 1u);
            Order o{};
            o.id = id; o.seq = id;
            o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 5);
            o.side = buy ? Side::BUY : Side::SELL;
            if (rng() & 1u) { o.type = OrderType::MARKET; o.price = 0; }
            else            { o.type = OrderType::IOC; o.price = buy ? (MID + BAND) : (MID - BAND); }
            ops.push_back(Op{ o, OP_SUBMIT });
            ++n_mktioc;
        }
    }

    std::printf("pre-generated %zu ops  (adds=%zu  cancels=%zu  mkt/ioc=%zu)\n",
                ops.size(), n_add, n_cancel, n_mktioc);
    std::printf("seed=%zu resting orders  arena=%zuMB  max_nodes=%u\n\n",
                SEED_ORDERS, ARENA_BYTES >> 20, MAX_NODES);

    // ---------- Timed runs on a freshly-seeded book ----------
    std::vector<double> ns_all;
    ns_all.reserve(REPS);
    for (int rep = 0; rep < REPS; ++rep) {
        Arena arena(ARENA_BYTES);
        OrderBook book(arena, MAX_NODES, ID_CAPACITY);
        Matcher matcher(book);

        // Seed a deep resting book (UNTIMED). Deterministic ids 1..SEED_ORDERS.
        std::size_t seeded = 0;
        {
            std::mt19937_64 srng(SEED_RNG ^ 0xABCDEFull);
            for (OrderId id = 1; id <= SEED_ORDERS; ++id) {
                if (book.add(make_limit(id, (srng() & 1u), srng))) ++seeded;
            }
        }

        std::vector<TradeEvent> egress;
        egress.reserve(1024);

        std::uint64_t sink = 0;                 // anti dead-code-elimination
        std::uint64_t fills = 0, trades = 0, cxl_hits = 0;

        const auto t0 = std::chrono::high_resolution_clock::now();
        for (const Op& op : ops) {
            if (op.kind == OP_SUBMIT) {
                const MatchResult r = matcher.submit(op.order, egress);
                sink += r.filled + r.trades;
                fills += r.filled; trades += r.trades;
                egress.clear();                 // drain mock egress -> stays bounded
            } else {
                const bool ok = book.cancel(op.order.id);
                sink += static_cast<std::uint64_t>(ok);
                cxl_hits += static_cast<std::uint64_t>(ok);
            }
        }
        const auto t1 = std::chrono::high_resolution_clock::now();

        const double ns     = std::chrono::duration<double, std::nano>(t1 - t0).count();
        const double mps    = static_cast<double>(N_OPS) / (ns / 1e9) / 1e6;
        const double ns_per = ns / static_cast<double>(N_OPS);
        ns_all.push_back(ns_per);

        std::printf("rep %d:  %6.2f M msgs/s   %6.2f ns/msg   "
                    "[seeded=%zu fills=%llu trades=%llu cxl_hits=%llu "
                    "book=%zu freenodes=%u chk=%llu]\n",
                    rep, mps, ns_per, seeded,
                    (unsigned long long)fills, (unsigned long long)trades,
                    (unsigned long long)cxl_hits, book.active_orders(),
                    book.free_node_count(), (unsigned long long)sink);
    }

    std::sort(ns_all.begin(), ns_all.end());
    const double ns_min = ns_all.front();
    const double ns_med = ns_all[ns_all.size() / 2];
    const double ns_max = ns_all.back();
    std::printf("\nns/msg  min=%.1f  median=%.1f  max=%.1f    "
                "(min-latency throughput = %.2f M msgs/s)\n",
                ns_min, ns_med, ns_max, 1000.0 / ns_min);
    return 0;
}
