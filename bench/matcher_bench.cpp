//
// bench/matcher_bench.cpp
// Core throughput baseline for the PIN matching engine, PLUS the A/B harness for the
// cancel-path software-prefetch optimization.
//
// Workload: 5,000,000 pre-generated operations (RNG excluded from timing) in a
// realistic mix -> 40% limit adds, 55% cancels (O(1) id_index_), 5% market/IOC.
// A deep resting book (SEED_ORDERS) is built first, UNTIMED, so the price map and
// id_index_ are realistically sized (cache-cold lookups, not an optimistic
// L1-resident structure). Cancels target live ids so they actually hit the index.
//
// Dispatch: adds & market/IOC -> Matcher::submit; cancels -> OrderBook::cancel.
//
// THE OPTIMIZATION UNDER TEST -- cancel() is two dependent cache misses (id-slab, then
// node pool). Because the op stream is known ahead, we software-prefetch the slab + node
// lines for upcoming cancels, hiding the miss latency behind the previous op's work
// (exactly the trick the ingress ring uses for its coherence miss). The prefetch is
// correctness-neutral, so the WITH and WITHOUT passes must produce identical checksums.
//
// MEASUREMENT -- absolute ns/op on this WSL2 box drifts ~30% run-to-run with thermal state,
// so a cross-run "before vs after" comparison is noise. Instead this runs an IN-PROCESS A/B:
// both variants execute back-to-back on freshly-seeded books, order alternated per rep to
// cancel warm-up bias, and we report the median RATIO (prefetch / plain). That ratio is
// thermal-invariant -- both passes see the same core state within a rep.
//
// Modes (argv[1]):  (none) -> A/B ratio    "on" -> single prefetch pass    "off" -> single plain pass
// The single-pass modes exist for the profiler (see profile.sh), which needs one clean path.
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
#include <cstring>
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

// Software-prefetch look-ahead distances (in ops). The slab line is warmed furthest out;
// the node line is warmed nearer, by which point that cancel's slab line is resident so
// reading loc.node to find the node index is itself cheap. Tuned by hand on this box --
// far enough to hide an LLC/DRAM miss (~a few ops of work), near enough that the 128 MB
// slab's eviction pressure hasn't dropped the line before use.
constexpr std::size_t PF_DIST_SLAB = 24;
constexpr std::size_t PF_DIST_NODE = 10;

enum OpKind : std::uint8_t { OP_SUBMIT = 0, OP_CANCEL = 1 };

struct Op {
    Order  order;   // SUBMIT: the order.  CANCEL: order.id holds the target id.
    OpKind kind;
};

// Cheap egress sink for the single-thread bench: counts trades, never full.
struct CountingSink {
    std::uint64_t count = 0;
    bool try_publish(const TradeEvent&) noexcept { ++count; return true; }
};

struct PassResult {
    double        ns_per_op = 0;
    std::uint64_t checksum  = 0;   // MUST be identical across prefetch on/off (correctness gate)
    std::uint64_t fills = 0, trades = 0, cxl_hits = 0;
    std::size_t   seeded = 0, book_live = 0;
    std::uint32_t free_nodes = 0;
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

// One fully self-contained timed pass on a fresh, deterministically-seeded book.
// Prefetch is a COMPILE-TIME parameter: with `if constexpr`, the plain pass carries
// literally no prefetch code, so the A/B compares the engine, not a runtime toggle.
template <bool Prefetch>
PassResult run_pass(const std::vector<Op>& ops) {
    Arena arena(ARENA_BYTES);
    OrderBook book(arena, MAX_NODES, ID_CAPACITY);
    Matcher matcher(book);

    // Seed a deep resting book (UNTIMED). Deterministic ids 1..SEED_ORDERS.
    std::size_t seeded = 0;
    {
        std::mt19937_64 srng(SEED_RNG ^ 0xABCDEFull);
        for (OrderId id = 1; id <= SEED_ORDERS; ++id)
            if (book.add(make_limit(id, (srng() & 1u), srng))) ++seeded;
    }

    CountingSink egress;
    std::uint64_t sink = 0, fills = 0, trades = 0, cxl_hits = 0;
    const std::size_t n = ops.size();

    const auto t0 = std::chrono::high_resolution_clock::now();
    for (std::size_t i = 0; i < n; ++i) {
        if constexpr (Prefetch) {
            // Two-stage pipeline: warm the slab line furthest ahead, the node line nearer.
            const std::size_t s = i + PF_DIST_SLAB;
            if (s < n && ops[s].kind == OP_CANCEL) book.prefetch_slab(ops[s].order.id);
            const std::size_t d = i + PF_DIST_NODE;
            if (d < n && ops[d].kind == OP_CANCEL) book.prefetch_node(ops[d].order.id);
        }
        const Op& op = ops[i];
        if (op.kind == OP_SUBMIT) {
            const MatchResult r = matcher.submit(op.order, egress);
            sink += r.filled + r.trades;
            fills += r.filled; trades += r.trades;
        } else {
            const bool ok = book.cancel(op.order.id);
            sink += static_cast<std::uint64_t>(ok);
            cxl_hits += static_cast<std::uint64_t>(ok);
        }
    }
    const auto t1 = std::chrono::high_resolution_clock::now();

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    return PassResult{ ns / static_cast<double>(n), sink, fills, trades, cxl_hits,
                       seeded, book.active_orders(), book.free_node_count() };
}

std::vector<Op> generate_ops() {
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
    std::printf("seed=%zu resting orders  arena=%zuMB  max_nodes=%u  pf_dist(slab=%zu node=%zu)\n\n",
                SEED_ORDERS, ARENA_BYTES >> 20, MAX_NODES, PF_DIST_SLAB, PF_DIST_NODE);
    return ops;
}

double median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

// Single-mode run (for the profiler): one variant, REPS times, min/median/max ns.
template <bool Prefetch>
int run_single(const std::vector<Op>& ops, const char* label) {
    std::vector<double> ns;
    ns.reserve(REPS);
    std::uint64_t chk0 = 0;
    for (int rep = 0; rep < REPS; ++rep) {
        const PassResult r = run_pass<Prefetch>(ops);
        ns.push_back(r.ns_per_op);
        if (rep == 0) chk0 = r.checksum;
        std::printf("[%s] rep %d:  %6.2f ns/msg  [fills=%llu trades=%llu cxl_hits=%llu "
                    "book=%zu freenodes=%u chk=%llu]\n",
                    label, rep, r.ns_per_op,
                    (unsigned long long)r.fills, (unsigned long long)r.trades,
                    (unsigned long long)r.cxl_hits, r.book_live, r.free_nodes,
                    (unsigned long long)r.checksum);
    }
    std::sort(ns.begin(), ns.end());
    std::printf("\n[%s] ns/msg  min=%.1f  median=%.1f  max=%.1f   (chk=%llu)\n",
                label, ns.front(), ns[ns.size() / 2], ns.back(), (unsigned long long)chk0);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<Op> ops = generate_ops();

    // ---- Profiler single-pass modes ----
    if (argc > 1 && std::strcmp(argv[1], "on")  == 0) return run_single<true >(ops, "prefetch");
    if (argc > 1 && std::strcmp(argv[1], "off") == 0) return run_single<false>(ops, "plain");

    // ---- Default: in-process A/B ratio (thermal-invariant) ----
    std::vector<double> plain_ns, pf_ns;
    plain_ns.reserve(REPS); pf_ns.reserve(REPS);
    std::uint64_t chk_plain = 0, chk_pf = 0;
    bool checksum_ok = true;

    for (int rep = 0; rep < REPS; ++rep) {
        // Alternate order each rep so neither variant systematically runs on a warmer core.
        PassResult a, b;
        if (rep & 1) { b = run_pass<true>(ops);  a = run_pass<false>(ops); }
        else         { a = run_pass<false>(ops); b = run_pass<true>(ops);  }

        plain_ns.push_back(a.ns_per_op);
        pf_ns.push_back(b.ns_per_op);
        if (rep == 0) { chk_plain = a.checksum; chk_pf = b.checksum; }
        if (a.checksum != chk_plain || b.checksum != chk_pf || a.checksum != b.checksum)
            checksum_ok = false;

        std::printf("rep %d:  plain %6.2f ns   prefetch %6.2f ns   ratio %.4f   "
                    "[cxl_hits=%llu book=%zu freenodes=%u]\n",
                    rep, a.ns_per_op, b.ns_per_op, b.ns_per_op / a.ns_per_op,
                    (unsigned long long)b.cxl_hits, b.book_live, b.free_nodes);
    }

    const double med_plain = median(plain_ns);
    const double med_pf    = median(pf_ns);
    const double ratio     = med_pf / med_plain;

    std::printf("\n==== cancel-prefetch A/B (median of %d reps, in-process) ====\n", REPS);
    std::printf("plain     median = %7.2f ns/msg\n", med_plain);
    std::printf("prefetch  median = %7.2f ns/msg\n", med_pf);
    std::printf("ratio (pf/plain) = %.4f   -> %+.1f%% %s\n",
                ratio, (ratio - 1.0) * 100.0, ratio < 1.0 ? "(faster)" : "(SLOWER)");
    std::printf("checksums identical: %s  (plain=%llu prefetch=%llu)\n",
                checksum_ok ? "YES" : "NO -- CORRECTNESS BUG",
                (unsigned long long)chk_plain, (unsigned long long)chk_pf);
    return checksum_ok ? 0 : 1;
}
