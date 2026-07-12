//
// bench/ab_bench.cpp
// Thermal-invariant A/B micro-benchmark for the matching engine.
//
// Two live engines (A, B) are seeded identically and fed the SAME op stream in
// CHUNKS. Each chunk times A and B back-to-back (same thermal window) -> one
// ratio sample B/A. Execution order is alternated per chunk so first-position
// bias cancels. Slow thermal / turbo drift moves nsA and nsB together, so the
// RATIO is invariant even when the absolute numbers wander.
//
// First use = calibration: A and B are the SAME engine -> ratio must be ~1.00
// with a tight spread while absolute ns drift, proving the harness. Real
// optimizations later plug in as a different B engine; a per-run state check
// (active_orders + checksum) guards that B is behaviour-preserving.
//
// Build (RELEASE, no sanitizers): see ab.sh
//
#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/memory/arena.hpp"
#include "workload.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

using namespace titan;
using namespace titan::bench;

namespace {

constexpr std::size_t   ARENA_BYTES = 32ull * 1024 * 1024;  // price maps only now (slab is separate)
constexpr std::uint32_t MAX_NODES   = 1u << 16;             // 65,536 PIN nodes
constexpr std::uint64_t ID_CAPACITY = 2'000'000;           // dense-slab id space (> max id used)
constexpr std::size_t   SEED_ORDERS = 500'000;             // deep resting book (untimed)
constexpr std::size_t   N_OPS       = 2'000'000;           // total ops, split across chunks
constexpr int           CHUNKS      = 15;
constexpr std::uint64_t GEN_RNG     = 0x9E3779B97F4A7C15ull;
constexpr std::uint64_t SEED_RNG    = GEN_RNG ^ 0xABCDEFull;

// One self-contained engine instance: arena + book + matcher + mock egress.
// Templated on the book/matcher types so different layout variants can be A/B'd.
template <class Book, class Match>
struct EngineT {
    Arena arena;
    Book book;
    Match matcher;
    std::vector<TradeEvent> egress;
    std::uint64_t sink = 0;

    EngineT() : arena(ARENA_BYTES), book(arena, MAX_NODES, ID_CAPACITY), matcher(book) {
        egress.reserve(1024);
    }

    void seed() {
        std::mt19937_64 g(SEED_RNG);
        for (OrderId id = 1; id <= SEED_ORDERS; ++id) book.add(make_limit(id, (g() & 1u), g));
    }

    inline void apply(const Op& op) noexcept {
        if (op.kind == OP_SUBMIT) {
            const MatchResult r = matcher.submit(op.order, egress);
            sink += r.filled + r.trades;
            egress.clear();
        } else {
            sink += static_cast<std::uint64_t>(book.cancel(op.order.id));
        }
    }
};

// A = baseline (slots-first PIN_Node);  B = hot/cold (metadata-first PIN_Node_HC).
using EngineBase = EngineT<OrderBook, Matcher>;
using EngineHC   = EngineT<OrderBookT<PIN_Node_HC>, MatcherT<OrderBookT<PIN_Node_HC>>>;

// Time applying ops[lo,hi) to engine e; return elapsed ns for that slice.
template <class Eng>
inline double time_slice(Eng& e, const std::vector<Op>& ops, std::size_t lo, std::size_t hi) {
    const auto t0 = std::chrono::high_resolution_clock::now();
    for (std::size_t i = lo; i < hi; ++i) e.apply(ops[i]);
    const auto t1 = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::nano>(t1 - t0).count();
}

double median(std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; }
double vmin(const std::vector<double>& v) { return *std::min_element(v.begin(), v.end()); }
double vmax(const std::vector<double>& v) { return *std::max_element(v.begin(), v.end()); }

// Fine-grained interleave: alternate A and B every MB ops, accumulating each
// side's time within a chunk of many mini-batches. Because A and B alternate
// tightly, any scheduler hiccup during a chunk lands on BOTH -> the chunk's
// tB/tA ratio stays stable even though absolute ns drift.
template <class EngA, class EngB>
void ab_compare(EngA& a, EngB& b, const std::vector<Op>& ops,
                const char* na, const char* nb) {
    constexpr std::size_t MB = 256;                  // interleave granularity (ops)
    const std::size_t n = ops.size();
    const std::size_t total_mb     = (n + MB - 1) / MB;
    const std::size_t mb_per_chunk = (total_mb + CHUNKS - 1) / CHUNKS;

    std::vector<double> nsA, nsB, ratio;
    double tA = 0, tB = 0;
    std::size_t ops_acc = 0, mb_idx = 0;
    bool a_first = true;

    for (std::size_t lo = 0; lo < n; lo += MB) {
        const std::size_t hi = std::min(lo + MB, n);
        if (a_first) { tA += time_slice(a, ops, lo, hi); tB += time_slice(b, ops, lo, hi); }
        else         { tB += time_slice(b, ops, lo, hi); tA += time_slice(a, ops, lo, hi); }
        a_first = !a_first;
        ops_acc += (hi - lo);

        if (++mb_idx % mb_per_chunk == 0 || hi == n) {   // close out a chunk
            ratio.push_back(tB / tA);
            nsA.push_back(tA / static_cast<double>(ops_acc));
            nsB.push_back(tB / static_cast<double>(ops_acc));
            tA = tB = 0; ops_acc = 0;
        }
    }

    std::printf("A = %-10s  B = %-10s   chunks=%zu  interleave=%zu ops\n",
                na, nb, ratio.size(), MB);
    std::printf("  %-10s  median=%7.1f ns   drift[min=%7.1f max=%7.1f]\n",
                na, median(nsA), vmin(nsA), vmax(nsA));
    std::printf("  %-10s  median=%7.1f ns   drift[min=%7.1f max=%7.1f]\n",
                nb, median(nsB), vmin(nsB), vmax(nsB));
    std::printf("  RATIO B/A  median=%.4f   spread[min=%.4f max=%.4f]   (<1.0 => B faster)\n",
                median(ratio), vmin(ratio), vmax(ratio));

    const bool ok = (a.book.active_orders() == b.book.active_orders()) && (a.sink == b.sink);
    std::printf("  state: A.orders=%zu B.orders=%zu  A.chk=%llu B.chk=%llu  -> %s\n",
                a.book.active_orders(), b.book.active_orders(),
                (unsigned long long)a.sink, (unsigned long long)b.sink,
                ok ? "MATCH (behaviour-preserving)" : "DIVERGED!");
}

}  // namespace

int main() {
    std::size_t adds = 0, cancels = 0, mktioc = 0;
    const std::vector<Op> ops = generate_ops(SEED_ORDERS, N_OPS, GEN_RNG, &adds, &cancels, &mktioc);
    std::printf("workload: %zu ops (adds=%zu cancels=%zu mkt/ioc=%zu)  seed=%zu\n\n",
                ops.size(), adds, cancels, mktioc, SEED_ORDERS);

    // ---- A = baseline node layout,  B = hot/cold (metadata-first) node layout ----
    EngineBase a;
    EngineHC   b;
    a.seed();
    b.seed();
    ab_compare(a, b, ops, "baseline", "hotcold");

    std::printf("\n(Identical-code zero-point is 0.986. B (hot/cold) is a real win only if its"
                " RATIO median lands clearly below ~0.97.)\n");
    return 0;
}
