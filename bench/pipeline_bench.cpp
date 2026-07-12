//
// bench/pipeline_bench.cpp
// Phase 3 wiring benchmark: Sequencer (producer thread) -> IngressRing -> Matcher
// (consumer thread), with a strict busy-spin wait strategy on both sides (no yield,
// lowest latency). Mock egress (std::vector<TradeEvent>) stays inside the Matcher
// thread so we isolate the ingress-to-matcher path.
//
// Reports, on an identical pre-generated order stream:
//   (A) matcher alone (single thread, no ring)   -> pure matching cost
//   (B) two-thread ring pipeline                  -> matching + ring/coherence cost
// The delta (B - A) is the ring overhead per message.
//
// Build (RELEASE, -pthread): see pipeline.sh
//
#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/memory/arena.hpp"
#include "titan/pipeline/ingress_ring.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

using namespace titan;
using namespace titan::pipeline;

namespace {

constexpr std::uint64_t N           = 5'000'000;
constexpr std::size_t   RING        = 1u << 16;              // 65536 slots (decouples the threads)
constexpr std::uint32_t MAX_NODES   = 1u << 16;             // PIN node pool
constexpr std::uint64_t ID_CAP      = N + 64;               // dense slab covers ids 1..N
constexpr std::size_t   ARENA_BYTES = 32ull * 1024 * 1024;  // backs the RB price indices
constexpr PriceTick     MID         = 10'000;
constexpr PriceTick     BAND        = 8;                    // tight band -> heavy crossing, small book
constexpr int           REPS        = 3;
constexpr std::uint64_t GEN_SEED    = 0xF00DFACEC0FFEEULL;

std::uint64_t g_sink = 0;   // anti dead-code-elimination

// A stream of marketable-ish limits: random side, random price in a tight band so
// the flow crosses heavily and the resting book stays small and cache-hot.
std::vector<Order> generate_orders() {
    std::mt19937_64 rng(GEN_SEED);
    std::vector<Order> v;
    v.reserve(N);
    for (std::uint64_t i = 0; i < N; ++i) {
        Order o{};
        o.id       = i + 1;
        o.price    = MID - BAND + static_cast<PriceTick>(rng() % (2 * BAND + 1));
        o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 4);
        o.side     = (rng() & 1u) ? Side::BUY : Side::SELL;
        o.type     = OrderType::LIMIT;
        v.push_back(o);
    }
    return v;
}

// (A) Matcher alone: submit every order on this thread. Returns ns/order.
double run_single(const std::vector<Order>& orders) {
    Arena arena(ARENA_BYTES);
    OrderBook book(arena, MAX_NODES, ID_CAP);
    Matcher matcher(book);
    std::vector<TradeEvent> egress; egress.reserve(1024);
    std::uint64_t s = 0;

    const auto t0 = std::chrono::high_resolution_clock::now();
    for (std::uint64_t i = 0; i < N; ++i) {
        Order o = orders[i];
        o.seq = i;
        const MatchResult r = matcher.submit(o, egress);
        s += r.filled + r.trades;
        egress.clear();
    }
    const auto t1 = std::chrono::high_resolution_clock::now();

    g_sink += s;
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(N);
}

// (B) Two-thread pipeline: Sequencer publishes into the ring, Matcher consumes.
// Book construction happens BEFORE the start barrier, so it is excluded from timing.
double run_pipeline(const std::vector<Order>& orders) {
    IngressRing<RING> ring;
    std::atomic<int>           ready{0};
    std::atomic<bool>          go{false};
    std::atomic<std::uint64_t> sink{0};

    // ---- Consumer: the Matcher ----
    std::thread matcher_thread([&] {
        Arena arena(ARENA_BYTES);
        OrderBook book(arena, MAX_NODES, ID_CAP);
        Matcher matcher(book);
        std::vector<TradeEvent> egress; egress.reserve(1024);
        std::uint64_t s = 0;

        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }        // busy-wait for start

        Order in{};
        for (std::uint64_t c = 0; c < N; ) {
            if (ring.try_consume(in)) {                        // strict busy-spin (no yield)
                const MatchResult r = matcher.submit(in, egress);
                s += r.filled + r.trades;
                egress.clear();
                ++c;
            }
        }
        sink.store(s, std::memory_order_relaxed);
    });

    // ---- Producer: the Sequencer ----
    std::thread sequencer([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        for (std::uint64_t i = 0; i < N; ++i) {
            Order o = orders[i];
            o.seq = i;                                         // monotonically increasing sequence
            while (!ring.try_publish(o)) { }                   // busy-spin if the ring is full
        }
    });

    while (ready.load(std::memory_order_acquire) != 2) { }     // both threads set up & at the barrier
    const auto t0 = std::chrono::high_resolution_clock::now();
    go.store(true, std::memory_order_release);
    sequencer.join();
    matcher_thread.join();
    const auto t1 = std::chrono::high_resolution_clock::now();

    g_sink += sink.load();
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(N);
}

double best_of(double (*fn)(const std::vector<Order>&), const std::vector<Order>& o, const char* tag) {
    double best = 1e18;
    for (int r = 0; r < REPS; ++r) {
        const double ns = fn(o);
        best = std::min(best, ns);
        std::printf("  %-10s rep %d:  %6.1f ns/msg   %5.2f M msgs/s\n",
                    tag, r, ns, 1000.0 / ns);
    }
    return best;
}

}  // namespace

int main() {
    std::printf("pre-generating %llu orders (band +/-%lld around %lld)...\n",
                (unsigned long long)N, (long long)BAND, (long long)MID);
    const std::vector<Order> orders = generate_orders();

    std::printf("\n(A) matcher alone (no ring):\n");
    const double a = best_of(run_single, orders, "single");

    std::printf("\n(B) sequencer -> ring -> matcher (2 threads, busy-spin):\n");
    const double b = best_of(run_pipeline, orders, "pipeline");

    std::printf("\n=========================================================\n");
    std::printf("  matcher alone : %6.1f ns/msg   (%.2f M msgs/s)\n", a, 1000.0 / a);
    std::printf("  ring pipeline : %6.1f ns/msg   (%.2f M msgs/s)\n", b, 1000.0 / b);
    std::printf("  ring overhead : %+.1f ns/msg   (%.1f%%)\n", b - a, 100.0 * (b - a) / a);
    std::printf("  checksum=%llu\n", (unsigned long long)g_sink);
    return 0;
}
