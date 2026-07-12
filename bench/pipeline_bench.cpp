//
// bench/pipeline_bench.cpp
// End-to-end Disruptor pipeline throughput, strict busy-spin everywhere (no yield).
//
//   A  inline            : matcher alone, 1 thread                 (pure matching)
//   B  2-thread          : Sequencer -> Ingress -> Matcher         (matcher sinks trades locally)
//   C  3-thread          : Sequencer -> Ingress -> Matcher -> Egress -> Publisher
//
// The Matcher publishes each TradeEvent to the EgressRing via try_publish with
// zero-drop backpressure (busy-spin if full). B vs C shows what the third core costs.
// A trade-value checksum cross-validates that every path produces identical fills.
//
// Build (RELEASE, -pthread, ASan OFF): see pipeline.sh
//
#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/book/trade_event.hpp"
#include "titan/memory/arena.hpp"
#include "titan/pipeline/egress_ring.hpp"
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
constexpr std::size_t   RING_IN     = 1u << 16;             // ingress slots
constexpr std::size_t   RING_OUT    = 1u << 18;             // egress slots (fits a full batch's trades)
constexpr std::size_t   LOCAL_CAP   = 1u << 20;             // matcher-local trade buffer reserve
constexpr std::uint32_t MAX_NODES   = 1u << 16;
constexpr std::uint64_t ID_CAP      = N + 64;
constexpr std::size_t   ARENA_BYTES = 32ull * 1024 * 1024;
constexpr PriceTick     MID         = 10'000;
constexpr PriceTick     BAND        = 8;                    // tight band -> heavy crossing, small book
constexpr int           REPS        = 3;
constexpr std::uint64_t GEN_SEED    = 0xF00DFACEC0FFEEULL;

std::uint64_t g_sink       = 0;   // anti dead-code-elimination
std::uint64_t g_chk_inline = 0;   // trade checksums (must all match)
std::uint64_t g_chk_2t     = 0;
std::uint64_t g_chk_3t     = 0;

inline std::uint64_t hash_trade(const TradeEvent& t) noexcept {
    return t.taker_id + t.maker_id + static_cast<std::uint64_t>(t.price) + t.quantity;
}

// Trade sink that folds every fill into a checksum; never full (returns true).
struct ChecksumSink {
    std::uint64_t chk = 0;
    bool try_publish(const TradeEvent& t) noexcept { chk += hash_trade(t); return true; }
};

// Matcher-thread-local sink: buffers trades so the matcher can batch-publish them to
// the egress ring after each ingress batch (one release-store, not one per trade).
struct BufferSink {
    std::vector<TradeEvent>& buf;
    bool try_publish(const TradeEvent& t) noexcept { buf.push_back(t); return true; }
};

std::vector<Order> generate_orders() {
    std::mt19937_64 rng(GEN_SEED);
    std::vector<Order> v; v.reserve(N);
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

// ---------------- A: matcher alone (1 thread) ----------------
double run_inline(const std::vector<Order>& orders) {
    Arena arena(ARENA_BYTES);
    OrderBook book(arena, MAX_NODES, ID_CAP);
    Matcher matcher(book);
    ChecksumSink sink;

    const auto t0 = std::chrono::high_resolution_clock::now();
    for (std::uint64_t i = 0; i < N; ++i) {
        Order o = orders[i]; o.seq = i;
        matcher.submit(o, sink);
    }
    const auto t1 = std::chrono::high_resolution_clock::now();

    g_sink += sink.chk; g_chk_inline = sink.chk;
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(N);
}

// ---------------- B: 2-thread (Sequencer -> Ingress -> Matcher) ----------------
double run_pipeline2(const std::vector<Order>& orders) {
    IngressRing<RING_IN> ingress;
    std::atomic<int>           ready{0};
    std::atomic<bool>          go{false};
    std::atomic<std::uint64_t> out_chk{0};

    std::thread matcher_thread([&] {
        Arena arena(ARENA_BYTES);
        OrderBook book(arena, MAX_NODES, ID_CAP);
        Matcher matcher(book);
        ChecksumSink sink;
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        std::uint64_t c = 0;
        while (c < N) {
            c += ingress.consume_batch([&](const Order& in) { matcher.submit(in, sink); });
        }
        out_chk.store(sink.chk, std::memory_order_relaxed);
    });
    std::thread sequencer([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        for (std::uint64_t i = 0; i < N; ++i) {
            Order o = orders[i]; o.seq = i;
            while (!ingress.try_publish(o)) { }
        }
    });

    while (ready.load(std::memory_order_acquire) != 2) { }
    const auto t0 = std::chrono::high_resolution_clock::now();
    go.store(true, std::memory_order_release);
    sequencer.join();
    matcher_thread.join();
    const auto t1 = std::chrono::high_resolution_clock::now();

    g_sink += out_chk.load(); g_chk_2t = out_chk.load();
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(N);
}

// ---------------- C: 3-thread (Sequencer -> Ingress -> Matcher -> Egress -> Publisher) ----------------
double run_pipeline3(const std::vector<Order>& orders) {
    IngressRing<RING_IN>  ingress;
    EgressRing<RING_OUT>  egress;
    std::atomic<int>           ready{0};
    std::atomic<bool>          go{false};
    std::atomic<bool>          matcher_done{false};
    std::atomic<std::uint64_t> pub_chk{0};

    // T2: Matcher — drains ingress, publishes each trade to egress (zero-drop).
    std::thread matcher_thread([&] {
        Arena arena(ARENA_BYTES);
        OrderBook book(arena, MAX_NODES, ID_CAP);
        Matcher matcher(book);
        std::vector<TradeEvent> local_trades;
        local_trades.reserve(LOCAL_CAP);              // pre-reserved -> no realloc on the hot path
        BufferSink sink{local_trades};
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        std::uint64_t c = 0;
        while (c < N) {
            // Match a whole ingress batch into the local buffer, then flush it to egress
            // with a SINGLE release-store (zero-drop busy-spin inside publish_batch).
            c += ingress.consume_batch([&](const Order& in) { matcher.submit(in, sink); });
            if (!local_trades.empty()) {
                egress.publish_batch(local_trades);
                local_trades.clear();
            }
        }
        matcher_done.store(true, std::memory_order_release);
    });

    // T3: Publisher — batch-drains egress, folds trades into a checksum.
    std::thread publisher([&] {
        std::uint64_t chk = 0;
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        for (;;) {
            const std::uint64_t got = egress.consume_batch([&](const TradeEvent& t) { chk += hash_trade(t); });
            if (got == 0 && matcher_done.load(std::memory_order_acquire) && egress.empty_approx()) break;
        }
        pub_chk.store(chk, std::memory_order_relaxed);
    });

    // T1: Sequencer.
    std::thread sequencer([&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!go.load(std::memory_order_acquire)) { }
        for (std::uint64_t i = 0; i < N; ++i) {
            Order o = orders[i]; o.seq = i;
            while (!ingress.try_publish(o)) { }
        }
    });

    while (ready.load(std::memory_order_acquire) != 3) { }
    const auto t0 = std::chrono::high_resolution_clock::now();
    go.store(true, std::memory_order_release);
    sequencer.join();
    matcher_thread.join();
    publisher.join();
    const auto t1 = std::chrono::high_resolution_clock::now();

    g_sink += pub_chk.load(); g_chk_3t = pub_chk.load();
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / static_cast<double>(N);
}

double best_of(double (*fn)(const std::vector<Order>&), const std::vector<Order>& o, const char* tag) {
    double best = 1e18;
    for (int r = 0; r < REPS; ++r) {
        const double ns = fn(o);
        best = std::min(best, ns);
        std::printf("  %-9s rep %d:  %6.1f ns/msg   %6.2f M msgs/s\n", tag, r, ns, 1000.0 / ns);
    }
    return best;
}

}  // namespace

int main() {
    std::printf("pre-generating %llu orders (band +/-%lld around %lld)...\n",
                (unsigned long long)N, (long long)BAND, (long long)MID);
    const std::vector<Order> orders = generate_orders();

    std::printf("\n(A) matcher alone (1 thread):\n");
    const double a = best_of(run_inline, orders, "inline");
    std::printf("\n(B) Sequencer -> Ingress -> Matcher (2 threads):\n");
    const double b = best_of(run_pipeline2, orders, "2-thread");
    std::printf("\n(C) Sequencer -> Ingress -> Matcher -> Egress -> Publisher (3 threads):\n");
    const double c = best_of(run_pipeline3, orders, "3-thread");

    std::printf("\n=========================================================\n");
    std::printf("  A inline    : %6.1f ns/msg   (%.2f M msgs/s)\n", a, 1000.0 / a);
    std::printf("  B 2-thread  : %6.1f ns/msg   (%.2f M msgs/s)\n", b, 1000.0 / b);
    std::printf("  C 3-thread  : %6.1f ns/msg   (%.2f M msgs/s)\n", c, 1000.0 / c);
    std::printf("  3rd-core impact (C vs B): %+.1f ns/msg   (%+.1f%%)\n", c - b, 100.0 * (c - b) / b);
    std::printf("  checksums: inline=%llu 2t=%llu 3t=%llu -> %s\n",
                (unsigned long long)g_chk_inline, (unsigned long long)g_chk_2t, (unsigned long long)g_chk_3t,
                (g_chk_inline == g_chk_2t && g_chk_2t == g_chk_3t) ? "MATCH" : "DIVERGED!");
    std::printf("  sink=%llu\n", (unsigned long long)g_sink);
    return 0;
}
