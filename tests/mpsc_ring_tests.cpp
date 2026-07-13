//
// tests/mpsc_ring_tests.cpp
// ThreadSanitizer stress for the Multi-Producer / Single-Consumer ring (MpscRing<T>).
// A deliberately tiny 1024-slot ring forces relentless full/empty cycling and wraparound
// under heavy producer contention, so any missing barrier on the per-cell published
// sequence surfaces as a data race (TSan) or as loss/duplication (the exact-once audit).
//
//   1. mpsc_basic_fifo_and_full_empty   -- single-threaded sanity: FIFO, empty, full.
//   2. mpsc_4producers_1consumer_no_loss -- 4 producers race 1,000,000 unique items through
//      a 1024-slot ring; 1 consumer drains. Asserts EXACTLY 1M received, zero loss, zero
//      duplication, zero out-of-range (torn) values.
//
#include "ut.hpp"

#include "titan/pipeline/mpsc_ring.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

using namespace titan::pipeline;

static inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

// ---- 1) Single-threaded semantics: FIFO order + full/empty boundaries -------------------
TEST_CASE(mpsc_basic_fifo_and_full_empty) {
    constexpr std::size_t RING = 1u << 3;   // 8 slots
    MpscRing<std::uint64_t, RING> ring;

    std::uint64_t out = 0;
    CHECK(!ring.try_consume(out));                       // empty at start

    for (std::uint64_t i = 0; i < RING; ++i) CHECK(ring.try_publish(i));   // fill to capacity
    CHECK(!ring.try_publish(999));                       // full -> rejects

    bool fifo_ok = true;
    for (std::uint64_t i = 0; i < RING; ++i) {
        if (!ring.try_consume(out) || out != i) { fifo_ok = false; break; }
    }
    CHECK(fifo_ok);                                      // drained in strict FIFO
    CHECK(!ring.try_consume(out));                       // empty again

    // wraparound: publish/consume another full lap to exercise the seq += Size recycle
    for (std::uint64_t i = 100; i < 100 + RING; ++i) CHECK(ring.try_publish(i));
    std::uint64_t got = ring.consume_batch([&](std::uint64_t) {});
    CHECK(got == RING);
    CHECK(ring.empty_approx());
}

// ---- 2) 4 producers, 1 consumer, 1,000,000 unique items, tiny ring ----------------------
TEST_CASE(mpsc_4producers_1consumer_no_loss) {
    constexpr std::uint64_t TOTAL     = 1'000'000;
    constexpr int           PRODUCERS = 4;
    constexpr std::uint64_t PER       = TOTAL / PRODUCERS;   // 250,000 each
    constexpr std::size_t   RING      = 1u << 10;            // 1024 slots -> ~977 wraparounds
    static_assert(PER * PRODUCERS == TOTAL, "TOTAL must divide evenly among producers");

    MpscRing<std::uint64_t, RING> ring;

    std::atomic<bool> go{false};

    // Producer p publishes the unique value range [p*PER, (p+1)*PER); zero-drop busy-spin.
    std::vector<std::thread> producers;
    producers.reserve(PRODUCERS);
    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p] {
            const std::uint64_t lo = static_cast<std::uint64_t>(p) * PER;
            const std::uint64_t hi = lo + PER;
            while (!go.load(std::memory_order_acquire)) { }
            for (std::uint64_t v = lo; v < hi; ++v)
                while (!ring.try_publish(v)) cpu_relax();
        });
    }

    // Single consumer: exact-once audit over the whole [0, TOTAL) value space.
    std::vector<std::uint8_t> seen(TOTAL, 0);
    std::uint64_t received = 0, dups = 0, oob = 0;
    std::thread consumer([&] {
        while (!go.load(std::memory_order_acquire)) { }
        while (received < TOTAL) {
            const std::uint64_t got = ring.consume_batch([&](std::uint64_t v) {
                if (v >= TOTAL)      ++oob;               // torn / impossible value
                else if (seen[v])    ++dups;             // duplicate delivery
                else                 seen[v] = 1;
                ++received;
            });
            if (got == 0) cpu_relax();
        }
    });

    go.store(true, std::memory_order_release);
    for (auto& t : producers) t.join();
    consumer.join();

    std::uint64_t missing = 0;
    for (std::uint64_t v = 0; v < TOTAL; ++v) if (!seen[v]) ++missing;

    CHECK(received == TOTAL);   // exactly 1,000,000 delivered
    CHECK(dups == 0);           // no duplication
    CHECK(oob == 0);            // no torn/out-of-range payloads (multi-producer publish is clean)
    CHECK(missing == 0);        // no loss -- every unique value arrived exactly once

    if (received == TOTAL && dups == 0 && oob == 0 && missing == 0)
        std::printf("    OK: %llu items via %d producers -> 1 consumer, ring=%zu slots, "
                    "exactly-once (0 loss / 0 dup / 0 torn)\n",
                    (unsigned long long)TOTAL, PRODUCERS, RING);
    else
        std::printf("    FAIL: received=%llu dups=%llu oob=%llu missing=%llu\n",
                    (unsigned long long)received, (unsigned long long)dups,
                    (unsigned long long)oob, (unsigned long long)missing);
}

int main() { return ut::run_all(); }
