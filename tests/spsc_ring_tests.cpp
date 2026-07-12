//
// tests/spsc_ring_tests.cpp
// ThreadSanitizer stress tests for the generic SPSC Disruptor ring (SpscRing<T>,
// exercised here via IngressRing = SpscRing<Order>). Every path uses a deliberately
// tiny 1024-slot ring to force heavy wraparound + full/empty cycling, so a single
// missing release/acquire barrier surfaces as a data race on the slot memory.
//
// Covered:
//   1. try_publish          -> try_consume       (single-element)
//   2. try_publish          -> consume_batch      (batch drain)
//   3. publish_batch(1..50) -> consume_batch      (batch publish + drain)
// Each pushes 5,000,000 items and asserts strict monotonic seq order + zero loss.
//
#include "ut.hpp"

#include "titan/book/order.hpp"
#include "titan/pipeline/ingress_ring.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

using namespace titan;
using namespace titan::pipeline;

static inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

TEST_CASE(spsc_ring_preserves_strict_order_under_contention) {
    constexpr std::uint64_t N    = 5'000'000;   // orders pushed through the ring
    constexpr std::size_t   RING = 1u << 10;    // 1024 slots -> ~4883 wraparounds

    IngressRing<RING> ring;

    std::atomic<bool>          producer_done{false};
    std::atomic<bool>          abort_run{false};      // let either thread stop the other on failure
    std::atomic<std::uint64_t> received{0};
    std::atomic<bool>          ordering_ok{true};
    std::atomic<std::uint64_t> bad_expected{0};
    std::atomic<std::uint64_t> bad_got{0};

    // ---- Consumer (the Matcher side): pop N, assert seq == expected, in order ----
    std::thread consumer([&] {
        Order out{};
        for (std::uint64_t expected = 0; expected < N; ++expected) {
            while (!ring.try_consume(out)) {
                if (producer_done.load(std::memory_order_acquire) && ring.empty_approx()) {
                    abort_run.store(true, std::memory_order_release);   // drained but short -> loss
                    return;
                }
                cpu_relax();
            }
            if (out.seq != expected) {                                 // reorder / tear / duplicate
                bad_expected.store(expected, std::memory_order_relaxed);
                bad_got.store(out.seq, std::memory_order_relaxed);
                ordering_ok.store(false, std::memory_order_relaxed);
                abort_run.store(true, std::memory_order_release);
                return;
            }
            received.store(expected + 1, std::memory_order_relaxed);
        }
    });

    // ---- Producer (the Sequencer side): push N orders stamped seq = i ----
    std::thread producer([&] {
        Order o{};
        o.side = Side::BUY; o.type = OrderType::LIMIT;
        o.price = 100; o.quantity = 1; o.remaining = 1;
        for (std::uint64_t i = 0; i < N; ++i) {
            o.id = i; o.seq = i;
            while (!ring.try_publish(o)) {
                if (abort_run.load(std::memory_order_acquire)) return;  // consumer bailed
                cpu_relax();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    CHECK(ordering_ok.load());
    CHECK(received.load() == N);

    if (!ordering_ok.load()) {
        std::printf("    ORDER VIOLATION: expected seq=%llu but got seq=%llu\n",
                    (unsigned long long)bad_expected.load(), (unsigned long long)bad_got.load());
    } else if (received.load() != N) {
        std::printf("    LOSS: received %llu of %llu orders\n",
                    (unsigned long long)received.load(), (unsigned long long)N);
    } else {
        std::printf("    OK: %llu orders, strict in-order, ring=%zu slots (~%llu wraparounds)\n",
                    (unsigned long long)N, RING, (unsigned long long)(N / RING));
    }
}

// Same stress, but the consumer uses consume_batch() (snapshot cursor, drain the run,
// prefetch, single release-store) instead of one-at-a-time try_consume. Verifies the
// batch path preserves strict order ACROSS batch boundaries, race-free under TSan.
TEST_CASE(spsc_batch_drain_preserves_strict_order) {
    constexpr std::uint64_t N    = 5'000'000;
    constexpr std::size_t   RING = 1u << 10;

    IngressRing<RING> ring;

    std::atomic<bool>          producer_done{false};
    std::atomic<bool>          abort_run{false};
    std::atomic<std::uint64_t> received{0};
    std::atomic<bool>          ordering_ok{true};
    std::atomic<std::uint64_t> bad_expected{0};
    std::atomic<std::uint64_t> bad_got{0};

    std::thread consumer([&] {
        std::uint64_t exp = 0;                                        // consumer-thread-local
        while (exp < N && ordering_ok.load(std::memory_order_relaxed)) {
            const std::uint64_t got = ring.consume_batch([&](const Order& o) {
                if (o.seq != exp && ordering_ok.load(std::memory_order_relaxed)) {
                    bad_expected.store(exp,   std::memory_order_relaxed);
                    bad_got.store(o.seq,      std::memory_order_relaxed);
                    ordering_ok.store(false,  std::memory_order_relaxed);
                }
                ++exp;
            });
            if (got == 0) {
                if (producer_done.load(std::memory_order_acquire) && ring.empty_approx()) break;
                cpu_relax();
            }
        }
        if (!ordering_ok.load(std::memory_order_relaxed))
            abort_run.store(true, std::memory_order_release);
        received.store(exp, std::memory_order_relaxed);
    });

    std::thread producer([&] {
        Order o{};
        o.side = Side::SELL; o.type = OrderType::LIMIT;
        o.price = 200; o.quantity = 1; o.remaining = 1;
        for (std::uint64_t i = 0; i < N; ++i) {
            o.id = i; o.seq = i;
            while (!ring.try_publish(o)) {
                if (abort_run.load(std::memory_order_acquire)) return;
                cpu_relax();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    CHECK(ordering_ok.load());
    CHECK(received.load() == N);

    if (!ordering_ok.load()) {
        std::printf("    BATCH ORDER VIOLATION: expected seq=%llu but got seq=%llu\n",
                    (unsigned long long)bad_expected.load(), (unsigned long long)bad_got.load());
    } else if (received.load() != N) {
        std::printf("    BATCH LOSS: received %llu of %llu orders\n",
                    (unsigned long long)received.load(), (unsigned long long)N);
    } else {
        std::printf("    OK: %llu orders via consume_batch, strict in-order, ring=%zu slots\n",
                    (unsigned long long)N, RING);
    }
}

// Producer publishes VARIABLE-SIZE batches (1..50) via publish_batch (single
// release-store per batch, wraparound copy, busy-spin on full); consumer drains via
// consume_batch. Strict order + zero loss across 5M items on a 1024-slot ring stresses
// the batch-publish barrier + wraparound under TSan. The consumer keeps draining even
// after a mismatch (records the first) so a bug fails cleanly instead of deadlocking the
// publish_batch producer, which busy-spins internally with no abort hook.
TEST_CASE(spsc_batch_publish_and_drain_preserves_order) {
    constexpr std::uint64_t N    = 5'000'000;
    constexpr std::size_t   RING = 1u << 10;

    IngressRing<RING> ring;

    std::atomic<bool>          producer_done{false};
    std::atomic<std::uint64_t> received{0};
    std::atomic<bool>          ordering_ok{true};
    std::atomic<std::uint64_t> bad_expected{0};
    std::atomic<std::uint64_t> bad_got{0};

    std::thread consumer([&] {
        std::uint64_t exp = 0;
        while (exp < N) {
            const std::uint64_t got = ring.consume_batch([&](const Order& o) {
                if (o.seq != exp && ordering_ok.load(std::memory_order_relaxed)) {
                    bad_expected.store(exp,  std::memory_order_relaxed);
                    bad_got.store(o.seq,     std::memory_order_relaxed);
                    ordering_ok.store(false, std::memory_order_relaxed);
                }
                ++exp;
            });
            if (got == 0) {
                if (producer_done.load(std::memory_order_acquire) && ring.empty_approx()) break;
                cpu_relax();
            }
        }
        received.store(exp, std::memory_order_relaxed);
    });

    std::thread producer([&] {
        std::mt19937_64 rng(0x1234ABCDULL);
        std::vector<Order> batch;
        batch.reserve(64);
        Order tmpl{};
        tmpl.side = Side::BUY; tmpl.type = OrderType::LIMIT;
        tmpl.price = 300; tmpl.quantity = 1; tmpl.remaining = 1;
        std::uint64_t i = 0;
        while (i < N) {
            std::size_t bsz = 1 + static_cast<std::size_t>(rng() % 50);   // 1..50 items
            if (i + bsz > N) bsz = static_cast<std::size_t>(N - i);
            batch.clear();
            for (std::size_t k = 0; k < bsz; ++k) {
                Order o = tmpl; o.id = i; o.seq = i;                       // seq = global monotonic
                batch.push_back(o);
                ++i;
            }
            ring.publish_batch(batch);   // one release-store; busy-spins if full (zero-drop)
        }
        producer_done.store(true, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    CHECK(ordering_ok.load());
    CHECK(received.load() == N);

    if (!ordering_ok.load()) {
        std::printf("    BATCH-PUB ORDER VIOLATION: expected seq=%llu but got seq=%llu\n",
                    (unsigned long long)bad_expected.load(), (unsigned long long)bad_got.load());
    } else if (received.load() != N) {
        std::printf("    BATCH-PUB LOSS: received %llu of %llu\n",
                    (unsigned long long)received.load(), (unsigned long long)N);
    } else {
        std::printf("    OK: %llu items via publish_batch(1..50) -> consume_batch, strict order, ring=%zu\n",
                    (unsigned long long)N, RING);
    }
}

int main() { return ut::run_all(); }
