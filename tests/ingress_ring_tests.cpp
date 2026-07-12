//
// tests/ingress_ring_tests.cpp
// Single-Producer / Single-Consumer stress test for the ingress Disruptor ring.
// Built under ThreadSanitizer (see tsan.sh) -- if a single release/acquire barrier
// is missing, TSan flags the plain slot read/write as a data race.
//
// The producer stamps order.seq = i (i = 0..N-1) and pushes N orders; the consumer
// pops N orders and asserts each order.seq == the expected running counter, strictly
// in order. A deliberately SMALL ring forces heavy wraparound + full/empty cycling
// so the barriers are exercised millions of times.
//
#include "ut.hpp"

#include "titan/book/order.hpp"
#include "titan/pipeline/ingress_ring.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>

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

int main() { return ut::run_all(); }
