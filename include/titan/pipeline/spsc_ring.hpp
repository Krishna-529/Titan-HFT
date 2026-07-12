#pragma once
//
// titan/pipeline/spsc_ring.hpp
// Generic hand-rolled LMAX-Disruptor-style Single-Producer / Single-Consumer ring,
// parameterised on the payload POD `T`. The ingress ring (T = Order) and the egress
// ring (T = TradeEvent) are thin aliases over this one implementation, so the subtle
// lock-free code lives in exactly one place.
//
// Mechanical-sympathy rules:
//   * Power-of-two capacity -> index = seq & (Size - 1)  (bitwise, no modulo).
//   * Slots are a pre-allocated CONTIGUOUS array of `T` PODs, stored BY VALUE.
//   * Producer cursor and consumer sequence each live ALONE on a 64-byte cache line
//     (alignas(64) + padding) -> no false sharing between the two threads.
//   * Hand-off is release/acquire only (the single happens-before edge).
//   * Each side keeps a private cache of the other's sequence, so the common path
//     never reads the other core's cache line.
//   * Batch-drain amortises the consumer's release-store over a whole run and
//     prefetches the next slot's payload while the handler works on the current one.
//
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

namespace titan::pipeline {

inline constexpr std::size_t CACHELINE = 64;

template <class T, std::size_t Size>
class SpscRing {
    static_assert(Size >= 2 && (Size & (Size - 1)) == 0, "Size must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,       "payload T must be a POD (memcpy slots)");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "64-bit atomics must be lock-free on this target");

    static constexpr std::uint64_t MASK = Size - 1;

    // Producer's private cache line: published cursor + cached consumer sequence.
    struct alignas(CACHELINE) Producer {
        std::atomic<std::uint64_t> cursor{0};   // next sequence to write (== published count)
        std::uint64_t cached_consumer{0};       // producer-local snapshot of consumer seq
        char _pad[CACHELINE - sizeof(std::atomic<std::uint64_t>) - sizeof(std::uint64_t)];
    };
    // Consumer's private cache line: next-to-read + cached producer cursor.
    struct alignas(CACHELINE) Consumer {
        std::atomic<std::uint64_t> next{0};     // next sequence to read (== consumed count)
        std::uint64_t cached_cursor{0};         // consumer-local snapshot of producer cursor
        char _pad[CACHELINE - sizeof(std::atomic<std::uint64_t>) - sizeof(std::uint64_t)];
    };

    static_assert(sizeof(Producer) == CACHELINE && alignof(Producer) == CACHELINE);
    static_assert(sizeof(Consumer) == CACHELINE && alignof(Consumer) == CACHELINE);

public:
    SpscRing() : slots_(Size) {}                    // one contiguous allocation of Size Ts
    SpscRing(const SpscRing&)            = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    static constexpr std::size_t capacity() noexcept { return Size; }

    // ---------------------------- PRODUCER (single thread) ----------------------------
    // Publish one item. Returns false if the ring is full. Wait-free, noexcept.
    bool try_publish(const T& item) noexcept {
        const std::uint64_t seq = prod_.cursor.load(std::memory_order_relaxed);
        if (seq - prod_.cached_consumer >= Size) {                       // maybe full: recheck
            prod_.cached_consumer = cons_.next.load(std::memory_order_acquire);
            if (seq - prod_.cached_consumer >= Size) return false;       // truly full
        }
        slots_[seq & MASK] = item;                                       // write POD by value
        prod_.cursor.store(seq + 1, std::memory_order_release);          // publish
        return true;
    }

    // ---------------------------- CONSUMER (single thread) ----------------------------
    // Consume one item into `out`. Returns false if empty. Wait-free, noexcept.
    bool try_consume(T& out) noexcept {
        const std::uint64_t seq = cons_.next.load(std::memory_order_relaxed);
        if (seq >= cons_.cached_cursor) {                                // maybe empty: recheck
            cons_.cached_cursor = prod_.cursor.load(std::memory_order_acquire);
            if (seq >= cons_.cached_cursor) return false;                // truly empty
        }
        out = slots_[seq & MASK];                                        // read POD by value
        cons_.next.store(seq + 1, std::memory_order_release);            // free the slot
        return true;
    }

    // Batch-drain: consume EVERY currently-available item, invoking fn(const T&) for
    // each, then publish the consumer sequence ONCE. Collapses the `next` cache-line
    // ping-pong from once-per-item to once-per-batch, and prefetches the next slot's
    // payload into L1 while fn() runs on the current one. Returns the count drained.
    template <class Handler>
    std::uint64_t consume_batch(Handler&& fn) noexcept {
        const std::uint64_t start = cons_.next.load(std::memory_order_relaxed);
        const std::uint64_t avail = prod_.cursor.load(std::memory_order_acquire);  // one acquire / batch
        if (start == avail) return 0;                                    // empty
        cons_.cached_cursor = avail;

        std::uint64_t seq = start;
        do {
            __builtin_prefetch(&slots_[(seq + 1) & MASK], 0, 1);         // pull next payload into L1
            fn(slots_[seq & MASK]);                                      // process current (fn copies it)
            ++seq;
        } while (seq != avail);

        cons_.next.store(seq, std::memory_order_release);                // publish consumed ONCE
        return seq - start;
    }

    // Approximate in-flight count (metrics only; not a synchronization point).
    std::uint64_t size_approx() const noexcept {
        return prod_.cursor.load(std::memory_order_acquire)
             - cons_.next.load(std::memory_order_acquire);
    }
    bool empty_approx() const noexcept { return size_approx() == 0; }

private:
    std::vector<T> slots_;   // pre-allocated contiguous ring storage (Size Ts, by value)
    Producer       prod_;    // own cache line
    Consumer       cons_;    // own cache line
};

} // namespace titan::pipeline
