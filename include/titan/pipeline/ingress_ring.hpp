#pragma once
//
// titan/pipeline/ingress_ring.hpp
// Hand-rolled LMAX-Disruptor-style ingress ring buffer: Single-Producer,
// Single-Consumer (SPSC). Represents the Sequencer publishing Orders into the
// queue and the Matcher consuming them.
//
// Mechanical-sympathy rules (all enforced below):
//   * Power-of-two capacity  -> index = seq & (Size - 1)  (bitwise, no modulo).
//   * Slots are a pre-allocated CONTIGUOUS array of Order PODs, stored BY VALUE
//     (no pointers, no per-item allocation, no indirection on the hot path).
//   * The producer cursor and the consumer sequence each live ALONE on their own
//     64-byte cache line (alignas(64) + padding) -> zero false sharing between
//     the writing thread and the reading thread.
//   * Cross-thread hand-off uses release/acquire only: the producer publishes a
//     slot with memory_order_release; the consumer observes it with
//     memory_order_acquire. That single pair is the happens-before edge that makes
//     the slot write visible before it is read. No locks, no atomics on the payload.
//   * Each side keeps a private (non-atomic) CACHE of the other's sequence, so the
//     common case never reads the other core's cache line at all.
//
// Zero-crash: fixed capacity, bounds via masking, trivially-copyable payload.
// SPSC contract: exactly one producer thread calls try_publish(); exactly one
// consumer thread calls try_consume(). (MPSC comes later for multi-gateway.)
//
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "titan/book/order.hpp"

namespace titan::pipeline {

inline constexpr std::size_t CACHELINE = 64;

template <std::size_t Size = (std::size_t{1} << 20)>   // 1,048,576 slots by default
class IngressRing {
    static_assert(Size >= 2 && (Size & (Size - 1)) == 0, "Size must be a power of two");
    static_assert(std::is_trivially_copyable_v<Order>,   "Order must be a POD (memcpy slots)");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "64-bit atomics must be lock-free on this target");

    static constexpr std::uint64_t MASK = Size - 1;      // seq & MASK == seq % Size

    // Producer's private cache line: published cursor + cached consumer sequence.
    // Only the producer writes this line; the consumer reads `cursor` (acquire).
    struct alignas(CACHELINE) Producer {
        std::atomic<std::uint64_t> cursor{0};   // next sequence to write (== published count)
        std::uint64_t cached_consumer{0};       // producer-local snapshot of consumer seq
        char _pad[CACHELINE - sizeof(std::atomic<std::uint64_t>) - sizeof(std::uint64_t)];
    };

    // Consumer's private cache line: next-to-read + cached producer cursor.
    // Only the consumer writes this line; the producer reads `next` (acquire).
    struct alignas(CACHELINE) Consumer {
        std::atomic<std::uint64_t> next{0};     // next sequence to read (== consumed count)
        std::uint64_t cached_cursor{0};         // consumer-local snapshot of producer cursor
        char _pad[CACHELINE - sizeof(std::atomic<std::uint64_t>) - sizeof(std::uint64_t)];
    };

    static_assert(sizeof(Producer) == CACHELINE && alignof(Producer) == CACHELINE);
    static_assert(sizeof(Consumer) == CACHELINE && alignof(Consumer) == CACHELINE);

public:
    IngressRing() : slots_(Size) {}                 // one contiguous allocation of Size Orders
    IngressRing(const IngressRing&)            = delete;
    IngressRing& operator=(const IngressRing&) = delete;

    static constexpr std::size_t capacity() noexcept { return Size; }

    // ---------------------------- PRODUCER (Sequencer) ----------------------------
    // Publish one order into the ring. Returns false if the ring is full (the
    // consumer has not yet freed a slot). noexcept, allocation-free, wait-free.
    bool try_publish(const Order& order) noexcept {
        const std::uint64_t seq = prod_.cursor.load(std::memory_order_relaxed);  // own seq
        // Full when Size unconsumed items are already in flight. Check the cached
        // consumer sequence first; only touch the consumer's cache line if it looks full.
        if (seq - prod_.cached_consumer >= Size) {
            prod_.cached_consumer = cons_.next.load(std::memory_order_acquire);
            if (seq - prod_.cached_consumer >= Size) return false;               // truly full
        }
        slots_[seq & MASK] = order;                                              // write POD by value
        prod_.cursor.store(seq + 1, std::memory_order_release);                  // publish (visibility edge)
        return true;
    }

    // ---------------------------- CONSUMER (Matcher) ------------------------------
    // Consume one order into `out`. Returns false if the ring is empty. noexcept,
    // allocation-free, wait-free.
    bool try_consume(Order& out) noexcept {
        const std::uint64_t seq = cons_.next.load(std::memory_order_relaxed);    // own seq
        if (seq >= cons_.cached_cursor) {
            cons_.cached_cursor = prod_.cursor.load(std::memory_order_acquire);   // observe publications
            if (seq >= cons_.cached_cursor) return false;                        // truly empty
        }
        out = slots_[seq & MASK];                                                // read POD by value
        cons_.next.store(seq + 1, std::memory_order_release);                    // free the slot
        return true;
    }

    // Batch-drain: consume EVERY currently-available order, invoking fn(const Order&)
    // for each, then publish the consumer sequence ONCE. Amortising the release-store
    // over the whole batch collapses the `next` cache-line ping-pong from once-per-msg
    // to once-per-batch. The loop prefetches the next slot's payload into L1 while fn()
    // runs on the current one, hiding the cross-core coherence transfer behind useful
    // work. Returns the number consumed (0 if empty). SPSC: consumer thread only.
    template <class Handler>
    std::uint64_t consume_batch(Handler&& fn) noexcept {
        const std::uint64_t start = cons_.next.load(std::memory_order_relaxed);   // own seq
        const std::uint64_t avail = prod_.cursor.load(std::memory_order_acquire); // one acquire / batch
        if (start == avail) return 0;                                             // ring empty
        cons_.cached_cursor = avail;

        std::uint64_t seq = start;
        do {
            __builtin_prefetch(&slots_[(seq + 1) & MASK], 0, 1);   // pull next payload into L1
            fn(slots_[seq & MASK]);                                // process current (fn copies it)
            ++seq;
        } while (seq != avail);

        cons_.next.store(seq, std::memory_order_release);          // publish consumed ONCE per batch
        return seq - start;
    }

    // Approximate in-flight count (metrics only; not a synchronization point).
    std::uint64_t size_approx() const noexcept {
        return prod_.cursor.load(std::memory_order_acquire)
             - cons_.next.load(std::memory_order_acquire);
    }
    bool empty_approx() const noexcept { return size_approx() == 0; }

private:
    std::vector<Order> slots_;   // pre-allocated contiguous ring storage (Size Orders, by value)
    Producer           prod_;    // own cache line
    Consumer           cons_;    // own cache line
};

} // namespace titan::pipeline
