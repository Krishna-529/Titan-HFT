#pragma once
//
// titan/pipeline/mpsc_ring.hpp
// Multi-Producer / Single-Consumer bounded lock-free ring -- the ingress primitive for
// MULTIPLE gateways feeding one matcher. Same mechanical-sympathy rules as SpscRing:
// power-of-two capacity (bitwise mask, no modulo), one contiguous slot array, and the
// producer / consumer cursors each alone on a 64-byte cache line (no false sharing).
//
// The core is Dmitry Vyukov's bounded MPMC algorithm, specialised to a single consumer:
//
//   * PER-CELL SEQUENCE ("published tracker"). Each slot carries its own
//     `std::atomic<uint64> seq`, initialised to its index. It encodes, in one word, both
//     "whose turn is it to WRITE this slot" and "is the data READY to READ":
//       - a producer may claim cell[pos] only when seq == pos      (slot free for this lap)
//       - after writing the payload it stores seq = pos + 1 (release)  -> DATA READY
//       - the consumer may read cell[pos] only when seq == pos + 1 (acquire)
//       - after reading it stores seq = pos + Size (release)          -> slot free next lap
//
//   * MULTI-PRODUCER CLAIM via CAS. Producers race on one shared `enqueue_pos_`; each
//     tries `compare_exchange_weak(pos, pos+1)` to win slot `pos`, retrying on loss. Only
//     AFTER winning does a producer touch the payload, and only THEN does it publish
//     seq = pos+1. So if producer B (higher pos) finishes before producer A (lower pos),
//     the consumer -- which advances strictly in pos order and gates on each cell's own
//     seq -- simply waits at A's slot and never reads B's data early or A's half-written
//     data. The per-cell release/acquire is the single happens-before edge per slot.
//
//   * SINGLE CONSUMER. Dequeue needs no CAS: only one thread advances `dequeue_pos_`.
//
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

namespace titan::pipeline {

inline constexpr std::size_t MPSC_CACHELINE = 64;

template <class T, std::size_t Size>
class MpscRing {
    static_assert(Size >= 2 && (Size & (Size - 1)) == 0, "Size must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,        "payload T must be a POD (memcpy slots)");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "64-bit atomics must be lock-free on this target");

    static constexpr std::uint64_t MASK = Size - 1;

    // One ring cell: its published-sequence word + the payload it guards.
    struct Cell {
        std::atomic<std::uint64_t> seq;
        T                          data;
    };

    // A cursor alone on its own cache line (producers hammer enqueue_; consumer owns dequeue_).
    struct alignas(MPSC_CACHELINE) Cursor {
        std::atomic<std::uint64_t> value{0};
        char _pad[MPSC_CACHELINE - sizeof(std::atomic<std::uint64_t>)];
    };

public:
    MpscRing() : cells_(std::make_unique<Cell[]>(Size)) {
        for (std::uint64_t i = 0; i < Size; ++i)
            cells_[i].seq.store(i, std::memory_order_relaxed);   // "slot i free for lap 0 at pos i"
    }
    MpscRing(const MpscRing&)            = delete;
    MpscRing& operator=(const MpscRing&) = delete;

    static constexpr std::size_t capacity() noexcept { return Size; }

    // ----------------------------- PRODUCER (any thread) -----------------------------
    // Claim the next free slot via CAS, write the payload, then publish the cell's seq so
    // the consumer can read it. Returns false if the ring is full. Wait-free-ish, noexcept.
    bool try_publish(const T& item) noexcept {
        std::uint64_t pos = enqueue_.value.load(std::memory_order_relaxed);
        Cell* cell;
        for (;;) {
            cell = &cells_[pos & MASK];
            const std::uint64_t seq = cell->seq.load(std::memory_order_acquire);
            const std::int64_t  dif = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(pos);
            if (dif == 0) {                                      // slot free for this lap: try to win it
                if (enqueue_.value.compare_exchange_weak(pos, pos + 1,
                        std::memory_order_relaxed, std::memory_order_relaxed))
                    break;                                       // won slot `pos` (pos unchanged)
                // lost the race: `pos` was refreshed by the CAS -> retry with the new head
            } else if (dif < 0) {
                return false;                                    // slot still holds an unconsumed item -> full
            } else {
                pos = enqueue_.value.load(std::memory_order_relaxed);  // another producer sped ahead
            }
        }
        cell->data = item;                                       // exclusive: we own this slot until publish
        cell->seq.store(pos + 1, std::memory_order_release);     // PUBLISH -> consumer may now read pos
        return true;
    }

    // ----------------------------- CONSUMER (single thread) --------------------------
    // Read the next in-order slot, but only once its own seq shows it is fully published
    // (acquire). A not-yet-written lower slot (a gap left by an in-flight producer) blocks
    // here rather than exposing later producers' data. Returns false if empty.
    bool try_consume(T& out) noexcept {
        const std::uint64_t pos = dequeue_.value.load(std::memory_order_relaxed);
        Cell* cell = &cells_[pos & MASK];
        const std::uint64_t seq = cell->seq.load(std::memory_order_acquire);
        const std::int64_t  dif = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(pos + 1);
        if (dif == 0) {                                          // published: safe to read
            out = cell->data;
            cell->seq.store(pos + Size, std::memory_order_release);   // free slot for the next lap
            dequeue_.value.store(pos + 1, std::memory_order_relaxed); // single consumer -> plain advance
            return true;
        }
        return false;                                            // dif < 0: empty / next slot not yet ready
    }

    // Drain the contiguous published prefix in one call, invoking fn(const T&) per item and
    // freeing each slot as it goes. Stops at the first gap (an unpublished lower slot) or
    // when empty. Single release-store of the dequeue cursor at the end. Returns count drained.
    template <class Handler>
    std::uint64_t consume_batch(Handler&& fn) noexcept {
        std::uint64_t pos = dequeue_.value.load(std::memory_order_relaxed);
        std::uint64_t n   = 0;
        for (;;) {
            Cell* cell = &cells_[pos & MASK];
            const std::uint64_t seq = cell->seq.load(std::memory_order_acquire);
            if (static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(pos + 1) != 0)
                break;                                           // next slot not published yet -> stop
            fn(static_cast<const T&>(cell->data));               // use payload before freeing the slot
            cell->seq.store(pos + Size, std::memory_order_release);
            ++pos;
            ++n;
        }
        if (n != 0) dequeue_.value.store(pos, std::memory_order_relaxed);   // publish consumed ONCE
        return n;
    }

    // Approximate in-flight count (metrics only; not a synchronization point).
    std::uint64_t size_approx() const noexcept {
        const std::uint64_t e = enqueue_.value.load(std::memory_order_acquire);
        const std::uint64_t d = dequeue_.value.load(std::memory_order_acquire);
        return e - d;
    }
    bool empty_approx() const noexcept { return size_approx() == 0; }

private:
    std::unique_ptr<Cell[]> cells_;     // contiguous slot array (per-cell seq + payload)
    Cursor                  enqueue_;   // shared by producers (CAS claim) -- own cache line
    Cursor                  dequeue_;   // single consumer only            -- own cache line
};

} // namespace titan::pipeline
