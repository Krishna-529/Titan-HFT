#pragma once
//
// titan/book/snapshot.hpp
// L2 order-book snapshot primitive + lock-free triple-buffer pool. This is the state-sharing
// mechanism for the gap-fill / late-join channel: the Matcher (single writer) periodically
// serializes a CONSISTENT L2 image of the book into a free buffer and publishes it; a dedicated
// Snapshot thread (single reader) claims the latest published buffer and multicasts it -- all
// WITHOUT the Matcher ever blocking on the hot path.
//
// Memory model (the crux):
//   * Body visibility rides the `published_` pointer: the writer fills a buffer, then
//     published_.store(idx) (>= release); the reader published_.load() (>= acquire) then reads
//     the body. That release/acquire edge makes every body write visible before any read.
//   * Reclamation safety uses PER-BUFFER atomic `in_use` flags, but the claim-vs-recycle
//     interaction is a StoreLoad pattern (reader: store in_use, load published; writer: store
//     published, load in_use). Plain release/acquire permits a Dekker double-miss there ->
//     a torn read. So the handshake atomics are SEQ_CST, which forbids the double-miss. This
//     path is cold (fired every K events, K ~ 10k), so seq_cst costs nothing measurable.
//
// Triple buffer (>=3 slots) + a single reader is sufficient and WAIT-FREE for the writer:
// at any instant the writer excludes the published slot and any in_use slot; a single reader
// holds at most one -> at least one slot is always free. If (defensively / with future extra
// readers) none is free, the writer SKIPS that snapshot cycle -- best-effort, never blocks.
//
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "titan/domain/types.hpp"

namespace titan {

inline constexpr std::uint64_t SNAPSHOT_MAGIC   = 0x544954414E534E50ULL;  // "TITANSNP"
inline constexpr std::uint32_t SNAPSHOT_VERSION = 1;

// One L2 aggregate price level: price + summed resting quantity + resting order count.
struct SnapshotLevel {
    PriceTick     price;        // 8  level price (ticks)
    std::uint64_t total_qty;    // 8  aggregate resting quantity at this price
    std::uint32_t order_count;  // 4  number of resting orders at this price
    std::uint8_t  side;         // 1  0 = BID (buy), 1 = ASK (sell)
    std::uint8_t  _pad[3];      // 3  -> deterministic 24-byte layout
};
static_assert(sizeof(SnapshotLevel) == 24,               "SnapshotLevel layout drift");
static_assert(std::is_trivially_copyable_v<SnapshotLevel>);
static_assert(std::is_standard_layout_v<SnapshotLevel>);

// Fixed 64-byte snapshot header. `feed_seq` is the Sequencer seq this image is consistent
// as-of (clients stitch: apply snapshot @ feed_seq, then replay incrementals with seq > it).
// `checksum` covers the levels -> integrity + torn-read detection.
struct SnapshotHeader {
    std::uint64_t magic;         // 8  SNAPSHOT_MAGIC
    std::uint32_t version;       // 4  SNAPSHOT_VERSION
    std::uint32_t level_count;   // 4  valid SnapshotLevel entries that follow
    std::uint64_t feed_seq;      // 8  consistent as-of this Sequencer sequence number
    std::uint32_t bid_levels;    // 4  count of BID levels within [0, level_count)
    std::uint32_t ask_levels;    // 4  count of ASK levels
    std::uint64_t checksum;      // 8  fold over the levels
    std::uint8_t  _reserved[24]; // 24 -> 64 total
};
static_assert(sizeof(SnapshotHeader) == 64,              "SnapshotHeader must be 64 bytes");
static_assert(std::is_trivially_copyable_v<SnapshotHeader>);
static_assert(std::is_standard_layout_v<SnapshotHeader>);

// One snapshot buffer: header + a fixed-capacity flat array of L2 levels. POD -> the reader
// can memcpy/transmit it straight onto the wire (MTU-safe chunking happens in the publisher).
template <std::size_t MaxLevels>
struct alignas(64) SnapshotBuffer {
    SnapshotHeader header;
    SnapshotLevel  levels[MaxLevels];
};

// ---------------------------------------------------------------------------------------------
// Lock-free single-writer / single-reader triple-buffer pool.
template <std::size_t MaxLevels, std::size_t Slots = 3>
class SnapshotPool {
    static_assert(Slots >= 3, "triple buffering needs >= 3 slots (published + in_use + free)");
    static constexpr std::size_t CACHELINE = 64;

public:
    using Buffer = SnapshotBuffer<MaxLevels>;
    static constexpr std::size_t MAX_LEVELS = MaxLevels;
    static constexpr std::size_t SLOT_COUNT = Slots;

    SnapshotPool() = default;
    SnapshotPool(const SnapshotPool&)            = delete;
    SnapshotPool& operator=(const SnapshotPool&) = delete;

    // WRITER (Matcher). Serialize a snapshot via fill(Buffer&) into a free slot, then publish it.
    // fill() must populate the header (incl. checksum) and levels. Returns true if published,
    // false if every slot is busy -> the caller SAFELY SKIPS this cycle (never blocks the book).
    template <class Fill>
    bool try_snapshot(Fill&& fill) noexcept {
        const int idx = reserve_free_slot();
        if (idx < 0) return false;                                   // all busy -> skip cycle
        fill(buffers_[idx]);                                         // exclusive: no reader can see idx yet
        published_.store(idx, std::memory_order_seq_cst);            // PUBLISH (>= release: body visible)
        return true;
    }

    // READER (Snapshot thread). Claim the latest published buffer, marking it in_use so the
    // writer will not recycle it. Returns nullptr if nothing is published / claim kept losing to
    // a fast writer. MUST call release(idx) when done reading.
    const Buffer* acquire(int& idx_out) noexcept {
        for (int attempt = 0; attempt < CLAIM_RETRIES; ++attempt) {
            const int idx = published_.load(std::memory_order_seq_cst);
            if (idx < 0) return nullptr;                             // nothing published yet
            in_use_[idx].v.store(true, std::memory_order_seq_cst);   // announce our claim
            if (published_.load(std::memory_order_seq_cst) == idx) { // still the live buffer?
                idx_out = idx;
                return &buffers_[idx];                               // safe: writer won't recycle in_use idx
            }
            in_use_[idx].v.store(false, std::memory_order_seq_cst);  // writer swapped mid-claim -> retry
        }
        return nullptr;
    }

    void release(int idx) noexcept {
        if (idx >= 0 && static_cast<std::size_t>(idx) < Slots)
            in_use_[idx].v.store(false, std::memory_order_seq_cst);
    }

    int published_index() const noexcept { return published_.load(std::memory_order_acquire); }

private:
    // Pick a slot the writer may overwrite: neither the live published one nor any in_use one.
    int reserve_free_slot() noexcept {
        const int pub = published_.load(std::memory_order_seq_cst);
        for (int j = 0; j < static_cast<int>(Slots); ++j) {
            if (j == pub) continue;                                       // never clobber the live buffer
            if (in_use_[j].v.load(std::memory_order_seq_cst)) continue;   // a reader still holds it
            return j;
        }
        return -1;
    }

    static constexpr int CLAIM_RETRIES = 1024;

    // Each flag alone on its own cache line (no false sharing between slots or with the cursor).
    struct alignas(CACHELINE) Flag { std::atomic<bool> v{false}; };

    alignas(CACHELINE) std::atomic<int> published_{-1};   // index of the live buffer (-1 = none)
    Flag                                in_use_[Slots];
    alignas(CACHELINE) Buffer           buffers_[Slots];
};

} // namespace titan
