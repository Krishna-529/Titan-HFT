#pragma once
//
// titan/pipeline/sequencer.hpp
// The Sequencer (pipeline stage T2, between the gateways and the matcher). It is the single
// source of truth for arrival order: it stamps a monotonic `seq` on every order, WRITE-AHEAD
// journals it to the WAL, then publishes it to the ingress ring. Journal-before-publish is
// the WAL invariant -- an order is recorded in append order before it can affect the book, so
// the log is a faithful, replayable command history.
//
// Two ways to drive it:
//   * publish(Order)                 -- one order (used by recovery/tests/benches).
//   * run(inbound_mpsc, stop_flag)   -- the active server loop: drain a Multi-Producer inbound
//                                       MpscRing (fed by one or more TcpGateways), publish()
//                                       each, and exit cleanly once upstream stops + drains.
//
// journaler.hpp is included FIRST so its _DEFAULT_SOURCE define exposes the POSIX
// mmap/msync calls under -std=c++20 before <sys/mman.h> is pulled in transitively.
//
// Hot-path cost: append() is a pure memcpy + cursor bump (no syscall). Durability is
// deferred off the per-order path to batch boundaries: every `flush_interval` orders we
// nudge writeback with MS_ASYNC (~free), and every `sync_every`-th such batch we take an
// MS_SYNC durability checkpoint -- bounding the crash loss window to
// flush_interval * sync_every orders without a per-order syscall.
//
// Concrete class, no virtuals -> fully inlinable, so the measured pipeline numbers hold.
//
// RECORD SCOPE (conscious decision): the WAL logs Order records ONLY. Today nothing but
// new orders traverses the ingress ring (there are no cancels/modifies on that path), so
// an Order-only log is a complete command stream. IF cancels/modifies ever enter ingress,
// this WAL must become a tagged command log (an op-type discriminator + a union/variant
// payload) -- replaying orders alone would then silently diverge from live state.
//
#include "titan/io/journaler.hpp"        // must be first (defines _DEFAULT_SOURCE)

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "titan/book/order.hpp"
#include "titan/domain/types.hpp"

namespace titan::pipeline {

template <class IngressRingT>
class Sequencer {
public:
    // flush_interval: orders between MS_ASYNC writeback nudges.
    // sync_every    : number of those intervals between MS_SYNC durability checkpoints
    //                 (loss window <= flush_interval * sync_every orders). 0 disables it.
    Sequencer(IngressRingT& ingress, io::Journaler& journal,
              std::uint32_t flush_interval = 1024, std::uint32_t sync_every = 64) noexcept
        : ingress_(ingress), journal_(journal),
          flush_interval_(flush_interval ? flush_interval : 1),
          sync_every_(sync_every) {}

    Sequencer(const Sequencer&)            = delete;
    Sequencer& operator=(const Sequencer&) = delete;

    // Publish ONE order: stamp seq -> write-ahead journal -> hand to ingress (zero-drop
    // busy-spin backpressure). No syscall on this path; durability rides the cadence below.
    void publish(Order o) noexcept {
        o.seq = next_seq_++;
        journal_.append(o);                                   // WRITE-AHEAD: log before it can match
        while (!ingress_.try_publish(o)) { /* zero-drop: spin until a slot frees */ }
        if (++since_flush_ >= flush_interval_) {
            since_flush_ = 0;
            if (sync_every_ && (++flush_no_ % sync_every_ == 0)) journal_.flush_sync();  // checkpoint
            else                                                 journal_.flush_async(); // writeback nudge
        }
    }

    // Active drain loop (the Sequencer thread, T2). Consumes orders from the inbound MPSC
    // ring (fed by one or more gateways), stamping + journaling + publishing each to the
    // outbound ingress ring via publish() -- so the durability cadence is applied uniformly.
    // Exits once `upstream_stopped` is set AND the inbound ring is fully drained (the shutdown
    // cascade: Gateway stops -> MPSC drains -> Sequencer stops), taking a final MS_SYNC on exit.
    template <class InboundMpsc>
    void run(InboundMpsc& inbound, std::atomic<bool>& upstream_stopped) noexcept {
        while (!upstream_stopped.load(std::memory_order_acquire) || !inbound.empty_approx()) {
            inbound.consume_batch([&](const Order& o) { publish(o); });   // stamp/journal/push each
        }
        flush();   // final durability checkpoint before the WAL is relied upon for recovery
    }

    // Force a durability checkpoint (e.g. on graceful drain, before relying on the WAL).
    void flush() noexcept { journal_.flush_sync(); }

    Seq           next_seq()  const noexcept { return next_seq_; }   // == count published so far
    std::uint64_t journaled() const noexcept { return journal_.count(); }

private:
    IngressRingT&   ingress_;
    io::Journaler&  journal_;
    Seq             next_seq_     = 0;   // monotonic arrival sequence (starts at 0)
    std::uint32_t   flush_interval_;
    std::uint32_t   sync_every_;
    std::uint32_t   since_flush_  = 0;
    std::uint64_t   flush_no_     = 0;
};

// ---------------------------------------------------------------------------------------
// Recovery / replay. Reconstruct engine state from a WAL by re-submitting its records, in
// append order, through a fresh Matcher. Reads UP TO count() records but the true durable
// boundary is defined by the append-order invariant:
//
//     record i is valid  <=>  wal[i].seq == base + i     (base = wal[0].seq)
//
// The Sequencer appends in strict seq order, so any torn / zero-filled tail (count led the
// durably-written pages at crash time) breaks the invariant and stops replay exactly at the
// last good record. This is robust to ANY id scheme (never uses id==0 as a sentinel) and
// does not trust `count` as the authoritative torn-tail marker.
//
// No re-journaling and no external publish -- pure state reconstruction. Returns the number
// of records replayed. `sink` receives the reconstructed trades (a checksum/vector collector
// in tests; discarded in production cold-start).
template <class Matcher, class Sink>
std::uint64_t replay(const io::Journaler& wal, Matcher& matcher, Sink& sink) noexcept {
    const std::uint64_t n = wal.count();
    if (n == 0) return 0;                                     // empty WAL -> clean no-op
    const Seq base = wal[0].seq;
    std::uint64_t i = 0;
    for (; i < n; ++i) {
        if (wal[i].seq != base + i) break;                    // torn/zeroed tail -> durable boundary
        matcher.submit(wal[i], sink);                         // rebuild book (submit copies by value)
    }
    return i;                                                 // records actually replayed
}

} // namespace titan::pipeline
