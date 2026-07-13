//
// tests/sequencer_tests.cpp
// Sequencer wiring + WAL recovery/replay. sequencer.hpp is included FIRST so its
// journaler.hpp -> _DEFAULT_SOURCE define exposes the POSIX calls under -std=c++20.
//
// Coverage:
//   1) sequencer_journals_then_publishes  -- seq stamped, WAL + ingress agree, in order.
//   2) recovery_replay_rebuilds_book       -- replay() reproduces a direct submit exactly.
//   3) recovery_stops_at_torn_tail         -- zeroed tail (count leads durable) -> stop at boundary.
//   4) recovery_empty_wal_noop             -- count==0 -> clean no-op.
//   5) recovery_idempotent                 -- two independent replays -> identical state.
//
// Ids are 0-BASED on purpose: the torn-tail detector must rely on the append-order seq
// invariant (seq == base + i), NEVER on an id==0 sentinel.
//
#include "titan/pipeline/sequencer.hpp"     // must be first (pulls journaler.hpp -> _DEFAULT_SOURCE)

#include "ut.hpp"
#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/book/trade_event.hpp"
#include "titan/memory/arena.hpp"
#include "titan/pipeline/ingress_ring.hpp"

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <random>
#include <unistd.h>
#include <vector>

using namespace titan;
using namespace titan::io;
using namespace titan::pipeline;

namespace {

constexpr std::uint64_t SN          = 4096;              // orders per test
constexpr std::size_t   RING        = 1u << 13;          // 8192 slots (holds SN, never full)
constexpr std::uint32_t MAX_NODES   = 1u << 12;
constexpr std::uint64_t ID_CAP      = SN + 64;
constexpr std::size_t   ARENA_BYTES = 32ull * 1024 * 1024;   // bench parity (arena churn headroom)
constexpr PriceTick     MID         = 10'000;
constexpr PriceTick     BAND        = 8;                 // tight band -> heavy crossing -> real trades

const char* WAL_PUB   = "/tmp/titan_seq_pub.wal";
const char* WAL_BUILD = "/tmp/titan_seq_build.wal";
const char* WAL_TORN  = "/tmp/titan_seq_torn.wal";
const char* WAL_EMPTY = "/tmp/titan_seq_empty.wal";
const char* WAL_IDEM  = "/tmp/titan_seq_idem.wal";

inline std::uint64_t hash_trade(const TradeEvent& t) noexcept {
    return t.taker_id + t.maker_id + static_cast<std::uint64_t>(t.price) + t.quantity;
}
struct ChecksumSink {
    std::uint64_t chk = 0;
    bool try_publish(const TradeEvent& t) noexcept { chk += hash_trade(t); return true; }
};

// Deterministic crossing workload (0-based ids; seq left for the Sequencer to stamp).
std::vector<Order> gen_orders() {
    std::mt19937_64 rng(0xC0FFEEULL);
    std::vector<Order> v; v.reserve(SN);
    for (std::uint64_t i = 0; i < SN; ++i) {
        Order o{};
        o.id       = i;                                  // 0-based on purpose
        o.price    = MID - BAND + static_cast<PriceTick>(rng() % (2 * BAND + 1));
        o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 4);
        o.side     = (rng() & 1u) ? Side::BUY : Side::SELL;
        o.type     = OrderType::LIMIT;
        v.push_back(o);
    }
    return v;
}

// Replay a WAL into a fresh Matcher; return {records replayed, trade checksum}.
struct ReplayOut { std::uint64_t n; std::uint64_t chk; };
ReplayOut replay_fresh(const io::Journaler& wal) {
    Arena arena(ARENA_BYTES);
    OrderBook book(arena, MAX_NODES, ID_CAP);
    Matcher matcher(book);
    ChecksumSink sink;
    const std::uint64_t n = replay(wal, matcher, sink);
    return {n, sink.chk};
}

}  // namespace

// 1) Stamp seq, write-ahead journal, publish. WAL and ingress must agree, in order.
TEST_CASE(sequencer_journals_then_publishes) {
    const std::vector<Order> orders = gen_orders();
    IngressRing<RING> ingress;
    Journaler wal(WAL_PUB, SN + 64);
    Sequencer<IngressRing<RING>> seq(ingress, wal, /*flush_interval=*/256, /*sync_every=*/4);

    for (std::uint64_t i = 0; i < SN; ++i) seq.publish(orders[i]);
    seq.flush();

    REQUIRE(seq.next_seq() == SN);
    REQUIRE(wal.count()    == SN);

    std::vector<Order> drained;
    drained.reserve(SN);
    ingress.consume_batch([&](const Order& o) { drained.push_back(o); });
    REQUIRE(drained.size() == SN);

    bool ring_ok = true, wal_ok = true;
    for (std::uint64_t i = 0; i < SN; ++i) {
        if (drained[i].id != orders[i].id || drained[i].seq != i) { ring_ok = false; break; }
        if (wal[i].id     != orders[i].id || wal[i].seq     != i) { wal_ok  = false; break; }
    }
    CHECK(ring_ok);                                      // ingress: order + seq exact
    CHECK(wal_ok);                                       // WAL: order + seq exact
}

// 2) replay() reconstructs exactly what a direct submit of the journaled records produces.
TEST_CASE(recovery_replay_rebuilds_book) {
    const std::vector<Order> orders = gen_orders();
    {
        IngressRing<RING> ingress;
        Journaler wal(WAL_BUILD, SN + 64);
        Sequencer<IngressRing<RING>> seq(ingress, wal, 256, 4);
        for (std::uint64_t i = 0; i < SN; ++i) seq.publish(orders[i]);
        seq.flush();
        ingress.consume_batch([](const Order&) {});      // drain (result unused here)
    }  // WAL closed (dtor msync)

    Journaler wal(WAL_BUILD);                             // reopen -> validate()
    REQUIRE(wal.count() == SN);

    // Reference: submit the journaled records directly to a fresh matcher.
    std::uint64_t ref_chk = 0;
    {
        Arena arena(ARENA_BYTES);
        OrderBook book(arena, MAX_NODES, ID_CAP);
        Matcher matcher(book);
        ChecksumSink sink;
        for (std::uint64_t i = 0; i < wal.count(); ++i) matcher.submit(wal[i], sink);
        ref_chk = sink.chk;
    }

    const ReplayOut r = replay_fresh(wal);
    CHECK(r.n   == SN);                                   // read every record, no early stop / overrun
    CHECK(r.chk == ref_chk);                             // identical reconstruction
    CHECK(r.chk != 0u);                                  // workload actually crossed (trades happened)
}

// 3) Zero the tail but leave count == SN (count leads the durable pages): replay must stop
//    at the last structurally-valid record via the seq==base+i invariant.
TEST_CASE(recovery_stops_at_torn_tail) {
    const std::vector<Order> orders = gen_orders();
    {
        IngressRing<RING> ingress;
        Journaler wal(WAL_TORN, SN + 64);
        Sequencer<IngressRing<RING>> seq(ingress, wal, 256, 4);
        for (std::uint64_t i = 0; i < SN; ++i) seq.publish(orders[i]);
        seq.flush();
        ingress.consume_batch([](const Order&) {});
    }

    // Zero the last 3 records directly in the file; header count stays SN.
    const std::uint64_t torn_at = SN - 3;
    const off_t off = static_cast<off_t>(sizeof(io::FileHeader) + torn_at * sizeof(Order));
    unsigned char zeros[3 * sizeof(Order)] = {0};
    const int fd = ::open(WAL_TORN, O_RDWR);
    REQUIRE(fd >= 0);
    const ssize_t w = ::pwrite(fd, zeros, sizeof(zeros), off);
    ::close(fd);
    REQUIRE(w == static_cast<ssize_t>(sizeof(zeros)));

    Journaler wal(WAL_TORN);
    REQUIRE(wal.count() == SN);                           // count still (falsely) claims all SN
    const ReplayOut r = replay_fresh(wal);
    CHECK(r.n == torn_at);                                // stopped exactly at the durable boundary
}

// 4) Empty WAL (count==0): replay is a clean no-op, no trades.
TEST_CASE(recovery_empty_wal_noop) {
    Journaler wal(WAL_EMPTY, 64);                         // create, never append
    REQUIRE(wal.count() == 0u);
    const ReplayOut r = replay_fresh(wal);
    CHECK(r.n   == 0u);
    CHECK(r.chk == 0u);
}

// 5) Replay is idempotent: two independent replays of the same WAL -> identical state.
TEST_CASE(recovery_idempotent) {
    const std::vector<Order> orders = gen_orders();
    {
        IngressRing<RING> ingress;
        Journaler wal(WAL_IDEM, SN + 64);
        Sequencer<IngressRing<RING>> seq(ingress, wal, 256, 4);
        for (std::uint64_t i = 0; i < SN; ++i) seq.publish(orders[i]);
        seq.flush();
        ingress.consume_batch([](const Order&) {});
    }

    Journaler wal(WAL_IDEM);
    const ReplayOut a = replay_fresh(wal);
    const ReplayOut b = replay_fresh(wal);
    CHECK(a.n   == SN);
    CHECK(a.n   == b.n);                                  // same record count
    CHECK(a.chk == b.chk);                                // same reconstructed trades
}
