//
// src/main.cpp  --  titan-server: multi-gateway 4-(+N)-thread topology.
//
//   T1..Tk (gateways)  each TcpGateway --try_publish--> ONE shared MpscRing   (multi-producer)
//   T(sequencer)       MpscRing  --run(): stamp seq -> WAL -> ingress -->  IngressRing (SPSC)
//   T(matcher)         IngressRing --consume_batch--> Matcher --publish_batch--> EgressRing
//   T(publisher)       EgressRing  --consume_batch--> counter/checksum (+ periodic stdout)
//
// MULTI-GATEWAY FAN-IN: pass several ports (e.g. `titan-server 9001 9002`) and each gets its
// own listener + epoll thread, but ALL push onto the SAME MpscRing. That ring's per-cell CAS
// claim + published-sequence tracker is what makes the concurrent multi-producer push safe --
// this binary is the live proof of it. Usage: `titan-server [port ...]` (default 9001).
//
// All stages busy-spin (no yield) for lowest latency; intended for dedicated/pinned cores.
//
// Graceful shutdown CASCADES in pipeline order, each stage draining its input before exiting:
//   SIGINT/SIGTERM -> stop() EVERY gateway (eventfd) -> all gw.run() return -> join all gateways
//     -> gateway_stopped -> Sequencer drains MpscRing -> sequencer_done
//       -> Matcher drains IngressRing -> matcher_done
//         -> Publisher drains EgressRing -> exit.
// No order is dropped and no ring is read after its producers are gone.
//
// tcp_gateway.hpp is included FIRST so its _GNU_SOURCE define lands before any libc header.
//
#include "titan/net/tcp_gateway.hpp"
#include "titan/net/udp_publisher.hpp"

#include "titan/book/matcher.hpp"
#include "titan/book/order_book.hpp"
#include "titan/book/trade_event.hpp"
#include "titan/io/journaler.hpp"
#include "titan/memory/arena.hpp"
#include "titan/pipeline/egress_ring.hpp"
#include "titan/pipeline/ingress_ring.hpp"
#include "titan/pipeline/mpsc_ring.hpp"
#include "titan/pipeline/sequencer.hpp"

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <thread>
#include <vector>

using namespace titan;
using namespace titan::io;
using namespace titan::net;
using namespace titan::pipeline;

namespace {

// ---- capacities (single-symbol venue, demo-sized; tune per deployment) ----
constexpr std::size_t   RING_MPSC    = 1u << 20;         // inbound MPSC ring (ALL gateways -> sequencer)
constexpr std::size_t   RING_IN      = 1u << 20;         // ingress SPSC (sequencer -> matcher)
constexpr std::size_t   RING_OUT     = 1u << 20;         // egress  SPSC (matcher  -> publisher)
constexpr std::uint32_t MAX_NODES    = 1u << 16;         // PIN node pool
constexpr std::uint64_t ID_CAP       = 1u << 22;         // ~4.2M distinct order ids (dense slab)
constexpr std::size_t   ARENA_BYTES  = 64ull * 1024 * 1024;
constexpr std::uint64_t WAL_CAP      = 1u << 22;         // ~4.2M orders (~160 MB). HARD bound:
                                                         // exceeding it aborts an append (Sequencer
                                                         // is noexcept) -> size for the session.
constexpr std::uint16_t DEFAULT_PORT = 9001;
constexpr int           MAX_GATEWAYS = 16;

// Outbound market-data multicast (Publisher). MCAST_IF = "127.0.0.1" routes the feed over
// loopback so a same-host listener receives it; set to a NIC IP for real network fan-out.
const char*             MCAST_GROUP  = "239.1.1.1";
constexpr std::uint16_t MCAST_PORT   = 30001;
const char*             MCAST_IF     = "127.0.0.1";
constexpr int           MCAST_TTL    = 1;
constexpr std::size_t   PUB_HWM      = 1u << 12;         // publisher drains egress in 4096-trade waves

using Mpsc    = MpscRing<Order, RING_MPSC>;
using Ingress = IngressRing<RING_IN>;
using Egress  = EgressRing<RING_OUT>;
using Gateway = TcpGateway<Mpsc>;

// Signal -> stop EVERY gateway. Async-signal-safe: only atomic loads + Gateway::stop()
// (one write() to an eventfd) over a fixed array (no std::vector internals in the handler).
std::atomic<bool> g_shutdown{false};
Gateway*          g_gateways[MAX_GATEWAYS] = {};
std::atomic<int>  g_gateway_count{0};

extern "C" void on_signal(int) {
    g_shutdown.store(true, std::memory_order_release);
    const int n = g_gateway_count.load(std::memory_order_acquire);
    for (int i = 0; i < n; ++i)
        if (g_gateways[i] != nullptr) g_gateways[i]->stop();
}

void install_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = &on_signal;
    ::sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;                       // no SA_RESTART: epoll_wait may return EINTR (run() handles it)
    ::sigaction(SIGINT,  &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
}

inline std::uint64_t hash_trade(const TradeEvent& t) noexcept {
    return t.taker_id + t.maker_id + static_cast<std::uint64_t>(t.price) + t.quantity + t.status;
}

// Matcher-thread egress sink: buffers trades and batch-publishes them to the egress ring
// (one release-store per flush). Auto-flushes at a high-water mark so the buffer stays
// bounded and never reallocates on the hot path; flush() drains the tail after each batch.
struct EgressSink {
    static constexpr std::size_t HWM = 1u << 14;          // flush every 16,384 trades
    Egress&                 egress;
    std::vector<TradeEvent> buf;

    bool try_publish(const TradeEvent& t) noexcept {
        buf.push_back(t);
        if (buf.size() >= HWM) { egress.publish_batch(buf); buf.clear(); }
        return true;
    }
    void flush() noexcept {
        if (!buf.empty()) { egress.publish_batch(buf); buf.clear(); }
    }
};

}  // namespace

int main(int argc, char** argv) {
    // Ports: every arg is a listen port; default to one port if none given.
    std::vector<std::uint16_t> ports;
    for (int i = 1; i < argc && static_cast<int>(ports.size()) < MAX_GATEWAYS; ++i) {
        const int p = std::atoi(argv[i]);
        if (p > 0 && p <= 65535) ports.push_back(static_cast<std::uint16_t>(p));
        else std::fprintf(stderr, "[titan-server] ignoring invalid port '%s'\n", argv[i]);
    }
    if (ports.empty()) ports.push_back(DEFAULT_PORT);

    try {
        Mpsc    mpsc;                                     // inbound: ALL gateways -> sequencer
        Ingress ingress;                                  // sequencer -> matcher
        Egress  egress;                                   // matcher   -> publisher
        Journaler wal("titan.wal", WAL_CAP);
        Sequencer<Ingress> seq(ingress, wal);

        // Construct every gateway BEFORE spawning threads so a bind/listen failure exits
        // cleanly (no orphaned spin threads to join). All share the one MpscRing.
        std::vector<std::unique_ptr<Gateway>> gateways;
        gateways.reserve(ports.size());
        for (const std::uint16_t p : ports) {
            gateways.push_back(std::make_unique<Gateway>(p, mpsc));
            g_gateways[gateways.size() - 1] = gateways.back().get();
        }
        g_gateway_count.store(static_cast<int>(gateways.size()), std::memory_order_release);
        install_signal_handlers();

        std::atomic<bool> gateway_stopped{false};         // set once ALL gateways stop (no more MPSC producers)
        std::atomic<bool> sequencer_done{false};          // set once Sequencer drains the MPSC ring
        std::atomic<bool> matcher_done{false};            // set once Matcher drains the ingress ring

        // ---- Sequencer: drains MPSC, stamps seq, journals, forwards to ingress ----
        std::thread sequencer_thread([&] {
            seq.run(mpsc, gateway_stopped);               // exits when gateway_stopped && MPSC drained
            sequencer_done.store(true, std::memory_order_release);
        });

        // ---- Matcher (arena/book are thread-local, as in the pipeline bench) ----
        std::thread matcher_thread([&] {
            Arena arena(ARENA_BYTES);
            OrderBook book(arena, MAX_NODES, ID_CAP);
            Matcher matcher(book);
            EgressSink sink{egress, {}};
            sink.buf.reserve(EgressSink::HWM);
            while (!sequencer_done.load(std::memory_order_acquire) || !ingress.empty_approx()) {
                ingress.consume_batch([&](const Order& in) { matcher.submit(in, sink); });
                sink.flush();
            }
            matcher_done.store(true, std::memory_order_release);
        });

        // ---- Publisher: drain egress and BLAST every TradeEvent out over UDP multicast ----
        std::thread publisher_thread([&] {
            UdpPublisher udp(MCAST_GROUP, MCAST_PORT, MCAST_IF, MCAST_TTL);
            std::vector<TradeEvent> batch;
            batch.reserve(PUB_HWM);
            std::uint64_t total = 0, chk = 0;
            auto flush = [&] { if (!batch.empty()) { udp.publish(batch); total += batch.size(); batch.clear(); } };
            while (!matcher_done.load(std::memory_order_acquire) || !egress.empty_approx()) {
                egress.consume_batch([&](const TradeEvent& t) {
                    chk += hash_trade(t);
                    batch.push_back(t);
                    if (batch.size() >= PUB_HWM) flush();       // bounded buffer -> no hot-path realloc
                    if ((total + batch.size()) % 100000 == 0)
                        std::printf("[publisher] multicast events=%llu  datagrams=%llu  dropped=%llu\n",
                                    (unsigned long long)(total + batch.size()),
                                    (unsigned long long)udp.datagrams(), (unsigned long long)udp.dropped());
                });
                flush();
            }
            flush();
            std::printf("[publisher] FINAL events=%llu  chk=%llu  | UDP sent=%llu dropped=%llu "
                        "datagrams=%llu (%s:%u via %s)\n",
                        (unsigned long long)total, (unsigned long long)chk,
                        (unsigned long long)udp.sent(), (unsigned long long)udp.dropped(),
                        (unsigned long long)udp.datagrams(), MCAST_GROUP, MCAST_PORT, MCAST_IF);
        });

        // ---- Gateways: one epoll thread each, all feeding the shared MpscRing ----
        std::vector<std::thread> gateway_threads;
        gateway_threads.reserve(gateways.size());
        for (auto& gw : gateways) {
            Gateway* g = gw.get();
            gateway_threads.emplace_back([g] { g->run(); });
        }

        std::printf("[titan-server] listening on %zu port(s):", ports.size());
        for (const std::uint16_t p : ports) std::printf(" 127.0.0.1:%u", p);
        std::printf("\n[titan-server] %zu gateway thread(s) -> 1 MpscRing (mpsc=%zu ingress=%zu egress=%zu, "
                    "WAL cap=%llu). Ctrl-C to stop.\n",
                    gateways.size(), RING_MPSC, RING_IN, RING_OUT, (unsigned long long)WAL_CAP);
        std::fflush(stdout);

        // Block until SIGINT/SIGTERM stops every gateway; each gw.run() then returns.
        for (auto& t : gateway_threads) t.join();

        std::printf("[titan-server] all gateways stopped; cascading drain...\n");
        gateway_stopped.store(true, std::memory_order_release);   // -> Sequencer -> Matcher -> Publisher
        sequencer_thread.join();
        matcher_thread.join();
        publisher_thread.join();

        std::printf("[titan-server] shutdown complete. journaled %llu orders to titan.wal\n",
                    (unsigned long long)wal.count());
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[titan-server] fatal: %s\n", e.what());
        return 1;
    }
}
