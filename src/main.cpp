//
// src/main.cpp  --  titan-server: the production entry point (final 4-thread topology).
//
//   T1 (main)      TcpGateway   --try_publish-->        MpscRing        (multi-producer inbound)
//   T2 (sequencer) MpscRing     --run(): stamp seq -> WAL -> ingress --> IngressRing (SPSC)
//   T3 (matcher)   IngressRing  --consume_batch--> Matcher --publish_batch--> EgressRing
//   T4 (publisher) EgressRing   --consume_batch--> counter/checksum (+ periodic stdout)
//
// The MpscRing decouples the network I/O thread(s) from the Sequencer, so multiple gateways
// can later feed one Sequencer without contending on the SPSC ingress ring. All stages
// busy-spin (no yield) for lowest latency -- the discipline the pipeline benchmark validated;
// intended for dedicated/pinned cores.
//
// Graceful shutdown CASCADES in pipeline order, each stage draining its input before exiting:
//   SIGINT/SIGTERM -> gateway.stop() (eventfd) -> gw.run() returns
//     -> gateway_stopped -> Sequencer drains MpscRing -> sequencer_done
//       -> Matcher drains IngressRing -> matcher_done
//         -> Publisher drains EgressRing -> exit.
// No order is dropped and no ring is read after its producer is gone.
//
// tcp_gateway.hpp is included FIRST so its _GNU_SOURCE define lands before any libc header.
//
#include "titan/net/tcp_gateway.hpp"

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
#include <thread>
#include <vector>

using namespace titan;
using namespace titan::io;
using namespace titan::net;
using namespace titan::pipeline;

namespace {

// ---- capacities (single-symbol venue, demo-sized; tune per deployment) ----
constexpr std::size_t   RING_MPSC    = 1u << 20;         // inbound MPSC ring (gateways -> sequencer)
constexpr std::size_t   RING_IN      = 1u << 20;         // ingress SPSC (sequencer -> matcher)
constexpr std::size_t   RING_OUT     = 1u << 20;         // egress  SPSC (matcher  -> publisher)
constexpr std::uint32_t MAX_NODES    = 1u << 16;         // PIN node pool
constexpr std::uint64_t ID_CAP       = 1u << 22;         // ~4.2M distinct order ids (dense slab)
constexpr std::size_t   ARENA_BYTES  = 64ull * 1024 * 1024;
constexpr std::uint64_t WAL_CAP      = 1u << 22;         // ~4.2M orders (~160 MB). HARD bound:
                                                         // exceeding it aborts an append (Sequencer
                                                         // is noexcept) -> size for the session.
constexpr std::uint16_t DEFAULT_PORT = 9001;

using Mpsc    = MpscRing<Order, RING_MPSC>;
using Ingress = IngressRing<RING_IN>;
using Egress  = EgressRing<RING_OUT>;
using Gateway = TcpGateway<Mpsc>;

// Signal -> gateway wake. The handler does only async-signal-safe work: an atomic store
// and Gateway::stop() (a single write() to an eventfd).
std::atomic<bool> g_shutdown{false};
Gateway*          g_gateway = nullptr;

extern "C" void on_signal(int) {
    g_shutdown.store(true, std::memory_order_release);
    if (g_gateway != nullptr) g_gateway->stop();
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
    const std::uint16_t port =
        (argc > 1) ? static_cast<std::uint16_t>(std::atoi(argv[1])) : DEFAULT_PORT;

    try {
        Mpsc    mpsc;                                     // inbound: gateway(s) -> sequencer
        Ingress ingress;                                  // sequencer -> matcher
        Egress  egress;                                   // matcher   -> publisher
        Journaler wal("titan.wal", WAL_CAP);
        Sequencer<Ingress> seq(ingress, wal);

        // Construct the gateway BEFORE spawning workers so a bind/listen failure exits
        // cleanly (no orphaned spin threads to join).
        Gateway gw(port, mpsc);
        g_gateway = &gw;
        install_signal_handlers();

        std::atomic<bool> gateway_stopped{false};         // set once gw.run() returns (no more MPSC producers)
        std::atomic<bool> sequencer_done{false};          // set once Sequencer drains the MPSC ring
        std::atomic<bool> matcher_done{false};            // set once Matcher drains the ingress ring

        // ---- T2: Sequencer -- drains MPSC, stamps seq, journals, forwards to ingress ----
        std::thread sequencer_thread([&] {
            seq.run(mpsc, gateway_stopped);               // exits when gateway_stopped && MPSC drained
            sequencer_done.store(true, std::memory_order_release);
        });

        // ---- T3: Matcher (arena/book are thread-local, as in the pipeline bench) ----
        std::thread matcher_thread([&] {
            Arena arena(ARENA_BYTES);
            OrderBook book(arena, MAX_NODES, ID_CAP);
            Matcher matcher(book);
            EgressSink sink{egress, {}};
            sink.buf.reserve(EgressSink::HWM);
            while (!sequencer_done.load(std::memory_order_acquire) || !ingress.empty_approx()) {
                ingress.consume_batch([&](const Order& in) { matcher.submit(in, sink); });
                sink.flush();                             // push this batch's trades to egress
            }
            matcher_done.store(true, std::memory_order_release);
        });

        // ---- T4: Publisher (counter/checksum; periodic stdout, no per-trade I/O) ----
        std::thread publisher_thread([&] {
            std::uint64_t fills = 0, rejects = 0, chk = 0;
            while (!matcher_done.load(std::memory_order_acquire) || !egress.empty_approx()) {
                egress.consume_batch([&](const TradeEvent& t) {
                    chk += hash_trade(t);
                    if (t.status == TRADE_STATUS_REJECTED) ++rejects; else ++fills;
                    const std::uint64_t total = fills + rejects;
                    if (total % 100000 == 0)
                        std::printf("[publisher] events=%llu  fills=%llu  rejects=%llu  chk=%llu\n",
                                    (unsigned long long)total, (unsigned long long)fills,
                                    (unsigned long long)rejects, (unsigned long long)chk);
                });
            }
            std::printf("[publisher] FINAL events=%llu  fills=%llu  rejects=%llu  chk=%llu\n",
                        (unsigned long long)(fills + rejects), (unsigned long long)fills,
                        (unsigned long long)rejects, (unsigned long long)chk);
        });

        // ---- T1: Gateway on the main thread; blocks until a signal calls stop() ----
        std::printf("[titan-server] listening on 127.0.0.1:%u  "
                    "(mpsc=%zu ingress=%zu egress=%zu slots, WAL cap=%llu orders)\n",
                    gw.port(), RING_MPSC, RING_IN, RING_OUT, (unsigned long long)WAL_CAP);
        std::printf("[titan-server] 4-thread pipeline up; send raw 40-byte Orders; Ctrl-C to stop.\n");
        std::fflush(stdout);

        gw.run();

        std::printf("[titan-server] signal received; cascading shutdown...\n");
        gateway_stopped.store(true, std::memory_order_release);   // -> Sequencer drains -> Matcher -> Publisher
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
