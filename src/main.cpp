//
// src/main.cpp  --  titan-server: the production entry point.
//
// Wires the verified 3-thread "Flash One" topology into a running process:
//
//   T1 (main)      TcpGateway  --Sequencer.publish()-->  IngressRing
//                    (epoll TCP ingress; stamps seq -> write-ahead journal -> ring)
//   T2 (matcher)   IngressRing --consume_batch--> Matcher --publish_batch--> EgressRing
//   T3 (publisher) EgressRing  --consume_batch--> counter/checksum (+ periodic stdout)
//
// All three stages busy-spin (no yield) for lowest latency -- the same discipline the
// pipeline benchmark validated. Intended for dedicated/pinned cores; it will hold ~100%
// on the matcher/publisher cores even while idle (an HFT trade-off, not a bug).
//
// Graceful shutdown: SIGINT/SIGTERM wake the gateway (eventfd), which returns from run();
// main then drains the pipeline (matcher finishes the ring, publisher finishes egress)
// and joins cleanly. The WAL is msync'd on the Journaler's destructor.
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
constexpr std::size_t   RING_IN      = 1u << 20;          // 1,048,576 ingress slots (~40 MB)
constexpr std::size_t   RING_OUT     = 1u << 20;          // 1,048,576 egress slots  (~32 MB)
constexpr std::uint32_t MAX_NODES    = 1u << 16;          // PIN node pool
constexpr std::uint64_t ID_CAP       = 1u << 22;          // ~4.2M distinct order ids (dense slab)
constexpr std::size_t   ARENA_BYTES  = 64ull * 1024 * 1024;
constexpr std::uint64_t WAL_CAP      = 1u << 22;          // ~4.2M orders (~160 MB). HARD bound:
                                                          // exceeding it aborts an append (Sequencer
                                                          // is noexcept) -> size for the session.
constexpr std::uint16_t DEFAULT_PORT = 9001;

using Ingress = IngressRing<RING_IN>;
using Egress  = EgressRing<RING_OUT>;
using Gateway = TcpGateway<Sequencer<Ingress>>;

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
        Ingress ingress;                                  // shared: gateway(main) -> matcher
        Egress  egress;                                   // shared: matcher -> publisher
        Journaler wal("titan.wal", WAL_CAP);
        Sequencer<Ingress> seq(ingress, wal);

        // Construct the gateway BEFORE spawning workers so a bind/listen failure exits
        // cleanly (no orphaned spin threads to join).
        Gateway gw(port, seq);
        g_gateway = &gw;
        install_signal_handlers();

        std::atomic<bool> gateway_stopped{false};         // set once gw.run() returns (no more ingress)
        std::atomic<bool> matcher_done{false};            // set once matcher drains ingress

        // ---- T2: Matcher (arena/book are thread-local, as in the pipeline bench) ----
        std::thread matcher_thread([&] {
            Arena arena(ARENA_BYTES);
            OrderBook book(arena, MAX_NODES, ID_CAP);
            Matcher matcher(book);
            EgressSink sink{egress, {}};
            sink.buf.reserve(EgressSink::HWM);
            while (!gateway_stopped.load(std::memory_order_acquire) || !ingress.empty_approx()) {
                ingress.consume_batch([&](const Order& in) { matcher.submit(in, sink); });
                sink.flush();                             // push this batch's trades to egress
            }
            matcher_done.store(true, std::memory_order_release);
        });

        // ---- T3: Publisher (counter/checksum; periodic stdout, no per-trade I/O) ----
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
                    "(ingress=%zu egress=%zu slots, WAL cap=%llu orders)\n",
                    gw.port(), RING_IN, RING_OUT, (unsigned long long)WAL_CAP);
        std::printf("[titan-server] send raw 40-byte Order structs; Ctrl-C (SIGINT) to stop.\n");
        std::fflush(stdout);

        gw.run();

        std::printf("[titan-server] signal received; draining pipeline...\n");
        gateway_stopped.store(true, std::memory_order_release);
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
