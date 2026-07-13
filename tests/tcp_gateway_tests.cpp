//
// tests/tcp_gateway_tests.cpp
// End-to-end TCP ingress: a real epoll gateway on a loopback port + a mock client that
// streams 10,000 raw binary Orders. Asserts the Sequencer received every one, intact and
// in sequence, and that the gateway leaks no file descriptors across its lifetime.
//
// tcp_gateway.hpp is included FIRST so its _GNU_SOURCE define lands before any libc header
// (features.h latches feature macros on first include -> accept4/SOCK_NONBLOCK must be armed).
//
#include "titan/net/tcp_gateway.hpp"

#include "ut.hpp"
#include "titan/book/order.hpp"
#include "titan/io/journaler.hpp"
#include "titan/pipeline/ingress_ring.hpp"
#include "titan/pipeline/sequencer.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

using namespace titan;
using namespace titan::net;
using namespace titan::io;
using namespace titan::pipeline;

namespace {

constexpr std::uint64_t GW_COUNT = 10'000;
constexpr std::size_t   GW_RING  = 1u << 14;      // 16384 > COUNT: never fills without a consumer
const char*             GW_WAL   = "/tmp/titan_gw.wal";

// Adapter satisfying TcpGateway's SequencerT (publish(Order)): forwards to the real
// Sequencer and bumps an atomic the test thread can poll race-free while the gw runs.
struct CountingSeq {
    Sequencer<IngressRing<GW_RING>>& inner;
    std::atomic<std::uint64_t>       n{0};
    void publish(Order o) noexcept { inner.publish(o); n.fetch_add(1, std::memory_order_relaxed); }
};

std::vector<Order> gw_make_orders() {
    std::vector<Order> v;
    v.reserve(GW_COUNT);
    for (std::uint64_t i = 0; i < GW_COUNT; ++i) {
        Order o{};
        o.id       = i;                                       // 0-based
        o.price    = 1000 + static_cast<PriceTick>(i % 100);
        o.quantity = o.remaining = 1u + static_cast<Qty>(i % 7);
        o.side     = (i & 1u) ? Side::SELL : Side::BUY;
        o.type     = OrderType::LIMIT;
        v.push_back(o);
    }
    return v;
}

// Count this process's open fds via /proc/self/fd. opendir's own fd is closed before we
// return, and we compare before/after with the same helper, so it cancels out.
int gw_open_fds() {
    int n = 0;
    DIR* d = ::opendir("/proc/self/fd");
    if (d == nullptr) return -1;
    while (::readdir(d) != nullptr) ++n;
    ::closedir(d);
    return n;
}

}  // namespace

TEST_CASE(tcp_gateway_receives_10k_orders_in_sequence) {
    IngressRing<GW_RING> ingress;
    Journaler wal(GW_WAL, GW_COUNT + 64);
    Sequencer<IngressRing<GW_RING>> seq(ingress, wal, /*flush_interval=*/4096, /*sync_every=*/8);
    CountingSeq cseq{seq};

    const int fds_before = gw_open_fds();
    REQUIRE(fds_before > 0);

    {  // gateway scoped so its dtor (fd cleanup) runs before the leak check below
        TcpGateway<CountingSeq> gw(0, cseq);              // port 0 -> OS-assigned ephemeral
        const std::uint16_t port = gw.port();
        REQUIRE(port != 0);

        std::thread gw_thread([&] { gw.run(); });

        // --- mock client: connect, stream 10k raw Orders, close ---
        const int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(cfd >= 0);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        REQUIRE(::connect(cfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);

        const std::vector<Order> orders = gw_make_orders();
        const char*       base  = reinterpret_cast<const char*>(orders.data());
        const std::size_t total = GW_COUNT * sizeof(Order);
        std::size_t sent = 0;
        while (sent < total) {                            // send() may short-write -> loop
            const ssize_t w = ::send(cfd, base + sent, total - sent, 0);
            if (w < 0) { if (errno == EINTR) continue; break; }
            sent += static_cast<std::size_t>(w);
        }
        REQUIRE(sent == total);
        ::close(cfd);

        // Wait (bounded) for the gateway to hand every order to the sequencer.
        for (int i = 0; i < 1000 && cseq.n.load(std::memory_order_relaxed) < GW_COUNT; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));

        gw.stop();
        gw_thread.join();
    }

    const int fds_after = gw_open_fds();

    CHECK(cseq.n.load() == GW_COUNT);                     // gateway delivered all 10k
    REQUIRE(wal.count() == GW_COUNT);                     // sequencer journaled all 10k
    CHECK(fds_after == fds_before);                       // no listen/epoll/eventfd/conn fd leaked

    // Drain the ingress ring (consumer side; gw thread already joined -> no data race) and
    // verify every order arrived intact and in strict sequence.
    const std::vector<Order> expect = gw_make_orders();
    std::vector<Order> got;
    got.reserve(GW_COUNT);
    ingress.consume_batch([&](const Order& o) { got.push_back(o); });
    REQUIRE(got.size() == GW_COUNT);

    bool ok = true;
    for (std::uint64_t i = 0; i < GW_COUNT; ++i) {
        if (got[i].id != i || got[i].seq != i ||
            got[i].price    != expect[i].price ||
            got[i].quantity != expect[i].quantity ||
            got[i].side     != expect[i].side) { ok = false; break; }
    }
    CHECK(ok);                                            // intact + in-sequence
}
