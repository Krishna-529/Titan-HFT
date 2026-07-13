//
// tests/tcp_blaster.cpp
// High-performance TCP order blaster for the end-to-end benchmark. Pre-allocates the whole
// order stream, then blasts it at the gateway as fast as the socket will accept (TCP flow
// control paces us -> send() completing == the pipeline ingested every order).
//
// Workload (deterministic, exactly N fills): one large resting SELL, then N BUY qty-1 orders
// that each partial-fill it -> exactly N TradeEvents, single maker. So "the N-th matching
// TradeEvent" is well-defined and the benchmark is reproducible.
//
// Emits T0_NS = CLOCK_MONOTONIC at the first send() (first byte on the wire). The Python
// listener stamps the same system-wide clock when it receives the N-th trade -> wire-to-wire.
//
#define _GNU_SOURCE 1
#include "titan/book/order.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

using namespace titan;

static std::uint64_t mono_ns() {
    struct timespec ts;
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<std::uint64_t>(ts.tv_nsec);
}

int main(int argc, char** argv) {
    const int  port = (argc > 1) ? std::atoi(argv[1]) : 9099;
    const long N    = (argc > 2) ? std::atol(argv[2]) : 1000000;   // taker orders == fills produced

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons((uint16_t)port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (sockaddr*)&a, sizeof(a)) != 0) { perror("connect"); return 1; }

    // Pre-allocate the whole stream: [0] = giant resting SELL, [1..N] = BUY qty 1 (each 1 fill).
    std::vector<Order> v;
    v.reserve(static_cast<std::size_t>(N) + 1);
    Order seed{};
    seed.id = 0; seed.price = 10000;
    seed.quantity = seed.remaining = static_cast<Qty>(N + 16);   // deeper than the taker flow -> never exhausts
    seed.side = Side::SELL; seed.type = OrderType::LIMIT;
    v.push_back(seed);
    for (long i = 1; i <= N; ++i) {
        Order o{};
        o.id = static_cast<std::uint64_t>(i); o.price = 10000;
        o.quantity = o.remaining = 1;
        o.side = Side::BUY; o.type = OrderType::LIMIT;
        v.push_back(o);
    }

    const char*       p     = reinterpret_cast<const char*>(v.data());
    const std::size_t total = v.size() * sizeof(Order);

    const std::uint64_t t0 = mono_ns();                          // first byte on the wire
    std::size_t sent = 0;
    while (sent < total) {
        const ssize_t w = ::send(fd, p + sent, total - sent, 0);
        if (w < 0) { if (errno == EINTR) continue; perror("send"); return 1; }
        sent += static_cast<std::size_t>(w);
    }
    const std::uint64_t t1 = mono_ns();                          // all orders accepted by the pipeline
    ::close(fd);

    std::printf("BLASTER T0_NS=%llu SEND_DONE_NS=%llu orders=%zu fills_expected=%ld bytes=%zu "
                "send_ms=%.2f ingest_rate=%.2f M/s\n",
                (unsigned long long)t0, (unsigned long long)t1, v.size(), N, total,
                (t1 - t0) / 1e6, (double)v.size() / ((t1 - t0) / 1e3));
    return 0;
}
