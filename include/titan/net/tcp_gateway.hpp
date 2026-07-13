#pragma once
//
// titan/net/tcp_gateway.hpp
// Edge-triggered epoll TCP ingress gateway (Linux/WSL2 target). Accepts client
// connections, reads the raw Order wire format directly off the socket, and hands each
// fully-received Order to the Sequencer (stamp seq -> journal -> publish).
//
// _GNU_SOURCE is required (not just _DEFAULT_SOURCE) because accept4() -- the atomic
// accept+SOCK_NONBLOCK primitive -- is a GNU/Linux extension, not part of the default
// POSIX exposure set. Must be defined before any system header is pulled in.
//
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

// Batched read: each recv() pulls up to READ_BUF bytes -- MANY Orders -- in ONE syscall,
// then we parse complete Order structs straight out of that in-memory chunk. This replaces
// the earlier one-recv()-per-Order design (a syscall per message; ~10k syscalls for 10k
// orders) with ~READ_BUF/sizeof(Order) Orders per syscall, so the read path is now bound
// only by socket-buffer refills, not by message count. A single Order split across recv()
// boundaries is carried in Conn::pending until the next chunk completes it (stream reassembly).
//
// Wire format: raw host-native Order bytes (a memcpy across the socket). This assumes
// the same machine/architecture as the engine on both ends -- NOT a portable encoding.
// A cross-arch/cross-endian deployment would need an explicit wire format; out of scope.
//
// Socket discipline:
//   * listen + accepted sockets are SOCK_NONBLOCK (accept4 sets it atomically on the
//     accepted fd -- no separate fcntl(), so no window where a blocking fd could stall
//     the event loop).
//   * TCP_NODELAY on every accepted connection -- disables Nagle so a lone Order isn't
//     held back coalescing with the next write (this buys latency, not throughput).
//   * Edge-triggered (EPOLLET) on the listen fd and every connection fd -- each
//     readiness notification is drained in a loop until EAGAIN/EWOULDBLOCK, per the
//     epoll(7) edge-triggered contract (miss this and a fd with data still queued can
//     stall forever -- ET only re-notifies on NEW readiness, not "still readable").
//
// Not on the matching-engine hot path: this is I/O-thread code. It is permitted to
// throw on setup failure (socket/bind/listen/epoll_ctl) and to allocate one map entry
// per LIVE TCP CONNECTION (not per order -- connections are rare/persistent in HFT).
//
// SequencerT only needs `void publish(Order) noexcept`, so this header does not depend
// on titan/pipeline/sequencer.hpp -- any conforming type (real Sequencer or a test
// mock) works, mirroring the Matcher's sink-templating pattern elsewhere in this repo.
//
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>

#include "titan/book/order.hpp"

namespace titan::net {

template <class SequencerT>
class TcpGateway {
public:
    // port == 0 -> ephemeral (OS-assigned); read the real bound port back via port().
    TcpGateway(std::uint16_t port, SequencerT& seq) : seq_(seq) {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (listen_fd_ < 0) throw std::runtime_error("tcp_gateway: socket() failed");

        const int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(listen_fd_);
            throw std::runtime_error("tcp_gateway: bind() failed");
        }
        socklen_t alen = sizeof(addr);
        if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &alen) == 0)
            bound_port_ = ntohs(addr.sin_port);

        if (::listen(listen_fd_, SOMAXCONN) != 0) {
            ::close(listen_fd_);
            throw std::runtime_error("tcp_gateway: listen() failed");
        }

        epoll_fd_ = ::epoll_create1(0);
        if (epoll_fd_ < 0) {
            ::close(listen_fd_);
            throw std::runtime_error("tcp_gateway: epoll_create1() failed");
        }

        stop_fd_ = ::eventfd(0, EFD_NONBLOCK);   // wakes epoll_wait for stop() -- no busy-poll
        if (stop_fd_ < 0) {
            ::close(epoll_fd_); ::close(listen_fd_);
            throw std::runtime_error("tcp_gateway: eventfd() failed");
        }

        arm(listen_fd_, EPOLLIN | EPOLLET);
        arm(stop_fd_,   EPOLLIN);                // level-triggered: read-then-exit, no drain loop needed
    }

    TcpGateway(const TcpGateway&)            = delete;
    TcpGateway& operator=(const TcpGateway&) = delete;

    // Closes every fd this gateway owns: live connections, the stop eventfd, the epoll
    // instance, the listen socket. Call after run() has returned (stop() + join()).
    ~TcpGateway() {
        for (auto& [fd, conn] : conns_) { (void)conn; ::close(fd); }
        if (stop_fd_   >= 0) ::close(stop_fd_);
        if (epoll_fd_  >= 0) ::close(epoll_fd_);
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    std::uint16_t port() const noexcept { return bound_port_; }

    // Blocking event loop -- call from a dedicated thread. Returns once stop() wakes it.
    void run() noexcept {
        epoll_event events[MAX_EVENTS];
        for (;;) {
            const int n = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, -1);
            if (n < 0) { if (errno == EINTR) continue; return; }
            for (int i = 0; i < n; ++i) {
                const int fd = events[i].data.fd;
                if (fd == stop_fd_)   { drain_stop(); return; }
                if (fd == listen_fd_) { accept_loop(); continue; }
                handle_conn(fd, events[i].events);
            }
        }
    }

    // Thread-safe wake for run() to return. Idempotent (repeated calls just re-signal).
    void stop() noexcept {
        const std::uint64_t one = 1;
        const ssize_t w = ::write(stop_fd_, &one, sizeof(one));
        (void)w;   // best-effort wake; EAGAIN means it's already signalled (eventfd saturated)
    }

private:
    static constexpr int         MAX_EVENTS = 64;
    static constexpr std::size_t READ_BUF   = 4096;   // per-recv() batch (~102 Orders/syscall)

    // Per-connection reassembly state. `pending` holds only a PARTIAL Order carried between
    // recv() chunks (a struct split across TCP/recv boundaries); `filled` is how many of its
    // leading bytes have arrived so far, in [0, sizeof(Order)).
    struct Conn {
        Order       pending{};
        std::size_t filled = 0;
    };

    void arm(int fd, std::uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0)
            throw std::runtime_error("tcp_gateway: epoll_ctl(ADD) failed");
    }

    void drain_stop() noexcept {
        std::uint64_t v;
        while (::read(stop_fd_, &v, sizeof(v)) > 0) { /* drain any residual signal */ }
    }

    // Listen fd is edge-triggered: must drain EVERY pending connection in one wave.
    void accept_loop() noexcept {
        for (;;) {
            const int cfd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;   // fully drained
                if (errno == EINTR) continue;
                return;                                                // give up on this wave
            }
            const int one = 1;
            ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));   // disable Nagle
            try {
                arm(cfd, EPOLLIN | EPOLLET);
            } catch (const std::runtime_error&) {
                ::close(cfd);
                continue;
            }
            conns_.emplace(cfd, Conn{});
        }
    }

    void close_conn(int fd) noexcept {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        ::close(fd);
        conns_.erase(fd);
    }

    // Connection fd is edge-triggered: drain until EAGAIN. Each recv() pulls a whole BATCH
    // of bytes (up to READ_BUF) in ONE syscall; parse_chunk() then walks that in-memory
    // buffer publishing every complete Order and carrying any trailing fragment forward.
    void handle_conn(int fd, std::uint32_t revents) noexcept {
        if (revents & (EPOLLERR | EPOLLHUP)) { close_conn(fd); return; }

        auto it = conns_.find(fd);
        if (it == conns_.end()) return;   // defensive: event for an already-closed fd
        Conn& c = it->second;

        std::array<std::uint8_t, READ_BUF> buf;
        for (;;) {
            const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
            if (n > 0) {
                parse_chunk(c, buf.data(), static_cast<std::size_t>(n));
                continue;                              // edge-triggered: keep draining
            }
            if (n == 0) { close_conn(fd); return; }     // peer sent FIN (graceful close)
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;   // drained for now
            if (errno == EINTR) continue;
            close_conn(fd); return;                     // real error -> drop the connection
        }
    }

    // Parse every complete Order out of one freshly-recv'd byte chunk, publishing each to the
    // Sequencer (stamp seq -> write-ahead journal -> ingress). Finishes a carried fragment
    // first, then walks whole Orders, then stashes any trailing partial Order in Conn::pending.
    void parse_chunk(Conn& c, const std::uint8_t* data, std::size_t len) noexcept {
        std::size_t off = 0;

        // (1) complete a partial Order carried from the previous recv().
        if (c.filled > 0) {
            const std::size_t need = sizeof(Order) - c.filled;
            const std::size_t take = (len < need) ? len : need;
            std::memcpy(reinterpret_cast<std::uint8_t*>(&c.pending) + c.filled, data, take);
            c.filled += take;
            off      += take;
            if (c.filled < sizeof(Order)) return;      // still incomplete -> await more bytes
            seq_.publish(c.pending);
            c.filled = 0;
        }

        // (2) publish every whole Order the chunk contains.
        while (len - off >= sizeof(Order)) {
            Order o;
            std::memcpy(&o, data + off, sizeof(Order));   // copy out of the (unaligned) byte buffer
            seq_.publish(o);
            off += sizeof(Order);
        }

        // (3) carry the trailing partial-Order fragment (if any) into pending for next time.
        const std::size_t rem = len - off;
        if (rem > 0) {
            std::memcpy(&c.pending, data + off, rem);
            c.filled = rem;
        }
    }

    SequencerT&                    seq_;
    int                            listen_fd_  = -1;
    int                            epoll_fd_   = -1;
    int                            stop_fd_    = -1;
    std::uint16_t                  bound_port_ = 0;
    std::unordered_map<int, Conn>  conns_;   // one entry per live connection, not per order
};

} // namespace titan::net
