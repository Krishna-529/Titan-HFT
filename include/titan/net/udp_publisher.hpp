#pragma once
//
// titan/net/udp_publisher.hpp
// Outbound market-data fan-out: blasts executed TradeEvents over UDP MULTICAST as raw binary
// (zero JSON -- the wire payload is a straight memcpy of the POD structs). This is the egress
// half of the I/O boundary, mirroring the TCP ingress gateway.
//
// Design (market-data semantics):
//   * NON-BLOCKING socket (SOCK_NONBLOCK). publish() must never stall the pipeline, so a full
//     socket send buffer DROPS the datagram rather than blocking -- UDP multicast is inherently
//     best-effort (gap-fill / snapshot recovery is a separate channel, out of scope). Drops are
//     counted, not fatal.
//   * IP_MULTICAST_IF selects the outbound interface, IP_MULTICAST_TTL bounds the scope
//     (1 = same subnet), IP_MULTICAST_LOOP lets same-host receivers get the feed (needed for a
//     local listener / testing).
//   * MTU-SAFE datagrams: a drained batch is chunked so each datagram carries whole TradeEvents
//     and stays under the Ethernet MTU (no IP fragmentation, which multiplies loss risk).
//
// _GNU_SOURCE is required for SOCK_NONBLOCK (a Linux socket-type flag); define it before any
// libc header is pulled in.
//
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "titan/book/trade_event.hpp"

namespace titan::net {

class UdpPublisher {
public:
    // group   : multicast group address, e.g. "239.1.1.1" (administratively-scoped range).
    // port    : destination UDP port.
    // iface_ip: outbound multicast interface ("0.0.0.0" = kernel default; "127.0.0.1" routes the
    //           feed over loopback for a same-host listener / local test; a NIC IP in production).
    // ttl     : IP_MULTICAST_TTL (1 = same subnet). loopback: deliver to local receivers too.
    UdpPublisher(const std::string& group, std::uint16_t port,
                 const std::string& iface_ip = "0.0.0.0", int ttl = 1, bool loopback = true) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK, 0);
        if (fd_ < 0) throw std::runtime_error("udp_publisher: socket() failed");

        in_addr ifaddr{};
        if (::inet_aton(iface_ip.c_str(), &ifaddr) == 0) { ::close(fd_); throw std::runtime_error("udp_publisher: bad iface ip"); }
        if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &ifaddr, sizeof(ifaddr)) != 0) {
            ::close(fd_); throw std::runtime_error("udp_publisher: IP_MULTICAST_IF failed");
        }
        const unsigned char ttl_c  = static_cast<unsigned char>(ttl);
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl_c, sizeof(ttl_c));
        const unsigned char loop_c = loopback ? 1 : 0;
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop_c, sizeof(loop_c));

        // Grow the send buffer to absorb bursts (fewer EAGAIN drops under a blast).
        const int sndbuf = 4 * 1024 * 1024;
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

        dst_.sin_family = AF_INET;
        dst_.sin_port   = htons(port);
        if (::inet_aton(group.c_str(), &dst_.sin_addr) == 0) { ::close(fd_); throw std::runtime_error("udp_publisher: bad group addr"); }
    }

    UdpPublisher(const UdpPublisher&)            = delete;
    UdpPublisher& operator=(const UdpPublisher&) = delete;
    ~UdpPublisher() { if (fd_ >= 0) ::close(fd_); }

    // Blast a drained batch of TradeEvents out as raw binary multicast, chunked into MTU-safe
    // datagrams. Best-effort: a full send buffer (EAGAIN) drops that datagram (counted). Never
    // blocks, never throws -> safe on the publisher hot path.
    void publish(const std::vector<TradeEvent>& batch) noexcept {
        const std::size_t n = batch.size();
        std::size_t i = 0;
        while (i < n) {
            const std::size_t chunk = std::min(n - i, TRADES_PER_DGRAM);
            const std::size_t bytes = chunk * sizeof(TradeEvent);
            const ssize_t s = ::sendto(fd_, &batch[i], bytes, 0,
                                       reinterpret_cast<const sockaddr*>(&dst_), sizeof(dst_));
            if (s < 0 && errno == EINTR) continue;                 // interrupted -> retry same chunk
            if (s == static_cast<ssize_t>(bytes)) { ++datagrams_; sent_ += chunk; }
            else                                  { ++drops_; dropped_ += chunk; }  // EAGAIN/short/error
            i += chunk;
        }
    }

    // Send ONE self-contained datagram of raw bytes (e.g. an L2 SnapshotBuffer). Best-effort,
    // non-blocking; caller keeps `bytes` <= one MTU-safe payload (depth-limited snapshot) so no
    // IP fragmentation. Returns true if the datagram was handed to the kernel.
    bool send_raw(const void* data, std::size_t bytes) noexcept {
        for (;;) {
            const ssize_t s = ::sendto(fd_, data, bytes, 0,
                                       reinterpret_cast<const sockaddr*>(&dst_), sizeof(dst_));
            if (s < 0 && errno == EINTR) continue;
            if (s == static_cast<ssize_t>(bytes)) { ++datagrams_; return true; }
            ++drops_;
            return false;
        }
    }

    std::uint64_t sent()      const noexcept { return sent_; }       // TradeEvents sent
    std::uint64_t dropped()   const noexcept { return dropped_; }    // TradeEvents dropped (buffer full)
    std::uint64_t datagrams() const noexcept { return datagrams_; }  // datagrams transmitted
    std::uint64_t drops()     const noexcept { return drops_; }      // datagrams dropped

private:
    // Keep each datagram under the 1500 B Ethernet MTU (no IP fragmentation): payload budget
    // 1440 B / sizeof(TradeEvent) events per datagram.
    static constexpr std::size_t MAX_DGRAM_BYTES  = 1440;
    static constexpr std::size_t TRADES_PER_DGRAM = MAX_DGRAM_BYTES / sizeof(TradeEvent);

    int           fd_        = -1;
    sockaddr_in   dst_{};
    std::uint64_t sent_      = 0;
    std::uint64_t dropped_   = 0;
    std::uint64_t datagrams_ = 0;
    std::uint64_t drops_     = 0;
};

} // namespace titan::net
