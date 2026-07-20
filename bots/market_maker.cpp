//
// bots/market_maker.cpp
// Titan HFT -- Avellaneda-Stoikov-flavored market-making bot (external client, C++20).
//
// This is NOT engine code: it is a stand-alone client that talks to a running `titan-server`
// over the real wire protocol. It has no zero-crash obligations -- it may allocate, lock, and
// use std::thread freely. Its job is to post two-sided resting liquidity and react to fills,
// so the pipeline (and the Web UI) has something live to show.
//
//   Order entry   : TCP  -> 127.0.0.1:9099   raw 40-byte Order structs (host-native, memcpy wire)
//   Trade feed    : UDP  <- 239.1.1.1:30001   TradeEvent[] (40 B each, several per datagram)
//   Snapshot feed : UDP  <- 239.1.1.1:30002   SnapshotHeader(64 B) + SnapshotLevel[](24 B)
//   Telemetry     : TCP  -> 127.0.0.1:8081   newline-delimited JSON {bot_id,pnl,inventory,status}
//
// We include the engine's own POD headers so the wire structs are LAYOUT-IDENTICAL by
// construction (they carry static_asserts), not re-declared and hoped-for.
//
// -------------------------------------------------------------------------------------------
// STRATEGY (a deliberately simple Avellaneda-Stoikov reduction)
//   * mid       : (best_bid + best_ask) / 2, read from the L2 snapshot feed.
//   * reservation r = mid - skew(inventory).  Inventory skew shifts BOTH quotes: long -> shift
//     down (ask gets aggressive, we lean to sell); short -> shift up (we lean to buy). Skew only
//     engages once |inventory| > SKEW_THRESHOLD, and is clamped, so small books quote symmetric.
//   * quotes    : bid = r - HALF_SPREAD,  ask = r + HALF_SPREAD   (HALF_SPREAD = 5 ticks).
//   * circuit breaker: |inventory| > HALT_INV -> stop quoting and FLATTEN with marketable IOC.
//
// -------------------------------------------------------------------------------------------
// PROTOCOL LIMITATION (important, and deliberately worked around here)
//   The wire Order has no CANCEL action -- OrderType is only LIMIT/MARKET/IOC, the gateway
//   submits every order, and OrderBook::add() REJECTS a duplicate id. So a client cannot cancel
//   or replace a resting quote today. Consequences, and how this bot copes:
//     1. Re-quote posts a NEW id each time; we THROTTLE (only when the mid moves >= REQUOTE_TICKS
//        or a timer elapses) so stale quotes don't pile up unboundedly. Old quotes rest until hit.
//     2. The "mass-cancel" circuit breaker cannot cancel; instead it HALTS quoting and sends
//        marketable IOC to drive inventory back toward flat -- the true risk-neutralizing action.
//   Wiring a real cancel path (an op-type on the wire -> gateway -> book.cancel) is an engine
//   change tracked separately; this bot is written to the protocol as it exists.
//
// Build/run:  see bot.sh   (g++ -O3 -std=c++20 -pthread -Iinclude ...)
//
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include "titan/book/order.hpp"
#include "titan/book/snapshot.hpp"
#include "titan/book/trade_event.hpp"
#include "titan/domain/types.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

using namespace titan;

namespace {

// ----------------------------------- endpoints (match src/main.cpp + ui/) -----------------------
constexpr const char* GW_HOST      = "127.0.0.1";
constexpr std::uint16_t GW_PORT    = 9099;          // engine TCP gateway (order entry)
constexpr const char* MCAST_GROUP  = "239.1.1.1";
constexpr const char* MCAST_IF     = "127.0.0.1";
constexpr std::uint16_t TRADES_PORT = 30001;        // incremental TradeEvent feed
constexpr std::uint16_t SNAP_PORT   = 30002;        // L2 snapshot feed
constexpr const char* UI_HOST      = "127.0.0.1";
constexpr std::uint16_t UI_PORT    = 8081;          // Web UI bot-telemetry sink

// ----------------------------------- strategy knobs -------------------------------------------
constexpr const char* BOT_ID          = "MM_TITAN";
constexpr PriceTick   HALF_SPREAD     = 5;          // ticks each side of the reservation price
constexpr long        SKEW_THRESHOLD  = 50;         // |inventory| beyond which we skew quotes
constexpr double      SKEW_PER_UNIT   = 0.20;       // reservation shift (ticks) per unit inventory
constexpr PriceTick   MAX_SKEW        = 40;         // clamp the skew so quotes stay sane
constexpr long        HALT_INV        = 500;        // circuit breaker: |inventory| past this -> flatten
constexpr Qty         QUOTE_QTY       = 5;          // size posted on each side
constexpr Qty         FLATTEN_CHUNK   = 50;         // IOC size per flatten pulse
constexpr PriceTick   REQUOTE_TICKS   = 1;          // re-quote once the mid has moved this far
constexpr std::uint64_t REQUOTE_MS    = 1000;       // ...or at least this often, to refresh liquidity
constexpr PriceTick   IOC_CROSS       = 20;         // how far an IOC reaches through the book to fill
constexpr PriceTick   SEED_MID        = 10'000;     // cold-start reference mid until the feed gives one
                                                    // (a solo MM must prime the book before a snapshot
                                                    //  can report a real two-sided mid -> avoids deadlock)

// Order-id band: ids must be < ID_CAP (1<<22 in the server). Start high to avoid colliding with
// manual UI entries; a demo run stays far below the 4.2M ceiling.
constexpr std::uint64_t ID_BASE = 1'000'000;
constexpr std::uint64_t ID_MAX  = (1u << 22) - 1;

// ----------------------------------- shared state ---------------------------------------------
std::mutex        g_mx;
PriceTick         g_best_bid = 0, g_best_ask = 0;
bool              g_have_mid = false;
long              g_inventory = 0;            // signed position (units)
double            g_cash      = 0.0;          // realized cash flow; PnL = cash + inv*mid
std::uint64_t     g_last_trade_px = 0;
std::atomic<bool> g_run{true};

struct MyOrder { Side side; PriceTick price; Qty remaining; };
std::unordered_map<std::uint64_t, MyOrder> g_orders;   // live ids we have posted
std::uint64_t g_next_id = ID_BASE;

std::uint64_t now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

// ----------------------------------- socket helpers -------------------------------------------
// Blocking TCP connect. Returns fd or -1. Caller retries.
int tcp_connect(const char* host, std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port   = htons(port);
    ::inet_pton(AF_INET, host, &a.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }
    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));   // no Nagle: quotes go out now
    return fd;
}

// Join a UDP multicast group on the loopback interface. Returns a bound, subscribed fd or -1.
int mcast_rx(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;
    const int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port        = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) { ::close(fd); return -1; }

    ip_mreq mr{};
    ::inet_pton(AF_INET, MCAST_GROUP, &mr.imr_multiaddr);
    ::inet_pton(AF_INET, MCAST_IF,    &mr.imr_interface);
    if (::setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mr, sizeof(mr)) != 0) { ::close(fd); return -1; }
    return fd;
}

bool send_all(int fd, const void* p, std::size_t n) {
    const auto* b = static_cast<const std::uint8_t*>(p);
    std::size_t off = 0;
    while (off < n) {
        const ssize_t k = ::send(fd, b + off, n - off, MSG_NOSIGNAL);
        if (k <= 0) return false;
        off += static_cast<std::size_t>(k);
    }
    return true;
}

// Fire one order at the gateway. seq is left 0 (the Sequencer stamps it). Records live LIMITs so
// the trade feed can attribute fills back to us.
bool fire(int gw_fd, std::uint64_t id, Side side, OrderType type, PriceTick px, Qty qty) {
    Order o{};
    o.id = id; o.seq = 0; o.price = px;
    o.quantity = o.remaining = qty;
    o.side = side; o.type = type;
    if (!send_all(gw_fd, &o, sizeof(o))) return false;
    if (type == OrderType::LIMIT) {
        std::lock_guard<std::mutex> lk(g_mx);
        g_orders[id] = MyOrder{side, px, qty};
    }
    return true;
}

std::uint64_t next_id() {
    if (g_next_id >= ID_MAX) g_next_id = ID_BASE;   // demo-scale wrap (won't collide in a short run)
    return g_next_id++;
}

// ----------------------------------- fill accounting ------------------------------------------
// Apply a fill against one of our orders (whether we were the maker or the taker -- we key off the
// stored side, so both are handled uniformly). Caller holds g_mx.
void apply_fill_locked(std::uint64_t id, PriceTick px, Qty q) {
    auto it = g_orders.find(id);
    if (it == g_orders.end() || q == 0) return;
    const Qty take = (q < it->second.remaining) ? q : it->second.remaining;
    if (it->second.side == Side::BUY) { g_inventory += take; g_cash -= static_cast<double>(px) * take; }
    else                              { g_inventory -= take; g_cash += static_cast<double>(px) * take; }
    it->second.remaining -= take;
    if (it->second.remaining == 0) g_orders.erase(it);
}

// ----------------------------------- feed listeners -------------------------------------------
void snapshot_thread() {
    int fd = -1;
    std::uint8_t buf[65536];
    while (g_run.load()) {
        if (fd < 0) { fd = mcast_rx(SNAP_PORT); if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; } }
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n < static_cast<ssize_t>(sizeof(SnapshotHeader))) continue;

        SnapshotHeader h{};
        std::memcpy(&h, buf, sizeof(h));
        if (h.magic != SNAPSHOT_MAGIC) continue;
        const std::size_t got = (static_cast<std::size_t>(n) - sizeof(h)) / sizeof(SnapshotLevel);
        const std::size_t nlv = (h.level_count < got) ? h.level_count : got;
        if (nlv == 0) continue;

        // Levels are bids best-first (h.bid_levels of them), then asks best-first.
        PriceTick bb = 0, ba = 0; bool hb = false, ha = false;
        if (h.bid_levels > 0) {
            SnapshotLevel L{}; std::memcpy(&L, buf + sizeof(h), sizeof(L));
            bb = L.price; hb = true;
        }
        if (h.ask_levels > 0 && h.bid_levels < nlv) {
            SnapshotLevel L{}; std::memcpy(&L, buf + sizeof(h) + h.bid_levels * sizeof(L), sizeof(L));
            ba = L.price; ha = true;
        }
        if (hb && ha) {
            std::lock_guard<std::mutex> lk(g_mx);
            g_best_bid = bb; g_best_ask = ba; g_have_mid = true;
        }
    }
    if (fd >= 0) ::close(fd);
}

void trade_thread() {
    int fd = -1;
    std::uint8_t buf[65536];
    while (g_run.load()) {
        if (fd < 0) { fd = mcast_rx(TRADES_PORT); if (fd < 0) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); continue; } }
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) continue;
        const std::size_t count = static_cast<std::size_t>(n) / sizeof(TradeEvent);

        std::lock_guard<std::mutex> lk(g_mx);
        for (std::size_t i = 0; i < count; ++i) {
            TradeEvent ev{};
            std::memcpy(&ev, buf + i * sizeof(TradeEvent), sizeof(ev));
            if (ev.status == TRADE_STATUS_REJECTED) {           // our order bounced (e.g. bad price)
                g_orders.erase(ev.taker_id);
                continue;
            }
            g_last_trade_px = static_cast<std::uint64_t>(ev.price);
            apply_fill_locked(ev.taker_id, ev.price, ev.quantity);   // we were the aggressor
            apply_fill_locked(ev.maker_id, ev.price, ev.quantity);   // ...or the resting maker
        }
    }
    if (fd >= 0) ::close(fd);
}

// ----------------------------------- telemetry ------------------------------------------------
void send_telemetry(int& ui_fd, double pnl, long inv, const char* status) {
    if (ui_fd < 0) { ui_fd = tcp_connect(UI_HOST, UI_PORT); if (ui_fd < 0) return; }
    char line[256];
    const int m = std::snprintf(line, sizeof(line),
        "{\"bot_id\":\"%s\",\"pnl\":%.2f,\"inventory\":%ld,\"status\":\"%s\"}\n",
        BOT_ID, pnl, inv, status);
    if (m <= 0 || !send_all(ui_fd, line, static_cast<std::size_t>(m))) { ::close(ui_fd); ui_fd = -1; }
}

}  // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);   // unbuffered: banner/status show live even when piped
    std::printf("[MM] %s  gateway tcp %s:%u   feeds udp %s:%u/%u   ui tcp %s:%u\n",
                BOT_ID, GW_HOST, GW_PORT, MCAST_GROUP, TRADES_PORT, SNAP_PORT, UI_HOST, UI_PORT);
    std::printf("[MM] half_spread=%lld skew@%ld halt@%ld qty=%u   (NOTE: no wire cancel -> IOC-flatten breaker)\n",
                (long long)HALF_SPREAD, SKEW_THRESHOLD, HALT_INV, QUOTE_QTY);

    std::thread snap_t(snapshot_thread);
    std::thread trade_t(trade_thread);

    int gw_fd = -1, ui_fd = -1;
    PriceTick last_quote_mid = 0;
    bool      have_quoted = false;
    std::uint64_t last_tele = 0, last_quote_ms = 0;

    while (g_run.load()) {
        // (Re)connect order entry.
        if (gw_fd < 0) {
            gw_fd = tcp_connect(GW_HOST, GW_PORT);
            if (gw_fd < 0) {
                if (now_ms() - last_tele > 400) { send_telemetry(ui_fd, 0.0, 0, "NO-GATEWAY"); last_tele = now_ms(); }
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                continue;
            }
        }

        // Snapshot of shared state.
        PriceTick bid, ask; bool have; long inv; double cash; std::uint64_t ltp;
        {
            std::lock_guard<std::mutex> lk(g_mx);
            bid = g_best_bid; ask = g_best_ask; have = g_have_mid;
            inv = g_inventory; cash = g_cash; ltp = g_last_trade_px;
        }

        // Effective mid: the snapshot's two-sided mid once we have one; else the last trade price;
        // else the cold-start seed so a solo MM can prime an empty book (see SEED_MID).
        const PriceTick mid = have ? (bid + ask) / 2
                                   : (ltp > 0 ? static_cast<PriceTick>(ltp) : SEED_MID);
        const double    pnl = cash + static_cast<double>(inv) * static_cast<double>(mid);
        const char* prime = have ? "" : "PRIMING:";

        const char* status;
        if (inv > HALT_INV || inv < -HALT_INV) {
            // ---- circuit breaker: stop quoting, flatten via marketable IOC ----
            status = "HALTED";
            if (inv > 0) fire(gw_fd, next_id(), Side::SELL, OrderType::IOC, mid - IOC_CROSS, FLATTEN_CHUNK);
            else         fire(gw_fd, next_id(), Side::BUY,  OrderType::IOC, mid + IOC_CROSS, FLATTEN_CHUNK);
        } else {
            // ---- Avellaneda-Stoikov-lite two-sided quoting ----
            PriceTick skew = 0;
            if (inv > SKEW_THRESHOLD || inv < -SKEW_THRESHOLD) {
                double s = static_cast<double>(inv) * SKEW_PER_UNIT;   // long -> +ve -> shift r down
                if (s >  static_cast<double>(MAX_SKEW)) s =  static_cast<double>(MAX_SKEW);
                if (s < -static_cast<double>(MAX_SKEW)) s = -static_cast<double>(MAX_SKEW);
                skew = static_cast<PriceTick>(std::llround(s));
            }
            const PriceTick r      = mid - skew;
            const PriceTick bid_px = r - HALF_SPREAD;
            const PriceTick ask_px = r + HALF_SPREAD;
            status = (skew != 0) ? "SKEWING" : "QUOTING";

            // Throttle: re-post when the mid has moved, OR every REQUOTE_MS to refresh liquidity
            // (no wire cancel, so every quote is a fresh resting order -- keep the churn bounded).
            const PriceTick moved = have_quoted ? (mid > last_quote_mid ? mid - last_quote_mid
                                                                        : last_quote_mid - mid) : REQUOTE_TICKS;
            const bool stale = (now_ms() - last_quote_ms) >= REQUOTE_MS;
            if (!have_quoted || moved >= REQUOTE_TICKS || stale) {
                const bool ok1 = (bid_px > 0) && fire(gw_fd, next_id(), Side::BUY,  OrderType::LIMIT, bid_px, QUOTE_QTY);
                const bool ok2 = fire(gw_fd, next_id(), Side::SELL, OrderType::LIMIT, ask_px, QUOTE_QTY);
                if (!ok1 || !ok2) { ::close(gw_fd); gw_fd = -1; continue; }   // gateway dropped -> reconnect
                last_quote_mid = mid; have_quoted = true; last_quote_ms = now_ms();
            }
        }

        if (now_ms() - last_tele > 400) {
            char st[48];
            std::snprintf(st, sizeof(st), "%s%s", prime, status);
            send_telemetry(ui_fd, pnl, inv, st);
            last_tele = now_ms();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    g_run.store(false);
    snap_t.join();
    trade_t.join();
    if (gw_fd >= 0) ::close(gw_fd);
    if (ui_fd >= 0) ::close(ui_fd);
    return 0;
}
