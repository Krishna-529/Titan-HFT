#!/usr/bin/env python3
"""
Titan HFT — fake market + bot simulator (UI smoke test, no C++ engine needed).

Emits, matching the exact C++ wire formats:
  * binary UDP MULTICAST trades    -> 239.1.1.1:30001  (TradeEvent, 40 B, several per datagram)
  * binary UDP MULTICAST snapshots -> 239.1.1.1:30002  (SnapshotHeader 64 B + L2 levels 24 B)
and streams newline-JSON bot telemetry over TCP -> 127.0.0.1:8081 (mock MM + Sniper bots).

Stdlib only.  Run:  python3 ui/fake_bot.py     (Ctrl-C to stop)
"""
import json
import math
import os
import random
import socket
import struct
import threading
import time

GROUP       = os.getenv("TITAN_MCAST_GROUP", "239.1.1.1")
IFACE       = os.getenv("TITAN_MCAST_IFACE", "127.0.0.1")   # same interface the UI joins on
TRADES_PORT = int(os.getenv("TITAN_TRADES_PORT", "30001"))
SNAP_PORT   = int(os.getenv("TITAN_SNAP_PORT",   "30002"))
BOT_HOST    = os.getenv("TITAN_BOT_HOST", "127.0.0.1")
BOT_PORT    = int(os.getenv("TITAN_BOT_PORT",    "8081"))
GW_PORT     = int(os.getenv("TITAN_GW_PORT",     "9099"))   # mock engine gateway (ingress)

# Wire formats — identical to ui/app.py and the C++ headers.
TRADE  = struct.Struct("<QQqQIBB2x")   # taker,maker,price,feed_seq,qty,side,status,pad -> 40
SNAP_H = struct.Struct("<QIIQIIQ24x")  # magic,ver,level_count,feed_seq,bid_n,ask_n,checksum -> 64
SNAP_L = struct.Struct("<qQIB3x")      # price,total_qty,order_count,side,pad -> 24
ORDER  = struct.Struct("<QQqIIBB6x")   # id,seq,price,qty,remaining,side,type,pad -> 40 (order.hpp)
MAGIC  = 0x544954414E534E50            # "TITANSNP"
MASK64 = (1 << 64) - 1


def mcast_tx():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,   socket.inet_aton(IFACE))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL,  1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)   # deliver to same-host listeners
    return s


class Sim:
    def __init__(self):
        self.mid = 10000.0
        self.seq = 0          # monotonic feed_seq, shared across trades + snapshots
        self.oid = 1          # synthetic order ids
        self.tx = mcast_tx()
        self.bot = None       # lazy TCP connection to the UI

    def step_price(self):
        # gentle mean-reverting random walk around 10000
        self.mid += random.uniform(-0.8, 0.8) + (10000.0 - self.mid) * 0.01
        self.mid = max(9900.0, min(10100.0, self.mid))

    def emit_trades(self):
        mid = int(round(self.mid))
        buf = bytearray()
        for _ in range(random.randint(1, 6)):
            self.seq += 1
            side = random.randint(0, 1)                       # 0=BUY, 1=SELL
            px   = mid + (1 if side == 0 else -1) * random.randint(0, 3)
            qty  = random.randint(1, 20)
            taker = self.oid; self.oid += 1
            maker = self.oid; self.oid += 1
            status = 0 if random.random() > 0.03 else 1       # ~3% rejects
            if status == 1:
                qty, maker = 0, 0
            buf += TRADE.pack(taker, maker, px, self.seq, qty, side, status)
        self.tx.sendto(bytes(buf), (GROUP, TRADES_PORT))

    def emit_snapshot(self):
        mid = int(round(self.mid))
        bids = [(mid - i, random.randint(20, 300), random.randint(1, 12), 0) for i in range(1, 9)]
        asks = [(mid + i, random.randint(20, 300), random.randint(1, 12), 1) for i in range(1, 9)]
        levels = bids + asks                                  # bids best-first, asks best-first
        chk = 0
        body = bytearray()
        for (p, tq, oc, sd) in levels:
            chk = (chk + (p & MASK64) + tq + oc + sd) & MASK64
            body += SNAP_L.pack(p, tq, oc, sd)
        self.seq += 1
        hdr = SNAP_H.pack(MAGIC, 1, len(levels), self.seq, len(bids), len(asks), chk)
        self.tx.sendto(hdr + bytes(body), (GROUP, SNAP_PORT))

    def send_bots(self, t):
        if self.bot is None:
            try:
                self.bot = socket.create_connection((BOT_HOST, BOT_PORT), timeout=1)
            except OSError:
                self.bot = None
                return
        mm_inv = int(90 * math.sin(t / 9.0))
        sn_inv = int(45 * math.sin(t / 2.3)) if random.random() > 0.4 else 0
        msgs = [
            {"bot_id": "MM_1",     "pnl": round(240 * math.sin(t / 6.0) + random.uniform(-15, 15), 2),
             "inventory": mm_inv,  "status": "QUOTING" if abs(mm_inv) < 60 else "HEDGING"},
            {"bot_id": "SNIPER_1", "pnl": round(120 + 80 * math.sin(t / 4.0) + t * 1.5, 2),
             "inventory": sn_inv,  "status": random.choice(["HUNTING", "HUNTING", "IDLE", "FIRED"])},
        ]
        try:
            for m in msgs:
                self.bot.sendall((json.dumps(m) + "\n").encode())
        except OSError:
            try: self.bot.close()
            except OSError: pass
            self.bot = None


def gateway_mock():
    """Mock the C++ engine's TCP gateway on port 9099. Receives binary Orders from ui/app.py
    (the manual-entry path), prints each, and injects a synthetic FILL into the trade multicast
    feed so the order visibly 'executes' in the UI. Runs on its own daemon thread."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", GW_PORT))
    srv.listen(4)
    tx = mcast_tx()
    gseq = 20_000_000                       # distinct feed_seq range for manual fills
    print(f"[fake_bot] mock gateway listening  tcp 127.0.0.1:{GW_PORT}", flush=True)
    while True:
        try:
            conn, _addr = srv.accept()
        except OSError:
            continue
        buf = b""
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while len(buf) >= ORDER.size:          # parse whole 40-byte Orders from the stream
                    oid, _sq, px, qty, _rem, side, otype = ORDER.unpack(buf[:ORDER.size])
                    buf = buf[ORDER.size:]
                    s = "BUY " if side == 0 else "SELL"
                    ty = {0: "LIMIT", 1: "MARKET", 2: "IOC"}.get(otype, otype)
                    print(f"[gateway] order {s} qty={qty:<4} @ {px:<7} id={oid} type={ty} -> inject fill",
                          flush=True)
                    gseq += 1
                    tx.sendto(TRADE.pack(oid, 0, px, gseq, qty, side, 0), (GROUP, TRADES_PORT))
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


def main():
    print(f"[fake_bot] trades->{GROUP}:{TRADES_PORT}  snap->{GROUP}:{SNAP_PORT}  "
          f"bots->{BOT_HOST}:{BOT_PORT}  (iface {IFACE})", flush=True)
    threading.Thread(target=gateway_mock, daemon=True).start()
    sim = Sim()
    t0 = time.monotonic()
    next_trade = next_snap = next_bot = 0.0
    while True:
        now = time.monotonic()
        t = now - t0
        sim.step_price()
        if now >= next_trade: sim.emit_trades();     next_trade = now + 0.12
        if now >= next_snap:  sim.emit_snapshot();    next_snap  = now + 0.50
        if now >= next_bot:   sim.send_bots(t);       next_bot   = now + 0.40
        time.sleep(0.03)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
