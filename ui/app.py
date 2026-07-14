#!/usr/bin/env python3
"""
Titan HFT — UI telemetry gateway.

Bridges the exchange's binary market-data plane to a browser, and proxies manual orders back:
  * joins the two UDP MULTICAST feeds and unpacks raw C++ POD structs (no JSON on the wire):
        trades    :30001  -> TradeEvent   (40 B, up to 36 per datagram)
        snapshots :30002  -> SnapshotBuffer (64 B header + N x 24 B L2 levels, one per datagram)
  * aggregates the live trade tape into 1-second OHLCV candles for the chart,
  * accepts newline-delimited JSON telemetry from trading bots over TCP :8081,
  * opens a persistent async TCP client to the engine's Gateway (:9099) and, on a manual order
    from the browser, struct.pack()s it into the exact C++ Order wire format and transmits it,
  * fans aggregated state (book / tape / candles / risk desk) to web clients over WebSocket.

Everything runs on ONE asyncio event loop, so the shared State needs no locks.

Wire formats MUST match include/titan/book/{order,trade_event,snapshot}.hpp.

Run:
    pip install fastapi "uvicorn[standard]"
    python ui/app.py                 # -> http://127.0.0.1:8080
"""
import asyncio
import itertools
import json
import os
import socket
import struct
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import uvicorn

# --------------------------------------------------------------------------------------
# Config (env-overridable; defaults mirror src/main.cpp)
# --------------------------------------------------------------------------------------
MCAST_GROUP  = os.getenv("TITAN_MCAST_GROUP", "239.1.1.1")
MCAST_IFACE  = os.getenv("TITAN_MCAST_IFACE", "127.0.0.1")   # must match the server's IP_MULTICAST_IF
TRADES_PORT  = int(os.getenv("TITAN_TRADES_PORT", "30001"))
SNAP_PORT    = int(os.getenv("TITAN_SNAP_PORT",   "30002"))
BOT_TCP_PORT = int(os.getenv("TITAN_BOT_PORT",    "8081"))
GW_HOST      = os.getenv("TITAN_GW_HOST", "127.0.0.1")       # engine gateway (order ingress)
GW_PORT      = int(os.getenv("TITAN_GW_PORT",  "9099"))
HTTP_HOST    = os.getenv("TITAN_HTTP_HOST", "127.0.0.1")
HTTP_PORT    = int(os.getenv("TITAN_HTTP_PORT", "8080"))
PUSH_HZ      = float(os.getenv("TITAN_PUSH_HZ", "10"))        # WebSocket broadcast rate (Hz)
BOOK_DEPTH   = int(os.getenv("TITAN_BOOK_DEPTH", "20"))       # levels/side pushed to the UI
TAPE_MAX     = 64                                             # trades retained for the tape
CANDLE_MAX   = 180                                            # 1s bars retained (~3 min)
BOT_STALE_S  = 5.0                                            # bot marked STALE after this idle gap
MANUAL_ID_BASE = 3_000_000                                    # manual order ids (< engine ID_CAP = 1<<22)

# --------------------------------------------------------------------------------------
# Binary wire formats (little-endian; sizes asserted below)
# --------------------------------------------------------------------------------------
TRADE  = struct.Struct("<QQqQIBB2x")   # taker,maker,price,feed_seq,qty,side,status,pad  -> 40 B
SNAP_H = struct.Struct("<QIIQIIQ24x")  # magic,ver,level_count,feed_seq,bid_n,ask_n,checksum -> 64 B
SNAP_L = struct.Struct("<qQIB3x")      # price,total_qty,order_count,side,pad            -> 24 B
ORDER  = struct.Struct("<QQqIIBB6x")   # id,seq,price,qty,remaining,side,type,pad        -> 40 B
assert (TRADE.size, SNAP_H.size, SNAP_L.size, ORDER.size) == (40, 64, 24, 40), "wire format drift"

SNAP_MAGIC = 0x544954414E534E50        # "TITANSNP"
SIDE   = {0: "BUY", 1: "SELL"}
STATUS = {0: "FILL", 1: "REJECT"}


# --------------------------------------------------------------------------------------
# Shared state (single-threaded asyncio -> no locks)
# --------------------------------------------------------------------------------------
class State:
    def __init__(self):
        self.bids = []          # [[price, total_qty, order_count], ...] best-first
        self.asks = []
        self.book_seq = 0
        self.snaps = 0
        self.tape = deque(maxlen=TAPE_MAX)
        self.trades = 0
        self.rejects = 0
        self.volume = 0
        self.last_price = None
        self.candles = deque(maxlen=CANDLE_MAX)   # completed 1s OHLCV bars
        self.cur_bar = None                       # forming bar
        self.bots = {}          # bot_id -> latest telemetry dict (+ _ts)

    def add_to_bar(self, price, qty):
        t = int(time.time())                      # 1-second bucket by arrival time (trades carry no ts)
        b = self.cur_bar
        if b is None or t != b["time"]:
            if b is not None:
                self.candles.append(b)
            self.cur_bar = {"time": t, "open": price, "high": price,
                            "low": price, "close": price, "volume": qty}
        else:
            if price > b["high"]: b["high"] = price
            if price < b["low"]:  b["low"] = price
            b["close"] = price
            b["volume"] += qty


S = State()
clients: set = set()            # active WebSocket connections
gw_writer: asyncio.StreamWriter = None   # persistent connection to the engine gateway
manual_ids = itertools.count(MANUAL_ID_BASE)


# --------------------------------------------------------------------------------------
# Feed parsers
# --------------------------------------------------------------------------------------
def on_trades(data: bytes):
    n = len(data) // TRADE.size
    for i in range(n):
        _taker, _maker, price, seq, qty, side, status = TRADE.unpack_from(data, i * TRADE.size)
        if status == 0:                           # FILL
            S.trades += 1
            S.volume += qty
            S.last_price = price
            S.add_to_bar(price, qty)
        else:                                     # REJECT
            S.rejects += 1
        S.tape.appendleft({
            "seq": seq, "price": price, "qty": qty,
            "side": SIDE.get(side, side), "status": STATUS.get(status, status),
        })


def on_snapshot(data: bytes):
    if len(data) < SNAP_H.size:
        return
    magic, _ver, lvls, seq, _bn, _an, _chk = SNAP_H.unpack_from(data, 0)
    if magic != SNAP_MAGIC or len(data) < SNAP_H.size + lvls * SNAP_L.size:
        return
    bids, asks = [], []
    off = SNAP_H.size
    for _ in range(lvls):
        price, total_qty, order_count, side = SNAP_L.unpack_from(data, off)
        off += SNAP_L.size
        (bids if side == 0 else asks).append([price, total_qty, order_count])
    S.bids, S.asks, S.book_seq = bids[:BOOK_DEPTH], asks[:BOOK_DEPTH], seq
    S.snaps += 1


# --------------------------------------------------------------------------------------
# UDP multicast (asyncio datagram endpoint over a pre-joined socket)
# --------------------------------------------------------------------------------------
class Mcast(asyncio.DatagramProtocol):
    def __init__(self, handler):
        self.handler = handler

    def datagram_received(self, data, addr):
        try:
            self.handler(data)
        except Exception:
            pass


def make_mcast_socket(port: int) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
    except OSError:
        pass
    s.bind(("", port))
    mreq = socket.inet_aton(MCAST_GROUP) + socket.inet_aton(MCAST_IFACE)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    s.setblocking(False)
    return s


# --------------------------------------------------------------------------------------
# TCP bot telemetry (newline-delimited JSON)
# --------------------------------------------------------------------------------------
async def handle_bot(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            bot_id = str(msg.get("bot_id") or f"{peer}")
            rec = S.bots.get(bot_id, {})
            rec.update(msg)
            rec["bot_id"] = bot_id
            rec["_ts"] = time.time()
            S.bots[bot_id] = rec
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# Engine gateway TCP client (manual order egress) + reconnect loop
# --------------------------------------------------------------------------------------
async def gateway_client():
    global gw_writer
    while True:
        try:
            reader, writer = await asyncio.open_connection(GW_HOST, GW_PORT)
            gw_writer = writer
            print(f"[titan-ui] gateway connected  tcp {GW_HOST}:{GW_PORT}", flush=True)
            while True:                            # engine gateway is ingress-only; wait for close
                if not await reader.read(4096):
                    break
        except Exception:
            pass
        gw_writer = None
        await asyncio.sleep(1.0)                    # reconnect backoff


async def submit_manual_order(msg: dict):
    if gw_writer is None or gw_writer.is_closing():
        return False, f"gateway {GW_HOST}:{GW_PORT} not connected"
    side = str(msg.get("side", "BUY")).upper()
    scode = 1 if side == "SELL" else 0
    try:
        price = int(msg["price"])
        qty = int(msg["qty"])
    except Exception:
        return False, "invalid price/qty"
    if qty <= 0:
        return False, "quantity must be > 0"
    oid = next(manual_ids)
    pkt = ORDER.pack(oid, 0, price, qty, qty, scode, 0)   # seq stamped by engine; type LIMIT
    try:
        gw_writer.write(pkt)
        await gw_writer.drain()
    except Exception as e:
        return False, f"send failed: {e}"
    return True, f"{side} {qty} @ {price}  (id {oid})"


# --------------------------------------------------------------------------------------
# WebSocket broadcast
# --------------------------------------------------------------------------------------
def snapshot_state() -> dict:
    now = time.time()
    best_bid = S.bids[0][0] if S.bids else None
    best_ask = S.asks[0][0] if S.asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
    bots = []
    for b in S.bots.values():
        age = now - b.get("_ts", now)
        live = age <= BOT_STALE_S
        bots.append({
            "bot_id": b.get("bot_id"), "pnl": b.get("pnl", 0.0),
            "inventory": b.get("inventory", 0),
            "status": b.get("status", "LIVE" if live else "STALE"),
            "live": live, "age": round(age, 1),
        })
    bots.sort(key=lambda x: str(x["bot_id"]))
    candles = list(S.candles)
    if S.cur_bar is not None:
        candles.append(dict(S.cur_bar))
    return {
        "type": "state", "ts": now,
        "book": {"bids": S.bids, "asks": S.asks, "seq": S.book_seq, "snaps": S.snaps},
        "tape": list(S.tape),
        "candles": candles,
        "stats": {
            "trades": S.trades, "rejects": S.rejects, "volume": S.volume,
            "last": S.last_price, "best_bid": best_bid, "best_ask": best_ask, "spread": spread,
            "gateway": gw_writer is not None and not gw_writer.is_closing(),
        },
        "bots": bots,
    }


async def broadcaster():
    interval = 1.0 / max(PUSH_HZ, 1.0)
    while True:
        await asyncio.sleep(interval)
        if not clients:
            continue
        payload = json.dumps(snapshot_state())
        for ws in list(clients):
            try:
                await ws.send_text(payload)
            except Exception:
                clients.discard(ws)


# --------------------------------------------------------------------------------------
# FastAPI app (lifespan — no deprecated on_event)
# --------------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: Mcast(on_trades),   sock=make_mcast_socket(TRADES_PORT))
    await loop.create_datagram_endpoint(lambda: Mcast(on_snapshot), sock=make_mcast_socket(SNAP_PORT))
    await asyncio.start_server(handle_bot, "0.0.0.0", BOT_TCP_PORT)
    tasks = [asyncio.create_task(broadcaster()), asyncio.create_task(gateway_client())]
    print(f"[titan-ui] trades {MCAST_GROUP}:{TRADES_PORT}  snapshot {MCAST_GROUP}:{SNAP_PORT}  "
          f"(iface {MCAST_IFACE})  bots tcp :{BOT_TCP_PORT}  gateway {GW_HOST}:{GW_PORT}  "
          f"web http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title="Titan HFT UI", lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(HERE / "index.html")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(json.dumps(snapshot_state()))   # prime immediately
        while True:
            raw = await ws.receive_text()                  # inbound: manual order commands
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("type") == "order":
                ok, detail = await submit_manual_order(msg)
                await ws.send_text(json.dumps({"type": "order_ack", "ok": ok, "detail": detail}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        clients.discard(ws)


if __name__ == "__main__":
    uvicorn.run(app, host=HTTP_HOST, port=HTTP_PORT, log_level="warning")
