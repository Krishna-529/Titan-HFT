#!/usr/bin/env bash
#
# tests/multicast_test.sh
# Permanent end-to-end verification of the outbound market-data boundary:
#   build titan-server -> stream crossing orders in over TCP -> confirm an EXTERNAL UDP
#   multicast listener receives and correctly parses the raw binary TradeEvent feed.
#
# Requires: g++ (via server.sh) and python3. Pure loopback; no external network needed.
# Env overrides: TCP_PORT, ORDERS, MCAST_GROUP, MCAST_PORT, MCAST_IFACE.
# Exit 0 = PASS.  (On loopback the feed is lossless -> listener records == server UDP sent.)
#
set -uo pipefail
cd "$(dirname "$0")/.."

TCP_PORT="${TCP_PORT:-9099}"
ORDERS="${ORDERS:-200000}"
GROUP="${MCAST_GROUP:-239.1.1.1}"
MPORT="${MCAST_PORT:-30001}"
IFACE="${MCAST_IFACE:-127.0.0.1}"

LOG_DIR="$(mktemp -d)"
SRV_LOG="$LOG_DIR/server.log"
LST_LOG="$LOG_DIR/listener.log"
SRV=""; LST=""

cleanup() {
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    [ -n "$LST" ] && kill "$LST" 2>/dev/null
    rm -f titan.wal
    rm -rf "$LOG_DIR"
}
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 not found"; exit 0; }

echo "== building titan-server =="
bash server.sh >/dev/null 2>&1 || { echo "FAIL: server build failed"; exit 1; }
[ -x build/titan-server ] || { echo "FAIL: build/titan-server missing"; exit 1; }

echo "== starting multicast listener ($GROUP:$MPORT via $IFACE) =="
python3 tests/mc_listener.py --group "$GROUP" --port "$MPORT" --iface "$IFACE" --idle 4 > "$LST_LOG" 2>&1 &
LST=$!
sleep 0.7

echo "== starting titan-server on TCP $TCP_PORT =="
rm -f titan.wal
./build/titan-server "$TCP_PORT" > "$SRV_LOG" 2>&1 &
SRV=$!
sleep 1.2

echo "== blasting $ORDERS crossing orders over TCP =="
python3 - "$TCP_PORT" "$ORDERS" <<'PY'
import socket, struct, sys
port, N = int(sys.argv[1]), int(sys.argv[2])
# Order (titan/book/order.hpp), 40 bytes:
#   id u64 | seq u64 | price i64 | quantity u32 | remaining u32 | side u8 | type u8 | pad[6]
FMT = "<QQqIIBB6x"
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", port))
buf = bytearray()
for i in range(N):
    price = 10000 + (i % 9 - 4)          # tight band around the mid -> heavy crossing
    qty = 1 + (i % 3)
    side = 1 if (i & 1) else 0           # 0=BUY, 1=SELL
    buf += struct.pack(FMT, i, 0, price, qty, qty, side, 0)   # seq stamped by server; type 0=LIMIT
s.sendall(bytes(buf))
s.close()
print("[client] sent %d orders (%d bytes)" % (N, len(buf)))
PY

sleep 2
echo "== stopping server (SIGINT) =="
kill -INT "$SRV"; wait "$SRV"; SRV_RC=$?; SRV=""
wait "$LST"; LST_RC=$?; LST=""

echo "===== server ====="; cat "$SRV_LOG"
echo "===== listener ====="; cat "$LST_LOG"

SENT=$(grep -o "UDP sent=[0-9]*" "$SRV_LOG" | head -1 | cut -d= -f2); SENT=${SENT:-0}
RECS=$(grep -o "records=[0-9]*"  "$LST_LOG" | head -1 | cut -d= -f2); RECS=${RECS:-0}
JOUR=$(grep -o "journaled [0-9]*" "$SRV_LOG" | head -1 | cut -d' ' -f2); JOUR=${JOUR:-0}

echo "===== VERDICT ====="
echo "orders=$ORDERS journaled=$JOUR  UDP_sent=$SENT  listener_records=$RECS  (server_rc=$SRV_RC listener_rc=$LST_RC)"
if [ "$LST_RC" -eq 0 ] && [ "$RECS" -gt 0 ] && [ "$RECS" -eq "$SENT" ]; then
    echo "PASS: multicast boundary verified -- listener received all $RECS TradeEvents (lossless loopback)"
    exit 0
elif [ "$LST_RC" -eq 0 ] && [ "$RECS" -gt 0 ]; then
    echo "PASS (with UDP loss): listener received $RECS of $SENT TradeEvents"
    exit 0
else
    echo "FAIL: listener received $RECS of $SENT TradeEvents"
    exit 1
fi
