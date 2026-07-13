#!/usr/bin/env bash
#
# tests/multicast_test.sh
# Permanent end-to-end verification of the DUAL outbound market-data boundary:
#   build titan-server -> stream crossing orders in over TCP -> confirm two EXTERNAL UDP
#   multicast listeners receive & parse both feeds:
#     * INCREMENTAL trade feed  (TradeEvent structs)  on MCAST_PORT
#     * L2 SNAPSHOT feed        (SnapshotBuffer)       on MCAST_SNAP_PORT (magic + checksum verified)
#
# Requires: g++ (via server.sh) and python3. Pure loopback; no external network needed.
# Env overrides: TCP_PORT, ORDERS, MCAST_GROUP, MCAST_PORT, MCAST_SNAP_PORT, MCAST_IFACE.
# Exit 0 = PASS (both feeds delivered valid data).
#
set -uo pipefail
cd "$(dirname "$0")/.."

TCP_PORT="${TCP_PORT:-9099}"
ORDERS="${ORDERS:-200000}"
GROUP="${MCAST_GROUP:-239.1.1.1}"
TPORT="${MCAST_PORT:-30001}"
SPORT="${MCAST_SNAP_PORT:-30002}"
IFACE="${MCAST_IFACE:-127.0.0.1}"

LOG_DIR="$(mktemp -d)"
SRV_LOG="$LOG_DIR/server.log"; TRD_LOG="$LOG_DIR/trades.log"; SNP_LOG="$LOG_DIR/snap.log"
SRV=""; TRD=""; SNP=""

cleanup() {
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    [ -n "$TRD" ] && kill "$TRD" 2>/dev/null
    [ -n "$SNP" ] && kill "$SNP" 2>/dev/null
    rm -f titan.wal
    rm -rf "$LOG_DIR"
}
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 not found"; exit 0; }

echo "== building titan-server =="
bash server.sh >/dev/null 2>&1 || { echo "FAIL: server build failed"; exit 1; }
[ -x build/titan-server ] || { echo "FAIL: build/titan-server missing"; exit 1; }

echo "== starting listeners (trades $GROUP:$TPORT, snapshot $GROUP:$SPORT) =="
python3 tests/mc_listener.py --kind trades   --group "$GROUP" --port "$TPORT" --iface "$IFACE" --idle 4 > "$TRD_LOG" 2>&1 &
TRD=$!
python3 tests/mc_listener.py --kind snapshot --group "$GROUP" --port "$SPORT" --iface "$IFACE" --idle 4 > "$SNP_LOG" 2>&1 &
SNP=$!
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
FMT = "<QQqIIBB6x"   # id u64 | seq u64 | price i64 | quantity u32 | remaining u32 | side u8 | type u8 | pad[6]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", port))
buf = bytearray()
for i in range(N):
    price = 10000 + (i % 9 - 4)          # tight band -> heavy crossing
    qty = 1 + (i % 3)
    side = 1 if (i & 1) else 0
    buf += struct.pack(FMT, i, 0, price, qty, qty, side, 0)
s.sendall(bytes(buf)); s.close()
print("[client] sent %d orders (%d bytes)" % (N, len(buf)))
PY

sleep 2
echo "== stopping server (SIGINT) =="
kill -INT "$SRV"; wait "$SRV"; SRV_RC=$?; SRV=""
wait "$TRD"; TRD_RC=$?; TRD=""
wait "$SNP"; SNP_RC=$?; SNP=""

echo "===== server ====="; cat "$SRV_LOG"
echo "===== trades feed ====="; cat "$TRD_LOG"
echo "===== snapshot feed ====="; cat "$SNP_LOG"

UDP_SENT=$(grep -o "UDP sent=[0-9]*" "$SRV_LOG" | head -1 | cut -d= -f2); UDP_SENT=${UDP_SENT:-0}
SNAP_SENT=$(grep -o "snapshots_sent=[0-9]*" "$SRV_LOG" | head -1 | cut -d= -f2); SNAP_SENT=${SNAP_SENT:-0}
TRECS=$(grep -o "records=[0-9]*" "$TRD_LOG" | head -1 | cut -d= -f2); TRECS=${TRECS:-0}
SVALID=$(grep -o "valid=[0-9]*" "$SNP_LOG" | head -1 | cut -d= -f2); SVALID=${SVALID:-0}
JOUR=$(grep -o "journaled [0-9]*" "$SRV_LOG" | head -1 | cut -d' ' -f2); JOUR=${JOUR:-0}

echo "===== VERDICT ====="
echo "orders=$ORDERS journaled=$JOUR | trades: UDP_sent=$UDP_SENT listener_records=$TRECS (rc=$TRD_RC)"
echo "                               | snapshot: server_sent=$SNAP_SENT listener_valid=$SVALID (rc=$SNP_RC)"
if [ "$TRD_RC" -eq 0 ] && [ "$TRECS" -gt 0 ] && [ "$SNP_RC" -eq 0 ] && [ "$SVALID" -gt 0 ]; then
    echo "PASS: BOTH feeds verified -- $TRECS trades + $SVALID valid L2 snapshots received"
    exit 0
else
    echo "FAIL: trades_records=$TRECS (rc=$TRD_RC), snapshot_valid=$SVALID (rc=$SNP_RC)"
    exit 1
fi
