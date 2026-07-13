#!/usr/bin/env bash
#
# bench_end_to_end.sh
# Wire-to-wire end-to-end throughput/latency baseline for titan-server.
#
#   TCP orders in  ->  gateway -> MpscRing -> sequencer -> ingress -> matcher -> egress
#                  ->  UDP multicast trade feed  ->  external Python listener
#
# Compiles the server in strict RELEASE (-O3 -march=native -DNDEBUG) with profiling hooks
# (-g -fno-omit-frame-pointer, for `perf`), then blasts N=1,000,000 orders and measures the
# wall-clock from the first TCP byte (blaster CLOCK_MONOTONIC T0) to the moment the listener
# receives the N-th matching TradeEvent (same system-wide CLOCK_MONOTONIC). Requires python3.
#
# Env overrides: TCP_PORT, ORDERS.
#
set -uo pipefail
cd "$(dirname "$0")"

TCP_PORT="${TCP_PORT:-9099}"
N="${ORDERS:-1000000}"
GROUP="239.1.1.1"; TPORT="30001"; IFACE="127.0.0.1"

LOG_DIR="$(mktemp -d)"
SRV_LOG="$LOG_DIR/server.log"; LST_LOG="$LOG_DIR/listener.log"; BLS_LOG="$LOG_DIR/blaster.log"
SRV=""; LST=""

cleanup() {
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    [ -n "$LST" ] && kill "$LST" 2>/dev/null
    rm -f titan.wal
    rm -rf "$LOG_DIR"
}
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 not found"; exit 0; }

echo "== building titan-server (RELEASE + frame pointers) =="
bash server.sh >/dev/null 2>&1 || { echo "FAIL: server build failed"; exit 1; }
[ -x build/titan-server ] || { echo "FAIL: titan-server missing"; exit 1; }

echo "== building tcp_blaster (RELEASE) =="
g++ -std=c++20 -O3 -march=native -DNDEBUG -g -fno-omit-frame-pointer -D_GNU_SOURCE -Iinclude \
    tests/tcp_blaster.cpp -o build/tcp_blaster || { echo "FAIL: blaster build failed"; exit 1; }

echo "== starting trade-feed listener (expect $N) =="
python3 tests/mc_listener.py --kind trades --group "$GROUP" --port "$TPORT" --iface "$IFACE" \
        --expect "$N" --idle 8 --deadline 120 --samples 1 > "$LST_LOG" 2>&1 &
LST=$!
sleep 0.7

echo "== starting titan-server on TCP $TCP_PORT =="
rm -f titan.wal
./build/titan-server "$TCP_PORT" > "$SRV_LOG" 2>&1 &
SRV=$!
sleep 1.2

echo "== blasting $N orders =="
./build/tcp_blaster "$TCP_PORT" "$N" > "$BLS_LOG" 2>&1
cat "$BLS_LOG"

wait "$LST"; LST_RC=$?; LST=""     # listener exits on N-th trade or idle/deadline
kill -INT "$SRV"; wait "$SRV"; SRV=""

echo "===== server ====="; cat "$SRV_LOG"
echo "===== listener ====="; grep -E "RESULT|STAMP" "$LST_LOG"

T0=$(grep -o "T0_NS=[0-9]*"    "$BLS_LOG" | cut -d= -f2)
SEND=$(grep -o "SEND_DONE_NS=[0-9]*" "$BLS_LOG" | cut -d= -f2)
HIT=$(grep -o "hit_ns=[0-9]*"  "$LST_LOG" | cut -d= -f2)
LAST=$(grep -o "last_ns=[0-9]*" "$LST_LOG" | cut -d= -f2)
RECS=$(grep -o "records=[0-9]*" "$LST_LOG" | tail -1 | cut -d= -f2)
REACHED=$(grep -o "reached=[0-9]*" "$LST_LOG" | cut -d= -f2)
UDP_SENT=$(grep -o "UDP sent=[0-9]*" "$SRV_LOG" | head -1 | cut -d= -f2)
UDP_DROP=$(grep -o "dropped=[0-9]*" "$SRV_LOG" | head -1 | cut -d= -f2)

echo "===== BASELINE ====="
awk -v N="$N" -v t0="$T0" -v send="$SEND" -v hit="$HIT" -v last="$LAST" -v recs="$RECS" \
    -v reached="$REACHED" -v usent="${UDP_SENT:-0}" -v udrop="${UDP_DROP:-0}" 'BEGIN {
    ingest_ms = (send - t0) / 1e6;
    printf "orders sent .............. %d\n", N;
    printf "server UDP sent/dropped .. %d / %d\n", usent, udrop;
    printf "listener trades received . %d  (%.2f%% of %d)\n", recs, 100.0*recs/N, N;
    printf "ingest (TCP accept) ...... %.2f ms  ->  %.2f M orders/s\n", ingest_ms, N/(ingest_ms*1e3);
    if (reached == 1 && hit > t0) {
        lat_ms = (hit - t0) / 1e6;
        printf "WIRE-TO-WIRE (1st byte -> %d-th trade received):\n", N;
        printf "  latency ................ %.2f ms\n", lat_ms;
        printf "  throughput ............. %.2f M orders/s  (%.2f M trades/s)\n", N/(lat_ms*1e3), N/(lat_ms*1e3);
    } else if (last > t0) {
        lat_ms = (last - t0) / 1e6;
        printf "WIRE-TO-WIRE (1st byte -> LAST delivered trade; target not fully received):\n";
        printf "  delivered .............. %d trades in %.2f ms\n", recs, lat_ms;
        printf "  throughput ............. %.2f M trades/s (delivered)\n", recs/(lat_ms*1e3);
        printf "  NOTE: %d trades lost on the loopback UDP receive path (rmem_max cushion); the\n", (usent-recs);
        printf "        engine/ingest rate above is the matching-logic baseline to optimize against.\n";
    }
}'
