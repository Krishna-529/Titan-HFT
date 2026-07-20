#!/usr/bin/env bash
#
# bot.sh - build (and optionally run) the market-making bot. RELEASE, threaded client.
#
# The bot is an external client of a RUNNING titan-server, not part of the engine build.
# Bring the whole demo up in three terminals:
#
#   1) engine   :  bash server.sh && ./build/titan-server 9099
#   2) web UI   :  python3 ui/app.py                 # serves the dashboard + joins the feeds
#   3) this bot :  bash bot.sh run                   # posts liquidity, streams telemetry
#
# (Add aggressor flow -- ui/fake_bot.py's sniper, the manual UI entry box, or tests/tcp_blaster --
#  so the book actually trades and the MM's inventory/PnL move.)
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -pthread -Iinclude)
OUT=build/market_maker

mkdir -p build
echo "== building market_maker (RELEASE -O3 -pthread) =="
if "$CXX" "${FLAGS[@]}" bots/market_maker.cpp -o "$OUT"; then
    echo "COMPILE_OK -> $OUT"
else
    echo "COMPILE_FAILED"; exit 1
fi

if [[ "${1:-}" == "run" ]]; then
    echo "== running (Ctrl-C to stop) =="
    exec "./$OUT"
fi
