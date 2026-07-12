#!/usr/bin/env bash
#
# bench.sh - RELEASE benchmark build & run. -O3 -march=native -DNDEBUG, NO sanitizers
# (sanitizers would badly distort the latency numbers). Output tee'd to build/bench.log.
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -Iinclude)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== building matcher_bench (RELEASE: -O3 -march=native -DNDEBUG) =="
    if "$CXX" "${FLAGS[@]}" bench/matcher_bench.cpp -o build/matcher_bench; then
        echo "COMPILE_OK"
        echo "== running =="
        ./build/matcher_bench
        echo "RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/bench.log
