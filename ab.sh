#!/usr/bin/env bash
#
# ab.sh - build & run the thermal-invariant A/B micro-benchmark.
# RELEASE: -O3 -march=native -DNDEBUG, NO sanitizers. Output tee'd to build/ab.log.
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -Iinclude -Ibench)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== building ab_bench (RELEASE) =="
    if "$CXX" "${FLAGS[@]}" bench/ab_bench.cpp -o build/ab_bench; then
        echo "COMPILE_OK"
        echo "== running =="
        ./build/ab_bench
        echo "RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/ab.log
