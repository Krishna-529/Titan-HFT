#!/usr/bin/env bash
#
# pipeline.sh - build & run the Sequencer -> IngressRing -> Matcher pipeline
# throughput benchmark. RELEASE: -O3 -march=native -DNDEBUG, no sanitizers, -pthread.
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -Iinclude -pthread)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== building pipeline_bench (RELEASE) =="
    if "$CXX" "${FLAGS[@]}" bench/pipeline_bench.cpp -o build/pipeline_bench; then
        echo "COMPILE_OK"
        echo "== running =="
        ./build/pipeline_bench
        echo "RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/pipeline.log
