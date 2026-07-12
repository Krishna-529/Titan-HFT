#!/usr/bin/env bash
#
# tsan.sh - build & run the SPSC ingress-ring stress test under ThreadSanitizer.
# TSan is mutually exclusive with ASan (build.sh), so this is a SEPARATE target:
# it detects any missing release/acquire barrier as a data race on the ring slots.
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -Wall -Wextra -Wpedantic -Iinclude -Itests
       -O2 -g -fsanitize=thread -fno-omit-frame-pointer -pthread)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== building spsc_ring_tests (ThreadSanitizer) =="
    if "$CXX" "${FLAGS[@]}" tests/spsc_ring_tests.cpp -o build/ring_tsan; then
        echo "COMPILE_OK"
        # setarch -R disables ASLR for the process: works around TSan's
        # "unexpected memory mapping" fatal on high-entropy kernels (Ubuntu 24.04 / WSL2).
        echo "== running (TSan; ASLR disabled via setarch -R) =="
        setarch "$(uname -m)" -R ./build/ring_tsan
        echo "RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/tsan.log
