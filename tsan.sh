#!/usr/bin/env bash
#
# tsan.sh - build & run the ring stress tests under ThreadSanitizer.
# TSan is mutually exclusive with ASan (build.sh), so this is a SEPARATE target:
# it detects any missing release/acquire barrier as a data race on the ring slots.
#   * SPSC ring (spsc_ring_tests): 1 producer / 1 consumer, strict-order + zero-loss.
#   * MPSC ring (mpsc_ring_tests): 4 producers / 1 consumer, exactly-once (0 loss/dup/torn).
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -Wall -Wextra -Wpedantic -Iinclude -Itests
       -O2 -g -fsanitize=thread -fno-omit-frame-pointer -pthread)

# setarch -R disables ASLR for the process: works around TSan's "unexpected memory mapping"
# fatal on high-entropy kernels (Ubuntu 24.04 / WSL2).
mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1

    echo "== building spsc_ring_tests (ThreadSanitizer) =="
    if "$CXX" "${FLAGS[@]}" tests/spsc_ring_tests.cpp -o build/ring_tsan; then
        echo "COMPILE_OK"
        echo "== running spsc (TSan; ASLR disabled via setarch -R) =="
        setarch "$(uname -m)" -R ./build/ring_tsan
        echo "SPSC_RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi

    echo "== building mpsc_ring_tests (ThreadSanitizer) =="
    if "$CXX" "${FLAGS[@]}" tests/mpsc_ring_tests.cpp -o build/mpsc_tsan; then
        echo "COMPILE_OK"
        echo "== running mpsc (TSan; ASLR disabled via setarch -R) =="
        setarch "$(uname -m)" -R ./build/mpsc_tsan
        echo "MPSC_RUN_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/tsan.log
