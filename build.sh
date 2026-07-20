#!/usr/bin/env bash
#
# build.sh - direct g++ build (no cmake needed yet). Compiles the unit tests with
# ASan + UBSan and runs them. All output is tee'd to build/build.log.
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
FLAGS=(-std=c++20 -Wall -Wextra -Wpedantic -Iinclude -D_DEFAULT_SOURCE -pthread
       -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== compiling tests (ASan+UBSan) =="
    if "$CXX" "${FLAGS[@]}" tests/tests.cpp tests/matcher_tests.cpp tests/rb_tree_tests.cpp tests/journaler_tests.cpp tests/sequencer_tests.cpp tests/tcp_gateway_tests.cpp tests/price_validation_tests.cpp -o build/titan_tests; then
        echo "COMPILE_OK"
        echo "== running tests =="
        ./build/titan_tests
        echo "TEST_EXIT=$?"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/build.log
