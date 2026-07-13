#!/usr/bin/env bash
#
# server.sh - build the production titan-server executable.
# RELEASE: -O3 -march=native -DNDEBUG, no sanitizers, -pthread. Output tee'd to build/server.log.
# _GNU_SOURCE exposes accept4/epoll (the gateway header also self-defines it; belt-and-suspenders).
#
set -uo pipefail
cd "$(dirname "$0")"

CXX="${CXX:-g++}"
# RELEASE + profiling hooks: -g -fno-omit-frame-pointer keep frame pointers & symbols so the
# binary is profileable under `perf` (negligible runtime cost on x86-64) without leaving RELEASE.
FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -g -fno-omit-frame-pointer -D_GNU_SOURCE -Iinclude -pthread)

mkdir -p build
{
    echo "== compiler =="
    "$CXX" --version | head -1
    echo "== building titan-server (RELEASE) =="
    if "$CXX" "${FLAGS[@]}" src/main.cpp -o build/titan-server; then
        echo "COMPILE_OK -> build/titan-server"
    else
        echo "COMPILE_FAILED"
    fi
} 2>&1 | tee build/server.log
