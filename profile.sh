#!/usr/bin/env bash
#
# profile.sh - CPU hotspot profile of the PIN matching engine.
#
# Prefers Linux `perf` (whole titan-server under the 1M-order blaster, --call-graph fp).
# Falls back to gprof on the single-threaded matcher_bench when perf is unavailable or broken
# (e.g. WSL2 whose Microsoft kernel has no matching linux-tools package). gprof's ITIMER_PROF
# reliably samples only the MAIN thread, so we profile matcher_bench (all matching on main) --
# it exercises the identical crossing + RBPriceIndex code paths without syscall/UDP noise.
#
# Output: perf_report.txt (perf path) or profile_report.txt (gprof path).
#
set -uo pipefail
cd "$(dirname "$0")"
PORT="${TCP_PORT:-9099}"
N="${ORDERS:-1000000}"

perf_works() {
    command -v perf >/dev/null 2>&1 || return 1
    perf record -o /tmp/.titan_perftest.data -- true >/dev/null 2>&1 || return 1
    rm -f /tmp/.titan_perftest.data
    return 0
}

if perf_works; then
    echo "== perf path: whole titan-server under 1M-order blast =="
    bash server.sh >/dev/null 2>&1
    g++ -std=c++20 -O3 -march=native -DNDEBUG -g -fno-omit-frame-pointer -D_GNU_SOURCE -Iinclude \
        tests/tcp_blaster.cpp -o build/tcp_blaster
    rm -f titan.wal perf.data
    ./build/titan-server "$PORT" >/tmp/titan_srv.log 2>&1 &
    SRV=$!; sleep 1.0
    perf record -F 999 --call-graph fp -o perf.data -p "$SRV" >/dev/null 2>&1 &
    PERF=$!; sleep 0.3
    ./build/tcp_blaster "$PORT" "$N"
    sleep 1.0
    kill -INT "$SRV"; wait "$SRV" 2>/dev/null
    kill -INT "$PERF" 2>/dev/null; wait "$PERF" 2>/dev/null
    perf report --stdio --no-children -i perf.data > perf_report.txt 2>/dev/null
    rm -f titan.wal
    echo "== top of perf_report.txt =="
    head -40 perf_report.txt
else
    echo "== perf unavailable -> gprof on single-threaded matcher_bench =="
    # -O2 (not -O3) retains function boundaries for attribution; -pg instruments; NDEBUG = release.
    g++ -std=c++20 -O2 -march=native -DNDEBUG -g -pg -fno-omit-frame-pointer -Iinclude \
        bench/matcher_bench.cpp -o build/matcher_prof || { echo "FAIL: build"; exit 1; }
    ( cd build && ./matcher_prof >matcher_prof_run.txt 2>&1 )   # gmon.out lands in build/
    tail -3 build/matcher_prof_run.txt
    gprof -p --demangle build/matcher_prof build/gmon.out > profile_report.txt 2>/dev/null
    echo "== flat profile (self time), top 25 =="
    # columns: %time cumulative self calls ... name
    sed -n '1,30p' profile_report.txt
fi
