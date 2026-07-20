#!/usr/bin/env bash
#
# profile.sh - CPU hotspot profile of the PIN matching engine.
#
# TARGET: bench/matcher_bench.cpp, built as a DEDICATED PROFILING BINARY with -fno-inline.
#
# ---------------------------------------------------------------------------------------
# WHY -fno-inline IS MANDATORY HERE
#
# At -O3 the entire crossing path inlines into main(). The previous profile
# (profile_report.txt, v1.4.9) attributed 76.37% of self time to `main` -- which is worth
# exactly nothing for attribution. -fno-inline preserves function boundaries so every
# symbol (RBPriceIndex::find, PIN_Node::at, OrderBookT::cancel, ...) reports its own self
# time and call count.
#
# STATE THE TRADE-OFF PLAINLY: -fno-inline changes the machine code being measured. Cross-
# function optimisation is gone, call overhead is real, absolute ns/op is inflated. Use
# this profile to learn WHERE time goes (relative attribution). NEVER quote its absolute
# numbers -- for those use bench.sh, which builds the true -O3 inlined binary.
#
# When reading the output, also subtract the BENCHMARK SCAFFOLDING before computing engine
# percentages. In the v1.4.9 run that was ~11.6% of samples and none of it is engine code:
#   vector<Op>::begin 4.07  _Destroy_aux 2.71  make_limit 1.54  push_back 1.27
#   mt19937::operator() 1.00  operator==(iterator) 0.63  forward<bool> 0.36
# ---------------------------------------------------------------------------------------
#
# TWO BACK ENDS, auto-selected:
#   perf  (preferred) -- sampling, no instrumentation, flamegraph-ready, sees all threads
#   gprof (fallback)  -- ITIMER_PROF + -pg instrumentation; reliably samples only the MAIN
#                        thread, which is fine here because matcher_bench does all matching
#                        on main, with no syscall/UDP noise.
#
# perf is CURRENTLY UNAVAILABLE on this machine: kernel 6.18.33.2-microsoft-standard-WSL2
# has no matching linux-tools package and no perf binary is installed. The perf path below
# is kept wired up and correct for a native-Linux box or a self-built perf -- see the
# "GETTING perf ON WSL2" note at the bottom of this file.
#
# USAGE
#   bash profile.sh            # auto: perf if usable, else gprof
#   bash profile.sh perf       # force perf   (fails loudly if unavailable)
#   bash profile.sh gprof      # force gprof
#   bash profile.sh build      # build both profiling targets, run nothing
#
# OUTPUT
#   perf path : perf.data + perf_report.txt
#   gprof path: build/gmon.out + profile_report.txt
#
set -uo pipefail
cd "$(dirname "$0")"

MODE="${1:-auto}"
CXX="${CXX:-g++}"
mkdir -p build

# Dedicated profiling flags.
#   -O3 -march=native -DNDEBUG : match the release build we actually ship
#   -fno-inline (+ -called-once): restore symbol boundaries -> real attribution
#   -g                          : line info, required for a useful flamegraph
#   -fno-omit-frame-pointer     : lets perf unwind with --call-graph fp (cheap, no DWARF)
PROF_FLAGS=(-std=c++20 -O3 -march=native -DNDEBUG -g
            -fno-inline -fno-inline-functions-called-once
            -fno-omit-frame-pointer -D_DEFAULT_SOURCE -Iinclude)

PERF_BIN=build/matcher_bench_prof     # sampling profiler: no instrumentation needed
GPROF_BIN=build/matcher_prof          # gprof: additionally needs -pg

build_perf_target() {
    echo "== building profiling target (perf): $PERF_BIN =="
    echo "   $CXX ${PROF_FLAGS[*]} bench/matcher_bench.cpp -o $PERF_BIN"
    "$CXX" "${PROF_FLAGS[@]}" bench/matcher_bench.cpp -o "$PERF_BIN" \
        || { echo "FAIL: build $PERF_BIN"; exit 1; }
    echo "   ok"
}

build_gprof_target() {
    echo "== building profiling target (gprof): $GPROF_BIN =="
    echo "   $CXX ${PROF_FLAGS[*]} -pg bench/matcher_bench.cpp -o $GPROF_BIN"
    "$CXX" "${PROF_FLAGS[@]}" -pg bench/matcher_bench.cpp -o "$GPROF_BIN" \
        || { echo "FAIL: build $GPROF_BIN"; exit 1; }
    echo "   ok"
}

perf_works() {
    command -v perf >/dev/null 2>&1 || return 1
    perf record -o /tmp/.titan_perftest.data -- true >/dev/null 2>&1 || return 1
    rm -f /tmp/.titan_perftest.data
    return 0
}

run_perf() {
    build_perf_target
    echo "== perf record: $PERF_BIN =="
    rm -f perf.data
    perf record -F 2999 --call-graph fp -o perf.data -- "./$PERF_BIN" || { echo "FAIL: perf record"; exit 1; }
    perf report --stdio --no-children -i perf.data > perf_report.txt 2>/dev/null
    echo "== top of perf_report.txt =="
    head -40 perf_report.txt
    echo
    echo "flamegraph:  perf script -i perf.data | stackcollapse-perf.pl | flamegraph.pl > flame.svg"
}

run_gprof() {
    build_gprof_target
    # Profile BOTH cancel-path variants of the SAME binary so the flat-profile shares are
    # directly comparable (no cross-build drift). `off` = plain cancel; `on` = software-
    # prefetched cancel. gmon.out is rewritten per run, so gprof-ify each immediately.
    for mode in off on; do
        echo "== running (gprof instrumented, cancel=$mode) =="
        ( cd build && "./$(basename "$GPROF_BIN")" "$mode" >"matcher_prof_${mode}.txt" 2>&1 )
        tail -2 "build/matcher_prof_${mode}.txt"
        gprof -p --demangle "$GPROF_BIN" build/gmon.out > "profile_report_${mode}.txt" 2>/dev/null
        echo "-- flat profile (self time), top 12, cancel=$mode --"
        sed -n '6,18p' "profile_report_${mode}.txt"
        echo
    done
    cp -f profile_report_on.txt profile_report.txt   # canonical = the optimized path
    echo "== compare: profile_report_off.txt  vs  profile_report_on.txt =="
}

case "$MODE" in
    build) build_perf_target; build_gprof_target ;;
    perf)  perf_works || { echo "FAIL: perf unavailable (see notes at the bottom of profile.sh)"; exit 1; }
           run_perf ;;
    gprof) run_gprof ;;
    auto)  if perf_works; then run_perf
           else echo "== perf unavailable -> gprof fallback on matcher_bench =="; run_gprof; fi ;;
    *)     echo "usage: bash profile.sh [auto|perf|gprof|build]"; exit 2 ;;
esac

# ---------------------------------------------------------------------------------------
# GETTING perf ON WSL2
#
# `apt install linux-tools-generic` installs a perf built for the Ubuntu kernel, which
# refuses to run against Microsoft's WSL2 kernel (version mismatch). The working route is
# to build perf from the matching WSL2 kernel source:
#
#   sudo apt install -y build-essential flex bison libelf-dev libdw-dev libunwind-dev \
#                       libslang2-dev python3-dev libssl-dev libzstd-dev
#   git clone --depth 1 -b linux-msft-wsl-6.6.y \
#       https://github.com/microsoft/WSL2-Linux-Kernel.git /tmp/WSL2-Linux-Kernel
#   make -C /tmp/WSL2-Linux-Kernel/tools/perf -j"$(nproc)"
#   sudo cp /tmp/WSL2-Linux-Kernel/tools/perf/perf /usr/local/bin/perf
#
# (Pick the branch matching `uname -r`.) Then unprivileged user-space sampling needs:
#   sudo sysctl -w kernel.perf_event_paranoid=1
#
# Until then the gprof fallback is the source of truth, and it is adequate: matcher_bench
# is single-threaded on main, which is exactly what gprof samples reliably.
# ---------------------------------------------------------------------------------------
