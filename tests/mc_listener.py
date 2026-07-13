#!/usr/bin/env python3
"""Titan multicast market-data listener / verifier.

Joins one of the two titan feeds, parses the raw binary structs off the wire, and exits
GRACEFULLY on either condition:
  * --expect N reached   -> stop once N records/snapshots have arrived (deterministic run), or
  * --idle S elapsed     -> stop after S seconds of silence (feed drained / server gone).
A hard --deadline caps total runtime so it can never hang.

  --kind trades   : incremental TradeEvent feed (40-byte structs, many per datagram).
  --kind snapshot : L2 SnapshotBuffer feed (one snapshot per datagram: 64-byte header + levels);
                    validates magic + recomputes the level checksum (torn / corrupt detection).

Exit code: 0 = success (>=1 valid record/snapshot, none malformed, and >=--expect if given); 1 else.
"""
import argparse
import socket
import struct
import sys
import time

MASK64 = (1 << 64) - 1

# TradeEvent (titan/book/trade_event.hpp), 40 bytes, little-endian:
#   taker_id u64 | maker_id u64 | price i64 | feed_seq u64 | quantity u32 | taker_side u8 | status u8 | pad[2]
TRADE_FMT = "<QQqQIBB2x"
TRADE_SZ = struct.calcsize(TRADE_FMT)  # 40
STATUS = {0: "FILL", 1: "REJECT"}

# SnapshotHeader (titan/book/snapshot.hpp), 64 bytes:
#   magic u64 | version u32 | level_count u32 | feed_seq u64 | bid_levels u32 | ask_levels u32 | checksum u64 | reserved[24]
SNAP_HDR_FMT = "<QIIQIIQ24x"
SNAP_HDR_SZ = struct.calcsize(SNAP_HDR_FMT)  # 64
# SnapshotLevel, 24 bytes: price i64 | total_qty u64 | order_count u32 | side u8 | pad[3]
SNAP_LVL_FMT = "<qQIB3x"
SNAP_LVL_SZ = struct.calcsize(SNAP_LVL_FMT)  # 24
SNAPSHOT_MAGIC = 0x544954414E534E50  # "TITANSNP"


def make_socket(group, port, iface):
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
    except OSError:
        pass
    rx.bind(("", port))
    mreq = socket.inet_aton(group) + socket.inet_aton(iface)
    rx.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    return rx


def run_trades(rx, args):
    datagrams = records = malformed = total_bytes = 0
    samples = []
    first_ns = hit_ns = last_ns = 0
    start = time.time()
    while True:
        if time.time() - start > args.deadline:
            break
        try:
            data, _ = rx.recvfrom(65535)
        except socket.timeout:
            break
        last_ns = time.monotonic_ns()               # wire-to-wire clock (system-wide CLOCK_MONOTONIC)
        if first_ns == 0:
            first_ns = last_ns
        datagrams += 1
        total_bytes += len(data)
        if len(data) == 0 or len(data) % TRADE_SZ != 0:
            malformed += 1
            continue
        n = len(data) // TRADE_SZ
        records += n
        for k in range(n):
            if len(samples) >= args.samples:
                break
            samples.append(struct.unpack_from(TRADE_FMT, data, k * TRADE_SZ))
        if args.expect and records >= args.expect:
            hit_ns = last_ns
            break

    print("[trades] RESULT datagrams=%d records=%d bytes=%d malformed=%d"
          % (datagrams, records, total_bytes, malformed), flush=True)
    # STAMP line (nanoseconds, CLOCK_MONOTONIC): the benchmark script diffs hit_ns/last_ns vs the
    # blaster's T0_NS for wire-to-wire latency. reached=1 iff the --expect count was received.
    print("[trades] STAMP first_ns=%d hit_ns=%d last_ns=%d records=%d expect=%d reached=%d"
          % (first_ns, hit_ns, last_ns, records, args.expect, 1 if (args.expect and records >= args.expect) else 0),
          flush=True)
    for s in samples:
        print("[trades] TradeEvent taker=%d maker=%d price=%d feed_seq=%d qty=%d side=%d status=%s"
              % (s[0], s[1], s[2], s[3], s[4], s[5], STATUS.get(s[6], s[6])), flush=True)
    return records > 0 and malformed == 0 and (args.expect == 0 or records >= args.expect)


def run_snapshot(rx, args):
    snaps = valid = bad = 0
    seqs = []
    samples = []
    start = time.time()
    while True:
        if time.time() - start > args.deadline:
            break
        try:
            data, _ = rx.recvfrom(65535)
        except socket.timeout:
            break
        snaps += 1
        if len(data) < SNAP_HDR_SZ:
            bad += 1
            continue
        magic, ver, lvl_count, feed_seq, bids, asks, checksum = struct.unpack_from(SNAP_HDR_FMT, data, 0)
        if magic != SNAPSHOT_MAGIC or len(data) < SNAP_HDR_SZ + lvl_count * SNAP_LVL_SZ:
            bad += 1
            continue
        chk = 0
        first_lvl = None
        for i in range(lvl_count):
            price, total_qty, order_count, side = struct.unpack_from(SNAP_LVL_FMT, data, SNAP_HDR_SZ + i * SNAP_LVL_SZ)
            chk = (chk + (price & MASK64) + total_qty + order_count + side) & MASK64
            if first_lvl is None:
                first_lvl = (price, total_qty, order_count, side)
        if chk != checksum:
            bad += 1
            continue
        valid += 1
        if len(seqs) < 1000000:
            seqs.append(feed_seq)
        if len(samples) < args.samples:
            samples.append((feed_seq, lvl_count, bids, asks, first_lvl))
        if args.expect and valid >= args.expect:
            break

    monotonic = all(seqs[i] <= seqs[i + 1] for i in range(len(seqs) - 1))
    print("[snapshot] RESULT datagrams=%d valid=%d bad=%d feed_seq_monotonic=%s range=[%s..%s]"
          % (snaps, valid, bad, monotonic,
             seqs[0] if seqs else "-", seqs[-1] if seqs else "-"), flush=True)
    for feed_seq, lc, b, a, lvl in samples:
        print("[snapshot] @feed_seq=%d levels=%d (bid=%d ask=%d) first=%s" % (feed_seq, lc, b, a, lvl), flush=True)
    return valid > 0 and bad == 0 and monotonic and (args.expect == 0 or valid >= args.expect)


def main():
    ap = argparse.ArgumentParser(description="Titan multicast feed listener/verifier")
    ap.add_argument("--kind", choices=["trades", "snapshot"], default="trades")
    ap.add_argument("--group", default="239.1.1.1")
    ap.add_argument("--port", type=int, default=30001)
    ap.add_argument("--iface", default="127.0.0.1")
    ap.add_argument("--expect", type=int, default=0)
    ap.add_argument("--idle", type=float, default=4.0)
    ap.add_argument("--deadline", type=float, default=60.0)
    ap.add_argument("--samples", type=int, default=3)
    args = ap.parse_args()

    rx = make_socket(args.group, args.port, args.iface)
    rx.settimeout(args.idle)
    print("[%s] joined %s:%d via %s (idle=%.1fs)" % (args.kind, args.group, args.port, args.iface, args.idle), flush=True)

    ok = run_trades(rx, args) if args.kind == "trades" else run_snapshot(rx, args)
    rx.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
