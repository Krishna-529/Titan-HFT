#!/usr/bin/env python3
"""Titan multicast market-data listener / verifier.

Joins the TradeEvent multicast feed, parses the raw 32-byte binary structs off the wire,
and exits GRACEFULLY on either condition:
  * --expect N reached   -> stop as soon as N records have arrived (deterministic run), or
  * --idle S elapsed     -> stop after S seconds of silence (feed drained / server gone).
A hard --deadline caps total runtime so it can never hang.

Exit code: 0 = success (>=1 record, none malformed, and >=--expect if given); 1 otherwise.
This is the permanent counterpart to src/titan-server's UdpPublisher; keeping it in-repo
makes the multicast boundary re-verifiable at any time (see tests/multicast_test.sh).
"""
import argparse
import socket
import struct
import sys
import time

# TradeEvent (titan/book/trade_event.hpp), 32 bytes, little-endian:
#   taker_id u64 | maker_id u64 | price i64 | quantity u32 | taker_side u8 | status u8 | pad[2]
FMT = "<QQqIBB2x"
SZ = struct.calcsize(FMT)  # 32
STATUS = {0: "FILL", 1: "REJECT"}


def main():
    ap = argparse.ArgumentParser(description="Titan multicast TradeEvent listener/verifier")
    ap.add_argument("--group", default="239.1.1.1", help="multicast group")
    ap.add_argument("--port", type=int, default=30001, help="multicast UDP port")
    ap.add_argument("--iface", default="127.0.0.1", help="interface IP to join on")
    ap.add_argument("--expect", type=int, default=0, help="stop once this many records arrive (0 = until idle)")
    ap.add_argument("--idle", type=float, default=4.0, help="stop after this many seconds of silence")
    ap.add_argument("--deadline", type=float, default=60.0, help="hard overall runtime cap (s)")
    ap.add_argument("--samples", type=int, default=3, help="decoded sample records to print")
    args = ap.parse_args()

    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
    except OSError:
        pass
    rx.bind(("", args.port))
    mreq = socket.inet_aton(args.group) + socket.inet_aton(args.iface)
    rx.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    rx.settimeout(args.idle)

    datagrams = records = malformed = total_bytes = 0
    samples = []
    start = time.time()
    print("[listener] joined %s:%d via %s (struct=%dB, expect=%s, idle=%.1fs)"
          % (args.group, args.port, args.iface, SZ, args.expect or "any", args.idle), flush=True)

    while True:
        if time.time() - start > args.deadline:
            break
        try:
            data, _addr = rx.recvfrom(65535)
        except socket.timeout:
            break  # graceful: idle for --idle seconds -> feed drained
        datagrams += 1
        total_bytes += len(data)
        if len(data) == 0 or len(data) % SZ != 0:
            malformed += 1
            continue
        n = len(data) // SZ
        records += n
        for k in range(n):
            if len(samples) >= args.samples:
                break
            samples.append(struct.unpack_from(FMT, data, k * SZ))
        if args.expect and records >= args.expect:
            break  # graceful: got the expected count

    rx.close()
    print("[listener] RESULT datagrams=%d records=%d bytes=%d malformed=%d"
          % (datagrams, records, total_bytes, malformed), flush=True)
    for s in samples:
        print("[listener] sample TradeEvent taker=%d maker=%d price=%d qty=%d side=%d status=%s"
              % (s[0], s[1], s[2], s[3], s[4], STATUS.get(s[5], s[5])), flush=True)

    ok = records > 0 and malformed == 0 and (args.expect == 0 or records >= args.expect)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
