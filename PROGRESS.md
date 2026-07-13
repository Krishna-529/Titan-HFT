# Titan HFT v1 (C++) — Progress

A bare-metal, zero-crash HFT matching core and Disruptor pipeline. This file tracks
the Flash One architecture against what is actually built, and is updated as
components land.

**Legend:** ✅ done & tested · 🟡 partial / foundation / mock · ❌ not started

## Component status

| Component (diagram) | Status | Done in | Date | Notes |
|---|---|---|---|---|
| UI / Localhost / Sim 1 / Sim 2 | ❌ | — | — | No UI/simulator in C++ v1 |
| TCP + Kernel Bypassing | 🟡 | (uncommitted) | 2026-07-13 | TCP ingress done; **kernel-bypass (io_uring/DPDK) not started** |
| **Gateway** | ✅ | v1.3.4 (batched v1.3.6; MPSC v1.4.2; multi v1.4.3) | 2026-07-13 | Edge-triggered **epoll** TCP ingress (`net/tcp_gateway.hpp`): `accept4`+`SOCK_NONBLOCK`, `TCP_NODELAY`, `eventfd` stop, RAII fd cleanup. **Batched recv** (4 KB/syscall). Pure I/O: pushes parsed Orders onto the inbound **MpscRing** (zero-drop `try_publish`), no seq/journal. **Multi-gateway fan-in**: N listeners (one epoll thread each) → 1 shared MpscRing, proven zero-loss under concurrent contention |
| **Ingress Queue (LMAX Disruptor)** | ✅ | v1.2.0/1.4.1 (wired v1.4.2) | 2026-07-13 | SPSC ring (`spsc_ring.hpp`): batch-drain + prefetch, TSan-proven. **MPSC ring** (`mpsc_ring.hpp`): Vyukov CAS-claim + per-cell published-seq, TSan-proven (4P/1C, 1M, exactly-once). **Both wired into `titan-server`**: Gateway→MpscRing→Sequencer→IngressRing(SPSC)→Matcher |
| **Matching Engine (using PIN)** | ✅ | v1.1.2–v1.1.4 (+hardening v1.3.3) | 2026-07-13 | PIN book, dense-slab O(1) id index, intrusive RB-tree price index, LIMIT/MARKET/IOC + partial fills. **Zero-crash under pool/arena exhaustion**: `submit` degrades to a `TRADE_STATUS_REJECTED` event (never terminates); construction is fail-fast |
| **Sequencer** | ✅ | v1.3.2 (run-loop v1.4.2) | 2026-07-13 | Real component (`pipeline/sequencer.hpp`): seq-stamp → **write-ahead journal** → publish, zero-drop backpressure + durability cadence. `publish()` (recovery/tests) + active `run(mpsc, stop_flag)` drain loop (server T2: MpscRing → IngressRing, clean-shutdown flag) |
| **Journaler** | ✅ | v1.3.1 (+cadence v1.3.2) | 2026-07-13 | mmap WAL, binary, 64B `FileHeader` ABI tripwire. Hot-path append = memcpy (no syscall). **Deferred durability**: `MS_ASYNC`/batch + `MS_SYNC` every K=64 (`sync_dirty` page-aligned tail). Append ~free; sync is the cost |
| **Egress Queue (LMAX Disruptor)** | 🟡 | v1.2.4 (+batch) | 2026-07-13 | **Wired**: matcher **batch-publishes** trades (thread-local buffer → `publish_batch`, one release-store/batch, zero-drop). SPSC |
| Publish Data / Trade Reporter | ✅ | v1.4.4 | 2026-07-13 | **UDP multicast** market data (`net/udp_publisher.hpp`): non-blocking socket, `IP_MULTICAST_IF/TTL/LOOP`, raw-binary TradeEvents (zero JSON), MTU-safe datagram chunking (45/dgram), best-effort drop-on-full. Publisher(T4) drains egress → `udp.publish(batch)`. Verified: external listener decoded 122,223 TradeEvents, zero loss |
| UDP / TCP feedback → UI | 🟡 | v1.4.4 | 2026-07-13 | Outbound UDP market-data multicast live (see above). UI/simulator consumer + TCP order-ack feedback still absent |
| **Power-failure recovery** | 🟡 | v1.3.2 | 2026-07-13 | `replay()` rebuilds book from WAL; torn-tail boundary via `seq==base+i` invariant (id-scheme & `count`-independent); empty-WAL no-op; idempotent. Bounded loss window between MS_SYNC checkpoints |
| **Server `main()` executable** | 🟡 | v1.3.6 (4-thread v1.4.2; multi-gw v1.4.3) | 2026-07-13 | `src/main.cpp` → `titan-server` (`server.sh`, RELEASE). **N-gateway** topology: `titan-server <port...>` → k gateway threads → 1 MpscRing → Sequencer → Ingress → Matcher → Egress → Publisher. SIGINT/SIGTERM **cascading** drain (stop+join all gateways → drain). Concurrent 2×125k blast: **exactly 250k journaled, zero loss**, exit 0. Single-symbol, no config file |

## Verified performance (WSL2, g++ 13, -O3 -march=native)

| Path | ns/msg | throughput |
|---|---|---|
| Matcher alone — deep power-law book | ~180 | ~5.6 M/s |
| Matcher alone — hot small book | ~47–77 | ~13–21 M/s |
| **2-thread** (Sequencer → Ingress → Matcher) | ~43–67 | **~15–23 M/s** |
| **3-thread** (+ Egress → Publisher, batch-publish) | ~72 | **~14–22 M/s** |

Absolute ns drift with WSL2 thermal state; the reliable metric is the **within-run**
ratio (+ identical trade checksums for correctness). Ring overheads, thermal-invariant:
- Ingress **batch-drain + prefetch** → 2-thread overhead **+107% → −9%** (faster than inline):
  prefetch hides the coherence transfer, and the producer core offloads ingestion.
- Egress **batch-publish** (matcher buffers a batch, one `publish_batch`/`cursor.store`) →
  3rd-core overhead **+60% → +7%**.

## Architectural milestones achieved

- **PMR arena** — one startup allocation (`monotonic_buffer_resource` + pool); zero
  OS-heap allocation on the hot path.
- **Dense-slab id index** — flat `id → {node,slot}` array; O(1), hash-free cancel.
- **Intrusive neighbor-aware RB-tree** price index — O(1) splice/graft, pred/succ
  links; ~5% faster than `std::pmr::map` and general over sparse price ranges.
- **PIN order book** — occupancy-mask nodes + intrusive FIFO chain (strict time
  priority under cancel/refill).
- **Generic SPSC Disruptor ring** (`SpscRing<T>`) — power-of-two mask, 64-byte-padded
  atomic sequences (no false sharing), release/acquire hand-off, cached
  opposite-sequence, **batch-drain + prefetch**. Ingress (`Order`) and egress
  (`TradeEvent`) are typed aliases over the one verified implementation.
- **Zero-crash discipline** — `noexcept` hot paths, bounds checks, ASan/UBSan on the
  core, **ThreadSanitizer gate** on the concurrency primitives.

## Milestone log (git)

| Version | Date | What landed |
|---|---|---|
| v1.1.1 | 2026-07-11 | Architecture redesign — PIN engine + LMAX disruptor planned |
| v1.1.2 | 2026-07-12 | Foundation: Order/PIN/PriceLevel/OrderBook (dense slab) + Matcher |
| v1.1.3 | 2026-07-12 | Intrusive RB-tree price index + GBM power-law workload |
| v1.1.4 | 2026-07-12 | RB index promoted to default; A/B scaffolding stripped |
| v1.2.0 | 2026-07-12 | Ingress SPSC ring + ThreadSanitizer gate |
| v1.2.2 | 2026-07-12 | Batch-drain + prefetch (ingress overhead +107% → −9%) |
| v1.2.3 | 2026-07-12 | Generic `SpscRing<T>`; egress ring foundation; batch TSan test |
| v1.2.4 | 2026-07-13 | 3-thread pipeline: egress wiring, zero-drop backpressure |
| v1.2.5 | 2026-07-13 | Egress `publish_batch` + matcher local buffer: 3rd-core overhead +60% → +7% |
| v1.3.0 | 2026-07-13 | Component Complete: Lock-Free Core Pipeline (all 4 ring methods TSan-proven) |
| v1.3.1 | 2026-07-13 | Journaler: mmap WAL + ABI safety harness + tests |
| v1.3.2 | 2026-07-13 | Real Sequencer + WAL recovery/replay + durability cadence + journaling-tax bench |
| *(pending)* v1.3.3 | 2026-07-13 | Matcher zero-crash under pool/arena exhaustion (REJECTED event); `TradeEvent.status`; construction fail-fast |
| *(pending)* v1.3.4 | 2026-07-13 | Edge-triggered epoll TCP ingress Gateway (`net/tcp_gateway.hpp`) + 10k-order end-to-end test (intact/in-seq + fd-leak) |
| v1.3.6 | 2026-07-13 | Gateway batched recv (4 KB/syscall); `titan-server` executable (`src/main.cpp`, `server.sh`) — 3-thread topology + graceful SIGINT/SIGTERM drain |
| v1.4.1 | 2026-07-13 | MPSC lock-free ring (`mpsc_ring.hpp`, Vyukov CAS-claim + per-cell published-seq); TSan gate — 4 producers / 1 consumer, 1M items, exactly-once, zero races |
| v1.4.2 | 2026-07-13 | Wire MPSC into the pipeline: Sequencer `run()` drain-loop, Gateway → MpscRing, 4-thread `titan-server` with cascading shutdown (smoke-tested 250k, race-free) |
| v1.4.3 | 2026-07-13 | Multi-gateway fan-in: `titan-server <port...>` spins N gateway threads onto one shared MpscRing; cascade stops+joins all gateways first. Concurrent 2×125k blast → exactly 250k journaled, zero loss |
| v1.4.4 | 2026-07-13 | UDP multicast Publisher (`net/udp_publisher.hpp`): raw-binary TradeEvent fan-out, non-blocking, MTU-safe. Publisher(T4) rewired egress → multicast. Verified end-to-end: external listener decoded 122,223 TradeEvents, zero loss |
| *(uncommitted)* v1.4.5 | 2026-07-13 | Permanent multicast harness (`tests/mc_listener.py` + `tests/multicast_test.sh`): self-contained build + blast + external-listener verify (graceful exit on count/idle). Snapshot & gap-fill = design phase (no code yet) |

## Planned build order
1. ~~Wire the egress ring into the matcher~~ ✅ v1.2.4 + `publish_batch`
2. ~~TSan-gate `publish_batch`~~ ✅ v1.3.0 (all 4 ring methods proven)
3. ~~Journaler (WAL) → power-failure recovery~~ ✅ v1.3.1 (WAL) + v1.3.2 (Sequencer wiring + replay)
4. ~~Matcher graceful degradation under pool/arena exhaustion (REJECTED event, never terminate)~~ ✅ v1.3.3
5. ~~TCP ingress Gateway (epoll, edge-triggered → Sequencer)~~ ✅ v1.3.4 (UDP publish + kernel-bypass still TODO)
6. ~~Server `main()`: gateway + sequencer + matcher + egress threads (real, not a bench)~~ ✅ v1.3.6 (`titan-server`, graceful shutdown)
7. ~~MPSC ingress ring primitive~~ ✅ v1.4.1 + ~~wire into pipeline~~ ✅ v1.4.2 + ~~multi-gateway fan-in~~ ✅ v1.4.3 (N listeners → 1 MpscRing, zero-loss under contention)
8. ~~UDP market-data multicast publish~~ ✅ v1.4.4 — kernel-bypass (io_uring/DPDK) still TODO
9. UI / market-data feed
