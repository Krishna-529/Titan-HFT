# Titan HFT v1 (C++) — Progress

A bare-metal, zero-crash HFT matching core and Disruptor pipeline. This file tracks
the Flash One architecture against what is actually built, and is updated as
components land.

**Legend:** ✅ done & tested · 🟡 partial / foundation / mock · ❌ not started

## Component status

| Component (diagram) | Status | Done in | Date | Notes |
|---|---|---|---|---|
| UI / Localhost / Sim 1 / Sim 2 | ❌ | — | — | No UI/simulator in C++ v1 |
| TCP + Kernel Bypassing | ❌ | — | — | No networking |
| Gateway | ❌ | — | — | — |
| **Ingress Queue (LMAX Disruptor)** | 🟡 | v1.2.0 (+batch) | 2026-07-12 | Generic SPSC ring; **batch-drain + prefetch**; **TSan-proven** (single + batch). Wired to matcher in a bench; **SPSC**, not yet MPSC |
| **Matching Engine (using PIN)** | ✅ | v1.1.2–v1.1.4 | 2026-07-12 | PIN book, dense-slab O(1) id index, intrusive RB-tree price index, LIMIT/MARKET/IOC + partial fills. 26 tests / 67k checks (ASan/UBSan) |
| Sequencer | 🟡 | — | 2026-07-12 | Mock driver thread (stamps monotonic seq) in the pipeline bench |
| Journaler | ❌ | — | — | No WAL |
| **Egress Queue (LMAX Disruptor)** | 🟡 | (pending) | 2026-07-12 | Ring **foundation built** (`SpscRing<TradeEvent>`, single + batch). **Not yet wired** — matcher still uses a mock `std::vector` |
| Publish Data / Trade Reporter | ❌ | — | — | — |
| UDP / TCP feedback → UI | ❌ | — | — | — |
| Power-failure recovery | ❌ | — | — | Needs the journaler |
| Server `main()` executable | ❌ | — | — | Runs via tests + benches only |

## Verified performance (WSL2, g++ 13, -O3 -march=native)

| Path | ns/msg | throughput |
|---|---|---|
| Matcher alone — deep power-law book | ~180 | ~5.6 M/s |
| Matcher alone — hot small book | ~51–76 | ~13–19 M/s |
| **Sequencer → ring → Matcher (2 threads)** | **~47** | **~21 M msgs/s** |

The two-thread pipeline runs **faster than inline matching** (ring overhead **−9%**):
software prefetch hides the cross-core coherence transfer, batch-drain amortises the
consumer's release-store, and the producer core offloads ingestion. Verified via an
in-process A/B (matcher-alone vs pipeline, back-to-back) and an identical checksum.

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
| *(pending)* | 2026-07-12 | Batch-drain + prefetch (~21M/s); generic `SpscRing<T>`; egress ring foundation |

## Planned build order
1. Wire the **egress ring** into the matcher (drop the mock vector)
2. Server `main()`: sequencer + matcher + egress threads
3. **Journaler** (WAL) → unlocks power-failure recovery
4. Gateway + networking (TCP ingress, UDP publish)
5. **MPSC** ingress (multiple gateways)
6. UI / market-data feed
