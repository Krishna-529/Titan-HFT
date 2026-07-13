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
| **Sequencer** | ✅ | v1.3.2 | 2026-07-13 | Real component (`pipeline/sequencer.hpp`): seq-stamp → **write-ahead journal** → publish, zero-drop backpressure. Concrete/inlinable (A/B/C held). SPSC |
| **Journaler** | ✅ | v1.3.1 (+cadence v1.3.2) | 2026-07-13 | mmap WAL, binary, 64B `FileHeader` ABI tripwire. Hot-path append = memcpy (no syscall). **Deferred durability**: `MS_ASYNC`/batch + `MS_SYNC` every K=64 (`sync_dirty` page-aligned tail). Append ~free; sync is the cost |
| **Egress Queue (LMAX Disruptor)** | 🟡 | v1.2.4 (+batch) | 2026-07-13 | **Wired**: matcher **batch-publishes** trades (thread-local buffer → `publish_batch`, one release-store/batch, zero-drop). SPSC |
| Publish Data / Trade Reporter | 🟡 | v1.2.4 | 2026-07-13 | Mock: a Publisher thread `consume_batch`-drains egress into a checksum. Real fan-out TBD |
| UDP / TCP feedback → UI | ❌ | — | — | — |
| **Power-failure recovery** | 🟡 | v1.3.2 | 2026-07-13 | `replay()` rebuilds book from WAL; torn-tail boundary via `seq==base+i` invariant (id-scheme & `count`-independent); empty-WAL no-op; idempotent. Bounded loss window between MS_SYNC checkpoints |
| Server `main()` executable | ❌ | — | — | Runs via tests + benches only |

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
| *(pending)* v1.3.2 | 2026-07-13 | Real Sequencer + WAL recovery/replay + durability cadence + journaling-tax bench |

## Planned build order
1. ~~Wire the egress ring into the matcher~~ ✅ v1.2.4 + `publish_batch`
2. ~~TSan-gate `publish_batch`~~ ✅ v1.3.0 (all 4 ring methods proven)
3. ~~Journaler (WAL) → power-failure recovery~~ ✅ v1.3.1 (WAL) + v1.3.2 (Sequencer wiring + replay)
4. Server `main()`: sequencer + matcher + egress threads (real, not a bench)
5. Wrap the matcher consume path against arena exhaustion (zero-crash gap, HANDOFF §9)
6. Gateway + networking (TCP ingress, UDP publish)
7. **MPSC** ingress (multiple gateways)
8. UI / market-data feed
