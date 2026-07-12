# Titan HFT v1 (C++) — Implementation Progress

Tracks each component of the Flash One pipeline against what is actually built.
Dates/versions are anchored to git commits. Updated as components land.

**Legend:** ✅ done & tested · 🟡 partial / primitive / mock · ❌ not started

| # | Component (diagram) | Status | Done in | Date | Notes |
|---|---|---|---|---|---|
| 1 | UI Coflactor / UI / Localhost | ❌ | — | — | No UI in C++ v1 (Java v0 had a React dashboard; not ported) |
| 2 | Simulation 1 / Simulation 2 | ❌ | — | — | No agent simulator in C++ v1 |
| 3 | TCP + Kernel Bypassing | ❌ | — | — | No networking of any kind |
| 4 | Gateway | ❌ | — | — | No network gateway |
| 5 | **Ingress Queue (LMAX Disruptor)** | 🟡 | v1.2.0 | 2026-07-12 | SPSC ring built + **TSan-proven** race-free (5M msgs). Wired to matcher only in a **bench**; SPSC not yet MPSC |
| 6 | **Matching Engine (using PIN)** | ✅ | v1.1.2–v1.1.4 | 2026-07-12 | PIN book (occupancy-mask + intrusive FIFO), dense-slab O(1) id index, neighbor-aware RB-tree price index, LIMIT/MARKET/IOC + partial fills. 26 tests / 67k checks under ASan/UBSan; ~76–180 ns/order |
| 7 | Sequencer | 🟡 | — | 2026-07-12 | Mock driver thread stamps a monotonic seq (in the pipeline **bench**, uncommitted). No real component |
| 8 | Journaler | ❌ | — | — | No WAL/journaling |
| 9 | Egress Queue (LMAX Disruptor) | ❌ | — | — | Mock `std::vector<TradeEvent>` inside the matcher; `TradeEvent` POD exists, ring does not |
| 10 | Publish Data | ❌ | — | — | Not built |
| 11 | Trade Reporter | ❌ | — | — | Not built |
| 12 | UDP feedback → UI | ❌ | — | — | No networking |
| 13 | TCP feedback → UI | ❌ | — | — | No networking |
| 14 | Power-failure recovery | ❌ | — | — | Needs the journaler |
| — | Server `main()` / running executable | ❌ | — | — | Runs only via tests + benches today |

## How much (three lenses)
- By diagram boxes: **~2 of 14 substantially done (~15%)**
- By hot path (Ingress → Matcher → Egress): **~half, hardest third finished**
- By engineering effort: **~35–40%** (core + ring are the hardest/most-optimized/most-tested)

## Milestone log (git)
| Version | Date | What landed |
|---|---|---|
| v1.1.1 | 2026-07-11 | Architecture redesign — PIN engine + LMAX disruptor planned |
| v1.1.2 | 2026-07-12 | Foundation: Order POD, PIN node, price level, OrderBook (dense slab), Matcher |
| v1.1.3 | 2026-07-12 | Neighbor-aware RB-tree price index + GBM power-law workload |
| v1.1.4 | 2026-07-12 | RB index promoted to default; A/B scaffolding stripped |
| v1.2.0 | 2026-07-12 | Ingress SPSC ring + ThreadSanitizer gate (race-free) |

## Planned build order (to close the gap)
1. Egress ring (Matcher → publishers) — real ring, drop the mock vector
2. Server `main()` loop: sequencer + matcher + egress threads
3. Journaler (WAL) → unlocks power-failure recovery
4. Gateway + networking (TCP ingress, UDP publish)
5. MPSC ingress (multiple gateways)
6. UI / market-data feed
