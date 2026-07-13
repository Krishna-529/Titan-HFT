# Titan HFT v1 (C++) — Session Handoff / "Resume Here"

> **Purpose.** This is the single document to read at the start of a new session so it
> doesn't feel new. It captures the whole journey, every architectural decision and the
> *why*, the current state, how to operate the repo, the measured performance, the
> gotchas, and exactly what to do next. It is intentionally detailed. Companion docs:
> `PROGRESS.md` (component board) and Claude's cross-session memory
> (`~/.claude/projects/C--Users-HPW-Desktop-Titan-HFT/memory/`, auto-loaded each session).

---

## 0. TL;DR (30 seconds)

- **What:** a bare-metal, zero-crash C++ HFT matching engine + LMAX-Disruptor pipeline
  (the "Flash One" architecture), rebuilt from a Java v0.
- **Where:** WSL2 Ubuntu, repo at `~/projects/titan-hft-v1`. Edit from Windows via the
  UNC path `\\wsl.localhost\Ubuntu\home\krishna\projects\titan-hft-v1\...`.
- **HEAD:** `v1.3.1` (`f13fddd`) — the **Journaler** (mmap WAL + ABI safety harness) is
  committed. `v1.3.0` (`2439f8e`) sealed the ThreadSanitizer-proven lock-free core.
- **Uncommitted right now (→ v1.3.2):** the **real Sequencer + WAL wiring + recovery/replay**:
  `include/titan/pipeline/sequencer.hpp` (new), `tests/sequencer_tests.cpp` (new, 5 tests),
  `include/titan/io/journaler.hpp` (added `sync_dirty`/`flush_async`/`flush_sync`),
  `bench/pipeline_bench.cpp` (journaling-tax measurement), `build.sh` edits. **All 34 tests
  pass under ASan/UBSan; TSan still 3/0.** Not committed.
- **Next action:** commit v1.3.2. Then: real server `main()` (a running process, not a bench),
  gateway/networking, MPSC ingress. Also worth addressing: the arena-exhaustion crash gap in
  the matcher (see §9).
- **Status of the whole diagram:** the matching core, the lock-free rings, the WAL, **and now
  the Sequencer (seq-stamp → write-ahead journal → publish) + WAL recovery/replay** are done.
  The rest of the I/O half (gateway, kernel-bypass networking, real publishers, UI, MPSC) is
  mock/absent.

---

## 1. Origin — where this came from

**v0 was a Java system** (still on Windows, untouched, as reference): a single-threaded
matching engine behind a `LinkedBlockingQueue`, with synchronous `ExecutionListener`
fan-out, a Javalin REST/WebSocket server, a React trading dashboard, and a GBM
multi-agent market simulator. It worked but carried Java's hidden GC tail-latency, and its
one engine thread also did JSON serialization + socket writes on the hot path. (At the very
start of the project a 2-page architecture PDF of v0 was generated under the Windows repo's
`docs/`.)

**v1 is a from-scratch C++ rewrite**, deliberately "very differently architectured." The
user (an HFT systems engineer) drove it toward the **"Flash One" Priority-Indicated Node
(PIN)** order book integrated into an **LMAX Disruptor** pipeline, under a hard
**zero-crash** mandate (reference material: PIN paper `2606.01183v5.pdf`, the LMAX
Disruptor paper + repo).

**Full target pipeline (the diagram we're building toward):**
`UI/Sim → Gateway (TCP + kernel-bypass) → Ingress Queue (LMAX Disruptor) → Matching Engine
(PIN) + Sequencer & Journaler → Egress Queue (LMAX Disruptor) → Publish Data / Trade
Reporter`, with UDP/TCP feedback to the UI and **journal-based recovery on power failure**.

---

## 2. The journey, phase by phase (the "whole conversation")

### Phase 1 — Foundation (v1.1.1 → v1.1.2)
Bottom-up, header-only core under `include/titan/`, each piece unit-tested with a tiny
zero-dependency harness (`tests/ut.hpp`) under **ASan + UBSan** (the zero-crash proof):
- `domain/types.hpp` — `PriceTick=int64` (fixed-point, never float on the hot path), `Qty`,
  `OrderId`, `Seq`, `Side`, `OrderType{LIMIT,MARKET,IOC}`, `INVALID_INDEX` sentinel.
- `book/order.hpp` — `Order`: a tightly-packed 40-byte POD (static_asserted trivially
  copyable / standard layout / size).
- `book/pin_node.hpp` — `PIN_Node`: `alignas(64)`, fixed capacity 64, a `uint64`
  occupancy mask (`__builtin_ctzll`, full-node guarded *before* the builtin so no UB), and
  an **intrusive doubly-linked FIFO chain** (`next_in_time`/`prev_in_time`/`head_slot`/
  `tail_slot`) so time priority survives cancel-then-refill.
- `book/price_level.hpp` — a price + head/tail node indices + aggregates.
- `memory/arena.hpp` — PMR arena: one big startup buffer → `monotonic_buffer_resource`
  (null upstream, so it never touches the OS heap) → `unsynchronized_pool_resource` (recycles
  freed blocks under churn → bounded memory). This was a user correction: naive
  `std::pmr::map` allocates per insert, breaking zero-alloc; PMR fixes it while keeping STL
  ergonomics.
- `book/order_book.hpp` — the book; `book/matcher.hpp` — price-time matching with
  LIMIT/MARKET/IOC + partial fills + multi-level sweeps.

### Phase 2 — Optimization, measured not guessed (v1.1.3 → v1.1.4)
A benchmark harness (`bench/matcher_bench.cpp`) drove a realistic add/cancel/market mix and
we iterated:
- **Dense-slab id index** — replaced `std::pmr::unordered_map<OrderId,Locator>` (≈2 cache
  misses) with a flat array `OrderId → {node,slot}` (hash-free, 1 miss, O(1) cancel).
  **~450 → ~180 ns/op, a ~2.5× win.** ✅ kept.
- **Free-on-empty node reclamation** — reclaim a PIN node the instant it empties. **Regressed**
  (unlinking touches cold neighbor nodes = extra misses). ❌ reverted.
- **PIN_Node hot/cold split** — metadata-first layout. **Wash** on a cache-hot book. ❌ dropped.
- To measure sub-30% effects through WSL2's thermal noise we built an **in-process A/B ratio
  harness** (run baseline vs candidate interleaved, report the ratio — thermal-invariant).
- **Neighbor-aware intrusive Red-Black price index** (`book/rb_price_index.hpp`) — user's
  specific design: **O(1) splice** on insert (one search finds pred/succ, attach at the unique
  null child), **O(1) graft** on delete (successor via the explicit `succ` link, no search),
  pred/succ neighbor links, CLRS rebalance with a sentinel node, arena-backed pool, exposes a
  `std::map`-like interface so it drops into `OrderBook`. ~5% faster than `std::pmr::map` and
  general over sparse price ranges. **Promoted to the default; A/B scaffolding stripped.** ✅
- The realistic **GBM + power-law workload** (β=2.23 depth around a Geometric-Brownian-Motion
  mid) was introduced here — it produces a shallow near-touch book with heavy crossing.

### Phase 3 — The lock-free pipeline (v1.2.0 → v1.3.0)
- **Hand-rolled SPSC ring** (LMAX Disruptor): power-of-two + bitwise mask, `alignas(64)`
  padded producer/consumer sequences (no false sharing), release/acquire hand-off, a **cached
  opposite-sequence** so the common path never reads the other core's cache line.
- **ThreadSanitizer gate** (`tsan.sh`) — a 2-thread stress at 5M items on a tiny 1024-slot ring
  (heavy wraparound). *Found the WSL2/Ubuntu-24.04 TSan "unexpected memory mapping" fatal →
  fixed with `setarch -R` (disable ASLR for the process).*
- **`consume_batch` (batch-drain + prefetch)** — snapshot the cursor once, drain the whole run,
  `__builtin_prefetch` the next slot while the matcher works the current one, one release-store
  per batch. **Ingress overhead went +107% → −9%** — the 2-thread pipeline runs *faster than
  inline matching* (prefetch hides the cross-core transfer; the producer core offloads ingestion).
- **DRY refactor** — extracted a **generic `SpscRing<T>`** (`pipeline/spsc_ring.hpp`);
  `IngressRing = SpscRing<Order>` and `EgressRing = SpscRing<TradeEvent>` are typed aliases;
  `TradeEvent` moved to its own light header `book/trade_event.hpp`.
- **3-thread topology** (`bench/pipeline_bench.cpp`): Sequencer → Ingress → Matcher → Egress →
  Publisher. The **Matcher's `submit` is templated on a sink** (`bool try_publish(const
  TradeEvent&)`): tests pass a vector sink, the pipeline passes the EgressRing with **zero-drop
  busy-spin backpressure**. First cut published each trade single-element → **+60% third-core
  cost**.
- **Egress `publish_batch`** — the Matcher buffers a whole ingress batch's trades locally, then
  flushes with one `publish_batch` (single cursor.store/batch). **Third-core cost +60% → +7%.**
- **Final TSan gate** — all four ring methods (`try_publish`/`try_consume`/`consume_batch`/
  `publish_batch`) proven race-free (variable 1–50-item batches, 5M items). Test file renamed
  `ingress_ring_tests.cpp` → `tests/spsc_ring_tests.cpp`. → **v1.3.0 sealed & tagged:
  "Component Complete: Lock-Free Core Pipeline."**

### Phase 4 — I/O half begins (uncommitted → v1.3.1)
- **Journaler** (`include/titan/io/journaler.hpp`) — append-only Write-Ahead Log over POSIX
  `mmap`. `append()` = pure `memcpy` into the mapped buffer + cursor bump (**zero syscalls / no
  blocking I/O on the hot path**); `msync` on graceful shutdown. Strictly binary (raw `Order`
  bytes). A 64-byte `FileHeader` (`magic=0x544954414E "TITAN"`, `version=1`,
  `order_size=sizeof(Order)`, plus a `count` write-cursor) is an **ABI tripwire**: `validate()`
  in the open-ctor throws on any mismatch. Pre-allocates with `posix_fallocate` (fallback
  `ftruncate`). `-std=c++20` is strict-ANSI so POSIX is exposed via `#define _DEFAULT_SOURCE`
  (also `-D_DEFAULT_SOURCE` in `build.sh`).
- `tests/journaler_tests.cpp` — (1) create + append 100k + close; (2) reopen, validate header,
  verify every payload byte-exact; (3) corrupt `order_size` → open aborts (throws). All green.

---

## 3. Current state (exact)

**HEAD = `v1.3.1` (`f13fddd`).** Working tree has the Sequencer + WAL wiring + recovery
uncommitted (→ v1.3.2):
```
 M build.sh                              # + tests/sequencer_tests.cpp
 M bench/pipeline_bench.cpp              # + journaling-tax measurement (section D)
 M include/titan/io/journaler.hpp        # + sync_dirty / flush_async / flush_sync
?? include/titan/pipeline/sequencer.hpp  # NEW: Sequencer + replay()
?? tests/sequencer_tests.cpp             # NEW: 5 tests (wiring + recovery)
```
Test counts: **ASan/UBSan suite = 34 tests / 67,430 checks** (`build.sh`). **TSan gate = 3
ring tests, zero data races** (`tsan.sh`). Both green.

Also uncommitted/updated this session: `PROGRESS.md` and this `HANDOFF.md`.

---

## 4. Architecture & decisions (with rationale — do not re-litigate)

| Decision | Why |
|---|---|
| **PIN order book** (occupancy-mask nodes + intrusive FIFO) | O(1) slot find via `ctzll`; strict time priority even under cancel-refill |
| **Dense-slab id index** (flat `id→{node,slot}`) | hash-free, 1 miss, O(1) cancel; beat unordered_map ~2.5× |
| **Neighbor-aware RB-tree price index** (O(1) splice/graft, pred/succ) | general over sparse prices; single-search insert; ~5% over pmr::map |
| **PMR arena** (monotonic+pool, null upstream) | zero OS-heap alloc after startup, bounded under churn |
| **Generic `SpscRing<T>`** (one impl, ingress+egress aliases) | one place for subtle lock-free code; both TSan-proven |
| **batch-drain + prefetch / batch-publish** | amortize the release-store + hide the coherence transfer → overheads went negative/near-zero |
| **Matcher sink templated** (`try_publish`) | pipeline uses EgressRing (zero-drop), tests use a vector sink — one matching path |
| **Journaler = mmap WAL, binary, header tripwire** | non-blocking hot-path persistence; guards raw-binary ABI drift |
| **Sequencer: seq → write-ahead journal → publish** | log-before-effect ⇒ WAL is a faithful replayable history; seq is the single source of arrival order |
| **Deferred durability cadence** (MS_ASYNC/batch, MS_SYNC every K=64) | append stays syscall-free; bounds loss window without a per-order syscall (append ~free, sync is the cost — §6) |
| **Recovery via `seq==base+i` invariant** | torn-tail boundary independent of id scheme / `count`; robust to zeroed un-flushed tail |
| **Zero-crash discipline** | `noexcept` hot paths, bounds checks, ASan/UBSan on core, TSan on concurrency (gap: matcher consume path on arena exhaustion — §9) |

**Memory-ordering model (the ring's one happens-before edge):** producer writes the slot →
`cursor.store(release)`; consumer `cursor.load(acquire)` → reads the slot. Slot reuse is the
mirror via the consumer's `next` release / producer's acquire. TSan validates both directions.

---

## 5. Pipeline topology (how the 3 threads run)

```
T1 Sequencer  --publish/publish_batch-->  IngressRing<Order>
                                             |  consume_batch (drain + prefetch)
                                             v
T2 Matcher    --submit(order, sink)-->  matches; buffers trades locally, then
              --publish_batch(trades)-->  EgressRing<TradeEvent>
                                             |  consume_batch
                                             v
T3 Publisher  --drains, checksums-->  (mock; real fan-out / Trade Reporter TBD)
```
Everything busy-spins (strict, no yield) for lowest latency. Backpressure is zero-drop end to
end (a full ring makes the upstream spin). Correctness is cross-validated by an
order-independent **trade checksum** identical across inline / 2-thread / 3-thread.

---

## 6. Measured performance (and how to read it)

WSL2 absolute timings drift **~30–150%** run-to-run (thermal throttling + VM scheduling, no
core pinning). **Trust the within-run A/B ratio, not the absolute ns.** Report min/median.

| Path | ns/msg (thermal-dependent) | throughput |
|---|---|---|
| Matcher alone — deep power-law book | ~180 | ~5.6 M/s |
| Matcher alone — hot small book | ~47–77 | ~13–21 M/s |
| 2-thread (Seq→Ingress→Matcher) | ~43–67 | ~15–23 M/s |
| 3-thread (+Egress→Publisher, batch-publish) | ~72 | ~14–22 M/s |

Thermal-invariant overhead deltas (the real story):
- Ingress batch-drain + prefetch: **+107% → −9%** (2-thread faster than inline).
- Egress batch-publish: **+60% → +7%** (third core nearly free).

**Journaling tax** (v1.3.2, `pipeline_bench` section D — sequencer publish-loop cost, WAL
on vs off, within-run so thermal-invariant):

| Sequencer publish loop | ns/order | Δ vs no-WAL |
|---|---|---|
| no-WAL | ~42 | — |
| WAL **append-only** | ~44.5 | **+2.5 ns (+6%)** |
| WAL **append + sync cadence** (flush 1024, MS_SYNC every 64) | ~116 | **+74 ns (+176%)** |

Read: the write-ahead **append itself is nearly free** (+2.5 ns memcpy; still below the
matcher's ~49 ns, so append-only journaling is *hidden* behind the matcher end-to-end). The
**`MS_SYNC` durability cadence dominates** (+74 ns) — at ~116 ns the Sequencer would become
the pipeline bottleneck. Durability, not logging, is the real cost; **K (sync_every) is the
throughput⇄loss-window knob.** (WAL on WSL2 ext4; real HW would use NVMe / an async writeback
thread.)

---

## 7. File map (tracked)

```
include/titan/
  domain/types.hpp                 aliases + enums (PriceTick int64, Side, OrderType)
  book/order.hpp                   Order POD (40B)
  book/pin_node.hpp                PIN node: occupancy mask + intrusive FIFO
  book/price_level.hpp             price + node-chain head/tail + aggregates
  book/order_book.hpp              OrderBook: dense slab + RB index + node pool
  book/rb_price_index.hpp          intrusive neighbor-aware Red-Black tree (default price index)
  book/trade_event.hpp             TradeEvent POD (32B)
  book/matcher.hpp                 MatcherT<Book>: price-time match, sink-templated submit
  memory/arena.hpp                 PMR arena (monotonic + pool)
  pipeline/spsc_ring.hpp           generic SpscRing<T> (the lock-free core)
  pipeline/ingress_ring.hpp        IngressRing = SpscRing<Order>  (alias)
  pipeline/egress_ring.hpp         EgressRing  = SpscRing<TradeEvent> (alias)
  pipeline/sequencer.hpp           [→v1.3.2] Sequencer (seq→journal→publish) + replay() recovery
  io/journaler.hpp                 mmap WAL + FileHeader safety harness (+ sync_dirty/flush_* in v1.3.2)
tests/
  ut.hpp                           tiny test harness (TEST_CASE/CHECK/REQUIRE)
  tests.cpp                        foundation tests (has the shared main())
  matcher_tests.cpp                matcher tests (no main; VecSink)
  rb_tree_tests.cpp                RB-tree invariant tests (no main)
  spsc_ring_tests.cpp              TSan ring tests (own main; built by tsan.sh)
  journaler_tests.cpp              WAL round-trip + safety-harness tests (no main)
  sequencer_tests.cpp              [→v1.3.2] Sequencer wiring + recovery/replay tests (no main)
bench/
  matcher_bench.cpp                single-thread matcher throughput
  pipeline_bench.cpp               A inline / B 2-thread / C 3-thread + checksum
scripts: build.sh (ASan/UBSan) · tsan.sh (TSan) · bench.sh · pipeline.sh
docs: PROGRESS.md · README.md · HANDOFF.md (this file)
```

Only `tests.cpp` has `main()` in the `build.sh` binary; the others register into the shared
`ut` registry. `spsc_ring_tests.cpp` has its own `main()` (separate TSan binary).

---

## 8. How to operate the repo (read before running commands)

- **Edit files** from Windows via UNC `\\wsl.localhost\Ubuntu\home\krishna\projects\titan-hft-v1\...`
  (Write/Edit/Read work over UNC; **Glob does NOT** traverse it — use `git ls-files`/`ls` to a
  `build/*.txt` and Read that).
- **Scripts** (each tees to `build/*.log` — Read the log, don't trust console stdout):
  - `bash build.sh` — ASan+UBSan unit tests.
  - `bash tsan.sh` — ThreadSanitizer ring tests (**runs via `setarch "$(uname -m)" -R`** to
    dodge the WSL2 ASLR fatal — already baked in).
  - `bash bench.sh` / `bash pipeline.sh` — **release** `-O3 -march=native -DNDEBUG`, sanitizers OFF.
- **Toolchain:** WSL2 Ubuntu, g++ 13, C++20.
- **Versioning:** commit subject is `vX.Y.Z - description`; the user often commits himself
  between turns — **always `git log` before assuming state**.
- **Committing long messages:** write the message to a temp file and `git commit -F file`.
- **PowerShell↔wsl bridge gotchas (this wastes the most round-trips):** inside
  `wsl -d Ubuntu bash -lc '…'`, **AVOID `|`, `()`, `&&`, and nested double-quotes** — they get
  mangled or truncate output. Keep commands simple, redirect to `build/*.log`, and Read that.

---

## 9. Open decisions & next steps

**DONE this session (→ v1.3.2): persistence + recovery wired** (the diagram's
`Sequencer & Journaler` + power-failure box):
1. **Real `Sequencer`** (`pipeline/sequencer.hpp`): stamps a monotonic `seq`, **write-ahead
   journals** (`append` before `try_publish` — an order is in the log in append order before
   it can affect the book), then publishes to ingress with zero-drop backpressure. Concrete,
   no virtuals → inlinable (A/B/C bench numbers held).
2. **Durability cadence (decided):** append stays syscall-free; durability is deferred off the
   per-order path. `flush_async()` = `MS_ASYNC` every `flush_interval` (default **1024**) orders
   (schedules writeback, ~free); `flush_sync()` = `MS_SYNC` every `sync_every`-th (default
   **K=64**) interval → durability checkpoint every 65,536 orders, also flushing the header page
   so `count` is durable. `sync_dirty` page-aligns and msyncs only the newly-dirtied tail (not
   the whole mapping). Loss window ≤ `flush_interval * sync_every` orders. Cost measured: §6.
3. **Recovery/replay** (`replay(wal, matcher, sink)`): reconstructs book state by re-submitting
   records in append order. **Torn-tail boundary via the append-order invariant**
   `wal[i].seq == base + i` (`base = wal[0].seq`) — stops at the first violation. This is robust
   to any id scheme (never an `id==0` sentinel) and does not trust `count` as the authoritative
   tear marker (`count` only bounds the scan; a zeroed/un-flushed tail breaks the invariant and
   halts replay). Empty-WAL (`count==0`) is a clean no-op; replay is idempotent. 5 tests cover
   all of this.

**RECORD SCOPE (conscious decision):** the WAL logs **Order records only**. Nothing but new
orders currently traverses ingress, so an Order-only log is a complete command stream. **If
cancels/modifies ever enter the ingress path, the WAL must become a tagged command log** (op-type
discriminator + union/variant payload) — replaying orders alone would silently diverge. Noted in
`sequencer.hpp`.

**FINDING — pre-existing crash gap in the matcher (NOT introduced here):** under genuine **arena
exhaustion**, a `bad_alloc` escapes the matcher's `noexcept` consume path (an *unwrapped*
allocation, likely `opp.erase(it)`/level-map churn in `cross()` — `add()`'s residual-rest path is
already wrapped) → `std::terminate`. Surfaced when a test arena was undersized (fixed by sizing to
32 MB bench parity). Violates the zero-crash mandate on exhaustion; worth wrapping the consume-path
map ops (or pre-flighting capacity) so exhaustion degrades gracefully like `add()` does.

**Durability caveat still true:** between `MS_SYNC` checkpoints a hard crash loses ≤ the loss
window's un-flushed appends (they're page-cache-only). The recovery invariant makes this *safe*
(no garbage replayed); tightening the window is the K knob (§6 shows its cost).

**Next (the rest of the I/O half — still mock/absent):**
- Real **server `main()`** (a running process wiring the 3 threads + Sequencer/WAL, not a bench).
- **Gateway + networking** (TCP order ingress, UDP market-data publish; kernel-bypass later).
- **MPSC ingress** (multiple gateways → one ring; CAS claim).
- **Publishers / Trade Reporter**, then the **UI / market-data feed**.

---

## 10. Version log (git)

| Version | Commit | What landed |
|---|---|---|
| v1.1.1 | 87048c1 | Architecture redesign — PIN engine + LMAX disruptor planned (2026-07-11) |
| v1.1.2 | af56a84 | Foundation: Order/PIN/PriceLevel/OrderBook (dense slab) + Matcher |
| v1.1.3 | 22a1c0e | Neighbor-aware RB-tree price index + GBM power-law workload |
| v1.1.4 | 7fd9616 | RB index promoted to default; A/B scaffolding stripped |
| v1.2.0 | 10e1418 | Ingress SPSC ring + ThreadSanitizer gate |
| v1.2.2 | 255a931 | Batch-drain + prefetch (ingress overhead +107% → −9%) |
| v1.2.3 | 833bbea | Generic `SpscRing<T>`; egress ring foundation; batch TSan test |
| v1.2.4 | ec784b3 | 3-thread pipeline: egress wiring, zero-drop backpressure |
| v1.2.5 | d6f3219 | Egress `publish_batch` + matcher local buffer (3rd-core +60% → +7%) |
| **v1.3.0** | **2439f8e** | **Component Complete: Lock-Free Core Pipeline (all 4 ring methods TSan-proven)** |
| v1.3.1 | f13fddd | Journaler: mmap WAL + ABI safety harness + tests |
| (pending) v1.3.2 | — | Real Sequencer (seq→write-ahead journal→publish) + WAL recovery/replay + durability cadence + journaling-tax bench |

---

## 11. Who you're working with

Krishna — an HFT systems engineer with deep C++/systems fluency (mechanical sympathy, cache
lines/false sharing, memory ordering, Disruptor internals, Red-Black trees). Working style:
disciplined **implement → measure → optimize**; insists on **sanitizer gates** (ASan/UBSan/TSan)
before trusting code; values **DRY** and honest reporting of negative results; drives the
architecture with specific, correct directives; commits at `vX.Y.Z` milestones (often himself);
prefers concise, technical answers over hand-holding.

---

*Keep this file current at each `vX.Y.Z` commit and when the context window runs low, alongside
`PROGRESS.md` and the Claude memory files.*
