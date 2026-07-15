# Titan HFT v1 — System Architecture

| | |
|---|---|
| **Component** | Single-symbol matching engine + market-data pipeline ("Flash One") |
| **Language / std** | C++20, header-only core under `include/titan/` |
| **Toolchain** | g++ 13, `-O3 -march=native -DNDEBUG` (release); `-g -fno-omit-frame-pointer` (profileable) |
| **Concurrency** | 5-stage thread topology, lock-free hand-offs only (no mutexes on any hot path) |
| **Status** | Core pipeline complete and sanitizer-verified; productionization (kernel-bypass, multi-symbol, client resync) is out of scope — see §11 |
| **Version** | v1.4.11 |

---

## 0. Abstract

Titan is a from-scratch C++ rebuild of a matching engine and its surrounding market-data
plane, engineered under three non-negotiable invariants: **zero heap allocation on any hot
path**, **deterministic (data-independent) control flow**, and **no lock on the critical
path**. Inbound order flow crosses the wire once, is stamped into a single total order,
write-ahead journaled, matched against a Priority-Indicated-Node (PIN) book, and fanned out
as raw binary on two multicast channels. Every concurrency primitive is a hand-rolled
lock-free structure verified race-free by ThreadSanitizer under adversarial stress; the
matching core is verified crash-free by ASan/UBSan. The design is memory-bound by
construction, and the optimization work documented here is accordingly cache-layout work,
not arithmetic work.

---

## 1. Design Philosophy & Load-Bearing Invariants

These are enforced structurally (types, `static_assert`, sanitizer gates), not by convention.

| Invariant | Enforcement |
|---|---|
| **Zero OS-heap allocation after startup** on the hot path | PMR arena (`monotonic_buffer_resource` over a single startup buffer, `null_memory_resource` upstream → exhaustion throws, never mallocs); pre-sized pools; fixed-capacity rings |
| **`noexcept` hot paths, no crash under exhaustion** | matcher `submit`/`cross`/`sweep` are `noexcept`; arena/pool exhaustion degrades to a `TRADE_STATUS_REJECTED` event, never `std::terminate` |
| **All inter-thread hand-offs are lock-free** | SPSC / MPSC rings + triple-buffer pool; release/acquire or seq_cst, never a mutex |
| **Fixed-point prices only** | `PriceTick = int64`; no floating point anywhere on the order path |
| **PODs on the wire and in shared memory** | every shared struct is `static_assert`-ed `trivially_copyable` + `standard_layout` + exact size |
| **Deterministic replay** | total order assigned once by the Sequencer; WAL is a faithful command log; recovery is byte-exact |

**Mechanical-sympathy stance.** The engine is treated as a cache/coherence machine, not an
instruction machine. Structures are laid out to minimize cache lines touched per operation
and to keep cross-core traffic to a single coherence transfer per hand-off. Profiling (§5)
confirms the arithmetic is free and the memory hierarchy is the cost; every optimization
targets the memory hierarchy.

---

## 2. System Topology

![Titan HFT architecture](Architecture.png)

| Thread | Role | Consumes | Produces | Hand-off out |
|---|---|---|---|---|
| **T1..Tk** | TCP gateways (one epoll loop each) | sockets | `Order` | MPSC ring (multi-producer) |
| **T(seq)** | Sequencer | MPSC ring | stamped `Order` + WAL record | Ingress SPSC ring |
| **T(match)** | Matcher | Ingress ring | `TradeEvent`, L2 snapshots | Egress SPSC ring + SnapshotPool |
| **T(pub)** | Incremental publisher | Egress ring | UDP datagrams | multicast `:30001` |
| **T5** | Snapshot publisher | SnapshotPool | UDP datagrams | multicast `:30002` |

Every stage busy-spins (no yield) for lowest latency; the pipeline is designed for
dedicated/pinned cores. Backpressure is zero-drop end-to-end (a full ring spins the upstream
producer). Shutdown is a strict drain cascade: gateways stop → MPSC drains → Sequencer stops
→ ingress drains → Matcher stops → egress drains → publishers stop, each stage joining only
after its input is provably empty and its upstream is gone.

---

## 3. Ingestion & Concurrency

### 3.1 TCP Gateway — `include/titan/net/tcp_gateway.hpp`

Edge-triggered `epoll` (`EPOLLET`) with `accept4(SOCK_NONBLOCK | SOCK_CLOEXEC)` (atomic accept
+ non-block, no post-accept `fcntl` window) and `TCP_NODELAY` on every connection (Nagle off —
a lone order is never held to coalesce). Shutdown is woken by an `eventfd` armed
level-triggered, so the blocking `epoll_wait(-1)` returns deterministically with no busy-poll.
All fds are RAII-owned; the test harness asserts zero fd leakage across the full
accept→read→FIN→destroy lifecycle via `/proc/self/fd`.

**Batched `recv()`.** The wire carries a raw 40-byte `Order` stream. Rather than one `recv()`
per message, the connection drains into a **4096-byte stack buffer** per readiness event and
parses complete `Order` structs out of that in-memory chunk, amortizing the user↔kernel
transition across ~102 orders instead of paying it per order. A single `Order` split across
`recv()` boundaries (TCP is a byte stream) is carried in a per-connection `pending` slot and
completed on the next chunk. Edge-triggered semantics require draining to `EAGAIN`; the read
loop does exactly that.

> The earlier design read `sizeof(Order)` per `recv()` — ~10⁶ syscalls for 10⁶ orders. The
> batched path collapses that to ~10³ syscalls (bounded by socket-buffer refills, not message
> count). The gateway is pure I/O: it stamps no sequence and touches no book state; it only
> `try_publish`es onto the inbound MPSC ring with zero-drop busy-spin.

### 3.2 Vyukov MPSC Fan-In Ring — `include/titan/pipeline/mpsc_ring.hpp`

Multiple gateway threads fan into **one** Sequencer through a bounded lock-free
Multi-Producer / Single-Consumer ring (Dmitry Vyukov's bounded-queue algorithm, specialized
to a single consumer). Power-of-two capacity (bitwise mask, no modulo); the producer and
consumer cursors each occupy their own 64-byte cache line (`alignas(64)` + padding) so the
producer CAS and the consumer advance never false-share.

Each cell carries its own `std::atomic<uint64_t> seq` — a **per-cell published tracker** that
encodes, in one word, both "whose turn is it to write this slot" and "is the payload ready to
read." The hand-off is a two-step barrier:

1. **Claim (multi-producer).** A producer reads the shared `enqueue` cursor and attempts
   `compare_exchange_weak(pos, pos+1)` — lock-free CAS contention among the network threads (a
   losing thread retries, so the claim is lock-free, not wait-free).
   The winner owns slot `pos`; losers retry with the refreshed cursor. A producer touches the
   payload **only after** winning the slot.
2. **Publish (release).** After the `memcpy` of the payload, the producer does
   `cell.seq.store(pos + 1, memory_order_release)`. The single consumer reads the cell only
   when `cell.seq.load(memory_order_acquire) == pos + 1`, then frees it with
   `cell.seq.store(pos + Size, release)` for the next lap.

The consequence is the property that makes multi-gateway fan-in correct: because the consumer
advances strictly in `pos` order and gates on **each cell's own** sequence, if producer B (a
higher slot) finishes before producer A (a lower slot), the consumer simply **waits at A's
slot** — it never reads B's data early, and never observes A's half-written payload. The
per-cell release/acquire is the single happens-before edge per slot; no lock, no torn read.

> **Verification.** 4 producer threads race **1,000,000** unique items through a deliberately
> tiny **1024-slot** ring (≈977 wraparounds, relentless full/empty cycling) into 1 consumer.
> Asserted: exactly 1,000,000 received, **zero loss, zero duplication, zero out-of-range**
> (torn) payloads. ThreadSanitizer reports **zero data races** (`tsan.sh`, `setarch -R` to
> defeat the WSL2 TSan ASLR fatal). The live-network equivalent — two clients blasting 125k
> orders each concurrently into two gateway ports — journals exactly 250,000 orders, zero loss.

### 3.3 The Sequencer — single point of total order — `include/titan/pipeline/sequencer.hpp`

The Sequencer is the single writer that collapses the concurrent inbound streams into one
total order. Its `run()` loop `consume_batch`es the MPSC ring and, per order:

```
o.seq = next_seq_++;          // monotonic arrival sequence — the system's source of truth
journal_.append(o);           // WRITE-AHEAD: logged in append order before it can affect the book
while (!ingress_.try_publish(o)) { /* zero-drop backpressure */ }
```

Journal-before-publish is the WAL invariant: an order is durable-in-log-order before it can
match, so the log is a faithful, replayable command history. The stamped `seq` propagates
downstream as the market-data `feed_seq` (§7), giving clients a single monotonic sequence to
detect gaps against.

---

## 4. Core Matching Engine & Cache Optimization

### 4.1 Structures

| Structure | File | Layout note |
|---|---|---|
| `Order` | `book/order.hpp` | 40 B POD (fixed-point price, `seq`, side, type) |
| `PIN_Node` | `book/pin_node.hpp` | `alignas(64)`, 64 slots + `uint64` occupancy mask + intrusive FIFO |
| `PriceLevel` | `book/price_level.hpp` | price + node-chain head/tail + aggregates (O(1) top-of-book) |
| `RBPriceIndex` | `book/rb_price_index.hpp` | intrusive neighbor-aware Red-Black tree (default price index) |
| `SlabEntry` | `book/order_book.hpp` | **32 B `alignas(32)`** dense id→locator + shadowed cancel fields |
| `Arena` | `memory/arena.hpp` | PMR monotonic + pool, null upstream |

**PIN order book.** A price level is an intrusive FIFO chain of `PIN_Node`s. Each node holds up
to 64 orders with a `uint64` occupancy mask; a free slot is found in O(1) via
`__builtin_ctzll` (guarded against the full-node `ctzll(0)` UB). Time priority is preserved by
an intrusive doubly-linked arrival chain (`next_in_time`/`prev_in_time`, `head`/`tail`), so
strict FIFO survives a cancel that frees a low physical slot which a later insert refills —
physical slot index is decoupled from time order.

**Price index.** A hand-rolled intrusive Red-Black tree with a single-search **O(1) splice** on
insert (one descent records in-order predecessor/successor; the new key attaches at the unique
null child), **O(1) graft** on delete (the two-child successor is read straight from the `succ`
link — no successor search), pred/succ neighbor links for O(1) traversal, and a CLRS
rebalance over a sentinel node (index 0) that makes the delete-fixup total (no null-parent
hazards). Arena-backed node pool; exposes a `std::map`-like interface so it drops into the
`OrderBook`. It replaced `std::pmr::map` (~5% faster and general over sparse price ranges).

**Id index.** A flat, dense `SlabEntry[]` indexed **directly by `OrderId`** — a hash-free O(1)
translation from id to physical location. This is pure user-space pointer arithmetic
(`locators_[id]`), not a hashed or pointer-linked lookup.

**Matcher.** `MatcherT<Book>` performs price-time matching for LIMIT / MARKET / IOC with partial
fills and multi-level sweeps. `submit` is templated on the egress **sink** (`bool
try_publish(const TradeEvent&)`), so tests use a vector collector and production uses the
egress ring through one identical matching path. The entire body is `noexcept` and guarded:
pool/arena exhaustion is caught and degraded to a `TRADE_STATUS_REJECTED` event — the pipeline
consumer never sees stack unwinding.

### 4.2 The engine is memory-bound (measured)

`perf` is unavailable on the WSL2 kernel (no `linux-tools`, no privilege); profiling used
**gprof** against the single-threaded matcher benchmark (realistic mix: 40% add / 55% cancel /
5% market-IOC over a 1M-order seeded book), which isolates the matching code with no syscall
noise. `-fno-inline` restored function boundaries for attribution.

| Category | ~% of engine self-time | Reading |
|---|---:|---|
| **Data-structure access** (PIN slot load + cancel-path id-slab→node→slot indirection) | **~60%** | pointer-chasing / cache misses dominate |
| RB-tree traversal (node dereferences, `find`, insert search) | ~10% | read-side chasing; rebalancing (rotations/fixups) ~0% |
| **Limit-order arithmetic** (crossing / fill math) | **< 3%** | the compute is effectively free |

The verdict drives the entire optimization program: **do not optimize the arithmetic path.**
The cost is the memory hierarchy — specifically the cancel path chasing id-slab → `PIN_Node` →
cold slot payload, and the `PIN_Node` slot load itself.

### 4.3 Cache-locality win — shadowing the cancel-hot fields in a 32 B `SlabEntry`

The original `SlabEntry` was 8 bytes (`{node, slot}`) on the reasoning "we touch the node
anyway on cancel, so don't duplicate price/side." The profile invalidated that reasoning:
touching the node (loading the cold slot payload to read `price`/`remaining`/`side`) was the
single largest cost. The refactor widens `SlabEntry` to **32 bytes, `alignas(32)`**, shadowing
the cancel-hot fields inline:

```
struct alignas(32) SlabEntry {   // 32 B, alignas(32): two pack into one 64-byte L1 line, no straddle
    uint32 node; uint32 slot;    // physical location (for the O(1) FIFO unlink)
    PriceTick price;             // shadowed: RB level lookup on cancel
    Qty       remaining;         // shadowed: level total_qty adjust
    Side      side;              // shadowed: bid/ask map select
    uint8 _pad[11];
};
```

`cancel(id)` is now satisfied from **one id-indexed load** — pure arithmetic — and touches the
node only for the O(1) occupancy-bit clear + FIFO unlink. The cold 2.5 KB slot payload is
**never fetched**. `alignas(32)` keeps an entry from straddling a cache line: two `SlabEntry`s
pack into a 64-byte L1 line (the hardware fetches the whole line, adjacent entry included), so a
lookup pulls exactly one line rather than two.

`remaining` is mutable (a resting maker is decremented on partial fill), so correctness
requires a sync: the matcher calls `note_partial_fill(id, remaining)` at the one site a
resting order's quantity changes, keeping the shadow exact. Full fills and cancels clear the
slab, so the shadow is never stale when read.

> **Result** (matcher_bench, 55%-cancel workload): **203.9 → 162.6 ns/msg (−20%)**,
> **4.90 → 6.15 M msgs/s (+25%)**. This is the largest single win in the program because it
> removes an entire cold cache-line load from the dominant (cancel) path.

### 4.4 Metadata-first layout inversion of `PIN_Node`

For cross-heavy flow, `PIN_Node` was inverted so the **control block**
(`occupancy_mask`, `head_slot`/`tail_slot`, chain links, and the intrusive FIFO arrays) sits at
**offset 0 under `alignas(64)`**, with the 2.5 KB `slots` payload placed immediately after on
its own cache line. Previously `slots` occupied offset 0, pushing `occupancy_mask` and
`head_slot` ~2 cache lines apart — so `front_node`'s empty-check and `sweep_level`'s
`head_slot` read hit two lines to locate an order. Inverted, they share the node's first line.

This is a correct, zero-cost, zero-mechanics-change layout (ASan-clean; `sizeof` unchanged). Its
throughput benefit is realized on **cold-node, deep-book** crossing; on the current benchmarks
(a hot single-node blaster; a cancel-heavy microbench) the delta sits **below the WSL2
run-to-run noise floor** and is not claimed as a measured speedup. It is documented as a
principled layout, not a benchmarked win — see §9 on measurement honesty.

---

## 5. Persistence — Write-Ahead Log — `include/titan/io/journaler.hpp`

An append-only WAL over a POSIX `mmap` region. `append()` is a straight `memcpy` of the raw
40-byte `Order` into the mapped buffer plus a header cursor bump — **no syscall, no blocking
I/O on the per-order path**. The measured marginal cost of the write-ahead append is **~2.5
ns/order** (A/B: WAL-append-only vs. no-WAL in the Sequencer publish loop) — a cache-resident
store into the page cache.

A 64-byte `FileHeader` is an ABI tripwire: `magic` (`"TITAN"`), `version`, `order_size ==
sizeof(Order)`, and a `count` write-cursor. `validate()` runs in the open-constructor and
throws on any mismatch, so a binary-incompatible or corrupt log fails loud at startup rather
than replaying garbage. Space is pre-reserved with `posix_fallocate` so hot-path page writes
never `SIGBUS`.

**Durability cadence (blocking I/O off the hot path).** `append` only dirties the page cache.
Durability is deferred to batch boundaries: `MS_ASYNC` every `flush_interval` (1024) orders
schedules writeback for free, and a blocking `MS_SYNC` checkpoint fires every `sync_every`
(64) such batches — **once per ~65,536 orders** — flushing only the newly-dirtied page range
(`sync_dirty` page-aligns the tail; it does not re-scan the whole mapping) plus the header
page. This bounds the crash loss window without a per-order syscall.

**Torn-tail recovery (sequence invariant, not zero-sentinels).** After a hard crash the header
`count` may lead the durably-written pages. Recovery does **not** trust `count` as the tail
marker and does **not** rely on a zero/`id==0` sentinel (fragile: `0` is a valid id and the
first `seq` is `0`). Instead it enforces the append-order invariant

```
record i is valid  ⇔  wal[i].seq == base + i        (base = wal[0].seq)
```

and replays until the first violation — that break **is** the durable boundary. This is robust
to any id scheme and to a zeroed/un-flushed tail. Empty-WAL replay is a clean no-op; replay is
idempotent.

---

## 6. Outbound Market Data — Dual-Feed Architecture

Two independent multicast channels, mirroring exchange practice: a latency-critical
**incremental** feed and a periodic **snapshot** feed for late-join / gap-fill. Both are raw
binary — **zero JSON, zero serialization framework** — the wire payload is a `memcpy` of PODs.

### 6.1 Incremental trade feed — `include/titan/net/udp_publisher.hpp` (T(pub), `:30001`)

A non-blocking (`SOCK_NONBLOCK`) UDP multicast socket: `IP_MULTICAST_IF` selects the egress
interface, `IP_MULTICAST_TTL` bounds scope, `IP_MULTICAST_LOOP` permits same-host receivers.
The publisher drains the egress ring and blasts `TradeEvent`s. Best-effort by design: a full
send buffer drops the datagram (counted, never blocks) — market data is inherently lossy and
recovered out-of-band by the snapshot channel.

**MTU-safe chunking (no IP fragmentation).** A drained batch is chunked so every datagram stays
under the Ethernet MTU: the payload budget is fixed at **1440 bytes**, so each datagram carries
up to `floor(1440 / sizeof(TradeEvent))` whole events. At the current **40-byte** `TradeEvent`
that is **36 events (1440 B) per datagram**. Keeping datagrams sub-MTU avoids IP fragmentation,
which multiplies loss probability (a single lost fragment discards the whole datagram).

**`TradeEvent` = 40 bytes**, carrying the Sequencer's monotonic **`feed_seq`** so a client
detects a gap by a break in the sequence and knows exactly where to resume after applying a
snapshot:

```
struct TradeEvent {   // 40 B, trivially copyable, 8-aligned
    OrderId taker_id, maker_id; PriceTick price;
    uint64  feed_seq;                 // monotonic; the client's stitching key
    Qty     quantity; Side taker_side; uint8 status; uint8 _pad[2];
};
```

### 6.2 L2 snapshot / gap-fill — `include/titan/book/snapshot.hpp` (T5, `:30002`)

A periodic full L2 image (`price`, `total_qty`, `order_count` per level) lets a client that
joined late or dropped a packet resync: apply the snapshot at its `feed_seq`, then replay
incrementals with a greater sequence. The hard constraint is that the Matcher — the single
writer of the book — must produce a **consistent** snapshot **without ever blocking** to serve
the network.

**Wait-free serialization on the Matcher.** Every `SNAPSHOT_EVERY = 10,000` matched orders
(a cheap modulo on the drain loop — no timer, no signal), the Matcher walks the RB index and
serializes the top `MaxLevels/2` levels per side into a free pool buffer, tagged with the
`feed_seq` the image is consistent as-of. Depth is capped (56 levels: `64 B` header + `56 × 24
B` = **1408 B**) so a whole snapshot is one MTU-safe datagram — no fragmentation, no
application-level reassembly. If no buffer is free the cycle is skipped; the Matcher never
blocks.

**Lock-free triple-buffer hand-off with a hazard-style claim.** The `SnapshotPool` is a 3-slot,
cache-line-aligned pool with a `published` atomic pointer/index and a per-buffer `in_use`
flag. The buffer-body publication rides `published` under **release/acquire** (all body writes
visible before any read). The reclamation handshake, however, is a **StoreLoad** pattern — the
reader stores `in_use` then loads `published`; the writer stores `published` then loads
`in_use` — which plain release/acquire cannot make safe (a Dekker double-miss would permit the
writer to recycle a buffer the reader is mid-read). The handshake atomics are therefore
**`memory_order_seq_cst`**: the reader claims a slot, then re-validates it is still the
published one; the writer, choosing a slot to overwrite, skips both the published slot and any
`in_use` slot, and safely skips the whole cycle if none is free. seq_cst is affordable here
because this path fires once per 10k orders, far off the per-order hot path. With one reader
and three slots the writer is wait-free (at least one slot is always claimable).

> **Verification.** 1 writer publishing **1,000,000** distinct generations, 1 reader
> continuously hazard-claiming, over a 3-slot pool. Torn reads are detected two ways — a
> per-generation stamp on every level and a header checksum — in addition to TSan. Result:
> **0 torn buffers, 0 data races, 0 writer starvation** (`tsan.sh`).

T5 (a dedicated thread, isolating the fat periodic snapshot from the latency-critical trade
feed) hazard-claims the latest buffer, transmits new generations only, and releases.

---

## 7. Concurrency & Memory-Ordering Model (consolidated)

Every shared-memory hand-off and its exact ordering. This is the whole of the synchronization
surface — there is no mutex, condition variable, or kernel futex anywhere on a hot path.

| Hand-off | Publish (writer) | Observe (reader) | Reclaim | Ordering rationale |
|---|---|---|---|---|
| **SPSC ring** (ingress, egress) | `cursor.store(release)` after slot write | `cursor.load(acquire)` before slot read | consumer `next.store(release)` | one h-b edge; cached opposite-sequence keeps the common path off the other core's line |
| **MPSC ring** | CAS-claim `enqueue` (relaxed) → write → `cell.seq.store(pos+1, release)` | `cell.seq.load(acquire) == pos+1` | `cell.seq.store(pos+Size, release)` | per-cell seq serializes out-of-order producer completion; single consumer needs no dequeue CAS |
| **Snapshot pool** | fill buffer → `published.store(idx, seq_cst)` | `published.load(seq_cst)` → read | `in_use[]` seq_cst hazard flags | StoreLoad reclamation handshake requires seq_cst (release/acquire admits a Dekker double-miss) |
| **WAL** | `memcpy` + `header.count` (page cache) | recovery reads `[0..count)` under `seq==base+i` | `MS_ASYNC`/`MS_SYNC` cadence | durability decoupled from ordering; recovery invariant is self-validating |

Cache-line discipline: ring producer/consumer cursors are each `alignas(64)` and padded (no
false sharing between the two cores); `PIN_Node`, `SnapshotBuffer`, and the snapshot `in_use`
flags are `alignas(64)`; `SlabEntry` is `alignas(32)` (single-line, split-free lookups).

---

## 8. Performance

**All figures are WSL2, loopback, single symbol, no core pinning — read them as within-run
ratios, not vendor benchmarks.** Absolute wall-clock drifts 30–150% run-to-run from VM
scheduling and thermal throttling; the load-bearing metrics are the A/B deltas measured in the
same process/run.

| Path | Metric | Value | Method |
|---|---|---|---|
| **Ingest** (front half: TCP → gateway → MPSC → Sequencer → ingress) | throughput | **~11 M orders/s** (9–20 M/s observed) | `bench_end_to_end.sh`, 1M orders, TCP-accept-complete |
| Front half, per order | amortized cost | **~90 ns** | inverse of ingest throughput (amortized, **not** a tail-latency percentile) |
| **Wire-to-wire** (first TCP byte → external listener receives the 1,000,000th trade) | throughput | **~4 M orders/s** (3.2–4.3) | `bench_end_to_end.sh`, shared `CLOCK_MONOTONIC` |
| Matcher alone, deep power-law book | latency | ~180 ns/op | `matcher_bench` (min of 7) |
| WAL append | marginal cost | **~2.5 ns/order** | journaling-tax A/B (append-only vs. no-WAL) |
| Slab-shadow refactor, cancel-heavy | Δ | **−20% ns, +25% throughput** | `matcher_bench` A/B, 55% cancels |
| UDP delivery, 1M trades loopback | loss | **0** (1,000,000/1,000,000) | dual-feed harness |

**Reading the two throughput numbers.** Ingest (~11 M/s) is the engine/front-half rate, paced
by TCP backpressure — the number to beat when optimizing matching logic. Wire-to-wire (~4 M/s)
is the full externally-observed path including match → egress → 36-events/datagram UDP send →
external receive; its tail is influenced by the receiver draining ~28k datagrams and is a
delivered-feed rate, not an engine rate. The methodology deliberately separates them so an
optimization is attributed to the right stage.

---

## 9. Verification & Testing

Correctness is gated by sanitizers, not by inspection. The two suites are mutually exclusive
(ASan and TSan cannot co-run) and are separate targets.

| Gate | Tool | Scope | Result |
|---|---|---|---|
| `build.sh` | ASan + UBSan | full core: book, matcher, RB tree, journaler, sequencer, gateway, snapshot | **37 tests / 67,474 checks / 0 failures** |
| `tsan.sh` (SPSC) | ThreadSanitizer | 1P/1C, 5M items, 1024-slot ring (heavy wraparound) | **0 data races**, strict order |
| `tsan.sh` (MPSC) | ThreadSanitizer | 4P/1C, 1M items, 1024-slot ring | **0 races**, exactly-once (0 loss/dup/torn) |
| `tsan.sh` (Snapshot) | ThreadSanitizer | 1W/1R, 1M generations, 3-slot pool | **0 races, 0 torn** |
| `bench_end_to_end.sh` | timing | 1M-order wire-to-wire | 0 UDP loss, deterministic checksum |
| `tests/multicast_test.sh` | integration | both feeds vs. external listener | trades + L2 snapshots verified, feed_seq monotonic |

TSan under WSL2 requires `setarch "$(uname -m)" -R` to disable ASLR (works around a
kernel-specific "unexpected memory mapping" fatal). Zero-crash under resource exhaustion is
part of the ASan suite: arena/pool exhaustion is exercised and asserted to degrade to a
`REJECTED` event, never `terminate`.

---

## 10. Repository Map

```
include/titan/
  domain/types.hpp        fixed-point PriceTick, ids, enums
  book/order.hpp          Order (40 B POD)
  book/pin_node.hpp       PIN node: occupancy mask + intrusive FIFO (metadata-first, alignas 64)
  book/price_level.hpp    price + node-chain + O(1) aggregates
  book/rb_price_index.hpp neighbor-aware intrusive Red-Black price index
  book/order_book.hpp     dense 32 B SlabEntry id-index + RB index + node pool; cancel/add
  book/matcher.hpp        MatcherT<Book>: price-time match, sink-templated, noexcept
  book/trade_event.hpp    TradeEvent (40 B) w/ feed_seq
  book/snapshot.hpp       L2 SnapshotLevel/Header + lock-free triple-buffer SnapshotPool
  memory/arena.hpp        PMR monotonic + pool (null upstream)
  pipeline/spsc_ring.hpp  generic SpscRing<T> (batch-drain + prefetch, batch-publish)
  pipeline/ingress_ring.hpp / egress_ring.hpp   typed SPSC aliases
  pipeline/mpsc_ring.hpp  Vyukov MPSC fan-in ring
  pipeline/sequencer.hpp  seq stamp → WAL → ingress; run() drain loop + recovery replay()
  io/journaler.hpp        mmap WAL + ABI tripwire + durability cadence
  net/tcp_gateway.hpp     edge-triggered epoll gateway (batched recv)
  net/udp_publisher.hpp   non-blocking multicast, MTU-safe chunking
src/main.cpp              titan-server: N-gateway 5-thread topology + cascading shutdown
scripts                  build.sh (ASan) · tsan.sh · bench.sh · pipeline.sh · bench_end_to_end.sh · profile.sh
```

---

## 11. Non-Goals & Current Limitations

Stated explicitly; a design doc that hides its boundaries is not trustworthy.

- **Single symbol.** One book, no symbol dispatch / sharding. Multi-symbol is a horizontal
  concern layered above this core, not built.
- **No kernel-bypass.** Ingress is `epoll`/TCP and egress is `sendto`/UDP through the kernel
  stack. `io_uring` / DPDK / `AF_XDP` are the intended next step; not present.
- **Server emits the gap-fill feed; the client resync loop is not built.** `feed_seq` and the
  L2 snapshot channel exist; the consumer-side apply-snapshot-then-replay logic is a client
  concern, out of scope.
- **Order-only WAL.** The log records `Order`s. Cancels/modifies do not currently traverse the
  ingress path; if they did, the WAL would need a tagged command record (noted at the source).
- **WAL is a hard-bounded mmap.** Capacity is fixed at open; exceeding it aborts an append.
  Sized per session; a rotating/ring WAL is future work.
- **Measurement environment is WSL2.** No bare-metal, no NUMA pinning, no percentile-latency
  harness. Absolute throughput/latency figures are indicative; sub-noise-floor optimizations
  (e.g. §4.4) are documented as principled, not benchmarked. Reproducing HFT-grade numbers
  requires bare metal + isolated/pinned cores + a hardware sampling profiler (`perf`).

---

*Titan HFT v1 — this document reflects the state at v1.4.11.*
