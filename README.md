# Titan HFT v1 (C++)

From-scratch C++20 rebuild of Titan HFT around the **"Flash One" Priority-Indicated
Node (PIN)** limit order book, later to be wrapped in a hand-rolled **LMAX Disruptor**
pipeline. Primary directive: **zero-crash** (noexcept hot path, strict bounds checks,
safe init, zero dynamic allocation in the matching loop).

## Status — Foundation phase (LOB data structures only)

Implemented (`include/titan/`):

| File | What |
|---|---|
| `domain/types.hpp`   | fixed-point `PriceTick`, `Qty`, `OrderId`, enums |
| `book/order.hpp`     | 40-byte trivially-copyable POD `Order` |
| `book/pin_node.hpp`  | `alignas(64)` PIN node: 64 slots + `occupancy_mask`, `__builtin_ctzll` insert with full-node guard |
| `book/price_level.hpp` | price level = intrusive FIFO chain of PIN nodes (by index) |
| `memory/arena.hpp`   | pre-allocated arena: `monotonic_buffer_resource` (null upstream) + `unsynchronized_pool_resource` |
| `book/order_book.hpp`| `std::pmr::map` price index + `std::pmr::unordered_map` id index; add + lazy cancel (no matching yet) |

No networking, no Disruptor, no matching logic yet — by design.

## Build & test

`g++` (>=11) is enough for now:

```bash
./build.sh          # compiles tests with ASan+UBSan and runs them
```

Once you install the full toolchain (`sudo apt install -y cmake ninja-build gdb`):

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build build && ctest --test-dir build --output-on-failure
```

## Next
`order_book` matching (price-time priority, partial fills, sweep) → then wrap in the
hand-rolled Disruptor rings.
