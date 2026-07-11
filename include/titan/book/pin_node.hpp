#pragma once
//
// titan/book/pin_node.hpp
// Priority-Indicated Node (PIN): a fixed block of up to CAPACITY resting orders
// with an O(1) occupancy bitmask. Nodes chain together (by pool index) to form
// the FIFO queue at a single price level (time priority within and across nodes).
//
// Zero-crash discipline:
//   * every mutator is noexcept
//   * the full-node case is guarded BEFORE calling __builtin_ctzll (ctzll(0) is UB)
//   * every slot access is bounds-checked
//
#include <array>
#include <cstdint>
#include <type_traits>

#include "titan/book/order.hpp"
#include "titan/domain/types.hpp"

namespace titan {

struct alignas(64) PIN_Node {
    static constexpr std::uint32_t CAPACITY = 64;  // must match the 64-bit mask

    std::array<Order, CAPACITY> slots;  // contiguous order storage
    std::uint64_t occupancy_mask;       // bit i set  => slots[i] is occupied
    std::uint32_t next;                 // next PIN_Node index in the level chain
    std::uint32_t prev;                 // prev PIN_Node index in the level chain

    // Put a pooled node into a known-empty, unlinked state before use.
    void reset() noexcept {
        occupancy_mask = 0;
        next = INVALID_INDEX;
        prev = INVALID_INDEX;
    }

    bool full()  const noexcept { return occupancy_mask == ~std::uint64_t(0); }
    bool empty() const noexcept { return occupancy_mask == 0; }

    std::uint32_t count() const noexcept {
        return static_cast<std::uint32_t>(__builtin_popcountll(occupancy_mask));
    }

    // Insert into the lowest free slot. Returns the slot index [0, CAPACITY) on
    // success, or INVALID_INDEX if the node is full. Never invokes UB.
    std::uint32_t insert(const Order& o) noexcept {
        const std::uint64_t free = ~occupancy_mask;      // 1-bits mark free slots
        if (free == 0) return INVALID_INDEX;             // FULL-NODE GUARD (before ctzll)
        const std::uint32_t slot =
            static_cast<std::uint32_t>(__builtin_ctzll(free));  // safe: free != 0
        slots[slot] = o;                                 // trivial copy of the POD
        occupancy_mask |= (std::uint64_t(1) << slot);    // mark occupied
        return slot;
    }

    // Lazily remove the order at `slot`. Bounds-checked and idempotent: returns
    // false (never crashes) for an out-of-range or already-free slot.
    bool remove(std::uint32_t slot) noexcept {
        if (slot >= CAPACITY) return false;              // BOUNDS CHECK
        const std::uint64_t bit = std::uint64_t(1) << slot;
        if ((occupancy_mask & bit) == 0) return false;   // already free
        occupancy_mask &= ~bit;                          // clear bit (lazy delete)
        return true;
    }

    // Safe accessor: nullptr if the slot is out of range or currently empty.
    Order* at(std::uint32_t slot) noexcept {
        if (slot >= CAPACITY) return nullptr;
        if ((occupancy_mask & (std::uint64_t(1) << slot)) == 0) return nullptr;
        return &slots[slot];
    }
};

static_assert(alignof(PIN_Node) == 64,                  "PIN_Node must be cache-line aligned");
static_assert(std::is_trivially_copyable_v<PIN_Node>,   "PIN_Node must be trivially copyable");
static_assert(std::is_standard_layout_v<PIN_Node>,      "PIN_Node must be standard layout");

} // namespace titan
