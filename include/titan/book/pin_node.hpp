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
    static constexpr std::uint32_t CAPACITY = 64;    // must match the 64-bit mask
    static constexpr std::uint8_t  NIL_SLOT = 0xFF;  // "no slot" sentinel (>= CAPACITY)

    std::array<Order, CAPACITY> slots;  // contiguous order storage
    std::uint64_t occupancy_mask;       // bit i set  => slots[i] is occupied

    // Intra-node FIFO chain: threads the OCCUPIED slots in strict arrival order so
    // time priority survives a cancel that frees a low slot which a later insert
    // then refills. Physical slot index (picked by the occupancy mask) is thus
    // decoupled from time order. Doubly linked => O(1) unlink on cancel/fill.
    std::array<std::uint8_t, CAPACITY> next_in_time;  // older slot -> newer slot
    std::array<std::uint8_t, CAPACITY> prev_in_time;  // newer slot -> older slot
    std::uint8_t  head_slot;             // oldest occupied slot (front) or NIL_SLOT
    std::uint8_t  tail_slot;             // newest occupied slot (back)  or NIL_SLOT

    std::uint32_t next;                  // next PIN_Node index in the level chain
    std::uint32_t prev;                  // prev PIN_Node index in the level chain

    // Put a pooled node into a known-empty, unlinked state before use. The link
    // arrays need not be cleared: they are only read for occupied slots, whose
    // links are always written on insert.
    void reset() noexcept {
        occupancy_mask = 0;
        head_slot = NIL_SLOT;
        tail_slot = NIL_SLOT;
        next = INVALID_INDEX;
        prev = INVALID_INDEX;
    }

    bool full()  const noexcept { return occupancy_mask == ~std::uint64_t(0); }
    bool empty() const noexcept { return occupancy_mask == 0; }

    std::uint32_t count() const noexcept {
        return static_cast<std::uint32_t>(__builtin_popcountll(occupancy_mask));
    }

    // Insert into the lowest free physical slot, then append to the FIFO tail so a
    // sweep consumes it AFTER every earlier arrival. Returns the slot index on
    // success, or INVALID_INDEX if the node is full. Never invokes UB.
    std::uint32_t insert(const Order& o) noexcept {
        const std::uint64_t free = ~occupancy_mask;      // 1-bits mark free slots
        if (free == 0) return INVALID_INDEX;             // FULL-NODE GUARD (before ctzll)
        const std::uint32_t slot =
            static_cast<std::uint32_t>(__builtin_ctzll(free));  // safe: free != 0
        slots[slot] = o;                                 // trivial copy of the POD
        occupancy_mask |= (std::uint64_t(1) << slot);    // mark occupied

        // Append to the tail of the arrival-order chain.
        const std::uint8_t s = static_cast<std::uint8_t>(slot);
        next_in_time[s] = NIL_SLOT;
        prev_in_time[s] = tail_slot;
        if (tail_slot == NIL_SLOT) head_slot = s;        // first order in the node
        else                       next_in_time[tail_slot] = s;
        tail_slot = s;
        return slot;
    }

    // Lazily remove the order at `slot` and splice it out of the FIFO chain in O(1).
    // Bounds-checked and idempotent: returns false (never crashes) for an
    // out-of-range or already-free slot.
    bool remove(std::uint32_t slot) noexcept {
        if (slot >= CAPACITY) return false;              // BOUNDS CHECK
        const std::uint64_t bit = std::uint64_t(1) << slot;
        if ((occupancy_mask & bit) == 0) return false;   // already free
        occupancy_mask &= ~bit;                          // clear bit (lazy delete)

        // Unlink from the arrival-order chain (fix up neighbours / head / tail).
        const std::uint8_t s = static_cast<std::uint8_t>(slot);
        const std::uint8_t p = prev_in_time[s];
        const std::uint8_t n = next_in_time[s];
        if (p != NIL_SLOT) next_in_time[p] = n; else head_slot = n;
        if (n != NIL_SLOT) prev_in_time[n] = p; else tail_slot = p;
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
