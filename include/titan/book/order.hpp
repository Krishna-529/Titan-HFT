#pragma once
//
// titan/book/order.hpp
// A resting limit order: a tightly-packed POD (Plain Old Data) struct.
// Trivially copyable so it can live by value inside PIN_Node slots and, later,
// inside Disruptor ring slots — copied with a single memcpy, no heap, no vtable.
//
#include <cstdint>
#include <type_traits>

#include "titan/domain/types.hpp"

namespace titan {

struct Order {
    OrderId      id;        // 8  unique id
    Seq          seq;       // 8  arrival order  -> time priority
    PriceTick    price;     // 8  limit price (ticks)
    Qty          quantity;  // 4  original quantity
    Qty          remaining; // 4  unfilled quantity
    Side         side;      // 1  BUY / SELL
    OrderType    type;      // 1  LIMIT / MARKET / IOC
    std::uint8_t _pad[6];   // 6  explicit padding -> deterministic 40-byte layout
};

// --- Layout guarantees (compile-time; zero runtime cost) ---
static_assert(std::is_trivially_copyable_v<Order>, "Order must be trivially copyable (POD)");
static_assert(std::is_standard_layout_v<Order>,    "Order must be standard layout");
static_assert(sizeof(Order) == 40,                 "Order layout drift - re-check field padding");
static_assert(sizeof(Order) <= 64,                 "Order must stay within a single cache line");
static_assert(alignof(Order) == 8,                 "Order should be 8-byte aligned");

} // namespace titan
