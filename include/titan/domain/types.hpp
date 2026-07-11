#pragma once
//
// titan/domain/types.hpp
// Core scalar aliases and enums shared across the engine.
// Fixed-point prices (integer ticks) — never floating point on the hot path.
//
#include <cstdint>

namespace titan {

using PriceTick = std::int64_t;   // price expressed in integer ticks (fixed-point)
using Qty       = std::uint32_t;  // order quantity
using OrderId   = std::uint64_t;  // globally unique order id
using Seq       = std::uint64_t;  // arrival sequence / timestamp (time priority)

enum class Side      : std::uint8_t { BUY = 0, SELL = 1 };
enum class OrderType : std::uint8_t { LIMIT = 0, MARKET = 1, IOC = 2 };

// Sentinel meaning "no index" for pool-based (index) links. Chosen as the max
// u32 so it can never collide with a real pool slot.
inline constexpr std::uint32_t INVALID_INDEX = 0xFFFFFFFFu;

} // namespace titan
