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

// --- Price admissibility bounds -------------------------------------------------------
// PriceTick is a SIGNED 64-bit value that arrives by raw memcpy straight off the socket
// (net/tcp_gateway.hpp parses host-native Order bytes with no validation), so an
// unguarded order can carry ANY int64: INT64_MAX, negatives, absurd magnitudes. Against
// the RB price index that is "merely" unbounded tree sprawl and cache pollution from a
// hostile client; against any flat/array-indexed book it would be an out-of-bounds write.
// These bounds are the engine's hard admissibility gate on that input path.
//
// Deliberately GENEROUS -- this is a garbage filter, not a market price band. It must
// clear every workload in the tree (bench/matcher_bench.cpp runs at MID = 100'000 +/- 128,
// so any ceiling at or below ~100'128 would reject the entire benchmark).
//
// NOTE: the tick SPAN of any future flat/array-indexed book is a SEPARATE and much
// narrower number -- 2^20 levels x 32 B would be 32 MB/side and nowhere near cache
// resident. Do not conflate "price we are willing to accept" with "price we can index
// into an array".
inline constexpr PriceTick MIN_VALID_PRICE = 1;         // 0 is reserved: the MARKET "unused" sentinel
inline constexpr PriceTick MAX_VALID_PRICE = 1 << 20;   // 1,048,576 ticks

[[nodiscard]] inline constexpr bool is_valid_price(PriceTick p) noexcept {
    return p >= MIN_VALID_PRICE && p <= MAX_VALID_PRICE;
}

} // namespace titan
