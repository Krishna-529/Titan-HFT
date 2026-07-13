#pragma once
//
// titan/book/trade_event.hpp
// TradeEvent: the fill event emitted by the matcher on every trade. A tightly-packed,
// trivially-copyable POD so it drops straight into a Disruptor egress ring slot
// (single memcpy, no heap, no vtable). Lives in its own header so the egress ring can
// use it without pulling in the whole matcher.
//
#include <cstdint>
#include <type_traits>

#include "titan/domain/types.hpp"

namespace titan {

// TradeEvent::status values. FILL is the overwhelming common case (a real trade);
// REJECTED is emitted by the matcher when an order cannot be admitted to the book under
// resource exhaustion (node/level pool full or arena exhausted) -- a graceful, zero-crash
// signal to the egress consumer instead of a silent drop. A REJECTED event carries the
// aggressor's id + side, price = the order's price, and quantity == 0 (no liquidity traded).
inline constexpr std::uint8_t TRADE_STATUS_FILL     = 0;
inline constexpr std::uint8_t TRADE_STATUS_REJECTED = 1;

struct TradeEvent {
    OrderId      taker_id;    // 8  aggressor (incoming) order id
    OrderId      maker_id;    // 8  resting order that was hit (0 on a rejection)
    PriceTick    price;       // 8  execution price = resting (maker) price; order price on reject
    std::uint64_t feed_seq;   // 8  Sequencer seq of the aggressor order -> per-message gap detection
    Qty          quantity;    // 4  traded quantity (0 on a rejection)
    Side         taker_side;  // 1  aggressor side (BUY => buyer lifted an ask)
    std::uint8_t status;      // 1  TRADE_STATUS_FILL | TRADE_STATUS_REJECTED
    std::uint8_t _pad[2];     // 2  explicit padding -> deterministic 40-byte layout
};

static_assert(sizeof(TradeEvent) == 40,                 "TradeEvent layout drift");
static_assert(std::is_trivially_copyable_v<TradeEvent>, "TradeEvent must be a POD");
static_assert(std::is_standard_layout_v<TradeEvent>,    "TradeEvent must be standard layout");
static_assert(alignof(TradeEvent) == 8,                 "TradeEvent should be 8-byte aligned");

} // namespace titan
