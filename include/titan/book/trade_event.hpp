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

struct TradeEvent {
    OrderId      taker_id;    // 8  aggressor (incoming) order id
    OrderId      maker_id;    // 8  resting order that was hit
    PriceTick    price;       // 8  execution price = resting (maker) price
    Qty          quantity;    // 4  traded quantity
    Side         taker_side;  // 1  aggressor side (BUY => buyer lifted an ask)
    std::uint8_t _pad[3];     // 3  explicit padding -> deterministic 32-byte layout
};

static_assert(sizeof(TradeEvent) == 32,                 "TradeEvent layout drift");
static_assert(std::is_trivially_copyable_v<TradeEvent>, "TradeEvent must be a POD");
static_assert(std::is_standard_layout_v<TradeEvent>,    "TradeEvent must be standard layout");
static_assert(alignof(TradeEvent) == 8,                 "TradeEvent should be 8-byte aligned");

} // namespace titan
