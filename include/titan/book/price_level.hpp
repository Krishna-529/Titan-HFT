#pragma once
//
// titan/book/price_level.hpp
// One price level = an intrusive FIFO chain of PIN_Nodes referenced by pool
// index (not owning pointers). `head` is the oldest node (front of time
// priority), `tail` the newest. Aggregates are kept incrementally so top-of-book
// queries are O(1). The push/pop logic lives in OrderBook, which owns the pool.
//
#include <cstdint>

#include "titan/domain/types.hpp"

namespace titan {

struct PriceLevel {
    PriceTick     price       = 0;
    std::uint32_t head        = INVALID_INDEX;  // first PIN_Node index in the chain
    std::uint32_t tail        = INVALID_INDEX;  // last  PIN_Node index in the chain
    std::uint64_t total_qty   = 0;              // aggregate resting quantity
    std::uint32_t order_count = 0;              // number of resting orders

    bool empty() const noexcept { return order_count == 0; }
};

} // namespace titan
