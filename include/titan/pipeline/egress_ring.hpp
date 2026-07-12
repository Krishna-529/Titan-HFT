#pragma once
//
// titan/pipeline/egress_ring.hpp
// Egress Disruptor ring: the Matcher publishes TradeEvents (without blocking on the
// match hot path), downstream publishers / trade reporters consume them. A thin typed
// alias over the same generic SpscRing as the ingress ring, so both share one verified
// lock-free implementation. Single-element (try_publish / try_consume) and batch-drain
// (consume_batch) are both available from the start.
//
#include <cstddef>

#include "titan/book/trade_event.hpp"
#include "titan/pipeline/spsc_ring.hpp"

namespace titan::pipeline {

template <std::size_t Size = (std::size_t{1} << 20)>   // 1,048,576 slots by default
using EgressRing = SpscRing<TradeEvent, Size>;

} // namespace titan::pipeline
