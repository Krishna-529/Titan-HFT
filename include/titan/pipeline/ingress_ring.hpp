#pragma once
//
// titan/pipeline/ingress_ring.hpp
// Ingress Disruptor ring: the Sequencer publishes Orders, the Matcher consumes them.
// A thin typed alias over the generic SpscRing (see spsc_ring.hpp for the mechanics
// and memory-ordering rationale). SPSC for now; MPSC (multi-gateway) comes later.
//
#include <cstddef>

#include "titan/book/order.hpp"
#include "titan/pipeline/spsc_ring.hpp"

namespace titan::pipeline {

template <std::size_t Size = (std::size_t{1} << 20)>   // 1,048,576 slots by default
using IngressRing = SpscRing<Order, Size>;

} // namespace titan::pipeline
