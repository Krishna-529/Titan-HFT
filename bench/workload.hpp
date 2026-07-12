#pragma once
//
// bench/workload.hpp
// Deterministic benchmark workload (RNG excluded from timing). Realistic HFT flow:
//   * mid-price follows Geometric Brownian Motion (GBM),
//   * limit depth from the touch is drawn from a POWER LAW (beta = 2.23) -> ~80%
//     of orders land within the top few ticks while a heavy tail models real
//     events (mid drifting through resting liquidity generates trades),
//   * op mix: 40% limit adds, 55% cancels (targeting live ids), 5% market/IOC.
//
#include <cmath>
#include <cstdint>
#include <random>
#include <vector>

#include "titan/book/order.hpp"
#include "titan/domain/types.hpp"

namespace titan::bench {

enum OpKind : std::uint8_t { OP_SUBMIT = 0, OP_CANCEL = 1 };

struct Op {
    Order  order;   // SUBMIT: the order.  CANCEL: order.id holds the target id.
    OpKind kind;
};

inline constexpr PriceTick MID0      = 100'000;   // starting mid (ticks)
inline constexpr double    GBM_SIGMA = 0.0003;    // per-op log-vol of the mid
inline constexpr double    PL_BETA   = 2.23;      // power-law depth exponent
inline constexpr PriceTick MAX_DEPTH = 500;       // cap on depth from the touch (ticks)

// Inverse-CDF power-law depth in [1, MAX_DEPTH]: P(depth >= d) ~ d^-(beta-1).
inline PriceTick powerlaw_depth(std::mt19937_64& rng) noexcept {
    const double u = (static_cast<double>(rng()) + 1.0)
                   / (static_cast<double>(UINT64_MAX) + 2.0);          // in (0,1)
    double d = std::pow(1.0 - u, -1.0 / (PL_BETA - 1.0));
    PriceTick depth = static_cast<PriceTick>(d);
    if (depth < 1)          depth = 1;
    if (depth > MAX_DEPTH)  depth = MAX_DEPTH;
    return depth;
}

// Power-law limit around a given mid tick. Bids sit below mid, asks above.
inline Order limit_at(OrderId id, bool buy, PriceTick mid_tick, std::mt19937_64& rng) noexcept {
    const PriceTick d = powerlaw_depth(rng);
    Order o{};
    o.id = id; o.seq = id;
    o.price = buy ? (mid_tick - d) : (mid_tick + d);
    if (o.price < 1) o.price = 1;
    o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 10);
    o.side = buy ? Side::BUY : Side::SELL;
    o.type = OrderType::LIMIT;
    return o;
}

// Seeding helper: power-law resting order around the initial mid MID0.
inline Order make_limit(OrderId id, bool buy, std::mt19937_64& rng) noexcept {
    return limit_at(id, buy, MID0, rng);
}

// Build n_ops operations. Mid evolves by GBM; limit depth is power-law; cancels
// target ids in [1, seed_orders] u {added} so they hit the index. Deterministic.
inline std::vector<Op> generate_ops(std::size_t seed_orders, std::size_t n_ops,
                                    std::uint64_t rng_seed,
                                    std::size_t* out_adds = nullptr,
                                    std::size_t* out_cancels = nullptr,
                                    std::size_t* out_mktioc = nullptr) {
    std::mt19937_64 rng(rng_seed);
    std::normal_distribution<double> gauss(0.0, 1.0);
    double mid = static_cast<double>(MID0);

    std::vector<Op> ops;
    ops.reserve(n_ops);

    std::vector<OrderId> live;
    live.reserve(seed_orders + n_ops / 2);
    for (OrderId id = 1; id <= seed_orders; ++id) live.push_back(id);

    OrderId next_id = seed_orders + 1;
    std::size_t n_add = 0, n_cancel = 0, n_mktioc = 0;

    for (std::size_t i = 0; i < n_ops; ++i) {
        // Evolve the GBM mid each op (log-return).
        mid *= std::exp(-0.5 * GBM_SIGMA * GBM_SIGMA + GBM_SIGMA * gauss(rng));
        if (mid < 1000.0) mid = 1000.0;                       // floor (defensive)
        const PriceTick m = static_cast<PriceTick>(mid + 0.5);

        const std::uint32_t roll = static_cast<std::uint32_t>(rng() % 100);
        if (roll < 40 || live.empty()) {                       // 40% limit add
            const OrderId id = next_id++;
            ops.push_back(Op{ limit_at(id, (rng() & 1u), m, rng), OP_SUBMIT });
            live.push_back(id);
            ++n_add;
        } else if (roll < 95) {                                // 55% cancel
            const std::size_t k = static_cast<std::size_t>(rng() % live.size());
            const OrderId id = live[k];
            live[k] = live.back(); live.pop_back();
            Order o{}; o.id = id;
            ops.push_back(Op{ o, OP_CANCEL });
            ++n_cancel;
        } else {                                               // 5% market / IOC
            const OrderId id = next_id++;
            const bool buy = (rng() & 1u);
            Order o{};
            o.id = id; o.seq = id;
            o.quantity = o.remaining = 1u + static_cast<Qty>(rng() % 5);
            o.side = buy ? Side::BUY : Side::SELL;
            if (rng() & 1u) {
                o.type = OrderType::MARKET; o.price = 0;
            } else {
                o.type = OrderType::IOC;
                o.price = buy ? (m + MAX_DEPTH) : (m > MAX_DEPTH ? m - MAX_DEPTH : 1);
            }
            ops.push_back(Op{ o, OP_SUBMIT });
            ++n_mktioc;
        }
    }
    if (out_adds)    *out_adds = n_add;
    if (out_cancels) *out_cancels = n_cancel;
    if (out_mktioc)  *out_mktioc = n_mktioc;
    return ops;
}

}  // namespace titan::bench
