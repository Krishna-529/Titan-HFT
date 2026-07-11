#pragma once
//
// titan/memory/arena.hpp
// Zero-OS-allocation memory arena for the index containers.
//
//   buffer_      one big std::byte buffer, allocated ONCE at startup
//     |
//   monotonic_   monotonic_buffer_resource over buffer_, upstream = null
//     |          -> never falls back to the OS heap; exhaustion throws bad_alloc
//     |             (a loud, debuggable tripwire, not a silent malloc/latency spike)
//   pool_        unsynchronized_pool_resource on top
//                -> recycles freed fixed-size blocks under insert/cancel churn,
//                   so steady-state memory is BOUNDED (monotonic alone only grows).
//                   `unsynchronized` = no locks (single-writer matching thread).
//
// Hand `pmr()` to std::pmr containers; all their allocations carve from buffer_.
//
#include <cstddef>
#include <memory_resource>
#include <vector>

namespace titan {

class Arena {
public:
    explicit Arena(std::size_t bytes)
        : buffer_(bytes),
          monotonic_(buffer_.data(), buffer_.size(), std::pmr::null_memory_resource()),
          pool_(&monotonic_) {}

    // Non-copyable / non-movable: the resources hold raw pointers into buffer_.
    Arena(const Arena&)            = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&&)                 = delete;
    Arena& operator=(Arena&&)      = delete;

    std::pmr::memory_resource* pmr() noexcept { return &pool_; }
    std::size_t capacity() const noexcept     { return buffer_.size(); }

private:
    std::vector<std::byte>                  buffer_;     // the single startup allocation
    std::pmr::monotonic_buffer_resource     monotonic_;  // arena over buffer_ (null upstream)
    std::pmr::unsynchronized_pool_resource  pool_;       // recycling pool over monotonic_
};

} // namespace titan
