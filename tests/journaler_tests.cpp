//
// tests/journaler_tests.cpp
// WAL round-trip + ABI safety-harness tests. journaler.hpp is included FIRST so its
// _DEFAULT_SOURCE define exposes the POSIX calls (mmap/pwrite/...) under -std=c++20.
//
#include "titan/io/journaler.hpp"

#include "ut.hpp"
#include "titan/book/order.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

using namespace titan;
using namespace titan::io;

namespace {
const char* MAIN_WAL    = "/tmp/titan_jrn_main.wal";
const char* CORRUPT_WAL = "/tmp/titan_jrn_corrupt.wal";
constexpr std::uint64_t N = 100'000;

Order mk(std::uint64_t i) {
    Order o{};
    o.id = i; o.seq = i;
    o.price    = 1000 + static_cast<PriceTick>(i % 500);
    o.quantity = o.remaining = 1u + static_cast<Qty>(i % 10);
    o.side = (i & 1u) ? Side::SELL : Side::BUY;
    o.type = OrderType::LIMIT;
    return o;
}
}  // namespace

// 1) Create a WAL, append 100k orders, close (destructor msyncs).
TEST_CASE(journaler_create_append_close) {
    Journaler wal(MAIN_WAL, 2 * N);                 // create, pre-allocate 200k slots
    CHECK(wal.count() == 0u);
    for (std::uint64_t i = 0; i < N; ++i) wal.append(mk(i));
    CHECK(wal.count() == N);
    CHECK(wal.capacity() >= N);
}

// 2) Re-open the same WAL, validate the safety-harness header, verify every payload.
TEST_CASE(journaler_reopen_validates_and_payloads_intact) {
    Journaler wal(MAIN_WAL);                         // open -> validate() runs in the ctor
    REQUIRE(wal.count() == N);
    bool all_ok = true;
    for (std::uint64_t i = 0; i < N; ++i) {
        const Order exp = mk(i);
        if (std::memcmp(&wal[i], &exp, sizeof(Order)) != 0) { all_ok = false; break; }
    }
    CHECK(all_ok);                                   // exact binary sequence intact
    CHECK(wal[0].id == 0u);
    CHECK(wal[N - 1].id == N - 1);
    CHECK(wal[12345].seq == 12345u);
}

// 3) Corrupt order_size in the header -> opening must abort (throw).
TEST_CASE(journaler_aborts_on_order_size_mismatch) {
    { Journaler wal(CORRUPT_WAL, 16); wal.append(mk(1)); wal.append(mk(2)); }  // a valid WAL

    // Overwrite the order_size field with a bogus value at its exact header offset.
    const int fd = ::open(CORRUPT_WAL, O_RDWR);
    REQUIRE(fd >= 0);
    std::uint16_t bad = 99;
    const ssize_t w = ::pwrite(fd, &bad, sizeof(bad),
                               static_cast<off_t>(offsetof(FileHeader, order_size)));
    ::close(fd);
    REQUIRE(w == static_cast<ssize_t>(sizeof(bad)));

    bool aborted = false;
    try {
        Journaler wal(CORRUPT_WAL);                  // validate() must reject it
        (void)wal;
    } catch (const std::exception&) {
        aborted = true;
    }
    CHECK(aborted);                                  // safety harness fired
}
