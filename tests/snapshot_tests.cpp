//
// tests/snapshot_tests.cpp
// ThreadSanitizer gate for the lock-free triple-buffer SnapshotPool (titan/book/snapshot.hpp).
// 1 Writer (Matcher) publishes 1,000,000 distinct snapshot generations into a 3-slot pool while
// 1 Reader (Snapshot thread) continuously claims the latest via the in_use flags. Proves:
//   * the Reader NEVER sees torn data (every claimed buffer is internally consistent), and
//   * the Writer NEVER overwrites an in_use buffer (a race would show as a torn buffer + TSan).
//
// Torn-read detection is belt-and-suspenders with TSan: every level of generation g is stamped
// with total_qty == g, and the header carries feed_seq == g plus a checksum over the levels. A
// mid-read overwrite would leave a level from a different generation and/or break the checksum.
//
#include "ut.hpp"

#include "titan/book/snapshot.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <thread>

using namespace titan;

static inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

namespace {

constexpr std::size_t LEVELS = 32;
constexpr std::size_t SLOTS  = 3;
using Pool = SnapshotPool<LEVELS, SLOTS>;

// Fill a buffer for generation g. Every level carries g (in total_qty) so any torn overwrite
// with a different generation is detectable; header.checksum folds the level fields.
std::uint64_t fill_gen(Pool::Buffer& b, std::uint64_t g) noexcept {
    b.header.magic       = SNAPSHOT_MAGIC;
    b.header.version     = SNAPSHOT_VERSION;
    b.header.level_count = LEVELS;
    b.header.feed_seq    = g;
    b.header.bid_levels  = LEVELS / 2;
    b.header.ask_levels  = LEVELS - LEVELS / 2;
    std::uint64_t chk = 0;
    for (std::size_t i = 0; i < LEVELS; ++i) {
        SnapshotLevel& L = b.levels[i];
        L.price       = static_cast<PriceTick>(g * 1000 + i);
        L.total_qty   = g;                          // generation stamp on every level
        L.order_count = static_cast<std::uint32_t>(i);
        L.side        = static_cast<std::uint8_t>(i & 1u);
        chk += static_cast<std::uint64_t>(L.price) + L.total_qty + L.order_count + L.side;
    }
    b.header.checksum = chk;
    return chk;
}

// Verify a claimed buffer is internally consistent (no torn write): magic, per-level generation
// stamp, and checksum must all agree. Returns false on any tear.
bool verify(const Pool::Buffer& b) noexcept {
    if (b.header.magic != SNAPSHOT_MAGIC) return false;
    if (b.header.level_count != LEVELS)   return false;
    const std::uint64_t g = b.header.feed_seq;
    std::uint64_t chk = 0;
    for (std::size_t i = 0; i < LEVELS; ++i) {
        const SnapshotLevel& L = b.levels[i];
        if (L.total_qty != g) return false;         // a level from another generation -> TORN
        chk += static_cast<std::uint64_t>(L.price) + L.total_qty + L.order_count + L.side;
    }
    return chk == b.header.checksum;                // checksum mismatch -> TORN
}

}  // namespace

// Single-threaded sanity: empty pool, publish, claim, integrity, release.
TEST_CASE(snapshot_pool_roundtrip_and_empty) {
    Pool pool;
    int idx = -1;
    CHECK(pool.acquire(idx) == nullptr);            // nothing published yet
    CHECK(pool.published_index() < 0);

    CHECK(pool.try_snapshot([&](Pool::Buffer& b) { fill_gen(b, 7); }));
    const Pool::Buffer* b = pool.acquire(idx);
    REQUIRE(b != nullptr);
    CHECK(b->header.feed_seq == 7u);
    CHECK(verify(*b));
    pool.release(idx);

    // republish a new generation and re-claim
    CHECK(pool.try_snapshot([&](Pool::Buffer& bb) { fill_gen(bb, 8); }));
    b = pool.acquire(idx);
    REQUIRE(b != nullptr);
    CHECK(b->header.feed_seq == 8u);
    CHECK(verify(*b));
    pool.release(idx);
}

// TSan stress: 1 writer / 1 reader over a tiny 3-slot pool, 1,000,000 generations.
TEST_CASE(snapshot_pool_writer_reader_no_torn) {
    constexpr std::uint64_t N = 1'000'000;

    Pool pool;
    std::atomic<bool>          done{false};
    std::atomic<std::uint64_t> reads{0};
    std::atomic<std::uint64_t> torn{0};
    std::atomic<std::uint64_t> skips{0};

    // Reader (Snapshot thread): claim the live buffer, verify integrity, release. Loops until
    // the writer is done, then does one final claim of the last published snapshot.
    std::thread reader([&] {
        int idx = -1;
        std::uint64_t r = 0, t = 0;
        auto read_once = [&] {
            const Pool::Buffer* b = pool.acquire(idx);
            if (b == nullptr) return false;
            if (verify(*b)) ++r; else ++t;
            pool.release(idx);
            return true;
        };
        while (!done.load(std::memory_order_acquire)) {
            if (!read_once()) cpu_relax();
        }
        read_once();                                 // final: last published buffer is stable
        reads.store(r, std::memory_order_relaxed);
        torn.store(t, std::memory_order_relaxed);
    });

    // Writer (Matcher): publish N distinct generations, zero-drop (spin if all slots busy).
    std::thread writer([&] {
        std::uint64_t s = 0;
        for (std::uint64_t g = 1; g <= N; ++g) {
            while (!pool.try_snapshot([&](Pool::Buffer& b) { fill_gen(b, g); })) { ++s; cpu_relax(); }
        }
        skips.store(s, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });

    writer.join();
    reader.join();

    CHECK(reads.load() > 0);          // the reader actually observed live snapshots
    CHECK(torn.load() == 0);          // and NEVER a torn one

    if (torn.load() == 0) {
        std::printf("    OK: %llu generations published, reader verified %llu snapshots, "
                    "0 torn, writer skips=%llu (3 slots, 1W/1R)\n",
                    (unsigned long long)N, (unsigned long long)reads.load(),
                    (unsigned long long)skips.load());
    } else {
        std::printf("    TORN READ: %llu torn of %llu reads -- buffer reuse race!\n",
                    (unsigned long long)torn.load(), (unsigned long long)(reads.load() + torn.load()));
    }
}

int main() { return ut::run_all(); }
