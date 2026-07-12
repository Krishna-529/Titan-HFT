//
// tests/rb_tree_tests.cpp
// Correctness proof for the intrusive Red-Black price index. validate() checks
// BST order + pred/succ links + all Red-Black properties; we call it relentlessly
// under random churn. Built with ASan+UBSan via build.sh.
//
#include "ut.hpp"

#include "titan/book/rb_price_index.hpp"

#include <functional>
#include <iterator>
#include <random>
#include <set>

using namespace titan;

TEST_CASE(rb_basic_insert_find_dup) {
    RBPriceIndex<std::less<PriceTick>> t(std::less<PriceTick>(), nullptr, 1024);
    CHECK(t.empty());
    auto [it1, ins1] = t.try_emplace(100);
    CHECK(ins1);
    it1->second.price = 100;
    auto [it2, ins2] = t.try_emplace(100);            // duplicate
    CHECK(!ins2);
    CHECK(it2->second.price == 100);                  // returns the existing node
    CHECK(t.size() == 1u);
    CHECK(t.find(100) != t.end());
    CHECK(t.find(999) == t.end());
    CHECK(t.validate());
}

TEST_CASE(rb_begin_is_best_ask_for_less) {
    RBPriceIndex<std::less<PriceTick>> t(std::less<PriceTick>(), nullptr, 1024);
    for (PriceTick p : {50, 10, 90, 30, 70}) { auto [it, ok] = t.try_emplace(p); (void)ok; it->second.price = p; }
    CHECK(t.validate());
    CHECK(t.begin()->first == 10);                    // lowest price = best ask
}

TEST_CASE(rb_begin_is_best_bid_for_greater) {
    RBPriceIndex<std::greater<PriceTick>> t(std::greater<PriceTick>(), nullptr, 1024);
    for (PriceTick p : {50, 10, 90, 30, 70}) { auto [it, ok] = t.try_emplace(p); (void)ok; it->second.price = p; }
    CHECK(t.validate());
    CHECK(t.begin()->first == 90);                    // highest price = best bid
}

TEST_CASE(rb_dense_erase_maintains_invariants) {
    RBPriceIndex<std::less<PriceTick>> t(std::less<PriceTick>(), nullptr, 2048);
    for (PriceTick p = 1; p <= 200; ++p) { auto [it, ok] = t.try_emplace(p); (void)ok; it->second.price = p; }
    REQUIRE(t.validate());
    CHECK(t.size() == 200u);
    for (PriceTick p = 2; p <= 200; p += 2) { t.erase(t.find(p)); REQUIRE(t.validate()); }
    CHECK(t.size() == 100u);
    CHECK(t.begin()->first == 1);
    for (PriceTick p = 1; p <= 200; p += 2) CHECK(t.find(p) != t.end());
    for (PriceTick p = 2; p <= 200; p += 2) CHECK(t.find(p) == t.end());
    for (PriceTick p = 1; p <= 200; p += 2) { t.erase(t.find(p)); REQUIRE(t.validate()); }
    CHECK(t.empty());
    CHECK(t.begin() == t.end());
}

TEST_CASE(rb_random_churn_matches_reference) {
    RBPriceIndex<std::greater<PriceTick>> t(std::greater<PriceTick>(), nullptr, 4096);
    std::mt19937_64 rng(0xBADC0FFEEULL);
    std::set<PriceTick> ref;

    for (int i = 0; i < 40000; ++i) {
        const PriceTick p = static_cast<PriceTick>(rng() % 600);
        if ((rng() & 1u) || ref.empty()) {
            const bool expect_new = (ref.find(p) == ref.end());
            auto [it, ok] = t.try_emplace(p);
            CHECK(ok == expect_new);
            it->second.price = p;
            ref.insert(p);
        } else {
            auto rit = ref.begin();
            std::advance(rit, static_cast<long>(rng() % ref.size()));
            const PriceTick q = *rit;
            auto fit = t.find(q);
            REQUIRE(fit != t.end());
            t.erase(fit);
            ref.erase(rit);
        }
        if ((i % 101) == 0) {
            REQUIRE(t.validate());
            CHECK(t.size() == ref.size());
            if (!ref.empty()) CHECK(t.begin()->first == *ref.rbegin());  // greater => max is best
        }
    }
    REQUIRE(t.validate());
    CHECK(t.size() == ref.size());
}
