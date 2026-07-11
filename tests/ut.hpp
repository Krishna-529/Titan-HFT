#pragma once
//
// tests/ut.hpp
// Minimal zero-dependency unit-test harness (doctest can replace it later).
// TEST_CASE(name) registers a test; CHECK/REQUIRE record assertions.
//
#include <cstdio>
#include <functional>
#include <vector>

namespace ut {

struct TestCase { const char* name; std::function<void()> fn; };

inline std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
inline int& fail_count()  { static int f = 0; return f; }
inline int& check_count() { static int c = 0; return c; }

struct Registrar {
    Registrar(const char* n, std::function<void()> f) { registry().push_back({n, std::move(f)}); }
};

inline void report_fail(const char* expr, const char* file, int line) {
    ++fail_count();
    std::printf("    [FAIL] %s:%d:  %s\n", file, line, expr);
}

inline int run_all() {
    int failed_tests = 0;
    for (auto& t : registry()) {
        const int before = fail_count();
        std::printf("[ RUN  ] %s\n", t.name);
        t.fn();
        if (fail_count() > before) { ++failed_tests; std::printf("[ FAIL ] %s\n", t.name); }
        else                       { std::printf("[  OK  ] %s\n", t.name); }
    }
    std::printf("\n==== tests=%zu  checks=%d  failing_tests=%d  failed_checks=%d ====\n",
                registry().size(), check_count(), failed_tests, fail_count());
    return failed_tests == 0 ? 0 : 1;
}

} // namespace ut

#define TEST_CASE(name)                                        \
    static void name();                                        \
    static ::ut::Registrar reg_##name(#name, name);            \
    static void name()

#define CHECK(expr)                                            \
    do { ++::ut::check_count();                                \
         if (!(expr)) ::ut::report_fail(#expr, __FILE__, __LINE__); } while (0)

#define REQUIRE(expr)                                          \
    do { ++::ut::check_count();                                \
         if (!(expr)) { ::ut::report_fail(#expr, __FILE__, __LINE__); return; } } while (0)
