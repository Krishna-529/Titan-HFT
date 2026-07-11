# Titan HFT v1 - foundation phase build (ready for when cmake/ninja are installed).
# Until then, use ./build.sh which drives g++ directly.
cmake_minimum_required(VERSION 3.16)
project(titan_hft_v1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)  # GNU builtins: __builtin_ctzll / __builtin_popcountll

# Header-only core.
add_library(titan_core INTERFACE)
target_include_directories(titan_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_options(titan_core INTERFACE -Wall -Wextra -Wpedantic)

# Unit tests. Debug builds enable ASan + UBSan (the zero-crash proof).
enable_testing()
add_executable(titan_tests tests/tests.cpp)
target_link_libraries(titan_tests PRIVATE titan_core)
target_compile_options(titan_tests PRIVATE
    $<$<CONFIG:Debug>:-fsanitize=address,undefined;-fno-omit-frame-pointer>)
target_link_options(titan_tests PRIVATE
    $<$<CONFIG:Debug>:-fsanitize=address,undefined>)
add_test(NAME unit COMMAND titan_tests)

# Usage:
#   cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
#   cmake --build build && ctest --test-dir build --output-on-failure
