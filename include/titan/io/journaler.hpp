#pragma once
//
// titan/io/journaler.hpp
// Append-only Write-Ahead Log over POSIX mmap. Appends are pure MEMORY writes (a
// memcpy into the mapped buffer) -- NO blocking disk I/O on the hot path. The OS
// flushes dirty pages asynchronously; msync() on shutdown makes them durable.
//
// Strictly binary: Order PODs are dumped raw (no JSON). A 64-byte FileHeader guards
// against ABI drift -- a raw reinterpret across a struct-layout change would be silent
// corruption, so opening a WAL whose order_size != sizeof(Order) (or wrong magic /
// version) throws immediately.
//
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1   // expose mmap/msync/ftruncate/posix_fallocate under -std=c++20
#endif

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "titan/book/order.hpp"

namespace titan::io {

inline constexpr std::uint64_t WAL_MAGIC   = 0x544954414EULL;  // "TITAN"
inline constexpr std::uint16_t WAL_VERSION = 1;

// Fixed 64-byte header at file offset 0. Order records follow, back to back.
struct FileHeader {
    std::uint64_t magic;        // WAL_MAGIC ("TITAN")
    std::uint16_t version;      // format version
    std::uint16_t order_size;   // sizeof(Order) at write time -> ABI tripwire
    std::uint32_t _pad;
    std::uint64_t count;        // committed order count (== write cursor)
    std::uint8_t  _reserved[40];
};
static_assert(sizeof(FileHeader) == 64,                  "FileHeader must be 64 bytes");
static_assert(std::is_trivially_copyable_v<FileHeader>,  "FileHeader must be a POD");
static_assert(std::is_standard_layout_v<FileHeader>,     "FileHeader must be standard layout");

class Journaler {
public:
    // CREATE: fresh WAL, pre-allocated for `capacity_orders`, fresh header, count = 0.
    Journaler(const std::string& path, std::uint64_t capacity_orders) {
        map_bytes_ = sizeof(FileHeader) + capacity_orders * sizeof(Order);
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) throw std::runtime_error("journaler: create/open failed: " + path);
        // Pre-allocate real blocks so page writes on the hot path never SIGBUS on space.
        if (::posix_fallocate(fd_, 0, static_cast<off_t>(map_bytes_)) != 0 &&
            ::ftruncate(fd_, static_cast<off_t>(map_bytes_)) != 0) {
            ::close(fd_);
            throw std::runtime_error("journaler: preallocate failed");
        }
        map_and_wire();
        header_->magic      = WAL_MAGIC;
        header_->version    = WAL_VERSION;
        header_->order_size = static_cast<std::uint16_t>(sizeof(Order));
        header_->_pad       = 0;
        header_->count      = 0;
        capacity_ = capacity_orders;
    }

    // OPEN (recovery / read): map + validate the header. Throws on ABI mismatch.
    explicit Journaler(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDWR);
        if (fd_ < 0) throw std::runtime_error("journaler: open failed: " + path);
        struct stat st{};
        if (::fstat(fd_, &st) != 0 || static_cast<std::size_t>(st.st_size) < sizeof(FileHeader)) {
            ::close(fd_);
            throw std::runtime_error("journaler: file too small / stat failed: " + path);
        }
        map_bytes_ = static_cast<std::size_t>(st.st_size);
        map_and_wire();
        try {
            validate();                                // throws on magic/version/size mismatch
        } catch (...) {
            ::munmap(map_, map_bytes_); ::close(fd_);   // ctor threw -> dtor won't run; clean up
            throw;
        }
        capacity_ = (map_bytes_ - sizeof(FileHeader)) / sizeof(Order);
    }

    Journaler(const Journaler&)            = delete;
    Journaler& operator=(const Journaler&) = delete;

    ~Journaler() {
        if (map_ && map_ != MAP_FAILED) {
            ::msync(map_, map_bytes_, MS_SYNC);        // graceful durable flush on shutdown
            ::munmap(map_, map_bytes_);
        }
        if (fd_ >= 0) ::close(fd_);
    }

    // Append one order: memcpy raw bytes at the cursor, advance. No syscall, no I/O.
    void append(const Order& order) {
        if (header_->count >= capacity_) throw std::runtime_error("journaler: WAL full");
        std::memcpy(&data_[header_->count], &order, sizeof(Order));
        ++header_->count;
    }

    std::uint64_t count()    const noexcept { return header_->count; }
    std::uint64_t capacity() const noexcept { return capacity_; }
    const Order&  operator[](std::uint64_t i) const noexcept { return data_[i]; }

    void sync() noexcept { if (map_ && map_ != MAP_FAILED) ::msync(map_, map_bytes_, MS_SYNC); }

    // Startup safety harness: aggressively reject a WAL whose binary layout isn't ours.
    void validate() const {
        if (header_->magic != WAL_MAGIC)
            throw std::runtime_error("journaler: bad magic (not a TITAN WAL)");
        if (header_->version != WAL_VERSION)
            throw std::runtime_error("journaler: unsupported WAL version");
        if (header_->order_size != sizeof(Order))
            throw std::runtime_error("journaler: order_size mismatch (Order ABI changed)");
    }

private:
    void map_and_wire() {
        map_ = ::mmap(nullptr, map_bytes_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (map_ == MAP_FAILED) {
            ::close(fd_);
            throw std::runtime_error("journaler: mmap failed");
        }
        header_ = reinterpret_cast<FileHeader*>(map_);
        data_   = reinterpret_cast<Order*>(reinterpret_cast<char*>(map_) + sizeof(FileHeader));
    }

    int           fd_        = -1;
    void*         map_       = nullptr;
    std::size_t   map_bytes_ = 0;
    FileHeader*   header_    = nullptr;
    Order*        data_      = nullptr;
    std::uint64_t capacity_  = 0;
};

} // namespace titan::io
