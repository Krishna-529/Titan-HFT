#pragma once
//
// titan/book/rb_price_index.hpp
// Intrusive, pooled Red-Black tree of PriceLevels, ordered by Compare, exposing a
// std::map-like subset so it drops into OrderBook in place of std::pmr::map.
//
// "Flash One" neighbour-aware mechanics:
//   * Insert = ONE search that also records the in-order predecessor P and
//     successor S; the new key attaches at the unique null child of P or S in
//     O(1) (splice), pred/succ links are relinked O(1), then standard O(log n)
//     Red-Black rebalancing. (std::map would search twice: find + insert.)
//   * Delete = O(1) graft: the 2-child successor is read straight from the succ
//     link (no successor search), then standard delete-fixup.
//   * pred/succ links give O(1) neighbour traversal; begin() (best per Compare)
//     is the cached in-order-first node.
//
// Zero-crash: index-based, pre-allocated pool; a real sentinel node (index 0)
// makes the CLRS delete-fixup total (no null-parent hazards). Pool exhaustion
// throws std::bad_alloc from try_emplace (OrderBook::add already wraps add in
// try/catch -> graceful rejection).
//
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <new>
#include <utility>
#include <vector>

#include "titan/book/price_level.hpp"
#include "titan/domain/types.hpp"

namespace titan {

template <class Compare>
class RBPriceIndex {
    static constexpr std::uint32_t NIL = 0;     // sentinel node index
    enum Color : std::uint8_t { RED = 0, BLACK = 1 };

    struct Node {
        std::pair<PriceTick, PriceLevel> kv{};  // value_type (map-like: .first/.second)
        std::uint32_t left = NIL, right = NIL, parent = NIL;
        std::uint32_t pred = NIL, succ = NIL;   // in-order (by Compare) neighbours
        Color color = BLACK;
    };

    std::vector<Node>          nodes_;
    std::vector<std::uint32_t> free_;
    std::uint32_t root_  = NIL;
    std::uint32_t begin_ = NIL;   // in-order first (best per Compare)
    std::size_t   size_  = 0;
    Compare       before_;        // "comes before": before_(a,b) => a precedes b

    // --- pool ---
    std::uint32_t alloc() {
        if (free_.empty()) throw std::bad_alloc();   // caller (OrderBook::add) catches
        const std::uint32_t i = free_.back();
        free_.pop_back();
        nodes_[i] = Node{};
        return i;
    }
    void release(std::uint32_t i) { free_.push_back(i); }

    Node&       N(std::uint32_t i)       noexcept { return nodes_[i]; }
    const Node& N(std::uint32_t i) const noexcept { return nodes_[i]; }

    // --- rotations (sentinel-safe) ---
    void rotate_left(std::uint32_t x) noexcept {
        const std::uint32_t y = N(x).right;
        N(x).right = N(y).left;
        if (N(y).left != NIL) N(N(y).left).parent = x;
        N(y).parent = N(x).parent;
        if (N(x).parent == NIL)            root_ = y;
        else if (x == N(N(x).parent).left) N(N(x).parent).left = y;
        else                               N(N(x).parent).right = y;
        N(y).left = x;
        N(x).parent = y;
    }
    void rotate_right(std::uint32_t x) noexcept {
        const std::uint32_t y = N(x).left;
        N(x).left = N(y).right;
        if (N(y).right != NIL) N(N(y).right).parent = x;
        N(y).parent = N(x).parent;
        if (N(x).parent == NIL)             root_ = y;
        else if (x == N(N(x).parent).right) N(N(x).parent).right = y;
        else                                N(N(x).parent).left = y;
        N(y).right = x;
        N(x).parent = y;
    }

    void insert_fixup(std::uint32_t z) noexcept {
        while (N(N(z).parent).color == RED) {
            const std::uint32_t p  = N(z).parent;
            const std::uint32_t gp = N(p).parent;
            if (p == N(gp).left) {
                const std::uint32_t u = N(gp).right;      // uncle
                if (N(u).color == RED) {
                    N(p).color = BLACK; N(u).color = BLACK; N(gp).color = RED; z = gp;
                } else {
                    if (z == N(p).right) { z = p; rotate_left(z); }
                    N(N(z).parent).color = BLACK;
                    N(N(N(z).parent).parent).color = RED;
                    rotate_right(N(N(z).parent).parent);
                }
            } else {
                const std::uint32_t u = N(gp).left;
                if (N(u).color == RED) {
                    N(p).color = BLACK; N(u).color = BLACK; N(gp).color = RED; z = gp;
                } else {
                    if (z == N(p).left) { z = p; rotate_right(z); }
                    N(N(z).parent).color = BLACK;
                    N(N(N(z).parent).parent).color = RED;
                    rotate_left(N(N(z).parent).parent);
                }
            }
        }
        N(root_).color = BLACK;
    }

    void transplant(std::uint32_t u, std::uint32_t v) noexcept {
        if (N(u).parent == NIL)            root_ = v;
        else if (u == N(N(u).parent).left) N(N(u).parent).left = v;
        else                               N(N(u).parent).right = v;
        N(v).parent = N(u).parent;         // v may be sentinel; needed by delete_fixup
    }

    void delete_fixup(std::uint32_t x) noexcept {
        while (x != root_ && N(x).color == BLACK) {
            if (x == N(N(x).parent).left) {
                std::uint32_t w = N(N(x).parent).right;   // sibling
                if (N(w).color == RED) {
                    N(w).color = BLACK; N(N(x).parent).color = RED;
                    rotate_left(N(x).parent); w = N(N(x).parent).right;
                }
                if (N(N(w).left).color == BLACK && N(N(w).right).color == BLACK) {
                    N(w).color = RED; x = N(x).parent;
                } else {
                    if (N(N(w).right).color == BLACK) {
                        N(N(w).left).color = BLACK; N(w).color = RED;
                        rotate_right(w); w = N(N(x).parent).right;
                    }
                    N(w).color = N(N(x).parent).color;
                    N(N(x).parent).color = BLACK;
                    N(N(w).right).color = BLACK;
                    rotate_left(N(x).parent); x = root_;
                }
            } else {
                std::uint32_t w = N(N(x).parent).left;
                if (N(w).color == RED) {
                    N(w).color = BLACK; N(N(x).parent).color = RED;
                    rotate_right(N(x).parent); w = N(N(x).parent).left;
                }
                if (N(N(w).right).color == BLACK && N(N(w).left).color == BLACK) {
                    N(w).color = RED; x = N(x).parent;
                } else {
                    if (N(N(w).left).color == BLACK) {
                        N(N(w).right).color = BLACK; N(w).color = RED;
                        rotate_left(w); w = N(N(x).parent).left;
                    }
                    N(w).color = N(N(x).parent).color;
                    N(N(x).parent).color = BLACK;
                    N(N(w).left).color = BLACK;
                    rotate_right(N(x).parent); x = root_;
                }
            }
        }
        N(x).color = BLACK;
    }

public:
    // Constructor is call-compatible with std::pmr::map(Compare, memory_resource*)
    // so OrderBook can build either container the same way. `capacity` bounds the
    // number of simultaneously-active price levels.
    RBPriceIndex(Compare cmp, std::pmr::memory_resource* /*unused*/,
                 std::size_t capacity = (std::size_t{1} << 16))
        : before_(cmp) {
        nodes_.resize(capacity + 1);                 // +1 for the sentinel at index 0
        nodes_[NIL].color = BLACK;
        free_.reserve(capacity);
        for (std::uint32_t i = static_cast<std::uint32_t>(capacity); i >= 1; --i) free_.push_back(i);
    }

    RBPriceIndex(const RBPriceIndex&)            = delete;
    RBPriceIndex& operator=(const RBPriceIndex&) = delete;

    // ---- std::map-like iterator (only what OrderBook/Matcher use) ----
    class iterator {
        RBPriceIndex* t_ = nullptr;
        std::uint32_t i_ = NIL;
    public:
        iterator() = default;
        iterator(RBPriceIndex* t, std::uint32_t i) noexcept : t_(t), i_(i) {}
        std::pair<PriceTick, PriceLevel>* operator->() const noexcept { return &t_->nodes_[i_].kv; }
        std::pair<PriceTick, PriceLevel>& operator*()  const noexcept { return  t_->nodes_[i_].kv; }
        bool operator==(const iterator& o) const noexcept { return i_ == o.i_; }
        bool operator!=(const iterator& o) const noexcept { return i_ != o.i_; }
        std::uint32_t node() const noexcept { return i_; }
    };

    iterator end()   noexcept { return iterator(this, NIL); }
    iterator begin() noexcept { return iterator(this, begin_); }
    bool     empty() const noexcept { return size_ == 0; }
    std::size_t size() const noexcept { return size_; }

    iterator find(PriceTick price) noexcept {
        std::uint32_t x = root_;
        while (x != NIL) {
            const PriceTick k = N(x).kv.first;
            if (price == k)            return iterator(this, x);
            x = before_(price, k) ? N(x).left : N(x).right;
        }
        return end();
    }

    // Single-search find-or-create. Returns {iterator, inserted}. On insert the
    // new level's payload is default-constructed (caller fills price/head/tail).
    std::pair<iterator, bool> try_emplace(PriceTick price) {
        std::uint32_t y = NIL, x = root_, pred = NIL, succ = NIL;
        while (x != NIL) {
            y = x;
            const PriceTick k = N(x).kv.first;
            if (price == k) return { iterator(this, x), false };
            if (before_(price, k)) { succ = x; x = N(x).left; }
            else                   { pred = x; x = N(x).right; }
        }
        const std::uint32_t z = alloc();             // may throw bad_alloc
        Node& zn = N(z);
        zn.kv.first = price;
        zn.left = zn.right = NIL;
        zn.parent = y;
        zn.color = RED;
        zn.pred = pred;
        zn.succ = succ;

        if (y == NIL)                    root_ = z;   // was empty
        else if (before_(price, N(y).kv.first)) N(y).left = z;
        else                             N(y).right = z;

        if (pred != NIL) N(pred).succ = z; else begin_ = z;   // new in-order first
        if (succ != NIL) N(succ).pred = z;

        insert_fixup(z);
        ++size_;
        return { iterator(this, z), true };
    }

    void erase(iterator it) noexcept {
        const std::uint32_t z = it.node();
        if (z == NIL) return;

        // Relink neighbour list (uses z's succ for O(1); done before tree surgery).
        if (N(z).pred != NIL) N(N(z).pred).succ = N(z).succ; else begin_ = N(z).succ;
        if (N(z).succ != NIL) N(N(z).succ).pred = N(z).pred;

        std::uint32_t y = z;
        Color y_orig = N(y).color;
        std::uint32_t x;
        if (N(z).left == NIL) {
            x = N(z).right; transplant(z, N(z).right);
        } else if (N(z).right == NIL) {
            x = N(z).left;  transplant(z, N(z).left);
        } else {
            y = N(z).succ;                            // O(1) graft: successor via link
            y_orig = N(y).color;
            x = N(y).right;
            if (N(y).parent == z) {
                N(x).parent = y;                      // x may be sentinel; set its parent
            } else {
                transplant(y, N(y).right);
                N(y).right = N(z).right; N(N(y).right).parent = y;
            }
            transplant(z, y);
            N(y).left = N(z).left; N(N(y).left).parent = y;
            N(y).color = N(z).color;
        }
        if (y_orig == BLACK) delete_fixup(x);
        N(NIL).parent = NIL; N(NIL).color = BLACK;    // scrub sentinel after fixup
        release(z);
        --size_;
    }

    // ---- test-only invariant checker (not on the hot path) ----
    // Verifies BST order, pred/succ consistency, and the Red-Black properties.
    bool validate() const {
        if (root_ == NIL) return size_ == 0 && begin_ == NIL;
        if (N(root_).color != BLACK) return false;
        // in-order walk via succ links from begin_
        std::size_t count = 0;
        std::uint32_t i = begin_;
        std::uint32_t prev = NIL;
        while (i != NIL) {
            ++count;
            if (N(i).pred != prev) return false;
            if (prev != NIL && !before_(N(prev).kv.first, N(i).kv.first)) return false;  // strict order
            prev = i;
            i = N(i).succ;
        }
        if (count != size_) return false;
        // Red property: red node has black children; and equal black-height.
        return check_node(root_) >= 0;
    }

private:
    int check_node(std::uint32_t x) const {   // returns black-height, or -1 on violation
        if (x == NIL) return 1;
        const std::uint32_t l = N(x).left, r = N(x).right;
        if (N(x).color == RED) {
            if (N(l).color != BLACK || N(r).color != BLACK) return -1;
        }
        if (l != NIL) {
            if (N(l).parent != x) return -1;
            if (!before_(N(l).kv.first, N(x).kv.first)) return -1;
        }
        if (r != NIL) {
            if (N(r).parent != x) return -1;
            if (!before_(N(x).kv.first, N(r).kv.first)) return -1;
        }
        const int lh = check_node(l);
        const int rh = check_node(r);
        if (lh < 0 || rh < 0 || lh != rh) return -1;
        return lh + (N(x).color == BLACK ? 1 : 0);
    }
};

} // namespace titan
