// The two handles: that split() gives them out once, that neither can be
// copied, that a moved-from one refuses, and that a real pair of threads
// carries everything through them in order. The ring's own try_push and
// try_pop are tested in test_spscring.cpp and are unchanged by any of this.
#include "spscring/spscring.hpp"

#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

#include "harness.hpp"

namespace {

using small_ring = spscring::ring<int, 4>;

// A handle that could be copied would be a second producer or a second
// consumer, which is the one rule the queue asks a caller to keep. A move has
// to be nothrow because the usual way a handle reaches its thread is by being
// moved into one.
static_assert(!std::is_copy_constructible_v<small_ring::producer>,
              "a copied producer would be a second producer");
static_assert(!std::is_copy_assignable_v<small_ring::producer>,
              "a copied producer would be a second producer");
static_assert(std::is_nothrow_move_constructible_v<small_ring::producer>,
              "a producer must reach its thread without a throw");
static_assert(!std::is_copy_constructible_v<small_ring::consumer>,
              "a copied consumer would be a second consumer");
static_assert(!std::is_copy_assignable_v<small_ring::consumer>,
              "a copied consumer would be a second consumer");
static_assert(std::is_nothrow_move_constructible_v<small_ring::consumer>,
              "a consumer must reach its thread without a throw");

// split() hands out the two ends and then has nothing left to hand out: a
// second caller gets nullopt rather than a second producer.
void split_hands_out_the_two_ends_once() {
    small_ring q;

    auto ends = q.split();
    CHECK(ends.has_value());
    if (!ends) return;
    CHECK(ends->first.capacity() == 4);
    CHECK(ends->second.size() == 0);

    CHECK(!q.split().has_value());
    CHECK(!q.split().has_value());
}

// A moved-from handle has given its ring away, so it says no rather than
// following a pointer it no longer holds - and the handle that took the ring
// still works.
void a_moved_from_handle_refuses() {
    small_ring q;

    auto ends = q.split();
    CHECK(ends.has_value());
    if (!ends) return;

    auto tx = std::move(ends->first);
    auto rx = std::move(ends->second);
    CHECK(tx.try_push(1));
    CHECK(rx.try_pop() == 1);

    auto moved_tx = std::move(tx);
    auto moved_rx = std::move(rx);

    const int value = 2;
    int drained[1] = {};
    CHECK(!tx.try_push(value));
    CHECK(tx.try_push_n(&value, 1) == 0);
    CHECK(!rx.try_pop().has_value());
    CHECK(rx.try_pop_n(drained, 1) == 0);
    CHECK(q.empty());  // a refusal stores to neither index

    CHECK(moved_tx.try_push(3));
    CHECK(moved_rx.try_pop() == 3);
}

// The handles are the ordinary way to use the queue, so they get the ordinary
// test: two real threads, every element accounted for, in order.
void two_threads_transfer_through_the_handles() {
    constexpr int total = 200000;
    spscring::ring<std::uint32_t, 1024> q;

    auto ends = q.split();
    CHECK(ends.has_value());
    if (!ends) return;

    auto tx = std::move(ends->first);
    auto rx = std::move(ends->second);
    CHECK(tx.capacity() == 1024);

    std::thread producer([tx = std::move(tx), total]() mutable {
        for (int i = 0; i < total; ++i) {
            while (!tx.try_push(static_cast<std::uint32_t>(i))) std::this_thread::yield();
        }
    });

    int received = 0;
    bool ordered = true;
    while (received < total) {
        const auto value = rx.try_pop();
        if (!value) {
            std::this_thread::yield();
            continue;
        }
        if (*value != static_cast<std::uint32_t>(received)) ordered = false;
        ++received;
    }
    producer.join();

    CHECK(received == total);
    CHECK(ordered);
    CHECK(rx.size() == 0);
}

// The flag split() exchanges lives in the padding after write_, so the object
// is the same three cache lines wide as it was before there was a flag.
void the_split_flag_costs_no_cache_line() {
    CHECK(sizeof(small_ring) == 3 * spscring::detail::cache_line);
    CHECK(alignof(small_ring) == spscring::detail::cache_line);
}

}  // namespace

int main() {
    split_hands_out_the_two_ends_once();
    a_moved_from_handle_refuses();
    two_threads_transfer_through_the_handles();
    the_split_flag_costs_no_cache_line();
    return harness::report("spscring split");
}
