#include "spscring/spscring.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "harness.hpp"

// The header declares this template and defines it nowhere, so the definition
// below is the only one that exists and it exists only here.
namespace spscring::detail {

template <typename Ring>
struct test_access {
    // Called before any thread touches the ring, so relaxed stores are enough.
    static void start_indices_at(Ring& queue, std::size_t index) {
        queue.write_.store(index, std::memory_order_relaxed);
        queue.read_.store(index, std::memory_order_relaxed);
    }
};

}  // namespace spscring::detail

namespace {

void an_empty_queue_gives_nothing() {
    spscring::ring<int, 4> q;
    CHECK(q.empty());
    CHECK(!q.full());
    CHECK(q.size() == 0);
    CHECK(!q.try_pop().has_value());
}

void it_keeps_order() {
    spscring::ring<int, 8> q;
    for (int i = 0; i < 5; ++i) CHECK(q.try_push(i));

    for (int i = 0; i < 5; ++i) {
        const auto value = q.try_pop();
        CHECK(value == i);
    }
    CHECK(q.empty());
}

// A full queue must refuse rather than overwrite: dropping the oldest element
// silently is a different data structure, and callers rarely want it.
void a_full_queue_refuses() {
    spscring::ring<int, 4> q;
    for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));

    CHECK(q.full());
    CHECK(!q.try_push(99));
    CHECK(q.size() == 4);

    // Taking one makes room for exactly one.
    CHECK(q.try_pop() == 0);
    CHECK(q.try_push(99));
    CHECK(!q.try_push(100));
}

// The indices count forever and are masked at use, so the queue has to survive
// far more pushes than it has slots.
void it_wraps_many_times() {
    spscring::ring<int, 4> q;
    for (int round = 0; round < 1000; ++round) {
        CHECK(q.try_push(round));
        const auto value = q.try_pop();
        CHECK(value == round);
    }
    CHECK(q.empty());
}

// The counter itself wraps at 2^64, and a capacity that is not a power of two
// would put that wrap in the middle of a lap of the slots and reorder the queue
// exactly once. Reaching the wrap honestly takes 2^64 pushes, so the indices
// start six short of it instead, and the queue then runs bursts of four, three,
// two and one straight through it.
void it_survives_the_counter_wrap() {
    using small_ring = spscring::ring<int, 4>;
    constexpr std::size_t start = SIZE_MAX - 5;
    // The index after n operations is start + n, which is below start once the
    // counter has gone round.
    const auto past_the_wrap = [](int operations) {
        return start + static_cast<std::size_t>(operations) < start;
    };

    small_ring q;
    spscring::detail::test_access<small_ring>::start_indices_at(q, start);
    CHECK(q.empty());

    constexpr int total = 32;
    constexpr int burst_sizes[] = {4, 3, 2, 1};

    int pushed = 0;
    int popped = 0;
    int refusals_before_the_wrap = 0;
    int refusals_after_the_wrap = 0;
    int counts_across_the_wrap = 0;

    for (int burst_index = 0; popped < total; ++burst_index) {
        const int burst = std::min(burst_sizes[burst_index % 4], total - pushed);

        for (int i = 0; i < burst; ++i) CHECK(q.try_push(pushed++));
        CHECK(q.size() == static_cast<std::size_t>(burst));

        if (past_the_wrap(pushed) && !past_the_wrap(popped)) {
            // write_ has passed 0 and read_ has not, so size() subtracts a huge
            // index from a tiny one. Unsigned arithmetic makes that the true
            // count, not a number near 2^64.
            CHECK(q.size() == static_cast<std::size_t>(burst));
            ++counts_across_the_wrap;
        }

        if (burst == static_cast<int>(q.capacity())) {
            // Full on both sides of the wrap means refusing on both sides.
            CHECK(!q.try_push(-1));
            if (past_the_wrap(pushed)) {
                ++refusals_after_the_wrap;
            } else {
                ++refusals_before_the_wrap;
            }
        }

        for (int i = 0; i < burst; ++i) {
            const auto value = q.try_pop();
            CHECK(value == popped++);
        }
        CHECK(q.size() == 0);
    }

    CHECK(q.empty());
    // The run is worthless if it never reached the wrap, or reached it with the
    // queue empty on both occasions it mattered.
    CHECK(past_the_wrap(pushed));
    CHECK(refusals_before_the_wrap >= 1);
    CHECK(refusals_after_the_wrap >= 1);
    CHECK(counts_across_the_wrap >= 1);
}

// One move assignment into the slot on the way in, one move construction out
// of it on the way out. Any more than that is a move the caller is paying for
// and cannot see.
void it_moves_rather_than_copies() {
    struct counted {
        int value = 0;
        int moves = 0;
        counted() = default;
        explicit counted(int v) : value(v) {}
        counted(counted&& other) noexcept : value(other.value), moves(other.moves + 1) {}
        counted& operator=(counted&& other) noexcept {
            value = other.value;
            moves = other.moves + 1;
            return *this;
        }
        counted(const counted&) = delete;
        counted& operator=(const counted&) = delete;
    };

    spscring::ring<counted, 4> q;
    CHECK(q.try_push(counted{7}));
    const auto out = q.try_pop();
    CHECK(out.has_value());
    CHECK(out->value == 7);
    CHECK(out->moves == 2);
}

// A move constructor that runs out of permission, so a test can ask what the
// queue has already published by the time the move throws. Move assignment
// stays noexcept, which is what the queue itself requires.
int move_budget = 0;

struct fragile {
    int value = 0;
    fragile() = default;
    explicit fragile(int v) : value(v) {}
    fragile(fragile&& other) : value(other.value) {
        if (move_budget == 0) throw std::runtime_error("no moves left");
        --move_budget;
    }
    fragile& operator=(fragile&& other) noexcept {
        value = other.value;
        return *this;
    }
    fragile(const fragile&) = delete;
    fragile& operator=(const fragile&) = delete;
};

// try_pop moves the element out of the slot exactly once, and does it before
// the release store that hands the slot back. A move that throws therefore
// leaves the element queued, where a second move after the store would throw
// with the queue already counting the element as popped - and lose it.
void a_failed_move_leaves_the_element_queued() {
    spscring::ring<fragile, 4> queued;
    fragile zero{0};
    fragile one{1};
    CHECK(queued.try_push(std::move(zero)));
    CHECK(queued.try_push(std::move(one)));

    // One move is a pop's whole budget, and all a pop should need.
    move_budget = 1;
    bool moved_once = true;
    try {
        const std::optional<fragile> first = queued.try_pop();
        CHECK(first.has_value() && first->value == 0);
    } catch (const std::runtime_error&) {
        moved_once = false;
    }
    CHECK(moved_once);
    CHECK(queued.size() == 1);

    move_budget = 1;
    const std::optional<fragile> second = queued.try_pop();
    CHECK(second.has_value() && second->value == 1);
    CHECK(queued.empty());

    // No budget at all: the move throws while the element still belongs to the
    // queue, so both are still there and a retry gets the first one.
    spscring::ring<fragile, 4> stuck;
    fragile zero_again{0};
    fragile one_again{1};
    CHECK(stuck.try_push(std::move(zero_again)));
    CHECK(stuck.try_push(std::move(one_again)));

    move_budget = 0;
    bool threw = false;
    try {
        const std::optional<fragile> nothing = stuck.try_pop();
        CHECK(!nothing.has_value());  // not reached: the move had no budget
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    CHECK(stuck.size() == 2);

    move_budget = 1;
    const std::optional<fragile> retried = stuck.try_pop();
    CHECK(retried.has_value() && retried->value == 0);
    CHECK(stuck.size() == 1);
}

// The test that matters, and the one people skip: two real threads, every
// element accounted for, in order.
void two_threads_lose_nothing_and_reorder_nothing() {
    constexpr int total = 200000;
    spscring::ring<std::uint32_t, 1024> q;
    std::atomic<bool> producer_done{false};

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) {
            while (!q.try_push(static_cast<std::uint32_t>(i))) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    int received = 0;
    bool ordered = true;
    while (received < total) {
        const auto value = q.try_pop();
        if (!value) {
            // Nothing available: either the producer is behind, or it has
            // finished and the queue is genuinely drained.
            if (producer_done.load(std::memory_order_acquire) && q.empty() && received < total) {
                break;  // would be a lost element; the check below reports it
            }
            std::this_thread::yield();
            continue;
        }
        if (*value != static_cast<std::uint32_t>(received)) ordered = false;
        ++received;
    }
    producer.join();

    CHECK(received == total);
    CHECK(ordered);
    CHECK(q.empty());
}

// A queue smaller than the burst forces the producer to wait, which is the path
// where a missing release makes elements appear before their contents.
void a_tiny_queue_still_carries_everything() {
    constexpr int total = 50000;
    spscring::ring<std::uint32_t, 2> q;

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) {
            while (!q.try_push(static_cast<std::uint32_t>(i))) std::this_thread::yield();
        }
    });

    int received = 0;
    bool ordered = true;
    while (received < total) {
        const auto value = q.try_pop();
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
}

// A third thread only ever reads the two indices, so it cannot corrupt the
// queue - but it can observe a pop and a push between its two loads, and an
// unsigned subtraction in the wrong order hands it a number near 2^64. The
// smallest queue and the longest run make that window land often; a capacity of
// 4 over 200000 elements caught it roughly once in three runs, which is not a
// test.
void a_watching_thread_never_sees_more_than_capacity() {
    constexpr int total = 1'000'000;
    spscring::ring<std::uint32_t, 2> q;
    std::atomic<bool> running{true};
    std::atomic<bool> ordered{true};
    std::atomic<std::size_t> largest_sample{0};

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) {
            while (!q.try_push(static_cast<std::uint32_t>(i))) std::this_thread::yield();
        }
    });

    std::thread consumer([&] {
        int received = 0;
        while (received < total) {
            const auto value = q.try_pop();
            if (!value) {
                std::this_thread::yield();
                continue;
            }
            if (*value != static_cast<std::uint32_t>(received)) {
                ordered.store(false, std::memory_order_relaxed);
            }
            ++received;
        }
    });

    std::thread watcher([&] {
        std::size_t largest = 0;
        while (running.load(std::memory_order_acquire)) {
            const std::size_t sample = q.size();
            if (sample > largest) largest = sample;
        }
        largest_sample.store(largest, std::memory_order_release);
    });

    producer.join();
    consumer.join();
    running.store(false, std::memory_order_release);
    watcher.join();

    CHECK(largest_sample.load(std::memory_order_acquire) <= q.capacity());
    CHECK(ordered.load(std::memory_order_relaxed));
}

void the_indices_do_not_share_a_cache_line() {
    using small_ring = spscring::ring<int, 4>;
    // Not a behavioural test - a layout one. Each index and the slots get a
    // line of their own, so the whole object is three lines wide and aligned to
    // one. A size that merely clears 128 bytes says nothing: the broken layout
    // was 192 bytes with the two indices 64 apart, inside one 128-byte line.
    CHECK(sizeof(small_ring) == 3 * spscring::detail::cache_line);
    CHECK(alignof(small_ring) == spscring::detail::cache_line);
#if !defined(SPSCRING_CACHE_LINE) && (defined(__aarch64__) || defined(__x86_64__))
    // Apple M-series lines are 128 bytes, and x86-64 fetches lines in pairs.
    CHECK(spscring::detail::cache_line == 128);
#endif
}

}  // namespace

int main() {
    an_empty_queue_gives_nothing();
    it_keeps_order();
    a_full_queue_refuses();
    it_wraps_many_times();
    it_survives_the_counter_wrap();
    it_moves_rather_than_copies();
    a_failed_move_leaves_the_element_queued();
    two_threads_lose_nothing_and_reorder_nothing();
    a_tiny_queue_still_carries_everything();
    a_watching_thread_never_sees_more_than_capacity();
    the_indices_do_not_share_a_cache_line();
    return harness::report("spscring");
}
