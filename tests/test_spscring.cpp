#include "spscring/spscring.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "harness.hpp"

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
    CHECK(out->moves > 0);
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
    spscring::ring<int, 4> q;
    // Not a behavioural test — a layout one. If these ever land within 64 bytes
    // of each other the queue is still correct and much slower, which is the
    // kind of regression nobody notices.
    CHECK(sizeof(q) >= 128);
}

}  // namespace

int main() {
    an_empty_queue_gives_nothing();
    it_keeps_order();
    a_full_queue_refuses();
    it_wraps_many_times();
    it_moves_rather_than_copies();
    two_threads_lose_nothing_and_reorder_nothing();
    a_tiny_queue_still_carries_everything();
    a_watching_thread_never_sees_more_than_capacity();
    the_indices_do_not_share_a_cache_line();
    return harness::report("spscring");
}
