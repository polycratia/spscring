// The heap variant: what create refuses, and that a ring whose capacity was a
// number rather than a template argument carries the same traffic. The ring
// tests live in test_spscring.cpp and are unchanged by it.
#include "spscring/spscring.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>

#include "harness.hpp"

namespace {

// A capacity that arrives at run time can be one the queue cannot honour, and
// the only honest answers are the queue that was asked for or none. Zero and
// anything that is not a power of two would break the masking; a byte size that
// overflows size_t would allocate less storage than the capacity promises.
// Nothing is rounded up to the next value that would have worked.
void create_refuses_a_capacity_it_cannot_honour() {
    CHECK(spscring::heap_ring<int>::create(0) == nullptr);
    CHECK(spscring::heap_ring<int>::create(3) == nullptr);
    CHECK(spscring::heap_ring<int>::create(1000) == nullptr);  // not quietly 1024
    // 2^63 slots of two bytes each is 2^64 bytes, which is not a size_t.
    CHECK(spscring::heap_ring<std::uint16_t>::create(std::size_t{1} << 63) == nullptr);
}

struct tracked {
    static int live;
    int value = 0;
    explicit tracked(int v) : value(v) { ++live; }
    tracked(tracked&& other) noexcept : value(other.value) { ++live; }
    tracked(const tracked&) = delete;
    tracked& operator=(const tracked&) = delete;
    ~tracked() { --live; }
};

int tracked::live = 0;

// The slots are allocated once in create; the destructor destroys the elements
// still in them and only then gives the storage back.
void the_destructor_destroys_what_is_still_queued() {
    tracked::live = 0;
    {
        const auto q = spscring::heap_ring<tracked>::create(4);
        CHECK(q != nullptr);
        if (!q) return;
        CHECK(tracked::live == 0);  // creating the ring constructs no T

        for (int i = 0; i < 3; ++i) CHECK(q->try_push(tracked{i}));
        CHECK(tracked::live == 3);

        const auto first = q->try_pop();
        CHECK(first.has_value() && first->value == 0);
    }
    CHECK(tracked::live == 0);
}

// The test that matters, on the type that shares every atomic access with the
// fixed-capacity ring: two real threads, every element accounted for, in order.
void two_threads_lose_nothing_and_reorder_nothing() {
    constexpr int total = 200000;
    const auto q = spscring::heap_ring<std::uint32_t>::create(1024);
    CHECK(q != nullptr);
    if (!q) return;
    CHECK(q->capacity() == 1024);

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) {
            while (!q->try_push(static_cast<std::uint32_t>(i))) std::this_thread::yield();
        }
    });

    int received = 0;
    bool ordered = true;
    while (received < total) {
        const auto value = q->try_pop();
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
    CHECK(q->empty());
}

}  // namespace

int main() {
    create_refuses_a_capacity_it_cannot_honour();
    the_destructor_destroys_what_is_still_queued();
    two_threads_lose_nothing_and_reorder_nothing();
    return harness::report("spscring heap_ring");
}
