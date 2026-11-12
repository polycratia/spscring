// A control for `make race`: the index protocol on its own, built three times,
// so that the ThreadSanitizer run which passes has been shown to fail when the
// ordering is wrong.
//
// This is a deliberate copy of the push/pop protocol rather than a use of the
// header. The point is a pair of broken builds, and the header has no way to
// produce one, which is as it should be.
//
//   make race-control
//
// SPSCRING_WEAKEN=load relaxes the consumer's acquire load of the write index;
// SPSCRING_WEAKEN=store relaxes the producer's release store. Either one breaks
// the release/acquire pair that makes an element visible before the index that
// advertises it, so the slot is written by one thread and read by the other
// with nothing ordering the two.
//
// What a passing run proves is that ThreadSanitizer in this toolchain catches
// this shape of missing acquire or release. It does not prove that the header
// is correct.
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <thread>

#define SPSCRING_WEAKEN_none 1
#define SPSCRING_WEAKEN_load 2
#define SPSCRING_WEAKEN_store 3

#ifndef SPSCRING_WEAKEN
#define SPSCRING_WEAKEN none
#endif

#define SPSCRING_CAT_(a, b) a##b
#define SPSCRING_CAT(a, b) SPSCRING_CAT_(a, b)
// A misspelt variant pastes to a name with no value, which is 0 here and
// matches none of the three, so it is a compile error rather than a silent
// third copy of the correct build.
#define SPSCRING_VARIANT SPSCRING_CAT(SPSCRING_WEAKEN_, SPSCRING_WEAKEN)

#if SPSCRING_VARIANT != SPSCRING_WEAKEN_none && SPSCRING_VARIANT != SPSCRING_WEAKEN_load && \
    SPSCRING_VARIANT != SPSCRING_WEAKEN_store
#error "SPSCRING_WEAKEN must be none, load or store"
#endif

namespace {

#if SPSCRING_VARIANT == SPSCRING_WEAKEN_load
constexpr std::memory_order publish = std::memory_order_release;
constexpr std::memory_order observe = std::memory_order_relaxed;
constexpr const char* variant = "relaxed load";
#elif SPSCRING_VARIANT == SPSCRING_WEAKEN_store
constexpr std::memory_order publish = std::memory_order_relaxed;
constexpr std::memory_order observe = std::memory_order_acquire;
constexpr const char* variant = "relaxed store";
#else
constexpr std::memory_order publish = std::memory_order_release;
constexpr std::memory_order observe = std::memory_order_acquire;
constexpr const char* variant = "correct";
#endif

constexpr std::size_t capacity = 64;  // a power of two, as in the header
constexpr int total = 200000;

int slots[capacity];
std::atomic<std::size_t> write_index{0};
std::atomic<std::size_t> read_index{0};

}  // namespace

int main() {
    std::thread producer([] {
        for (int i = 0; i < total; ++i) {
            for (;;) {
                const std::size_t write = write_index.load(std::memory_order_relaxed);
                const std::size_t read = read_index.load(std::memory_order_acquire);
                if (write - read != capacity) {
                    slots[write & (capacity - 1)] = i;
                    write_index.store(write + 1, publish);
                    break;
                }
                std::this_thread::yield();
            }
        }
    });

    int received = 0;
    int out_of_order = 0;
    while (received < total) {
        const std::size_t read = read_index.load(std::memory_order_relaxed);
        const std::size_t write = write_index.load(observe);
        if (read == write) {
            std::this_thread::yield();
            continue;
        }
        const int value = slots[read & (capacity - 1)];
        read_index.store(read + 1, std::memory_order_release);
        if (value != received) ++out_of_order;
        ++received;
    }
    producer.join();

    std::printf("race_control [%s]: %d transferred, %d out of order\n", variant, received,
                out_of_order);
    return out_of_order == 0 ? 0 : 1;
}
