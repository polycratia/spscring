// One producer, one consumer, a million messages, and a count at the end.
//
//   make demo
#include "spscring/spscring.hpp"

#include <chrono>
#include <cstdio>
#include <cstdint>
#include <thread>

namespace {

struct message {
    std::uint64_t sequence = 0;
    std::uint64_t payload = 0;
};

}  // namespace

int main() {
    constexpr int total = 1'000'000;
    spscring::ring<message, 4096> queue;

    const auto started = std::chrono::steady_clock::now();

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) {
            message m{static_cast<std::uint64_t>(i), static_cast<std::uint64_t>(i) * 2};
            // A full queue refuses rather than overwriting, so back-pressure is
            // the caller's to handle - here by yielding.
            while (!queue.try_push(m)) std::this_thread::yield();
        }
    });

    std::uint64_t received = 0;
    std::uint64_t out_of_order = 0;
    while (received < total) {
        const auto m = queue.try_pop();
        if (!m) {
            std::this_thread::yield();
            continue;
        }
        if (m->sequence != received) ++out_of_order;
        ++received;
    }
    producer.join();

    const auto elapsed = std::chrono::steady_clock::now() - started;
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

    std::printf("sent and received: %llu messages\n", static_cast<unsigned long long>(received));
    std::printf("out of order:      %llu\n", static_cast<unsigned long long>(out_of_order));
    std::printf("elapsed:           %lld ms\n", static_cast<long long>(ms));
    std::printf("queue empty:       %d\n", queue.empty());
    return 0;
}
