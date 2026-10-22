// spscring — a single-producer single-consumer ring buffer whose memory
// ordering is written down.
//
// Everyone writes one of these. Most of them are wrong in one of three ways,
// and none of the three shows up in testing on x86:
//
//   1. Relaxed loads on the other side's index. The queue then works on x86,
//      whose hardware happens to order the accesses, and drops or duplicates
//      elements on ARM, which does not. This one ships.
//   2. Head and tail sharing a cache line. Correct, and roughly an order of
//      magnitude slower than it should be, because the two threads fight over
//      the line on every operation.
//   3. Counting with a size_t that wraps at a non-multiple of the capacity, so
//      the queue silently reorders once, years in.
//
// This is not a cleverer queue. It is the ordinary one with the reasoning
// attached, so the next person does not have to re-derive it.
//
// Header-only, C++17, no allocation after construction, no exceptions.
#ifndef SPSCRING_SPSCRING_HPP
#define SPSCRING_SPSCRING_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace spscring {

namespace detail {

constexpr bool is_power_of_two(std::size_t n) noexcept { return n != 0 && (n & (n - 1)) == 0; }

// Two indices written by two different threads must not share a cache line, or
// each write invalidates the other's line and the queue spends its time on
// coherency traffic.
//
// 64 is not the universal answer. Apple M-series cores have 128-byte lines
// (hw.cachelinesize reports 128 on an M2), and on x86-64 the adjacent-line
// prefetcher fetches lines in pairs, so two indices in neighbouring 64-byte
// lines still travel together. Both architectures therefore get 128, and
// everything else keeps 64. std::hardware_destructive_interference_size would
// answer this for us but is inconsistently available.
#if defined(SPSCRING_CACHE_LINE)
constexpr std::size_t cache_line = SPSCRING_CACHE_LINE;
#elif defined(__aarch64__) || defined(__x86_64__)
constexpr std::size_t cache_line = 128;
#else
constexpr std::size_t cache_line = 64;
#endif

static_assert(is_power_of_two(cache_line), "SPSCRING_CACHE_LINE must be a power of two");
static_assert(cache_line >= alignof(std::atomic<std::size_t>),
              "SPSCRING_CACHE_LINE must be at least alignof(std::atomic<std::size_t>)");

}  // namespace detail

/// A bounded queue for exactly one producing thread and one consuming thread.
///
/// Capacity must be a power of two: the indices then wrap by masking, and a
/// wrap of the counter itself stays aligned to the capacity, which is what
/// keeps the queue correct after 2^64 operations instead of subtly wrong.
template <typename T, std::size_t Capacity>
class ring {
    static_assert(detail::is_power_of_two(Capacity), "capacity must be a power of two");
    static_assert(std::is_nothrow_move_assignable_v<T> || std::is_copy_assignable_v<T>,
                  "elements must be assignable without throwing across the queue boundary");

  public:
    ring() = default;
    ring(const ring&) = delete;
    ring& operator=(const ring&) = delete;

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

    /// Called only by the producing thread.
    template <typename U>
    [[nodiscard]] bool try_push(U&& value) {
        // Our own index: nobody else writes it, so a relaxed load is enough.
        const std::size_t write = write_.load(std::memory_order_relaxed);
        // The consumer's index must be acquired: everything it did before
        // publishing that index has to be visible before we reuse the slot.
        const std::size_t read = read_.load(std::memory_order_acquire);

        if (write - read == Capacity) return false;  // full

        slots_[write & (Capacity - 1)] = std::forward<U>(value);
        // Release: the element must be visible to the consumer before the index
        // that advertises it. Reversing these two lines is the classic bug.
        write_.store(write + 1, std::memory_order_release);
        return true;
    }

    /// Called only by the consuming thread.
    [[nodiscard]] std::optional<T> try_pop() {
        const std::size_t read = read_.load(std::memory_order_relaxed);
        const std::size_t write = write_.load(std::memory_order_acquire);

        if (write == read) return std::nullopt;  // empty

        T value = std::move(slots_[read & (Capacity - 1)]);
        // Release: the producer may reuse this slot as soon as it sees the new
        // index, and it must not do so before our move has finished.
        read_.store(read + 1, std::memory_order_release);
        return value;
    }

    /// An instantaneous count. Both threads may call it; by the time it returns
    /// it is already history, so it is for metrics and never for control flow.
    ///
    /// A third thread may call it as well, and always gets a value in
    /// [0, Capacity]. read_ only grows and never passes write_, so loading it
    /// before write_ keeps the difference non-negative for any observer, and the
    /// clamp covers an observer whose two loads straddle a pop followed by a
    /// push. For the producer and the consumer the result is exact.
    [[nodiscard]] std::size_t size() const noexcept {
        const std::size_t read = read_.load(std::memory_order_acquire);
        const std::size_t write = write_.load(std::memory_order_acquire);
        return std::min(write - read, Capacity);
    }

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    [[nodiscard]] bool full() const noexcept { return size() == Capacity; }

  private:
    // Indices count forever and are masked at use. Subtraction is correct
    // across the unsigned wrap, and because the capacity is a power of two the
    // wrap does not shift the alignment.
    alignas(detail::cache_line) std::atomic<std::size_t> write_{0};
    alignas(detail::cache_line) std::atomic<std::size_t> read_{0};
    alignas(detail::cache_line) T slots_[Capacity]{};
};

}  // namespace spscring

#endif  // SPSCRING_SPSCRING_HPP
