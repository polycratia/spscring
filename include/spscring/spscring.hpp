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

// Declared and never defined. A test can write the definition and start the
// indices anywhere it likes - just below their wrap, say, which is otherwise
// 2^64 pushes away - while a build that does not define it gains no way in.
template <typename Ring>
struct test_access;

}  // namespace detail

/// A bounded queue for exactly one producing thread and one consuming thread.
///
/// Capacity must be a power of two: the indices then wrap by masking, and a
/// wrap of the counter itself stays aligned to the capacity, which is what
/// keeps the queue correct after 2^64 operations instead of subtly wrong.
///
/// An element lives from its push to its pop: constructed in place in a slot,
/// destroyed as it leaves. T therefore needs no default constructor, and a
/// ring holds no T until something is pushed into it.
template <typename T, std::size_t Capacity>
class ring {
    static_assert(detail::is_power_of_two(Capacity), "capacity must be a power of two");
    static_assert(std::is_move_constructible_v<T>,
                  "elements must be move constructible: try_pop moves them out of their slot");
    static_assert(std::is_nothrow_destructible_v<T>,
                  "elements must be destructible without throwing: the queue destroys them");

  public:
    ring() = default;
    ring(const ring&) = delete;
    ring& operator=(const ring&) = delete;

    /// Destroys whatever is still queued. There is no synchronisation here and
    /// none is possible, so this may run only once both threads have been
    /// joined and nobody is pushing or popping any more.
    ~ring() {
        const std::size_t read = read_.load(std::memory_order_relaxed);
        const std::size_t write = write_.load(std::memory_order_relaxed);
        for (std::size_t index = read; index != write; ++index) slot(index)->~T();
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

    /// Called only by the producing thread.
    template <typename U, typename = std::enable_if_t<std::is_constructible_v<T, U&&>>>
    [[nodiscard]] bool try_push(U&& value) {
        // Our own index: nobody else writes it, so a relaxed load is enough.
        const std::size_t write = write_.load(std::memory_order_relaxed);
        // The consumer's index must be acquired: everything it did before
        // publishing that index has to be visible before we reuse the slot.
        const std::size_t read = read_.load(std::memory_order_acquire);

        if (write - read == Capacity) return false;  // full

        // The slot is raw storage, and the consumer destroyed whatever used to
        // live there before it handed the slot back, so this constructs rather
        // than assigns.
        ::new (raw_slot(write)) T(std::forward<U>(value));
        // Release: the element must be visible to the consumer before the index
        // that advertises it. Reversing these two lines is the classic bug.
        write_.store(write + 1, std::memory_order_release);
        return true;
    }

    /// Builds min(n, free slots) elements in consecutive slots, wrapping past
    /// the end of the array, and publishes the lot with one release store.
    /// Returns how many it built; the caller resumes at std::next(first, that).
    /// A std::move_iterator moves the source elements instead of copying them.
    ///
    /// Called only by the producing thread.
    ///
    /// One store is the point: a batch that advertised itself once per element
    /// would be one chance per element for the consumer to see it half built.
    /// A construction that throws destroys what this call has already built and
    /// leaves write_ where the consumer last saw it, so the batch either
    /// happened or did not.
    template <typename ForwardIt>
    [[nodiscard]] std::size_t try_push_n(ForwardIt first, std::size_t n) {
        const std::size_t write = write_.load(std::memory_order_relaxed);
        const std::size_t read = read_.load(std::memory_order_acquire);

        const std::size_t count = std::min(n, Capacity - (write - read));
        if (count == 0) return 0;  // nothing asked for, or no room: no store

        std::size_t built = 0;
        try {
            for (; built < count; ++built, ++first) ::new (raw_slot(write + built)) T(*first);
        } catch (...) {
            while (built-- != 0) slot(write + built)->~T();
            throw;
        }
        // Every element of the batch is visible before the single index that
        // advertises all of them.
        write_.store(write + count, std::memory_order_release);
        return count;
    }

    /// Called only by the consuming thread.
    [[nodiscard]] std::optional<T> try_pop() {
        // The result is built before the slot is handed back, and every path
        // returns the same named object, so the element is moved exactly once
        // and NRVO keeps it that way. A second move after the release store
        // would, if it threw, destroy an element the queue already counts as
        // popped.
        std::optional<T> out;

        const std::size_t read = read_.load(std::memory_order_relaxed);
        const std::size_t write = write_.load(std::memory_order_acquire);

        if (read != write) {  // not empty
            T* const element = slot(read);
            out.emplace(std::move(*element));
            // The element's life ends here, not at the next push into this
            // slot: a popped element holds nothing once the caller has it.
            element->~T();
            // Release: the producer may reuse this slot as soon as it sees the
            // new index, and it must not do so before the move and this
            // destructor have finished.
            read_.store(read + 1, std::memory_order_release);
        }
        return out;
    }

    /// Moves up to max elements to out, destroying each slot as it goes, and
    /// hands all of them back with one release store. Returns the count.
    ///
    /// Called only by the consuming thread.
    ///
    /// An element written to out has left the queue, so a handover that throws
    /// releases exactly the elements already handed over - with that same
    /// single store, before the exception leaves - and the element whose
    /// handover threw stays queued rather than being counted as popped.
    template <typename OutputIt>
    [[nodiscard]] std::size_t try_pop_n(OutputIt out, std::size_t max) {
        const std::size_t read = read_.load(std::memory_order_relaxed);
        const std::size_t write = write_.load(std::memory_order_acquire);

        const std::size_t count = std::min(max, write - read);
        if (count == 0) return 0;  // nothing asked for, or empty: no store

        std::size_t moved = 0;
        try {
            for (; moved < count; ++moved, ++out) {
                T* const element = slot(read + moved);
                *out = std::move(*element);
                element->~T();
            }
        } catch (...) {
            read_.store(read + moved, std::memory_order_release);
            throw;
        }
        read_.store(read + count, std::memory_order_release);
        return count;
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
    friend struct detail::test_access<ring>;

    static constexpr std::size_t slot_alignment = std::max(detail::cache_line, alignof(T));

    [[nodiscard]] void* raw_slot(std::size_t index) noexcept {
        return slots_ + (index & (Capacity - 1)) * sizeof(T);
    }

    [[nodiscard]] T* slot(std::size_t index) noexcept {
        return std::launder(reinterpret_cast<T*>(raw_slot(index)));
    }

    // Indices count forever and are masked at use. Subtraction is correct
    // across the unsigned wrap, and because the capacity is a power of two the
    // wrap does not shift the alignment.
    alignas(detail::cache_line) std::atomic<std::size_t> write_{0};
    alignas(detail::cache_line) std::atomic<std::size_t> read_{0};
    // Raw storage rather than an array of T: the queue owns a slot's bytes for
    // its whole life and the element in them only from a push to a pop. The
    // alignment keeps the slots off the indices' lines and every slot aligned
    // for T.
    alignas(slot_alignment) unsigned char slots_[Capacity * sizeof(T)];
};

}  // namespace spscring

#endif  // SPSCRING_SPSCRING_HPP
