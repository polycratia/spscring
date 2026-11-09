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
// Header-only, C++17, no exceptions, and no allocation after construction:
// heap_ring does its one allocation in create and none afterwards.
#ifndef SPSCRING_SPSCRING_HPP
#define SPSCRING_SPSCRING_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
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

// Keeps the slots off the indices' lines and every slot aligned for T.
template <typename T>
constexpr std::size_t slot_alignment = cache_line > alignof(T) ? cache_line : alignof(T);

// Declared and never defined. A test can write the definition and start the
// indices anywhere it likes - just below their wrap, say, which is otherwise
// 2^64 pushes away - while a build that does not define it gains no way in.
template <typename Ring>
struct test_access;

/// The two indices, and every atomic access the queue makes, in one place.
///
/// Derived supplies capacity() and slot(index), which hands back the raw
/// storage of the slot an index names. Everything else - the push, the pop,
/// the batches, the count, and the order of each load and store - lives here,
/// so a compile-time capacity and a run-time one share one copy of the
/// reasoning rather than two that can drift apart.
template <typename Derived, typename T>
class ring_core {
    static_assert(std::is_move_constructible_v<T>,
                  "elements must be move constructible: try_pop moves them out of their slot");
    static_assert(std::is_nothrow_destructible_v<T>,
                  "elements must be destructible without throwing: the queue destroys them");

  public:
    ring_core(const ring_core&) = delete;
    ring_core& operator=(const ring_core&) = delete;

    /// Called only by the producing thread.
    template <typename U, typename = std::enable_if_t<std::is_constructible_v<T, U&&>>>
    [[nodiscard]] bool try_push(U&& value) {
        // Our own index: nobody else writes it, so a relaxed load is enough.
        const std::size_t write = write_.load(std::memory_order_relaxed);
        // The consumer's index must be acquired: everything it did before
        // publishing that index has to be visible before we reuse the slot.
        const std::size_t read = read_.load(std::memory_order_acquire);

        if (write - read == ring_capacity()) return false;  // full

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

        const std::size_t count = std::min(n, ring_capacity() - (write - read));
        if (count == 0) return 0;  // nothing asked for, or no room: no store

        std::size_t built = 0;
        try {
            for (; built < count; ++built, ++first) ::new (raw_slot(write + built)) T(*first);
        } catch (...) {
            while (built-- != 0) element(write + built)->~T();
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
            T* const taken = element(read);
            out.emplace(std::move(*taken));
            // The element's life ends here, not at the next push into this
            // slot: a popped element holds nothing once the caller has it.
            taken->~T();
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
                T* const taken = element(read + moved);
                *out = std::move(*taken);
                taken->~T();
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
    /// [0, capacity()]. read_ only grows and never passes write_, so loading it
    /// before write_ keeps the difference non-negative for any observer, and the
    /// clamp covers an observer whose two loads straddle a pop followed by a
    /// push. For the producer and the consumer the result is exact.
    [[nodiscard]] std::size_t size() const noexcept {
        const std::size_t read = read_.load(std::memory_order_acquire);
        const std::size_t write = write_.load(std::memory_order_acquire);
        return std::min(write - read, ring_capacity());
    }

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    [[nodiscard]] bool full() const noexcept { return size() == ring_capacity(); }

    /// The pushing end: try_push, try_push_n and capacity(), and nothing that
    /// belongs to the other side.
    ///
    /// It holds a pointer, so the ring has to outlive it; that is a claim the
    /// caller makes and nothing here checks. Move-only, because a handle that
    /// could be copied would be a second producer, and a moved-from one refuses
    /// rather than following the pointer it gave away.
    class producer {
      public:
        producer(producer&& other) noexcept : ring_(std::exchange(other.ring_, nullptr)) {}
        producer& operator=(producer&& other) noexcept {
            ring_ = std::exchange(other.ring_, nullptr);
            return *this;
        }
        producer(const producer&) = delete;
        producer& operator=(const producer&) = delete;
        ~producer() = default;

        template <typename U, typename = std::enable_if_t<std::is_constructible_v<T, U&&>>>
        [[nodiscard]] bool try_push(U&& value) {
            return ring_ != nullptr && ring_->try_push(std::forward<U>(value));
        }

        template <typename ForwardIt>
        [[nodiscard]] std::size_t try_push_n(ForwardIt first, std::size_t n) {
            return ring_ == nullptr ? 0 : ring_->try_push_n(first, n);
        }

        [[nodiscard]] std::size_t capacity() const noexcept {
            return ring_ == nullptr ? 0 : ring_->capacity();
        }

      private:
        friend ring_core<Derived, T>;

        explicit producer(Derived* ring) noexcept : ring_(ring) {}

        Derived* ring_ = nullptr;
    };

    /// The popping end: try_pop, try_pop_n and size(). The same ownership and
    /// the same refusal as producer.
    class consumer {
      public:
        consumer(consumer&& other) noexcept : ring_(std::exchange(other.ring_, nullptr)) {}
        consumer& operator=(consumer&& other) noexcept {
            ring_ = std::exchange(other.ring_, nullptr);
            return *this;
        }
        consumer(const consumer&) = delete;
        consumer& operator=(const consumer&) = delete;
        ~consumer() = default;

        [[nodiscard]] std::optional<T> try_pop() {
            if (ring_ == nullptr) return std::nullopt;
            return ring_->try_pop();
        }

        template <typename OutputIt>
        [[nodiscard]] std::size_t try_pop_n(OutputIt out, std::size_t max) {
            return ring_ == nullptr ? 0 : ring_->try_pop_n(out, max);
        }

        [[nodiscard]] std::size_t size() const noexcept {
            return ring_ == nullptr ? 0 : ring_->size();
        }

      private:
        friend ring_core<Derived, T>;

        explicit consumer(Derived* ring) noexcept : ring_(ring) {}

        Derived* ring_ = nullptr;
    };

    /// Hands out one producer and one consumer the first time it is called and
    /// std::nullopt every time after. Called by whoever owns the ring, before
    /// the two threads exist.
    ///
    /// The ring's own try_push and try_pop stay public, so this does not make
    /// the single-producer rule unbreakable - it keeps it for the code that goes
    /// through the handles.
    [[nodiscard]] std::optional<std::pair<producer, consumer>> split() noexcept {
        // Relaxed: the exchange itself is the whole decision, and the ring was
        // constructed before any caller could reach this. A handle gets to the
        // thread that uses it by being moved into it, which synchronises on its
        // own.
        if (split_.exchange(true, std::memory_order_relaxed)) return std::nullopt;
        return std::pair<producer, consumer>(producer(&self()), consumer(&self()));
    }

  protected:
    ring_core() = default;
    ~ring_core() = default;

    /// Destroys whatever is still queued. There is no synchronisation here and
    /// none is possible, so this may run only once both threads have been
    /// joined and nobody is pushing or popping any more. The derived destructor
    /// calls it, where the derived object and so its storage is still whole.
    void destroy_queued() noexcept {
        const std::size_t read = read_.load(std::memory_order_relaxed);
        const std::size_t write = write_.load(std::memory_order_relaxed);
        for (std::size_t index = read; index != write; ++index) element(index)->~T();
    }

  private:
    friend struct test_access<Derived>;

    [[nodiscard]] Derived& self() noexcept { return *static_cast<Derived*>(this); }
    [[nodiscard]] const Derived& self() const noexcept {
        return *static_cast<const Derived*>(this);
    }

    [[nodiscard]] std::size_t ring_capacity() const noexcept { return self().capacity(); }
    [[nodiscard]] void* raw_slot(std::size_t index) noexcept { return self().slot(index); }
    [[nodiscard]] T* element(std::size_t index) noexcept {
        return std::launder(reinterpret_cast<T*>(raw_slot(index)));
    }

    // Indices count forever and are masked at use. Subtraction is correct
    // across the unsigned wrap, and because the capacity is a power of two the
    // wrap does not shift the alignment.
    alignas(cache_line) std::atomic<std::size_t> write_{0};
    // Touched only by split(), which runs before the queue carries anything, so
    // it rides in the padding after write_ instead of widening the object by a
    // line of its own.
    std::atomic<bool> split_{false};
    alignas(cache_line) std::atomic<std::size_t> read_{0};
};

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
class ring : public detail::ring_core<ring<T, Capacity>, T> {
    static_assert(detail::is_power_of_two(Capacity), "capacity must be a power of two");

  public:
    ring() = default;

    ~ring() { this->destroy_queued(); }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

  private:
    friend class detail::ring_core<ring, T>;

    [[nodiscard]] void* slot(std::size_t index) noexcept {
        return slots_ + (index & (Capacity - 1)) * sizeof(T);
    }

    // Raw storage rather than an array of T: the queue owns a slot's bytes for
    // its whole life and the element in them only from a push to a pop.
    alignas(detail::slot_alignment<T>) unsigned char slots_[Capacity * sizeof(T)];
};

/// The same queue with its capacity taken at run time and its slots on the
/// heap, for a capacity that is a configuration value rather than a constant.
///
/// create is the only way in, because a capacity that arrives at run time can
/// be one the queue cannot honour and a constructor would have to throw to say
/// so. It never rounds up: a request for 1000 is refused rather than quietly
/// becoming 1024.
template <typename T>
class heap_ring : public detail::ring_core<heap_ring<T>, T> {
  public:
    /// Returns null for a capacity of 0, for one that is not a power of two,
    /// for one whose byte size does not fit in a size_t, and for a failed
    /// allocation. Nothing on this path throws.
    [[nodiscard]] static std::unique_ptr<heap_ring> create(std::size_t capacity) noexcept {
        if (!detail::is_power_of_two(capacity)) return nullptr;  // 0 included
        if (capacity > std::numeric_limits<std::size_t>::max() / sizeof(T)) return nullptr;

        void* const slots =
            ::operator new(capacity * sizeof(T), std::align_val_t{detail::slot_alignment<T>},
                           std::nothrow);
        if (slots == nullptr) return nullptr;

        auto* const queue =
            new (std::nothrow) heap_ring(capacity, static_cast<unsigned char*>(slots));
        if (queue == nullptr) {
            ::operator delete(slots, std::align_val_t{detail::slot_alignment<T>});
            return nullptr;
        }
        return std::unique_ptr<heap_ring>(queue);
    }

    /// Destroys whatever is still queued and then frees the slots, which were
    /// allocated once in create. As for the fixed-capacity ring, this may run
    /// only once both threads are done with the queue.
    ~heap_ring() {
        this->destroy_queued();
        ::operator delete(slots_, std::align_val_t{detail::slot_alignment<T>});
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

  private:
    friend class detail::ring_core<heap_ring, T>;

    heap_ring(std::size_t capacity, unsigned char* slots) noexcept
        : capacity_(capacity), slots_(slots) {}

    [[nodiscard]] void* slot(std::size_t index) noexcept {
        return slots_ + (index & (capacity_ - 1)) * sizeof(T);
    }

    // Written once in create and read-only afterwards, so they cost nothing
    // where they land: beside read_, the line a push has to fetch anyway.
    std::size_t capacity_;
    unsigned char* slots_;
};

}  // namespace spscring

#endif  // SPSCRING_SPSCRING_HPP
