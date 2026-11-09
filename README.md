# spscring

A single-producer single-consumer ring buffer whose memory ordering is written
down.

Everyone writes one of these. Most of them are wrong in one of three ways, and
none of the three shows up in testing on x86:

1. **Relaxed loads on the other thread's index.** The queue works on x86, whose
   hardware happens to order the accesses, and drops or duplicates elements on
   ARM, which does not. This one ships.
2. **Head and tail sharing a cache line.** Correct, and roughly an order of
   magnitude slower than it should be, because the two threads fight over the
   line on every single operation.
3. **A counter that wraps at a non-multiple of the capacity,** so the queue
   silently reorders once, years in.

This is not a cleverer queue. It is the ordinary one with the reasoning
attached, so the next person does not have to re-derive it.

## Use

```cpp
#include "spscring/spscring.hpp"

spscring::ring<message, 4096> queue;   // capacity must be a power of two

// producing thread
if (!queue.try_push(m)) { /* full: back-pressure is yours to handle */ }
const std::size_t sent = queue.try_push_n(first, n);   // 0 if full

// consuming thread
if (const auto m = queue.try_pop()) { handle(*m); }
const std::size_t got = queue.try_pop_n(std::back_inserter(batch), 32);

// a capacity known only at start-up
auto configured = spscring::heap_ring<message>::create(n);   // null unless n is 2^k
```

Header-only, C++17, no exceptions, and no allocation after construction:
`heap_ring` does its one allocation in `create` and none afterwards.

## The ordering, in four lines

```cpp
// push
const auto write = write_.load(std::memory_order_relaxed);  // ours; nobody else writes it
const auto read  = read_.load(std::memory_order_acquire);   // theirs; must see their progress
::new (raw_slot(write)) T(std::forward<U>(value));          // built in the slot
write_.store(write + 1, std::memory_order_release);         // element visible before the index
```

The release on the store is what makes the element visible before the index that
advertises it. Swapping those two lines is the classic bug, and it is invisible
on x86.

The consumer is the mirror image: its release on `read_` is what stops the
producer reusing a slot before the move out of it has finished. It moves the
element exactly once, straight into the `std::optional` it returns, destroys
what is left in the slot, and only then advances `read_` — so an element whose
move throws is still queued, rather than destroyed by a queue that has already
counted it as popped.

`size()` loads `read_` first, `write_` second, and clamps the difference to the
capacity. A monitoring thread's two loads can straddle a pop followed by a push;
in the other order the unsigned subtraction hands it a number near 2^64, and
`empty()` and `full()` inherit it. With the order and the clamp any third thread
sees a value in `[0, capacity()]`, and the producer and the consumer still get an
exact answer.

Indices count forever and are masked at use. Because the capacity is a power of
two, the eventual wrap of the counter itself stays aligned to the capacity —
which is the difference between a queue that is correct after 2^64 operations
and one that reorders exactly once. `it_survives_the_counter_wrap` is that
claim as a test: it starts both indices at `SIZE_MAX - 5` and runs 32 elements
through bursts of four, three, two and one, checking every value out, the
refusal of a fifth push into a full queue on each side of the wrap, and the
count in the state where the write index has passed zero and the read index has
not. The header declares `detail::test_access` and defines it nowhere, so the
test can place the indices there and a build that is not the test suite gains
no API.

## A batch is one store

`try_push_n(first, n)` builds `min(n, free)` elements in consecutive slots,
wrapping past the end of the array, and then advances `write_` once. It returns
the number it built, and the caller resumes at `std::next(first, k)`.
`try_pop_n(out, max)` is the mirror: it moves up to `max` elements to `out`,
destroys each slot as it goes, and hands all of them back with one release
store. A `std::move_iterator` moves the source elements rather than copying
them.

The single store is the point. A batch of sixteen that advertised itself
sixteen times would give the consumer sixteen chances to see a half-published
batch; with one store it sees none of the batch or all of it.

On the way in a batch is all or nothing. If an element's construction throws,
the elements this call already built are destroyed and `write_` stays where the
consumer last saw it, so a throwing batch is a batch that never happened. On
the way out the trade is different, because an element written to `out` has
already left the queue: a throwing handover releases exactly the elements
already handed over, with that same single store, before the exception
propagates, and the element whose handover threw stays queued. A zero-length
call, a full queue on push and an empty one on pop return 0 and store to
neither index.

## The life of an element

The slots are raw storage, not an array of `T`. Constructing a `ring<T, N>`
constructs no `T` at all; `try_push` builds one in the slot from its argument,
and `try_pop` moves it into the `std::optional` it returns and then runs `~T()`
on the slot. So `T` needs no default constructor, and a popped element gives up
whatever it held as it leaves, instead of sitting in the slot until some later
push happens to overwrite it.

The destructor destroys the elements still in `[read_, write_)`. It does that
with no synchronisation, because none is possible: a queue may be destroyed only
once both threads are done with it.

## A capacity that is not a constant

When the capacity is a configuration value rather than a number in the source,
`heap_ring<T>::create(capacity)` puts the slots on the heap and hands back a
`std::unique_ptr`, or null:

```cpp
auto queue = spscring::heap_ring<message>::create(capacity);
if (!queue) { /* 0, not a power of two, too large for a size_t, or out of memory */ }
```

It refuses rather than rounding. A request for 1000 does not become 1024: a
queue that holds a different number of elements than its owner asked for is a
queue whose owner's arithmetic is wrong somewhere else, and silence there is
worse than a null. Nothing on the path throws — the object comes from
`new (std::nothrow)` and the slots from the aligned `operator new` with
`std::nothrow` — so a failed allocation is the same null as a bad capacity. The
slots are allocated once, in `create`, and freed in the destructor after the
elements still queued are destroyed.

Both types derive from `detail::ring_core`, which owns the two indices and every
one of the atomic accesses above; the derived class only says how large it is
and where a slot lives. There is one copy of the ordering, not two, and
`detail::test_access` reaches the indices of either.

## The separation between the indices

The two indices sit 128 bytes apart on aarch64 and x86-64, and 64 bytes apart
elsewhere. 64 is the number everybody writes down and it is too small on both
of the architectures this is likely to run on: Apple M-series cores have
128-byte lines (`hw.cachelinesize` reports 128 on an M2), and x86-64's
adjacent-line prefetcher fetches lines in pairs, so two indices in neighbouring
64-byte lines still travel together.

Define `SPSCRING_CACHE_LINE` to override the choice. A value that is not a power
of two, or smaller than `alignof(std::atomic<std::size_t>)`, is a compile error
rather than a silently worse layout. The slots take the larger of that number
and `alignof(T)`.

## Handing out the two ends

`split()` returns one `producer` and one `consumer` the first time it is called
and `std::nullopt` every time after, decided by a single atomic exchange:

```cpp
spscring::ring<message, 4096> queue;
auto ends = queue.split();              // std::nullopt on a second call
auto [tx, rx] = std::move(*ends);

std::thread sender([tx = std::move(tx)]() mutable { tx.try_push(m); });
if (const auto m = rx.try_pop()) { handle(*m); }
```

`producer` has `try_push`, `try_push_n` and `capacity()`; `consumer` has
`try_pop`, `try_pop_n` and `size()`. Both are move-only, so one handle cannot
become two, and a moved-from handle refuses: `try_push` returns `false` and
`try_pop` returns `std::nullopt` rather than touching a ring it no longer points
at.

The handles hold a pointer. The ring has to outlive them, which the header says
and nothing checks. The flag that remembers the split lives in the padding of
`write_`'s cache line, so the object is the same three lines wide as before, and
`split()` is on the shared core, so `heap_ring` has it too.

## What it is not

**Not multi-producer or multi-consumer.** Two producers will corrupt it.
`split()` keeps the rule for code that goes through the handles, since it hands
them out once and neither can be copied. The ring's own `try_push` and
`try_pop` stay public, though, so a caller that reaches past the handles is
still the one who has to get this right.

**Not lock-free in the sense of wait-free.** `try_push` and `try_pop` never
block, but a full queue means the producer has to decide what to do, and this
library does not decide for it.

**Not a benchmark winner.** No fetch-add tricks, no unbounded variant. It is a
correct baseline you can read in one sitting.

## Status

| | |
|---|---|
| Implemented | bounded ring for one producer and one consumer, elements constructed on push and destroyed on pop, a single move out per element, no default constructor required of `T`, power-of-two capacity, a heap-allocated variant whose run-time capacity is checked and never rounded, batch push and pop that each publish with one store and wrap past the end of the array, one producer and one consumer handle handed out once by `split()`, indices on cache lines of their own (128 bytes on aarch64 and x86-64, overridable), instantaneous `size` for metrics, safe to sample from a third thread, order kept across the 2^64 index wrap |
| Not yet | a blocking wrapper with futex-style waiting |

## Development

```bash
make test   # 2257 checks under AddressSanitizer and UndefinedBehaviorSanitizer
make race   # the same suites under ThreadSanitizer
make demo   # one million messages through a 4096-slot queue
```

`make race` is not optional in spirit: ThreadSanitizer is the only one of the
three that can see a missing acquire or release, and a lock-free queue that has
not been through it has not been checked.

## License

MIT © polycratia — <https://polycratia.com>
