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

// consuming thread
if (const auto m = queue.try_pop()) { handle(*m); }
```

Header-only, C++17, no allocation after construction, no exceptions.

## The ordering, in four lines

```cpp
// push
const auto write = write_.load(std::memory_order_relaxed);  // ours; nobody else writes it
const auto read  = read_.load(std::memory_order_acquire);   // theirs; must see their progress
slots_[write & mask] = value;
write_.store(write + 1, std::memory_order_release);         // element visible before the index
```

The release on the store is what makes the element visible before the index that
advertises it. Swapping those two lines is the classic bug, and it is invisible
on x86.

The consumer is the mirror image: its release on `read_` is what stops the
producer reusing a slot before the move out of it has finished. It moves the
element exactly once, straight into the `std::optional` it returns, and only
then advances `read_` — so an element whose move throws is still queued, rather
than destroyed by a queue that has already counted it as popped.

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

## The separation between the indices

The two indices sit 128 bytes apart on aarch64 and x86-64, and 64 bytes apart
elsewhere. 64 is the number everybody writes down and it is too small on both
of the architectures this is likely to run on: Apple M-series cores have
128-byte lines (`hw.cachelinesize` reports 128 on an M2), and x86-64's
adjacent-line prefetcher fetches lines in pairs, so two indices in neighbouring
64-byte lines still travel together.

Define `SPSCRING_CACHE_LINE` to override the choice. A value that is not a power
of two, or smaller than `alignof(std::atomic<std::size_t>)`, is a compile error
rather than a silently worse layout.

## What it is not

**Not multi-producer or multi-consumer.** Two producers will corrupt it. The
name says so, and the type cannot enforce it, so this is the one thing a caller
has to get right.

**Not lock-free in the sense of wait-free.** `try_push` and `try_pop` never
block, but a full queue means the producer has to decide what to do, and this
library does not decide for it.

**Not a benchmark winner.** No batching, no fetch-add tricks, no unbounded
variant. It is a correct baseline you can read in one sitting.

## Status

| | |
|---|---|
| Implemented | bounded ring for one producer and one consumer, move-through with a single move out per element, power-of-two capacity, indices on cache lines of their own (128 bytes on aarch64 and x86-64, overridable), instantaneous `size` for metrics, safe to sample from a third thread, order kept across the 2^64 index wrap |
| Not yet | batch push and pop, a blocking wrapper with futex-style waiting, a heap-allocated runtime-capacity variant |

## Development

```bash
make test   # 2153 checks under AddressSanitizer and UndefinedBehaviorSanitizer
make race   # the same suite under ThreadSanitizer
make demo   # one million messages through a 4096-slot queue
```

`make race` is not optional in spirit: ThreadSanitizer is the only one of the
three that can see a missing acquire or release, and a lock-free queue that has
not been through it has not been checked.

## License

MIT © polycratia — <https://polycratia.com>
