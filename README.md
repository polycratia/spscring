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
producer reusing a slot before the move out of it has finished.

Indices count forever and are masked at use. Because the capacity is a power of
two, the eventual wrap of the counter itself stays aligned to the capacity —
which is the difference between a queue that is correct after 2^64 operations
and one that reorders exactly once.

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
| Implemented | bounded ring for one producer and one consumer, move-through, power-of-two capacity, cache-line separated indices, instantaneous `size` for metrics |
| Not yet | batch push and pop, a blocking wrapper with futex-style waiting, a heap-allocated runtime-capacity variant |

## Development

```bash
make test   # 2036 checks under AddressSanitizer and UndefinedBehaviorSanitizer
make race   # the same suite under ThreadSanitizer
make demo   # one million messages through a 4096-slot queue
```

`make race` is not optional in spirit: ThreadSanitizer is the only one of the
three that can see a missing acquire or release, and a lock-free queue that has
not been through it has not been checked.

## License

MIT
