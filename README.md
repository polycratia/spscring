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

## The ordering, access by access

One row per atomic access: the order it asks for, the access on the other
thread it pairs with, what breaks if it is weakened, and the test that fails
when it is. The copy of this table in `include/spscring/spscring.hpp`, above
`detail::ring_core`, is the authoritative one — it sits beside the accesses it
describes, and this is a copy of it.

| Access | Order | Pairs with | What weakening breaks | Test that fails |
|---|---|---|---|---|
| `try_push`: `write_.load` | relaxed | nothing — nobody else writes `write_` | nothing weaker to ask for | already relaxed |
| `try_push`: `read_.load` | acquire | `read_.store` in `try_pop` and `try_pop_n` | a slot reused before the consumer's move out of it and its `~T()` have finished | reasoning only |
| `try_push`: `write_.store` | release | `write_.load` in `try_pop` and `try_pop_n` | an element read before its contents on ARM | `make race-control`, store variant |
| `try_push_n`: `write_.load` | relaxed | nothing — nobody else writes `write_` | nothing weaker to ask for | already relaxed |
| `try_push_n`: `read_.load` | acquire | `read_.store` in `try_pop` and `try_pop_n` | slots reused before the consumer's moves out of them have finished | reasoning only |
| `try_push_n`: `write_.store`, one for the batch | release | `write_.load` in `try_pop` and `try_pop_n` | a half-built batch becomes visible, and its elements before their contents | reasoning only |
| `try_pop`: `read_.load` | relaxed | nothing — nobody else writes `read_` | nothing weaker to ask for | already relaxed |
| `try_pop`: `write_.load` | acquire | `write_.store` in `try_push` and `try_push_n` | an element read before its contents on ARM | `make race-control`, load variant |
| `try_pop`: `read_.store` | release | `read_.load` in `try_push` and `try_push_n` | the producer reuses the slot before the move out of it and `~T()` have finished | reasoning only |
| `try_pop_n`: `read_.load` | relaxed | nothing — nobody else writes `read_` | nothing weaker to ask for | already relaxed |
| `try_pop_n`: `write_.load` | acquire | `write_.store` in `try_push` and `try_push_n` | a batch read before the contents of its elements | reasoning only |
| `try_pop_n`: `read_.store`, one for the batch and one more on the throwing path | release | `read_.load` in `try_push` and `try_push_n` | slots reused while the moves out of them are still running | reasoning only |
| `size`: `read_.load`, loaded first | acquire | `read_.store` in `try_pop` and `try_pop_n` | loading `write_` first lets an observer's two loads straddle a pop and a push, and the unsigned subtraction hands it a count near 2^64, with `empty()` and `full()` inheriting it | `a_watching_thread_never_sees_more_than_capacity` |
| `size`: `write_.load`, loaded second | acquire | `write_.store` in `try_push` and `try_push_n` | nothing for the count itself; the acquire is for a caller that reads a slot on the strength of a count | reasoning only |
| `split`: `split_.exchange` | relaxed | nothing — the exchange is the whole decision | nothing weaker to ask for; `split()` runs before either thread exists | already relaxed |

Two rows name a test, and no more. The header offers no way to ask for the wrong
ordering, which is the point of it, so `tests/race_control.cpp` is a copy of the
index protocol built three times — correct, with the consumer's acquire load
relaxed, and with the producer's release store relaxed — and those two
weakenings are the two rows that name `make race-control`. Every other row says
`reasoning only`, which is exactly what it is: a claim no test here can fail on,
written down so it can be argued with. `already relaxed` means the access is to
the thread's own index, which nobody else writes, so there is nothing weaker to
ask for.

`try_pop` moves the element exactly once, straight into the `std::optional` it
returns, destroys what is left in the slot, and only then advances `read_` — so
an element whose move throws is still queued, rather than destroyed by a queue
that has already counted it as popped.

`size()` clamps the difference to the capacity as well as loading the two
indices in that order. The order keeps the subtraction non-negative for any
observer; the clamp covers the observer whose loads straddle a pop followed by a
push. With both, a third thread sees a value in `[0, capacity()]`, and the
producer and the consumer still get an exact answer.

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
one of the atomic accesses in the table above; the derived class only says how
large it is and where a slot lives. There is one copy of the ordering, not two,
and `detail::test_access` reaches the indices of either.

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
make test          # the three suites under AddressSanitizer and UndefinedBehaviorSanitizer
make race          # the same three under ThreadSanitizer
make race-control  # shows that make race can fail
make demo          # one million messages through a 4096-slot queue
```

The three suites are `tests/test_spscring.cpp` (ordering, element lifetimes, the
counter wrap, the batch functions, the throwing paths, and the three-thread
`size()` test), `tests/test_heap_ring.cpp` (what `create` refuses, and a
run-time capacity carrying two threads' traffic) and `tests/test_split.cpp` (the
handles, their move-only types, and the layout of the split flag). Each prints
how many checks it ran; the number is not written down here, because every test
added changes it and a count in a README that nobody updates is worse than no
count at all.

`make race` is not optional in spirit: ThreadSanitizer is the only one of the
three that can see a missing acquire or release, and a lock-free queue that has
not been through it has not been checked.

A clean ThreadSanitizer run is worth only as much as the tool's teeth, so
`make race-control` measures them. `tests/race_control.cpp` is a minimal copy of
the push/pop index protocol rather than a use of the header — the header offers
no way to ask for the wrong ordering, which is the point of it — and the target
builds that copy three times, transferring 200,000 ints through 64 slots between
two threads each time: once correct, once with the consumer's acquire load of
the write index relaxed, and once with the producer's release store relaxed.
Those last two are bug 1 above, from each side, and they are the two weakenings
the table names. The target passes only when the correct build exits 0 and both
weakened builds exit 66, the exit code the runs ask ThreadSanitizer for;
otherwise it prints `make race has no teeth: <variant> exited <code>` and fails.
The runs also set `abort_on_error=0`, because ThreadSanitizer aborts by default
on macOS and a reported run then dies with SIGABRT and exit 134 instead. On
Apple clang 21 the three exit 0, 66 and 66.

It is not part of `make test`, and what it proves is bounded: that
ThreadSanitizer in this toolchain catches this shape of weakened ordering, not
that the header is correct.

## License

MIT © polycratia — <https://polycratia.com>
