# Challenges and Solutions: A Development Journal

Every bug, wrong prediction, and design dead end that actually happened while
building this system — what broke, why, how it was found, and what fixed it.

Nothing here is invented for the write-up. Each entry has a file you can open
and, where relevant, a test that now prevents it from coming back.

**Why keep this?** Because the finished code shows only the answers. The
interesting part of building a system is the wrong turns, and those are exactly
what disappear from a clean repository. A `git log` of "add storage engine"
tells you nothing about the two hours spent on why a benchmark reported
negative latency.

---

## Contents

| # | Challenge | Phase | How it was caught |
|---|---|---|---|
| 1 | Non-ASCII never reaches the wire as UTF-8 | 0 | A failing test I wrote wrong |
| 2 | Pipelining was 1.05x, not "several times" | 1 | Benchmark contradicted the design doc |
| 3 | Latency measured as **−4.7 µs** | 1 | An impossible number |
| 4 | `acks=0` has no reply in a request/response protocol | 3 | Design, before coding |
| 5 | Retention silently served the wrong records | 2 | A test assertion |
| 6 | O(n) call on the hot write path | 2 | Code review of my own work |
| 7 | Retention re-scanned immutable files forever | 2 | Code review |
| 8 | The CLI lied while every test passed | 2 | Running it by hand |
| 9 | Two threads, one socket, permanent desync | 3 | A test that failed with "Bad magic" |
| 10 | Every key landed in partition 0 | 3 | A test assertion |
| 11 | Emulating Java's 32-bit overflow in Python | 3 | Known in advance, still fiddly |
| 12 | A test threshold that was wrong, not the code | 3 | Statistics |
| 13 | Windows would have corrupted every byte offset | 2 | Anticipated |
| 14 | Storage rewrite broke only the tests that deserved to | 2 | Design payoff |
| 15 | Documentation links rotted silently | — | A script |
| 16 | The reaper evicted members that were obeying the protocol | 4 | Three failing tests |
| 17 | Generation bumped too early, giving the wrong error | 4 | A failing test |
| 18 | A sequential driver cannot exercise a rendezvous | 4 | A chaos script that deadlocked |
| 19 | `Consumer` wasn't thread-safe and didn't say so | 4 | The same chaos script |

---

## 1. Non-ASCII never reaches the wire as UTF-8

**Phase 0 · [tests/test_protocol.py](../tests/test_protocol.py)**

**What happened.** I wrote a test asserting that a message containing `é` and
an emoji produces a body *longer* than its character count, to prove the length
header counts bytes and not characters. It failed: body and string were both
exactly 47.

**Why.** `json.dumps` defaults to `ensure_ascii=True`, so non-ASCII is escaped
to `é` before it is ever encoded. The body is pure ASCII. My mental model
of the encoding was simply wrong.

**The fix.** The code was right; my assumption was not. Rather than deleting the
test I split it in two: one that pins the real invariant (`length ==
len(body)`), and one that documents the *cost* of the escaping —
`é` travels as **six** bytes rather than two, and an emoji as **twelve**, sent
as a surrogate pair.

**What it taught.** Up to 6x bandwidth on non-English text, on a system whose
whole job is moving bytes. That is a real number I would not have known without
writing a test that failed for the wrong reason. A failing test is sometimes a
discovery about the platform rather than a bug in the code.

---

## 2. Pipelining was 1.05x, not "several times"

**Phase 1 · [docs/phase-1/phase-1-protocol.md](phase-1/phase-1-protocol.md)**

**What happened.** Phase 1 added correlation IDs specifically so a client could
stop waiting for each acknowledgement. I wrote in the design doc, *before
coding*, that this would be "worth several times the throughput on loopback."

The benchmark said **1.05x**.

**Diagnosis.** Rather than explaining it away, I instrumented where the time
actually went — comparing a cheap in-memory request (`METADATA`) against a
`PRODUCE`, each sequential and pipelined:

```
cheap request (METADATA), sequential : 130.1 µs
same request, pipelined              :  91.9 µs
→ loopback round-trip wait           :  38.2 µs   ← what pipelining removes
PRODUCE, sequential                  : 253.4 µs
→ broker work (thread hop + disk)    : 215.2 µs   ← what it cannot remove
```

**The answer.** Pipelining removes *waiting*. On loopback the wait is 38 µs of
a 253 µs request; the other 215 µs is the broker genuinely working. **The
broker was never idle, so there was no idle time to reclaim.** My Phase 0
commentary had asserted the broker "sits idle most of that time" — an
assumption this measurement disproved.

A second cause: the broker serves one request per connection at a time, so
pipelined requests queue and are still handled serially. Pipelining removes the
*client's* waiting, not the *broker's* serialisation.

**The fix.** I rewrote the phase doc, the `net.py` docstring, and
[benchmarks.md](benchmarks.md) around the measurement, keeping the wrong
prediction visible rather than quietly deleting it. The benchmark now projects
the gain at realistic round-trip times, where it is 2.9x on a LAN and 190x
cross-region.

**What it taught.** Latency-hiding optimisations have **no measurable value on
localhost**, because their benefit is a property of the network. Benchmark only
locally and you conclude the feature is useless — then ship something that
collapses at 50 ms.

---

## 3. The benchmark reported latency of −4.7 µs

**Phase 1 · [bench/bench.py](../bench/bench.py)**

**What happened.** The round-trip estimate — sequential minus pipelined time
for a cheap request — came out **negative**.

**Why.** It is a difference of two similar numbers, and I was computing it
*after* the produce runs, by which point the broker held several topics. The
`METADATA` response had grown larger and noisier than the difference being
measured, so noise swamped the signal.

**The fix.** Move the measurement to the start, on an empty broker, and warm up
first. It became a stable 38 µs. The function now carries a comment explaining
that it must run first and why.

**What it taught.** Differences of similar quantities are the fragile part of
any measurement — give them the quietest conditions available. And an
**impossible result is a bug in the instrument, not a discovery**. A negative
wait was obvious; had it read 4.7 µs instead of −4.7 µs I would have believed
it and drawn the wrong conclusion from it.

---

## 4. `acks=0` has no reply, in a strictly request/response protocol

**Phase 3 · [broker/broker.py](../broker/broker.py), [producer/producer.py](../producer/producer.py)**

**The problem, found while designing rather than debugging.** Real Kafka's
`acks=0` means the broker sends *nothing* back. But every layer here assumes
one request produces exactly one response — `SyncConnection.request()` sends
then reads, and correlation IDs match them up.

Three options:

| Option | Why not |
|---|---|
| Reply anyway, client ignores it | The unread reply stays in the socket and the *next* response reads it as a header. Permanent desync. |
| Reply immediately, before appending | Keeps the protocol shape, but invents a semantic Kafka does not have. Dishonest. |
| **Broker genuinely sends nothing** ← chosen | Faithful, and the stream stays in sync precisely *because* nothing is written. |

**The fix.** `_handle_produce_v1` returns `None` for `acks=0`, and the
connection loop skips the reply entirely. The producer calls `send_request()`
without a matching read, and resolves its futures with offset `-1` — there is
no acknowledgement, so there is no offset to report.

A test covers the case that would expose a mistake: mixing `acks=0` and
`acks=1` on the same connection
(`test_the_connection_survives_mixing_acks_zero_and_one`).

**What it taught.** "Request/response" was an assumption baked into three
layers, not a property anyone had chosen. Implementing the one case that
violates it made the assumption visible — and `acks=0` resolving to `-1` is now
a much better teaching artefact than a comment would have been. You asked not to
be told; you are not told.

---

## 5. Retention silently served records from the wrong offset

**Phase 2 · [broker/log/log.py](../broker/log/log.py)**

**What happened.** A test asserted that after retention deleted through offset
295, reading from offset 0 returns nothing. It returned records starting at
295.

**Why.** `_segment_index_containing(0)` binary-searched the surviving segments
and, finding none whose base offset was ≤ 0, returned index 0 — the *oldest
surviving* segment. Reading from it returned everything in it.

**Why this was the most dangerous bug in the project.** No exception, no log
line, no error code. A consumer asking to replay from the beginning would
believe it had done so while **295 messages silently vanished**. Every other
bug here announces itself; this one produces plausible, wrong data.

**The fix.** A guard in `Log.read`: below `log_start_offset`, return nothing.
The broker turns that into `OFFSET_OUT_OF_RANGE` — "you fell so far behind that
your data is gone" — which is the honest answer and is deliberately
non-retriable, since the data will not come back.

**What it taught.** The bug lived in the gap between two components that were
each individually reasonable. Segment lookup is correct in isolation; the
missing precondition belonged to the caller. **Silent wrongness is worse than
loud failure**, and it is exactly what a test asserting `== []` catches and a
test asserting "doesn't crash" does not.

---

## 6. An O(n) call on the hot write path

**Phase 2 · [broker/log/segment.py](../broker/log/segment.py)**

**What happened.** `Segment.append` decided whether to add an index entry with:

```python
if not self.index.entries() or self._bytes_since_index >= interval:
```

`entries()` builds a fresh list of every index entry. On **every single
append**. The write cost grew with the segment's size — quadratic overall.

**How it was caught.** Reading my own code back before running the benchmark,
not by a test. No test would have caught it: the behaviour is perfectly
correct, just slower and slower.

**The fix.** `len(self.index) == 0`, which is O(1).

**What it taught.** Correctness tests do not catch complexity regressions. The
seek-profile benchmark exists to catch the read-side version of this problem;
there is no equivalent guard on the write side, which is a real gap.

---

## 7. Retention re-scanned immutable files on every sweep

**Phase 2 · [broker/log/segment.py](../broker/log/segment.py)**

**What happened.** Time-based retention needs each segment's newest timestamp,
and `largest_timestamp()` computed it by scanning the whole segment. The
retention loop runs on a timer over every segment, so the cleaner's cost was
proportional to the entire log — precisely what segments exist to avoid.

**The fix.** Cache it, but **only once the segment is sealed**. A sealed segment
is immutable, so the cached answer cannot go stale; the active one is still
computed each time because it is still changing.

**What it taught.** "Is this immutable yet?" turned out to be the precondition
that makes caching safe, and the code already knew the answer (`self._fh is
None`). Immutability is not just a nice property of the append-only design —
it is what licenses optimisations elsewhere.

---

## 8. The CLI lied while every test passed

**Phase 2 · [tools/kfk.py](../tools/kfk.py)**

**What happened.** Phase 2 changed a partition from one flat file
(`data/orders-0.log`) to a directory of segments (`data/orders-0/`). All 120
tests passed. Then I ran the CLI by hand:

```
Offsets   : earliest=0  latest=3000  (3000 messages)
Log file  : ./data2\p2-0.log
Log size  : 0 bytes   (~0 bytes/message)
```

3,000 messages occupying zero bytes. `cmd_describe` was still `os.path.getsize`
on a path that no longer existed, and `os.path.exists` returned False, so it
reported 0 rather than crashing.

**The fix.** Sum the `.log` files in the partition directory; report the
directory and segment count instead of a fictional file.

**What it taught.** The tests covered the broker and the storage engine but
never the CLI — so the CLI silently rotted. And the failure mode was the worst
kind: a defensive `if exists` turned a crash into a plausible wrong number.
**Defensive coding that converts a loud failure into a quiet lie is a net
loss.** Every phase's verification step now includes running the tools by hand
for this reason.

---

## 9. Two threads, one socket, permanent desynchronisation

**Phase 3 · [producer/producer.py](../producer/producer.py), [common/net.py](../common/net.py)**

**What happened.** After adding the background sender thread, one test failed
with:

```
ProtocolError: Bad magic 0x7b226572 (expected 0xcafe)
```

`0x7b226572` is `{"er` — the beginning of a JSON body being read as a header.

**Why.** The sender thread used the connection for `PRODUCE`. A caller thread
used the *same* connection for `METADATA`, inside `send()`, to look up a
partition count. A request and its response are a pair, and that pair is not
atomic:

```
sender  ──PRODUCE──►
caller  ──METADATA─►
sender  ◄──────────── reads the METADATA reply as its own
```

After that the stream is permanently desynchronised.

**The fix.** A lock in `Producer` covering the whole request/response pair, and
an explicit "NOT THREAD-SAFE" warning on `SyncConnection` naming this exact
symptom — so the next person to hit `Bad magic 0x7b226572` finds the cause in
the docstring instead of deriving it.

**What it taught.** The bug was introduced by a feature (the sender thread) in a
component that had been single-threaded and correct for three phases. **Adding
concurrency invalidates assumptions that were never written down.** The error
message was also very far from the cause — which is why the fix included
documenting the symptom, not just removing it.

---

## 10. Every key landed in partition 0

**Phase 3 · [producer/producer.py](../producer/producer.py), [broker/broker.py](../broker/broker.py)**

**What happened.** A test produced 200 messages with 200 distinct keys to a
4-partition topic and asserted all four partitions were used. All 200 went to
partition 0.

**Why — an ordering problem, not a hashing one.** The producer needs the
partition count *before* routing its first message. For a brand-new topic:

1. Producer asks `METADATA` → the topic does not exist → falls back to 1
2. All 200 `send()` calls route with `count = 1` → partition 0
3. The first `PRODUCE` auto-creates the topic at its real width of 4
4. The producer's cached `1` is now wrong

Batching made it worse: all 200 `send()` calls happen before the sender thread
flushes, so *every* message routes with the stale count.

**The fix.** The one real Kafka uses: an `allow_auto_topic_creation` flag on the
metadata request. The producer sets it, so the topic is created during the
lookup and the count it caches is the real one. The flag defaults to **off**,
because a plain query must not conjure the thing it is asking about — that
would make "does this exist?" unanswerable, and it is why `LIST_OFFSETS`
deliberately does not create topics either.

**What it taught.** I had implemented auto-creation and metadata lookup
separately, each sensible alone, and never asked what happens on the *first*
message to a *new* topic. Kafka's flag had looked like a curiosity in the
protocol spec; it exists for precisely this.

---

## 11. Emulating Java's 32-bit overflow in Python

**Phase 3 · [producer/partitioner.py](../producer/partitioner.py)**

**The problem.** murmur2 is defined over Java's `int`: 32 bits, signed,
wrapping on overflow. Python integers are arbitrary precision and never wrap,
so a direct transcription produces different numbers — and the whole reason for
choosing murmur2 was that a Python producer and a Java producer agree on where
a key goes.

**The fix.** An `_i32` helper applied after every arithmetic step, plus care
with the shift operator: Java's `>>>` is a *logical* shift, so it needs
`(value & 0xFFFFFFFF) >> n` rather than Python's `>>`, which propagates the
sign bit.

Then a second, easier-to-miss issue in the modulo:

```python
(murmur2(key) & 0x7fffffff) % num_partitions
```

The mask is not there to avoid negative indices — Python's `%` already returns
a non-negative result. It is there because **Java's does not**: `-7 % 3` is `2`
in Python and `-1` in Java. Without masking first, the two languages would
disagree about where about half of all keys belong.

**What it taught.** "Port this algorithm" quietly means "port the arithmetic
model it was written in." The failure would have been silent — a
well-distributed, deterministic, *incompatible* hash that passes every test you
would think to write.

---

## 12. A test threshold that was wrong, not the code

**Phase 3 · [tests/test_partitioner.py](../tests/test_partitioner.py)**

**What happened.** A quick check of murmur2's distribution asserted that the
busiest and quietest of 12 partitions differ by less than 1.1x. It measured
1.108 and "failed".

**Why.** With 12,000 keys over 12 buckets, each bucket is binomial with
sd ≈ 30, so a spread of ±50 is under 2σ — entirely normal. My threshold was a
guess, and the guess was tighter than the natural variance of the thing being
measured.

**The fix.** Derive the tolerance instead of guessing it:

```python
sd = (n * (1/p) * (1 - 1/p)) ** 0.5
assert worst_deviation < 4 * sd
```

Four standard deviations is a bound a healthy hash will not cross by chance,
and a genuinely skewed one will.

**What it taught.** A test with an arbitrary threshold on a random quantity is
either flaky or useless, and you cannot tell which until it fails. If you are
asserting on something statistical, the tolerance has to come from the
statistics.

---

## 13. Windows would have silently corrupted every byte offset

**Phase 2 · [broker/log/](../broker/log/)**

**Anticipated rather than suffered**, and worth recording because it would have
been extremely hard to find.

Python's text mode on Windows translates `\n` to `\r\n` on write. The Phase 0/1
log was JSON lines, and reading in text mode translates back — so it worked,
invisibly, on both platforms.

Phase 2 stores **byte positions** in the index. One extra byte per record on
Windows would make every index entry point slightly wrong, and the reader would
land mid-record. Worse, the corruption would be **platform-specific**: correct
on Linux and CI, broken on a developer's Windows machine — or the reverse.

**The fix.** Binary mode everywhere in the storage engine, and
`newline='\n'` explicitly on the one text handle that predates it.

**What it taught.** Text mode is a convenience that assumes bytes and characters
are interchangeable. The moment you store a byte offset, they are not.

---

## 14. A storage rewrite that broke only the tests that deserved to

**Phase 2 · [tests/test_partition.py](../tests/test_partition.py)**

Not a bug — the clearest design feedback in the project.

Phase 2 replaced the entire storage engine. `broker.py` did not change a single
line, and of 120 tests exactly **three** broke. All three had reached past
`Partition`'s public API into file internals: `partition.log_path`,
`TAIL_READ_BYTES`, parsing JSON lines directly.

Every test that used only `append` / `fetch` / `next_offset` survived untouched.

**What it taught.** The tests that broke were the ones testing *implementation*
rather than *behaviour*, and the refactor sorted them automatically. I rewrote
`test_partition.py` to cover what `Partition` actually owns — identity, the
lock, delegation — and moved format testing to `test_log.py`, where it belongs.
**A refactor is a free audit of how well your tests are coupled**, and it is
worth paying attention to which ones break.

---

## 15. Documentation links rotted silently

**All phases**

Docs were reorganised into `docs/phase-N/` subdirectories partway through, and
every relative link in every file broke at once. Markdown links fail silently —
nothing errors, they just go nowhere.

**The fix.** A 12-line script that walks every `.md`, extracts every link, and
checks the target exists. It found and fixed all of them, and it is now run
after any doc restructuring.

**What it taught.** Documentation has referential integrity just like code, and
nothing checks it by default. If a class of error cannot announce itself, write
the thing that makes it announce itself.

---

## 16. The reaper evicted members that were obeying the protocol

**Phase 4 · [broker/coordinator.py](../broker/coordinator.py)**

**What happened.** Three group tests failed with
`UNKNOWN_MEMBER_ID: member ... was evicted during the rebalance` — raised from
inside the very join the member was still waiting on.

**Why.** The heartbeat thread starts only *after* `subscribe()` returns, and
`subscribe()` blocks inside `JOIN_GROUP` until the rendezvous completes. So a
member waiting in the rendezvous sends no heartbeats for the entire rebalance.
With a session timeout (800 ms) shorter than the rebalance timeout (5 s), the
reaper evicted members for doing exactly what the protocol told them to do.

**The fix.** A member with a request **parked in the coordinator** is alive by
definition — we are holding its connection open right now. `_expire_members`
now skips members in `joined_members` while a rebalance is in progress. Real
Kafka reasons the same way: an in-purgatory request counts as liveness.

**What it taught.** Liveness detection has to account for the states your own
protocol puts clients into. "No heartbeat" is not the same as "no evidence of
life", and conflating them punished correct behaviour.

---

## 17. The generation bumped too early, giving the wrong error

**Phase 4 · [broker/coordinator.py](../broker/coordinator.py)**

**What happened.** A test expected a heartbeat during a rebalance to return
`REBALANCE_IN_PROGRESS`. It returned `ILLEGAL_GENERATION`.

**Why.** I incremented the generation in `begin_rebalance()`, at the *start*.
So an existing member's next heartbeat carried a number that was already stale
and failed the fencing check before it ever reached the state check.

Both codes are actionable, so this looked harmless. It is not: they say
different things. `ILLEGAL_GENERATION` means "you are a zombie, throw away your
work"; `REBALANCE_IN_PROGRESS` means "the group is reorganising, rejoin". I had
collapsed two distinct situations into the more alarming one.

**The fix.** Bump the generation in `complete_join()`, once membership for the
round is settled — which is what Kafka does. Fencing is unaffected, because it
never depended on the early bump: an *evicted* member is removed from the
membership dict and fails the membership check regardless of generation.

**What it taught.** I had written a confident comment justifying the early bump
("this is what fences the previous generation immediately"). It was wrong, and
the test is what caught it. **A comment explaining why a design is right is not
evidence that it is.**

---

## 18. A sequential driver cannot exercise a rendezvous

**Phase 4 · [chaos/kill_consumer.py](../chaos/kill_consumer.py)**

**What happened.** The chaos script showed every consumer stranded in its own
generation — 7, 8 and 9 — each believing it owned all four partitions. The
group never converged. Meanwhile a minimal two-consumer script converged
perfectly in 230 ms, which meant the coordinator was fine and the script was
not.

**Why.** The script drove all consumers from one loop, polling them in turn.
But **`JOIN_GROUP` blocks**: the coordinator holds it until every member has
rejoined. So the driver entered alice's rejoin, alice waited for bob, and the
driver never reached bob. The rendezvous timed out, dropped the absentees, and
left each member alone.

**The fix.** Give each consumer its own thread, which is how consumers actually
run. It then converged immediately: all three in generation 3, four partitions,
zero duplicates.

**What it taught.** A rendezvous protocol requires genuinely concurrent
participants — that is a property of the protocol, not of the harness. I also
wasted two rounds theorising before writing the minimal reproduction that
isolated it in one run. **When a complex case fails and you cannot see why,
build the simplest case that should work.** If it works, the bug is in the
difference.

---

## 19. `Consumer` was not thread-safe and did not say so

**Phase 4 · [consumer/consumer.py](../consumer/consumer.py)**

**What happened.** While debugging #18 the script hit
`ProtocolError: Correlation mismatch: sent 2, got 1` — two threads using one
`Consumer`, each reading the other's reply, exactly like the producer bug in #9.

**The fix, and why it is not a lock that makes it work.** A non-blocking guard
that raises immediately:

> `Consumer is not thread-safe: poll() was called while another thread was
> still using this consumer. Give each thread its own Consumer.`

Serialising instead would have *hidden* a design error: two threads sharing a
consumer also share its position and assignment, so even a perfectly safe
connection would produce nonsense. Kafka's consumer does the same and calls it
`ConcurrentModificationException`.

**What it taught.** When misuse is possible, detecting it loudly beats making
it silently sort-of-work — the same principle as #5 and #8. Note also that the
producer needed a *lock* (its sender thread legitimately shares the socket)
while the consumer needed a *refusal* (its heartbeat thread gets its own
connection instead). The right answer depended on whether the sharing was
legitimate.

---

## Cross-cutting lessons

**Measure the claim, and keep the number that argues against you.** Phase 1's
prediction about pipelining was wrong by an order of magnitude. Keeping the
wrong prediction next to the real measurement is more instructive than a
document that only ever turned out to be right.

**Silent wrongness is the enemy.** The dangerous bugs here — retention serving
the wrong offsets (#5), the CLI reporting zero bytes (#8), keys all landing in
partition 0 (#10) — produced plausible results and no errors. The loud ones
(desync, CRC failures) were fixed in minutes.

**Tests catch behaviour; reading catches complexity.** #6 and #7 were both
found by re-reading code that was already correct.

**Run the thing by hand.** #8 passed 120 tests and was visibly broken the first
time a human looked at its output.

**New concurrency invalidates old assumptions.** #9 broke a component that had
been correct for three phases, the moment a second thread touched it.

**An impossible result is a broken instrument.** #3 and #12 were both
measurement bugs presenting as findings.
