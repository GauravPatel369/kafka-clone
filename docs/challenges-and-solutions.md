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
| 20 | `METADATA` only ever reported itself | 5 | A chaos script reporting 0 stored |
| 21 | The roster recorded port 0 | 5 | 30 tests timing out at once |
| 22 | `CREATE_TOPICS` waited on an unreachable peer | 5 | One test timing out |
| 23 | Topic metadata never replicated — partitions vanished on failover | 5 | Running it by hand |

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

## 20. `METADATA` only ever reported itself

**Phase 5 · [broker/broker.py](../broker/broker.py)**

**What happened.** The chaos script printed
`messages stored per broker: b1=0 b2=0 b3=0 — total: 0` after producing 600
messages.

**Why, twice over.** `_handle_metadata` returned
`'brokers': [{'node_id': self.node_id, ...}]` — only itself. A client
bootstrapping to broker 1 learned that partitions lived on brokers 2 and 3, and
then had no address for either. Every send failed.

And **the script never noticed**, because it called `producer.send()` 600 times
and inspected none of the returned futures. A future nobody inspects is an
error nobody sees — which is the entire reason `get()` re-raises.

**The fix.** `'brokers': self.cluster_config.as_list()`, and the script now
checks every future and prints a failure count.

**What it taught.** Two failures compounding: the code was wrong, and the
harness was built so it could not tell. The second is the more dangerous habit
— I had written the "check your futures" lesson into the Phase 3 docs and then
did not do it.

---

## 21. The roster recorded port 0

**Phase 5 · [broker/broker.py](../broker/broker.py)**

**What happened.** Fixing #20 immediately broke **30 tests**, most of them by
timing out.

**Why.** Tests start brokers with `port=0` — "OS, pick a free port" — and read
the real port back after binding. But `ClusterConfig.single_node(node_id, host,
port)` is built in `__init__`, *before* `start()` binds. So the roster recorded
port **0**, `METADATA` handed that to clients, and clients dutifully dialled
port 0 and hung until their socket timeout.

**The fix.** Correct the roster entry in `start()`, right after the real port is
known.

**What it taught.** A value that is only knowable *after* construction cannot be
captured *during* it. The bug was latent from the moment the roster was added
and invisible until something actually read it — the fix for #20 did not cause
it, it revealed it.

---

## 22. `CREATE_TOPICS` waited on an unreachable peer

**Phase 5 · [broker/broker.py](../broker/broker.py)**

**What happened.** A test's setup line — a plain `CREATE_TOPICS` — timed out.

**Why.** Creating a topic triggers `_refresh_placement()`, which **awaited**
`_push_controller_update()`, which tried to connect to every peer. With one
peer unreachable, the client's request sat behind a full connect timeout.

**The fix.** Fire the broadcast as a background task. The push is something the
controller owes its peers, not part of the requester's transaction.

**What it taught.** One dead broker was causing a **cluster-wide latency
problem** rather than a local one — the shape of a cascading failure, in
miniature. Also: `asyncio` keeps only a weak reference to tasks, so a
fire-and-forget task needs a strong reference or it can be collected
mid-flight.

The consequence is real and had to be tested for: peers now converge *shortly
after* a change rather than instantly, so `test_partitions_are_spread_across_the_cluster`
polls until they agree. A peer that has not caught up answers NOT_LEADER, and
the client refreshes — the gap is covered, not hidden.

---

## 23. Topic metadata never replicated — partitions vanished on failover

**Phase 5 · [broker/metadata.py](../broker/metadata.py)**

**What happened.** Running a real three-process cluster by hand: create
`orders` with 6 partitions, produce, kill broker 1. `kfk cluster status` on
broker 2 showed **one** partition led, total. Five had disappeared.

**Why.** The controller pushed the *leader map* but not the *topic list*.
Broker 1 knew `orders` had 6 partitions; brokers 2 and 3 auto-created it with 1
when a produce arrived. When broker 1 died, the new controller recomputed
placement from **its own** metadata — which said one partition.

**The fix.** Ship topic definitions with every `CONTROLLER_UPDATE`, and have
peers adopt them via `ensure_topic()`. Widening is accepted (the controller
correcting an under-informed guess); narrowing is refused, because losing
partitions strands their data and remaps every key.

**What it taught.** I had *documented* this gap in the split-brain script
("ours does not replicate topic metadata") and treated it as acceptable. It was
not — it made failover silently lose partitions. **Writing a limitation down is
not the same as evaluating it.**

And it was only found by running the thing by hand, exactly like #8. All 234
tests passed throughout, because every test creates its topic on the broker it
then talks to.

---

## 24. A quorum of two healthy brokers could not elect a leader

**Phase 5 · [broker/raft/node.py](../broker/raft/node.py)**

**What happened.** `chaos/kill_controller.py` killed the controller of a
three-broker cluster. The two survivors — a clear majority of three — never
elected a replacement. Not slowly: *never*. Terms climbed one per second,
forever, with no leader and no error:

    Node 2 starting election for term 6 (needs 2 of 3 votes)
    Node 3 starting election for term 6 (needs 2 of 3 votes)
    Node 2 lost election for term 6: 1 of 2 votes
    Node 3 lost election for term 6: 1 of 2 votes

**Why.** `_begin_election` tallied votes with `asyncio.gather`, which finishes
at the pace of the **slowest** peer — and a dead peer is the slowest possible
peer, because it costs the full RPC timeout every time. So each candidate sat
for two seconds inside `gather`, waiting on a corpse, while its own 1–2 s
election timer expired underneath it. The other survivor then campaigned for a
higher term, and when `gather` finally returned, the guard
`self.current_term != term` discarded a vote that had been **granted**. Both
nodes did this to each other, in lockstep, indefinitely.

**The fix.** Count each vote as it arrives and promote on the majority
immediately, rather than tallying at the end. The candidate stops caring about
the dead peer entirely.

**What it taught.** *"Concurrent" is not the same as "does not wait."* The RPCs
were already issued in parallel; the bug was in waiting for all of them. Any
code that awaits every peer runs at the speed of the worst one, so on a
distributed system that phrasing — "wait for all" — deserves suspicion wherever
it appears.

---

## 25. The same bug, twice more, and why every test still passed

**Phase 5 · [broker/raft/node.py](../broker/raft/node.py),
[broker/broker.py](../broker/broker.py)**

**What happened.** With the vote count fixed, the survivors elected in
milliseconds — and then flapped. Leadership alternated between them roughly
once a second, terms climbing without end, on a cluster where nothing was
wrong with either node.

**Why.** "Wait for the slowest peer" was in three places, not one:

| Where | What it cost |
|---|---|
| `_begin_election` tallying votes | a granted vote discarded as stale (#24) |
| `_run` **awaiting** the election | a node won in ~5 ms, then sent nothing for 2 s |
| `_heartbeat_tick` via `_replicate_to_all` | a 150 ms heartbeat became a 2 s one |
| `Broker._heartbeat_peers` | membership flapped against a 700 ms session timeout |

Each is the same mistake in a different costume, and they compound: a 2 s
heartbeat against a 1–2 s election timeout means healthy followers depose a
healthy leader on schedule. Raft stayed **safe** throughout — never two
leaders — but nothing ever committed, because each new leader was deposed
before it finished anything.

The membership flap made it worse in a way worth noticing: liveness changes
drive placement, so the controller re-proposed the partition map on every
flap, and each proposal spent its own two seconds waiting on the same corpse.
One dead broker out of three kept the survivors permanently busy
rediscovering each other.

**The fix.** Timers fire work off and return. `_heartbeat_tick` spawns
per-peer replication instead of gathering it; `_begin_election` spawns its
canvass; `_heartbeat_peers` spawns per-peer heartbeats. Each keeps an
`_inflight` set so a slow peer is skipped rather than queued behind itself.
`_replicate_to_all` still awaits — that is correct for `propose`, which needs
the outcome.

**What it taught.** *A cadence must be a property of the thing keeping time,
never of the thing it is waiting for.* That is the single sentence version of
all four bugs.

And the reason none of this was caught: **the test transport had no dead
peers.** A blocked message returned `None` instantly, so waiting for every
peer cost nothing and the implementation looked correct in all 24 Raft tests.
A real dead broker is not silent, it is *slow*. Modelling failure as silence
tested a network that does not exist.

The fix there is a `down_delay` in the test network and one test that asserts
the survivors elect **and are still leading 1.5 s later** — because electing
once was never the hard part. It fails against every version of the code that
shipped before it.

---

## 26. A one-voter quorum could no longer elect itself

**Phase 5 · [broker/raft/node.py](../broker/raft/node.py)**

**What happened.** The fix for #24 broke 42 tests at once. A single broker
never became a controller, so every request was answered
`NOT_LEADER_OR_FOLLOWER`.

**Why.** Promotion had moved into the RequestVote *response* handler. A lone
voter has no peers, so it sends no requests, so it receives no responses, so
nothing ever checked that its own vote was already a majority.

**The fix.** Check the threshold once after casting the self-vote, before
canvassing.

**What it taught.** Incremental algorithms need their base case checked
explicitly: "on each arrival, test the total" silently skips the case where
nothing arrives. This one was caught instantly and loudly, which is the whole
difference between it and #24 — and the reason it is a footnote rather than a
day. Worth noting that quorums of one are not a toy: they are what a
development cluster and most of this test suite actually run.

---

## 27. A test that could not pass, and the boundary it found

**Phase 5 · [tests/test_raft_snapshot.py](../tests/test_raft_snapshot.py)**

**What happened.** Writing the tests for log compaction, one refused to pass:
a follower was partitioned, the leader compacted and moved on, the partition
healed — and the follower never converged. Eleven of twelve tests passed; this
one looped forever, printing `peer 2 needs a snapshot (wants index 1, log
starts at 9)` several times a second.

**Why.** The warning was correct and the test was wrong. Partitioning the
follower *before* any replication left it needing index 1, which the leader had
long since discarded. That is not the case Step A handles — it is precisely the
case `InstallSnapshot` exists for, and `InstallSnapshot` had not been built yet.
The test was asking Step A to do Step B's job.

**The fix.** Rewrite the setup so the follower is behind the leader's *last*
index but at or after its *snapshot* index: stay connected across two
compactions, then miss fewer entries than the threshold. That is the genuine
Step A case, and it passes.

**What it taught.** Two things, and the second is the useful one.

The narrow lesson: compaction introduces a failure mode that did not previously
exist. Before it, "behind" was one state and always recoverable. After it,
"behind" splits into *recoverable* and *unrecoverable*, and the line between
them is `first_index`.

The broader one: a test that cannot pass is worth more than a test that was
never written, provided you work out *why* rather than adjusting it until it
goes green. Loosening the assertion here would have produced a passing suite
that quietly claimed a guarantee the code does not offer. The failing test was
the thing that made the Step A / Step B boundary concrete enough to document.

**A footnote of the same shape.** `docs/phase-5/raft-snapshots.md` originally
claimed that deleting the log sentinel would break "only the follower catch-up
case". Running it broke seven tests immediately — the sentinel also anchors the
offset arithmetic. The *subtle* break turned out to be keeping the sentinel and
zeroing its term, which fails three. The doc now says both, because an exercise
whose stated outcome is wrong teaches the reader to distrust the exercises.

---

## 28. Two numbers stated from memory, one of them wrong by twenty times

**Phase 5 · [docs/phase-5/raft-install-snapshot.md](phase-5/raft-install-snapshot.md)**

**What happened.** The `InstallSnapshot` doc justified sending a snapshot in
one message rather than chunking it, and put a number on where that breaks:
"roughly ten thousand partitions." The number was invented — a plausible-
sounding figure written to make the trade-off feel concrete.

**Why it was wrong.** Measuring took four lines. A partition assignment
serialises to about 54 bytes of JSON, and `MAX_REQUEST_BYTES` is 10 MB, so the
real ceiling is near **190,000 partitions** — off by a factor of twenty, and
wrong in the direction that makes the design look worse than it is.

**The fix.** Build the snapshot at four sizes, measure each, and put the table
in the doc.

**What it taught.** The same lesson as Phase 1's pipelining prediction, which
was also wrong by an order of magnitude: **a number in a document is a claim,
and a claim nobody measured is decoration.** The tell is grammatical —
"roughly", "on the order of", "should be around" are what invented numbers
wear. This one mattered a little more than most, because the whole point of
stating a limit is so a reader can decide whether it applies to them, and a
reader with 20,000 partitions would have been told to chunk for no reason.

---

## 29. Eleven passing tests, and every partition had one copy

**Phase 6 · [broker/broker.py](../broker/broker.py) · [broker/raft/machine.py](../broker/raft/machine.py)**

**What happened.** Step 1 of replication added replica sets: `compute_assignment`
grew a replication factor, walked the broker ring, and returned
`{leader, replicas}`. Eleven unit tests pinned down every property that
mattered — no duplicate brokers in a set, leadership still spread evenly,
deterministic output, per-topic factors, clamping to the cluster size. All
green.

Then a real three-broker cluster created `orders` with `replication_factor=3`
and printed this:

```
orders-0    1        [1]
orders-1    2        [2]
```

**Why.** `CREATE_TOPIC` carried the partition count and *not* the factor. The
factor was written into the local `MetadataStore` of whichever broker served
the request — but placement runs on the **controller**, which need not be that
broker, and reads its own store. The number never travelled.

A second bug sat behind it: `ensure_topic` returned early when the width
already matched, so even a broker that *did* learn the factor would not have
applied it.

**The fix.** Commit `replication_factor` inside the `CREATE_TOPIC` command,
keep it in the state machine, carry it through `snapshot`/`restore`, and let
`ensure_topic` adopt a changed factor in either direction — width is
structural and may only widen, a factor is policy and may go down.

**What it taught.** The tests were not weak; they were aimed one layer too
deep. Every one called `compute_assignment` directly, and **a test that hands
the algorithm its inputs can never discover that the inputs never arrive.**
This is the same shape as #24 (an in-memory transport with no slow peers) and
#28 (a number nobody measured): the unit is right, and the wiring around it is
what is wrong. The scratchpad script that started three real brokers found it
in one run.

The rule this project keeps re-learning: **exercise the seam, not the
function.** Phase 6's later steps get an end-to-end check before their unit
tests are believed.

---

## 30. The ISR froze at `[1]`, and `acks=all` was 358x slower

**Phase 6 · [broker/controller.py](../broker/controller.py) · [broker/replica.py](../broker/replica.py)**

Steps 4 and 5 passed every unit test and then failed twice on three real
brokers, in ways the unit tests structurally could not see. Both are the same
mistake wearing different clothes: **reasoning about a component while ignoring
who it actually talks to.**

### The ISR that could never grow

`_observed_isr` had two branches — *I lead this partition, so I can see the
lag* and *I do not, so carry the committed value forward*. Correct-looking, and
fatal, because **the controller usually leads none of the partitions it is
placing.**

```
ctl=2 leader=1
t=0.0  committed_isr=[1]  observed=[2]  proposed=[[1]]  differs=False
t=3.5  committed_isr=[1]  observed=[2]  proposed=[[1]]  differs=False
```

The ISR was committed as `[1]` in the instant before the followers first
fetched, and then nothing could ever change it: broker 2 was the controller and
had no fetches to observe, so it carried `[1]` forward forever. Every
`acks=all` write failed `min.insync.replicas` on a cluster with nothing wrong
with it, permanently.

**Fix:** partition leaders report their own ISR on the heartbeat they were
already sending, and the controller aggregates. A report is only believed from
the broker that actually leads that partition.

### The leader that lied about its replicas

Isolate a leader that is *also* the controller and it cannot commit anything —
including the news that its followers have gone. It kept reporting
`ISR [1, 2, 3]` from the last committed value while observing `[1]`, and
accepted `acks=all` writes claiming three copies while holding one.

**Fix:** acceptance uses the **intersection** of committed and observed. That
can only ever be smaller, which is the safe direction — a leader may refuse a
write it could have taken, and may never take one it should refuse.

### 358x

```
acks=1     1,637 msg/s   p50   0.36 ms
acks=all       5 msg/s   p50 218.19 ms      → 358x
```

218 ms is not a coincidence: it is `REPLICA_FETCH_IDLE_MS`, exactly. **A
producer waiting for replication was waiting for the follower's sleep
schedule.** A follower discovers records by asking, so `acks=all` costs
*however long until the follower next asks* — not a round trip.

**Fix:** long-polling. The leader holds an empty fetch open (`max_wait_ms`)
instead of answering "nothing", and returns the instant a record lands. That is
what Kafka's `fetch.max.wait.ms` is for. 358x → ~6x.

### What it taught

The prediction in the phase doc said `acks=all` would cost 30–50%. The measured
cost is ~600%, and the reasoning error is the interesting part: the prediction
priced replication as a **network** cost (0.035 ms of loopback RTT) when it is
really a **scheduling** cost — an append, a poll, an append, another poll to
report the position, and a leader noticing.

That is the third time this project has been wrong about a number by reasoning
from an architecture diagram instead of from what the code actually waits on
(#7 pipelining, #28 the snapshot ceiling). The tell is the same every time:
**a prediction that names a mechanism but not a wait.**

---

## 31. A fencing check pointed the wrong way, and fenced nothing

**Phase 6 step 6 · [broker/broker.py](../broker/broker.py)**

**What happened.** Leader-epoch fencing went in: the controller bumps a
per-partition epoch whenever leadership changes, commits it through Raft,
`METADATA` hands it to clients, and clients send it back on produce and fetch.
`_not_leader` compares it. All 315 tests passed.

Then a direct check, on three real brokers:

```
after failover: leader=2 epoch=2
  old leader 1 still thinks epoch=1, leads=True
  produce to STALE leader carrying epoch 2 → error_code=0     ← fenced nothing
```

**Why.** The comparison was backwards. I had written:

```python
if int(epoch) < current:        # the CLIENT is stale
```

which is the harmless direction — we still lead the partition, so serving the
request is correct. The dangerous case is the mirror image: **the caller knows
a *newer* epoch than the broker does.** The only way that can happen is that
leadership moved and this broker has not heard — which is precisely the
data-plane split-brain the mechanism exists to stop.

**The fix.** Check both, with the important one first:

```python
if theirs > mine:   # THIS BROKER is stale — the case the feature is for
if theirs < mine:   # the caller's map is old; refuse so it refreshes
```

After it, the same call returns `error_code=74`, *"broker 1 believes it leads
orders-0 at epoch 1, but the caller knows epoch 2; this broker is stale."*

**What it taught.** Everything around the check was right — the epoch was
computed correctly, committed correctly, propagated correctly, and the error
code existed with a good docstring. The single comparison operator was
inverted, and **a fencing check pointed the wrong way is indistinguishable
from a working one until something is actually stale.** No unit test would
have caught it, because a unit test would have asserted whatever the
implementation did.

The general form, and the fourth time this project has met it: **a guard is
only tested by the thing it guards against.** Journal #24 needed a *slow* dead
peer, #29 needed a real controller that led no partitions, #30 needed a leader
cut off from its own quorum. This needed a broker that had genuinely lost
leadership without knowing. In every case the ingredient was a real failure,
not a mock of one.

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

**A bug found once is a bug worth searching for.** #24 was fixed, and the same
mistake was still in three other places (#25). After understanding *why*
something was wrong, grep for the shape of it before moving on.

**Model failure the way it actually fails.** The Raft tests modelled a dead
node as instant silence, which is cheap, obvious, and wrong: dead things are
slow, not quiet. An entire class of timing bug was invisible for a whole phase
because of that one simplification in a test helper.
