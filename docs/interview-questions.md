# Interview Preparation: Concepts and Questions from This Project

A map from *what you built* to *what you will be asked*. Every question here is
one that actually gets asked about messaging, distributed systems, or a project
like this on a CV.

---

## How to use this

**Ground every answer in something you did or measured.** The difference
between a forgettable answer and a memorable one is rarely knowledge — it is
specificity:

> ❌ "Pipelining improves throughput because you don't wait for each response."
>
> ✅ "I implemented pipelining and measured 1.05x — basically nothing. The cost
> breakdown showed the loopback round trip was 38 µs while a produce cost
> 253 µs, so 215 µs was the broker actually working. There was no idle time to
> reclaim. It only pays off when round trip dominates work — projected 2.9x on
> a LAN, 190x cross-region."

The second answer demonstrates that you measure, that you were willing to be
wrong, and that you understand the mechanism. That is what is being tested.

### Be honest about what is built

**Built: Phases 0 and 1.** Single broker, append-only log, framed protocol with
correlation IDs and error codes, at-least-once consumer, tests, benchmarks,
chaos scripts, admin CLI.

**Designed but not built: Phases 2–7** — segments and index, partitioning,
consumer groups, clustering, replication.

Say so. "I've implemented through Phase 1; partitioning and replication are
designed and documented but not written yet" is a completely respectable
answer, and it is the one that survives a follow-up question. Claiming
replication you have not built collapses the moment someone asks how you handle
the high watermark, and everything you *did* build loses credibility with it.

---

# Part A — Concept questions

## A1. Fundamentals: what is Kafka, really?

### Q: What is the difference between Kafka and a message queue like RabbitMQ?

**The core answer.** A queue is *destructive*: a consumer takes a message and
it is gone. Kafka is an *append-only log*: messages stay on disk and consumers
track their own position (an offset).

Everything else follows from that one choice:

| Consequence | Why |
|---|---|
| Multiple independent consumers | Reading doesn't mutate, so N consumers read the same bytes |
| Replay | The data is still there; reset an integer and reprocess |
| Cheap fan-out | One more reader = one more cursor, not a copy of the data |
| Very high throughput | The broker tracks one integer per consumer, not per-message delivery state |

> The line that lands: **"A queue broker must remember, per message per
> consumer, whether it was delivered and acknowledged. A log broker remembers
> one integer per consumer. That is the whole performance story."**

**Follow-up trap — "So Kafka is always better?"** No. A queue gives you
per-message acknowledgement, dead-letter queues, priority, and per-message TTL
essentially for free. Kafka gives you none of those. If you have 10,000 job
types with individual retries and priorities, a queue is the right tool. Kafka
wins for high-volume event streams with multiple consumers and replay needs.

**In this repo:** [docs/00-overview.md](00-overview.md) §2;
`test_two_groups_each_receive_every_message` in
[tests/test_end_to_end.py](../tests/test_end_to_end.py).

### Q: Explain topic, partition, and offset.

- **Topic** — a named stream. Purely logical.
- **Partition** — an ordered append-only log file. **The real unit of
  everything**: ordering, parallelism, replication, and assignment are all per
  partition.
- **Offset** — a monotonically increasing integer position within one
  partition. Assigned by the broker, never reused, never changed.

> The line that lands: **"Translate 'topic' into 'set of partitions' in your
> head and most of Kafka becomes obvious."**

### Q: Does Kafka guarantee ordering?

**Within a partition, yes. Across a topic, no.** And that is deliberate, not a
limitation.

Global ordering would force every write through a single point, capping
throughput at one machine forever. Kafka instead lets *you* choose what must be
ordered: use a key (customer ID, account ID), and all events for that key hash
to the same partition and are strictly ordered. Different keys may interleave,
and you almost never care.

> **"Scalability and ordering trade against each other, and the partition key
> is the dial."**

**Follow-up trap — "What breaks ordering even within a partition?"** Two
things, and knowing them is the mark of someone who has actually operated this:
1. **Retries with `max.in.flight.requests.per.connection > 1`** (default 5): a
   retried request can land *after* one sent later. The idempotent producer
   (sequence numbers per partition) fixes it.
2. **Increasing the partition count** — `hash(key) % N` changes when N changes,
   so a key that used to go to partition 2 now goes to partition 5, and its old
   and new events are in different logs. This is why partition counts are
   painful to change and why you over-provision them up front.

---

## A2. Storage: why the log design is fast

### Q: Why is an append-only log fast?

**Physics.** A random write requires a disk seek (~10 ms on spinning media).
Appending to a file you already have open requires none — the head is already
there.

```
Random writes:      ~100/sec
Sequential writes:  ~100,000/sec
```

SSDs narrow the gap but do not close it: sequential writes still avoid
write-amplification and use full erase blocks.

Then the consequences: messages are immutable, so many readers need no locking;
offsets are dense, so a position is a meaningful permanent address; deletion is
per-file rather than per-message, so it is also sequential.

**In this repo:** [broker/partition.py](../broker/partition.py) module
docstring.

### Q: How does Kafka read from the middle of a huge log efficiently?

**Segments plus a sparse index.** The log is split into segment files named by
their base offset (`00000000000000000000.log`). Each has an `.index` mapping
offset → byte position, with an entry every ~4 KB rather than per message.

To read offset 5,000,000: binary search the segment list, binary search the
index to the nearest entry, then scan forward a few KB.

**Why *sparse*?** A dense index of a billion messages would be gigabytes of
RAM. Sparse trades a short scan for a tiny index — and the scan is sequential,
which is the operation disks are good at.

> **Honest framing for your project:** "My current `fetch` scans the log from
> byte zero every time — O(n). I measured it: fetch latency climbs from ~1 ms
> at the start of a 5,000-message log to 13.8 ms at the end. Segments and a
> sparse index are Phase 2 specifically to flatten that curve."

That answer is *better* than having built it, because it shows you measured the
problem before reaching for the solution.

### Q: What is log compaction and when would you use it?

Two retention policies:
- **Delete** (default) — discard segments older than `retention.ms` or beyond
  `retention.bytes`.
- **Compact** — keep the *latest value for each key* forever, discarding
  superseded ones.

Compaction turns a log into a **changelog you can replay into a table**: read
from offset 0 and you get the current state of every key. It is how
`__consumer_offsets` works, and how Kafka Streams state stores are rebuilt
after a crash.

**Follow-up trap — "How do you delete a key under compaction?"** A **tombstone**:
a record with the key and a `null` value. Compaction keeps it for
`delete.retention.ms` so every consumer sees the deletion, then removes it.

### Q: Does Kafka fsync every write?

**No — and this is the question that separates memorised answers from
understanding.**

Kafka writes to the OS page cache and returns. It does *not* fsync by default
(`log.flush.interval.messages` defaults to effectively infinite).

- `flush()` → OS page cache. Survives **process crash**. ~1 µs.
- `fsync()` → physical disk. Survives **power loss**. ~1–10 ms.

That is a 1000x difference. Kafka's position: **buy durability with replication
rather than with fsync.** With three copies on three machines, losing one
node's page cache loses nothing, because two others have the data. Paying 10 ms
per write to defend against a failure that replication already covers is a bad
trade.

**Follow-up trap — "So when can Kafka lose data?"** A **correlated** failure:
a rack-wide power cut takes all three replicas' page caches simultaneously.
Replication defends against *independent* failures. This is why
rack-awareness and multi-AZ placement exist.

**In this repo:** the `append` method's comment in
[broker/partition.py](../broker/partition.py); Exercise 3 in
[phase-0-foundation.md](phase-0/phase-0-foundation.md) has you add the fsync and
measure the collapse.

---

## A3. Networking and protocol

### Q: You're sending messages over TCP. How does the receiver know where one message ends?

**It doesn't, unless you tell it.** TCP is a *byte stream*: it guarantees your
bytes arrive in order and guarantees nothing about grouping.
`sendall(b"HELLO"); sendall(b"WORLD")` can arrive as `HELLOWORLD`, or `HEL` +
`LOWOR` + `LD`.

So every protocol on TCP must do its own **framing**. Three options:

| Strategy | Trade-off |
|---|---|
| Delimiter (newline) | Simple; needs escaping, breaks on binary, can't pre-allocate |
| **Length prefix** | Binary-safe, no escaping, allocate once. Kafka, gRPC, everyone serious |
| Close-to-signal-end | One message per connection. HTTP/1.0, and why it was slow |

**Follow-up trap — "What's wrong with `sock.recv(n)`?"** It returns *up to* n
bytes, not n. Code that assumes otherwise works perfectly on localhost — where
everything arrives at once — and fails the moment there is a real network. You
must loop until you have exactly n. A return of `b''` is EOF, not "nothing yet".

**In this repo:** `_recv_exactly` in [common/net.py](../common/net.py).

### Q: Why does a protocol need a correlation ID?

Without one, a client cannot tell which response answers which request, so it
can only ever have one request in flight — and pays a full round trip of doing
nothing per message. With one, it can pipeline: send N, then match N replies by
ID.

**This is where you tell the story about being wrong** (see A6 below).

**Follow-up trap — "Kafka guarantees responses come back in request order per
connection. So why does it need correlation IDs at all?"** Ordering alone tells
you nothing if you reconnect or a response is dropped, and the ID makes a
packet capture diagnosable. Also note the ordering guarantee has a famous
consequence: with `max.in.flight > 1` plus retries, a retried request can land
after a later one and reorder a partition.

### Q: Why version each API separately instead of versioning the protocol once?

**Rolling upgrades.** A ten-broker cluster restarts one node at a time over an
hour; old and new coexist throughout and must interoperate. Per-API versioning
means adding a field to `Fetch` doesn't force a new `Produce`.

Real Kafka goes further: the `ApiVersions` request (key 18) lets a client ask
each broker what it supports and pick the highest common version *per API*.

> **"A protocol with no version number cannot be upgraded without downtime."**

### Q: What's the security problem in `body_length = read_int(); buffer = allocate(body_length)`?

The length arrives **from the network** and is trusted. A client can claim 4 GB
and the server dutifully tries to allocate it. Three connections take the
process down — no credentials required, just a socket.

**A claimed length is a request to commit *your* memory, so validate it before
honouring it.** Real Kafka has two limits for two purposes:
`socket.request.max.bytes` (100 MB, bounds one allocation) and
`message.max.bytes` (1 MB, bounds what a consumer must buffer).

**In this repo:** `MAX_REQUEST_BYTES` in
[common/protocol.py](../common/protocol.py) and the check in `handle_client`;
`test_oversized_request_is_refused_before_allocation`.

### Q: Why disable Nagle's algorithm (`TCP_NODELAY`)?

Nagle delays small writes up to ~40 ms hoping to coalesce them. Great for bulk
transfer; terrible for request/response, where an 80-byte request waits for
company that never arrives. The right way to fill packets is batching in the
*application*, which knows what belongs together — not letting the kernel guess
by stalling.

---

## A4. Delivery semantics — the highest-value topic

### Q: Explain at-most-once, at-least-once, and exactly-once.

It comes down to **where you commit the offset relative to doing the work**:

```
  fetch ──► commit ──► process        fetch ──► process ──► commit
                  ▲                                     ▲
             crash = LOST                        crash = REDELIVERED
             at-most-once                        at-least-once
```

There is no third option, because a crash can always land in the gap between
"did the work" and "recorded that I did it".

| Semantic | Failure mode | Use for |
|---|---|---|
| At-most-once | Messages **lost** | Metrics, click tracking — where losing a few beats double-counting |
| At-least-once | Messages **duplicated** | Almost everything. The sane default |
| Exactly-once | Neither | Needs the work and the commit to be one atomic operation |

**The line that lands:** *"Exactly-once **delivery** is impossible;
exactly-once **processing** is achievable."* You cannot guarantee the network
delivers once. You *can* make duplicate delivery have no visible effect — make
processing idempotent (UPSERT not INSERT, a dedup key, check-before-act).

**Follow-up trap — "But Kafka advertises exactly-once semantics. Explain."**
Kafka's EOS is real but narrow: the idempotent producer (a producer ID +
per-partition sequence number, so the broker discards duplicate retries) plus
transactions spanning multiple partitions and the offset commit. It gives
exactly-once for **Kafka-to-Kafka** pipelines. The moment you write to an
external database, you are back to making that write idempotent yourself.

**In this repo — and this is your best story.** `chaos/kill_consumer_midprocess.py`
runs the *same crash twice*, moving one line:

```
RUN A (commit first)   msg-5..msg-9 LOST FOREVER — silently
RUN B (commit last)    0 lost, msg-0..msg-4 processed twice
```

> "The original code committed the offset inside `poll()`, before the caller
> had processed anything — at-most-once by accident, with silent data loss on
> every crash. I made the commit explicit, defaulted to at-least-once, and
> wrote a chaos script that demonstrates both by moving one line."

### Q: Why default to at-least-once rather than at-most-once?

Duplicate work is **loud, visible, and curable** (idempotency). Lost data is
**silent, permanent, and usually unrecoverable** — you find out from a
customer. Given a forced choice, take the failure you can detect and fix.

### Q: A consumer commits after every batch of 1000 instead of every batch of 10. What changes?

Fewer disk/network writes for commits, but the **duplicate window** grows: a
crash replays up to 1000 messages instead of 10. Same trade-off, with a dial on
it. It also means a rebalance hands another consumer up to 1000 already-done
messages.

---

## A5. Concurrency

### Q: Why does your broker use asyncio instead of a thread per connection?

A broker's work is almost entirely *waiting* — on the network, on the disk.
Actual computation is a rounding error.

| Model | Trade-off |
|---|---|
| Thread per connection | Simple; ~8 MB stack each, falls over ~1,000 clients |
| Process per connection | Isolation, but heavier and can't share state without IPC |
| **Event loop** | ~KB per connection, tens of thousands. But **one blocking call freezes everything**, and you get one core |

**Follow-up trap — "Your storage layer does blocking file I/O. Doesn't that
freeze the loop?"** Yes, which is why `_route` hands those calls to a thread
pool via `run_in_executor`. The loop stays free while a worker blocks on disk.
Mixing the two models is the standard asyncio pattern; forgetting to is the
standard asyncio bug.

### Q: Your `append()` takes a lock. Walk me through what happens without it.

```
thread A reads next_offset → 41
thread B reads next_offset → 41      ← both got 41
thread A writes record 41, sets next_offset = 42
thread B writes record 41, sets next_offset = 42
```

Two messages now claim offset 41; offset 42 never exists. **Every guarantee in
the system — ordering, replay, deduplication, replication — rests on "an offset
names exactly one message."** The lock is what makes that true.

It matters *because* of `run_in_executor`: the executor is a **pool**, so two
appends genuinely run at the same instant on different threads.

**In this repo:** `test_concurrent_appends_never_duplicate_an_offset` runs 8
threads × 50 messages and asserts the offsets are unique and dense.

**Follow-up trap — "Your `fetch()` takes no lock. Is that a race?"** Almost.
Records are immutable and only appended, so a reader can never see a record
*change*. It can see a line still being flushed, which is handled by skipping
anything that won't parse. That is "probably fine" rather than "provably
correct" — a length prefix plus CRC32 makes it the latter.

### Q: What's wrong with `open(path, 'w')` to save a counter?

`'w'` **truncates to zero bytes, then** writes. In between, the file is empty.
Crash there — microseconds wide, but 3am is long — and you restart from a
zero-byte file. In this project that meant the consumer silently reprocessed
the entire topic from offset 0.

The fix is **write-temp → fsync → atomic rename**:

```python
with open(tmp, 'w') as f:
    f.write(str(offset)); f.flush(); os.fsync(f.fileno())
os.replace(tmp, path)
```

A rename is atomic at the filesystem level: a reader sees the complete old file
or the complete new one, never a partial one.

**Follow-up trap — "Why the fsync before the rename?"** Without it the rename
can reach disk before the *data*, leaving a correctly-named file full of zeros
after a power cut — exactly the failure you were preventing. (And `os.replace`,
not `os.rename`: on Windows, rename fails if the destination exists.)

---

## A6. Performance and measurement

### Q: Tell me about a time you measured something and were wrong.

**Use this one. It is the strongest thing in the project.**

> "Phase 1 added correlation IDs specifically to enable pipelining. I wrote in
> the design doc, before coding, that it would be worth several times the
> throughput. I measured **1.05x**.
>
> So I instrumented the cost breakdown. The loopback round trip was 38 µs;
> a produce cost 253 µs. The other 215 µs was the broker actually working — a
> thread hop to the executor, a file write, a flush. Pipelining removes
> *waiting*, and the broker was never idle, so there was nothing to reclaim.
>
> There was a second reason: my broker handles one request per connection at a
> time, so pipelined requests queue and are still processed serially.
> Pipelining removes the client's waiting, not the broker's serialisation.
>
> The general lesson: **latency-hiding optimisations have no measurable value
> on localhost**, because their benefit is a property of the network. Projected
> from the measured work: 2.9x on a LAN, 190x cross-region. If I had only
> benchmarked locally I'd have concluded the feature was useless and shipped
> something that collapses at 50 ms.
>
> I rewrote the doc around the measurement rather than deleting the
> prediction."

### Q: Why report p99 rather than an average latency?

An average hides the tail. 99 requests at 1 ms and one at 2000 ms averages
21 ms — a number describing no request that actually happened. p99 is what your
angriest user feels. **A change that improves the mean while worsening p99 has
made the system worse, and only percentiles show it.**

### Q: You measured a value of −4.7 µs for network latency. What did you do?

**A negative wait is impossible, so it's a bug in the instrument, not a
discovery.** The estimate was a difference of two similar numbers (sequential
minus pipelined), taken after the broker had accumulated topics — so the
responses had grown larger and noisier than the difference being measured.
Moving that measurement to the start, on an empty broker, gave a stable 38 µs.

> **"Differences of similar quantities are the fragile part of any measurement.
> Give them the quietest conditions you can, and treat an impossible result as
> a broken instrument."**

### Q: Your produce throughput is ~4,000 msg/s. Real Kafka does millions. Why?

Be specific rather than defensive — the gap is entirely explainable:

1. **One message per request.** ~215 µs of broker work per message, paid every
   time. Batching amortises it — the single biggest available win.
2. **JSON bodies.** ~3x the bytes of a binary encoding, and `json.loads` is the
   hot path in every fetch.
3. **O(n) fetch.** Rescans from byte zero; no segments, no index.
4. **Python + GIL.** One core for the event loop.
5. **No zero-copy.** Real Kafka serves consumers with `sendfile`, moving bytes
   from page cache to socket without ever entering user space.
6. **Loopback, one client, one thread.** Nothing here measures concurrency.

> "The gap is design, not mystery. I chose readability over speed deliberately —
> JSON lines mean you can `cat` the log file — and documented what each choice
> costs."

---

## A7. Distributed systems (designed in this project, not yet built)

Answer these as design knowledge, and say so.

### Q: How do consumer groups scale consumption?

Consumers sharing a `group_id` **divide the partitions between them** — each
partition is read by exactly one member. Add a consumer, partitions
redistribute. Different groups each get the full stream.

**Follow-up trap — "What limits consumer parallelism?"** **The partition
count.** 10 partitions means at most 10 useful consumers in a group; an 11th
sits idle. Partitions are the unit of parallelism, which is why you
over-provision them.

### Q: What is a rebalance and why is it disruptive?

When membership changes (a consumer joins, leaves, or misses heartbeats), the
group coordinator reassigns partitions. In the **eager** protocol every
consumer stops, revokes everything, and waits for the new assignment —
stop-the-world. **Cooperative incremental rebalancing** (`CooperativeSticky`)
only moves the partitions that must move.

**Follow-up trap — "A consumer pauses 40s for GC with a 10s session timeout.
What happens?"** It is declared dead and its partitions reassigned. When it
wakes and commits, its **generation ID is stale** and the commit is rejected —
zombie fencing. Meanwhile another consumer has reprocessed from the last
committed offset. This is why `max.poll.interval.ms` exists separately from
`session.timeout.ms`.

### Q: How does replication work, and what does `acks` control?

Each partition has a leader and followers. Followers *fetch* from the leader
(pull, like consumers). The **ISR** (in-sync replicas) is the set caught up
within `replica.lag.time.max.ms`. The **high watermark** is the highest offset
replicated to all ISR members — **consumers may only read up to it**, so they
never see data a failover would erase.

| `acks` | Waits for | Risk |
|---|---|---|
| `0` | nothing | Fire and forget; loss on any failure |
| `1` | leader's log | Leader dies before replication → lost |
| `all` | every ISR member | Slowest replica sets your latency |

**Follow-up trap — "`acks=all` with `replication.factor=3`. Is data safe?"**
**Not necessarily.** If `min.insync.replicas=1` and two replicas fall out of
the ISR, "all" means "the leader alone" — you have `acks=1` while believing you
have three copies. You need `min.insync.replicas=2`, which makes the write
*fail* when the ISR shrinks. That is a deliberate CAP choice: consistency over
availability.

### Q: Where does Kafka sit on CAP?

**It's configurable, which is the real answer.**
- `acks=all` + `min.insync.replicas=2` → **CP**: writes fail during a partition
  rather than risk loss.
- `acks=1` → **AP**: writes succeed and may be lost.
- `unclean.leader.election.enable=true` → availability over correctness:
  promotes an out-of-sync replica, accepting **known data loss** to keep serving.

### Q: What is a controller and what is split-brain?

The controller is the broker that owns partition→leader assignment and reacts
to failures. Older Kafka elected it via ZooKeeper; **KRaft** (production-ready
in 3.3, ZooKeeper removed in 4.0) uses a Raft quorum of controllers instead.

**Split-brain** is two nodes both believing they are in charge — during a
network partition, each side sees the other as dead. Naive election ("lowest
live ID wins") is vulnerable: partition the cluster and you get two
controllers, two leaders for a partition, and divergent logs.

Quorum consensus prevents it: a leader needs a **majority**, and a minority
partition mathematically cannot form one. Plus **fencing** — epoch/term numbers
mean a stale leader's writes are rejected once a new epoch exists.

> Good honest framing: "My design uses lowest-live-ID election because it's the
> clearest teaching path. I documented that it's split-brain-prone under a
> network partition and what Raft adds — quorum, terms, fencing."

### Q: How do you keep a client working when a broker dies?

The `NOT_LEADER_OR_FOLLOWER` error code plus metadata refresh:

1. Client sends to broker 2 (its cached metadata says leader)
2. Broker 2 replies `NOT_LEADER_OR_FOLLOWER` (**retriable**)
3. Client refreshes metadata, learns broker 5 now leads
4. Client retries to broker 5 → success

The application sees a few extra milliseconds. **Metadata plus that one error
code is the entire failover mechanism from the client's side.**

**In this repo:** the code and its exception exist in
[common/errors.py](../common/errors.py) already, precisely so Phase 5 adds a
cluster rather than a protocol change.

### Q: Why classify errors as retriable or not?

Because the only question a client really asks is *should I try again?*

- `NOT_LEADER_OR_FOLLOWER` → **retry**, the message is fine, it knocked on the
  wrong door.
- `UNKNOWN_TOPIC_OR_PARTITION` → **retry** (surprisingly): usually metadata
  hasn't propagated yet.
- `INVALID_REQUEST` → **never retry**, it resends identical bad bytes forever.

Deciding this by matching English text is how you get an outage caused by
someone improving an error message.

**Follow-up trap — "Why is `REQUEST_TIMED_OUT` the nastiest error?"** Because
it tells you **nothing about what happened**. The request may have been lost,
or fully applied with only the response lost. Retrying may therefore
*duplicate* the write — which is why at-least-once is the honest default and
why the idempotent producer exists.

---

# Part B — Questions about *your* project

### Q: Why did you build this?

Have a real answer. A good shape: *"I could describe what Kafka does but not
why any of it was designed that way. Building it phase by phase, with the
trade-offs written down before the code, was the only way I found to actually
understand it — and every phase has a benchmark and a failure demo, so the
claims are testable rather than recited."*

### Q: What was the hardest part?

Candidates, all true:
- **Framing over TCP.** Realising `recv(n)` returns *up to* n, and that code
  which works perfectly on localhost fails on a real network.
- **The commit-ordering bug.** Understanding *why* committing before processing
  is at-most-once, and that the choice cannot be avoided — only made
  deliberately.
- **Being wrong about pipelining**, and having to rewrite a document around a
  measurement that contradicted it.

### Q: What would you do differently?

- **A binary record format with a CRC from day one.** JSON lines are readable
  but ~3x the bytes, `json.loads` is the hot path, and there is no corruption
  detection — a torn write can parse into something wrong rather than failing.
- **Segments from the start.** The O(n) fetch is architectural; retrofitting it
  in Phase 2 touches everything.
- **Benchmark with simulated network latency**, not just loopback. Loopback is
  the one environment where latency optimisations cannot show value.

### Q: What's the biggest weakness of what you've built?

Pick the real one and be direct: *"Fetch is O(n) — it rescans the log from byte
zero on every read, so latency grows with log size. I measured it: ~1 ms at the
start of a 5,000-message log, 13.8 ms at the end. It's the single biggest flaw
and it's what Phase 2 exists to fix."*

Naming your own worst flaw with a number is a strong signal.

### Q: How did you test a networked system?

Integration tests against a **real broker on a real socket** — no mocks. Two
details that make it practical:
- **`port=0`** asks the OS for any free port, so tests never collide with a dev
  broker and the suite can run in parallel.
- **A readiness event**, not `sleep(0.1)`. Sleep-and-hope is the number one
  source of flaky integration tests: too slow on fast machines, too short on
  loaded CI.

*"A mock broker would only prove my mock matches my assumptions — and the bugs
that matter here are exactly the ones in the assumptions."*

### Q: How would you scale this to a million messages per second?

Ordered by impact:
1. **Batching** — many messages per request. Kills the ~215 µs per-message
   broker cost. Biggest single win.
2. **Binary format + compression** — smaller payloads, faster parsing.
3. **Segments + index** — make reads O(log n).
4. **Partitions across brokers** — horizontal write scaling.
5. **Zero-copy reads** (`sendfile`) — page cache straight to socket.
6. **Multiple processes** — get past one core.

---

# Part C — Exercises you might be handed

**Design a rate limiter / a URL shortener / a chat system** — the pub-sub
concepts transfer. Look for: what needs ordering (→ partition key), what
tolerates duplicates (→ at-least-once), what must not be lost (→ `acks=all` +
`min.insync.replicas`).

**"Implement a length-prefixed protocol reader."** The `_recv_exactly` loop.
They are checking you know `recv` returns *up to* n, that `b''` means EOF, and
that you validate the length before allocating.

**"How would you deduplicate messages in an at-least-once system?"** Idempotent
processing: a natural idempotency key (order ID), `INSERT ... ON CONFLICT DO
NOTHING`, or a seen-set with a TTL. Note that dedup state itself needs bounding.

**"Design the metadata request/response for a cluster."** Brokers list, per
topic per partition: leader, replicas, ISR, high watermark, log start offset,
and a **per-partition error code** — because one unavailable partition must not
fail the whole response.

---

# Part D — Answers that sound good and are wrong

| ❌ Wrong | ✅ Right |
|---|---|
| "Kafka is a message queue." | It's a distributed commit log. Reading is non-destructive. |
| "Kafka guarantees exactly-once delivery." | Exactly-once *processing*, within Kafka, via idempotent producer + transactions. Delivery cannot be guaranteed once. |
| "Kafka guarantees message ordering." | **Within a partition.** Never across a topic. |
| "Kafka is fast because it's in memory." | It writes every message to disk. It's fast because the writes are *sequential* and it uses the page cache and zero-copy. |
| "More partitions is always better." | More open files, more memory, slower failover, costlier rebalances — and changing the count breaks key→partition mapping. |
| "`acks=all` means the data is safe." | Only with `min.insync.replicas ≥ 2`. Otherwise "all" can mean "the leader alone". |
| "The consumer tells the broker it's done." | The consumer stores an offset. The broker tracks no per-message delivery state — that's the whole design. |
| "Kafka fsyncs every message." | It relies on replication for durability, not fsync. |
| "ZooKeeper is required." | KRaft replaced it; ZooKeeper was removed in Kafka 4.0. |

---

# Part E — Questions worth asking them

Asking good questions is scored. These come naturally from this project:

- "What delivery semantics do your consumers assume — and is that assumption
  written down anywhere, or is it folklore?"
- "How do you handle consumer lag alerting? What happens when a consumer falls
  behind retention?"
- "What's your `min.insync.replicas`, and has anyone verified it's what you
  think it is?"
- "How do you handle schema evolution on your topics?"
- "Have you had a rebalance storm, and what caused it?"

---

## One-page cheat sheet

| Concept | The one-sentence version |
|---|---|
| Log vs queue | Reading is non-destructive, so the broker stores one integer per consumer instead of per-message delivery state |
| Partition | The unit of ordering, parallelism, replication, and assignment |
| Offset | Broker-assigned, monotonic, immutable — an offset names exactly one message |
| Ordering | Within a partition only; the key is the dial between ordering and scale |
| Sequential I/O | ~1000x faster than random; the reason the log is append-only |
| Segments + sparse index | Makes reads O(log n) with a tiny index and a short sequential scan |
| Compaction | Keep the latest value per key → a changelog you can replay into a table |
| fsync vs flush | Kafka buys durability with replication, not fsync (1000x cheaper) |
| Framing | TCP is a byte stream; length-prefix your messages or lose the boundaries |
| Correlation ID | Lets replies be matched, which lets requests be pipelined |
| API versioning | Per-API, so a ten-broker rolling upgrade doesn't need a flag day |
| At-least-once | Process, *then* commit. Duplicates are curable; loss is not |
| Exactly-once | Delivery: impossible. Processing: idempotency, or Kafka transactions |
| Consumer group | Partitions divided among members; partition count caps parallelism |
| Rebalance | Membership change → reassignment; eager stops the world, cooperative doesn't |
| ISR + high watermark | Consumers read only what's replicated, so failover can't erase what they saw |
| `acks=all` | Only safe with `min.insync.replicas ≥ 2` |
| NOT_LEADER + metadata | Those two things together *are* client-side failover |
| Retriable errors | The only question a client asks is "try again?" — answer it with a number, not prose |
| Split-brain | Prevented by quorum (a minority can't form one) plus epoch fencing |
| p99 not mean | The average describes no request that actually happened |
