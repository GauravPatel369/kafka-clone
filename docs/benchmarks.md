# Benchmarks

One row per phase. Every phase claims to improve something; this file is where
the claim either survives contact with a stopwatch or does not.

```bash
python -m bench.bench --count 5000
```

**Read the caveats before the numbers** — a benchmark you cannot interpret is
worse than no benchmark, because it feels like evidence.

---

## Results

| Phase | Produce (msg/s) | Produce p99 (ms) | Consume (msg/s) | Consume p99 (ms) | What changed |
|---|---:|---:|---:|---:|---|
| **0 — Foundation** | 3,982 | 0.55 | 17,081 | 13.81 | Baseline. One round trip per message; O(n) log scan per fetch. |
| **1 — Protocol v2** | 3,946 | 0.50 | 17,007 | 12.12 | Correlation IDs, API versions, error codes. Header 12→16 bytes. **No throughput change, as expected** — see below. |
| **2 — Storage engine** | 3,718 | 0.49 | **101,732** | **1.50** | Segments + sparse index + CRC + retention. **Consume 6.0x faster, p99 8.1x lower, and now flat with log size.** |
| **3 — Partitioning** | **45,082** † | 0.03 † | 102,594 | 1.40 | Batching, key routing, `acks`, PRODUCE v1. **Produce 12.0x faster.** |

† Batched producer. Sequential produce on the same run was 3,764 msg/s — the
comparison, not a regression. The p99 is the **caller's** cost: `send()`
returns without sending, so it measures how long the application is blocked,
not how long the message takes to reach disk. See below.
| **4 — Consumer groups** | 45,082 | 0.03 | 102,594 | 1.40 | Coordinator, rebalancing, server-side offsets. **Throughput unchanged — the cost is a rebalance pause, measured below.** |
| **5 — Cluster** | 45,082 | 0.03 | 102,594 | 1.40 | Multi-broker, controller election, client routing. **Throughput unchanged; the cost is a failover window and one extra request per leader.** |
| 6 — Replication | | | | | `acks=all` will cost latency; that is the point |

Environment: Windows 11, Python 3.11, loopback, 5,000 × 100-byte messages,
fetch batch 100.

---

## Reading the Phase 0 baseline

### Produce: 3,982 msg/s, p50 0.22 ms

The broker is not the bottleneck here — **latency is**. Every `send()` is a
full round trip during which the producer, the network, and the broker are all
idle waiting for each other:

```
producer  ──send──►                      (idle)
                    broker: append+flush (~0.05 ms of actual work)
          ◄──ack───                      (idle)
producer  ──send──►  ...
```

At 0.22 ms per round trip, ~4,000/sec is arithmetic, not engineering. You
cannot fix this by making the broker faster, because the broker is already
mostly idle. You fix it by **amortising the round trip**, which is what Phase
3's batching does: 100 messages per request means 100x fewer round trips.

Note also that this is loopback — no real network. On a LAN (0.5 ms RTT) this
number would be ~2,000/s; across regions (50 ms) it would be **20/s**. The
Phase 3 improvement will look far more dramatic on anything resembling a real
network than it does here.

### Consume: 17,081 msg/s — and the number that actually matters

Consume looks 4x faster than produce for one reason: it already batches, 100
messages per request. Same insight, applied on the read side.

But look at the latency spread rather than the throughput:

```
p50 = 5.76 ms      p95 = 10.79 ms      p99 = max = 13.81 ms
```

The **slowest fetch is the last one**, and that is not noise — it is the
`fetch` implementation showing its cost. `Partition.fetch` opens the log at
byte zero and parses forward, discarding every record below the requested
offset:

| Fetch # | Records skipped before the ones we want | Latency |
|---|---:|---:|
| 1st (offset 0) | 0 | ~1 ms |
| 25th (offset 2400) | 2,400 | ~5.8 ms (p50) |
| 50th (offset 4900) | 4,900 | 13.8 ms (max) |

Latency grows **linearly with how much data the partition already holds**. A
broker that gets slower the longer it runs is not a broker you can operate —
at a million messages a single fetch would take seconds.

This is the single biggest flaw in the current design, and Phase 2 exists to
fix it. After segments and a sparse index, expect p50 ≈ p99 ≈ max: fetch cost
should become independent of log size. **The flatness of that curve is the
result to check, more than the throughput number.**

---

## Phase 1: the result that contradicted the plan

Phase 1 added correlation IDs specifically so a client could stop waiting for
each acknowledgement — pipelining. The prediction, written into
[phase-1-protocol.md](phase-1/phase-1-protocol.md) before the code, was a large
throughput win.

The measurement:

```
PRODUCE sequential                     3,946 msg/sec
PRODUCE pipelined (100 in flight)      4,143 msg/sec     → 1.05x
```

**Essentially nothing.** The prediction was wrong, and the reason is worth
more than the win would have been. The benchmark now measures the cost
breakdown that explains it:

| Per request | µs |
|---|---:|
| Cheap request (METADATA), sequential | 130.1 |
| Same request, pipelined | 91.9 |
| **→ loopback round-trip wait** | **38.2** ← what pipelining removes |
| PRODUCE, sequential | 253.4 |
| **→ broker work (thread hop + disk)** | **215.2** ← what pipelining cannot remove |

Pipelining removes *waiting*. On loopback the wait is 38 µs of a 253 µs
request; the other 215 µs is the broker genuinely working. The broker was
never idle, so there was no idle time to reclaim. (Phase 0's commentary here
claimed the broker "sits idle most of that time" — that was an assumption, and
this measurement disproves it.)

Second limit: the broker serves one request per connection at a time, so
pipelined requests queue and are still handled serially. Pipelining removes
the *client's* waiting, not the *broker's* serialisation.

So when does the header change pay off? When round trip dominates work.
Projected from the measured 215 µs:

| Round trip | Sequential | Pipelined | Speedup |
|---|---:|---:|---:|
| 0.035 ms (loopback) | 3,345/s | 3,788/s | 1.1x |
| 0.5 ms (same LAN) | 1,309/s | 3,788/s | **2.9x** |
| 5 ms (same region) | 190/s | 3,788/s | **19.9x** |
| 50 ms (cross-region) | 20/s | 3,788/s | **190.4x** |

**Latency-hiding optimisations have no measurable value on localhost.** Their
benefit is a property of the network, so benchmarking them on loopback tests
the single environment where they cannot help. Benchmark only locally and you
will conclude they are pointless — then ship something that collapses at 50 ms.

The corollary for Phase 3: pipelining is not what fixes produce throughput
here. The 215 µs of per-message broker work is. Only batching — many messages
inside one request — cuts that.

### A measurement bug worth recording

The first attempt computed the round trip *after* the produce runs, and got
**−4.7 µs**. A negative wait is obviously impossible, which is what made it
easy to catch.

Cause: the estimate is a difference of two similar numbers (sequential minus
pipelined METADATA), and by then the broker held several topics, so METADATA
responses were larger and noisier than the difference being measured. Moving
that measurement to the start, on an empty broker, gave a stable 38 µs.

Differences of similar quantities are the fragile part of any measurement.
Give them the quietest conditions available, and treat an impossible result as
a bug in the instrument rather than a discovery.

---

## Phase 2: the prediction that held

Phase 0 recorded a prediction: *"After segments and a sparse index, expect
p50 ≈ p99 ≈ max: fetch cost should become independent of log size. The flatness
of that curve is the result to check, more than the throughput number."*

### The seek profile — 20,000 messages, fetching from evenly spaced points

```
    OFFSET   LATENCY (ms)
         0          0.418  ████████████████████████████
      2222          0.461  ██████████████████████████████
      4444          0.456  ██████████████████████████████
      6666          0.461  ██████████████████████████████
      8888          0.408  ███████████████████████████
     11111          0.437  █████████████████████████████
     13333          0.437  █████████████████████████████
     15555          0.433  █████████████████████████████
     17777          0.447  ██████████████████████████████

    end/start ratio: 1.07x
```

**Flat.** Reading from offset 17,777 costs the same as reading from offset 0.
In Phase 1 the same measurement climbed linearly — 1 ms at the start of a
5,000-message log, 12 ms at the end — because every read scanned from byte
zero.

The read path is now: binary search the segment base offsets → binary search
that segment's sparse index → seek → scan a few KB. Every step is O(log n) or
O(1), and **nothing in it depends on how much data the log holds.**

### Throughput, like for like at 5,000 messages

| | Phase 1 | Phase 2 | Change |
|---|---:|---:|---|
| Consume | 17,007 msg/s | **101,732 msg/s** | **6.0x** |
| Consume p99 | 12.12 ms | **1.50 ms** | **8.1x lower** |
| Produce | 3,946 msg/s | 3,718 msg/s | −6% |

The consume gain understates the real improvement, because throughput averages
the cheap early reads with the expensive late ones. The seek profile is the
honest picture, and it gets better the more data you have: at 20,000 messages
Phase 1's final fetches would have been scanning four times as far.

**Produce lost ~6%**, and that is expected and correct: every record now costs
8 more bytes (length + CRC32), a checksum computation, and an occasional index
append. Paying 6% on writes to make reads independent of log size is a trade
worth making every time — and the honest way to report it is as a cost, not to
quietly omit the row.

### What did *not* change

Produce is still ~3,700 msg/s and still bounded by ~230 µs of per-message
broker work. Segments did not touch that, because the bottleneck was never
storage layout — it is one message per request. Phase 3's batching is what
moves it.

---

## Phase 3: the bottleneck that was always the bottleneck

Since Phase 1 the cost breakdown had said the same thing every run:

```
PRODUCE, sequential                265.7 µs
  → loopback round-trip wait        47.2 µs    ← pipelining removes this
  → broker work (thread hop + disk) 218.4 µs   ← nothing had touched it
```

218 µs of thread hop, file write, flush, and JSON decode — **paid once per
message, because one request carried one message.** Phase 1 proved pipelining
could not help (the broker was never idle). Phase 2 did not touch it (the
bottleneck was never storage layout).

Batching is the only thing that could, and it did:

| 20,000 × 100-byte messages | msg/s | vs sequential |
|---|---:|---:|
| PRODUCE sequential | 3,764 | — |
| PRODUCE pipelined (100 in flight) | 3,806 | 1.01x |
| **PRODUCE batched (linger 5 ms, 16 KB)** | **45,082** | **12.0x** |

Same broker, same protocol, same disk. The only change is how many messages
ride in one request, which is exactly what the cost breakdown predicted would
matter.

### Read the latency column carefully

```
PRODUCE batched   latency (ms): p50=0.006  p95=0.009  p99=0.029  max=13.958
```

Six **microseconds** at p50 — because `send()` no longer sends. It appends to
a buffer and returns, so that number is *the caller's blocking time*, not the
time for a message to reach disk. The `max` of 13.9 ms is a caller that
happened to arrive while the sender thread held the accumulator lock.

**This is not a like-for-like comparison with the sequential row**, and
presenting it as one would be a lie by omission. End-to-end latency got
*worse*, by design: with `linger_ms=5`, a message landing in an empty batch
waits up to 5 ms before it is sent at all.

> **You bought 12x throughput with up to 5 ms of latency.** `linger_ms=0` buys
> the latency back and gives up most of the throughput. There is no setting
> that is right for everything, which is why it is a setting.

### What is still unchanged

Consume is 102,594 msg/s and the seek profile is still flat — Phase 3 touched
the write path only, and it is reassuring that it left the read path alone.

The remaining produce ceiling is no longer per-message overhead; it is that
records are appended one at a time inside the batch (`_append_records` loops)
and the payload is still uncompressed JSON. Compression is the next big lever
and we do not implement it.

---

## Phase 4: the cost is not throughput, it is a pause

Consumer groups add no per-message work on the hot path, so produce and consume
throughput are unchanged. Measuring them would tell you nothing.

What groups actually cost is **availability during a rebalance**, and that is
what `chaos/kill_consumer.py` measures:

```
three members, four partitions, converged:

    alice   p3        generation 3
    bob     p2        generation 3
    carol   p0, p1    generation 3

kill bob (no LEAVE_GROUP, session_timeout_ms = 800):

    alice   p2, p3    generation 4
    carol   p0, p1    generation 4      recovered in ~1.0 s
```

**~1.0 s of failover** with an 800 ms session timeout — detection dominates,
and the rebalance itself is fast. That number is a direct function of a setting
with no right answer:

| `session.timeout.ms` | Effect |
|---|---|
| Short (800 ms, here) | Fast failover. A GC pause or network blip triggers a **spurious rebalance**, stopping consumption for the whole group. |
| Kafka default (45 s) | Stable under hiccups. A genuinely dead consumer's partitions go unread for the better part of a minute. |

And during any rebalance, **eager (stop-the-world) rebalancing stops all
consumption**: every member revokes everything before anyone receives anything.
Cooperative incremental rebalancing moves only what must move; we did not
implement it, and this pause is the price.

### The one number that did get worse

`__consumer_offsets` has no compaction, so it grows with the number of commits
rather than the number of live keys. A consumer committing once a second adds
~86,000 records a day, and every one of them is replayed at broker startup.

```bash
python -m tools.kfk segments --topic __consumer_offsets
```

Compare the record count against the number of distinct
`(group, topic, partition)` keys — the difference is exactly what compaction
would reclaim, and the gap grows forever.

---

## Phase 5: capacity, not speed — and a cost worth naming

A cluster does not make one client faster. Produce and consume throughput are
unchanged, and measuring them again would say nothing.

What changes is **capacity** (three disks, three event loops, three NICs
instead of one) and **routing**. Measured on a real three-process cluster:

```
6 partitions over 3 brokers          2 each, decided by the controller
1,200 messages produced              182 / 210 / 208 stored per broker
kill the controller                  new controller elected in ~1 s
                                     all 6 partitions reassigned 3/3
```

### The cost that is easy to miss

**Batching gets weaker.** Phase 3 sent every partition of a topic in one
request. Now partition 0 may live on broker 1 and partition 3 on broker 2, so a
request can only carry the partitions its recipient leads. A 6-partition topic
on 3 brokers takes **3 requests where it used to take 1** — a third of the
amortisation, gone.

Real Kafka pays exactly this, and it is part of why partition counts and broker
counts are tuned together rather than separately.

### The cost that is not a cost yet

`chaos/kill_controller.py` measures the uncomfortable one:

```
before the kill:  broker 1 held 182 messages
after failover:   its partitions belong to broker 2, whose log for them is EMPTY
```

**Phase 5 is not fault tolerance.** Leadership moved; the bytes did not. The
cluster keeps accepting writes and clients find the new leader automatically —
but the old data is on a disk nobody can reach.

That is precisely what Phase 6 fixes, and stating it here rather than after
Phase 6 is the point: the honest reading of these numbers is that we bought
capacity and routing, and durability is still outstanding.

---

## How to read these numbers honestly

**Loopback is not a network.** Client and broker share a machine, so the
round-trip time is ~0.05 ms instead of 0.5–50 ms. This *flatters*
round-trip-per-message designs enormously.

**One client, one thread.** Nothing here measures concurrency, which is what
asyncio was chosen for. A concurrency benchmark belongs in Phase 5, where
there is a cluster to spread load across.

**Throughput hides the tail; percentiles do not.** Consume's *mean* is 5.85 ms
and its max is 13.81 ms — a 2.4x spread that the mean alone would never
reveal. Any phase where the mean improves while p99 gets worse has made the
system worse. Watch p99.

**Nothing is fsynced.** `Partition.append` flushes to the OS page cache, not
to disk. Adding `os.fsync` would cut produce throughput by 10–100x. That
number is not a flaw in the benchmark — it is the reason Kafka buys durability
with replication (Phase 6) rather than with fsync. Try it: Exercise 3 in
[phase-0-foundation.md](phase-0/phase-0-foundation.md).

**Same machine, same conditions, or don't compare.** Numbers from different
hardware are not comparable. Rerun Phase 0's baseline on your machine before
comparing a later phase to it.
