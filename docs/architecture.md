# How This Kafka Is Built

An explanation of the whole system: what each piece does, how a message moves
through it, and why each mechanism exists.

This is the document to read — or to talk from — when the question is *"explain
how you implemented Kafka."* It goes **outside in**: the shape of the system,
then one message end to end, then each subsystem in the order you would need to
understand it.

> Related, and deliberately not duplicated here:
> [flows.md](flows.md) traces all 26 flows as exact call sequences with the
> code that does the work — flows 1–18 for a single broker, 19–26 for the
> cluster. This document is the map; that one is the reference.
> [interview-questions.md](interview-questions.md) turns all of this into
> questions and answers. Each phase doc argues *why* its design was chosen.

---

## 1. The shape of the system

Four processes, one protocol.

```
   ┌──────────────┐                                    ┌──────────────┐
   │  PRODUCER    │                                    │  CONSUMER    │
   │              │                                    │              │
   │ partitioner  │                                    │ assignor     │
   │ batching     │                                    │ heartbeats   │
   │ retries      │                                    │ offset commit│
   └──────┬───────┘                                    └──────┬───────┘
          │  PRODUCE                                   FETCH  │
          │  (to the partition's LEADER)                      │
          └───────────────────┬───────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼─────┐         ┌─────▼────┐         ┌──────▼───┐
   │ BROKER 1 │◄───────►│ BROKER 2 │◄───────►│ BROKER 3 │
   │          │  Raft   │          │  Raft   │          │
   │ leads    │  +      │ leads    │  +      │ leads    │
   │ p0 p3    │ replica │ p1 p4    │ replica │ p2 p5    │
   │          │ fetches │          │ fetches │          │
   └──────────┘         └──────────┘         └──────────┘
```

Every broker runs the same code. There is no separate "coordinator service" or
"metadata service" — a broker is simultaneously a storage node, a group
coordinator, a Raft voter, a controller candidate, and a replica fetcher. That
is homogeneous replication, and it is how Kafka works too.

### The two planes, which is the most important idea here

```
   CONTROL PLANE                        DATA PLANE
   ─────────────────────────────        ─────────────────────────────
   who leads which partition            every message anyone produced
   which topics exist, how wide         gigabytes
   kilobytes, events per HOUR           events per SECOND
                                        
   replicated by RAFT                   replicated by FOLLOWER FETCH
   majority agreement, fsync each       leader + ISR, page cache
```

Two replication mechanisms, deliberately. Running message traffic through Raft
would mean an fsync and a majority round trip **per message** — roughly a 100x
slowdown to solve a problem that has a cheaper answer. This split is the single
thing most worth understanding about the design, and the source of the most
common confusion: *"we have consensus, why isn't my data safe?"*

---

## 2. One message, end to end

Follow `producer.send("order-42", key="user-7")` all the way to a consumer.

### On the producer

**1. Which partition?** `murmur2(key) % num_partitions`
([producer/partitioner.py](../producer/partitioner.py)). Same key → same
partition → ordered, forever. A keyless message goes to a *sticky* partition
that changes when a batch fills, which spreads load without breaking batching.

**2. Which broker?** The client holds a metadata cache mapping
`(topic, partition) → broker`, refreshed on a TTL or on error
([common/cluster_client.py](../common/cluster_client.py)).

**3. Don't send it yet.** The record goes into a per-partition accumulator
([producer/batch.py](../producer/batch.py)). A background sender thread flushes
when the batch hits `batch.size` or `linger.ms` expires. `send()` returns a
`Future` immediately.

> **This is where 12x throughput came from.** One request per message cost
> ~215 µs of broker work each; 100 messages per request amortises it. The
> price is up to `linger_ms` of added latency, which is why it is a setting.

### On the wire

```
┌──────────┬───────────┬─────────┬─────────┬────────────────┬───────────┐
│ magic 4B │ length 4B │ api_key │ api_ver │ correlation_id │ body JSON │
│  0xCAFE  │  of body  │   2B    │   2B    │      4B        │           │
└──────────┴───────────┴─────────┴─────────┴────────────────┴───────────┘
```

A fixed 16-byte header, so the reader always knows how much to read next
([common/protocol.py](../common/protocol.py)). Each field earns its place:

| Field | Earns |
|---|---|
| `magic` | catches a client talking to the wrong port |
| `length` | frames the message — TCP is a byte stream, not messages |
| `api_key` | which request this is |
| `api_version` | rolling upgrades: old and new brokers coexist |
| `correlation_id` | many requests in flight, matched to replies |

`length` is bounded by `MAX_REQUEST_BYTES`, because honouring an untrusted
length field means committing that much memory — a corrupted `0xFFFFFFFF`
would otherwise ask for 4 GB.

### On the broker

**4. Am I the leader for this partition?** If not, return
`NOT_LEADER_OR_FOLLOWER`. The client refreshes its metadata and retries —
**that pair is client-side failover**, with no proxy hop and no redirect.

**5. Enough replicas?** With `acks=all`, refuse now with `NOT_ENOUGH_REPLICAS`
if the ISR is smaller than `min.insync.replicas`. Before the append, not after
— writing first and failing the acknowledgement would leave the record in the
log where recovery would replicate it, so a producer told FAILED would find its
message in the topic anyway.

**6. Append.** Under the partition's lock, because the broker hands writes to a
thread pool and two producers really can call it at the same instant. Without
the lock two messages claim offset 41 and offset 42 never exists — and *every*
guarantee in the system rests on an offset naming exactly one message.

**7. Wait for replication**, if `acks=all`: poll until the high watermark
passes this offset.

### On disk

```
data/orders-0/
├── 00000000000000000000.log     sealed, immutable
├── 00000000000000000000.index   sparse: offset → byte position
└── 00000000000000011640.log     ACTIVE — takes appends
```

Each record is `[length 4B][crc32 4B][JSON payload]`
([broker/log/record.py](../broker/log/record.py)). The CRC is what makes a torn
write *detectable* rather than served as data — the failure mode that matters
is not a record that fails to parse, but one that parses into something wrong:

```
{"offset": 41, "value": "hello world"}    written
{"offset": 41, "value": "hello"}          torn — and still valid JSON
```

### Back out to the consumer

**8. Fetch**, clamped to the **high watermark** — the highest offset every
in-sync replica holds. Records above it exist on the leader and are invisible.

**9. Find the offset without scanning:** binary search the segment base offsets
→ binary search that segment's sparse index → seek → scan at most one index
interval forward. Measured **1.07x** from offset 0 to offset 17,777: flat.

**10. Process, then commit.** In that order — committing first is at-most-once
and silently drops messages on a crash.

---

## 3. The subsystems

Read these in order; each depends on the ones before it.

### 3.1 Storage — `broker/log/`

The layer that makes reads independent of log size.

| File | Lines | Does |
|---|---:|---|
| `record.py` | 173 | one record: length + CRC32 + JSON |
| `index.py` | 219 | sparse offset → byte position |
| `segment.py` | 355 | one `.log` and its `.index` |
| `log.py` | 493 | ordered segments, rolling, retention, recovery |

**The sparse index is the whole idea.** One entry per `index.interval.bytes`
(~0.2% of the log), so lookup is a binary search plus a bounded scan. A dense
index would be O(1) and cost memory proportional to record count — unusable at
a billion records.

One rule is load-bearing: **the index must land at or before the target, never
past it.** The caller scans forward, so landing early costs microseconds while
landing late would silently skip records.

**Recovery** reads only the *last* segment. `iter_records` stops at the first
corrupt record, so a torn tail costs the partial write and nothing before it.

### 3.2 Partitioning and the producer

`Partition` ([broker/partition.py](../broker/partition.py), 189 lines) is
identity plus a lock, over a `Log`. That split matters: the storage format was
rewritten completely in Phase 2 while `broker.py` and its tests did not move a
line. **A storage rewrite that forces edits everywhere was not encapsulated to
begin with.**

The producer ([producer/](../producer/), ~1,070 lines) adds routing, batching,
`acks`, and retries with exponential backoff and jitter.

> The sender sends **one batch at a time per connection**, which is what stops
> a retried batch landing after a later one and reordering a partition. There
> is no `max.in.flight` setting because there is nothing safe to raise it to:
> Kafka allows 5 in flight *only because* the idempotent producer's
> `(producer_id, sequence)` lets a broker reject an out-of-order batch, and
> this clone has no such thing.

### 3.3 Consumer groups — `broker/coordinator.py`

539 lines implementing the protocol that makes consumption scale:

```
JOIN_GROUP    everyone joins; the first member becomes group leader
SYNC_GROUP    the LEADER computes the assignment; others fetch it
HEARTBEAT     liveness; a silent member is evicted
OFFSET_COMMIT committed positions, stored in __consumer_offsets
```

**The assignment is computed by a client, not the broker.** That is Kafka's
real design and it is not an accident: it means a new assignment strategy ships
with the application rather than requiring a broker upgrade.

**Generation fencing** is the mechanism worth naming. Every rebalance
increments a generation number. A consumer that paused for 60 seconds, was
evicted, and then commits arrives with a stale generation and is rejected with
`ILLEGAL_GENERATION`. Without it, a zombie's late commit rewinds the group.

> The same pattern appears three times in this project: **group generation**,
> **Raft term**, and **leader epoch**. All one idea — a monotonic counter that
> makes a stale actor detectable.

### 3.4 The cluster — `broker/cluster.py`, `controller.py`

Brokers read a static `cluster.json` and heartbeat each other in a full mesh —
every broker to every other, so each holds its own liveness opinion. There is
no central health checker.

The **controller** is one broker, and it owns one decision: which broker leads
which partition. It computes a placement, **proposes it to the Raft quorum**,
and applies it only once a majority has committed it.

> That ordering is the fix for split-brain. Phase 5's first version elected
> `min(live_broker_ids)` — ten lines, no messages, and two controllers under a
> network partition, each assigning partitions differently with no error
> anywhere. `chaos/split_brain.py` used to print two controllers; it now prints
> one, with the minority sitting as candidates.

### 3.5 Consensus — `broker/raft/`

| File | Lines | Does |
|---|---:|---|
| `state.py` | 151 | durable `currentTerm` and `votedFor` |
| `log.py` | 462 | the replicated log, the election restriction |
| `node.py` | 845 | follower / candidate / leader, three RPCs |
| `machine.py` | 371 | what a committed entry *means* |
| `snapshot.py` | 116 | compaction: the answer without the journal |

Three RPCs: `RequestVote` (52), `AppendEntries` (53), `InstallSnapshot` (54).
An `AppendEntries` with no entries **is** the heartbeat — replication and
liveness are the same message, so a leader cannot appear alive while failing to
replicate.

Two rules do most of the work:

**`votes_needed = len(voters) // 2 + 1`** — a majority of *all* voters, not of
those reachable. Two disjoint majorities of one set cannot exist, so two
leaders in one term cannot exist. That is arithmetic, not best effort. The
price is availability: the minority side goes quiet even though its machines
are healthy. **Raft chooses consistency.**

**The election restriction** (`is_up_to_date`) — a candidate whose log is
behind cannot win, which is what makes committed entries permanent. Four lines.
Delete them and Raft still elects leaders and still passes every happy-path
test, while quietly losing committed data.

**Snapshots**: a node folds its applied prefix into a snapshot and discards it,
so restart is *snapshot plus tail* rather than a full replay. Compaction
creates a failure that could not previously exist — a follower needing an entry
the leader has thrown away — which is exactly what `InstallSnapshot` is for.
`chaos/lagging_follower.py` shows a broker acquiring 12 topics whose log
entries exist nowhere.

### 3.6 Replication — `broker/replica.py`

For every partition this broker should hold but does not lead, a loop:

```
read my own log end  →  FETCH from the leader  →  append what came back
```

That is all of it. **A follower is a consumer with a `replica_id`** — the same
`FETCH` API, so replication is a thin layer over the read path rather than a
second system.

Three rules make it correct:

**Followers pull, they are not pushed to.** The leader keeps no per-follower
queues; a slow follower costs it one integer. A restarting follower resumes
from its own log, because the log *is* the record of where it got to.

**A follower copies offsets, it never assigns them.** `append_replicated` takes
the leader's offset, skips records at or before its own end (a restart
legitimately re-fetches them), and **raises on a gap** rather than writing a
log with a hole in it.

**The high watermark is what a consumer may see** — the minimum across the
in-sync set. Withholding data the leader already has looks perverse until you
ask what happens without it:

```
leader   0 1 2 3 4 5 6 7 8 9    consumer reads offset 9, acts on it
A        0 1 2 3 4 5 6 7        leader dies
B        0 1 2 3 4 5 6 7 8      B elected, log ends at 8
                                next produce becomes offset 9
```

Offset 9 now names a *different message* from the one a consumer already
processed. That is worse than data loss.

**Consumers are clamped; followers are not**, and the asymmetry is circular:
the act of a follower reading a record is what *makes* it safe. Clamp followers
too and nothing replicates past the watermark, so the watermark never advances,
so nothing is ever readable — a cluster deadlocked at offset 0, looking
perfectly healthy.

**The ISR** is the set still fetching within `replica.lag.time.max.ms` —
measured in *time*, not records behind. Kafka shipped a count threshold and
removed it in 0.9, because a producer burst puts every follower thousands of
records behind and a count rule would eject the whole ISR at exactly the moment
durability matters most.

**Failover promotes from the ISR**, not from the ring. The ring spreads
leadership evenly across a healthy cluster; on a failure it would hand the
partition to whichever broker comes next, which may hold nothing. An in-sync
replica holds everything below the high watermark by definition, so promoting
from the ISR means every record a consumer was allowed to read is already
there. When no in-sync replica is live we **refuse to elect** — the partition
goes unavailable rather than silently losing data, which is
`unclean.leader.election.enable=false` and is a choice rather than an absence
of one.

**The leader epoch** is bumped on every leadership change for that partition,
committed with the placement, handed to clients in `METADATA` and sent back on
produce and fetch. A broker refuses a request carrying a *newer* epoch than its
own — the only way a caller can know a higher number is that leadership moved
and this broker has not heard.

> Note it is **not** the Raft term. The term increments when the *controller*
> changes; the epoch must increment when a *partition's* leader changes, and
> one controller reassigns many partitions across many terms.

---

## 4. What each phase added, and what it cost

| Phase | Added | Measured cost or gain |
|---|---|---|
| 0 | Tests, benchmarks, chaos scripts; 4 bug fixes | baseline 3,982 msg/s |
| 1 | Correlation IDs, API versions, error codes | **no change** — and finding out why was the point |
| 2 | Segments, sparse index, CRC32, retention | consume **6x**, seeks **flat**; produce −6% |
| 3 | Partitioning, batching, `acks`, retries | produce **12x**, costs `linger_ms` latency |
| 4 | Consumer groups, coordinator, fencing | throughput unchanged; **~1 s** rebalance pause |
| 5 | Cluster, Raft, client routing | capacity, not speed; batching gets weaker |
| 6 | Replica sets, fetchers, HW, ISR, `acks=all`, failover | `acks=all` **~6x** slower — and **0 messages lost** across a leader failure |

The Phase 1 row is the most instructive. Pipelining was supposed to raise
throughput and delivered 1.05x, because the broker was never idle — the
loopback round trip was 38 µs of a 253 µs request and the other 215 µs was real
work. Projected on a real network it is 2.9x on a LAN and 190x cross-region.
**Latency-hiding optimisations have no measurable value on localhost**, which
is the one environment where they cannot help.

---

## 5. What is not built

Stated plainly, because a system that only lists its strengths is a sales
pitch.

- **Epoch fencing needs somebody to know the newer number.** A stale leader is
  refused once a refreshed client sends the newer epoch, or once the broker
  learns it was replaced. An *isolated* leader with an *unaware* client can
  still take `acks=1` writes that will be lost — `chaos/kill_leader.py` loses
  201 messages exactly this way. `acks=all` is what protects that case.
- **No idempotent producer**, so `acks=all` is at-least-once — a timeout must
  be retried and the retry can duplicate.
- **No compression, no zero-copy.** Real Kafka serves reads with `sendfile`;
  we decode every record into Python objects and re-serialise. Largest
  remaining performance gap.
- **No compaction on `__consumer_offsets`**, so it grows with commits rather
  than live keys.
- **Eager rebalancing only** — all consumption stops during a rebalance.
- **Static quorum membership.** Voters are fixed at startup; Raft can
  reconfigure through its own log and ours cannot.

---

## 6. The four things worth saying about it

If you take four ideas from this system, take these.

**1. An offset names exactly one message.** That single sentence explains the
partition lock, why followers copy offsets instead of assigning them, why the
high watermark exists, and why leader-epoch fencing is needed.

**2. The control plane and the data plane have different physics.** Metadata is
kilobytes changing hourly; messages are gigabytes changing constantly. One
replication mechanism priced for both would be wrong for the common case.

**3. Every failure detector is the same dilemma.** `session.timeout.ms`, the
broker session timeout, the Raft election timeout, `replica.lag.time.max.ms` —
all asking one question with no right answer: *how long is silence tolerated
before it counts as death?* Short detects fast and turns a GC pause into a
failover; long is stable and leaves failures unnoticed.

**4. A test harness that models failure wrongly will agree with you
enthusiastically.** The most expensive bug here made two healthy brokers of
three unable to keep a leader, and every test passed — because the in-memory
transport modelled a dead peer as *instant silence* when a real one is *slow*.
Four separate places waited on the slowest peer. **Concurrent is not the same
as does not wait.**

---

→ Deeper: [flows.md](flows.md) for exact call sequences ·
[challenges-and-solutions.md](challenges-and-solutions.md) for the thirty
things that went wrong · [interview-questions.md](interview-questions.md) for
all of this as questions.
