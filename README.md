# Kafka Clone — A Distributed Pub-Sub System, Built to Be Understood

A publish-subscribe messaging system in the style of Apache Kafka, written in
Python from scratch, one phase at a time.

The goal is not to produce a Kafka replacement. It is to build every mechanism
that makes Kafka work — the append-only log, partitioning, consumer groups,
cluster membership, leader election, replication — closely enough to explain
*why* each one exists and *what it costs*.

**Everything is hand-written against the Python standard library.** No
message-queue libraries, no frameworks. Importing a solution would hide the
exact mechanism we are trying to see. `pytest` is the only dependency, and it
is only for tests.

---

## Start here

Two ways in, depending on how you like to learn:

- **Concepts first** → **[docs/00-overview.md](docs/00-overview.md)**
  What pub-sub solves, why Kafka is a log rather than a queue, and the six
  words you need.
- **Code first** → **[docs/phase-0/code-reading-guide.md](docs/phase-0/code-reading-guide.md)**
  Which file to open in what order, one message traced byte by byte from
  `send()` to disk, a concept→file→function map, and five things to run
  yourself.

Then follow the phases in order. Each has its own doc.

Want the whole system explained in one pass?
**[docs/architecture.md](docs/architecture.md)** is the "how this Kafka is
built" walkthrough — the shape of the system, one message traced end to end,
then each subsystem in the order you need to understand it. Start here if you
have to *explain* the project to someone.

Preparing to be asked about this? **[docs/interview-questions.md](docs/interview-questions.md)**
maps every concept here to the questions it gets asked as, with the measured
numbers from this repo to answer them with — and opens with a timed 3–4 minute
spoken pitch.

Want to know exactly how a given flow is implemented?
**[docs/flows.md](docs/flows.md)** traces all **26 paths** through the real call
sequence with the code for each step — produce, fetch, rebalance, commit,
retention and recovery for a single broker (1–18), then controller election,
Raft commits, placement, snapshots, client routing and partition failover for
the cluster (19–26).

Want the wrong turns rather than the clean result?
**[docs/challenges-and-solutions.md](docs/challenges-and-solutions.md)** is the
development journal: thirty-one real bugs, wrong predictions, and design dead
ends — what broke, how it was caught, and what fixed it.

---

## Quick start

```bash
pip install -r requirements.txt

# Terminal 1 — the broker
python -m broker.broker

# Terminal 2 — create a 4-partition topic and produce to it
python -m tools.kfk topics create --topic orders --partitions 4
python -m producer.producer --topic orders --count 2000

# Terminal 3 — join a consumer group (run several; they divide the partitions)
python -m consumer.consumer --topic orders --group billing --loop

# Look at what happened
python -m tools.kfk topics describe --topic orders   # per-partition spread
python -m tools.kfk segments --topic orders
python -m tools.kfk groups describe --group billing   # assignment + lag
python -m tools.kfk tail     --topic orders --count 5
```

> Run modules with `python -m`, from the repository root — not
> `python broker/broker.py`. `-m` puts the repo root on the import path so
> `common`, `broker`, `producer` and `consumer` all resolve as packages.

### See a failure mode, and measure the system

```bash
python -m chaos.kill_consumer_midprocess   # watch at-most-once lose data
python -m chaos.corrupt_log                # watch CRCs catch three kinds of damage
python -m chaos.partition_balance          # watch load balancing and its cost
python -m chaos.kill_consumer              # kill a consumer, watch failover + fencing
python -m chaos.kill_controller            # kill the controller, watch re-election
python -m chaos.split_brain                # partition the network, count the controllers
python -m chaos.lagging_follower           # compact past a follower, watch InstallSnapshot rescue it
python -m chaos.kill_leader                # kill a leader mid-produce: acks=all loses 0, acks=1 loses 201
python -m bench.bench --count 20000        # throughput, percentiles, seek profile
pytest -q                                  # the test suite
```

The chaos scripts are the most instructive minute in the repo. The first runs
the same crash twice, moving one line, and shows five messages vanish in one
run and get reprocessed in the other. The second corrupts a log three
different ways — including one that Phase 1 would have served to a consumer as
real data. The fourth kills a group member and shows its partitions taken over
in ~1 s, then shows the "dead" consumer coming back and being refused.

---

## Architecture today (end of Phase 6)

```
   ┌──────────┐                                    ┌──────────┐
   │ Producer │──── PRODUCE {topic,value,key} ────►│          │
   └──────────┘                                    │          │
                                                   │  BROKER  │
   ┌──────────┐                                    │  :9092   │
   │ Consumer │──── FETCH {topic,offset,max} ─────►│ (asyncio)│
   │          │◄─── messages + high_watermark ─────│          │
   └────┬─────┘                                    └────┬─────┘
        │                                               │
  __consumer_offsets                             data/orders-0/
  (committed positions, on the BROKER)           ├── 00000000000000000000.log
  keyed by group|topic|partition                 ├── 00000000000000000000.index
                                                 └── 00000000000000011640.log  ACTIVE
```

Several brokers form a cluster from a static `cluster.json`. They heartbeat
each other, and a **Raft quorum** elects one of them as the **controller**.
The controller assigns each partition to a broker — but proposes that decision
to the quorum and applies it only once a majority has committed it, then pushes
the committed map to its peers. A broker refuses a
partition it does not lead with `NOT_LEADER_OR_FOLLOWER`, and the client
refreshes its metadata and retries — that pair *is* client-side failover.

Consumers sharing a `group_id` form a group: the coordinator divides the
topic's partitions among them, one member each, and reassigns when a member
stops heartbeating. A generation number fences a member that was evicted while
paused, so its late commit cannot rewind the group.

Each partition is **copied to several brokers**. One leads and takes every read
and write; the others run a fetch loop against it, appending the leader's
records with the leader's offsets — a follower is a consumer with a
`replica_id`. A consumer may only read up to the **high watermark**, the
highest offset every in-sync replica holds, so no consumer can act on a record
that a failover could erase. A replica that stops fetching leaves the **ISR**
so it cannot freeze that watermark, and `acks=all` with `min.insync.replicas`
refuses a write outright when too few copies remain — availability given up for
durability, on purpose.

A partition is a **directory of segments**, each named by the offset of its
first record. One segment is active and takes appends; the rest are sealed and
immutable. Beside each is a sparse index (offset → byte position, ~0.2% of the
log) that makes a read cost the same whether the log holds a thousand records
or a billion. Every record carries a length prefix and a CRC32, so a torn write
is detected and truncated rather than served as data.

Wire format — a fixed 16-byte header so the reader always knows how much to
read next, then a JSON body:

```
┌──────────┬───────────┬─────────┬─────────┬────────────────┬───────────┐
│ magic 4B │ length 4B │ api_key │ api_ver │ correlation_id │ body JSON │
│  0xCAFE  │  of body  │   2B    │   2B    │      4B        │           │
└──────────┴───────────┴─────────┴─────────┴────────────────┴───────────┘
```

`correlation_id` lets a client have many requests in flight and match the
replies; `api_version` is what makes a rolling upgrade possible. Every
response carries a numeric `error_code`, classified retriable or not, so
clients decide what to retry without parsing English.

---

## Roadmap

| Phase | Title | Adds | Status |
|---|---|---|---|
| **0** | [Foundation](docs/phase-0/phase-0-foundation.md) | Tests, benchmarks, chaos scripts, admin CLI; four bug fixes | ✅ done |
| **1** | [Protocol v2](docs/phase-1/phase-1-protocol.md) | Correlation IDs, API keys + versions, numeric error codes, `LIST_OFFSETS`, `METADATA` | ✅ done |
| **2** | [Storage engine](docs/phase-2/phase-2-storage.md) | Segments, sparse index, CRC32, retention, truncation | ✅ done |
| **3** | [Partitioning](docs/phase-3/phase-3-partitioning.md) | Key hashing, batching, `acks`, retries, PRODUCE v1 — **load balancing** | ✅ done |
| **4** | [Consumer groups](docs/phase-4/phase-4-consumer-groups.md) | Coordinator, rebalancing, generation fencing, server-side offsets | ✅ done |
| **5** | [Cluster](docs/phase-5/phase-5-cluster.md) | Multi-broker, membership, **Raft** controller election, client routing | ✅ done |
| **6** | [Replication](docs/phase-6/phase-6-replication.md) | Leader/follower, ISR, high watermark, `acks=all`, failover — **fault tolerance** | ✅ done |
| 7 | Capstone | Full demo, benchmark table, honest limitations | |

Every phase is runnable on its own. Check out any phase's final commit and the
system works.

### What each phase's doc contains

1. **The problem** — what breaks in the previous phase
2. **How real Kafka solves it** — and why that way
3. **Options + trade-offs** — the alternatives, and what each costs
4. **The decision** — what we built and why
5. **Code walkthrough** — the actual files and functions
6. **How real Kafka differs** — the simplifications we took
7. **Exercises** — things to try that make it stick

---

## Layout

```
common/     protocol.py   wire format: encode/decode
            cluster_config.py  the static broker roster
            cluster_client.py  client routing, metadata cache, failover
            net.py        SyncConnection — framed client transport
            config.py     every default in one place
            errors.py     numeric error codes, retriable or not
broker/     broker.py     asyncio TCP server, request routing
            cluster.py    membership, liveness, the local leader map
            controller.py placement, proposed through Raft
            replica.py    follower fetch loops: a follower is just a consumer
            raft/         state.py    durable currentTerm and votedFor
                          log.py      the replicated log, election restriction
                          node.py     follower / candidate / leader
                          machine.py  what a committed entry means
                          snapshot.py compaction: the answer without the journal
            metadata.py   which topics exist and how wide they are
            coordinator.py group membership, generations, liveness
            offset_store.py __consumer_offsets
            partition.py  identity + the write lock, over a Log
            log/          record.py  one record: length + CRC32 + JSON
                          index.py   sparse offset → byte position
                          segment.py one .log and its .index
                          log.py     ordered segments, rolling, retention
producer/   producer.py   write client: async send, retries, acks
            partitioner.py murmur2 key routing + sticky batching
            batch.py      accumulator, sender thread, futures
consumer/   consumer.py   read client: subscribe/assign, heartbeats, commits
            assignor.py   range and round-robin partition assignment
tools/      kfk.py        admin CLI
tests/      conftest.py   real broker on an ephemeral port
bench/      bench.py      throughput + latency percentiles
chaos/                    scripts that break things on purpose
docs/       00-overview.md          concepts, before any code
            architecture.md         the whole system explained in one pass
            benchmarks.md           one row per phase
            flows.md                every code path, traced end to end
            interview-questions.md  concepts → questions, with our numbers
            phase-0/                foundation + code reading guide
            phase-1/                protocol v2
            phase-2/                storage engine
            phase-3/                partitioning + batching
            phase-4/                consumer groups
            phase-5/                the cluster + Raft snapshots
            phase-6/                replication
```

---

## Known limitations at Phase 6

Stated plainly, because a system that only lists its strengths is a sales
pitch:

- **Epoch fencing needs somebody to know the newer number.** A stale leader is
  refused when a client that has refreshed its metadata sends the newer epoch,
  or once the broker itself learns it was replaced. An *isolated* leader with
  an *unaware* client can still accept `acks=1` writes that will be lost —
  `chaos/kill_leader.py` loses 201 messages exactly this way. What protects
  that case is `acks=all`, which an isolated leader cannot satisfy. The
  alternative would be a leader that stops serving when it cannot reach its own
  ISR; Kafka does not do this either.
- **No idempotent producer.** So `acks=all` is at-least-once: a producer that
  times out must retry, and the retry can duplicate. Kafka's
  `(producer_id, sequence)` dedup is what closes this.
- **No leader epoch fencing.** Raft fixed the *control* plane: a minority can
  no longer elect a controller, so two brokers cannot both decide who leads a
  partition. It did not fix the *data* plane — a stale broker will still accept
  a write for a partition it no longer leads. Phase 6 adds the fencing, using
  the Raft term as the epoch. `chaos/split_brain.py` shows exactly where the
  guarantee stops.
- **Snapshots are unchunked and never fire by default.** The Raft log is
  compacted, restart replays snapshot-plus-tail, and a follower that falls off
  the back is caught up with `InstallSnapshot`
  ([Step A](docs/phase-5/raft-snapshots.md) ·
  [Step B](docs/phase-5/raft-install-snapshot.md)). Two honest gaps: a snapshot
  is sent in one message, which caps the cluster at about 190,000 partitions
  (measured); and the threshold is set far above anything this cluster reaches,
  so `chaos/lagging_follower.py` is the only thing that runs the mechanism
  outside its unit tests.
- **The quorum is fixed at startup.** Raft can reconfigure its voter set
  through its own log (joint consensus); ours cannot.
- **No compression.** Real Kafka compresses a batch as a unit, which is where
  batching's *other* big win comes from. The remaining produce ceiling is
  largely uncompressed JSON.
- **Unbounded producer memory.** A real producer has `buffer.memory` and blocks
  when it is full, applying backpressure. Ours grows until the process dies.
- **No idempotent producer, so only one batch is in flight at a time.** A
  retried batch could otherwise land after a later one and reorder a partition.
  Kafka permits 5 in flight *because* `(producer_id, sequence)` lets a broker
  reject an out-of-order batch; without that there is nothing safe to raise it
  to, so the pipelining Phase 1's correlation IDs made possible is unused on
  the produce path.
- **Partition counts are immutable.** Stricter than Kafka, which permits
  increases. See `chaos/partition_balance.py` for why changing them is costly.
- **No compaction.** Retention deletes old segments; it cannot keep the latest
  value per key. Phase 4's `__consumer_offsets` will need it.
- **No fsync.** Writes reach the OS page cache, not the platter. A process
  kill loses nothing; a power cut loses the tail. Kafka's answer is
  replication, which arrives in Phase 6. (`fsync_messages` / `fsync_ms` let you
  turn it on and measure what it costs.)
- **No zero-copy reads.** Real Kafka serves consumers with `sendfile`, moving
  bytes from page cache to socket without entering user space. We decode every
  record into Python objects and re-serialise them — the largest remaining
  performance gap.
- **No compaction on `__consumer_offsets`.** It grows with the number of
  commits rather than the number of live keys, and every record is replayed at
  broker startup.
- **No `max.poll.interval.ms`.** A consumer wedged inside processing keeps
  heartbeating and holds its partitions.
- **Eager rebalancing only.** All consumption stops while a rebalance runs.
- **No producer batching.** One message per request, so ~215 µs of broker work
  is paid per message. This — not the network — is what caps produce
  throughput. Phase 3.
- **No version negotiation.** Everything is `api_version=0`; there is no
  `ApiVersions` request, so a client cannot ask what a broker supports.

---

## Requirements

Python 3.9+ (developed on 3.11). `pytest` for the test suite. Nothing else.
