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

Preparing to be asked about this? **[docs/interview-questions.md](docs/interview-questions.md)**
maps every concept here to the questions it gets asked as, with the measured
numbers from this repo to answer them with.

Want the wrong turns rather than the clean result?
**[docs/challenges-and-solutions.md](docs/challenges-and-solutions.md)** is the
development journal: fifteen real bugs, wrong predictions, and design dead
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

# Terminal 3 — read one partition back
python -m consumer.consumer --topic orders --partition 0 --group demo

# Look at what happened
python -m tools.kfk topics describe --topic orders   # per-partition spread
python -m tools.kfk segments --topic orders
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
python -m bench.bench --count 20000        # throughput, percentiles, seek profile
pytest -q                                  # the test suite
```

The chaos scripts are the most instructive minute in the repo. The first runs
the same crash twice, moving one line, and shows five messages vanish in one
run and get reprocessed in the other. The second corrupts a log three
different ways — including one that Phase 1 would have served to a consumer as
real data.

---

## Architecture today (end of Phase 0)

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
   offsets/orders-0-demo.txt                     data/orders-0/
   (committed position,                          ├── 00000000000000000000.log
    written atomically)                          ├── 00000000000000000000.index
                                                 └── 00000000000000011640.log  ACTIVE
```

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
| 4 | Consumer groups | Coordinator, rebalance, server-side offsets, delivery semantics | next |
| 5 | Cluster | Multi-broker, membership, controller election, metadata routing | |
| 6 | Replication | Leader/follower, ISR, high watermark, failover — **fault tolerance** | |
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
            net.py        SyncConnection — framed client transport
            config.py     every default in one place
            errors.py     numeric error codes, retriable or not
broker/     broker.py     asyncio TCP server, request routing
            metadata.py   which topics exist and how wide they are
            partition.py  identity + the write lock, over a Log
            log/          record.py  one record: length + CRC32 + JSON
                          index.py   sparse offset → byte position
                          segment.py one .log and its .index
                          log.py     ordered segments, rolling, retention
producer/   producer.py   write client: async send, retries, acks
            partitioner.py murmur2 key routing + sticky batching
            batch.py      accumulator, sender thread, futures
consumer/   consumer.py   read client, offset tracking, commit semantics
tools/      kfk.py        admin CLI
tests/      conftest.py   real broker on an ephemeral port
bench/      bench.py      throughput + latency percentiles
chaos/                    scripts that break things on purpose
docs/       00-overview.md          concepts, before any code
            benchmarks.md           one row per phase
            interview-questions.md  concepts → questions, with our numbers
            phase-0/                foundation + code reading guide
            phase-1/                protocol v2
            phase-2/                storage engine
            phase-3/                partitioning + batching
```

---

## Known limitations at Phase 0

Stated plainly, because a system that only lists its strengths is a sales
pitch:

- **One broker.** Partitions exist, but they all live on one machine — Phase 5.
- **No compression.** Real Kafka compresses a batch as a unit, which is where
  batching's *other* big win comes from. The remaining produce ceiling is
  largely uncompressed JSON.
- **Unbounded producer memory.** A real producer has `buffer.memory` and blocks
  when it is full, applying backpressure. Ours grows until the process dies.
- **No idempotent producer.** `max_in_flight` defaults to 1 because a retried
  batch could otherwise land after a later one and reorder a partition.
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
- **Consumer groups are a filename.** `group_id` only namespaces the local
  offset file; the broker knows nothing about groups. Phase 4.
- **Offsets are local files.** Move a consumer to another machine and it
  starts over. Phase 4.
- **No producer batching.** One message per request, so ~215 µs of broker work
  is paid per message. This — not the network — is what caps produce
  throughput. Phase 3.
- **No version negotiation.** Everything is `api_version=0`; there is no
  `ApiVersions` request, so a client cannot ask what a broker supports.

---

## Requirements

Python 3.9+ (developed on 3.11). `pytest` for the test suite. Nothing else.
