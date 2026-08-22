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
- **Code first** → **[docs/code-reading-guide.md](docs/code-reading-guide.md)**
  Which file to open in what order, one message traced byte by byte from
  `send()` to disk, a concept→file→function map, and five things to run
  yourself.

Then follow the phases in order. Each has its own doc.

---

## Quick start

```bash
pip install -r requirements.txt

# Terminal 1 — the broker
python -m broker.broker

# Terminal 2 — send some messages
python -m producer.producer --topic orders --count 20

# Terminal 3 — read them back
python -m consumer.consumer --topic orders --group demo

# Look at what happened
python -m tools.kfk describe --topic orders
python -m tools.kfk tail     --topic orders --count 5
```

> Run modules with `python -m`, from the repository root — not
> `python broker/broker.py`. `-m` puts the repo root on the import path so
> `common`, `broker`, `producer` and `consumer` all resolve as packages.

### See a failure mode, and measure the system

```bash
python -m chaos.kill_consumer_midprocess   # watch at-most-once lose data
python -m bench.bench --count 5000         # throughput + latency percentiles
pytest -q                                  # the test suite
```

The chaos script is the most instructive 30 seconds in the repo: it runs the
same crash twice, moving one line, and shows five messages vanish in one run
and get reprocessed in the other.

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
   offsets/orders-0-demo.txt                     data/orders-0.log
   (committed position,                          (append-only JSON lines,
    written atomically)                           one file per partition)
```

Wire format — a fixed 12-byte header so the reader always knows how much to
read next, then a JSON body:

```
┌────────────┬────────────┬────────────┬──────────────────┐
│  magic 4B  │ length 4B  │  type 4B   │   body (JSON)    │
│   0xCAFE   │            │            │                  │
└────────────┴────────────┴────────────┴──────────────────┘
```

---

## Roadmap

| Phase | Title | Adds | Status |
|---|---|---|---|
| **0** | [Foundation](docs/phase-0-foundation.md) | Tests, benchmarks, chaos scripts, admin CLI; four bug fixes | ✅ done |
| 1 | Protocol v2 | Correlation IDs, API keys + versions, numeric error codes | next |
| 2 | Storage engine | Segments, sparse index, CRC32, retention, flush policy | |
| 3 | Partitioning | Key hashing, batching, `acks`, retries — **load balancing** | |
| 4 | Consumer groups | Coordinator, rebalance, server-side offsets, delivery semantics | |
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
broker/     broker.py     asyncio TCP server, request routing
            partition.py  the append-only log
producer/   producer.py   write client
consumer/   consumer.py   read client, offset tracking, commit semantics
tools/      kfk.py        admin CLI
tests/      conftest.py   real broker on an ephemeral port
bench/      bench.py      throughput + latency percentiles
chaos/                    scripts that break things on purpose
docs/                     one file per phase, plus benchmarks
```

---

## Known limitations at Phase 0

Stated plainly, because a system that only lists its strengths is a sales
pitch:

- **One partition per topic, one broker.** No distribution yet — Phases 3 and 5.
- **`fetch` is O(n).** Every read scans the log from byte zero, so latency
  grows with log size. See the numbers in [benchmarks](docs/benchmarks.md).
  Phase 2 fixes it.
- **The log grows forever.** No retention or compaction until Phase 2.
- **No fsync.** Writes reach the OS page cache, not the platter. A process
  kill loses nothing; a power cut loses the tail. Kafka's answer is
  replication, which arrives in Phase 6.
- **Consumer groups are a filename.** `group_id` only namespaces the local
  offset file; the broker knows nothing about groups. Phase 4.
- **Offsets are local files.** Move a consumer to another machine and it
  starts over. Phase 4.
- **No error codes.** Failures are strings in a JSON field, so clients cannot
  decide programmatically what to retry. Phase 1.
- **One request in flight per connection.** No pipelining until the protocol
  carries a correlation ID. Phase 1.

---

## Requirements

Python 3.9+ (developed on 3.11). `pytest` for the test suite. Nothing else.
