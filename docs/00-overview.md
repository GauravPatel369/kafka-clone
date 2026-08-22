# 00 — Overview: What We Are Building and Why

> Read this before anything else. It explains the mental model that every later
> phase assumes.

---

## 1. The problem publish-subscribe solves

Imagine an e-commerce system. When an order is placed, five different things must
happen: charge the card, reserve inventory, email the customer, update analytics,
and notify the warehouse.

The naive design is **direct calls**:

```
                 ┌──► Payment Service
                 ├──► Inventory Service
  Order Service ─┼──► Email Service
                 ├──► Analytics Service
                 └──► Warehouse Service
```

This looks fine until you ask hard questions:

| Question | What goes wrong |
|---|---|
| Email service is down. Now what? | The order fails, or Order Service must buffer and retry — it now owns email's reliability problem. |
| Analytics team wants order data too. | Someone edits and redeploys Order Service. To add a *reader*. |
| Email service is slow (200 ms). | Every order now takes 200 ms longer, because Order Service waits. |
| Order Service crashes mid-fan-out. | Some services got the event, some didn't. There is no record of who got what. |

The root cause is **coupling**: the producer of the event knows, and depends on,
every consumer of it.

**Publish-subscribe** inverts this. The Order Service writes the event to a
*topic* and moves on. Anyone interested subscribes. Nobody knows about anybody.

```
                        ┌─────────────────┐        ┌──► Payment
  Order Service ──────► │  topic: orders  │ ──────►├──► Inventory
       (producer)       │   (the broker)  │        ├──► Email
                        └─────────────────┘        └──► Analytics
                                                        (consumers)
```

Now: adding a consumer requires zero changes to the producer. A slow consumer
slows only itself. A dead consumer catches up when it returns. And there *is* a
record of who got what — because of the next idea, which is the important one.

---

## 2. The one idea that makes Kafka different: it is a log, not a queue

Most message brokers (RabbitMQ, ActiveMQ, SQS) are **queues**. A queue is
destructive: a consumer takes a message and the message is *gone*.

Kafka is an **append-only log**. Messages are appended to the end of a file and
stay there. Consumers do not "take" messages; they *read* at a position they
track themselves, called an **offset**.

```
  QUEUE (RabbitMQ)                    LOG (Kafka)

  ┌───┬───┬───┬───┐                   ┌───┬───┬───┬───┬───┬───┐
  │ D │ C │ B │ A │ ──► consumer      │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │
  └───┴───┴───┴───┘                   └───┴───┴───┴───┴───┴───┘
        ▲                                       ▲       ▲
   A is consumed → deleted             consumer-X   consumer-Y
                                        at off 3     at off 5

  Two consumers COMPETE for            Two consumers read INDEPENDENTLY,
  messages; each message goes          each at its own position; nothing
  to exactly one of them.              is deleted when read.
```

Look at what falls out of this single design choice:

| Consequence | Why it follows from "it's a log" |
|---|---|
| **Multiple independent consumers** | Reading doesn't mutate. Analytics and Email both read every message, at their own pace. |
| **Replay** | A bug in the email service? Fix it, reset the offset to yesterday, reprocess. In a queue, that data is gone forever. |
| **Cheap fan-out** | Adding a consumer costs one more read cursor, not a copy of the data. |
| **Ordering is knowable** | Offsets are dense and increasing. "Message 5 comes after message 4" is a fact on disk, not a hope. |
| **Fast writes** | Appending to the end of a file is a sequential disk write — the single fastest thing a disk does, ~100x faster than random writes even on SSDs. |
| **The broker is dumb** | The broker doesn't track per-consumer delivery state. That's why one Kafka broker handles what would need a cluster of queue servers. |

That last row is the biggest one and it is worth saying plainly:

> **A queue broker must remember, per message, per consumer, whether it was
> delivered and acknowledged. A log broker remembers one integer per consumer.**

That is the whole performance story. Everything else is engineering.

The cost of this design — and there is always a cost — is that **the consumer
now owns its own progress**. If the consumer loses track of its offset, it
either reprocesses messages or skips them. Phase 4 is entirely about managing
that responsibility properly.

---

## 3. The vocabulary

You need six words. Everything in this project is built from them.

```
                        BROKER (a server process)
    ┌────────────────────────────────────────────────────────────┐
    │                                                            │
    │   TOPIC "orders"                                           │
    │   ┌──────────────────────────────────────────────────┐     │
    │   │ PARTITION 0   [0][1][2][3][4]  ← append here      │     │
    │   │ PARTITION 1   [0][1][2]        ← append here      │     │
    │   │ PARTITION 2   [0][1][2][3]     ← append here      │     │
    │   └──────────────────────────────────────────────────┘     │
    │                                                            │
    └────────────────────────────────────────────────────────────┘
             ▲                              │
             │ produce                      │ fetch(offset)
             │                              ▼
        PRODUCER                        CONSUMER  (in a CONSUMER GROUP)
```

**Broker** — one server process. It accepts TCP connections, stores messages on
disk, and serves reads. A **cluster** is several brokers cooperating.

**Topic** — a named stream of messages. `orders`, `payments`, `clicks`. Purely a
logical name; the physical thing is the partition.

**Partition** — an ordered, append-only log file. *This is the real unit of
everything.* Ordering is guaranteed **within** a partition and nowhere else.
Parallelism is per partition. Replication is per partition. When you read the
rest of these docs, translate "topic" into "set of partitions" in your head.

**Offset** — the position of a message inside a partition. A plain integer,
starting at 0, assigned by the broker at write time, never reused, never
changed. `orders-2 @ offset 41` uniquely identifies one message forever.

**Producer** — a client that appends messages. Decides *which* partition each
message goes to. That decision is the load-balancing mechanism (Phase 3).

**Consumer / Consumer group** — a client that reads. Consumers with the same
`group_id` form a group and *divide the partitions between them* — each
partition is read by exactly one member. That is how consumption scales. Two
different groups reading the same topic each get the full stream (Phase 4).

### Why "ordering only within a partition" is not a cop-out

It's the deliberate trade. Global ordering across a whole topic would require
every write to go through a single point — which caps throughput at one machine
forever. Kafka instead says: *you* choose what must be ordered. Put a customer
ID as the message key, and all events for that customer land in the same
partition and are strictly ordered. Events for different customers may
interleave, and you almost never care.

**Scalability and ordering are traded against each other, and the partition key
is the dial.**

---

## 4. What "distributed" buys us — and what it costs

The abstract for this project asks for four things. Here is what each actually
means in Kafka terms, and which phase delivers it:

| Requirement | Mechanism | Phase |
|---|---|---|
| Topic-based messaging | Topics → partitions → append-only log files | 1–2 |
| **Load balancing** | Producer-side partitioning (spread writes) + consumer groups (spread reads) + spreading partitions across brokers | 3, 4, 5 |
| **Scalability** | Add partitions → more parallelism. Add brokers → more capacity. Segments + index → reads stay O(log n) as data grows. | 2, 3, 5 |
| **Failure handling** | Retries, offset commit semantics, consumer heartbeats + rebalance, replication + ISR + leader failover | 0, 4, 6 |

And the costs, stated honestly up front, because a system that only lists
benefits is a sales pitch and not an education:

- **More partitions** means more open file handles, more memory, longer failover
  (each partition needs a new leader elected), and slower rebalances.
- **Replication** means every write happens N times and `acks=all` waits for the
  slowest replica. Durability is bought with latency.
- **A cluster** means you now have consensus problems, split-brain risk, and
  metadata that can be stale on clients. A single broker has none of these.
- **Consumer groups** mean rebalances, and a rebalance briefly stops consumption.

Distribution is not free. Every phase from here will name what it costs.

---

## 5. How this repository teaches

Seven phases. Each phase is a working system on its own — you can stop at the
end of any phase and run it. Each phase has a doc in `docs/` with the same shape:

1. **The problem** — what breaks in the previous phase.
2. **How real Kafka solves it** — and why that way.
3. **Options + trade-offs** — the alternatives we rejected and what they cost.
4. **The decision** — what we built and why.
5. **Code walkthrough** — the actual files and functions.
6. **How real Kafka differs** — the simplifications we took.
7. **Exercises** — things to try that make the idea stick.

| Phase | Title | What it adds |
|---|---|---|
| [0](phase-0/phase-0-foundation.md) | Foundation | Test harness, benchmarks, chaos scripts, admin CLI, and four bug fixes to the prototype |
| 1 | Protocol v2 | Correlation IDs, API keys/versions, real error codes |
| 2 | Storage engine | Segments, sparse index, CRC, retention, flush policy |
| 3 | Partitioning | Key hashing, batching, `acks`, producer retries — *load balancing* |
| 4 | Consumer groups | Coordinator, rebalance, server-side offsets, delivery semantics |
| 5 | Cluster | Multi-broker, membership, controller election, metadata routing |
| 6 | Replication | Leader/follower, ISR, high watermark, failover — *fault tolerance* |
| 7 | Capstone | Full demo, benchmark table, honest limitations |

**Ground rule for the whole project:** everything is hand-written using only the
Python standard library (plus `pytest`). Using a message-queue library would
hide exactly the mechanism we're trying to understand.

---

## 6. Where we start

Phase 1 of the original prototype already exists and works: one broker, one
partition per topic, a JSON-over-TCP protocol, producers and consumers that talk
to it. It is roughly 650 lines.

It also has four bugs, an O(n) read path, no tests, and no way to measure
anything. Phase 0 fixes that foundation before we build on it — because a bug in
the storage layer becomes a bug in the replication layer, and by then it is much
harder to see.

→ Continue to [Phase 0 — Foundation](phase-0/phase-0-foundation.md).

If you would rather see the working system before reading about how it got
that way, [the code reading guide](phase-0/code-reading-guide.md) walks the current
codebase file by file and traces one message from `send()` all the way to
disk.