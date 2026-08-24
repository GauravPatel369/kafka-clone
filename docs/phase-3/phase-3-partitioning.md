# Phase 3 — Partitioning, Batching, and Load Balancing

> **Goal:** more than one partition per topic, a producer that decides which
> one each message goes to, and batching that finally amortises the ~230 µs of
> per-message broker work.
>
> This is where **load balancing** — one of the four things the project set out
> to demonstrate — stops being a word in the roadmap.

---

## 1. The problem

### One partition is one of everything

Through Phase 2 a topic had exactly one partition. That caps three things at
once:

- **One write path.** Every message for a topic goes through one file, one
  lock, one broker. You cannot add capacity.
- **One reader.** A consumer group could never have more than one useful
  member, because there is only one thing to assign.
- **One machine.** Nothing can be spread, because there is nothing to spread.

Partitions are the unit of parallelism in Kafka. Without them, "distributed" is
not available — Phases 5 and 6 have nothing to distribute.

### The producer is still paying full price per message

The Phase 1 cost breakdown, unchanged by Phase 2:

```
PRODUCE, sequential           253 µs per message
  → loopback round trip        38 µs      (pipelining removes this)
  → broker work               215 µs      (nothing so far has touched it)
```

That 215 µs is a thread hop to the executor, a file write, a flush, and a JSON
encode/decode — **paid once per message**, because one request carries one
message. Phase 1 proved pipelining cannot help: the broker was never idle.

The only way to cut per-message cost is to stop having one request per message.

### `key` has been decorative since Phase 0

Every message has carried a `key` field. Nothing has ever read it. It was
stored, returned, and ignored — the placeholder for the mechanism this phase
finally builds.

---

## 2. How real Kafka solves it

### The producer chooses the partition

Not the broker. That is worth sitting with, because it is backwards from most
systems: there is no load balancer in front deciding where writes go.

```
                            ┌──────────────► partition 0  (leader: broker 1)
   Producer ── partitioner ─┼──────────────► partition 1  (leader: broker 2)
                            └──────────────► partition 2  (leader: broker 3)
```

The producer holds cluster metadata, computes a partition, and connects
**directly to that partition's leader**. No proxy hop, no coordinator in the
data path. This is what makes Kafka clients "thick", and it is why `METADATA`
and `NOT_LEADER_OR_FOLLOWER` were built back in Phase 1.

### The key decides, and that is the ordering dial

```
    key = null      →  spread across partitions (best balance)
    key = "cust-42" →  murmur2("cust-42") % num_partitions  (always the same one)
```

Ordering is guaranteed *within* a partition. So keying by customer ID means
every event for that customer is strictly ordered, while different customers
interleave freely — and you almost never care.

> **Scalability and ordering trade against each other, and the partition key is
> the dial.** Key everything by one value and you have one partition's
> throughput with total ordering. Key nothing and you have perfect balance with
> no ordering at all.

### Batching is what actually makes producers fast

```
   WITHOUT batching                   WITH batching (linger.ms = 5)

   send ──► [1 msg] ──► broker        send ─┐
   send ──► [1 msg] ──► broker        send ─┼─ accumulate 5 ms ─┐
   send ──► [1 msg] ──► broker        send ─┘                    │
                                                        [200 msgs] ──► broker
   200 requests, 200x broker work      1 request, ~1x broker work
```

`send()` does not send. It appends to an in-memory batch and returns a
**Future**. A background sender thread ships whole batches when they fill
(`batch.size`) or when they have waited long enough (`linger.ms`).

Note this **forces** the API change: a `send()` that returned an offset would
have to block for the round trip, which defeats the batching entirely. The
Future is not a stylistic choice — it is what batching requires of the caller.

---

## 3. Options considered

### Who picks the partition?

| Option | Trade-off |
|---|---|
| **Producer-side** ← chosen | No proxy in the data path; the client talks straight to the leader. Cost: every client needs cluster metadata and the logic to use it — "thick clients". |
| Broker-side (any broker forwards) | Dumb clients. But every message takes two network hops, and the receiving broker becomes a bottleneck. |
| A load balancer in front | Familiar, and completely breaks key→partition ordering, because the LB does not know about keys. |

### Which hash?

| Option | Trade-off |
|---|---|
| Python's `hash()` | **Actively wrong.** `hash()` on `str` is randomised per process by PYTHONHASHSEED. Two producers would send the same key to *different* partitions, silently destroying per-key ordering — and it would work perfectly in every single-process test. |
| `md5` / `sha256` | Deterministic and well-distributed, but ~20x slower than needed, on the hot path, to defend against an attacker who does not exist. |
| **murmur2** ← chosen | What Kafka actually uses. Fast, well-distributed, deterministic across processes and languages — so a Python producer and a Java producer agree on where a key goes. |

The `hash()` row is the important one: it is the plausible choice, it passes
every test you would naturally write, and it fails only in production with more
than one producer process.

### Which partition for keyless messages?

| Option | Trade-off |
|---|---|
| Round-robin per message | Perfect balance. Terrible batching: with 10 partitions, 10 batches each fill 10x slower, so you send 10 small requests instead of 1 large one. |
| **Sticky** ← chosen | Fill one partition's batch, send it, then switch. Same balance *over time*, far better batching. This is Kafka's KIP-480, added in 2.4 precisely because round-robin was defeating batching. |
| Random | Simple; same batching problem as round-robin, plus worse balance in the short run. |

Sticky partitioning is a lovely example of two features interacting: the
"obviously fair" policy is the wrong one once batching exists.

### `acks`

| Level | Waits for | Risk | Our behaviour |
|---|---|---|---|
| `0` | nothing | Total silence on failure — losses are invisible | Broker sends **no response at all** |
| `1` | the leader's log | Lost if the leader dies before replication | Default |
| `all` | every in-sync replica | Slowest replica sets your latency | Same as `1` until Phase 6 has replicas |

`acks=0` is implemented faithfully: the broker sends nothing back. That is a
useful thing to see in a protocol that is otherwise strictly request/response —
and it is why the producer's Future resolves with offset `-1`. You asked not to
be told, so you are not told.

### Explicit topics or auto-creation?

| Option | Trade-off |
|---|---|
| Auto-create (Phases 0–2) | One less command in a demo. Producing to `"oders"` silently creates a new topic and your messages go where nobody reads. |
| **Explicit `CREATE_TOPICS`, auto-create configurable** ← chosen | Matches Kafka's `auto.create.topics.enable`, which most production clusters turn **off** for exactly that reason. Auto-created topics get 1 partition, so relying on it also silently costs you all your parallelism. |

---

## 4. The decision

### PRODUCE gets a v1 — and v0 keeps working

This is the first real use of the `api_version` field built in Phase 1.

```
v0 (Phase 1)   {topic, partition, key, value}
               → one record, one partition, one error code

v1 (Phase 3)   {topic, acks, partitions: [
                   {partition: 0, records: [{key, value}, ...]},
                   {partition: 3, records: [{key, value}, ...]}]}
               → many records, many partitions, ONE ERROR CODE PER PARTITION
```

**The broker speaks both.** `SUPPORTED_VERSIONS[API_PRODUCE] = (0, 1)`, and a
v0 request still works exactly as before.

That is not backwards-compatibility theatre — it is the entire point of
per-API versioning demonstrated in one commit. A rolling upgrade works because
a broker can serve old and new clients at once, and here you can watch it
happen.

**Per-partition error codes** matter because one request now touches several
partitions. If partition 3 is unavailable, partitions 0 and 1 must still
succeed and report so. One top-level code could not express that — which is
exactly what Exercise 7 of Phase 1 asked you to work out.

### The producer becomes asynchronous

```python
future = producer.send('order placed', key='cust-42')   # returns immediately
producer.flush()                                        # waits for everything
offset = future.get()                                   # or wait for just one
```

- `RecordAccumulator` — per-partition buffers, tracks bytes and age
- `Sender` — a daemon thread that drains ready batches
- `FutureRecordMetadata` — resolved with an offset, or with the error

`send_and_wait()` remains for the cases (tests, demos) where a blocking call is
what you actually want. It is honestly named: it is `send()` plus a wait, not a
different mechanism.

### Retries

Retriable errors and connection failures are retried with **exponential backoff
plus jitter**:

```python
delay = min(base * 2**attempt, max_backoff)
delay *= 0.5 + random.random()        # jitter
```

The jitter is not decoration. Without it, every producer that failed at the
same moment retries at the same moment, and the broker that just recovered is
hit by a synchronised thundering herd. Randomising spreads the retries out.

**`max_in_flight` defaults to 1**, and that is deliberate: with more than one
batch in flight and retries enabled, a retried batch can land *after* one sent
later, silently reordering a partition. Kafka's default is 5, which is why
`enable.idempotence` exists to make it safe. We document the hazard rather than
implementing sequence numbers.

---

## 5. Code walkthrough

| File | Role |
|---|---|
| [`broker/metadata.py`](../../broker/metadata.py) | Topic registry persisted to `data/_metadata.json`. Atomic writes — the same temp+fsync+rename pattern as consumer offsets. |
| [`producer/partitioner.py`](../../producer/partitioner.py) | `murmur2`, and the three strategies: key-hash, sticky, explicit. |
| [`producer/batch.py`](../../producer/batch.py) | `RecordAccumulator`, `Sender`, `FutureRecordMetadata`. |
| [`producer/producer.py`](../../producer/producer.py) | Async `send()`, `flush()`, retries, `acks`. |
| [`broker/broker.py`](../../broker/broker.py) | PRODUCE v1, `CREATE_TOPICS`, metadata-aware `METADATA`, partition validation. |
| [`tools/kfk.py`](../../tools/kfk.py) | `topics create` / `list` / `describe`. |

### The partitioner, in full

```python
def partition_for(key, num_partitions, explicit=None):
    if explicit is not None:
        return explicit                                   # caller overrode
    if key is None:
        return sticky_choice(num_partitions)              # batch-friendly
    return (murmur2(key.encode()) & 0x7fffffff) % num_partitions
```

The `& 0x7fffffff` masks off the sign bit. murmur2 returns a *signed* 32-bit
integer, and `-7 % 3` is `2` in Python but `-1` in Java — so without the mask a
Python and a Java producer would disagree about where a key belongs. Masking
first makes the value non-negative in both languages, and the results match.

---

## 6. How real Kafka differs

- **Compression.** Real batches are compressed as a unit (`snappy`, `lz4`,
  `zstd`), which is where batching's *other* big win comes from — compressing
  200 similar JSON records together is far more effective than compressing each
  alone. We do not compress at all.
- **The idempotent producer.** A producer ID plus a per-partition sequence
  number lets the broker discard duplicate retries, which is what makes
  `max.in.flight=5` safe. We default to 1 instead.
- **`buffer.memory` and blocking.** A real producer has a bounded memory pool
  and `send()` blocks (or throws) when it is full. Our accumulator is unbounded
  — a producer that outruns its broker will grow until it dies.
- **Partition counts can only increase.** Kafka forbids decreasing them,
  because shrinking would strand data and remap keys. We forbid changes
  entirely, which is stricter and simpler.
- **Per-topic configuration.** Real topics carry their own retention, segment
  size, and cleanup policy. Ours is broker-wide.

---

## 7. Exercises

1. **Watch the batching win.** Run `python -m bench.bench --count 20000` and
   compare the three produce rows. Then set `linger_ms=0` and explain why
   throughput collapses back toward the Phase 2 number.
2. **See the balance.** Run `python -m chaos.partition_balance` and read all
   three experiments. Predict experiment 3's output before you get to it.
3. **Break ordering deliberately.** Produce keyed messages to a 3-partition
   topic, then re-create it with 4 partitions and produce the same keys. How
   many keys changed partition? (Expected: about 3 in 4.) What does that do to
   a consumer that assumed per-key ordering?
4. **Use the wrong hash.** Replace `murmur2` with Python's `hash()`. Confirm
   every test still passes. Then run two producer *processes* and compare where
   the same key lands. This is the bug the tests cannot catch.
5. **Feel `linger.ms`.** Set it to 500 and send one message. Measure the
   latency. Batching is not free — you bought throughput with latency, and this
   is the bill.
6. **Watch `acks=0` lose data silently.** Produce with `acks=0` to a
   nonexistent partition. What does the producer report? What *should* an
   application do about that?

---

## 8. Verification

```bash
pytest -q
python -m chaos.partition_balance
python -m bench.bench --count 20000

python -m broker.broker                                     # terminal 1
python -m tools.kfk topics create --topic orders --partitions 4
python -m producer.producer --topic orders --count 2000
python -m tools.kfk topics describe --topic orders          # message spread
```

The headline is the **produce throughput row** and the **even spread across
partitions**. Record both in [`benchmarks.md`](../benchmarks.md).

→ Next: **Phase 4 — Consumer groups**, which is the read-side half of load
balancing: dividing these partitions among members and reassigning them when
one dies.
