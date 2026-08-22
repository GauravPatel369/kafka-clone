# Reading the Code: A Guided Tour of the Working System

> **A note on the numbering, because it collides.**
>
> The original prototype's docstrings called themselves "Phase 1" and promised
> things in "Phase 2". The roadmap in [00-overview.md](00-overview.md) uses a
> different numbering, where Phase 1 is *Protocol v2* — which is not built yet.
>
> This document is about **the code that exists right now**: the single-broker
> pub-sub system as it stands at the end of Phase 0. It is the thing the old
> docstrings meant by "Phase 1". Everywhere else in `docs/`, phase numbers
> follow the roadmap.

This is a reading guide, not a reference. It answers: *in what order do I open
these files, and what am I looking for in each one?*

Everything here you can verify yourself — the traces below are real byte
counts and real line numbers, not illustrations.

---

## 1. The seven files that matter

Roughly 1,500 lines total, and about 400 of them are actual logic. The rest is
explanation.

```
                     ┌──────────────────────────────────────┐
                     │  common/protocol.py    74 lines      │
                     │  the wire format — the CONTRACT      │
                     └──────────────────┬───────────────────┘
                                        │ everyone imports this
             ┌──────────────────────────┼──────────────────────────┐
             │                          │                          │
   ┌─────────▼─────────┐    ┌───────────▼──────────┐   ┌───────────▼─────────┐
   │ common/net.py     │    │ broker/broker.py     │   │ common/config.py    │
   │ client transport  │    │ TCP server, routing  │   │ defaults            │
   └─────────┬─────────┘    └───────────┬──────────┘   └─────────────────────┘
             │                          │
      ┌──────┴───────┐        ┌─────────▼──────────┐
      │              │        │ broker/partition.py│
┌─────▼─────┐  ┌─────▼─────┐  │ the append-only log│
│producer.py│  │consumer.py│  │ ← THE HEART        │
└───────────┘  └───────────┘  └────────────────────┘
```

**Read them in this order.** Each one only depends on the ones above it.

| # | File | Lines | Read it for |
|---|---|---|---|
| 1 | [common/protocol.py](../common/protocol.py) | 74 | The contract. Nothing else makes sense first. |
| 2 | [broker/partition.py](../broker/partition.py) | 351 | The append-only log. **The single most important file.** |
| 3 | [broker/broker.py](../broker/broker.py) | 366 | How bytes become `partition.append()` calls |
| 4 | [common/net.py](../common/net.py) | 306 | The client side of the same conversation |
| 5 | [producer/producer.py](../producer/producer.py) | 186 | The write client — thin on purpose |
| 6 | [consumer/consumer.py](../consumer/consumer.py) | 378 | The read client. **Where the hard trade-offs live.** |
| 7 | [common/config.py](../common/config.py) | 49 | Defaults, and why each was chosen |

If you only read two: `partition.py` and `consumer.py`. The first is the idea
Kafka is built on; the second is where you have to make a choice that cannot
be made correctly, only made deliberately.

---

## 2. Trace one message end to end

This is the fastest way to make the system click. Follow one `send()` all the
way to disk. Every number below is real — reproduce them yourself with the
snippet in §5.

### The call

```python
producer.send("hello")        # topic='orders'
```

### Step 1 — build the payload · [producer.py:100](../producer/producer.py#L100)

A plain dict. Nothing is encoded yet.

```python
{'topic': 'orders', 'partition': 0, 'value': 'hello', 'key': None}
```

The `partition: 0` is hardcoded today, and that hardcoding *is* the reason
there is no load balancing yet. Phase 3 replaces it with a partitioner.

### Step 2 — frame it · [protocol.py:42](../common/protocol.py#L42)

`encode_message` does two things: `json.dumps` the payload, then prepend a
12-byte header.

```
b'\x00\x00\xca\xfe'  \x00\x00\x00\x42  \x00\x00\x00\x01  {"topic": "orders", ...}
 └──── magic ─────┘  └─ length=66 ──┘  └─ type=1 ─────┘  └──── 66 bytes ───────┘
        4 bytes           4 bytes          4 bytes            the body

total on the wire: 78 bytes  (12 header + 66 body)
```

Three things to notice:

- **`0xCAFE` is a sanity check**, not security. It catches "you connected to
  the wrong port" and "the stream desynchronised" immediately, instead of
  letting garbage be interpreted as a length field.
- **`length=66` is the whole point.** The receiver reads 12 bytes, learns 66,
  reads exactly 66 more. That is how you get message boundaries out of a
  protocol (TCP) that has none.
- **The payload is 5 bytes of actual data in a 78-byte frame.** 93% overhead.
  That is the price of a readable format and one round trip per message, and
  it is what Phase 3's batching amortises away.

### Step 3 — send · [net.py:215](../common/net.py#L215)

`sendall`, not `send`. `send()` returns how many bytes it *actually* queued,
which under load is fewer than you gave it. Using `send()` and ignoring the
return value is a bug that only appears when the system is busy.

### Step 4 — the broker reads the header · [broker.py:214](../broker/broker.py#L214)

```python
header_bytes = await reader.readexactly(HEADER_SIZE)     # blocks until 12 bytes
magic, body_length, msg_type = decode_header(header_bytes)
if magic != MAGIC: break                                 # hang up on garbage
body_bytes = await reader.readexactly(body_length)        # then exactly 66
```

The same two-step dance as the client, in asyncio form. `readexactly` is
asyncio's version of the loop at [net.py:262](../common/net.py#L262) — both
exist because `recv(n)` is a *maximum*, not a promise.

### Step 5 — route · [broker.py:137](../broker/broker.py#L137)

`msg_type == 1` → `MSG_PRODUCE` → get the partition, append.

### Step 6 — cross the thread boundary · [broker.py:156](../broker/broker.py#L156)

```python
offset = await asyncio.get_running_loop().run_in_executor(
    None, partition.append, value, key
)
```

**This line is the most important one in the broker.** `partition.append`
writes to a file, which blocks. Calling it directly would freeze the event
loop — every other connected client stops while this one disk write happens.
`run_in_executor` hands it to a worker thread instead.

It is also the reason the next step needs a lock: the executor is a *pool*, so
two appends genuinely can run at the same instant.

### Step 7 — append · [partition.py:140](../broker/partition.py#L140)

```python
with self._lock:                        # ← one writer at a time
    offset = self.next_offset           # 41
    record = {"offset": 41, "key": None, "value": "hello", "timestamp": ...}
    self._fh.write(json.dumps(record) + '\n')
    self._fh.flush()                    # → OS page cache (NOT the disk)
    self.next_offset += 1
    return offset
```

One line appended to `data/orders-0.log`, 71 bytes:

```json
{"offset": 41, "key": null, "value": "hello", "timestamp": 1787163171}
```

**Sit with the lock for a second.** Without it:

```
thread A reads next_offset → 41
thread B reads next_offset → 41          ← both got 41
thread A writes record 41, sets next_offset = 42
thread B writes record 41, sets next_offset = 42
```

Two messages now claim offset 41, and offset 42 never exists. Every guarantee
in Kafka — ordering, replay, deduplication, replication — is built on *an
offset names exactly one message*. That lock is what makes it true.
[test_partition.py](../tests/test_partition.py) proves it with 8 threads.

**And sit with `flush()` too.** It pushes data to the OS page cache, not to
the physical disk. Kill the broker process: nothing is lost. Pull the power
cord: the last few messages are gone. That is a deliberate 1000x speed
choice, and it only becomes defensible in Phase 6, when three machines hold
copies.

### Step 8 — reply

`{'status': 'ok', 'offset': 41, ...}` goes back through the same framing, and
`producer.send()` returns `41`.

### The whole path in one picture

```
producer.send("hello")
  │
  ├─ producer.py:100     build dict
  ├─ protocol.py:42      + 12-byte header  → 78 bytes
  ├─ net.py:215          sendall
  │                          ~~~ TCP ~~~
  ├─ broker.py:214       readexactly(12) → length=66 → readexactly(66)
  ├─ broker.py:137       route: type 1 = PRODUCE
  ├─ broker.py:156       run_in_executor  ← event loop stays free
  ├─ partition.py:140    lock → assign offset 41 → write → flush → unlock
  ├─ broker.py:251       encode + write reply
  │                          ~~~ TCP ~~~
  └─ net.py:233          recv → {'offset': 41}          returns 41
```

---

## 3. Trace a read — and find the flaw

`consumer.poll()` is the mirror image, with one difference that matters.

| Step | Location | What happens |
|---|---|---|
| 1 | [consumer.py:144](../consumer/consumer.py#L144) | Send `{'topic', 'partition', 'offset': position, 'max'}` |
| 2 | [broker.py:166](../broker/broker.py#L166) | Route to `MSG_FETCH` |
| 3 | [broker.py:174](../broker/broker.py#L174) | `run_in_executor(partition.fetch, ...)` |
| 4 | [partition.py:196](../broker/partition.py#L196) | **Scan the log from byte zero** |
| 5 | [consumer.py:169](../consumer/consumer.py#L169) | Advance `position` — and commit *nothing* |

### Look hard at step 4

```python
with open(self.log_path, 'r', ...) as f:
    for line in f:                          # ← from the TOP. Every time.
        record = json.loads(line)
        if record['offset'] < start_offset:
            continue                        # ← parsed it just to throw it away
        messages.append(record)
```

To read offset 4,900 the broker parses 4,900 JSON objects it does not want.
This is **O(n) in how much data the partition already holds**, and the
benchmark shows it plainly:

```
fetch #1  (offset 0)      ~1 ms
fetch #25 (offset 2400)   ~5.8 ms      ← p50
fetch #50 (offset 4900)   13.8 ms      ← max
```

A broker that gets slower the longer it runs is not a broker you can operate.
At a million messages one fetch takes seconds. **This is the single biggest
flaw in the current design**, and Phase 2 (segments + sparse index) exists to
fix it. See [benchmarks.md](benchmarks.md).

### And look hard at step 5

```python
self.position = messages[-1]['offset'] + 1     # in memory
# no commit. deliberately.
```

`poll()` advances an in-memory cursor and writes nothing to disk. `commit()`
is a separate call at [consumer.py:192](../consumer/consumer.py#L192).

That separation *is* the delivery guarantee. Where you call `commit()` decides
whether a crash loses messages or repeats them, and there is no third option:

```
fetch → commit → process           fetch → process → commit
              ▲                                   ▲
       crash = LOST                        crash = REDELIVERED
       (at-most-once)                      (at-least-once) ← our default
```

Do not read about this. Run it:

```bash
python -m chaos.kill_consumer_midprocess
```

Same crash, same data, one line moved: five messages vanish in one run and get
reprocessed in the other.

---

## 4. Where is X implemented?

| Concept | File · function |
|---|---|
| Wire format | [protocol.py:42](../common/protocol.py#L42) `encode_message` |
| Message framing (client) | [net.py:262](../common/net.py#L262) `_recv_exactly` |
| Message framing (broker) | [broker.py:214](../broker/broker.py#L214) `readexactly` |
| **Offset assignment** | [partition.py:140](../broker/partition.py#L140) `append` |
| Append-only write | [partition.py:140](../broker/partition.py#L140) `append` |
| Read path (the O(n) one) | [partition.py:196](../broker/partition.py#L196) `fetch` |
| Crash recovery | [partition.py:255](../broker/partition.py#L255) `_recover_offset` |
| Topic auto-creation | [broker.py:101](../broker/broker.py#L101) `_get_partition` |
| Request routing | [broker.py:130](../broker/broker.py#L130) `_route` |
| Concurrency (one client) | [broker.py:195](../broker/broker.py#L195) `handle_client` |
| Blocking-I/O escape hatch | [broker.py:156](../broker/broker.py#L156) `run_in_executor` |
| Consumer position | [consumer.py:144](../consumer/consumer.py#L144) `poll` |
| **Delivery semantics** | [consumer.py:192](../consumer/consumer.py#L192) `commit` |
| Atomic offset write | [consumer.py:285](../consumer/consumer.py#L285) `_save_offset` |
| Replay | [consumer.py:206](../consumer/consumer.py#L206) `seek` |
| Consumer lag | [consumer.py:179](../consumer/consumer.py#L179) `lag` |

### And where isn't it yet?

Just as useful to know. The gap between this column and the one above is the
roadmap:

| Concept | Where it will live | Phase |
|---|---|---|
| Key-based routing | `producer/partitioner.py` | 3 |
| Batching, `acks`, retries | `producer/batch.py` | 3 |
| Segments + index | `broker/log/` | 2 |
| Retention / compaction | `broker/log/cleaner.py` | 2 |
| Error codes | `common/errors.py` | 1 |
| Group coordination, rebalance | `broker/coordinator.py` | 4 |
| Cluster membership, election | `broker/cluster.py`, `controller.py` | 5 |
| Replication, ISR, failover | `broker/replica.py` | 6 |

---

## 5. Do these five things

Reading is not understanding. Each of these takes under a minute.

**1 — See the actual bytes.**

```bash
python -c "from common.protocol import *; \
w = encode_message(MSG_PRODUCE, {'topic':'orders','partition':0,'value':'hello','key':None}); \
print(len(w), 'bytes'); print(w[:12].hex(' ')); print(w[12:])"
```

You should get 78 bytes and `00 00 ca fe 00 00 00 42 00 00 00 01`. Find the
`42` — that's 66 in hex, the body length.

**2 — Read the log with your own eyes.**

```bash
python -m broker.broker --quiet &
python -m producer.producer --topic tour --count 5
cat data/tour-0.log
```

Five JSON lines with offsets 0–4. **This plain file is the entire database.**
No index, no catalogue, no metadata — the log *is* the state. That is why
`_recover_offset` can rebuild everything by reading the last line.

**3 — Prove that reading is not destructive.**

```bash
python -m consumer.consumer --topic tour --group alice
python -m consumer.consumer --topic tour --group bob
```

Both groups get all five messages. In a queue, each message would go to
exactly one of them. This one behaviour is the whole difference between a log
and a queue, and everything else follows from it.

**4 — Break the offset and watch time travel.**

```bash
python -m consumer.consumer --topic tour --group alice          # nothing new
python -m consumer.consumer --topic tour --group alice --from-beginning
```

All five again. Replay is possible only because nothing was deleted when it
was read.

**5 — Watch the flaw in §3 with your own timer.**

```bash
python -m bench.bench --count 200
python -m bench.bench --count 20000
```

Compare the consume `max` latency. It grows with the log. Now you know exactly
what Phase 2 has to fix, and you have the number to check it against.

---

## 6. Things that confuse people

**"Where does the broker track which consumer read what?"**
It doesn't. That is the design, not an omission. The broker stores messages;
consumers store one integer each. A queue broker must remember delivery state
per message per consumer — that bookkeeping is why queue brokers are
expensive. Kafka's broker remembers *nothing* about you, which is why one
broker handles what would take a cluster of queue servers.

**"Why is there a lock inside an asyncio program?"**
Because [broker.py:156](../broker/broker.py#L156) hands file I/O to a *thread
pool*. The event loop is single-threaded; the executor is not. Any state the
executor touches needs real locking.

**"`key` is stored but nothing uses it."**
Correct, today. From Phase 3 the key decides the partition
(`hash(key) % num_partitions`), which is what guarantees that all events for
one customer stay in order. Right now it's just a field along for the ride.

**"Two consumers in the same group both read everything — isn't that wrong?"**
It is a Phase 0 limitation, yes. `group_id` currently only namespaces the
local offset filename; the broker has never heard of groups. Real group
semantics — dividing partitions between members, reassigning when one dies —
is Phase 4.

**"`fetch` reads the file with no lock. Isn't that a race?"**
Almost. Records are immutable and only ever appended, so a reader can never
see a record *change*. It can see a line that is still being flushed, and
`fetch` handles that by skipping anything that won't parse and counting it in
`corrupt_lines`. That's "probably fine" rather than "provably correct" —
Phase 2's length prefix and CRC32 make it the latter.

**"`decode_body` returns `high_watermark` — what is that?"**
One past the newest message in the partition. Subtract your position from it
and you get **lag**, the most-watched number in any Kafka deployment: growing
lag means consumers can't keep up, and eventually retention deletes data
nobody read. In Phase 6 it stops meaning "everything written" and starts
meaning "everything *replicated*", which is a much more interesting number.

---

## 7. Then read the tests

[tests/](../tests/) is the executable version of this document. Each test
names a property the system must have, and most name why:

- [test_partition.py](../tests/test_partition.py) — `test_concurrent_appends_never_duplicate_an_offset`
  is the one to read first. Delete the lock in `append` and watch it fail.
- [test_end_to_end.py](../tests/test_end_to_end.py) — `test_poll_does_not_commit`
  and `test_at_least_once_redelivers_when_processing_fails` pin down the
  delivery semantics.
- [test_protocol.py](../tests/test_protocol.py) — `test_magic_is_big_endian_on_the_wire`
  exists to stop someone "simplifying" the `!` out of the struct format.

A good way to finish: pick a test, break the code it covers, and confirm it
fails for the reason you expected. If it doesn't, one of you is wrong, and
finding out which is the actual learning.

---

---

## Where to go from here

- [00-overview.md](00-overview.md) — the concepts, if you read the code first
  and want the theory second
- [phase-0-foundation.md](phase-0-foundation.md) — the four bugs this code
  used to have, and why each fix is the way it is
- [benchmarks.md](benchmarks.md) — the numbers behind §3's flaw

Phase 1 (Protocol v2) is next to be built: correlation IDs, API keys and
versions, and numeric error codes — which between them fix the
one-request-in-flight limit and the "errors are prose in a JSON field" problem
you just read about.
