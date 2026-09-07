# Implementation Flows: Every Path, Traced Through the Code

For each thing the system does, this traces the **exact call sequence** with
file and line, shows the code that does the work, and says why it is written
that way.

The phase docs explain *why* each mechanism exists. The
[code reading guide](phase-0/code-reading-guide.md) is a tour for a newcomer.
**This is the reference**: open it next to the code when you need to know what
actually happens.

Flows 1–18 are what a single broker does; flows 19–26 are what happens once
there are several, and are where the interesting failures live.

Line numbers were accurate when written and drift as the code changes. Function
names are stable — if a line has moved, search the name.

---

## Contents

| # | Flow | Starts at |
|---|---|---|
| 1 | [Broker startup](#1-broker-startup) | `python -m broker.broker` |
| 2 | [Create a topic](#2-create-a-topic) | `kfk topics create` |
| 3 | [Produce a message](#3-produce-a-message) | `producer.send()` |
| 4 | [Write it to disk](#4-write-it-to-disk) | `Partition.append()` |
| 5 | [Roll a segment](#5-roll-a-segment) | `Log._maybe_roll()` |
| 6 | [Fetch messages](#6-fetch-messages) | `consumer.poll()` |
| 7 | [Join a consumer group](#7-join-a-consumer-group) | `consumer.subscribe()` |
| 8 | [Commit an offset](#8-commit-an-offset) | `consumer.commit()` |
| 9 | [Detect a dead consumer](#9-detect-a-dead-consumer) | the heartbeat thread |
| 10 | [Where a consumer starts](#10-where-a-consumer-starts-reading) | `OFFSET_FETCH` |
| 11 | [Assignment strategies](#11-assignment-strategies) | the group leader |
| 12 | [Leave a group cleanly](#12-leave-a-group-cleanly) | `consumer.close()` |
| 13 | [Manual assignment](#13-manual-assignment-and-the-thread-guard) | `consumer.assign()` |
| 14 | [Rebalance edge cases](#14-rebalance-edge-cases) | timeouts |
| 15 | [Observe a group](#15-observe-a-group) | `kfk groups describe` |
| 16 | [Retention](#16-retention-deleting-old-data) | the cleaner task |
| 17 | [Crash recovery](#17-crash-recovery) | `Log._load()` |
| 18 | [Error handling](#18-how-an-error-travels) | anywhere |

**Cluster flows (Phases 5 and 6):**

| # | Flow | Starts at |
|---|---|---|
| 19 | [Cluster startup and membership](#19-cluster-startup-and-membership) | `--cluster cluster.json` |
| 20 | [Electing a controller (Raft)](#20-electing-a-controller-raft) | an election timeout |
| 21 | [Committing a metadata decision](#21-committing-a-metadata-decision) | `raft.propose()` |
| 22 | [Placing partitions](#22-placing-partitions) | `controller.recompute()` |
| 23 | [Snapshotting the Raft log](#23-snapshotting-the-raft-log) | `_maybe_snapshot()` |
| 24 | [Catching up a lost follower](#24-catching-up-a-follower-that-fell-off-the-log) | `INSTALL_SNAPSHOT` |
| 25 | [Routing a client](#25-routing-a-client-to-the-right-broker) | the metadata cache |
| 26 | [Failing over a partition leader](#26-failing-over-a-partition-leader) | a broker dies |

---

## 1. Broker startup

```
python -m broker.broker
  └─ broker.py:1122   main()                    parse args, configure logging
     └─ broker.py:99  Broker.__init__()         build the four subsystems
     └─ broker.py:951 Broker.start()            bind, then serve until stopped
```

### What `__init__` builds, in order

The order is load-bearing — each depends on the one before:

```python
# broker/broker.py:99
self.log_config = log_config or LogConfig()        # 1. storage tuning
self.metadata   = MetadataStore(data_dir, ...)     # 2. which topics exist
self.coordinator = GroupCoordinator(node_id)       # 3. group membership
self.offsets    = OffsetStore(data_dir, self.log_config)   # 4. needs (1)
```

> This ordering was a bug once: `OffsetStore` was constructed before
> `log_config` existed, and every test errored with
> `'Broker' object has no attribute 'log_config'`. Constructor ordering is real
> coupling, not just style.

### `start()` — bind, then run three things concurrently

```python
# broker/broker.py:951
server = await asyncio.start_server(self.handle_client, self.host, self.port)
self.port = server.sockets[0].getsockname()[1]   # port=0 → OS picks; read it back
self.ready.set()                                  # tests wait on this, not sleep()

cleaner = asyncio.create_task(self._retention_loop())    # broker.py:1014
reaper  = asyncio.create_task(self._group_reap_loop())   # broker.py:986

async with server:
    await self._shutdown.wait()                   # not serve_forever()
```

Three details that each fix a real problem:

- **`port = 0`** asks the OS for any free port, then we read back what it chose.
  This is why the 210-test suite never collides with a dev broker on 9092.
- **`self.ready`** is a `threading.Event` (not asyncio) because the waiter is a
  *different thread* — the test fixture. Sleep-and-hope is the number one cause
  of flaky integration tests.
- **`await self._shutdown.wait()`** instead of `serve_forever()` so shutdown is
  orderly: stop accepting → close every partition's file handle. `stop()`
  ([broker.py:1050](../broker/broker.py#L1050)) uses `call_soon_threadsafe`,
  the only sanctioned way to touch a loop from outside its thread.

### How to explain this in an interview

- **The 30-second pitch**: *"Broker startup enforces strict initialization order: storage configuration first, then metadata, group coordinator, and offset store. The broker runs on an asyncio TCP server with background coroutines for retention and dead consumer reaping. Using `port=0` gives ephemeral port binding for test isolation, and graceful shutdown cleanly drains connections and closes file handles."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When `python -m broker.broker` starts up, execution divides into two phases: synchronous component wiring in `__init__` and asynchronous lifecycle management in `start()`.*
  >
  > *First, in `__init__`, component construction order is strictly coupled: `log_config` is instantiated first because the storage engine tuning parameters are needed by subsequent components. Next, `MetadataStore` loads known topic definitions from disk. Then `GroupCoordinator` initializes consumer group state machines in memory. Finally, `OffsetStore` is constructed to manage `__consumer_offsets`, which requires `log_config` to be already initialized. If this order is violated, initialization crashes.*
  >
  > *Second, in `start()`, we invoke `asyncio.start_server()` to bind our TCP listener. In tests, we pass `port=0`, which instructs the OS kernel to bind any free ephemeral port, preventing port collisions across concurrent test runs; we immediately read `getsockname()[1]` back to discover the port. We then set a `threading.Event` (`self.ready`) so outer test threads know the broker is accepting sockets without relying on flaky `time.sleep()`.*
  >
  > *Concurrently, the broker launches two background asyncio tasks: `_retention_loop`, which periodically scans and deletes expired log segments, and `_group_reap_loop`, which monitors consumer group member heartbeat timestamps. The server loop waits on `_shutdown.wait()` rather than blocking with `serve_forever()`. When shutdown is requested, `stop()` schedules an event via `call_soon_threadsafe`, stopping new inbound connections, cancelling background cleaner tasks, and cleanly closing all open segment file descriptors across all partitions to prevent file descriptor leaks or unflushed buffers."*
- **Key mechanism to highlight**: Constructor dependency ordering — `OffsetStore` depends on `log_config` already existing; asynchronous server loop with background tasks rather than thread-per-connection.
- **Trap / Follow-up**: *"Why use an asyncio event loop if file I/O blocks?"* → The event loop multiplexes network I/O across thousands of idle connections. Whenever blocking disk I/O is needed (like appending to a partition log), it hops to a thread pool via `run_in_executor`.

---

## 2. Create a topic

```
kfk topics create --topic orders --partitions 4
  └─ tools/kfk.py     cmd_topics_create()
     └─ CREATE_TOPICS request (api_key 19)
        └─ broker.py:282  _handle_create_topics()
           └─ metadata.py MetadataStore.create_topic()
              ├─ validate_topic_name()
              └─ _save()      atomic write
```

### Why a topic must be *declared*, not inferred

```python
# broker/metadata.py — create_topic
config = TopicConfig(partitions=partitions, replication_factor=1)
self.topics[name] = config
self._save()
```

Note what it does **not** do: create any directories. Partition logs still
appear lazily on first write.

That split matters. The producer needs the partition **count** to route a
message — `murmur2(key) % count` — and it needs it *before* any of those
partitions contain anything. Counting directories would give a number that
grows as data arrives, so key routing would shift under running clients.

### The atomic save

```python
# broker/metadata.py — _save
tmp_path = self.path + '.tmp'
with open(tmp_path, 'w', encoding='utf-8') as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.flush()
    os.fsync(fh.fileno())        # data physically on disk...
os.replace(tmp_path, self.path)  # ...before the name swaps
```

Same pattern as consumer offsets in Phase 0. Opening the real file with `'w'`
truncates it to zero bytes first; a crash in that window leaves an **empty
metadata file**, which reads back as "no topics exist" while every partition's
data sits on disk unreachable. The `fsync` must precede the rename or the
rename can land first and leave a correctly-named file full of zeros.

### How to explain this in an interview

- **The 30-second pitch**: *"Topic creation declares partition counts upfront before any messages arrive. We write topic configurations atomically using a write-flush-fsync-rename pattern to prevent corrupted 0-byte files on crash. Crucially, topic creation creates zero log directories on disk — partition folders appear lazily on the first write."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When an administrator runs `kfk topics create --topic orders --partitions 4`, a `CREATE_TOPICS` RPC (API key 19) is dispatched to the broker.
  >
  > On the broker, `_handle_create_topics` validates the topic name against strict regex criteria (rejecting special characters and internal prefixes like `__consumer_offsets`). It then delegates to `MetadataStore.create_topic`.
  >
  > Here is the critical design choice: `create_topic` stores the schema in memory (`TopicConfig(partitions=4, replication_factor=1)`) and writes it to disk, but it creates ZERO partition directories or `.log` files on the filesystem. Partitions are instantiated lazily only when the first produce request arrives.
  >
  > Why must partition count be declared in advance if directories are lazy? Because producers route records by computing `murmur2(key) % num_partitions`. The producer needs an authoritative partition count *before* writing any records. If partition counts were derived by counting directories, the topic would report 0 or 1 partition initially, routing all initial keys to partition 0 until other directories were created, permanently scrambling key ordering.
  >
  > To persist metadata safely, `_save` executes a write-flush-fsync-rename sequence: we write JSON to a temporary file (`metadata.json.tmp`), flush Python's buffers, invoke `os.fsync(fd)` to force disk write, and finally execute an atomic filesystem rename via `os.replace`. If power is lost at any microsecond during this process, the file on disk is guaranteed to be either the complete prior metadata or the complete new metadata — never an empty or half-written file that would render all partition logs on disk unreadable."*
- **Key mechanism to highlight**: Partitions must be declared in advance because producers hash `murmur2(key) % num_partitions`. If directories were created lazily without a central declaration, partition count would fluctuate as data arrived, breaking key ordering.
- **Trap / Follow-up**: *"What happens if power is cut during topic creation?"* → Atomic rename (`os.replace`) ensures the filesystem exposes either the complete previous metadata file or the complete new metadata, never a truncated or empty file.

---

## 3. Produce a message

The full path, from `send()` to an offset. **`send()` does not send.**

```
producer.send('order placed', key='cust-42')
  └─ producer.py:177  Producer.send()
     ├─ producer.py:355  _num_partitions()      cached; METADATA on first use
     ├─ partitioner.py:238  DefaultPartitioner.partition()
     │  └─ partitioner.py:126  partition_for_key()  → murmur2 % 4
     └─ batch.py:210  RecordAccumulator.append() → returns a Future
                                    ⋮
   (background)  batch.py:296  Sender.run()
     └─ batch.py:304  _drain_once()
        └─ producer.py:246  _send_batches()
           └─ producer.py:265  _send_topic_batches()
              └─ producer.py:304  _request_with_retries()
                 └─ net.py:252  SyncConnection.request()   PRODUCE v1
```

### Step 1 — route

```python
# producer/producer.py:177
topic = topic or self.topic
num_partitions = self._num_partitions(topic)
target = self._partitioner.partition(topic, key, num_partitions, partition)

future, became_full = self._accumulator.append(topic, target, key, value)

if became_full and key is None and partition is None:
    self._partitioner.on_batch_full(topic, num_partitions)
```

The last two lines are the sticky partitioner: fill **one** partition's batch,
then move on. Round-robin per message would spread consecutive messages across
all four partitions, so each batch fills 4x more slowly — four small requests
where one large one would do.

### Step 2 — the hash

```python
# producer/partitioner.py:126
def partition_for_key(key, num_partitions):
    if isinstance(key, str):
        key = key.encode('utf-8')
    return (murmur2(key) & 0x7fffffff) % num_partitions
```

Two non-obvious things:

- **`murmur2`, not Python's `hash()`.** `hash()` on a `str` is randomised per
  process by PYTHONHASHSEED, so two producer processes would route the same key
  to different partitions — silently destroying per-key ordering while passing
  every single-process test.
- **`& 0x7fffffff`** is not to avoid negative indices (Python's `%` is already
  non-negative). It is because **Java's is not**: `-7 % 3` is `2` in Python and
  `-1` in Java. Masking first makes both agree.

### Step 3 — buffer and return

```python
# producer/batch.py:210
batch = self._batches.get((topic, partition)) or ProducerBatch(topic, partition)
future = batch.append(key, value)
became_full = batch.is_full(self.batch_size)
if became_full:
    self._ready.notify()          # wake the sender early
return future, became_full
```

`send()` returns here. Nothing has been sent.

**The Future is forced, not stylistic.** To return an offset, `send()` would
have to wait for the reply — and a message that waits for its own reply was
never batched with anything. Blocking and batching are mutually exclusive.

### Step 4 — the sender thread ships it

```python
# producer/batch.py:296
while self._running.is_set():
    self.accumulator.wait_for_work(self.poll_interval)   # linger_ms lives here
    self._drain_once()
self._drain_once(force=True)      # drain on shutdown, don't discard
```

`wait_for_work` is a condition variable with a timeout: the timeout implements
`linger_ms`, the notify implements "a batch filled early".

### Step 5 — one request, many partitions

```python
# producer/producer.py:265
payload = {
    'topic': topic,
    'acks': self.acks,
    'partitions': [
        {'partition': b.partition, 'records': b.records} for b in batches
    ],
}
response = self._request_with_retries(payload)
```

This is PRODUCE **v1**. v0 (one record, one partition) still works — the broker
branches on `api_version` at [broker.py:219](../broker/broker.py#L219). That is
per-API versioning demonstrated rather than described.

### Step 6 — resolve the futures

```python
# producer/producer.py:265 (continued)
for batch in batches:
    entry = results.get(batch.partition)
    if entry.get('error_code', 0) != 0:
        batch.fail(error_for_code(...))       # per-partition error
    else:
        batch.complete(entry['base_offset'])  # record i → base_offset + i
```

The broker returns **one** `base_offset` per partition, not one per record —
offsets in a batch are contiguous. Per-partition error codes are why v1 exists:
one bad partition must not fail the nine that succeeded.

### `acks=0` — the one request with no response

```python
# producer/producer.py:304
with self._conn_lock:
    if self.acks == 0:
        self._conn.send_request(API_PRODUCE, payload, 1)   # send, do NOT read
        return None
    return self._conn.request(API_PRODUCE, payload, 1)
```

and on the broker:

```python
# broker/broker.py:826 (handle_client)
if response is not None:
    await _reply(writer, header, response)
```

`_handle_produce_v1` returns `None` for `acks=0` and the broker writes
**nothing**. Writing an unread reply would leave bytes in the socket that the
*next* response gets read as — permanent desync. Sending nothing is what keeps
the stream in sync.

> **`self._conn_lock`** covers the request *and* its response as one unit. The
> sender thread and caller threads share one socket; interleaving them makes
> each read the other's reply. That was a real bug, and its symptom was
> `Bad magic 0x7b226572` — which is `{"er`, a JSON body read as a header.

### How to explain this in an interview

- **The 30-second pitch**: *"Calling `producer.send()` is asynchronous and non-blocking — it buffers records into a `RecordAccumulator` and returns a Future immediately. A background sender thread wakes when batches reach `batch_size` or when `linger_ms` expires, batching writes over the wire. This batching amortized per-request network and disk overhead, boosting throughput 12x (from 3,764 to 45,082 msg/s)."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"The produce path begins when client code calls `producer.send(topic, value, key)`. The most vital operational concept is that `send()` does not touch the network — it buffers and returns a `Future` immediately.
  >
  > First comes partition routing: the producer queries its cached metadata for partition count. If a key is present, `partition_for_key` hashes it using 32-bit Murmur2 masked with `& 0x7fffffff` (matching Java Kafka's sign arithmetic and avoiding Python's randomized process seed). If no key is provided, the sticky partitioner routes all consecutive records to the same partition until that partition's batch fills, preventing round-robin scattering where batches fill 4x slower.
  >
  > Second is buffering: records append to a thread-safe `RecordAccumulator`. Each `(topic, partition)` tuple maintains an active `ProducerBatch`. When an append causes a batch to reach `batch_size`, the accumulator notifies the background sender thread's condition variable.
  >
  > Third is transmission: a dedicated background `Sender` thread loops calling `wait_for_work()`, waiting on the condition variable with a timeout of `linger_ms`. When `linger_ms` expires or a batch becomes full, the sender drains pending batches, groups them into a single `PRODUCE v1` request containing multiple partitions, and transmits it over the TCP connection.
  >
  > Fourth is future resolution: when the broker responds, it returns a single `base_offset` and an error code per partition. The sender thread iterates through the batch, sets each record's future result to `base_offset + index`, and wakes awaiting application threads. If a partition failed, only that partition's batch is failed or retried with exponential backoff and randomized jitter.
  >
  > For `acks=0`, the producer sends the payload and returns `None` immediately without waiting. Crucially, the broker handler detects `acks=0` and writes zero bytes back. If the broker sent an unread response, leftover bytes in the TCP stream would misalign the framing header on the next request, causing corrupted magic errors."*
- **Key mechanism to highlight**: Sticky partitioning batches keyless records to the same partition until full, maximizing batch density. Murmur2 hashing with `& 0x7fffffff` matches Java Kafka and avoids Python's randomized hash seed.
- **Trap / Follow-up**: *"How does `acks=0` work over TCP?"* → The producer writes bytes and immediately returns without waiting. The broker processes the message and intentionally replies with *nothing*. If the broker sent an unread response, leftover bytes on the TCP socket would desynchronize framing and corrupt the next request header.

---

## 4. Write it to disk

```
broker.py:315  _handle_produce_v1()
  └─ broker.py:200  _run_blocking()           → executor thread
     └─ broker.py:1072  _append_records()
        └─ partition.py:88  Partition.append()   ← the lock
           └─ log.py:185  Log.append()
              ├─ log.py:203  _maybe_roll()
              ├─ segment.py:119  Segment.append()
              │  ├─ record.py:84  encode_record()   length + CRC32 + JSON
              │  └─ index.py:119  Index.append()    every 4 KB
              └─ log.py:227  _maybe_fsync()
```

### The executor hop, and why the lock exists

```python
# broker/broker.py:200
return await asyncio.get_running_loop().run_in_executor(None, fn, *args)
```

`partition.append` blocks on file I/O. Calling it directly would freeze the
event loop — every other connected client waits for this one disk write.

But the executor is a **pool**, so two appends genuinely run at the same
instant on different threads. Hence:

```python
# broker/partition.py:88
def append(self, value, key=None):
    with self._lock:
        return self.log.append(value, key)
```

Without that lock:

```
thread A reads next_offset → 41
thread B reads next_offset → 41      ← both got 41
thread A writes record 41, next_offset = 42
thread B writes record 41, next_offset = 42
```

Two messages claim offset 41; offset 42 never exists. Every guarantee in the
system rests on *an offset naming exactly one message*.
`test_concurrent_appends_never_duplicate_an_offset` runs 8 threads × 50
messages against this.

### The record on disk

```python
# broker/log/record.py:84
payload = json.dumps(record).encode('utf-8')
header  = struct.pack('!I I', len(payload), zlib.crc32(payload))
return header + payload
```

```
┌──────────────┬──────────────┬────────────────────────────┐
│  length 4B   │   crc32 4B   │   payload (JSON bytes)     │
└──────────────┴──────────────┴────────────────────────────┘
```

The **length** makes record boundaries explicit, so a reader can skip a record
without parsing it — which is what the index scan needs. The **CRC** lets a
reader know the bytes are intact *before* interpreting them, which is the case
Phase 1 could not handle: a torn record that still parses into a plausible
message and gets served as real data.

### Flush, but not fsync

```python
# broker/log/segment.py:119
self._fh.write(encoded)
self._fh.flush()      # Python buffer → OS page cache. Readers see it now.
```

| Level | Call | Survives | Cost |
|---|---|---|---|
| Python buffer | (nothing) | nothing | free |
| OS page cache | `flush()` | **process kill** | ~1 µs |
| Physical disk | `os.fsync()` | **power loss** | ~1–10 ms |

We flush. `fsync` is configurable and **off** by default
([log.py:227](../broker/log/log.py#L227)), exactly as real Kafka ships. Kafka's
position is that durability is bought with *replication*, not fsync — which
only becomes defensible once Phase 6 exists.

### The sparse index entry

```python
# broker/log/segment.py:119 (continued)
if len(self.index) == 0 or self._bytes_since_index >= self.index_interval_bytes:
    self.index.append(offset, position)
    self._bytes_since_index = 0
```

`len(self.index)`, not `self.index.entries()` — `entries()` builds a whole list,
and this runs on **every append**. That was a real bug: an O(n) call on the hot
write path made append cost quadratic in segment size. No test caught it;
re-reading the code did.

### How to explain this in an interview

- **The 30-second pitch**: *"Produce writes hop from the asyncio loop to a worker thread pool via `run_in_executor`. Inside the partition, a threading lock serializes appends to ensure contiguous, non-duplicate offsets. Each record is prefixed with a 4-byte length and a CRC32 checksum, appended to the active segment, and flushed to the OS page cache (fsync is skipped by default)."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a produce request arrives on the broker's asyncio event loop, `_handle_produce_v1` unpacks the records. Because file I/O operations block the OS thread, the broker immediately offloads disk appending to a worker thread pool using `run_in_executor(None, partition.append, records)`.*
  >
  > *Inside the executor thread, execution enters `Partition.append`. Here, a threading lock (`self._lock`) is acquired. This lock is load-bearing: because the executor is a multi-threaded pool, two produce requests for the same partition run concurrently on separate OS threads. Without the partition lock, both threads could read `next_offset = 41` simultaneously and both write records claiming offset 41, causing duplicate offsets and skipping offset 42. The lock guarantees the foundational invariant that an offset names exactly one message.*
  >
  > *Inside `Log.append`, the log checks if the active segment must roll (if `size >= segment_bytes` or `time >= segment_ms`). It then encodes each record in `Segment.append`: each record is structured with a 4-byte payload length, a 4-byte CRC32 checksum, and the raw serialized payload. The CRC32 checksum allows readers to verify that bytes have not been torn or corrupted before parsing JSON.*
  >
  > *The record is written to the `.log` file descriptor and immediately followed by `file.flush()`. This flushes Python's internal buffer into the operating system page cache, making the data instantly visible to concurrent readers. However, `os.fsync()` is intentionally omitted by default. In distributed log architectures, durability is achieved through multi-broker replication, not single-machine fsync. Paying 1–10 ms per write for disk platter sync would throttle throughput from 45,000 to ~100 msgs/sec.*
  >
  > *Finally, if the bytes written since the last index entry exceed `index_interval_bytes` (4 KB), a new sparse index entry `(offset, byte_position)` is appended to the companion `.index` file, keeping index overhead to a negligible 0.2% of the log."*
- **Key mechanism to highlight**: The partition write lock preserves the golden rule: 'an offset names exactly one message'. Without it, concurrent executor threads would produce duplicate offsets. CRC32 ensures torn writes on crash are detected before being served to consumers.
- **Trap / Follow-up**: *"Why does Kafka flush to OS page cache instead of fsyncing every message?"* → `fsync` takes 1–10 ms (physical platter seek / SSD erase block sync), limiting throughput to ~100 writes/sec. Page cache takes ~1 µs (~100,000 writes/sec). In distributed systems, durability is bought with multi-node replication, not local disk fsync.

---

## 5. Roll a segment

```python
# broker/log/log.py:203
active = self.active_segment
too_big = active.size_bytes >= self.config.segment_bytes
too_old = (self.config.segment_ms > 0 and active.size_bytes > 0
           and (time.time() - active.created_at) * 1000 >= self.config.segment_ms)

if too_big or too_old:
    active.seal()
    self.segments.append(Segment.create(self.directory, active.next_offset, ...))
```

```
data/orders-0/
    00000000000000000000.log     base offset 0      sealed
    00000000000000000000.index
    00000000000000011640.log     base offset 11640  ACTIVE ← appends land here
    00000000000000011640.index
```

**20 digits, zero-padded**, so `sorted(os.listdir(...))` returns segments in
offset order with no parsing. Without padding, `"10000.log"` sorts before
`"9471.log"` and the structure silently breaks.

**`segment_ms` exists** because retention only ever deletes *sealed* segments.
A low-traffic topic whose single segment never fills would keep its data
forever regardless of policy.

Measured index overhead on a real run: **0.19% of the log**.

### How to explain this in an interview

- **The 30-second pitch**: *"Logs are split into segment files named after their 20-digit zero-padded base offset. When an active segment exceeds `segment_bytes` or `segment_ms`, it is sealed into an immutable file and a new active segment is created. Alongside each `.log` file sits a sparse `.index` mapping offset to byte position every 4 KB, consuming only ~0.2% disk overhead."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"A partition log is not a single monolithic file; it is a directory of segmented files. Flow 5 manages segment rolling in `Log._maybe_roll()`.*
  >
  > *Each segment consists of two companion files: `00000000000000000000.log` (storing raw records) and `00000000000000000000.index` (storing sparse offset-to-byte positions). The filename is strictly formatted as a 20-digit zero-padded string representing the base offset of the first record in that segment.*
  >
  > *During every append, `_maybe_roll` checks two thresholds on the active segment: size and age. If `size_bytes >= segment_bytes` (e.g. 10 MB or 1 GB), or if `segment_ms` has elapsed since segment creation, the segment is sealed. Sealing marks the segment as immutable and closes its append file handle. A new segment is immediately created whose base offset is `active.next_offset`.*
  >
  > *Zero-padding to 20 digits is not cosmetic — it is a critical algorithmic choice. In Python, `sorted(os.listdir(partition_dir))` sorts filenames lexicographically. Without fixed-width padding, `"10000.log"` would sort before `"20.log"`, corrupting offset ordering. With 20-digit zero padding, alphabetical sort perfectly equals numeric offset sort without needing custom parsing.*
  >
  > *Furthermore, rolling by time via `segment_ms` is essential for retention. Log retention policies (like deleting data older than 7 days) only ever delete sealed segments — the active segment currently receiving appends is never eligible for deletion. On a low-throughput topic where messages arrive slowly, a segment might take months to fill by size alone; without `segment_ms`, old data would sit in the active segment and escape retention indefinitely."*
- **Key mechanism to highlight**: 20-digit zero-padded filenames allow lexical sorting (`sorted(os.listdir())`) to naturally match offset order. `segment_ms` guarantees low-traffic partitions eventually roll sealed segments so retention can clean them up.
- **Trap / Follow-up**: *"Can retention delete data from the active segment?"* → Never. Retention only purges sealed segments. If segments didn't roll by time (`segment_ms`), a trickle of data on an old partition would never be deleted.

---

## 6. Fetch messages

```
consumer.poll()
  └─ consumer.py:455  Consumer.poll()
     ├─ consumer.py:296  _ensure_joined()   only if a rebalance is pending
     └─ consumer.py:490  _fetch_one()       per assigned partition, round-robin
        └─ FETCH request
           └─ broker.py:418  _handle_fetch()
              └─ partition.py:95  Partition.fetch()
                 └─ log.py:243  Log.read()
                    ├─ log.py:278  _segment_index_containing()   binary search
                    └─ segment.py:177  Segment.read()
                       ├─ index.py:149  Index.lookup()           binary search
                       └─ record.py:134 iter_records()           short scan
```

### The read path — the whole point of Phase 2

```python
# broker/log/segment.py:177
position = self.index.lookup(start_offset)      # binary search, 8-byte entries
with open(self.log_path, 'rb') as fh:
    for _pos, _size, record in iter_records(fh, position):
        if record['offset'] < start_offset:
            continue                            # short scan, ≤ index interval
        messages.append(record)
        if len(messages) >= max_messages:
            break
```

Two binary searches and a few KB of sequential read. **Nothing in that path
depends on how much data the log holds** — measured flat at 1.07x from offset 0
to offset 17,777.

### The index contract

```python
# broker/log/index.py:149
i = bisect.bisect_right(self._offsets, relative) - 1
return self._positions[i] if i >= 0 else 0
```

**At or before, never past.** The caller scans forward, so landing early costs
microseconds while landing late would silently skip records — messages
reported missing that are sitting right there on disk.

### The guard that stops silent data loss

```python
# broker/log/log.py:243
if start_offset < self.log_start_offset:
    return []
```

Without it, asking for offset 0 after retention deleted through 295 quietly
returns records starting at 295. The consumer believes it read from the
beginning; 295 messages vanish with no error anywhere. The broker turns this
empty result into `OFFSET_OUT_OF_RANGE`
([broker.py:418](../broker/broker.py#L418)) — the honest answer.

### The second bound — the high watermark (Phase 6)

The same read path now takes an exclusive upper limit, and who is asking
decides whether it applies:

```python
# broker/broker.py — _handle_fetch
replica_id = payload.get('replica_id')
if replica_id is not None:
    self.cluster.record_replica_position(topic, part_id, replica_id, start_offset)

watermark = self.cluster.high_watermark(topic, part_id, partition.next_offset)
limit = None if replica_id is not None else watermark
messages = await self._run_blocking(partition.fetch, start_offset, max_msgs, limit)
```

Two things happen in those five lines.

**The request is the progress report.** A follower asking for offset 150 has,
by asking, proved it holds 0..149. No acknowledgement message and no reporting
API — which also means the leader learns each position one round trip late, so
the watermark is permanently a little conservative. That is the right direction
for a safety bound to be wrong in.

**Consumers are clamped and followers are not**, and the asymmetry is circular:
the act of a follower reading a record is what *makes* it safe for consumers.
Clamp followers too and nothing replicates past the watermark, so the watermark
never advances, so nothing is ever readable — a cluster deadlocked at offset 0
while looking perfectly healthy.

The clamp is a cap on how many records to read, not a filter afterwards
([log.py](../broker/log/log.py)), so a consumer sitting at the watermark does
the same small seek it always did instead of reading a batch and discarding it.

### Replication — the fetch path, used by a broker

```
ReplicaFetcher._follow()          one task per followed partition
  └─ replica.py  _fetch_once()
     ├─ local.next_offset                       ask from our own log end
     ├─ FETCH {replica_id: N}  →  the leader    unclamped, per above
     └─ partition.py  append_replicated()
        └─ log.py  Log.append_replicated()      offsets COPIED, never assigned
```

`Log.append` stamps `next_offset` onto each record, which is right for a leader
— deciding a message is number 41 is the leader's job. A follower doing the
same would be re-deciding it, and the moment two logs differ by one record
every later offset names a different message on the two machines. So
`append_replicated` copies the number, skips records at or before its own end
(a restart legitimately re-fetches them), and raises on a gap rather than
writing a log with a hole in it.

### `acks=all` — waiting for the watermark

```
_handle_produce_v1(acks='all')
  ├─ _enough_replicas()          committed ISR ∩ observed ISR
  │    └─ < min.insync           → NOT_ENOUGH_REPLICAS, BEFORE the append
  ├─ _append_records()           the write itself
  └─ _await_replication()        poll until hw > offset, else REQUEST_TIMED_OUT
```

Three things worth noticing.

**`acks=all` counts nothing.** The high watermark already means "every in-sync
replica has this", so waiting for `hw > offset` is the entire implementation.
No per-follower bookkeeping and no acknowledgement protocol — Step 3 built the
mechanism, Step 5 consults it.

**The ISR check is the intersection of committed and observed**, not either
alone. A leader cut off from the quorum cannot *commit* the news that its
followers have gone, so trusting the committed set it would keep acknowledging
writes claiming three copies while holding one. The intersection can only ever
be smaller, which is the safe direction.

**The check runs before the append.** Writing first and failing the
acknowledgement would leave the record in the leader's log, where a later ISR
recovery would replicate it — so a producer told FAILED would find its message
in the topic anyway.

### Long-polling — why an empty fetch is held, not answered

```
_handle_fetch()
  └─ no messages and max_wait_ms > 0
       └─ poll every 2 ms until data lands or the window expires
```

Without this, a caught-up follower sleeps between fetches and only discovers a
new record when it next wakes — so `acks=all` waits for the follower's *poll
interval* rather than for the network. Measured: 218 ms per message, an
`acks=all` cost of **358x**. Holding the request instead costs one parked
coroutine and returns the instant a record arrives. 358x → ~6x.

### The ISR — observed by leaders, committed by the controller

```
follower FETCH → cluster.record_replica_position()   timestamps the fetch
                                    │
leader heartbeat ── isr_report ──►  controller
                                    └─ _observed_isr()  own | reported | committed
                                       └─ raft.propose(ASSIGN_PARTITIONS)
                                          └─ cluster.set_placement()
```

Only a partition's **leader** sees its followers' fetches, and the controller
usually leads none of the partitions it is placing — so leaders report on the
heartbeat they were already sending, and a report is believed only from the
broker that actually leads that partition. The first version omitted this and
the ISR froze at the value it had before any follower first fetched.

### Round-robin across partitions

```python
# consumer/consumer.py:455
for i in range(len(assignment)):
    if len(messages) >= max_messages: break
    index = (self._next_partition_index + i) % len(assignment)
    ...
self._next_partition_index = (self._next_partition_index + 1) % len(assignment)
```

Always starting at partition 0 would starve later partitions whenever the first
has enough messages to fill `max_messages` — a consumer that silently never
reads half its assignment.

### How to explain this in an interview

- **The 30-second pitch**: *"Consumers fetch messages via a two-stage binary search: first binary searching segment base offsets to locate the target segment, then binary searching the sparse index to find the nearest file offset, followed by a short sequential scan (≤4 KB). This makes read latency flat O(1) regardless of whether the log holds 1,000 or 1,000,000 records. Consumers are clamped to the high watermark, while replica followers fetch unclamped."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When `consumer.poll()` runs, it iterates through its assigned partitions round-robin to prevent partition starvation. For each partition, it sends a `FETCH` request to the partition leader with `start_offset` and `max_messages`.
  >
  > On the broker, `_handle_fetch` resolves the segment via a two-stage binary search:
  >
  > 1. `_segment_index_containing(start_offset)` binary searches the sorted base offsets of all segments to locate the segment holding this range.
  > 2. Inside that segment, `index.lookup(start_offset)` uses binary search (`bisect_right - 1`) on the sparse index to find the largest indexed offset at or before `start_offset`, returning its byte position in the `.log` file.
  > 3. The broker seeks directly to that byte position and scans forward sequentially, verifying record lengths and CRC32 checksums until `start_offset` is reached. It reads up to `max_messages` and returns. Because the sparse index has entries every 4 KB, the sequential scan never reads more than ~4 KB of disk. This yields flat O(1) latency — measured at 1.07x seek ratio from offset 0 to 17,777 across 20,000 messages.
  >
  > In a replicated cluster (Phase 6), the broker inspects who is fetching:
  >
  > If a regular consumer is fetching, the read is strictly clamped to the **high watermark** — the highest offset replicated to all in-sync replicas (ISR). Any record above the watermark exists only on the leader and is withheld from consumers, preventing them from acting on records that a leader failover might wipe out.
  >
  > If a replica follower is fetching (passing `replica_id`), the read is unclamped! Followers must read past the watermark because follower fetching is the exact mechanism that advances the watermark. If followers were clamped, the cluster would deadlock at offset 0. Furthermore, long-polling holds empty follower fetch requests for up to `max_wait_ms` rather than returning immediately, slashing `acks=all` latency from 358x to 6x."*
- **Key mechanism to highlight**: The sparse index contract ('at or before, never past') ensures zero records are skipped. Followers pull using the exact same FETCH API, but passing `replica_id` proves follower progress without separate ack RPCs.
- **Trap / Follow-up**: *"Why are consumers clamped to the high watermark while follower replicas are unclamped?"* → Followers fetching data is the very mechanism that advances the high watermark. Clamping followers would create a circular deadlock where the watermark never moves and the cluster freezes at offset 0.

---

## 7. Join a consumer group

The rendezvous. **`JOIN_GROUP` blocks.**

```
consumer.subscribe(['orders'])
  └─ consumer.py:266  subscribe()
     ├─ consumer.py:296  _ensure_joined()
     │  ├─ JOIN_GROUP  ────────────────► broker.py:623  _handle_join_group()
     │  │                                  └─ coordinator.py:297  join_group()
     │  │                                     ├─ _expire_members()
     │  │                                     ├─ begin_rebalance()
     │  │                                     └─ await join_complete   ← HELD
     │  ├─ assignor.py:155  assign()     ← LEADER ONLY, on the client
     │  └─ SYNC_GROUP  ────────────────► coordinator.py:388  sync_group()
     ├─ consumer.py:345  _refresh_positions()   OFFSET_FETCH
     └─ consumer.py:384  _start_heartbeat()
```

### The coordinator holds the request

```python
# broker/coordinator.py:297
group.begin_rebalance()
group.joined_members.add(member_id)

if group.joined_members >= set(group.members) and group.state == PREPARING_REBALANCE:
    group.complete_join()
event = group.join_complete

if not event.is_set():
    await asyncio.wait_for(event.wait(), timeout=rebalance_timeout_ms / 1000.0)
```

The coordinator cannot answer the first member until it knows who else is
coming, because the leader needs the full member list. Real Kafka calls this
holding area **purgatory**.

This is also the clearest justification for asyncio in the project: an awaited
`Event` costs a few hundred bytes, where a thread-per-request server would park
an OS thread per member of every rebalancing group.

> **Consequence worth knowing:** a driver that polls consumers *sequentially*
> deadlocks — it enters alice's rejoin, waits for bob, and never reaches bob. A
> rendezvous needs genuinely concurrent participants. That is why
> `chaos/kill_consumer.py` gives every consumer its own thread.

### `joined_members` is separate from `members`, deliberately

A member is still a member while it is being waited for, so "have they all
rejoined?" cannot be answered from the membership dict. Getting this wrong made
the join complete on the *first* member, and the leader then computed an
assignment for a group of one — silently leaving every other consumer idle.

### The leader computes the assignment — on the client

```python
# consumer/consumer.py:296
if self.is_leader:
    subscriptions = {m['member_id']: m['topics'] for m in response['members']}
    assignment = compute_assignment(self.assignor, subscriptions,
                                    response.get('partition_counts', {}))
    sync_payload['assignments'] = {...}
```

The coordinator never decides who gets what. That is why a custom assignor —
rack-aware, weighted, sticky — ships with your application and never touches a
broker. **Membership and fencing are the broker's problem; policy is the
client's.**

```python
# consumer/assignor.py:81  range_assign
per_member, remainder = divmod(count, len(members))
start = 0
for i, member_id in enumerate(members):
    take = per_member + (1 if i < remainder else 0)
    assignment[member_id].extend((topic, p) for p in range(start, start + take))
    start += take
```

### The generation bumps at `complete_join`, not `begin_rebalance`

```python
# broker/coordinator.py:180
def complete_join(self):
    self.generation_id += 1     # once membership for this round is settled
    self.state = COMPLETING_REBALANCE
```

Bumping at the *start* was a bug: an existing member's heartbeat would then
carry a stale generation and be answered `ILLEGAL_GENERATION` ("you are a
zombie") when the truth was `REBALANCE_IN_PROGRESS` ("rejoin"). Two different
situations collapsed into the more alarming one.

Fencing is unaffected — an *evicted* member is removed from `members` entirely
and fails the membership check regardless of generation.

### How to explain this in an interview

- **The 30-second pitch**: *"Consumer group partition assignment uses a two-phase protocol: `JOIN_GROUP` and `SYNC_GROUP`. The coordinator holds join requests in purgatory until all members arrive. The first member elected as group leader computes the partition assignment on the client side, then submits it via `SYNC_GROUP` for the coordinator to distribute to followers."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Consumer group membership is orchestrated by a two-phase protocol: `JOIN_GROUP` followed by `SYNC_GROUP`.
  >
  > When an application invokes `consumer.subscribe()`, the consumer checks `_ensure_joined()`. It transmits a `JOIN_GROUP` request containing its `member_id` (initially blank on first join) and topic subscriptions.
  >
  > On the broker, `GroupCoordinator.join_group` manages the rendezvous:
  > If the group is `STABLE`, it transitions to `PREPARING_REBALANCE`. The coordinator intentionally does not answer the request immediately — it parks the connection on an asyncio `Event` (Kafka's purgatory) and waits up to `rebalance_timeout_ms` for all known group members to check in.
  >
  > Once all members have called `JOIN_GROUP` (or the timeout fires), `complete_join()` increments `generation_id` and wakes all parked coroutines.
  >
  > The response designates the first consumer to join as the **group leader** and provides it with the full roster of active members and their subscriptions. All other consumers receive a response identifying them as followers without member lists.
  >
  > The group leader executes the partition assignment algorithm locally on the client using its configured assignor (Range or Round-Robin). It then sends `SYNC_GROUP` containing the full partition assignment map to the coordinator.
  >
  > Follower consumers simultaneously send `SYNC_GROUP` with empty payloads. The coordinator stores the leader's assignment and replies to each member with only its assigned partitions.
  >
  > Decoupling assignment policy from the broker is a massive architectural win: custom partition assignment logic (rack-aware, stream joins, sticky) can be upgraded in client application libraries without touching broker servers."*
- **Key mechanism to highlight**: Computing assignment on the client keeps partition assignment policy decoupled from broker deployment — custom assignors (range, round-robin, sticky) ship with client code without broker upgrades.
- **Trap / Follow-up**: *"Why does the coordinator hold `JOIN_GROUP` requests in purgatory instead of responding immediately?"* → The group leader cannot compute a balanced assignment until the full member roster is known. Asyncio event waiting allows the broker to hold hundreds of connections in purgatory with negligible memory.

---

## 8. Commit an offset

```
consumer.commit()
  └─ consumer.py:526  commit()  →  _commit_locked()  [consumer.py:541]
     └─ OFFSET_COMMIT ─────────► broker.py:699  _handle_offset_commit()
        ├─ coordinator.py:238  _check_member()      ← the fence
        └─ offset_store.py:136  OffsetStore.commit()
           ├─ Partition.append()  to __consumer_offsets
           └─ update the in-memory map
```

### The fence, which is four lines

```python
# broker/coordinator.py:238
if member_id not in group.members:
    raise errors.UnknownMemberIdError(...)     # you were evicted
if generation_id != group.generation_id:
    raise errors.IllegalGenerationError(...)   # the world moved on
```

```
generation 7 : consumer A owns p0, commits offset 100
A pauses for a 40-second GC
coordinator times A out → generation 8 → p0 reassigned to B
B reads from 100, processes, commits 150
A wakes and commits 120, still believing it is generation 7
                                        ↑
                   REJECTED: ILLEGAL_GENERATION
```

Without the rejection, A's stale commit rewinds the group from 150 to 120 and
30 messages are silently reprocessed. **A cannot detect this itself** — from
inside a pause, no time has passed. Only the coordinator knows.

### Log first, then the map

```python
# broker/offset_store.py:136
with self._lock:
    self._partition.append(str(offset), key=offset_key(group_id, topic, partition))
    self._offsets[(group_id, topic, partition)] = offset
```

Order matters. If the broker dies between them, replay recovers the committed
value — the map is derived, so a lost update to it costs nothing. The other way
round, the map could report a position the log never recorded, and a restart
would silently rewind the group.

### Rebuilt by replay

```python
# broker/offset_store.py:98
for record in records:
    group_id, topic, partition = record['key'].split('|')
    self._offsets[(group_id, topic, int(partition))] = int(record['value'])
```

Last record per key wins — correct **only** because `__consumer_offsets` has
one partition and is therefore strictly ordered.

> **Known gap:** real Kafka *compacts* this topic. We do not, so it grows with
> the number of commits rather than live keys, and all of it is replayed at
> startup.

### Where you call `commit()` is the delivery guarantee

```python
# consumer/consumer.py:614  consume_loop
messages = self.poll()          # fetch
for m in messages:
    process_fn(m)               # do the work
if messages and auto_commit:
    self.commit()               # only now say it is done
```

```
fetch ──► commit ──► process        fetch ──► process ──► commit
                ▲                                     ▲
           crash = LOST                        crash = REDELIVERED
         (at-most-once)                        (at-least-once) ← default
```

There is no third option: a crash can always land between "did the work" and
"recorded that I did it".

### How to explain this in an interview

- **The 30-second pitch**: *"Offset commits persist consumer progress to an internal append-only topic `__consumer_offsets`. Every commit includes a `generation_id` and `member_id`. If an evicted consumer wakes up from a long GC pause and attempts to commit, the coordinator fences it with `ILLEGAL_GENERATION`, preventing stale zombies from rewinding group progress."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a consumer finishes processing a batch, it calls `consumer.commit()`. This constructs an `OFFSET_COMMIT` request containing the topic, partition, offset, `member_id`, and `generation_id`.
  >
  > On the broker, `_handle_offset_commit` executes a critical four-line fence in `_check_member`:
  >
  > 1. It verifies `member_id in group.members` (ensuring the consumer was not evicted).
  > 2. It verifies `generation_id == group.generation_id`.
  >
  > Suppose consumer A paused for a 40-second garbage collection freeze. The coordinator timed it out, bumped the generation to 8, and reassigned its partition to consumer B. Consumer B reads from offset 100 and commits offset 150. When paused consumer A wakes up, it still believes it is in generation 7 and attempts to commit offset 120. The coordinator immediately rejects the commit with `ILLEGAL_GENERATION`. Without this fence, the zombie consumer would silently overwrite and rewind the group's committed offset from 150 back to 120, causing 30 messages to be reprocessed.
  >
  > Once validated, `OffsetStore.commit` follows write-ahead log semantics: it appends the commit record to the on-disk partition log `__consumer_offsets` first, and updates the in-memory hash map second. If the broker crashes between the two steps, startup replay reads the log and reconstructs the committed map without data loss.
  >
  > The placement of `commit()` in application code governs delivery guarantees: committing before processing gives at-most-once delivery (a crash loses data); committing after processing gives at-least-once delivery (a crash duplicates messages, which is curable with idempotent handling)."*
- **Key mechanism to highlight**: Generation fencing guarantees that only the current partition owner can commit offsets. On disk, commits write to `__consumer_offsets` log first before updating the in-memory lookup table, ensuring crash recovery via replay.
- **Trap / Follow-up**: *"What is the trade-off between committing before vs after processing?"* → Committing before processing is at-most-once (crashes lose data silently). Committing after processing is at-least-once (crashes duplicate data). At-least-once is the only safe default because duplication is visible and curable via idempotency.

---

## 9. Detect a dead consumer

```
consumer.py:400  _heartbeat_loop()        own thread, own connection
  └─ HEARTBEAT ─────────► coordinator.py:434  heartbeat()
                            ├─ _check_member()
                            └─ RebalanceInProgressError if not STABLE

broker.py:986  _group_reap_loop()          every group_reap_ms
  └─ coordinator.py:516  reap_expired()
     └─ coordinator.py:257  _expire_members()
```

### Own thread, own connection — both matter

```python
# consumer/consumer.py:176
self._heartbeat_conn = SyncConnection(host, port)
```

- **Own thread** so a slow `process_fn` does not look like a dead process.
- **Own connection** because a request and its response are a pair; two threads
  on one socket eventually read each other's replies. Also stops a heartbeat
  queueing behind a slow fetch, which would defeat the point of the thread.

### The stale-notice guard

```python
# consumer/consumer.py:400
except (RebalanceInProgressError, IllegalGenerationError, UnknownMemberIdError):
    with self._state_lock:
        stale = generation_id != self.generation_id
    if not stale:
        self._need_rejoin.set()
```

A heartbeat sent *before* a rejoin completed comes back saying "rebalance in
progress" — true when sent, stale on arrival. Acting on it is catastrophic, not
merely wasteful: the rejoin starts a **brand new** rebalance on a group that had
just stabilised, which makes every other member rejoin, forever. It showed up as
members stranded in generations 7, 8 and 9, each believing it owned everything.

### Purgatory counts as liveness

```python
# broker/coordinator.py:257
parked = (group.joined_members
          if group.state in (PREPARING_REBALANCE, COMPLETING_REBALANCE)
          else frozenset())

expired = [m.member_id for m in group.members.values()
           if m.member_id not in parked and m.is_expired(now)]
```

The heartbeat thread cannot start until the join completes, so a member waiting
in the rendezvous sends nothing for the whole rebalance. Without this, the
reaper evicted members for doing exactly what the protocol told them to do.

**A member with a request parked in the coordinator is alive by definition** —
we are holding its connection open right now.

### Why the reaper is a timer

Eviction otherwise happens only when *somebody* calls in — and if every member
of a group dies at once, nobody ever calls in again. The group would sit forever
in `Stable`, holding partitions for processes that no longer exist.

**Measured:** with `session_timeout_ms=800`, a killed member's partitions are
taken over in **~1.0 s**.

### How to explain this in an interview

- **The 30-second pitch**: *"Consumer liveness is maintained by a dedicated background heartbeat thread using an independent TCP connection. If heartbeats stop for `session_timeout_ms`, the coordinator evicts the member and triggers a rebalance. Eviction is driven by a periodic background timer (`reap_expired`), taking over failed partitions in ~1.0s."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Consumer liveness is monitored through a dedicated background heartbeat loop.
  >
  > In the consumer process, `_heartbeat_loop` runs on its own OS thread using its own dedicated TCP socket connection (`_heartbeat_conn`). Running on a separate thread ensures that expensive or slow record processing in the application loop does not block heartbeats and cause false-positive evictions. Running on a separate socket ensures heartbeats never queue behind large multi-megabyte `FETCH` responses on the socket buffer.
  >
  > Every `heartbeat_interval_ms` (e.g. 300 ms), the thread sends a `HEARTBEAT` request. On the broker, `GroupCoordinator.heartbeat` verifies the member and generation, and updates the member's `last_heartbeat` monotonic timestamp. If the group is currently rebalancing, the coordinator replies with `REBALANCE_IN_PROGRESS`, prompting the consumer to rejoin.
  >
  > On the broker, a background asyncio task `_group_reap_loop` runs every `group_reap_ms` calling `coordinator.reap_expired()`. It checks if `now - last_heartbeat > session_timeout_ms`. If a member has gone silent, the coordinator removes it from `group.members` and transitions the group to `PREPARING_REBALANCE`.
  >
  > Crucially, members parked in purgatory during `JOIN_GROUP` are excluded from eviction — a consumer waiting for a rebalance to finish is active by definition.
  >
  > Having the reaper run on a periodic timer is essential: if all consumers in a group crash simultaneously, no inbound requests ever arrive again; the timer ensures abandoned partitions are reclaimed within ~1.0 second."*
- **Key mechanism to highlight**: Using a separate connection and thread for heartbeats isolates liveness from data processing. Purgatory join requests count as active liveness so rebalancing members are not prematurely evicted.
- **Trap / Follow-up**: *"What happens if a consumer's processing thread deadlocks but its heartbeat thread keeps running?"* → The heartbeat thread would keep the consumer alive indefinitely. Real Kafka introduced `max.poll.interval.ms` (checking time between `poll()` calls) to detect processing stalls independently of network heartbeats.

---

## 10. Where a consumer starts reading

Every consumer answers this before fetching anything, and the answer is **not**
"offset 0".

```
consumer.py:345  _refresh_positions()
  ├─ OFFSET_FETCH ────────► broker.py:744  _handle_offset_fetch()
  │                           └─ offset_store.py:152  OffsetStore.fetch()
  └─ per partition, if offset < 0:
     consumer.py:510  _list_offset()  →  LIST_OFFSETS (earliest | latest)
```

### −1 and 0 mean different things

```python
# broker/offset_store.py:152
return self._offsets.get((group_id, topic, partition), -1)
```

- **−1** — this group has *never committed* this partition. Apply
  `auto_offset_reset`.
- **0** — this group has genuinely committed the beginning of the log.

Collapsing them makes a brand-new group either silently replay everything or
silently skip it, depending on which default you happened to pick.

### Applying the reset policy

```python
# consumer/consumer.py:345
offset = committed.get(tp, -1)
if offset < 0:
    offset = (self._list_offset(tp, TIMESTAMP_LATEST)
              if self.auto_offset_reset == 'latest'
              else self._list_offset(tp, TIMESTAMP_EARLIEST))
    resolved[tp] = -1          # position moves; committed stays -1
else:
    resolved[tp] = offset
position[tp] = offset
```

`resolved[tp] = -1` in the reset branch is deliberate: the *position* moves,
but `committed` stays −1 because nothing has been committed. Setting it to the
resolved offset would make the next `commit()` a no-op — the consumer would
believe it had recorded a position it never wrote.

| `auto_offset_reset` | Behaviour | Risk |
|---|---|---|
| `earliest` (our default) | Process the whole history | Expensive; reprocesses |
| `latest` | Start from now | **Silently skips** everything that arrived while away |

No safe default exists, which is why Kafka makes you choose. `latest` sits
behind a surprising number of "we lost a day of events" incidents.

### An unknown topic is tolerated here

```python
# consumer/consumer.py:510
except UnknownTopicOrPartitionError:
    return 0
```

`LIST_OFFSETS` is a *query*, so unlike PRODUCE and FETCH it does not
auto-create the topic. But an empty partition starts at 0 whichever end you
asked for, so failing would force the caller to handle a distinction with no
consequence.

### How to explain this in an interview

- **The 30-second pitch**: *"When a consumer starts, it calls `OFFSET_FETCH`. If a committed offset exists, it resumes from there. If no offset was ever committed (-1), it queries `LIST_OFFSETS` and applies `auto_offset_reset`: `earliest` seeks to the log start, while `latest` seeks to the log end (the high watermark). Offset -1 and offset 0 are strictly distinct."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a consumer initializes its assigned partitions, it calls `_refresh_positions()`. It does not assume offset 0.
  >
  > First, it issues an `OFFSET_FETCH` request to the broker. `OffsetStore.fetch` looks up `(group_id, topic, partition)`. If a committed offset exists, the consumer initializes its fetch cursor to that offset and resumes consumption seamlessly.
  >
  > Second, if no committed offset exists, the store returns `-1`. The distinction between `-1` and `0` is vital: `0` means the group explicitly committed offset 0; `-1` means the group has never committed anything for this partition.
  >
  > When offset is `-1`, the consumer applies its configured `auto_offset_reset` policy:
  >
  > If `auto_offset_reset = 'earliest'`, it sends `LIST_OFFSETS` with timestamp `EARLIEST` to discover the oldest available offset (the base offset of the oldest active segment) and seeks there.
  > If `auto_offset_reset = 'latest'`, it sends `LIST_OFFSETS` with timestamp `LATEST` to discover the log end offset (the high watermark) and positions its cursor at the tail.
  >
  > Crucially, when applying `auto_offset_reset`, the consumer sets its in-memory fetch position to the resolved offset, but leaves its committed offset as `-1`. This ensures that subsequent calls to `commit()` record the position only after messages are genuinely processed.
  >
  > Finally, if a consumer attempts to fetch an offset smaller than `log_start_offset` (because log retention deleted older segments), the broker returns `OFFSET_OUT_OF_RANGE`, forcing the consumer to re-evaluate its reset policy rather than reading corrupted gaps."*
- **Key mechanism to highlight**: Differentiation between -1 (no record ever committed) and 0 (explicitly committed to the first record). If a consumer's committed offset has been purged by retention, the broker returns `OFFSET_OUT_OF_RANGE`.
- **Trap / Follow-up**: *"What is the operational danger of `auto_offset_reset=latest`?"* → If a new consumer group deploys or an existing group's offsets expire, `latest` silently jumps to the end of the log and skips all historic backlogs without error. `earliest` is safer because re-processing duplicates beats silent data omission.

---

## 11. Assignment strategies

The group leader runs one of these — on the client, never on the broker.

```python
# consumer/assignor.py:155
def assign(protocol, subscriptions, partition_counts):
    return ASSIGNORS.get(protocol, range_assign)(subscriptions, partition_counts)
```

Unknown names fall back to range rather than failing the rebalance: a group
that cannot form is worse than one balanced differently than intended.

### Range — contiguous runs, per topic

```python
# consumer/assignor.py:81
per_member, remainder = divmod(count, len(members))
start = 0
for i, member_id in enumerate(members):
    take = per_member + (1 if i < remainder else 0)
    assignment[member_id].extend((topic, p) for p in range(start, start + take))
    start += take
```

```
orders p0..p6, 3 members     A: p0 p1 p2     B: p3 p4     C: p5 p6
```

- **Co-partitioning** is why it is the default: with two topics partitioned the
  same way, member A gets `orders-0` *and* `payments-0`. If you are joining
  those streams, the data is on one consumer.
- The remainder always goes to the *earliest* members, so the same member is
  permanently busier after every rebalance.

### Round-robin — `round_robin_assign`, deal them out in turn

```python
# consumer/assignor.py:118
for topic, partition in _all_partitions(subscriptions, partition_counts):
    for _ in range(len(members)):              # bounded: one full lap
        member_id = members[index % len(members)]
        index += 1
        if topic in subscriptions[member_id]:
            assignment[member_id].append((topic, partition))
            break
```

Skew never exceeds one partition across many topics; co-partitioning is lost.

The inner loop **skips members that did not subscribe to this topic** — a group
may legitimately hold members reading different topics. Without the skip a
member is handed a partition of a topic it is not reading, and those messages
silently stop being consumed.

### The invariant both must hold

**Each partition goes to exactly one member.** Twice means duplicate
processing; zero times means a partition silently stops being read. Neither
raises anywhere, which is why `test_every_partition_assigned_exactly_once`
checks the property directly rather than any particular layout.

Sorting in `_all_partitions` ([assignor.py:64](../consumer/assignor.py#L64)) is
load-bearing: iterating a set unsorted would produce a different layout from
identical inputs on every rebalance, moving partitions for no reason.

### How to explain this in an interview

- **The 30-second pitch**: *"We implement Range and Round-Robin partition assignment on the client side. Range assigns contiguous partition blocks per topic to preserve co-partitioning (critical for stream joins). Round-robin deals partitions out across all group members to maximize balance and minimize skew across multiple topics."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When consumer group rebalancing enters `SYNC_GROUP`, the group leader executes partition assignment on the client. Our codebase implements two distinct assignor strategies: Range and Round-Robin.
  >
  > 1. **RangeAssignor**: For each subscribed topic, it divides the total partitions by the number of subscribed members (`divmod(count, len(members))`). It assigns contiguous slices of partitions to members in sorted order.
  > The primary advantage of Range is **co-partitioning**: if an application consumes two topics (e.g. `orders` and `payments`) that have identical partition counts and are keyed identically, Range assigns `orders-0` and `payments-0` to the exact same consumer process. This is mandatory for stateful stream joins.
  > The disadvantage is **partition skew**: if partition counts don't divide evenly (e.g. 3 partitions over 2 consumers), consumer 0 gets the remainder partition. Across 10 topics, consumer 0 gets 20 partitions while consumer 1 gets only 10, causing a 2x workload imbalance.
  >
  > 2. **RoundRobinAssignor**: It collates all partitions across all topics in a deterministic sorted list and deals them out one by one across all consumers. This guarantees that workload skew never exceeds 1 partition across the entire cluster. However, co-partitioning is lost. Furthermore, the assignor explicitly skips members that didn't subscribe to a given topic.
  >
  > Both assignors strictly enforce the invariant that **every partition is assigned to exactly one member**. Assigning zero times drops a partition; assigning twice duplicates processing. Both algorithms sort member IDs and partition lists before calculating to guarantee deterministic assignments that avoid churn across identical rebalance inputs."*
- **Key mechanism to highlight**: Assignment is deterministic and strictly enforced to ensure every partition is assigned to exactly one member. Both algorithms sort member IDs and partition lists first to prevent churn between rebalances.
- **Trap / Follow-up**: *"What is the main drawback of Range assignment?"* → If multiple topics have partition counts not evenly divisible by member count (e.g., 3 partitions over 2 consumers across 10 topics), member 0 consistently gets the extra partition for all 10 topics, creating a 66/33 workload imbalance.

---

## 12. Leave a group cleanly

```
consumer.close()
  └─ consumer.py:218  close()
     ├─ _stop_heartbeat()
     ├─ LEAVE_GROUP ────────► broker.py:693  _handle_leave_group()
     │                          └─ coordinator.py:459  leave_group()
     │                             ├─ del group.members[member_id]
     │                             ├─ reassign leader if it was this member
     │                             └─ begin_rebalance()   ← begin ONLY
     └─ close both connections
```

**Why an explicit API at all:** without it, a consumer shutting down normally
holds its partitions until its session times out — up to ten seconds of stalled
consumption on every deploy.

### Begin, but do not complete

```python
# broker/coordinator.py:459
if group.members:
    group.begin_rebalance()      # NOT complete_join()
else:
    group.state = EMPTY
    group.leader_id = None
```

The survivors have not rejoined yet; completing here would hand the leader a
member list nobody confirmed. They find out on their next heartbeat, which
answers `REBALANCE_IN_PROGRESS` — that is what the heartbeat is for beyond
proving liveness.

### Best effort, deliberately

```python
# consumer/consumer.py:218
except (KafkaError, BrokerConnectionError):
    pass
```

Failing to leave cleanly costs a session timeout, not correctness. Raising here
would mask whatever the caller was actually shutting down for.

### How to explain this in an interview

- **The 30-second pitch**: *"When a consumer shuts down cleanly, `close()` immediately sends a `LEAVE_GROUP` request. The coordinator removes the member and begins a rebalance instantly, avoiding the multi-second stall that would otherwise occur while waiting for `session_timeout_ms` to expire."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a consumer application shuts down gracefully, `consumer.close()` executes a clean departure sequence:
  >
  > 1. It stops the background heartbeat thread (`_stop_heartbeat()`). Cancelling the heartbeat thread *before* sending `LEAVE_GROUP` is vital: otherwise, an in-flight heartbeat packet could arrive at the broker immediately after the leave request, inadvertently resurrecting the member.
  > 2. It sends an explicit `LEAVE_GROUP` request to the coordinator.
  > 3. It closes all underlying TCP sockets.
  >
  > On the broker, `coordinator.leave_group` handles the request:
  > If the member exists, it deletes the member from `group.members`. If the departing member was the group leader, it selects the next available member as the new leader.
  >
  > Then, it invokes `group.begin_rebalance()`, transitioning the group to `PREPARING_REBALANCE`.
  >
  > Crucially, it begins the rebalance, but does NOT complete it. The coordinator must wait for surviving members to rejoin. Surviving members find out about the rebalance on their next heartbeat, which returns `REBALANCE_IN_PROGRESS`, prompting them to rejoin immediately.
  >
  > Why have `LEAVE_GROUP` at all? Without an explicit leave API, the coordinator has no way of knowing a consumer stopped; it must wait for the consumer's `session_timeout_ms` (e.g. 10–30s) to expire before triggering a rebalance. On every rolling deploy, an explicit `close()` avoids tens of seconds of halted consumption.
  >
  > Notice also that `close()` wraps `LEAVE_GROUP` in best-effort exception suppression (`try/except: pass`): if the network is broken during shutdown, the consumer must terminate cleanly without hanging."*
- **Key mechanism to highlight**: The coordinator begins rebalance but does *not* complete it immediately (`begin_rebalance()`, not `complete_join()`). Surviving members discover the rebalance on their next heartbeat (`REBALANCE_IN_PROGRESS`) and rejoin.
- **Trap / Follow-up**: *"Why is the LEAVE_GROUP call wrapped in best-effort exception suppression (`try/except: pass`)?"* → If network failure prevents `LEAVE_GROUP` from reaching the broker, the consumer should still terminate cleanly. The coordinator will evict it anyway when its session timeout expires.

---

## 13. Manual assignment, and the thread guard

```
consumer.assign([('orders', 2)])
  └─ consumer.py:281  assign()
     ├─ self.subscription = []          ← no group
     └─ consumer.py:345  _refresh_positions()
```

That is the entire flow. No JOIN_GROUP, no SYNC_GROUP, no heartbeat thread, no
generation.

| | `subscribe()` | `assign()` |
|---|---|---|
| Who chooses partitions | coordinator | you |
| Failover if you die | yes | **no** |
| Zombie fencing on commit | yes | **no** |
| Rebalance pause | yes | no |
| Heartbeat overhead | yes | no |

The commit path reflects it:

```python
# consumer/consumer.py:541
if self.subscription and self.member_id:
    payload['member_id'] = self.member_id
    payload['generation_id'] = self.generation_id
```

No membership means nothing to fence against, so the fields are absent and
`_handle_offset_commit` skips the check. **You trade protection for control**,
which is the right call for a partition-repair tool or a debugging reader.

### The thread guard — `_exclusive`

```python
# consumer/consumer.py:251  _exclusive()
if not self._access_lock.acquire(blocking=False):
    raise RuntimeError(
        f"Consumer is not thread-safe: {operation}() was called while "
        f"another thread was still using this consumer. ...")
```

Wrapped around `subscribe`, `assign`, `poll` and `commit`.

**It refuses rather than serialising, and that is the point.** A lock that made
concurrent use "work" would hide a design error: two threads sharing a consumer
also share its `position` and `assignment`, so even a perfectly safe socket
would produce nonsense.

> Contrast the producer, which *does* use a lock
> ([producer.py:304](../producer/producer.py#L304)) because its sender thread
> legitimately shares the socket. The right answer depended on whether the
> sharing was legitimate — not on a rule about locks.

### How to explain this in an interview

- **The 30-second pitch**: *"Kafka provides two consumption models: dynamic group coordination via `subscribe()` and static manual partition assignment via `assign()`. They are strictly mutually exclusive. Manual assignment bypasses the coordinator, heartbeat threads, rebalances, and generation fencing, giving the developer raw offset control."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Kafka offers two mutually exclusive consumption modes: `subscribe()` (dynamic group management) and `assign()` (static manual partition assignment).
  >
  > When an application calls `consumer.assign([('orders', 2)])`, it explicitly claims ownership of specific partitions:
  > It sets `self.subscription = []`, signifying that no group coordinator is used. It invokes `_refresh_positions()` to resolve starting offsets, and begins polling.
  >
  > Notice what is absent: there is no `JOIN_GROUP`, no `SYNC_GROUP`, no background heartbeat thread, and no `generation_id`. When committing offsets in manual mode, `member_id` and `generation_id` are omitted; the broker's `_handle_offset_commit` skips member validation and applies the commit directly.
  >
  > The trade-off is clear: you gain absolute control (zero rebalance pauses, zero heartbeat network traffic, deterministic assignment), but you sacrifice fault tolerance (if the consumer dies, no other consumer automatically takes over its partitions). Manual assignment is designed for specialized partition-repair utilities, ETL batch extractors, or stream processors (like Flink) that manage partition assignment in their own stateful coordinator.
  >
  > To prevent concurrency bugs, `Consumer` guards `subscribe`, `assign`, `poll`, and `commit` with `_exclusive`: a non-blocking lock (`acquire(blocking=False)`). If another thread is already inside the consumer, it immediately raises `RuntimeError`.
  >
  > Why refuse with an error instead of using a mutex to serialize concurrent calls? Because a consumer maintains mutable internal cursor state (`position` and `assignment`). Two application threads sharing one consumer would interleave reads and offset commits, creating chaotic non-deterministic bugs. Refusing immediately forces the developer to fix the concurrency architecture."*
- **Key mechanism to highlight**: The `_exclusive` non-blocking thread guard raises `RuntimeError` immediately if multiple threads invoke consumer methods simultaneously. Consumers are strictly not thread-safe by design because they track mutable internal read cursors.
- **Trap / Follow-up**: *"Why does the consumer reject concurrent threads with an error rather than using a mutex to serialize them?"* → Serializing calls with a lock would mask a fatal application bug: two threads sharing one consumer share its partition offsets and assignment, causing chaotic interleaving. Refusing immediately exposes the design flaw.

---

## 14. Rebalance edge cases

Three ways a rebalance does not go to plan. Each would otherwise wedge a group
permanently.

### A member never rejoins

```python
# broker/coordinator.py:297
except asyncio.TimeoutError:
    if group.state == PREPARING_REBALANCE:
        for absent in set(group.members) - group.joined_members:
            del group.members[absent]
        if group.leader_id not in group.members:
            group.leader_id = next(iter(group.members), None)
        group.complete_join()
```

Waiting forever would let one dead consumer freeze the whole group. Dropping
the absentees is essential rather than tidy: leave them in and the leader
assigns partitions to a consumer that is not listening, and **those partitions
stop being read with nothing reporting it**.

### The leader dies before uploading the assignment

```python
# broker/coordinator.py:388
except asyncio.TimeoutError:
    self._group(group_id).begin_rebalance()
    raise errors.RebalanceInProgressError(
        f"the leader of group {group_id} did not send an assignment; rejoining")
```

Followers waiting on `sync_complete` would otherwise wait forever with no
assignment. Forcing another round is the only exit.

### Every member dies at once

```python
# broker/broker.py:986  _group_reap_loop
await asyncio.sleep(interval)
evicted = await self.coordinator.reap_expired()
```

Eviction otherwise happens only when *somebody* calls in — and if everyone is
gone, nobody ever does. The group would sit in `Stable` forever holding
partitions for processes that no longer exist, and `kfk groups describe` would
show a healthy group consuming nothing.

### A stale reply lands after a rejoin

Covered in [§9](#9-detect-a-dead-consumer), but it belongs on this list: acting
on an obsolete `REBALANCE_IN_PROGRESS` starts a *new* rebalance on a group that
had just stabilised, and the group never converges.

### How to explain this in an interview

- **The 30-second pitch**: *"Rebalances handle three critical failure modes: slow members that fail to rejoin within `rebalance_timeout_ms` (they are dropped and the group completes without them), leaders that die before submitting assignments (the coordinator times out and forces a new rebalance), and whole-group death (cleaned up by a periodic background reaper)."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"A consumer group rebalance is a distributed rendezvous prone to timing edge cases. Our coordinator explicitly handles four failure scenarios:
  >
  > 1. **A member never rejoins**: When a rebalance begins, the coordinator waits up to `rebalance_timeout_ms` for all members in `group.members` to call `JOIN_GROUP`. If a consumer crashes or network-partitions during the rebalance window, the timeout expires. The coordinator does not hang forever — it removes the absent members from `group.members`, reassigns the group leader if the leader went missing, and calls `complete_join()`. The rebalance finishes with the surviving members. If it didn't drop absent members, partitions would be assigned to a dead consumer and sit unconsumed indefinitely.
  > 2. **The leader dies before uploading the assignment**: After `JOIN_GROUP` finishes, the coordinator waits for the elected group leader to upload partition assignments via `SYNC_GROUP`. If the leader process crashes or pauses before sending `SYNC_GROUP`, follower consumers waiting in purgatory would deadlock. The coordinator enforces a timeout: when the leader times out, it aborts the rendezvous and invokes `begin_rebalance()`, kicking off a new election cycle where another member becomes leader.
  > 3. **Every member dies simultaneously**: If power is cut to an entire consumer rack, all members die at once. Because no incoming heartbeats arrive, passive request-driven eviction would never trigger. The broker runs a periodic timer loop `_group_reap_loop` that sweeps dead members and transitions the group to `EMPTY`.
  > 4. **Stale heartbeat reply after rejoin**: If a delayed heartbeat packet sent during generation 7 arrives at the client after generation 8 has already stabilized, the response says `REBALANCE_IN_PROGRESS`. The client checks whether its current generation matches the generation when the heartbeat was sent; if stale, it ignores the signal, preventing an infinite cascading rebalance storm."*
- **Key mechanism to highlight**: Dropping non-responsive members at timeout expiry is vital — if the coordinator waited forever, a single stalled process would freeze the entire consumer group indefinitely.
- **Trap / Follow-up**: *"What is the 'stop-the-world' cost of an eager rebalance protocol?"* → In eager rebalancing, all consumers revoke their partitions and pause processing while waiting in purgatory. For large clusters, cooperative sticky rebalancing allows unaffected partitions to continue processing during the rebalance.

---

## 15. Observe a group

```
kfk groups describe --group billing
  └─ tools/kfk.py  cmd_groups_describe()
     ├─ DESCRIBE_GROUPS ──► broker.py:803  _handle_describe_group()
     │                        └─ coordinator.py:491  describe()
     ├─ OFFSET_FETCH  ────► broker.py:744  (every partition for the group)
     └─ METADATA      ────► broker.py:512  (high watermarks)
```

Three requests, because lag is a **join** across three sources: who owns what,
where they have committed, and where the log ends.

```python
# tools/kfk.py  cmd_groups_describe
lag = max(0, end - current) if current >= 0 else end
```

```
  TOPIC                PART   CURRENT       END     LAG  OWNER
  orders                  0       100       501     401  (unassigned)
  orders                  1         0       495     495  (unassigned)

  total lag: 1,900
  ⚠ 4 partition(s) committed but not currently assigned — nobody is reading them
```

That warning is the useful part. A partition with committed offsets and no
owner means the group has stopped consuming it — invisible in a plain lag
number, which would just keep climbing.

### `kfk groups list` — `LIST_GROUPS`

```python
# broker/broker.py:776  _handle_list_groups
for group_id in await self.coordinator.list_groups():   # live groups
    ...
for group_id in self.offsets.groups():                  # plus historic ones
    live.setdefault(group_id, {'state': 'Empty (has offsets)', ...})
```

The union of two sources, deliberately. A group whose members have all gone
away still has committed positions; listing only live groups would hide it —
and that is precisely the group you go looking for after a shutdown.

### A group with no live members is still a group

```python
# broker/broker.py:803
except errors.UnknownMemberIdError:
    if group_id in self.offsets.groups():
        described = {'group_id': group_id, 'state': 'Empty', ...}
    else:
        raise
```

Every consumer shutting down is normal, and the committed offsets still exist.
Erroring would make "where did my consumer get to?" unanswerable after a
shutdown — exactly when you want to look.

### `FIND_COORDINATOR` — trivial today, load-bearing tomorrow

```python
# broker/broker.py:609
return {'error_code': errors.NONE, 'node_id': self.node_id,
        'host': self.host, 'port': self.port}
```

Always "me", because there is one broker. The API exists now so that **Phase 5
adds a cluster rather than a protocol change** — real Kafka hashes the group id
to choose a coordinator, spreading group state across brokers.

### How to explain this in an interview

- **The 30-second pitch**: *"Consumer group monitoring joins data across three separate sources: `DESCRIBE_GROUPS` (active members and assigned partitions), `OFFSET_FETCH` (committed offsets), and `METADATA` (log end offsets). Consumer lag is calculated per partition as `log_end_offset - committed_offset`."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Monitoring consumer group health via `kfk groups describe --group billing` joins operational metrics across three distinct APIs:
  >
  > 1. `DESCRIBE_GROUPS` (API key 23): Queries the broker's `GroupCoordinator` to retrieve active group state (`Stable`, `PreparingRebalance`, `CompletingRebalance`, `Empty`), current `generation_id`, the group leader, and the list of active members with their assigned topic-partitions.
  > 2. `OFFSET_FETCH` (API key 9): Queries `OffsetStore` to fetch the committed offset for every partition owned by the group.
  > 3. `METADATA` / `LIST_OFFSETS` (API key 3): Queries partition leaders to get the current **log end offset** (the high watermark) for each partition.
  >
  > The CLI joins these three datasets to calculate **consumer lag**:
  > `lag = max(0, log_end_offset - committed_offset)`
  >
  > Consumer lag is the single most critical operational metric in stream processing. A consumer can report healthy CPU, normal memory, and zero exceptions while quietly falling hopelessly behind. Lag directly measures the unprocessed message backlog.
  >
  > The CLI highlights a crucial warning: **committed but unassigned partitions**. If a partition has committed offsets recorded in `__consumer_offsets` but is not listed in any member's current assignment, it means consumption on that partition has stalled completely.
  >
  > Furthermore, `kfk groups list` joins active in-memory groups from `GroupCoordinator` with historical groups stored in `OffsetStore`. Even if every consumer in a group has shut down and the group state is `Empty`, its committed offsets remain intact and discoverable."*
- **Key mechanism to highlight**: Identifying unassigned partitions with committed offsets flags dead or neglected topic partitions where messages are accumulating with nobody consuming them. Listing groups queries both active memory state and historic `__consumer_offsets`.
- **Trap / Follow-up**: *"Why is consumer lag the primary SLA metric rather than latency or CPU?"* → A consumer process can show low CPU usage and zero errors while quietly falling further behind incoming traffic. Consumer lag measures actual backlog size and processing deficit directly.

---

## 16. Retention (deleting old data)

```
broker.py:1014  _retention_loop()          every retention_check_ms
  └─ partition.py:147  delete_old_segments()
     └─ log.py:306  Log.delete_old_segments()
        └─ segment.py:341  Segment.delete()
```

```python
# broker/log/log.py:306
if self.config.retention_ms > 0:
    cutoff = time.time() - (self.config.retention_ms / 1000.0)
    while len(self.segments) > 1:                 # never the active segment
        oldest = self.segments[0]
        if oldest.largest_timestamp() >= cutoff:
            break
        oldest.delete()
        deleted.append(self.segments.pop(0).base_offset)
```

**Delete files, never records.** Removing individual records means rewriting a
file, which destroys the sequential-append property the whole design rests on.

Two consequences worth stating:

- **Granularity.** Retention is only as precise as one segment: a record can
  outlive `retention_ms` by however long its segment took to fill. Real Kafka
  behaves identically, and it surprises people expecting per-record expiry.
- **`len(self.segments) > 1`** — you never delete the file you are writing to.

`largest_timestamp()` ([segment.py:207](../broker/log/segment.py#L207)) caches
its answer **once the segment is sealed**. Sealed means immutable, so the cache
cannot go stale; without it the cleaner's cost was proportional to the whole
log, which is exactly what segments exist to avoid.

### How to explain this in an interview

- **The 30-second pitch**: *"Data retention runs periodically in a background cleaner loop. It evaluates sealed segments against `retention_ms` and `retention_bytes`, deleting entire files from disk. Retention deletes complete segment files, never individual records, preserving sequential disk I/O and zero-seek performance. The active segment is never deleted."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Log retention runs as a background task `_retention_loop` every `retention_check_ms`. It delegates to `Partition.delete_old_segments()`, which calls `Log.delete_old_segments()`.
  >
  > The retention engine evaluates sealed segments against configured limits: `retention_ms` (time-based) and `retention_bytes` (size-based).
  >
  > The critical architectural principle is: **delete complete segment files, never individual records**.
  > Removing individual records would require rewriting `.log` and `.index` files on disk, turning sequential O(1) appending into expensive random-access rewrites and destroying zero-copy sequential performance.
  >
  > The loop inspects segments from oldest to newest with two strict rules:
  >
  > 1. It loops while `len(self.segments) > 1`: the **active segment is never deleted**, ensuring the write head always has an open file to append into.
  > 2. It calls `oldest.largest_timestamp()`. Once a segment is sealed, this timestamp is cached in memory, making retention checks O(1) without touching disk.
  >
  > When an expired segment is identified, `Segment.delete()` executes: it closes open file handles and unlinks both `.log` and `.index` from disk.
  >
  > Finally, `Log` advances `self.log_start_offset` to the base offset of the new oldest segment. This offset bound ensures that if a client requests an offset that was purged by retention, the broker immediately returns `OFFSET_OUT_OF_RANGE` rather than quietly serving missing data gaps."*
- **Key mechanism to highlight**: Granularity is by segment, not by record. Deleting records in place would force random writes and file rewrites, destroying append performance. Cached `largest_timestamp()` on sealed segments ensures cleanup evaluation is O(1) per segment, not O(N) over all records.
- **Trap / Follow-up**: *"What happens if a topic receives so few messages that its active segment never rolls?"* → Without `segment_ms`, an active segment that never reaches `segment_bytes` would stay open forever and its records would never be deleted regardless of `retention_ms`. `segment_ms` guarantees segments roll by time.

---

## 17. Crash recovery

```
Log.__init__()
  └─ log.py:112  _load()
     ├─ sorted(os.listdir())          20-digit names → offset order, free
     ├─ sealed segments:  size = file size, next = next segment's base
     └─ ACTIVE segment:   segment.py:238  recover()
        └─ record.py:134  iter_records()   stops at the first bad record
```

### Only the active segment is scanned

```python
# broker/log/log.py:112
if is_active:
    rebuild = not os.path.exists(segment.index_path)
    self.corrupt_bytes_recovered += segment.recover(rebuild_index=rebuild)
    segment.open_for_append()
else:
    segment.size_bytes  = os.path.getsize(segment.log_path)
    segment.next_offset = base_offsets[i + 1]
```

Sealed segments are immutable, so only the active one can have been mid-write.
On a 500 GB partition that is the difference between seconds and hours.

### Repair, not skip

```python
# broker/log/segment.py:238
for position, size, record in iter_records(fh):
    valid_end = position + size

truncated = file_size - valid_end
if truncated > 0:
    with open(self.log_path, 'r+b') as fh:
        fh.truncate(valid_end)
    self.index.truncate_after(valid_end)
```

`iter_records` stops at the first record whose length is impossible or whose
CRC fails. Everything after is unrecoverable **by definition** — the length
field that would say where the next record starts is exactly what we cannot
trust — so it is cut off.

Phase 1 could only *skip* a bad line and leave it in the file forever, with the
next append landing after it: permanent garbage in the middle of the log.

**Verified by `chaos/corrupt_log.py`** across three damage types, including one
that Phase 1 would have served as real data.

### How to explain this in an interview

- **The 30-second pitch**: *"On broker startup, crash recovery loads partition logs in O(1) time. All sealed segments are trusted based on their immutable file length. Only the final active segment is scanned record-by-record using CRC32 checksums. If a torn write is found at the tail (e.g. from an abrupt crash or power cut), the log and index are truncated to the last intact record."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a broker restarts after a crash or ungraceful shutdown, `Log._load()` initializes all partition logs.
  >
  > Crash recovery is designed to be O(1) with respect to total data volume:
  >
  > 1. It reads the partition directory with `sorted(os.listdir())`. Because segment files are named using 20-digit zero-padded base offsets, sorting gives exact offset order with zero disk reads.
  > 2. For all older, **sealed segments**, the broker trusts their immutable state. It reads file sizes via `os.path.getsize` and assigns `next_offset` as the base offset of the subsequent segment. It does NOT re-scan sealed segments. On a 500 GB drive, this avoids hours of scanning on startup.
  > 3. Only the final **active segment** is scanned via `segment.recover()`. Because an abrupt crash or power failure can only corrupt in-flight appends at the active write head, only the active segment can contain torn writes.
  >
  > In `recover()`, `iter_records` reads records sequentially. For each record, it validates the 4-byte length prefix and verifies the 4-byte CRC32 checksum against the payload.
  >
  > If a torn write or corrupted byte is encountered, scanning stops immediately. The code truncates the `.log` file at `valid_end` using `fh.truncate(valid_end)` and trims the companion `.index` file to match.
  >
  > Repairing by truncation rather than skipping is mathematically mandatory: if a record length header is corrupted, you cannot know where the next record begins. Truncating the corrupted tail restores log integrity and resets `next_offset` so subsequent appends cleanly resume."*
- **Key mechanism to highlight**: Repair by truncation rather than skipping. If a torn write occurs, the record length field cannot be trusted to locate subsequent records, so truncating the tail is the only provably safe recovery.
- **Trap / Follow-up**: *"Why not fsync every record instead of relying on tail truncation during crash recovery?"* → Fsyncing every record reduces produce throughput by roughly 100x–1000x due to disk I/O latency. Flushing to page cache and repairing the tail on startup gives maximum write speed with corruption safety.

---

## 18. How an error travels

```
broker raises/returns          →  wire                →  client raises
─────────────────────────────────────────────────────────────────────
errors.NotLeaderError          →  {'error_code': 6}   →  NotLeaderError
coordinator._check_member()    →  {'error_code': 22}  →  IllegalGenerationError
Log.read() guard               →  {'error_code': 1}   →  OffsetOutOfRangeError
```

### On the broker

```python
# broker/broker.py:219 — one translation layer for every group API
try:
    return await handler(payload)
except errors.KafkaError as exc:
    return _error(exc.error_code, str(exc))
```

```python
# broker/broker.py:1092
def _error(error_code, message):
    return {'error_code': error_code, 'error_message': message}
```

Both fields, for different readers: the **code** is for the client's logic, the
**message** is for the human reading the log afterwards.

### On the client

```python
# common/errors.py — raise_for_error
error_code = response.get('error_code', NONE)
if error_code != NONE:
    raise error_for_code(error_code, response.get('error_message', ''), **context)
return response
```

Every client call routes through this, so error handling cannot be accidentally
skipped at one call site — the most common way error handling rots.

### The only question that matters

```
KafkaError
├── RetriableError                     ← except THIS to retry
│   ├── NotLeaderError (6)             refresh metadata, resend
│   ├── RebalanceInProgressError (27)  rejoin the group
│   ├── IllegalGenerationError (22)    rejoin; discard in-flight work
│   └── UnknownTopicOrPartitionError (3)
└── (non-retriable)
    ├── OffsetOutOfRangeError (1)      the data is gone; apply reset policy
    ├── InvalidRequestError (42)       retrying resends the same bad bytes
    └── UnsupportedVersionError (35)
```

```python
# producer/producer.py:304
except (RetriableError, BrokerConnectionError) as exc:
    if attempt >= self.retries:
        raise
    delay = min(self.retry_backoff_ms * (2 ** attempt), self.max_backoff_ms)
    delay *= 0.5 + random.random()      # jitter
```

The **jitter** is the part people leave out. Without it, every producer that
failed at the same instant retries at the same instant, and the broker that
just recovered is knocked over by a synchronised thundering herd.

The hierarchy is shaped by **what you do about it**, not by what went wrong —
which is why one `except RetriableError` covers every case, and why new
retriable errors added in later phases work with existing retry loops for free.

### How to explain this in an interview

- **The 30-second pitch**: *"Errors are transmitted as numeric error codes matching Apache Kafka's protocol specification. The client maps them into an exception hierarchy categorized by whether the error is retriable (e.g., `NOT_LEADER_OR_FOLLOWER`, `REBALANCE_IN_PROGRESS`) or non-retriable (e.g., `INVALID_REQUEST`). Retriable errors automatically trigger exponential backoff with randomized jitter."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Error propagation is strictly structured across the network boundary:
  >
  > 1. On the broker, every request handler is wrapped in a unified exception boundary. Handlers raise specific Python exceptions (e.g. `NotLeaderError`, `IllegalGenerationError`, `OffsetOutOfRangeError`). The boundary catches `KafkaError` and formats a standard wire response: `{'error_code': int, 'error_message': str}`. The numeric code is for machine logic; the string message is for human debugging.
  > 2. On the client, all responses pass through `common/errors.py:raise_for_error()`. If `error_code != NONE`, it looks up the code and raises the corresponding typed Python exception. This ensures call sites cannot accidentally ignore error responses.
  > 3. The exception hierarchy is split by remediation action rather than root cause:
  > - **`RetriableError`**: Errors representing transient cluster state changes (e.g. `NOT_LEADER_OR_FOLLOWER` (6), `REBALANCE_IN_PROGRESS` (27), `UNKNOWN_TOPIC_OR_PARTITION` (3), `REQUEST_TIMED_OUT` (7)). When caught, the client refreshes metadata and retries.
  > - **Non-retriable errors**: Errors where retrying would repeat the identical failure (e.g. `OFFSET_OUT_OF_RANGE` (1), `INVALID_REQUEST` (42), `CORRUPT_MESSAGE` (2)). These bubble immediately to application code.
  >
  > In client retry loops, retries use exponential backoff (`backoff * 2^attempt`) capped at `max_backoff_ms`, multiplied by a randomized **jitter** factor (`0.5 + random()`).
  > Jitter is critical: without jitter, if a leader restarts and fails requests from 50 producers simultaneously, all 50 producers retry at the exact same millisecond, creating a synchronized thundering herd that knocks the recovering leader back offline."*
- **Key mechanism to highlight**: The distinction between retriable and non-retriable errors is the foundational primitive for client self-healing. Jitter on exponential backoff prevents thundering herd catastrophes where recovering brokers are knocked offline by synchronized retry spikes.
- **Trap / Follow-up**: *"Why is `UNKNOWN_TOPIC_OR_PARTITION` classified as retriable?"* → During topic creation or cluster partition rebalancing, metadata propagation has a small propagation lag across brokers. A client writing to a newly created topic might reach a broker a few milliseconds before its controller update arrives; retrying allows metadata to settle.

---

## Reading order, if you want to follow one thing end to end

**A message's whole life:** §3 → §4 → §5 → §6.

**Failure handling:** §12 → §9 → §11.

**What makes it distributed:** §2 → §3 (routing) → §7 (assignment).

---

# Part 2 — Cluster flows (Phases 5 and 6)

Flows 1–18 are everything a **single broker** does. These are what happens once
there are several, and they are where the interesting failures live.

---

## 19. Cluster startup and membership

```
python -m broker.broker --node-id 2 --cluster cluster.json
  └─ broker.py  Broker.__init__()
     ├─ ClusterConfig.load()          the static roster — this IS discovery
     ├─ ClusterState(node_id, ...)    liveness, leader map, replica sets, ISR
     ├─ Controller(cluster, metadata) placement
     ├─ RaftNode(node_id, voters...)  the quorum
     └─ ReplicaFetcher(self)          follower loops
  └─ Broker.start()
     ├─ raft.start()                  election timer begins
     ├─ replicas.start() + reconcile()
     └─ _membership_loop()            forever
```

### The membership tick — the whole cluster mechanism in one loop

```python
# broker/broker.py — _membership_loop
while True:
    await asyncio.sleep(interval)
    await self._heartbeat_peers()          # who answers?
    self._sync_metadata_from_raft()        # mirror committed topics locally
    elected = self.cluster.set_controller(self.raft.leader_id)
    if self.cluster.is_controller and await self.controller.recompute():
        await self._push_controller_update()
    self.replicas.reconcile()              # start/stop follower loops
```

Five things, in an order that matters. Liveness first, because placement
depends on it. Leadership is **read from Raft**, not computed — that one line
is what replaced Phase 5's `min(live_broker_ids)` and fixed split-brain.

### Heartbeats are a full mesh, and must not wait

```python
# broker/broker.py — _heartbeat_peers
for peer in peers:
    if peer.node_id in self._heartbeats_inflight:
        continue                            # already talking to it
    self._heartbeats_inflight.add(peer.node_id)
    self._spawn(self._heartbeat_and_record(peer))
```

Every broker heartbeats every other broker, so each holds its own liveness
opinion. There is no central health checker — which is exactly what
`chaos/split_brain.py` exploits.

**The round is fired off, never awaited.** `gather` finishes with the slowest
peer, and a dead peer is the slowest possible peer: it costs a full 2 s connect
timeout against a 700 ms session timeout, so a healthy peer would be refreshed
only every 2 s and spend most of its life "expired". That was journal #26 —
`live_brokers` flapping `{1}` ↔ `{1,2}` once a second on a cluster with nothing
wrong with it.

### How to explain this in an interview

- **The 30-second pitch**: *"Brokers form a cluster from a static seed roster `cluster.json`. Each broker executes a background membership loop that heartbeats all peers in a full mesh. Liveness is tracked locally in `ClusterState`. Heartbeats are launched concurrently without awaiting the slowest peer, and socket connection failures immediately mark peers as down."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When a broker starts with `--cluster cluster.json`, it transitions from a standalone broker to a clustered peer:
  >
  > 1. `ClusterConfig.from_file` parses the static cluster seed roster, mapping `node_id -> (host, port)`.
  > 2. `ClusterState` initializes local views of cluster membership, partition leadership, replica sets, and In-Sync Replicas (ISR).
  > 3. The broker starts its Raft subsystem (`RaftNode`) and launches `_membership_loop()`, which runs continuously every interval.
  >
  > Inside the membership loop, the broker executes five operations:
  >
  > First, it calls `_heartbeat_peers()`. Every broker heartbeats all other peers in a full mesh. To prevent a single dead broker from hanging the loop, heartbeats are launched concurrently (`_spawn`) without awaiting the round. A dead node's 2-second connection timeout runs in the background, allowing healthy peers to refresh without delay. If a connection is actively refused, the peer is marked down immediately (`mark_down`).
  >
  > Second, it synchronizes topic metadata committed by the Raft quorum.
  > Third, it queries Raft for the current leader ID and records it in `ClusterState.set_controller()`.
  > Fourth, if this broker is the elected Raft leader, it executes `controller.recompute()`: it computes partition-to-broker assignments and proposes changes to Raft. If committed, it broadcasts `CONTROLLER_UPDATE` RPCs to all peers.
  > Fifth, `replicas.reconcile()` starts or stops follower replication loops based on whether this broker leads or follows each partition.
  >
  > This loop embodies the core principle: **liveness is a local opinion, but controller authority is a quorum truth**."*
- **Key mechanism to highlight**: Asynchronous fire-and-forget peer heartbeats prevent a single dead node from pacing the entire loop at its 2-second connection timeout, which previously caused healthy peers to falsely expire and flap cluster membership.
- **Trap / Follow-up**: *"Why is cluster membership a local opinion rather than a global fact?"* → In distributed systems, a node cannot remotely determine whether a peer is dead or simply partitioned away. Each node maintains its own belief (`_last_seen` timestamp); consensus (Raft) is what reconciles these divergent views into authoritative decisions.

---

## 20. Electing a controller (Raft)

```
no heartbeat within election_timeout
  └─ raft/node.py  _begin_election()
     ├─ state.set_term(term+1, voted_for=self)   DURABLE, before asking anyone
     ├─ _reset_election_timer()
     ├─ check: am I already a majority?          (a 1-voter quorum is)
     └─ for each peer: _spawn(_collect_vote())   ← spawn, never gather
        └─ REQUEST_VOTE (api 52)
           └─ raft/node.py  handle_request_vote()
              ├─ term < current            → reject
              ├─ _observe_term()           → adopt a higher term, step down
              ├─ can_vote_for(candidate)   → one vote per term
              ├─ log.is_up_to_date()       → THE ELECTION RESTRICTION
              └─ state.record_vote()       DURABLE, before replying
     └─ on a majority: _become_leader() → immediate AppendEntries
```

### Three rules, and what each one buys

```python
# broker/raft/node.py
votes_needed = len(self.voters) // 2 + 1
```

A majority of **all** voters, not of those reachable. Two disjoint majorities
of one set cannot exist, so two leaders in one term cannot exist. That is
arithmetic, not best effort — and the price is that the minority side goes
quiet even though its machines are healthy.

```python
timeout = self.election_timeout_ms * (1.0 + random.random())
```

Randomised, or every node becomes a candidate at the same instant, splits the
vote, and times out together — forever.

```python
# broker/raft/log.py  is_up_to_date
if other_last_term != self.last_term:
    return other_last_term > self.last_term
return other_last_index >= self.last_index
```

The election restriction: a candidate whose log is behind cannot win, which is
what makes committed entries permanent. Compare the last **term** first — a
longer log is not automatically better, because a node can accumulate many
uncommitted entries in a term that never committed.

### Counting votes as they arrive, never gathering

`_collect_vote` promotes the moment a majority exists, and `_begin_election`
**spawns** the canvass rather than awaiting it. Both were journal #24: with
`gather`, two survivors of three would each sit for a dead peer's full timeout
while their own election timers expired underneath them, then discard a vote
that had been *granted* because the term had moved on. Terms climbing forever,
no leader, with a healthy majority available.

### How to explain this in an interview

- **The 30-second pitch**: *"To prevent split-brain partition leadership, the controller is elected using Raft consensus. When a broker's randomized election timer expires, it increments `currentTerm`, votes for itself, and requests votes from peers. To win, a candidate must secure votes from a strict majority of ALL voters (`len(voters)//2 + 1`) and satisfy the election log up-to-date restriction."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"In Phase 5, controller election was naively `min(live_broker_ids)`. Under a network partition, both sides had different views of who was alive, electing two controllers and causing split-brain. We replaced this with Raft consensus (`broker/raft/`):
  >
  > 1. **Election Timeout**: Each broker runs an election timer randomized between `T` and `2T` (e.g. 1500–3000 ms). Randomization is crucial: if timers were identical, all nodes would become candidates simultaneously, split the vote evenly, and time out indefinitely.
  > 2. **Becoming Candidate**: When the timer fires without receiving heartbeats, the node increments `currentTerm` and votes for itself. It persists `currentTerm` and `votedFor` to disk (`state.set_term()`) using `fsync` *before* sending any RPCs, ensuring that even after a crash, a node never votes twice in the same term.
  > 3. **Requesting Votes**: The candidate spawns `REQUEST_VOTE` RPCs to all peers. It does not wait for all replies (`spawn`, never `gather`); it promotes to leader the exact millisecond a majority is reached.
  > 4. **Voter Evaluation**: When a peer receives `REQUEST_VOTE`, it evaluates:
  > - Term check: If candidate term < currentTerm, reject. If candidate term > currentTerm, update currentTerm and step down to follower.
  > - Vote check: Has this node already voted for someone else in this term?
  > - **Election Restriction (`is_up_to_date`)**: The voter compares candidate's last log entry against its own. It compares `last_term` first, then `last_index`. If the candidate's log is less up-to-date, the vote is rejected. This guarantees that a node missing committed entries can never win an election.
  > 5. **Becoming Leader**: Once the candidate receives votes from a strict majority of all configured voters (`len(voters)//2 + 1`), it transitions to `LEADER` and immediately broadcasts heartbeats (`AppendEntries`) to suppress elections on peers. Because two disjoint majorities of the same set cannot exist, split-brain is mathematically impossible."*
- **Key mechanism to highlight**: The election restriction (`is_up_to_date`) ensures a node missing committed log entries can never become leader. Durable disk persistence of `currentTerm` and `votedFor` before granting votes ensures a node never casts two votes in the same term across crashes.
- **Trap / Follow-up**: *"What went wrong when your Raft test harness modelled dead nodes as instant silence?"* → Real dead peers block for a full connect timeout. Waiting on dead nodes stalled the election loop past follower election timeouts, causing healthy nodes to repeatedly depose their leader and terms to climb endlessly without committing anything.

---

## 21. Committing a metadata decision

Everything the cluster agrees on takes this path. There are exactly two
callers: `CREATE_TOPIC` and `ASSIGN_PARTITIONS`.

```
controller decides something
  └─ raft/node.py  propose(command)
     ├─ not leader?  → return False, caller must NOT apply
     ├─ log.append(term, command)      fsync per entry
     ├─ _replicate_to_all()
     │   └─ APPEND_ENTRIES (api 53) to every voter
     │      └─ handle_append_entries()
     │         ├─ term check, _observe_term()
     │         ├─ log.matches(prev_index, prev_term)   Log Matching Property
     │         ├─ log.append_entries(entries)          truncate on conflict
     │         └─ commit_index = min(leader_commit, last_index)
     └─ poll until commit_index >= target
  └─ machine.apply(entry)              only committed entries
  └─ cluster.set_placement(...)        only now is it true locally
```

### Propose, then apply — never the other way round

```python
# broker/controller.py — recompute
committed = await self.raft.propose({'type': 'ASSIGN_PARTITIONS', ...})
if not committed:
    return False                        # ← the entire point
version = self.cluster.metadata_version + 1
self.cluster.set_placement(assignment, version)
```

A controller that applied first and replicated afterwards would be handing out
a map nobody agreed to — the Phase 5 split-brain failure with extra steps.
`CONTROLLER_UPDATE` therefore carries only *committed* metadata: it is a
delivery mechanism, not a second source of truth.

### The commit rule that is easy to omit

```python
# broker/raft/node.py — _advance_commit_index
for index in range(self.log.last_index, self.commit_index, -1):
    if self.log.term_at(index) != self.current_term:
        continue                        # ← Figure 8 of the Raft paper
    replicas = sum(1 for v in self.voters if self.match_index.get(v, 0) >= index)
    if replicas >= self.votes_needed:
        self.commit_index = index
```

A leader may only commit an entry **from its own term** by counting replicas.
Earlier-term entries become committed indirectly, carried along when a
same-term entry commits. Leave the term check out and an entry replicated to a
majority can still be overwritten by a later leader.

### How to explain this in an interview

- **The 30-second pitch**: *"Metadata changes (topic creation and partition placement) are proposed to the Raft quorum as log entries. The controller appends the proposal to its Raft log and replicates it via `AppendEntries`. Once a majority acknowledges receipt, the entry commits and is applied to the in-memory state machine. Propose first, apply second — never the other way around."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"When the controller commits a cluster metadata change (such as `CREATE_TOPIC` or `ASSIGN_PARTITIONS`), it executes through `raft.propose(command)`:
  >
  > 1. `propose` asserts that this broker is the active Raft leader; if not, it returns `False`, and the controller immediately discards the decision.
  > 2. The leader appends the command to its local `RaftLog` at `(current_term, command)` and invokes `fsync()` to ensure durability.
  > 3. It broadcasts `APPEND_ENTRIES` RPCs (API key 53) to all follower voters.
  > 4. Each follower evaluates:
  > - Is the leader's term current?
  > - **Log Matching Property**: Does the follower's log match the leader's `prev_log_index` and `prev_log_term`? If not, the follower rejects, causing the leader to decrement `next_index` and retry. If conflicting uncommitted entries exist, the follower truncates them and appends the leader's entries.
  > 5. On the leader, when follower acknowledgements arrive, `_advance_commit_index()` checks whether a majority has replicated entries.
  >
  > Here, our implementation strictly enforces **Figure 8 of the Raft paper**: a leader may only advance `commit_index` directly for log entries originating from its *current term*. Entries from previous terms are committed only indirectly when a current-term entry reaches majority. Omitting this check allows a subtle distributed anomaly where an entry replicated to a majority can still be overwritten by a subsequent leader.
  >
  > Once committed, `MetadataStateMachine.apply(entry)` executes, updating local metadata. The controller then updates `ClusterState` and pushes `CONTROLLER_UPDATE` to all peers."*
- **Key mechanism to highlight**: Figure 8 rule from the Raft paper: a leader can only commit an entry from its *current term* by counting replicas. Earlier-term entries are committed indirectly. Omitting the term check permits committed entries to be overwritten by subsequent terms.
- **Trap / Follow-up**: *"What happens if a minority controller attempts to assign partitions?"* → The minority controller cannot obtain majority acknowledgement in Raft, so `propose()` fails. The controller discards the uncommitted decision and does not update local state or notify peers, preventing split-brain corruption.

---

## 22. Placing partitions

```
membership tick, or a topic was just created
  └─ controller.py  recompute()
     └─ assign_partitions()
        ├─ widths from the LOCAL metadata store       (see the caveat below)
        ├─ machine.compute_assignment(live, rf, topics)
        │    walk the ring: leader = live[i], replicas = next RF brokers
        ├─ _observed_isr(item)      own view | leader's report | committed
        ├─ _elect_leader(item, live)   prefer someone in the ISR
        └─ _stamp_epoch(item)          bump only if the leader changed
     ├─ machine.assignment_differs()?  → no change, propose nothing
     ├─ raft.propose(ASSIGN_PARTITIONS)
     └─ cluster.set_placement()
  └─ _push_controller_update()      CONTROLLER_UPDATE (api 28) to peers
```

### The ring, and why it is wrong for failover

```python
# broker/raft/machine.py — compute_assignment
start = index % len(live)
replicas = [live[(start + offset) % len(live)] for offset in range(factor)]
```

Walking the ring from a rotating start gives two properties for free: no broker
appears twice in one replica set, and leadership spreads evenly. Both are easy
to break — take the same N brokers for every partition and you replicate
perfectly while piling every leader onto one machine.

But the ring is a *placement* rule, and failover needs an *election* rule:

```python
# broker/controller.py — _elect_leader
eligible = [n for n in item['replicas'] if n in committed_isr and n in live]
if not eligible:
    item['leader'] = previous          # refuse to elect; go unavailable
    return
current = self.raft.machine.leader_for(topic, partition)
item['leader'] = current if current in eligible else eligible[0]
```

Prefer someone who already has the data. Keep the current leader if it is still
eligible, because every unnecessary move costs a metadata refresh on every
client.

### The caveat worth knowing

`widths` comes from the **local metadata store**, not the committed set, and
that is uncomfortable on purpose: auto-created topics never reach the Raft log,
so placing only committed topics would leave every auto-created partition
unassigned and unservable. The honest fix is to propose auto-creations too.

### How to explain this in an interview

- **The 30-second pitch**: *"Partition placement distributes leadership and replicas evenly across live brokers using a deterministic ring algorithm. When electing a leader during failover, the controller strictly promotes from the committed In-Sync Replica (ISR) set, never from the general broker ring, guaranteeing the new leader already holds all committed messages."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Partition placement executes on the controller via `controller.recompute()`:
  >
  > 1. It queries `ClusterState.live_brokers()`, which returns a deterministically sorted list of active broker IDs.
  > 2. It calls `MetadataStateMachine.compute_assignment()`. For new topic partitions, it walks the broker ring:
  > `leader = live[start % len(live)]`, and replicas are assigned to the subsequent `replication_factor` brokers in the ring.
  > This ring algorithm guarantees two properties: no broker appears twice in a partition's replica set, and partition leadership is distributed uniformly across the cluster.
  > 3. Crucially, **the ring is used for initial placement, but NEVER for failover**. When an existing leader dies, `_elect_leader()` evaluates:
  > `eligible = [n for n in replicas if n in committed_isr and n in live]`
  > The controller strictly elects from the **committed In-Sync Replica (ISR)** set, because in-sync replicas are guaranteed to hold all records below the high watermark.
  > If NO in-sync replica is alive, the controller refuses to elect anyone: `item['leader'] = previous` (retaining the dead leader). This implements `unclean.leader.election.enable=false`. The partition goes temporarily unavailable until an ISR replica revives, rather than electing an out-of-sync broker and silently discarding committed user data.
  > 4. If leadership changed, `_stamp_epoch()` increments the partition's `leader_epoch`.
  > 5. The controller proposes `ASSIGN_PARTITIONS` to Raft. Only when Raft commits does the placement take effect and get broadcast to peers."*
- **Key mechanism to highlight**: Unclean leader election is disabled (`unclean.leader.election.enable=false`). If zero in-sync replicas are alive, the partition becomes unavailable rather than electing an out-of-sync broker and silently dropping data.
- **Trap / Follow-up**: *"Why can't failover use the same ring algorithm used for initial partition placement?"* → The ring distributes load evenly assuming blank slates, but on failover the next node in the ring might hold zero replicated data. Failover must be constrained to the ISR.

---

## 23. Snapshotting the Raft log

```
_apply_committed()
  └─ _maybe_snapshot()
     ├─ last_applied - snapshot_last_index < threshold?  → return
     ├─ snapshots.save(index, term, machine.snapshot())   FSYNC FIRST
     └─ log.compact_to(index)                             destroy second
```

### Order is the entire safety argument

Write the snapshot and fsync it, *then* truncate the log. Reverse them and a
crash in between loses both copies of the same entries. The general rule for
replacing A with B on disk: **B must be durable before A is destroyed.**

`RaftLog._load` skips entries the snapshot already covers, which turns the
crash window between the two steps into a no-op.

### The sentinel moves; it is never deleted

```python
# broker/raft/log.py
self._entries = [LogEntry(index, term, {'type': 'SENTINEL'})] + kept
def _pos(self, index):
    return index - self.snapshot_last_index
```

Compaction breaks the identity that every accessor rested on — *list position
is the log index* — so all six go through `_pos()`. The sentinel's meaning
changed (from "before time began" to "the last entry I folded in") but its job
did not: be the thing a `prev_log_index` can legally point at when catching a
follower up from the oldest entry we hold.

### How to explain this in an interview

- **The 30-second pitch**: *"To prevent infinite Raft log disk growth, nodes periodically snapshot their state machine and compact the log. The snapshot serializes the applied metadata state up to `last_included_index` and `last_included_term`. Once the snapshot is persisted and fsync'd, earlier log entries are discarded."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"In Raft consensus, the log cannot grow indefinitely. Flow 23 implements state machine log compaction in `_maybe_snapshot()`:
  >
  > 1. After applying committed entries, `RaftNode` checks if `last_applied - snapshot_last_index >= snapshot_threshold` (e.g. 500 entries).
  > 2. If triggered, `machine.snapshot()` serializes the full metadata state machine (topics, partitions, assignments, ISRs) into bytes.
  > 3. The snapshot manager writes `(last_applied_index, last_applied_term, snapshot_data)` to a file and executes `fsync()`.
  > 4. Only AFTER the snapshot is physically fsync'd does `log.compact_to(last_applied_index)` discard superseded log entries.
  >
  > Disk ordering is the entire safety guarantee: **the replacement state must be durable on disk before the old log entries are destroyed**. If a crash occurs during compaction, startup recovery loads the snapshot and ignores duplicate log entries.
  >
  > 5. In memory, compaction breaks the simple invariant that list array index equals Raft log index. Our `RaftLog` replaces entry 0 with a sentinel entry representing `last_included_index` and `last_included_term`. All accessors map log indices through `_pos(index) = index - snapshot_last_index`. The sentinel provides an anchor so subsequent `AppendEntries` can verify `prev_log_index` and `prev_log_term` when replicating from the oldest retained entry."*
- **Key mechanism to highlight**: Strict disk ordering: write and fsync the snapshot *before* truncating the log. If a crash occurs during compaction, loading `snapshot + log tail` remains deterministic and safe.
- **Trap / Follow-up**: *"Why does the Raft log keep a sentinel entry at index 0 after compaction?"* → The sentinel holds `last_included_index` and `last_included_term`. It gives follower replication (`prev_log_index` / `prev_log_term`) a legal baseline reference when replicating from the oldest retained entry.

---

## 24. Catching up a follower that fell off the log

```
leader replicating to a peer
  └─ raft/node.py  _replicate_once(peer)
     ├─ next_index < log.first_index?
     │   └─ _send_snapshot(peer)              INSTALL_SNAPSHOT (api 54)
     │      └─ handle_install_snapshot()
     │         ├─ term < current            → refuse (stale leader)
     │         ├─ last_index <= commit_index → ignore (old snapshot)
     │         ├─ snapshots.save(...)         durable first
     │         ├─ machine.restore(state)      REPLACE, not merge
     │         └─ log.reset_to_snapshot()     discard everything
     └─ else: APPEND_ENTRIES with entries from next_index
```

**Compaction is what creates this failure.** Before it, "behind" was one state
and always recoverable — back `next_index` up until the logs meet, and they
always did, because index 1 was always there. After it, a follower can need an
entry the leader has thrown away, and the leader cannot even build the message
(`prev_log_term` is inside its snapshot).

This is the only place in the system where **a node destroys committed local
state because a peer told it to**. That is why the two guards are a fence
rather than input validation: remove the term check and a stale leader rewinds
a follower; remove the `commit_index` check and a duplicate packet does.

`chaos/lagging_follower.py` shows a broker acquiring 12 topics whose log
entries exist nowhere in the cluster.

### How to explain this in an interview

- **The 30-second pitch**: *"When a follower lags so far behind that its `next_index` precedes the leader's oldest retained log entry, standard `AppendEntries` cannot catch it up. The leader sends an `INSTALL_SNAPSHOT` RPC containing the compacted snapshot, resetting the follower's log baseline and state machine in one atomic step."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Log compaction introduces a new distributed failure mode: a follower that was offline or network-partitioned can fall behind further than the leader's oldest retained log entry.
  >
  > When the leader prepares `_replicate_once(peer)`:
  >
  > 1. It checks if `peer.next_index <= log.first_index`. If so, regular `AppendEntries` cannot catch up the follower because the preceding entries have already been deleted from the leader's disk.
  > 2. The leader switches to `INSTALL_SNAPSHOT` (API key 54), transmitting its latest compacted snapshot.
  > 3. When the lagging follower receives `INSTALL_SNAPSHOT`:
  > - It checks `term < currentTerm`: if the sender is a stale leader, it rejects.
  > - It checks `last_included_index <= commit_index`: if this is a duplicate or obsolete snapshot, it ignores it.
  > - It writes the snapshot to disk with `fsync()`.
  > - It calls `machine.restore(state)`: it completely replaces its in-memory metadata state machine with the snapshot's state.
  > - It resets its Raft log via `log.reset_to_snapshot()`, discarding all local log entries up to `last_included_index`.
  >
  > This is the only flow in the system where **a node destroys its own committed local log because a remote peer ordered it to**. That is why the term fence and commit index fence are so rigorous: they guarantee that a rogue or stale partitioned leader can never rewind follower state."*
- **Key mechanism to highlight**: A follower only accepts snapshots if the leader's term is current and the snapshot's index exceeds the follower's `commit_index`, preventing stale leaders or duplicate packets from rewinding committed state.
- **Trap / Follow-up**: *"What is the primary limitation of our InstallSnapshot implementation?"* → The snapshot is transmitted in a single RPC payload rather than chunked streaming. This caps metadata capacity at roughly ~190,000 partitions before hitting TCP request size bounds.

---

## 25. Routing a client to the right broker

```
producer.send() / consumer.poll()
  └─ cluster_client.py  leader_for(topic, partition)
     ├─ cache hit and fresh?  → use it
     └─ METADATA (api 3) to any broker
        └─ returns leader, replicas, isr, leader_epoch per partition
  └─ send to that broker
     └─ NOT_LEADER_OR_FOLLOWER or FENCED_LEADER_EPOCH?
        ├─ refresh metadata
        └─ retry
```

**That pair is client-side failover.** There is no redirect and no proxy hop —
the broker refuses and the client re-routes, which is why Kafka clients are
thick libraries rather than thin HTTP wrappers.

### Refusing is the whole point

```python
# broker/broker.py — _not_leader
if not self.cluster.leads(topic, partition):
    return _error(errors.NOT_LEADER_OR_FOLLOWER, ...)
```

Without this a broker asked for a partition it does not lead would happily
create an empty local log and serve zero messages. The client would see an
**empty topic** rather than an error — silent wrongness, which is the failure
mode this project keeps trying to avoid.

### How to explain this in an interview

- **The 30-second pitch**: *"Clients maintain a local metadata cache mapping `(topic, partition) -> broker_id`. The client establishes direct TCP connections to the partition leader with no intermediate load-balancer proxy. When leadership moves, the broker returns `NOT_LEADER_OR_FOLLOWER`, prompting the client to refresh metadata and retry."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Kafka clusters do not use load balancers or proxy routers in front of brokers. Clients are 'thick' and manage direct leader routing:
  >
  > 1. `ClusterClient` maintains an in-memory routing cache: `_leaders[(topic, partition)] -> node_id` and `_brokers[node_id] -> (host, port)`.
  > 2. When `producer.send()` or `consumer.poll()` executes, it calls `leader_for(topic, partition)`. If cached, it retrieves an open, pooled TCP socket to that broker from `_connections`.
  > 3. If the cache is empty or stale (>30s TTL), it queries `METADATA` from any reachable bootstrap broker and updates its tables.
  > 4. When the client sends a request to the cached broker:
  > - If the broker is alive and still leads that partition, the request succeeds with zero routing hops.
  > - If the leader died, the socket fails with `BrokerConnectionError`. The client drops the connection, invalidates its metadata cache, and retries once: the retry sends `METADATA` to any surviving broker, learns the newly elected leader, and connects directly to it.
  > - If the broker is alive but no longer leads the partition (e.g. after failover), the broker checks `ClusterState.leads()`: it explicitly refuses with error code `NOT_LEADER_OR_FOLLOWER` (code 6). The client catches `NotLeaderError`, invalidates its cache, refreshes metadata, and retries.
  >
  > Refusing is the entire design: if brokers forwarded requests or opened missing partitions locally, clients would experience silent data corruption. Direct partition routing maximizes throughput and scales linearly with broker count."*
- **Key mechanism to highlight**: Direct leader socket connections maximize throughput and eliminate proxy bottlenecks. Broker refusal (`NOT_LEADER_OR_FOLLOWER`) acts as the active cache invalidation mechanism, preventing brokers from serving silent empty reads for unled partitions.
- **Trap / Follow-up**: *"Why not use a load balancer in front of the brokers?"* → A proxy adds extra network hops and CPU memory copies on data streams moving hundreds of thousands of messages per second. Smart clients routing directly to leaders scale linearly.

---

## 26. Failing over a partition leader

The flow the whole of Phase 6 exists for.

```
broker holding orders-0 dies
  └─ peers stop hearing its heartbeats
     └─ cluster.live_brokers() drops it       (broker.session.timeout.ms)
  └─ controller's next membership tick
     └─ recompute() → assign_partitions()
        ├─ _elect_leader: eligible = replicas ∩ committed ISR ∩ live
        ├─ _stamp_epoch: leader changed → epoch + 1
        └─ raft.propose(ASSIGN_PARTITIONS)    committed by a majority
  └─ cluster.set_placement() everywhere
     ├─ new leader stops fetching this partition (it leads now)
     └─ other replicas start fetching FROM IT
  └─ clients get NOT_LEADER_OR_FOLLOWER → refresh → retry
```

### Why the promotion is safe

An in-sync replica holds everything below the high watermark **by definition**.
A consumer could only ever read below the watermark. Therefore every record any
consumer was allowed to act on is already on the new leader — which is the
whole reason the ISR is maintained as agreed state rather than a leader's
private opinion.

### Fencing the broker that has not heard

```python
# broker/broker.py — _not_leader
if theirs > mine:
    return _error(errors.FENCED_LEADER_EPOCH,
                  f"broker {self.node_id} believes it leads ... at epoch "
                  f"{mine}, but the caller knows epoch {theirs}")
```

The caller knowing a *newer* epoch is the only way this broker can discover it
is stale. Journal #31: the first version compared these backwards — refusing
the stale *client* instead of the stale *broker* — and fenced nothing, with
every test passing.

### What this flow does NOT protect

An **isolated** leader with an **unaware** client. Neither side knows a newer
epoch, so nothing objects, and `acks=1` writes go into a log nobody will read.

```
acks=all   acked 201   readable 201   LOST 0
acks=1     acked 400   readable 200   LOST 200
```

What protects that case is `acks=all` — an isolated leader cannot satisfy it,
because its ISR has collapsed to itself. `chaos/kill_leader.py` runs both.

### How to explain this in an interview

- **The 30-second pitch**: *"When a leader dies, heartbeats stop and the controller notices during `recompute()`. It promotes an in-sync replica from the committed ISR, bumps the `leader_epoch`, and commits the new assignment through Raft. Clients encountering connection errors or `NOT_LEADER_OR_FOLLOWER` refresh metadata and retry against the new leader."*
- **Full deep-dive explanation (2–3 minutes)**:
  > *"Leader failover is the core fault-tolerance mechanism of Phase 6:
  >
  > 1. **Detection**: Broker 1 dies. Peers stop receiving its heartbeats. After `broker_session_timeout_ms` (e.g. 3s) of silence, `ClusterState.live_brokers()` drops broker 1 on all surviving nodes.
  > 2. **Reassignment**: On the controller's next membership tick, `controller.recompute()` notices a dead leader. It calls `assign_partitions()` and invokes `_elect_leader()`:
  > It filters candidate replicas by `replicas ∩ committed_isr ∩ live_brokers`. Because the new leader is selected from the In-Sync Replica set, it is mathematically guaranteed to already hold all records below the high watermark. Zero committed records are lost.
  > 3. **Leader Epoch Fencing**: The controller increments `leader_epoch` for that partition via `_stamp_epoch()`.
  > 4. **Commit & Broadcast**: The controller proposes `ASSIGN_PARTITIONS` to the Raft quorum. Once committed by a majority, it updates `ClusterState` and broadcasts `CONTROLLER_UPDATE` to all surviving brokers. The promoted replica stops its follower fetcher loop and opens the partition for active client reads and writes.
  > 5. **Client Redirection**: Clients attempting to write to the old leader encounter connection refusals or `NOT_LEADER_OR_FOLLOWER`. They refresh metadata, discover the new leader, and retry.
  > 6. **Zombie Leader Fencing**: If the old leader was merely network-isolated and comes back online still believing it leads the partition, any client that has already refreshed metadata sends the new `leader_epoch`. The zombie broker detects `theirs > mine` and refuses with `FENCED_LEADER_EPOCH`.
  >
  > Furthermore, `acks=all` with `min.insync.replicas` protects against an isolated leader whose clients have not yet refreshed: the isolated leader cannot satisfy `acks=all` because its ISR has collapsed to itself, rejecting writes before data loss can occur."*
- **Key mechanism to highlight**: Leader epoch fencing: every partition has an epoch counter incremented on leadership changes. If a client sends a newer epoch to a stale, zombie broker, the broker refuses with `FENCED_LEADER_EPOCH`.
- **Trap / Follow-up**: *"Can messages be lost during a leader failover?"* → With `acks=all`, 0 messages are lost (proven in `chaos/kill_leader.py` — 0 lost of 201 acked) because writes only commit after reaching the high watermark. With `acks=1`, messages written to the leader's page cache but not yet fetched by followers are lost when that leader dies (201 lost of 400).

---

## Reading order for the cluster flows

If you want to follow the distributed half end to end:

**19** (a cluster forms) → **20** (it elects a controller) → **21** (how any
decision is agreed) → **22** (what the controller decides) → **26** (what
happens when a broker dies).

Flows **23** and **24** are the Raft log's own housekeeping and can be read
later. Flow **25** is the client's half of everything above.
