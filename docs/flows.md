# Implementation Flows: Every Path, Traced Through the Code

For each thing the system does, this traces the **exact call sequence** with
file and line, shows the code that does the work, and says why it is written
that way.

The phase docs explain *why* each mechanism exists. The
[code reading guide](phase-0/code-reading-guide.md) is a tour for a newcomer.
**This is the reference**: open it next to the code when you need to know what
actually happens.

Line numbers are accurate as of the end of Phase 4. Function names are stable;
if a line has drifted, search the name.

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

---

## Reading order, if you want to follow one thing end to end

**A message's whole life:** §3 → §4 → §5 → §6.

**Failure handling:** §12 → §9 → §11.

**What makes it distributed:** §2 → §3 (routing) → §7 (assignment).
