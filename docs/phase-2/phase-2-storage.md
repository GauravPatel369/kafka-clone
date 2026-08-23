# Phase 2 — Storage Engine: Segments, Sparse Index, CRC, Retention

> **Goal:** make read cost independent of how much data the log holds, detect
> corruption instead of parsing it, and stop the log growing forever.
>
> This is the phase where the benchmark curve changes shape.

---

## 1. The problem

Three of them, and the first is measured, not asserted.

### Problem 1 — every read scans the whole file

`Partition.fetch` opens the log at byte zero and parses forward, discarding
every record below the requested offset:

```python
for line in f:
    record = json.loads(line)          # parsed it...
    if record['offset'] < start_offset:
        continue                       # ...just to throw it away
```

Reading offset 5,000,000 means parsing five million records you do not want.
The Phase 1 benchmark shows the cost directly — consume latency across one run
over a 5,000-message log:

| Fetch | Records skipped first | Latency |
|---|---:|---:|
| 1st (offset 0) | 0 | ~1 ms |
| 25th (offset 2,400) | 2,400 | 5.9 ms (p50) |
| 50th (offset 4,900) | 4,900 | 12.1 ms (p99 = max) |

**Latency grows linearly with data already stored.** At a million messages a
single fetch takes seconds. A broker that gets slower the longer it runs is not
a broker you can operate — and note the failure mode is insidious: it passes
every test, performs beautifully in a demo, and degrades in production over
weeks.

### Problem 2 — corruption is silently parsed, not detected

A record is a JSON line. A torn write — the broker killed mid-`write`, or a
bad sector — produces a line that either:

- **fails to parse** → we skip it and count it. Fine.
- **parses into something wrong** → we return it as data. Not fine.

The second case is real. `{"offset": 41, "value": "hello world"}` truncated to
`{"offset": 41, "value": "hello"}` is valid JSON and a plausible message.
Nothing anywhere would notice. **A storage layer that cannot tell you its data
is intact is a storage layer you cannot trust**, and every later phase —
replication especially — is built on trusting it.

There is also a recovery gap. `_recover_offset` finds the last *parseable*
record, but the torn bytes stay in the file forever, and the next append lands
after them. The log now has permanent garbage in the middle.

### Problem 3 — the log grows forever

There is no retention. A topic taking 1 MB/s fills a terabyte in twelve days
and then the broker dies. Deleting individual records is not an option: it
would mean rewriting the file, which destroys the sequential-append property
the whole design rests on.

---

## 2. How real Kafka solves it

### Segments — the log is many files, not one

```
data/orders-0/
    00000000000000000000.log     ← base offset 0, sealed
    00000000000000000000.index
    00000000000000005193.log     ← base offset 5193, sealed
    00000000000000005193.index
    00000000000000009471.log     ← ACTIVE, being appended to
    00000000000000009471.index
```

Each file is named by the **base offset** of its first record, zero-padded to
20 digits so lexical order equals numeric order — which means the directory
listing is already sorted, with no parsing.

One segment is *active* and receives appends. When it hits `segment.bytes` (or
`segment.ms`), it is sealed and a new one starts.

This single change unlocks everything else:

| Capability | Why segments make it possible |
|---|---|
| **Fast lookup** | Binary search the file names to find which segment holds an offset |
| **Retention** | Delete a whole *file* — one cheap operation, no rewriting |
| **Recovery** | Only the active segment can be torn; sealed ones are never written again |
| **Compaction** | Rewrite one sealed segment at a time, in the background |

### The sparse index — the actual fix for Problem 1

Alongside each `.log` is an `.index`: fixed-width entries mapping an offset to
a byte position.

```
       .index  (8 bytes per entry)              .log
    ┌───────────┬──────────┐
    │ rel_off 0 │ pos    0 │ ─────────────────► record 9471
    │ rel_off 47│ pos 4096 │ ─────────────────► record 9518
    │ rel_off 93│ pos 8192 │ ─────────────────► record 9564
    └───────────┴──────────┘
```

To find offset 9540: binary search the index → nearest entry is (47, 4096) →
`seek(4096)` → scan forward past a handful of records. **Two seeks and a few KB
read, regardless of whether the log holds a thousand records or a billion.**

**Why *sparse*?** An entry per record would be an index of a billion entries —
gigabytes of memory to keep useful. One entry per `index.interval.bytes`
(4 KB in real Kafka) gives an index ~0.1% the size of the log, at the cost of a
short scan. And that scan is *sequential*, which is the operation disks are
best at. Kafka deliberately trades a tiny amount of the thing disks are good at
for a large amount of the thing they are bad at.

**Why relative offsets?** Storing `offset - base_offset` fits in 4 bytes
instead of 8. On a large broker the index is memory-mapped and hot; halving it
matters.

### CRC — detecting corruption instead of parsing it

Each record gets a length prefix and a checksum:

```
┌──────────────┬──────────────┬────────────────────────────┐
│  length 4B   │   crc32 4B   │   payload (JSON bytes)     │
└──────────────┴──────────────┴────────────────────────────┘
```

Now a reader knows *before* parsing whether the bytes are intact. A torn tail
fails its CRC, and recovery **truncates the file back to the last valid
record** rather than leaving garbage in the middle forever.

The length prefix does a second job: it makes record boundaries explicit, so a
reader can skip a record without parsing it — which is exactly what the index
scan needs.

### Retention — delete files, never records

`retention.ms` and `retention.bytes`. A background cleaner deletes **whole
sealed segments** whose newest record is older than the limit, or that push the
log over the size budget. The active segment is never deleted.

The visible consequence: `log_start_offset` stops being 0. A consumer asking
for an offset below it gets `OFFSET_OUT_OF_RANGE` — **"you fell so far behind
that your data is gone."** That is the failure consumer lag monitoring exists
to prevent, and it was unreachable before this phase.

---

## 3. Options considered

### Record format

| Option | Trade-off |
|---|---|
| JSON lines (Phase 0–1) | `cat` the file and read it. No corruption detection, no explicit boundaries, and every skipped record must still be parsed. |
| **Length + CRC32 + JSON payload** ← chosen | Boundaries and integrity, payload still legible in a hex dump. Costs 8 bytes/record and the ability to `cat`. |
| Full binary (varints, packed fields) | What real Kafka does. ~3x smaller and much faster to parse — and completely opaque without a tool. |

We give up `cat`. That was a genuine teaching benefit, so it is replaced
deliberately: `kfk dump-log` becomes the way to read a log file, which is
exactly what real Kafka does (`kafka-dump-log.sh`). **The tool replaces the
convenience, and the tool can also verify the checksums** — something `cat`
never could.

### Index density

| Option | Trade-off |
|---|---|
| No index (Phase 0–1) | Nothing to maintain; O(n) reads. |
| Dense — every record | O(1) lookup, no scan. Index is ~8 bytes × every record: gigabytes, and it must be rebuilt entirely on recovery. |
| **Sparse — every N bytes** ← chosen | ~0.1% of log size, one short sequential scan. The scan is the operation disks are good at. |

### When to fsync

| Option | Trade-off |
|---|---|
| Every record | Survives power loss. ~1–10 ms per write: collapses throughput 10–100x. |
| **Never; rely on the OS** ← chosen default | Survives process kill, not power loss. What real Kafka defaults to. |
| Every N records / N ms | A dial between the two. Implemented and configurable, off by default. |

Kafka's position — and ours from Phase 6 — is that **durability is bought with
replication, not with fsync.** Three copies on three machines survive one
node's page cache vanishing. Paying 10 ms per write to defend against something
replication already covers is a bad trade. The dial exists so you can *measure*
the cost yourself (Exercise 3).

### Where does the storage code live?

`Partition` was doing two jobs: being a named, concurrency-controlled thing the
broker addresses, and being the on-disk format. Those change for different
reasons, so they split:

- **`broker/log/`** — `Log`, `Segment`, `Index`, record encoding. Knows nothing
  about topics, brokers, or locks.
- **`broker/partition.py`** — `Partition` keeps its name and public API
  (`append`, `fetch`, `close`), owns the lock and the identity, and delegates
  storage to a `Log`.

Broker code and existing tests are unchanged, which is the point: a storage
rewrite that forces changes everywhere was not encapsulated to begin with.

---

## 4. The decision

### On disk

One **directory** per partition, replacing the single flat file:

```
data/orders-0/00000000000000000000.log
data/orders-0/00000000000000000000.index
```

> **Breaking change.** Phase 0/1 logs (`data/orders-0.log`) cannot be read.
> There is no migration: this is a learning project with no deployed data, and
> a converter would be more code than the feature. Delete `data/` when you
> upgrade. Real Kafka would never have this luxury — which is exactly why real
> Kafka versions its record format and ships converters.

### Defaults, and why they are not Kafka's

| Setting | Ours | Real Kafka | Why the difference |
|---|---:|---:|---|
| `segment_bytes` | 1 MB | 1 GB | So you can watch rolling happen in a demo instead of after a week |
| `index_interval_bytes` | 4 KB | 4 KB | Same |
| `retention_ms` | 7 days | 7 days | Same |
| `retention_bytes` | unlimited | unlimited | Same |
| `fsync_messages` | off | off | Same — replication is the durability story |

### What the code gains

```python
log.append(value, key)          # → offset; rolls a segment when full
log.read(offset, max_messages)  # → records, via index lookup
log.truncate_to(offset)         # → drops everything at or above (Phase 6 needs this)
log.delete_old_segments()       # → retention; returns what it deleted
log.recover()                   # → truncates a torn tail, rebuilds a missing index
```

`truncate_to` is built now although nothing calls it until Phase 6. It is what
a follower does when it discovers its log has diverged from a new leader's:
truncate to the high watermark and re-fetch. Building it with the storage layer
is far easier than bolting it on later.

---

## 5. Code walkthrough

| File | Role |
|---|---|
| [`broker/log/record.py`](../../broker/log/record.py) | Encode/decode one record: length + CRC32 + JSON. `iter_records` yields `(offset, position, record)` and stops cleanly at a torn tail. |
| [`broker/log/index.py`](../../broker/log/index.py) | Sparse offset→position index. Fixed 8-byte entries, binary search, rebuildable from the log. |
| [`broker/log/segment.py`](../../broker/log/segment.py) | One `.log` + `.index` pair. Append, read-from-offset, seal, delete, recover. |
| [`broker/log/log.py`](../../broker/log/log.py) | The ordered list of segments. Rolling, lookup, retention, truncation, recovery. |
| [`broker/partition.py`](../../broker/partition.py) | Unchanged public API; now a thin facade over `Log`. |
| [`tools/kfk.py`](../../tools/kfk.py) | `dump-log` reads the binary format and verifies CRCs; new `segments` command. |

### The lookup, end to end

```python
def read(self, offset, max_messages):
    segment = self._segment_containing(offset)   # binary search file names
    if segment is None:
        return []
    return segment.read(offset, max_messages)    # index → seek → short scan
```

and inside the segment:

```python
position = self.index.lookup(offset)   # binary search, 8-byte entries
file.seek(position)                    # jump straight there
for off, pos, record in iter_records(file):
    if off >= offset:
        collect(record)                # scan forward a few KB at most
```

Two binary searches and a short sequential read. **Nothing in that path depends
on how much data the log holds** — which is the whole phase in one sentence.

### Recovery, which is where the CRC earns its place

On startup, only the **active** segment can be damaged — sealed segments are
never written again. So recovery scans just that one:

```python
valid_end = 0
for offset, position, record in iter_records(file):
    valid_end = position + record_size    # last byte of the last GOOD record

if valid_end < file_size:
    file.truncate(valid_end)              # cut the torn tail off, for real
```

`iter_records` stops at the first record whose length is impossible or whose
CRC fails. Everything after it is unrecoverable by definition — we cannot know
where the next record starts — so it is removed rather than left in place.

Phase 0 could only *skip* a bad line and leave it in the file forever. Now the
file is repaired.

---

## 6. How real Kafka differs

- **Records are batched on disk.** Kafka stores a *record batch* header once for
  many records, amortising the CRC and metadata. We store one record at a time,
  so our 8-byte overhead is per record rather than per batch.
- **Indexes are memory-mapped.** Kafka `mmap`s the index so lookups are memory
  reads with no syscall. We open and seek — simpler, slower.
- **There is a second index.** `.timeindex` maps timestamp → offset, which is
  what makes "replay from 9am" and `LIST_OFFSETS` by real timestamp work. We
  still only support the earliest/latest sentinels.
- **Compaction is implemented.** We ship delete-retention only. Compaction
  needs a key→latest-offset map and segment rewriting; it is what Phase 4's
  `__consumer_offsets` will actually need, and we will revisit it there.
- **Zero-copy reads.** Kafka serves consumers with `sendfile`, moving bytes from
  page cache to socket without entering user space. We read into Python objects
  and re-serialise them — the single largest remaining performance gap.

---

## 7. Exercises

1. **Watch the curve flatten.** Run `python -m bench.bench --count 20000` and
   read the SEEK PROFILE table. In Phase 1 latency climbed with offset; it
   should now be flat. Predict what it looks like at 200,000 messages.
2. **Watch segments roll.** Produce 50,000 messages, then
   `python -m tools.kfk segments --topic orders`. Count the files. Check the
   index sizes against the log sizes — what fraction are they, and does that
   match `index_interval_bytes`?
3. **Measure the fsync cost.** Set `fsync_messages=1` and rerun the benchmark.
   Expect a 10–100x collapse. That number is the entire argument for buying
   durability with replication instead.
4. **Corrupt a log on purpose.** Append junk bytes to a `.log` file, restart the
   broker, and watch recovery truncate them. Then flip a single byte *inside* a
   valid record and confirm the CRC catches it — this is the case Phase 1 would
   have served as real data.
5. **Delete an index and restart.** It should be rebuilt from the log. Why is
   that safe, and why is the reverse (rebuilding a log from an index) not?
6. **Break the sparse index deliberately.** Set `index_interval_bytes` to
   1,000,000 so almost nothing is indexed. Measure the seek profile again. What
   have you turned the system back into?

---

## 8. Verification

```bash
pytest -q                                  # correctness, incl. corruption cases
python -m chaos.corrupt_log                # torn tail + bit flip, live
python -m bench.bench --count 20000        # the seek profile is the result
python -m tools.kfk segments --topic orders
python -m tools.kfk dump-log --topic orders --verify
```

The headline number is not throughput — it is **the flatness of the seek
profile**. Record it in [`benchmarks.md`](../benchmarks.md).

→ Next: **Phase 3 — Partitioning** (key hashing, batching, `acks`, retries),
which is where load balancing becomes real and where the ~215 µs of
per-message broker work finally gets amortised.
