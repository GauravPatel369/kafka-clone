# Phase 0 — Foundation: Making the Prototype Trustworthy

> **Goal:** before adding a single feature, build the harness that will measure
> every later phase, and fix the four bugs in the prototype that would otherwise
> be inherited by all of them.
>
> **Nothing in this phase adds a Kafka feature.** That is the point.

---

## 1. The problem

The prototype works. You can start the broker, produce five messages, consume
them back. That is genuinely a working pub-sub system.

But consider what happens in Phase 6, when we implement replication and a
follower broker's log doesn't match the leader's. You will need to answer: *is
the replication logic wrong, or was the storage layer always subtly broken?*
With no tests and no baseline measurements, you cannot answer that. You will
spend a day guessing.

So Phase 0 answers three questions permanently:

1. **Is it correct?** → a `pytest` suite that starts a real broker on a real socket.
2. **Is it fast, and did my change make it slower?** → a benchmark harness with a recorded baseline.
3. **What actually happens when something dies?** → chaos scripts that kill things on purpose.

Plus: fix the bugs first, so the baseline is a baseline of *correct* behaviour.

---

## 2. The four bugs

These are all real. Each one is instructive, which is why we're walking through
them rather than silently patching.

### Bug 1 — At-most-once delivery: the offset is committed before the work is done

This is the most important bug in the codebase, and it is invisible until
something crashes.

Original `Consumer.poll()`:

```python
messages = response.get('messages', [])
if messages:
    self.offset = messages[-1]['offset'] + 1
    self._save_offset()          # ← committed to disk HERE
return messages                  # ← caller processes them AFTER
```

And the caller:

```python
messages = self.poll()           # offset already saved as "done"
for msg in messages:
    process_fn(msg)              # ← if we crash here, those messages are lost
```

The consumer has told the world "I have processed through offset 41" *before it
processed anything*. Crash in the middle of `process_fn` and those messages are
never re-delivered — the saved offset says they're finished.

This is **at-most-once** delivery: every message is delivered zero or one times.
Silent data loss on every crash.

#### The three delivery semantics

This is the single most important trade-off in all of messaging, so it's worth
laying out properly:

| Semantic | How you get it | Failure result | Use when |
|---|---|---|---|
| **At-most-once** | Commit offset, *then* process | Messages **lost** on crash | Metrics, logs, click tracking — where losing a few is cheaper than double-counting |
| **At-least-once** | Process, *then* commit offset | Messages **duplicated** on crash | Almost everything. The default, and what real Kafka defaults to. |
| **Exactly-once** | Process and commit **atomically** | Neither | Requires transactions across your database *and* the broker. Expensive and narrow. |

There is no fourth option. In a distributed system you must choose which of
"lost" or "duplicated" you can tolerate, because a crash can always land in the
gap between "did the work" and "recorded that I did the work".

```
  AT-MOST-ONCE                        AT-LEAST-ONCE

  fetch  ──►  commit  ──►  process    fetch  ──►  process  ──►  commit
                    ▲                                     ▲
                 crash here                            crash here
                 = message LOST                        = message REDELIVERED
                   (gone forever)                        (process it twice)
```

**Decision:** we switch to **at-least-once**, and we make the commit *explicit*
rather than a hidden side effect of `poll()`. Losing data silently is a much
worse default than doing work twice — and duplicates have a practical cure
(make `process_fn` idempotent) while lost data does not.

`poll()` no longer commits anything. `commit()` is now a method you call.
`consume_loop()` calls it after `process_fn` returns.

> The exactly-once row above is why people say "exactly-once delivery is
> impossible, exactly-once *processing* is achievable." The delivery genuinely
> can't be guaranteed; what you can do is make duplicate delivery have no
> visible effect. Keep that distinction — Phase 4 returns to it.

### Bug 2 — The offset file is written non-atomically

```python
def _save_offset(self):
    with open(self._offset_file, 'w') as f:   # 'w' TRUNCATES the file to 0 bytes
        f.write(str(self.offset))             # then writes
```

Between the truncate and the write, the file is **empty**. If the process is
killed in that window — or the machine loses power — the file on disk is a zero-
byte file. On restart, `_load_offset()` catches `ValueError` and returns `0`, so
the consumer silently **reprocesses the entire topic from the beginning**.

The window is microseconds. Microseconds happen, and they happen at 3am.

**The fix — write-temp-then-rename:**

```python
tmp = self._offset_file + '.tmp'
with open(tmp, 'w') as f:
    f.write(str(self.offset))
    f.flush()
    os.fsync(f.fileno())      # force to physical disk before the rename
os.replace(tmp, self._offset_file)   # atomic on POSIX and on Windows
```

`os.replace` is an **atomic rename**: the filesystem guarantees that any reader
sees either the complete old file or the complete new file, never a partial one.
Crash at any instant and one of the two intact versions survives.

> **Why the `fsync`?** Without it, the rename could reach disk before the *data*
> does — leaving a renamed file full of zeros after a power cut. The `fsync`
> orders the two. Note we use `os.replace`, not `os.rename`: on Windows,
> `os.rename` fails if the destination exists. `os.replace` overwrites on both
> platforms.

This write-temp-fsync-rename pattern is how essentially every database, editor,
and config manager writes a file safely. Learn it once, use it forever.

### Bug 3 — Reopening the log file on every single append

```python
def append(self, value, key=None):
    with self._lock:
        with open(self.log_path, 'a') as f:   # open + close, EVERY message
            f.write(json.dumps(record) + '\n')
```

Every message pays for an `open()` syscall, a `close()` syscall, and on Windows
the file-system metadata churn that goes with them. For a system whose entire
premise is "sequential appends are fast", this throws that advantage away.

**The fix:** open the handle once in `__init__` and keep it. The existing
`threading.Lock` already serialises writers, so a shared handle is safe.

This raises a question the fix must answer: **if the writer holds an open buffered
handle, will a reader see the data?**

```python
self._fh.write(line)
self._fh.flush()      # Python buffer → OS page cache. Readers see it now.
                      # NOT os.fsync() — data is not on physical disk yet.
```

| Level | Call | Survives | Cost |
|---|---|---|---|
| Python buffer | (nothing) | nothing | free |
| OS page cache | `f.flush()` | **process crash** | ~1 µs |
| Physical disk | `os.fsync()` | **power loss / OS crash** | ~1–10 ms (!) |

We flush, we do not fsync. That's a deliberate 1000x speed difference, and it
means: *kill the broker process and no data is lost; pull the power cord and the
last few messages are.*

That trade is exactly what real Kafka makes, and its justification is
`replication`: with three copies on three machines, losing the tail of one node's
page cache doesn't lose data, because two other machines have it. **Kafka buys
durability with replication rather than with fsync** — which is only a valid
trade once Phase 6 exists. Until then, this is a known, documented weakness.
Phase 2 makes the flush policy configurable so you can measure the cost yourself.

### Bug 4 — `writer.close()` without `await writer.wait_closed()`

```python
finally:
    writer.close()     # asks asyncio to close; returns immediately
```

`close()` only *schedules* the close. Buffered response bytes may not have been
written to the socket yet, and the FD isn't released when the coroutine ends.
Under load this shows up as clients seeing truncated responses and as the
broker slowly leaking file descriptors.

**The fix:**

```python
writer.close()
try:
    await writer.wait_closed()
except (ConnectionError, OSError):
    pass       # peer already vanished — nothing to flush to
```

The `try/except` matters: if the client hung up first, `wait_closed()` raises,
and an exception in a `finally` block would mask the real error.

> **Bonus, same file:** the connection loop caught `Exception` and logged it, so a
> malformed JSON body from one client killed that connection with a scary log
> line. We now catch `json.JSONDecodeError` narrowly and reply with a structured
> error instead of dropping the connection. Phase 1 gives errors real codes.

---

## 3. What the harness gives us

### `common/net.py` — one client transport

`producer.py` and `consumer.py` each contained a byte-identical copy of
`_recv_response()` and `_recvexactly()`. Duplicated code is how two components
drift apart and then disagree about the protocol.

Both now use `SyncConnection` from `common/net.py`. Everything that later needs
to talk to a broker — the admin CLI, the benchmark, the replication fetcher in
Phase 6 — uses the same class, so a protocol change happens in exactly one place.

The class exists mainly so that `_recvexactly` lives once:

```python
while len(buf) < n:
    chunk = self._sock.recv(n - len(buf))
    if not chunk:
        raise BrokerConnectionError(...)
    buf.extend(chunk)
```

**Why this loop is mandatory:** TCP is a byte *stream*, not a message stream. It
guarantees your bytes arrive in order; it guarantees nothing about how they're
grouped. `send(100 bytes)` may arrive as one `recv` of 100, or two of 50, or 100
of 1. It may also arrive glued to the *next* message's bytes. Any protocol over
TCP must therefore carry its own framing — which is what our 12-byte
length-prefixed header is for, and why we must read exactly the number of bytes
it names, no more and no fewer.

### `tests/` — a real broker on a real socket

`tests/conftest.py` provides a `broker` fixture that starts an actual `Broker` in
a background thread with its own asyncio loop, on **port 0** — meaning "OS,
give me any free port" — with a `tmp_path` data directory.

Port 0 matters more than it looks: it means tests never collide with your
running dev broker on 9092, and the suite can run in parallel with itself.

These are **integration tests**, not mocks. They open TCP connections, write
bytes, and read files off disk. A mock of the broker would only prove our mock
matches our assumptions.

### `bench/bench.py` — the number that keeps us honest

Reports produce and consume throughput (msgs/sec, MB/sec) and latency
percentiles (p50/p95/p99). Every phase appends a row to
[`benchmarks.md`](benchmarks.md).

**Why percentiles and not an average:** an average hides the tail. A system that
answers in 1 ms 99 times and 2000 ms once has a 21 ms average — which describes
no request that actually happened. p99 is what your users feel. Averages in
latency reporting are close to useless; the phases where a change makes the
average better and the p99 worse are exactly the ones worth catching.

### `chaos/` — deliberately breaking things

Correctness tests prove the system works when nothing goes wrong. Distributed
systems are interesting precisely when something does.

`chaos/kill_consumer_midprocess.py` demonstrates Bug 1 *as a live experiment*.
Since the commit is now an explicit call, both semantics are reachable through
the same API just by moving one line:

```python
# Run A — at-most-once (the old behaviour)     # Run B — at-least-once (fixed)
msgs = consumer.poll()                          msgs = consumer.poll()
consumer.commit()          # ← commit first     for m in msgs:
for m in msgs:                                      process(m)   # crash here
    process(m)   # crash here                   consumer.commit()  # ← commit last
# restart: messages GONE                        # restart: messages REPLAYED
```

The script runs both and prints which messages survived. Seeing three specific
messages vanish is worth more than reading a paragraph about at-most-once.

### `tools/kfk.py` — the operational view

A system you cannot inspect is a system you cannot debug. `kfk` lets you look at
broker state from outside: dump a log file, describe a partition, tail messages.
It grows every phase (`kfk topics`, `kfk groups`, `kfk cluster`).

---

## 4. Running things: `python -m`

The prototype started each file with:

```python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

That's a workaround for running `python broker/broker.py` — which puts
`broker/` on the path, not the repo root, so `import common.protocol` fails.

The standard fix is to run modules with `-m` from the repo root:

```bash
python -m broker.broker        # not: python broker/broker.py
```

`-m` puts the **current working directory** on `sys.path`, so `common`,
`broker`, `producer`, and `consumer` all resolve as packages. The path hacks are
deleted. `pyproject.toml` sets `pythonpath = ["."]` for pytest so tests work the
same way.

---

## 5. Code walkthrough

| File | What changed |
|---|---|
| `common/config.py` | **New.** Shared defaults (host, port, dirs, sizes) so magic numbers live in one place. |
| `common/net.py` | **New.** `SyncConnection` — framed request/response over a blocking socket. Replaces duplicated code in both clients. |
| `broker/partition.py` | Persistent write handle + `flush()`; `close()`; recovery reads the last line without loading the whole file; corrupt-line counting. |
| `broker/broker.py` | `await writer.wait_closed()`; narrow exception handling; graceful shutdown that closes partitions; path hack removed. |
| `producer/producer.py` | Uses `SyncConnection`; `quiet` mode so benchmarks aren't dominated by `print`; path hack removed. |
| `consumer/consumer.py` | **`poll()` no longer commits.** New `commit()`, atomic offset write, `seek()`/`seek_to_beginning()`, `auto_commit` flag on `consume_loop`. |
| `tests/` | **New.** conftest fixture + protocol, partition, and end-to-end tests. |
| `bench/bench.py` | **New.** Throughput and latency percentiles. |
| `chaos/kill_consumer_midprocess.py` | **New.** Live demonstration of at-most-once vs at-least-once. |
| `tools/kfk.py` | **New.** Admin CLI. |

---

## 6. How real Kafka differs

- **Offsets aren't in local files.** Real consumers commit to an internal Kafka
  topic, `__consumer_offsets`, so the position survives the machine, not just the
  process. We get there in Phase 4; local files are a Phase 0–3 stand-in.
- **Records are binary, not JSON lines.** Real Kafka uses a compact binary
  record batch with CRC, varint encoding, and optional compression. We keep JSON
  because you can `cat` the log file and read it — a real teaching advantage,
  paid for in ~3x bandwidth. Phase 2 adds the CRC.
- **Kafka never rewrites a file to update state.** Everything, including consumer
  offsets, is an append to a log. Our `.tmp` + rename is the classic non-log way
  to update a small file safely; both patterns are worth knowing.

---

## 7. Exercises

1. **Watch at-most-once lose data.** Run `python -m chaos.kill_consumer_midprocess`
   and read the summary. Then change the commit ordering yourself and predict the
   output before rerunning.
2. **Prove the non-atomic write is real.** Write a loop that saves an offset
   10,000 times while a second process reads the file continuously. With `'w'`
   you will eventually read an empty string; with `os.replace` you never will.
3. **Measure the flush trade.** Add `os.fsync(self._fh.fileno())` after the
   `flush()` in `Partition.append` and rerun `bench/bench.py`. Expect a 10–100x
   throughput collapse. That number is why replication exists.
4. **Break the framing.** Send a header claiming `body_length = 1000` and then
   only 10 bytes. What does the broker do? (This is why Phase 1 adds
   `MAX_REQUEST_BYTES` and a read timeout.)
5. **Find the remaining race.** `Partition.append` is locked, but `fetch` reads
   the file with no lock at all. Under what interleaving could a reader see a
   partial line? (Hint: `flush()` is not atomic either. Phase 2's length-prefix
   + CRC is the real fix.)

---

## 8. Verification

```bash
pip install -r requirements.txt

pytest -q                                  # 1. correctness
python -m chaos.kill_consumer_midprocess   # 2. failure behaviour
python -m bench.bench --count 5000         # 3. baseline numbers

# 4. manual demo
python -m broker.broker                    # terminal 1
python -m producer.producer --count 20     # terminal 2
python -m consumer.consumer --group demo   # terminal 3
python -m tools.kfk describe --topic orders
```

Record the benchmark row in [`benchmarks.md`](benchmarks.md). That is the
number every later phase is compared against.

→ Next: **Phase 1 — Protocol v2** (correlation IDs, API keys, real error codes).
