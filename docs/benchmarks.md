# Benchmarks

One row per phase. Every phase claims to improve something; this file is where
the claim either survives contact with a stopwatch or does not.

```bash
python -m bench.bench --count 5000
```

**Read the caveats before the numbers** — a benchmark you cannot interpret is
worse than no benchmark, because it feels like evidence.

---

## Results

| Phase | Produce (msg/s) | Produce p99 (ms) | Consume (msg/s) | Consume p99 (ms) | What changed |
|---|---:|---:|---:|---:|---|
| **0 — Foundation** | 3,982 | 0.55 | 17,081 | 13.81 | Baseline. One round trip per message; O(n) log scan per fetch. |
| 1 — Protocol v2 | | | | | Correlation IDs, API keys, error codes |
| 2 — Storage engine | | | | | Segments + sparse index → fetch should stop degrading |
| 3 — Partitioning | | | | | Batching + `linger.ms` → produce should jump ~10-50x |
| 4 — Consumer groups | | | | | Server-side offsets; expect a small commit cost |
| 5 — Cluster | | | | | Partitions across brokers |
| 6 — Replication | | | | | `acks=all` will cost latency; that is the point |

Environment: Windows 11, Python 3.11, loopback, 5,000 × 100-byte messages,
fetch batch 100.

---

## Reading the Phase 0 baseline

### Produce: 3,982 msg/s, p50 0.22 ms

The broker is not the bottleneck here — **latency is**. Every `send()` is a
full round trip during which the producer, the network, and the broker are all
idle waiting for each other:

```
producer  ──send──►                      (idle)
                    broker: append+flush (~0.05 ms of actual work)
          ◄──ack───                      (idle)
producer  ──send──►  ...
```

At 0.22 ms per round trip, ~4,000/sec is arithmetic, not engineering. You
cannot fix this by making the broker faster, because the broker is already
mostly idle. You fix it by **amortising the round trip**, which is what Phase
3's batching does: 100 messages per request means 100x fewer round trips.

Note also that this is loopback — no real network. On a LAN (0.5 ms RTT) this
number would be ~2,000/s; across regions (50 ms) it would be **20/s**. The
Phase 3 improvement will look far more dramatic on anything resembling a real
network than it does here.

### Consume: 17,081 msg/s — and the number that actually matters

Consume looks 4x faster than produce for one reason: it already batches, 100
messages per request. Same insight, applied on the read side.

But look at the latency spread rather than the throughput:

```
p50 = 5.76 ms      p95 = 10.79 ms      p99 = max = 13.81 ms
```

The **slowest fetch is the last one**, and that is not noise — it is the
`fetch` implementation showing its cost. `Partition.fetch` opens the log at
byte zero and parses forward, discarding every record below the requested
offset:

| Fetch # | Records skipped before the ones we want | Latency |
|---|---:|---:|
| 1st (offset 0) | 0 | ~1 ms |
| 25th (offset 2400) | 2,400 | ~5.8 ms (p50) |
| 50th (offset 4900) | 4,900 | 13.8 ms (max) |

Latency grows **linearly with how much data the partition already holds**. A
broker that gets slower the longer it runs is not a broker you can operate —
at a million messages a single fetch would take seconds.

This is the single biggest flaw in the current design, and Phase 2 exists to
fix it. After segments and a sparse index, expect p50 ≈ p99 ≈ max: fetch cost
should become independent of log size. **The flatness of that curve is the
result to check, more than the throughput number.**

---

## How to read these numbers honestly

**Loopback is not a network.** Client and broker share a machine, so the
round-trip time is ~0.05 ms instead of 0.5–50 ms. This *flatters*
round-trip-per-message designs enormously.

**One client, one thread.** Nothing here measures concurrency, which is what
asyncio was chosen for. A concurrency benchmark belongs in Phase 5, where
there is a cluster to spread load across.

**Throughput hides the tail; percentiles do not.** Consume's *mean* is 5.85 ms
and its max is 13.81 ms — a 2.4x spread that the mean alone would never
reveal. Any phase where the mean improves while p99 gets worse has made the
system worse. Watch p99.

**Nothing is fsynced.** `Partition.append` flushes to the OS page cache, not
to disk. Adding `os.fsync` would cut produce throughput by 10–100x. That
number is not a flaw in the benchmark — it is the reason Kafka buys durability
with replication (Phase 6) rather than with fsync. Try it: Exercise 3 in
[phase-0-foundation.md](phase-0-foundation.md).

**Same machine, same conditions, or don't compare.** Numbers from different
hardware are not comparable. Rerun Phase 0's baseline on your machine before
comparing a later phase to it.
