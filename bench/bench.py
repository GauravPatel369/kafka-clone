"""
CONCEPT: Measure, Don't Guess
------------------------------
Every phase of this project claims to make something better. This script is
what turns those claims into numbers.

Run it after each phase and append the row to docs/benchmarks.md. When Phase 3
adds batching and Phase 2 adds an index, the improvement should be visible
here — and if it isn't, the feature didn't work, no matter how good the code
looks.

    python -m bench.bench --count 5000


WHY PERCENTILES, NOT AVERAGES
-------------------------------
Suppose 99 requests take 1 ms and one takes 2000 ms. The average is 21 ms — a
number describing no request that actually happened. It makes a system with a
two-second stall look like a system with a mild 21 ms delay.

    p50 (median)  what a typical request feels
    p95           what a bad moment feels like
    p99           what your angriest user feels
    max           the worst thing that happened

The tail is where the truth lives. Averages are the single most misleading
statistic in performance work: a change that improves the average while
wrecking p99 is a change that made your system worse, and only percentiles
will tell you.

WHAT THIS MEASURES: end-to-end client latency — encode, send, broker handling,
disk write, response, decode. Not just the broker's internal time. That is
deliberate: the client's experience is the thing that matters, and "the broker
was fast, the client just waited" is not a defence.

CAVEAT, stated up front: this runs the client and the broker on the same
machine over loopback. Real network latency (0.5-50 ms) is absent, which
flatters the round-trip-per-message design badly. On a real network the Phase
3 batching improvement will be far larger than it looks here.
"""

import argparse
import asyncio
import statistics
import threading
import time

from broker.broker import Broker
from common.net import SyncConnection
from common.protocol import API_FETCH, API_METADATA, API_PRODUCE
from producer.producer import Producer


def percentile(sorted_values: list, fraction: float) -> float:
    """
    Nearest-rank percentile. With 100 samples, p99 is the 99th slowest.

    We compute it by hand rather than reaching for numpy: this project has no
    runtime dependencies, and the definition is three lines and worth knowing.
    """
    if not sorted_values:
        return 0.0
    index = min(int(len(sorted_values) * fraction), len(sorted_values) - 1)
    return sorted_values[index]


def summarise(latencies_ms: list) -> dict:
    ordered = sorted(latencies_ms)
    return {
        'p50': percentile(ordered, 0.50),
        'p95': percentile(ordered, 0.95),
        'p99': percentile(ordered, 0.99),
        'max': ordered[-1] if ordered else 0.0,
        'mean': statistics.fmean(ordered) if ordered else 0.0,
    }


def format_row(label: str, count: int, seconds: float, payload_bytes: int,
               stats: dict) -> str:
    rate = count / seconds if seconds else 0.0
    mb = (count * payload_bytes) / (1024 * 1024)
    return (
        f"\n  {label}\n"
        f"    messages    : {count:,} in {seconds:.2f}s\n"
        f"    throughput  : {rate:,.0f} msg/sec   ({mb / seconds:.2f} MB/sec)\n"
        f"    latency (ms): p50={stats['p50']:.3f}  p95={stats['p95']:.3f}  "
        f"p99={stats['p99']:.3f}  max={stats['max']:.3f}  mean={stats['mean']:.3f}"
    )


def bench_produce(port: int, topic: str, count: int, value: str) -> tuple:
    """Send `count` messages one at a time, timing each round trip."""
    latencies = []
    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        payload = {'topic': topic, 'partition': 0, 'value': value, 'key': None}

        # Warm-up: the first request pays for lazy partition creation, file
        # opening, and Python's import/JIT-ish warm-up costs. Including it
        # would put a huge outlier in the max column that says nothing about
        # steady-state behaviour.
        conn.request(API_PRODUCE, payload)

        started = time.perf_counter()
        for _ in range(count):
            # perf_counter, not time.time(): it is monotonic (never jumps
            # backwards when NTP adjusts the clock) and has much finer
            # resolution. Using time.time() for durations is a classic way to
            # record a negative latency at 2am.
            t0 = time.perf_counter()
            conn.request(API_PRODUCE, payload)
            latencies.append((time.perf_counter() - t0) * 1000)
        elapsed = time.perf_counter() - started

    return elapsed, latencies


def bench_produce_pipelined(port: int, topic: str, count: int, value: str,
                            depth: int) -> tuple:
    """
    Send `count` messages with up to `depth` requests in flight at once.

    Identical work, identical protocol, identical broker — the only difference
    is that the client stops waiting for each acknowledgement before sending
    the next request. That is possible solely because the header now carries a
    correlation_id to match replies by.

    Latency is measured per BATCH, not per message: with `depth` requests in
    flight there is no meaningful per-message round trip to time. Compare the
    throughput column, not the latency column.

    Depth is bounded deliberately. Sending unboundedly many requests before
    reading any reply deadlocks once both socket buffers fill — we block
    writing while the broker blocks writing replies nobody is draining. Real
    clients cap this with `max.in.flight.requests.per.connection`.
    """
    latencies = []
    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        payload = {'topic': topic, 'partition': 0, 'value': value, 'key': None}
        conn.request(API_PRODUCE, payload)          # warm-up

        started = time.perf_counter()
        remaining = count
        while remaining > 0:
            batch = min(depth, remaining)
            t0 = time.perf_counter()
            conn.pipeline([(API_PRODUCE, payload)] * batch)
            latencies.append((time.perf_counter() - t0) * 1000)
            remaining -= batch
        elapsed = time.perf_counter() - started

    return elapsed, latencies


def bench_roundtrip(port: int, count: int, depth: int) -> tuple:
    """
    Time the cheapest possible request, sequentially and pipelined.

    WHY THIS MEASUREMENT EXISTS
      Comparing sequential and pipelined PRODUCE alone cannot tell you *why*
      the numbers came out as they did, because a PRODUCE mixes two very
      different costs: the network round trip (which pipelining removes) and
      the broker's own work — a thread hop to the executor, a file write, a
      flush (which pipelining cannot touch, since the broker handles one
      request per connection at a time).

      METADATA on an empty registry does no disk I/O and no thread hop, so its
      per-request cost is protocol overhead plus the round trip and nothing
      else. Subtracting the pipelined time from the sequential time isolates
      the WAIT — which is the difference between a benchmark that reports a
      number and one that explains it.

    MUST RUN FIRST, on an empty broker. Once topics exist, the METADATA
    response grows and gets more variable, and the subtraction — a difference
    of two similar numbers — drowns in that noise. (It measured *negative* on
    the first attempt for exactly this reason.) Differences of similar
    quantities are always the fragile part of a measurement; give them the
    quietest conditions you can.

    Returns (per_request_sequential_us, per_request_pipelined_us).
    """
    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        for _ in range(20):
            conn.request(API_METADATA, {})          # warm-up

        started = time.perf_counter()
        for _ in range(count):
            conn.request(API_METADATA, {})
        sequential = time.perf_counter() - started

        started = time.perf_counter()
        remaining = count
        while remaining > 0:
            batch = min(depth, remaining)
            conn.pipeline([(API_METADATA, {})] * batch)
            remaining -= batch
        pipelined = time.perf_counter() - started

    return sequential / count * 1e6, pipelined / count * 1e6


def bench_consume(port: int, topic: str, count: int, batch: int) -> tuple:
    """Read everything back, timing each fetch round trip."""
    latencies = []
    received = 0
    offset = 0

    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        started = time.perf_counter()
        while received < count:
            t0 = time.perf_counter()
            response = conn.request(API_FETCH, {
                'topic': topic, 'partition': 0, 'offset': offset, 'max': batch,
            })
            latencies.append((time.perf_counter() - t0) * 1000)

            messages = response.get('messages', [])
            if not messages:
                break
            received += len(messages)
            offset = messages[-1]['offset'] + 1
        elapsed = time.perf_counter() - started

    return elapsed, latencies, received


def bench_produce_batched(port: int, topic: str, count: int, value: str,
                          linger_ms: float = 5.0, partitions_hint: int = 1) -> tuple:
    """
    Produce through the real batching producer.

    **THE Phase 3 measurement.** Phase 1 showed that ~215 us of every 253 us
    produce was broker work — a thread hop, a file write, a flush, a JSON
    decode — paid once per message because one request carried one message.
    Pipelining could not touch it, because the broker was never idle.

    Batching is the only thing that can: put 200 messages in one request and
    that per-request cost is paid once for all of them instead of 200 times.

    Uses `send()` + `flush()`, not `send_and_wait()`. The latter waits for each
    message's reply, which defeats batching entirely — it is the Phase 2
    behaviour with extra steps, and measuring it here would report the wrong
    number confidently.
    """
    latencies = []
    started = time.perf_counter()

    with Producer('127.0.0.1', port, topic, quiet=True,
                  linger_ms=linger_ms, batch_size=16384) as producer:
        futures = []
        for _ in range(count):
            t0 = time.perf_counter()
            futures.append(producer.send(value))
            # Time only the CALLER's cost. With batching, send() returns
            # without sending, so this measures how long the application is
            # blocked — which is the thing an application actually feels.
            latencies.append((time.perf_counter() - t0) * 1000)
        producer.flush()

    elapsed = time.perf_counter() - started
    return elapsed, latencies


def bench_seek_profile(port: int, topic: str, count: int, samples: int = 9) -> list:
    """
    Time a fetch from evenly spaced points through the log.

    **THE Phase 2 measurement.** It answers one question: does read cost depend
    on how much data the log already holds?

        Phase 1 (scan from byte zero)   latency climbs linearly with offset
        Phase 2 (segments + index)      latency is flat

    A throughput number cannot show this — throughput averages the cheap early
    reads with the expensive late ones and reports something in between. You
    have to sample *across* the log to see the shape.

    Each sample is repeated and the MINIMUM is taken. For a question about
    algorithmic cost, the minimum is the cleanest estimator: it strips out
    scheduler noise and background load, which add time but never remove it.
    (For a question about user experience you would want p99 instead — the
    right statistic depends on the question.)
    """
    results = []
    repeats = 5

    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        conn.request(API_FETCH, {'topic': topic, 'offset': 0, 'max': 1})   # warm-up

        for i in range(samples):
            offset = int(count * i / samples)
            best = float('inf')
            for _ in range(repeats):
                t0 = time.perf_counter()
                conn.request(API_FETCH,
                             {'topic': topic, 'offset': offset, 'max': 10})
                best = min(best, (time.perf_counter() - t0) * 1000)
            results.append((offset, best))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone benchmark  (run with: python -m bench.bench)')
    parser.add_argument('--count', type=int, default=2000,
                        help='Messages to produce and consume')
    parser.add_argument('--size', type=int, default=100,
                        help='Message payload size in bytes')
    parser.add_argument('--batch', type=int, default=100,
                        help='Messages per fetch request')
    parser.add_argument('--pipeline-depth', type=int, default=100,
                        help='Requests in flight at once for the pipelined run')
    parser.add_argument('--data-dir', default=None,
                        help='Defaults to a fresh temp directory')
    args = parser.parse_args()

    import tempfile
    data_dir = args.data_dir or tempfile.mkdtemp(prefix='kafka-bench-')
    topic = f"bench-{int(time.time())}"
    value = 'x' * args.size

    # Run the broker in-process with logging OFF. A log line per message costs
    # far more than the message, and we would end up benchmarking the
    # terminal. Measuring the wrong thing confidently is worse than not
    # measuring at all.
    import logging
    logging.disable(logging.CRITICAL)

    broker = Broker(host='127.0.0.1', port=0, data_dir=data_dir)
    thread = threading.Thread(target=lambda: asyncio.run(broker.start()), daemon=True)
    thread.start()
    if not broker.ready.wait(timeout=5):
        raise SystemExit("Broker failed to start")

    print(f"Kafka-clone benchmark")
    print(f"  broker    : 127.0.0.1:{broker.port} (in-process, loopback)")
    print(f"  data dir  : {data_dir}")
    print(f"  messages  : {args.count:,} x {args.size} bytes")
    print(f"  fetch size: {args.batch} messages/request")

    try:
        # FIRST, while the broker is still empty — see bench_roundtrip.
        cheap_us, cheap_pipe_us = bench_roundtrip(
            broker.port, args.count, args.pipeline_depth)
        rtt_us = cheap_us - cheap_pipe_us

        p_elapsed, p_latencies = bench_produce(broker.port, topic, args.count, value)
        print(format_row('PRODUCE sequential (one round trip per message)',
                         args.count, p_elapsed, args.size, summarise(p_latencies)))

        pipe_topic = topic + '-pipelined'
        q_elapsed, q_latencies = bench_produce_pipelined(
            broker.port, pipe_topic, args.count, value, args.pipeline_depth)
        print(format_row(
            f'PRODUCE pipelined ({args.pipeline_depth} requests in flight)',
            args.count, q_elapsed, args.size, summarise(q_latencies)))

        speedup = (p_elapsed / q_elapsed) if q_elapsed else 0
        print(f"\n    → pipelining is {speedup:.2f}x on identical work")

        b_elapsed, b_latencies = bench_produce_batched(
            broker.port, topic + '-batched', args.count, value)
        print(format_row('PRODUCE batched (linger_ms=5, batch_size=16KB)',
                         args.count, b_elapsed, args.size, summarise(b_latencies)))
        batch_speedup = (p_elapsed / b_elapsed) if b_elapsed else 0
        print(f"\n    → batching is {batch_speedup:.1f}x faster than sequential"
              f"\n      (latency shown is the CALLER's cost — send() returns "
              f"without sending)")

        produce_us = p_elapsed / args.count * 1e6
        work_us = max(produce_us - rtt_us, 1.0)

        print(f"\n  COST BREAKDOWN (per request, microseconds)")
        print(f"    cheap request (METADATA), sequential : {cheap_us:7.1f}")
        print(f"    same request, pipelined              : {cheap_pipe_us:7.1f}")
        print(f"    → loopback round-trip wait           : {rtt_us:7.1f}"
              f"   ← what pipelining removes")
        print(f"    PRODUCE, sequential                  : {produce_us:7.1f}")
        print(f"    → broker work (thread hop + disk)    : {work_us:7.1f}"
              f"   ← what pipelining CANNOT remove")
        if rtt_us <= 0:
            print("    (round trip measured at or below the noise floor — "
                  "rerun on an idle machine)")

        c_elapsed, c_latencies, received = bench_consume(
            broker.port, topic, args.count, args.batch)
        print(format_row(f'CONSUME ({args.batch} messages per round trip)',
                         received, c_elapsed, args.size, summarise(c_latencies)))

        # ── The Phase 2 result ─────────────────────────────────
        profile = bench_seek_profile(broker.port, topic, args.count)
        first_ms = profile[0][1]
        last_ms = profile[-1][1]
        slope = (last_ms / first_ms) if first_ms else 0

        print(f"\n  SEEK PROFILE — fetch latency vs position in the log")
        print(f"    {'OFFSET':>10}  {'LATENCY (ms)':>13}")
        for offset, ms in profile:
            bar = '█' * max(1, int(ms / max(last_ms, first_ms, 0.001) * 30))
            print(f"    {offset:>10}  {ms:>13.3f}  {bar}")
        print(f"\n    end/start ratio: {slope:.2f}x"
              f"   ← flat (~1x) means read cost is independent of log size")
    finally:
        broker.stop()
        thread.join(timeout=5)

    # Projections from the numbers just measured. Loopback is the WORST case
    # for pipelining, so extrapolating is the only way to see what it is for.
    def projected(rtt_ms):
        rtt = rtt_ms * 1000
        return (1e6 / (work_us + rtt), 1e6 / work_us)

    print(f"""
  Reading these numbers
  ---------------------
  Pipelining came out at {speedup:.2f}x, and that is the honest result: on
  LOOPBACK it barely helps. It is worth understanding exactly why, because
  the reason is more useful than the number.

  Pipelining removes the time a client spends WAITING for a reply. The cost
  breakdown above shows that on loopback the wait is only ~{rtt_us:.0f} us,
  while a PRODUCE costs ~{produce_us:.0f} us -- the other ~{work_us:.0f} us is
  the broker actually working: a thread hop to the executor, a file write, a
  flush. The broker is NOT idle during a produce, so there is little idle time
  to reclaim.

  A second reason: our broker handles one request per connection at a time
  (read, route, reply, repeat). Pipelined requests therefore queue up and are
  still processed serially. Pipelining removes the CLIENT's waiting, not the
  BROKER's serialisation. Real Kafka behaves the same way per connection --
  that in-order handling is exactly what makes its ordering guarantee work.

  So when IS it worth four bytes of header? When the round trip dominates the
  work -- which is every real network. Using the ~{work_us:.0f} us of broker
  work measured above:

      round trip     sequential      pipelined     speedup
      ----------     ----------      ---------     -------""")
    for rtt_ms, label in ((0.035, 'loopback'), (0.5, 'same LAN'),
                          (5.0, 'same region'), (50.0, 'cross-region')):
        seq, pipe = projected(rtt_ms)
        print(f"      {rtt_ms:6.3f} ms     {seq:9,.0f}/s    {pipe:9,.0f}/s"
              f"     {pipe / seq:6.1f}x   ({label})")

    print("""
  That is the mechanism the correlation_id bought. It is invisible here and
  decisive at 50 ms.

  The real fix for produce throughput is BATCHING -- many messages inside ONE
  request -- which cuts the per-message broker work as well as the waiting.
  That is Phase 3, and it is the row that will move this benchmark.

  Watch consume latency as the log grows: `fetch` still scans the file from
  byte zero every time, so it gets steadily worse with more data. Phase 2's
  segments and index are what flatten that curve.

  Record this run in docs/benchmarks.md.""")


if __name__ == '__main__':
    main()
