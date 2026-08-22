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
from common.protocol import MSG_FETCH, MSG_PRODUCE


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
        conn.request(MSG_PRODUCE, payload)

        started = time.perf_counter()
        for _ in range(count):
            # perf_counter, not time.time(): it is monotonic (never jumps
            # backwards when NTP adjusts the clock) and has much finer
            # resolution. Using time.time() for durations is a classic way to
            # record a negative latency at 2am.
            t0 = time.perf_counter()
            conn.request(MSG_PRODUCE, payload)
            latencies.append((time.perf_counter() - t0) * 1000)
        elapsed = time.perf_counter() - started

    return elapsed, latencies


def bench_consume(port: int, topic: str, count: int, batch: int) -> tuple:
    """Read everything back, timing each fetch round trip."""
    latencies = []
    received = 0
    offset = 0

    with SyncConnection('127.0.0.1', port, timeout=30.0) as conn:
        started = time.perf_counter()
        while received < count:
            t0 = time.perf_counter()
            response = conn.request(MSG_FETCH, {
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone benchmark  (run with: python -m bench.bench)')
    parser.add_argument('--count', type=int, default=2000,
                        help='Messages to produce and consume')
    parser.add_argument('--size', type=int, default=100,
                        help='Message payload size in bytes')
    parser.add_argument('--batch', type=int, default=100,
                        help='Messages per fetch request')
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
        p_elapsed, p_latencies = bench_produce(broker.port, topic, args.count, value)
        print(format_row('PRODUCE (one round trip per message)',
                         args.count, p_elapsed, args.size, summarise(p_latencies)))

        c_elapsed, c_latencies, received = bench_consume(
            broker.port, topic, args.count, args.batch)
        print(format_row(f'CONSUME ({args.batch} messages per round trip)',
                         received, c_elapsed, args.size, summarise(c_latencies)))
    finally:
        broker.stop()
        thread.join(timeout=5)

    print("""
  Reading these numbers
  ---------------------
  Produce is one full network round trip per message, so its throughput is
  capped by latency, not by the broker. Consume fetches many messages per
  round trip, which is why it looks so much faster -- that difference IS the
  argument for the producer batching arriving in Phase 3.

  Watch consume latency as the log grows: `fetch` scans the file from byte
  zero every time, so it gets steadily worse with more data. Phase 2's
  segments and index are what flatten that curve.

  Record this run in docs/benchmarks.md.""")


if __name__ == '__main__':
    main()
