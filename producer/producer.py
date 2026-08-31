"""
CONCEPT: The Producer — the Write Side, and the Load Balancer
----------------------------------------------------------------
The producer creates messages, decides which partition each one belongs to,
batches them, and ships them to the broker.

That second job is the important one. **There is no load balancer in front of a
Kafka cluster** — the producer holds cluster metadata and routes each message
itself. This is what makes Kafka clients "thick", and it is why `METADATA` and
`NOT_LEADER_OR_FOLLOWER` were built back in Phase 1.

    producer.send('order placed', key='cust-42')
        │
        ├─ metadata: topic 'orders' has 4 partitions
        ├─ partitioner: murmur2('cust-42') % 4 → partition 3
        ├─ accumulator: buffer it in partition 3's batch
        └─ returns a Future immediately
                            ⋮
        sender thread: batch full (or linger_ms elapsed) → one request


THREE THINGS CHANGED IN PHASE 3
---------------------------------
**1. `send()` is asynchronous.** It returns a `FutureRecordMetadata`, not an
offset. This is forced, not stylistic: returning an offset means waiting for
the reply, and a message that waits for its own reply was never batched with
anything. Blocking and batching are mutually exclusive. `send_and_wait()`
exists for the cases where blocking is genuinely what you want.

**2. The key finally does something.** It has been carried and ignored since
Phase 0. Now `murmur2(key) % num_partitions` routes it — which is what
guarantees that every event for one customer lands in one partition and is
therefore strictly ordered.

**3. `acks` is real.** `acks=0` means the broker sends no reply at all, which
is worth seeing in a protocol that is otherwise strictly request/response.


WHAT BATCHING BOUGHT, AND WHAT IT COST
----------------------------------------
The Phase 1 breakdown said 215 us of every 253 us produce was broker work,
paid once per message. Batching amortises that over a whole batch — the single
largest win available in the project, and `bench/bench.py` measures it.

The cost is latency: with `linger_ms=5`, a message sent into an empty batch
waits up to 5 ms before anyone sees it. You bought throughput with latency, and
`linger_ms=0` buys it back at the price of most of the throughput. There is no
setting that is right for everything, which is why it is a setting.
"""

import argparse
import logging
import random
import threading
import time

from common.cluster_client import ClusterClient
from common.config import DEFAULT_HOST, DEFAULT_PORT
from common.errors import KafkaError, RetriableError, error_for_code, raise_for_error
from common.net import BrokerConnectionError
from common.protocol import API_PRODUCE
from producer.batch import RecordAccumulator, Sender
from producer.partitioner import DefaultPartitioner

log = logging.getLogger(__name__)

# PRODUCE v1 carries batches for several partitions with per-partition error
# codes. v0 (one record, one partition) still works — see the Phase 1 notes on
# why per-API versioning is what makes a rolling upgrade possible.
PRODUCE_VERSION_BATCHED = 1


class Producer:
    """
    An asynchronous, batching, partition-aware producer.

        with Producer(topic='orders') as p:
            p.send('order placed', key='cust-42')
            p.send('order shipped', key='cust-42')   # same partition, ordered
            p.flush()
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 topic: str = 'default', quiet: bool = False,
                 acks: int = 1,
                 batch_size: int = 16384,
                 linger_ms: float = 5.0,
                 retries: int = 3,
                 retry_backoff_ms: float = 100.0,
                 max_backoff_ms: float = 5000.0,
                 sticky: bool = True):
        self.host = host
        self.port = port
        self.topic = topic
        self.quiet = quiet

        # 0   = fire and forget; the broker sends NO reply
        # 1   = the leader has it in its log
        # all = every in-sync replica has it (identical to 1 until Phase 6)
        self.acks = acks

        self.retries = retries
        self.retry_backoff_ms = retry_backoff_ms
        self.max_backoff_ms = max_backoff_ms

        # From Phase 5 the producer talks to a CLUSTER, not a broker. The
        # ClusterClient owns one connection per broker plus the metadata cache,
        # and routes each partition's batch to whichever broker leads it —
        # including the refresh-and-retry when that map goes stale.
        self._cluster = ClusterClient([(host, port)], timeout=30.0)

        # CONCEPT: one socket, two threads — and why this lock is mandatory.
        #
        # The SENDER thread uses the connection for PRODUCE. A CALLER thread
        # uses it for METADATA, inside send(), when it needs a partition count.
        # A SyncConnection is a request followed by a matching response, and
        # that pair is NOT atomic:
        #
        #     sender  ──PRODUCE──►
        #     caller  ──METADATA─►
        #     sender  ◄──────────── reads the METADATA reply as its own
        #
        # After that the stream is permanently desynchronised: the next read
        # starts mid-message and interprets body bytes as a header. The symptom
        # is "Bad magic 0x7b226572" — which is `{"er`, the beginning of a JSON
        # body. A confusing error a long way from its cause.
        #
        # This bug is invisible in single-threaded tests and appeared the
        # moment Phase 3 added a background sender. Correlation IDs would let
        # a smarter client demultiplex the replies; a lock is the honest
        # simple answer, and metadata lookups are cached so it is rarely
        # contended.
        self._conn_lock = threading.Lock()

        self._partitioner = DefaultPartitioner(sticky=sticky)
        self._accumulator = RecordAccumulator(batch_size=batch_size,
                                              linger_ms=linger_ms)
        self._sender = None

        # Cached partition counts, refreshed from METADATA on first use of a
        # topic. A producer that asked the broker how many partitions a topic
        # has before EVERY message would spend more time asking than sending —
        # this cache is the same idea as a client's metadata cache in real
        # Kafka, and it goes stale for the same reasons.
        self._partition_counts = {}

    # ── Lifecycle ──────────────────────────────────────────────

    def connect(self) -> 'Producer':
        self._cluster.refresh()
        self._sender = Sender(self._accumulator, self._send_batches)
        self._sender.start()
        self._log(f"connected to broker at {self.host}:{self.port} "
                  f"(acks={self.acks})")
        return self

    def close(self) -> None:
        """
        Flush everything buffered, stop the sender, close the socket.

        The order matters. Stopping the sender first would strand whatever is
        still in the accumulator, and those futures would never resolve —
        callers waiting on them would hang until their timeout with no
        explanation of why.
        """
        if self._sender is not None:
            try:
                self._sender.flush()
            finally:
                self._sender.stop()
                self._sender = None
        self._cluster.close()

    def __enter__(self) -> 'Producer':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Sending ────────────────────────────────────────────────

    def send(self, value: str, key: str = None, partition: int = None,
             topic: str = None):
        """
        Buffer one message. Returns a Future, immediately — nothing is sent yet.

        Args:
            value:     the message body.
            key:       routing key. Same key → same partition → ordered.
                       None → the sticky partitioner spreads messages.
            partition: bypass the partitioner entirely. Useful for tests and
                       for deliberate placement; skips the ordering guarantee
                       the key would have given you.
            topic:     override this producer's default topic.

        The returned Future resolves with the assigned offset once the broker
        acknowledges. With `acks=0` it resolves with -1 as soon as the bytes
        are on the wire, because there is no acknowledgement to wait for.
        """
        topic = topic or self.topic
        num_partitions = self._num_partitions(topic)
        target = self._partitioner.partition(topic, key, num_partitions, partition)

        future, became_full = self._accumulator.append(topic, target, key, value)

        if became_full and key is None and partition is None:
            # The sticky partitioner fills ONE partition's batch, then moves on.
            # Telling it here — rather than on a timer — is what keeps batches
            # large while still spreading load evenly over time.
            self._partitioner.on_batch_full(topic, num_partitions)

        return future

    def send_and_wait(self, value: str, key: str = None,
                      partition: int = None, topic: str = None,
                      timeout: float = 30.0) -> int:
        """
        Send one message and block for its offset.

        Honestly named: this is `send()` plus a wait, not a different
        mechanism. Convenient for tests and demos, and it gives up the
        batching — every call waits out a full round trip, which is exactly the
        Phase 2 behaviour this phase was built to escape. Use `send()` plus
        `flush()` when throughput matters.
        """
        future = self.send(value, key=key, partition=partition, topic=topic)
        self.flush()
        return future.get(timeout)

    def send_many(self, values, key: str = None, partition: int = None,
                  topic: str = None) -> list:
        """
        Send an iterable of values and return their offsets, in order.

        Now genuinely batched: every message is buffered first, then one
        `flush()` ships them together. Through Phase 2 this was a loop of
        individual round trips.
        """
        futures = [self.send(v, key=key, partition=partition, topic=topic)
                   for v in values]
        self.flush()
        return [f.get() for f in futures]

    def flush(self, timeout: float = 30.0) -> None:
        """Send everything buffered and wait for it to be acknowledged."""
        if self._sender is not None:
            self._sender.flush(timeout)

    # ── The send path (sender thread) ──────────────────────────

    def _send_batches(self, batches: list) -> None:
        """
        Ship drained batches. Called on the sender thread, never by callers.

        Batches are grouped by topic so that several partitions travel in ONE
        request — which is what PRODUCE v1's per-partition structure exists
        for, and why its errors are per-partition too.
        """
        by_topic = {}
        for batch in batches:
            by_topic.setdefault(batch.topic, []).append(batch)

        for topic, topic_batches in by_topic.items():
            try:
                self._send_topic_batches(topic, topic_batches)
            except Exception as exc:
                for batch in topic_batches:
                    batch.fail(exc)

    def _send_topic_batches(self, topic: str, batches: list) -> None:
        """
        Ship one topic's batches, grouped by the broker that leads them.

        CONCEPT: batching is now bounded by leadership.
          Phase 3 sent every partition of a topic in one request, because there
          was one broker. Now partition 0 may live on broker 1 and partition 3
          on broker 2 — so a request can only carry the partitions its
          recipient actually leads.

          That is a real cost of distributing: fewer partitions per request
          means less amortisation. A 6-partition topic on 3 brokers gets 3
          requests where it used to get 1. Kafka pays exactly this, and it is
          part of why partition counts and broker counts are tuned together.
        """
        by_leader = {}
        for batch in batches:
            try:
                leader = self._cluster.leader_for(topic, batch.partition)
            except KafkaError as exc:
                batch.fail(exc)
                continue
            by_leader.setdefault(leader, []).append(batch)

        for leader, leader_batches in by_leader.items():
            self._send_to_leader(topic, leader, leader_batches)

    def _send_to_leader(self, topic: str, leader: int, batches: list) -> None:
        payload = {
            'topic': topic,
            'acks': self.acks,
            'partitions': [
                {'partition': batch.partition, 'records': batch.records}
                for batch in batches
            ],
        }

        response = self._request_with_retries(topic, batches[0].partition, payload)

        if response is None:
            # acks=0: no reply exists, so there is no offset to report and no
            # error to detect. THAT IS THE SEMANTIC, not an oversight — you
            # asked not to be told.
            for batch in batches:
                batch.complete(-1)
            return

        raise_for_error(response, topic=topic)

        results = {entry['partition']: entry
                   for entry in response.get('partitions', [])}

        for batch in batches:
            entry = results.get(batch.partition)
            if entry is None:
                batch.fail(KafkaError(
                    f"broker did not report partition {batch.partition}"))
            elif entry.get('error_code', 0) != 0:
                # Per-partition errors are why v1 exists: one bad partition
                # must not fail the nine that succeeded.
                batch.fail(error_for_code(entry['error_code'],
                                          entry.get('error_message', ''),
                                          topic=topic, partition=batch.partition))
            else:
                batch.complete(entry['base_offset'])

    def _request_with_retries(self, topic: str, partition: int, payload: dict):
        """
        Send one PRODUCE request, retrying retriable failures.

        CONCEPT: exponential backoff WITH JITTER.
          Doubling the delay stops a struggling broker being hammered. The
          jitter is the part people leave out, and it matters: without it,
          every producer that failed at the same instant retries at the same
          instant, and the broker that just came back is knocked over by a
          synchronised thundering herd. Randomising spreads them out.

        Only RetriableError and connection failures are retried. Retrying an
        INVALID_REQUEST would resend identical bad bytes forever — which is
        exactly the distinction the Phase 1 error taxonomy was built to make,
        and why it is `except RetriableError` here rather than a string match.
        """
        attempt = 0
        while True:
            try:
                # The lock must cover the request AND its response as one unit
                # — that pair is what another thread must not interleave with.
                with self._conn_lock:
                    if self.acks == 0:
                        # No reply is coming, so do not read one. The
                        # connection stays in sync because the broker
                        # genuinely sends nothing back.
                        #
                        # acks=0 forfeits leader routing too: with no response
                        # there is no NOT_LEADER to learn from, so a message
                        # sent to a stale leader is simply lost. That is the
                        # semantic, not an oversight — you asked not to be told.
                        node_id = self._cluster.leader_for(topic, partition)
                        self._cluster.connection_for(node_id).send_request(
                            API_PRODUCE, payload, PRODUCE_VERSION_BATCHED)
                        return None

                    return self._cluster.request_for_partition(
                        topic, partition, API_PRODUCE, payload,
                        PRODUCE_VERSION_BATCHED)

            except (RetriableError, BrokerConnectionError) as exc:
                if attempt >= self.retries:
                    raise
                delay = min(self.retry_backoff_ms * (2 ** attempt),
                            self.max_backoff_ms)
                delay *= 0.5 + random.random()        # jitter
                self._log(f"retriable failure ({exc}); retry "
                          f"{attempt + 1}/{self.retries} in {delay:.0f}ms")
                time.sleep(delay / 1000.0)
                attempt += 1

                if isinstance(exc, BrokerConnectionError):
                    # The broker we reached is gone. Drop the cached map so the
                    # retry re-resolves the leader rather than dialling the
                    # same dead address again.
                    with self._conn_lock:
                        self._cluster.invalidate()

    # ── Metadata ───────────────────────────────────────────────

    def _num_partitions(self, topic: str) -> int:
        """
        How many partitions this topic has, cached by the ClusterClient.

        The cache is why a producer can route a message without a network round
        trip, and why a client can be WRONG after a topic changes — the general
        shape of every metadata cache, and the reason failover works by
        invalidating on an error rather than by polling.

        `allow_auto_create` matters: without it a brand-new topic answers "does
        not exist", we fall back to 1, and every keyed message goes to
        partition 0 — then the first PRODUCE creates it at its real width and
        the cached 1 is wrong. Real Kafka producers set exactly this flag.
        """
        cached = self._partition_counts.get(topic)
        if cached is not None:
            return cached

        with self._conn_lock:
            count = self._cluster.partition_count(topic, allow_auto_create=True)
        self._partition_counts[topic] = count
        return count

    def refresh_metadata(self) -> None:
        """Drop both caches so the next send re-reads counts and leaders."""
        self._partition_counts.clear()
        self._cluster.invalidate()

    # ── Internals ──────────────────────────────────────────────

    def _log(self, message: str) -> None:
        if not self.quiet:
            print(f"[Producer] {message}")

    def __repr__(self) -> str:
        return (f"Producer(topic={self.topic!r}, broker={self.host}:{self.port}, "
                f"acks={self.acks})")


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone producer  (run with: python -m producer.producer)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--count', type=int, default=5)
    parser.add_argument('--prefix', default='message')
    parser.add_argument('--keys', type=int, default=3,
                        help='Distinct keys to cycle through; 0 for keyless')
    parser.add_argument('--acks', default='1', choices=['0', '1', 'all'])
    parser.add_argument('--linger-ms', type=float, default=5.0)
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    acks = 0 if args.acks == '0' else (1 if args.acks == '1' else 'all')

    started = time.perf_counter()
    with Producer(args.host, args.port, args.topic, quiet=args.quiet,
                  acks=acks, linger_ms=args.linger_ms) as producer:
        futures = [
            producer.send(f"{args.prefix}-{i}",
                          key=f"key-{i % args.keys}" if args.keys else None)
            for i in range(args.count)
        ]
        producer.flush()

    elapsed = time.perf_counter() - started

    if acks == 0:
        print(f"\nSent {args.count} messages to {args.topic!r} with acks=0 "
              f"in {elapsed:.2f}s.\nNo offsets: with acks=0 the broker sends no "
              f"reply, so success is unverifiable by design.")
    else:
        spread = {}
        for future in futures:
            spread[future.partition] = spread.get(future.partition, 0) + 1
        print(f"\nSent {args.count} messages to {args.topic!r} in {elapsed:.2f}s "
              f"({args.count / elapsed:,.0f} msg/sec).")
        print("Partition spread:  " +
              "  ".join(f"p{p}={n}" for p, n in sorted(spread.items())))


if __name__ == '__main__':
    main()
