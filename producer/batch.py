"""
CONCEPT: Batching — The Thing That Actually Makes Producers Fast
------------------------------------------------------------------
The Phase 1 measurement, unchanged by Phase 2:

    PRODUCE, sequential           253 us per message
      -> loopback round trip       38 us    (pipelining removes this)
      -> broker work              215 us    (nothing had touched it)

215 microseconds of thread hop, file write, flush, and JSON encoding — paid
once per message, because one request carried one message. Phase 1 proved
pipelining could not help: the broker was never idle, so there was no idle time
to reclaim.

The only way to cut per-message cost is to stop sending one message per
request.

    WITHOUT batching                    WITH batching (linger_ms = 5)

    send ──► [1 msg] ──► broker         send ─┐
    send ──► [1 msg] ──► broker         send ─┼─ accumulate ─┐
    send ──► [1 msg] ──► broker         send ─┘               │
                                                    [200 msgs] ──► broker

    200 requests, 200x broker work      1 request, ~1x broker work


THE API CHANGE IS FORCED, NOT COSMETIC
----------------------------------------
Through Phase 2, `send()` returned an offset. It cannot any more.

To return an offset, `send()` must wait for the broker's reply — and a message
that waits for its own reply is a message that was never batched with anything.
Blocking and batching are mutually exclusive.

So `send()` returns a **Future**: a receipt you can wait on later, or ignore.
The caller keeps going, messages pile up, and a background thread ships them
together. Kafka's producer API works exactly this way, and now the reason is
visible: the Future is what makes batching possible from the caller's side.

    future = producer.send('order placed', key='cust-42')   # returns at once
    producer.flush()                                        # wait for all
    offset = future.get()                                   # or wait for one


WHAT BATCHING COSTS
---------------------
Latency, and it is a real bill. With `linger_ms=5`, a message sent into an
empty batch waits up to 5 ms before anyone sees it. You bought throughput with
latency; `linger_ms=0` buys it back and gives up most of the throughput.

There is no setting that is right for everything, which is why it is a setting.
"""

import threading
import time


class FutureRecordMetadata:
    """
    A receipt for a message that has not been sent yet.

    Resolved by the sender thread with an offset, or with the exception that
    stopped it. Waiting is `threading.Event`, so `get()` blocks efficiently
    rather than spinning.
    """

    __slots__ = ('_event', '_offset', '_exception', 'topic', 'partition')

    def __init__(self, topic: str, partition: int):
        self.topic = topic
        self.partition = partition
        self._event = threading.Event()
        self._offset = None
        self._exception = None

    # ── Resolution (sender thread only) ────────────────────────

    def set_result(self, offset: int) -> None:
        self._offset = offset
        self._event.set()

    def set_exception(self, exception: BaseException) -> None:
        self._exception = exception
        self._event.set()

    # ── Waiting (caller thread) ────────────────────────────────

    def get(self, timeout: float = 30.0) -> int:
        """
        Block until the message is acknowledged; return its offset.

        Raises whatever the send failed with, so an error surfaces at the point
        you actually asked about the result rather than vanishing into a
        background thread. **A producer that swallows errors in its sender
        thread is a producer that loses data quietly**, which is the failure
        mode this whole project keeps trying to avoid.
        """
        if not self._event.wait(timeout):
            raise TimeoutError(
                f"no acknowledgement for {self.topic}-{self.partition} "
                f"after {timeout}s")
        if self._exception is not None:
            raise self._exception
        return self._offset

    @property
    def done(self) -> bool:
        return self._event.is_set()

    @property
    def succeeded(self) -> bool:
        return self._event.is_set() and self._exception is None

    def __repr__(self) -> str:
        if not self.done:
            state = 'pending'
        elif self._exception is not None:
            state = f'failed({type(self._exception).__name__})'
        else:
            state = f'offset={self._offset}'
        return f"FutureRecordMetadata({self.topic}-{self.partition}, {state})"


class ProducerBatch:
    """
    Messages accumulating for one (topic, partition), plus their futures.

    One batch becomes one entry in a PRODUCE v1 request. The broker assigns a
    contiguous run of offsets, so the batch resolves its futures as
    `base_offset + i` — the reply carries one offset for many records, which is
    part of why batching saves bytes as well as round trips.
    """

    __slots__ = ('topic', 'partition', 'records', 'futures',
                 'size_bytes', 'created_at')

    # Rough per-record overhead in the JSON encoding: braces, field names,
    # quotes, commas. Used only to decide when a batch is "full", so an
    # estimate is fine — the alternative is serialising every record twice.
    RECORD_OVERHEAD = 40

    def __init__(self, topic: str, partition: int):
        self.topic = topic
        self.partition = partition
        self.records = []
        self.futures = []
        self.size_bytes = 0
        self.created_at = time.monotonic()

    def append(self, key, value: str) -> FutureRecordMetadata:
        future = FutureRecordMetadata(self.topic, self.partition)
        self.records.append({'key': key, 'value': value})
        self.futures.append(future)
        self.size_bytes += (len(value) + len(key or '') + self.RECORD_OVERHEAD)
        return future

    def is_full(self, batch_size: int) -> bool:
        return self.size_bytes >= batch_size

    def age_ms(self) -> float:
        # monotonic, not time.time(): this measures a DURATION, and wall clock
        # can jump backwards when NTP corrects it — which would make a batch
        # appear to have negative age and never be sent.
        return (time.monotonic() - self.created_at) * 1000

    def complete(self, base_offset: int) -> None:
        """Resolve every future with its offset."""
        for i, future in enumerate(self.futures):
            future.set_result(base_offset + i if base_offset >= 0 else -1)

    def fail(self, exception: BaseException) -> None:
        """Fail every future in this batch with the same error."""
        for future in self.futures:
            future.set_exception(exception)

    def __len__(self) -> int:
        return len(self.records)

    def __repr__(self) -> str:
        return (f"ProducerBatch({self.topic}-{self.partition}, "
                f"{len(self.records)} records, {self.size_bytes}B, "
                f"{self.age_ms():.1f}ms old)")


class RecordAccumulator:
    """
    In-memory buffers, one per (topic, partition), waiting to be sent.

    Thread-safe: caller threads append while the sender thread drains.

    NOT bounded, and that is a real limitation. A real producer has
    `buffer.memory` and blocks (or throws) when it is full, so a producer
    outrunning its broker applies backpressure to the application instead of
    growing until the process dies. Ours will happily grow forever — noted
    honestly rather than quietly.
    """

    def __init__(self, batch_size: int = 16384, linger_ms: float = 5.0):
        self.batch_size = batch_size
        self.linger_ms = linger_ms

        self._batches = {}                  # (topic, partition) -> ProducerBatch
        self._lock = threading.Lock()

        # Signalled when a batch becomes ready, so the sender wakes up
        # immediately on a full batch instead of sleeping out its linger.
        self._ready = threading.Condition(self._lock)

    def append(self, topic: str, partition: int, key, value: str):
        """
        Buffer one record. Returns (future, batch_became_full).

        `batch_became_full` tells the sticky partitioner to move on: the whole
        point of stickiness is to fill one partition's batch and then switch.
        """
        with self._lock:
            key_pair = (topic, partition)
            batch = self._batches.get(key_pair)
            if batch is None:
                batch = ProducerBatch(topic, partition)
                self._batches[key_pair] = batch

            future = batch.append(key, value)
            became_full = batch.is_full(self.batch_size)

            if became_full:
                self._ready.notify()
            return future, became_full

    def drain(self, force: bool = False) -> list:
        """
        Remove and return every batch that is ready to send.

        Ready means full, or older than `linger_ms`. `force=True` takes
        everything regardless — that is what `flush()` and `close()` use, since
        waiting out a linger at shutdown would just delay the exit.
        """
        with self._lock:
            ready = []
            for key_pair, batch in list(self._batches.items()):
                if force or batch.is_full(self.batch_size) \
                        or batch.age_ms() >= self.linger_ms:
                    ready.append(batch)
                    del self._batches[key_pair]
            return ready

    def wait_for_work(self, timeout: float) -> None:
        """
        Block until a batch is full or `timeout` elapses.

        The timeout is what implements `linger_ms`: with nothing full, the
        sender wakes after the linger and takes whatever has accumulated. The
        condition is what stops it waiting when a batch fills early.
        """
        with self._lock:
            if not self._batches:
                self._ready.wait(timeout)
            else:
                self._ready.wait(min(timeout, self.linger_ms / 1000.0))

    def has_pending(self) -> bool:
        with self._lock:
            return bool(self._batches)

    def pending_records(self) -> int:
        with self._lock:
            return sum(len(batch) for batch in self._batches.values())


class Sender(threading.Thread):
    """
    The background thread that ships batches.

    Kafka has exactly this — a thread named `kafka-producer-network-thread` —
    and for the same reason: `send()` must return immediately, so somebody else
    has to do the sending.

    A daemon thread so a forgotten `close()` cannot hang the process at exit.
    That is a safety net, not a licence: exiting without `close()` drops
    whatever is still buffered, which is why `Producer` is a context manager.
    """

    def __init__(self, accumulator: RecordAccumulator, send_fn,
                 poll_interval: float = 0.05):
        super().__init__(name='producer-sender', daemon=True)
        self.accumulator = accumulator
        self.send_fn = send_fn
        self.poll_interval = poll_interval

        self._running = threading.Event()
        self._running.set()
        self._idle = threading.Event()      # set when nothing is buffered
        self._idle.set()

    def run(self) -> None:
        while self._running.is_set():
            self.accumulator.wait_for_work(self.poll_interval)
            self._drain_once()

        # Drain whatever is left, so close() does not silently discard it.
        self._drain_once(force=True)

    def _drain_once(self, force: bool = False) -> None:
        batches = self.accumulator.drain(force=force)
        if not batches:
            if not self.accumulator.has_pending():
                self._idle.set()
            return

        self._idle.clear()
        try:
            self.send_fn(batches)
        except Exception as exc:
            # A bug in the send path must not kill the sender thread and leave
            # every future hanging forever. Fail the batches explicitly so
            # callers get an exception from `get()` rather than a timeout with
            # no explanation.
            for batch in batches:
                batch.fail(exc)
        finally:
            if not self.accumulator.has_pending():
                self._idle.set()

    def flush(self, timeout: float = 30.0) -> None:
        """Send everything buffered and wait for the sender to go idle."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._drain_once(force=True)
            if not self.accumulator.has_pending():
                return
        raise TimeoutError(f"flush did not complete within {timeout}s")

    def stop(self, timeout: float = 30.0) -> None:
        self._running.clear()
        self.accumulator._ready.acquire()
        try:
            self.accumulator._ready.notify_all()
        finally:
            self.accumulator._ready.release()
        self.join(timeout)
