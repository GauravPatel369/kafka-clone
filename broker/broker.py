"""
CONCEPT: The Broker — A TCP Server
------------------------------------
The broker is the long-running server process at the centre of the system. It:

  1. Listens on a TCP port (9092, matching real Kafka)
  2. Accepts connections from producers and consumers
  3. Routes each request to the right partition
  4. Sends back a response

                 ┌─────────────────────────────────────┐
                 │             BROKER                  │
    Producer A ──┤── handle_client() ──┐               │
    Producer B ──┤── handle_client()   ├─► _route() ──┐│
    Consumer   ──┤── handle_client() ──┘              ││
                 │                                    ▼│
                 │                      partitions{}   │
                 │              orders-0.log  orders-1.log
                 └─────────────────────────────────────┘


WHY ASYNCIO AND NOT THREADS?
------------------------------
A broker's job is almost entirely waiting. Waiting for a request to arrive,
waiting for a disk write, waiting for a socket to drain. Actual computation is
a rounding error.

  ┌──────────────────┬────────────────────────────────────────────────────┐
  │ Model            │ Trade-off                                          │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Thread per       │ Dead simple — blocking code just works. But each   │
  │ connection       │ thread costs ~8 MB of stack and a context switch.  │
  │                  │ Falls over somewhere around 1,000 clients.         │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Process per      │ True parallelism and crash isolation. Heavier      │
  │ connection       │ still, and processes cannot share the partition    │
  │                  │ registry without IPC.                              │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Event loop       │ One thread, one small coroutine per connection     │
  │ (asyncio)        │ (~KB). Tens of thousands of clients. Cost: ONE     │
  │  ← WE USE THIS   │ blocking call freezes EVERY connection, and you    │
  │                  │ get one CPU core.                                  │
  └──────────────────┴────────────────────────────────────────────────────┘

That "one blocking call freezes everything" cost is not hypothetical — our
storage layer does blocking file I/O. So `_route` hands those calls to a
thread pool via `run_in_executor`: the loop stays free while a worker blocks
on the disk. Mixing the two models is the standard asyncio pattern, and
forgetting to is the standard asyncio bug.

(Real Kafka uses network threads plus I/O threads — same idea, different
plumbing. The JVM has cheap threads; Python does not.)


PHASE 1: WHAT CHANGED IN HERE
-------------------------------
The routing logic is the same shape, but every answer is now structured:

  * Requests carry an `api_key` and `api_version`; unsupported combinations
    are rejected with UNSUPPORTED_VERSION instead of being half-handled.
  * Every response carries a numeric `error_code`, so clients decide what to
    retry by asking the code rather than by matching English.
  * A body length larger than MAX_REQUEST_BYTES is refused BEFORE allocating.
  * Two new read-only APIs, LIST_OFFSETS and METADATA, so clients and the
    admin CLI can ask about state instead of inferring it.
"""

import argparse
import asyncio
import json
import logging
import os
import threading

from broker.partition import Partition
from common import errors
from common.config import DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT
from common.protocol import (
    API_FETCH, API_LIST_OFFSETS, API_METADATA, API_PRODUCE,
    HEADER_SIZE, MAGIC, MAX_REQUEST_BYTES,
    TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
    api_name, decode_body, decode_header, encode_message, is_supported,
)

log = logging.getLogger(__name__)


class Broker:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 data_dir: str = DEFAULT_DATA_DIR, node_id: int = 0):
        self.host = host
        self.port = port          # rewritten with the real port if port == 0
        self.data_dir = data_dir

        # Every broker in a cluster needs a stable identity: partition
        # assignments, leadership, and replica sets are all expressed as
        # broker IDs, not host:port (which can change when a machine is
        # replaced). With one broker it is always 0 — but METADATA already
        # reports it, so Phase 5 adds peers rather than a new concept.
        self.node_id = node_id

        # CONCEPT: The partition registry.
        #   (topic, partition_id) → Partition
        # Keyed by the pair because a topic is only a *name*; the partition is
        # what holds data. Everything in Kafka is ultimately addressed by
        # (topic, partition).
        self.partitions: "dict[tuple, Partition]" = {}
        self._registry_lock = threading.Lock()

        # Set once bound and accepting. Tests wait on this instead of sleeping
        # and hoping. threading.Event, not asyncio.Event, because the waiter
        # is a different thread than the loop.
        self.ready = threading.Event()

        self._loop = None
        self._shutdown = None     # asyncio.Event, created inside start()

    # ── Partition management ───────────────────────────────────

    def _get_partition(self, topic: str, partition_id: int) -> Partition:
        """
        Return the partition, creating it on first use.

        CONCEPT: Lazy auto-creation — convenient, and a genuine footgun.
          Producing to "oders" instead of "orders" raises no error. It
          silently creates a new topic and your messages go somewhere nobody
          reads. Real Kafka makes this configurable
          (`auto.create.topics.enable`) and most production clusters turn it
          OFF for exactly this reason. Phase 3 adds explicit CREATE_TOPICS.

        Note the deliberate split introduced in Phase 1: **writes create,
        queries do not.** PRODUCE and FETCH auto-create; LIST_OFFSETS and
        METADATA use `_find_partition` and report
        UNKNOWN_TOPIC_OR_PARTITION instead. A query that conjures the thing it
        was asking about makes it impossible to ask "does this exist?".

        The lock: `_get_partition` runs on the loop thread today, so nothing
        can interleave. It is held anyway because Phase 6's replication
        fetcher touches this registry from another thread, and a registry that
        is only accidentally safe is a bug waiting for a future phase.
        """
        key = (topic, partition_id)
        with self._registry_lock:
            if key not in self.partitions:
                self.partitions[key] = Partition(topic, partition_id, self.data_dir)
                log.info("Created partition %s-%s", topic, partition_id)
            return self.partitions[key]

    def _find_partition(self, topic: str, partition_id: int):
        """Look up a partition without creating it. Returns None if absent."""
        with self._registry_lock:
            return self.partitions.get((topic, partition_id))

    async def _run_blocking(self, fn, *args):
        """
        Run blocking file I/O on the executor so the event loop stays free.

        CONCEPT: the escape hatch for blocking code.
          `partition.append()` writes to a file, which blocks. Calling it
          directly would stop the loop dead — every other connected client
          waits for this one disk write. `run_in_executor` hands it to a
          worker thread and returns an awaitable, so other connections keep
          being served.

          This is also precisely why Partition needs its threading.Lock: the
          executor is a POOL, so two appends really can run at the same
          instant on different threads.
        """
        return await asyncio.get_running_loop().run_in_executor(None, fn, *args)

    # ── Request routing ────────────────────────────────────────

    async def _route(self, api_key: int, api_version: int, payload: dict) -> dict:
        """
        Dispatch one request and return the response body.

        Every return value carries `error_code`. Success is
        `errors.NONE` (0) — sent explicitly rather than omitted, so a client
        never has to distinguish "no error" from "the field is missing
        because an older broker did not set it".
        """
        if not is_supported(api_key, api_version):
            # Two different failures share this branch, and the message
            # distinguishes them: an api_key we do not implement at all, and
            # one we implement at other versions only. Real Kafka separates
            # them; for us the client fix is the same either way.
            return _error(
                errors.UNSUPPORTED_VERSION,
                f"{api_name(api_key)} v{api_version} is not supported by this broker")

        if api_key == API_PRODUCE:
            return await self._handle_produce(payload)
        if api_key == API_FETCH:
            return await self._handle_fetch(payload)
        if api_key == API_LIST_OFFSETS:
            return await self._handle_list_offsets(payload)
        if api_key == API_METADATA:
            return await self._handle_metadata(payload)

        # Unreachable: is_supported() already rejected unknown keys. Kept as a
        # belt-and-braces guard, because "unreachable" and "unreached" differ.
        return _error(errors.INVALID_REQUEST, f"unhandled api_key {api_key}")

    async def _handle_produce(self, payload: dict) -> dict:
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        value = payload.get('value', '')
        key = payload.get('key')

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")

        partition = self._get_partition(topic, part_id)
        offset = await self._run_blocking(partition.append, value, key)

        log.info("PRODUCE -> %s-%s offset=%s value=%r", topic, part_id, offset, value)

        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'offset': offset,
        }

    async def _handle_fetch(self, payload: dict) -> dict:
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        start_offset = payload.get('offset', 0)
        max_msgs = payload.get('max', 10)

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")
        if start_offset < 0:
            return _error(errors.OFFSET_OUT_OF_RANGE,
                          f"offset {start_offset} is negative")

        partition = self._get_partition(topic, part_id)

        # Asking beyond the end is NOT an error — it is what every caught-up
        # consumer does on every poll. It returns an empty list, and the
        # consumer waits. Only asking below `log_start_offset` (data aged out
        # by retention, from Phase 2) is genuinely out of range.
        if start_offset < partition.log_start_offset:
            return _error(
                errors.OFFSET_OUT_OF_RANGE,
                f"offset {start_offset} is below the log start "
                f"{partition.log_start_offset}; that data has been deleted")

        messages = await self._run_blocking(partition.fetch, start_offset, max_msgs)

        log.info("FETCH  ← %s-%s from=%s returned=%s",
                 topic, part_id, start_offset, len(messages))

        # `high_watermark` is one past the last message. A consumer subtracts
        # its position from it to get LAG — the most-watched number in any
        # Kafka deployment, because unbounded lag means retention will delete
        # data nobody has read. In Phase 6 this stops meaning "everything
        # written" and starts meaning "everything REPLICATED", which is a much
        # more interesting number.
        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'messages': messages,
            'count': len(messages),
            'high_watermark': partition.next_offset,
            'log_start_offset': partition.log_start_offset,
        }

    async def _handle_list_offsets(self, payload: dict) -> dict:
        """
        Answer "where does this partition start / end?" without reading data.

        CONCEPT: why this deserves its own API.
          A consumer that wants to start at the newest message could fetch
          everything and throw it away — absurd for a large log. LIST_OFFSETS
          answers from metadata the broker already holds, in constant time and
          zero bytes of payload.

          The timestamp field uses Kafka's sentinels: -1 (LATEST) and
          -2 (EARLIEST). Real Kafka also accepts an actual millisecond
          timestamp and answers "the first offset at or after that time",
          which is how "replay from 9am" works. We support only the
          sentinels — the general form needs the time index that Phase 2's
          storage engine introduces.
        """
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        timestamp = payload.get('timestamp', TIMESTAMP_LATEST)

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")

        # A query, so it does not create. See _get_partition's docstring.
        partition = self._find_partition(topic, part_id)
        if partition is None:
            return _error(errors.UNKNOWN_TOPIC_OR_PARTITION,
                          f"no partition {topic}-{part_id} on this broker")

        if timestamp == TIMESTAMP_EARLIEST:
            offset = partition.log_start_offset
        elif timestamp == TIMESTAMP_LATEST:
            offset = partition.next_offset
        else:
            return _error(
                errors.INVALID_REQUEST,
                f"timestamp {timestamp} not supported; use "
                f"{TIMESTAMP_EARLIEST} (earliest) or {TIMESTAMP_LATEST} (latest). "
                f"Lookup by real timestamp needs the time index from Phase 2.")

        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'timestamp': timestamp,
            'offset': offset,
        }

    async def _handle_metadata(self, payload: dict) -> dict:
        """
        Describe the cluster and its topics.

        CONCEPT: this is the API that makes Kafka clients "thick".
          A client does not send data to some load balancer that figures out
          where it goes. It asks for metadata, learns which broker leads which
          partition, caches that, and connects to the right broker directly —
          no proxy hop, no extra network round trip on the data path.

          The cost is that the cache goes stale exactly when it matters (a
          leader just failed). The cure is NOT_LEADER_OR_FOLLOWER: the wrong
          broker says so, the client refreshes, and retries. Metadata plus
          that one error code is the whole failover mechanism, and both exist
          now, before there is a cluster to use them.

        Request: {'topics': ['orders']} — or omit/None for every topic.
        """
        requested = payload.get('topics')

        with self._registry_lock:
            by_topic = {}
            for (topic, part_id), partition in self.partitions.items():
                by_topic.setdefault(topic, []).append((part_id, partition))

        if requested:
            missing = [t for t in requested if t not in by_topic]
            by_topic = {t: p for t, p in by_topic.items() if t in requested}
        else:
            missing = []

        topics = [
            {
                'name': topic,
                'error_code': errors.NONE,
                'partitions': [
                    {
                        'partition': part_id,
                        # With one broker, every partition is led by us and has
                        # exactly one replica, which is trivially in-sync.
                        # Phase 5 assigns real leaders; Phase 6 real replicas
                        # and a real ISR. The SHAPE is right now, so clients
                        # written against it keep working.
                        'leader': self.node_id,
                        'replicas': [self.node_id],
                        'isr': [self.node_id],
                        'high_watermark': partition.next_offset,
                        'log_start_offset': partition.log_start_offset,
                    }
                    for part_id, partition in sorted(parts, key=lambda p: p[0])
                ],
            }
            for topic, parts in sorted(by_topic.items())
        ]

        # A topic that was asked for but does not exist is reported per topic,
        # not as a failure of the whole request — one unknown name must not
        # hide the nine that were fine.
        topics.extend({
            'name': name,
            'error_code': errors.UNKNOWN_TOPIC_OR_PARTITION,
            'partitions': [],
        } for name in sorted(missing))

        return {
            'error_code': errors.NONE,
            'brokers': [{'node_id': self.node_id,
                         'host': self.host, 'port': self.port}],
            'controller_id': self.node_id,
            'topics': topics,
        }

    # ── Connection handling ────────────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """
        One coroutine per connected client, for the life of the connection.

        Reads requests until the client hangs up. One connection carries many
        requests — reconnecting per message would spend more time on TCP
        handshakes than on data.
        """
        addr = writer.get_extra_info('peername')
        log.info("Client connected: %s", addr)

        try:
            while True:
                # Step 1: the fixed 16-byte header. readexactly() is asyncio's
                # version of the loop in common/net.py, for the same reason:
                # TCP delivers a byte stream with no message boundaries.
                try:
                    header_bytes = await reader.readexactly(HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    break                     # clean disconnect, not an error

                header = decode_header(header_bytes)

                # Step 2: validate framing before trusting anything in it.
                if header.magic != MAGIC:
                    # Not our protocol, or the stream desynchronised. With no
                    # delimiters there is no way to resynchronise, so hanging
                    # up is the only correct move.
                    log.warning("Bad magic from %s: %#x — closing",
                                addr, header.magic)
                    break

                # Step 3: refuse absurd lengths BEFORE allocating for them.
                #
                # `body_length` is an unsigned 32-bit integer straight off the
                # network. Trusting it means a client can claim 4 GB and have
                # us try to allocate it — a denial of service that needs no
                # credentials, only a socket. We answer, then close: the
                # connection cannot continue, because the bytes it claimed are
                # still queued behind us and would be read as a header.
                if header.body_length > MAX_REQUEST_BYTES:
                    log.warning("Oversized request from %s: %s bytes",
                                addr, header.body_length)
                    await _reply(writer, header, _error(
                        errors.MESSAGE_TOO_LARGE,
                        f"request is {header.body_length:,} bytes, limit is "
                        f"{MAX_REQUEST_BYTES:,}"))
                    break

                try:
                    body_bytes = await reader.readexactly(header.body_length)
                except asyncio.IncompleteReadError:
                    log.warning("Client %s vanished mid-body", addr)
                    break

                # Step 4: decode. A bad body is the CLIENT's fault, so answer
                # with an error and keep the connection alive. Phase 0's broad
                # `except Exception` killed the connection here and buried
                # real bugs in the same handler.
                try:
                    payload = decode_body(body_bytes)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Malformed body from %s: %s", addr, exc)
                    await _reply(writer, header, _error(
                        errors.INVALID_REQUEST, f"malformed body: {exc}"))
                    continue

                if not isinstance(payload, dict):
                    await _reply(writer, header, _error(
                        errors.INVALID_REQUEST,
                        f"body must be a JSON object, got {type(payload).__name__}"))
                    continue

                # Step 5: handle it.
                try:
                    response = await self._route(
                        header.api_key, header.api_version, payload)
                except Exception:
                    # A bug in a handler must not take down the connection or
                    # leave the client waiting forever for a reply that never
                    # comes. Log the traceback for us; return a generic code
                    # to them. UNKNOWN_SERVER_ERROR is non-retriable, which is
                    # right: hammering a broker that is throwing exceptions
                    # helps nobody.
                    log.exception("Handler crashed on %s from %s",
                                  api_name(header.api_key), addr)
                    response = _error(errors.UNKNOWN_SERVER_ERROR,
                                      "internal broker error")

                # Step 6: answer, echoing api_key and correlation_id.
                await _reply(writer, header, response)

        except (ConnectionResetError, BrokenPipeError):
            # The client died or the network dropped. Extremely normal; not
            # worth a stack trace.
            log.info("Client %s disconnected abruptly", addr)
        except Exception:
            log.exception("Unhandled error serving %s", addr)
        finally:
            writer.close()
            # CONCEPT: close() only *schedules* the close.
            #   Buffered response bytes may not have reached the socket, and
            #   the file descriptor is not released until it completes.
            #   Without wait_closed(), a busy broker leaks descriptors and
            #   clients occasionally see truncated replies.
            #
            #   The try/except is required: if the peer already vanished,
            #   wait_closed() raises — and an exception from a finally block
            #   replaces whatever real error we were handling.
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            log.info("Client disconnected: %s", addr)

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        """
        Bind the port and serve until `stop()` is called.

        We await a shutdown event rather than `serve_forever()` so shutdown is
        orderly: stop accepting, then close every partition's file handle.
        Tests need this, and so does anyone who would rather not leave the
        last few messages sitting in a buffer at exit.
        """
        self._loop = asyncio.get_running_loop()
        self._shutdown = asyncio.Event()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        # port=0 means "OS, pick any free port". Read back what it chose so
        # tests can connect — which is why the suite never collides with a dev
        # broker already sitting on 9092.
        self.port = server.sockets[0].getsockname()[1]

        log.info("Broker %s listening on %s:%s", self.node_id, self.host, self.port)
        log.info("Data directory: %s", os.path.abspath(self.data_dir))
        self.ready.set()

        async with server:
            await self._shutdown.wait()

        self._close_partitions()
        log.info("Broker stopped.")

    def stop(self) -> None:
        """
        Ask the broker to shut down. Safe to call from any thread.

        `call_soon_threadsafe` is the ONLY sanctioned way to touch an asyncio
        loop from outside its thread. Setting the event directly is a race:
        the loop may be blocked in select() and never notice.
        """
        if self._loop is not None and self._shutdown is not None:
            self._loop.call_soon_threadsafe(self._shutdown.set)

    def _close_partitions(self) -> None:
        with self._registry_lock:
            for partition in self.partitions.values():
                try:
                    partition.close()
                except OSError as exc:
                    log.warning("Error closing %r: %s", partition, exc)


# ── Response helpers ───────────────────────────────────────────

def _error(error_code: int, message: str) -> dict:
    """
    Build an error body.

    Both fields matter and they serve different readers: `error_code` is for
    the client's code (retry or not), `error_message` is for the human reading
    the log afterwards. Sending only the code makes debugging miserable;
    sending only the message is Phase 0's mistake.
    """
    return {'error_code': error_code, 'error_message': message}


async def _reply(writer: asyncio.StreamWriter, header, body: dict) -> None:
    """
    Send a response echoing the request's api_key and correlation_id.

    The echo is what lets a client match replies to requests and therefore
    pipeline them. Getting it wrong is invisible under sequential load and
    catastrophic under concurrent load, which is why there is a test pinning it.
    """
    writer.write(encode_message(header.api_key, body,
                                header.correlation_id, header.api_version))
    # drain() applies BACKPRESSURE: if this client is not reading fast enough
    # it pauses us, instead of letting the send buffer grow without bound.
    # Skip it and one slow consumer can drive the broker out of memory.
    await writer.drain()


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone broker  (run with: python -m broker.broker)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--node-id', type=int, default=0,
                        help='Broker id. Only meaningful from Phase 5 onward.')
    parser.add_argument('--quiet', action='store_true',
                        help='Only log warnings — use when benchmarking, since '
                             'a log line per message costs more than the write.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    broker = Broker(host=args.host, port=args.port,
                    data_dir=args.data_dir, node_id=args.node_id)
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        # Ctrl-C unwinds out of asyncio.run before start()'s cleanup runs, so
        # close the partition handles here too. Losing buffered writes on a
        # deliberate shutdown would be an embarrassing way to lose data.
        broker._close_partitions()
        log.info("Broker stopped (interrupted).")


if __name__ == '__main__':
    main()
