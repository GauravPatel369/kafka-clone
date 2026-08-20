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

There are three ways to handle many connections at once:

  ┌──────────────────┬────────────────────────────────────────────────────┐
  │ Model            │ Trade-off                                          │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Thread per       │ Dead simple to write — blocking code just works.   │
  │ connection       │ But each thread costs ~8 MB of stack and a context │
  │                  │ switch to schedule. 10,000 clients = 80 GB of      │
  │                  │ stack. Falls over somewhere around 1,000.          │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Process per      │ True parallelism, crash isolation. But processes   │
  │ connection       │ are even heavier, and they cannot share the        │
  │                  │ partition registry without IPC.                    │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Event loop       │ One thread, one small coroutine object per         │
  │ (asyncio)        │ connection (~KB). Handles tens of thousands.       │
  │  ← WE USE THIS   │ Cost: ONE blocking call anywhere freezes EVERY     │
  │                  │ connection, and you only get one CPU core.         │
  └──────────────────┴────────────────────────────────────────────────────┘

That "one blocking call freezes everything" cost is not hypothetical — our
storage layer does blocking file I/O. So `_route` hands those calls to a
thread pool with `run_in_executor`: the event loop stays free while a worker
thread blocks on the disk. Mixing the two models like this is the standard
asyncio pattern, and forgetting to do it is the standard asyncio bug.

(Real Kafka uses a small pool of network threads plus a pool of I/O threads —
the same idea, different plumbing. The JVM has cheap threads; Python does not.)
"""

import argparse
import asyncio
import json
import logging
import os
import threading

from broker.partition import Partition
from common.config import DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT
from common.protocol import (
    HEADER_SIZE, MAGIC,
    MSG_ACK, MSG_FETCH, MSG_PRODUCE,
    decode_body, decode_header, encode_message,
)

log = logging.getLogger(__name__)


class Broker:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 data_dir: str = DEFAULT_DATA_DIR):
        self.host = host
        self.port = port          # rewritten with the real port if port == 0
        self.data_dir = data_dir

        # CONCEPT: The partition registry.
        #   (topic, partition_id) → Partition
        # Keyed by the pair because a topic is only a *name*; the partition
        # is the thing that actually holds data. Everything in Kafka is
        # ultimately addressed by (topic, partition).
        self.partitions: "dict[tuple, Partition]" = {}
        self._registry_lock = threading.Lock()

        # Set once the server is bound and accepting. Tests wait on this
        # instead of sleeping and hoping. (threading.Event, not asyncio.Event,
        # because the waiter is a different thread than the event loop.)
        self.ready = threading.Event()

        self._loop = None
        self._shutdown = None     # asyncio.Event, created inside start()

    # ── Partition management ───────────────────────────────────

    def _get_partition(self, topic: str, partition_id: int) -> Partition:
        """
        Return the partition, creating it on first use.

        CONCEPT: Lazy auto-creation — convenient, and a genuine footgun.
          Producing to "oders" instead of "orders" does not raise an error.
          It silently creates a brand new topic, and your messages go
          somewhere nobody is reading. Real Kafka makes this configurable
          (`auto.create.topics.enable`) and most production clusters turn it
          OFF for exactly this reason.

          Phase 3 adds an explicit CREATE_TOPIC request and makes
          auto-creation a flag. For now it keeps the demo one command shorter.

        The lock: `_get_partition` runs on the event loop thread, so two
        coroutines cannot interleave here today. It is held anyway because
        Phase 6's replication fetcher touches this registry from a different
        thread, and a registry that is only accidentally safe is a bug
        waiting for a future phase to trigger it.
        """
        key = (topic, partition_id)
        with self._registry_lock:
            if key not in self.partitions:
                self.partitions[key] = Partition(topic, partition_id, self.data_dir)
                log.info("Created partition %s-%s", topic, partition_id)
            return self.partitions[key]

    # ── Request routing ────────────────────────────────────────

    async def _route(self, msg_type: int, payload: dict) -> dict:
        """
        The dispatcher: given a message type, do the thing and return a reply.

        This is the entire "business logic" of the broker. Everything else in
        this file is plumbing to get bytes in and out.
        """
        if msg_type == MSG_PRODUCE:
            topic = payload.get('topic', 'default')
            part_id = payload.get('partition', 0)
            value = payload.get('value', '')
            key = payload.get('key')

            partition = self._get_partition(topic, part_id)

            # CONCEPT: run_in_executor — the escape hatch for blocking code.
            #
            # partition.append() writes to a file, which blocks. Calling it
            # directly would stop the event loop dead: every other connected
            # client waits for this one disk write. run_in_executor hands the
            # call to a worker thread and gives the loop back an awaitable,
            # so other connections keep being served while the disk works.
            #
            # This is also precisely why Partition needs its threading.Lock —
            # "the executor" is a POOL, so two appends really can run at the
            # same instant on different threads.
            offset = await asyncio.get_running_loop().run_in_executor(
                None, partition.append, value, key
            )

            log.info("PRODUCE → %s-%s offset=%s value=%r",
                     topic, part_id, offset, value)

            return {'status': 'ok', 'offset': offset,
                    'topic': topic, 'partition': part_id}

        elif msg_type == MSG_FETCH:
            topic = payload.get('topic', 'default')
            part_id = payload.get('partition', 0)
            start_offset = payload.get('offset', 0)
            max_msgs = payload.get('max', 10)

            partition = self._get_partition(topic, part_id)

            messages = await asyncio.get_running_loop().run_in_executor(
                None, partition.fetch, start_offset, max_msgs
            )

            log.info("FETCH  ← %s-%s from=%s returned=%s",
                     topic, part_id, start_offset, len(messages))

            # `high_watermark` is the offset one past the last message. A
            # consumer subtracts its own position from it to get its LAG —
            # the single most-watched number in any Kafka deployment, because
            # lag that grows without bound means consumers cannot keep up.
            # In Phase 6 this stops being "everything written" and becomes
            # "everything replicated", which is a much more interesting number.
            return {'status': 'ok', 'messages': messages, 'count': len(messages),
                    'high_watermark': partition.next_offset}

        log.warning("Unknown msg_type=%s", msg_type)
        return {'status': 'error', 'reason': f'unknown msg_type {msg_type}'}

    # ── Connection handling ────────────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """
        One coroutine per connected client, for the life of the connection.

        The loop reads requests until the client hangs up. One connection
        carries many requests — reconnecting per message would spend more
        time on TCP handshakes than on data.
        """
        addr = writer.get_extra_info('peername')
        log.info("Client connected: %s", addr)

        try:
            while True:
                # Step 1: read the fixed 12-byte header. readexactly() is the
                # asyncio equivalent of the _recv_exactly loop in
                # common/net.py, and exists for the same reason: TCP delivers
                # a byte stream with no message boundaries.
                try:
                    header_bytes = await reader.readexactly(HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    break                     # clean disconnect — not an error

                # Step 2: validate framing before trusting anything.
                magic, body_length, msg_type = decode_header(header_bytes)
                if magic != MAGIC:
                    # Either not our protocol, or the stream desynchronised.
                    # There is no way to resynchronise a stream with no
                    # delimiters, so the only correct move is to hang up.
                    log.warning("Bad magic from %s: %#x — closing", addr, magic)
                    break

                # Step 3: read the body.
                try:
                    body_bytes = await reader.readexactly(body_length)
                except asyncio.IncompleteReadError:
                    log.warning("Client %s vanished mid-body", addr)
                    break

                # Step 4: decode. A bad body is the CLIENT's fault, so we
                # answer with an error and keep the connection alive. The
                # original code caught bare `Exception` around the whole loop,
                # which meant one malformed request killed the connection —
                # and buried real bugs in the same handler.
                try:
                    payload = decode_body(body_bytes)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Malformed body from %s: %s", addr, exc)
                    writer.write(encode_message(
                        MSG_ACK, {'status': 'error', 'reason': f'malformed body: {exc}'}
                    ))
                    await writer.drain()
                    continue

                # Step 5 & 6: handle it, answer it.
                response = await self._route(msg_type, payload)
                writer.write(encode_message(MSG_ACK, response))

                # drain() applies BACKPRESSURE: if this client is not reading
                # fast enough, it pauses us instead of letting the send buffer
                # grow without bound. Skip it and one slow consumer can drive
                # the broker out of memory.
                await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            # The client process died or the network dropped. Extremely
            # normal; not worth a stack trace.
            log.info("Client %s disconnected abruptly", addr)
        except Exception:
            # Anything else is OUR bug. Log it with the traceback.
            log.exception("Unhandled error serving %s", addr)
        finally:
            writer.close()
            # CONCEPT: close() only *schedules* the close.
            #   Buffered response bytes may not have reached the socket yet,
            #   and the file descriptor is not released until the close
            #   completes. Without wait_closed(), a busy broker slowly leaks
            #   descriptors and clients occasionally see truncated replies.
            #
            #   The try/except is required: if the peer already vanished,
            #   wait_closed() raises — and an exception thrown from a finally
            #   block replaces whatever real error we were handling.
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            log.info("Client disconnected: %s", addr)

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        """
        Bind the port and serve until `stop()` is called.

        We await a shutdown event rather than `serve_forever()` so that
        shutdown is orderly: stop accepting, then close every partition's
        file handle. Tests need this, and so does anyone who does not want
        the last few messages stuck in a buffer at exit.
        """
        self._loop = asyncio.get_running_loop()
        self._shutdown = asyncio.Event()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        # port=0 means "OS, pick any free port". Read back what it chose, so
        # tests can connect. This is why the suite never collides with a dev
        # broker already sitting on 9092.
        self.port = server.sockets[0].getsockname()[1]

        log.info("Broker listening on %s:%s", self.host, self.port)
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
        loop from outside its thread. Setting the event directly from another
        thread is a race: the loop may be blocked in select() and never
        notice. This is how the test fixture stops the broker.
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


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone broker  (run with: python -m broker.broker)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--quiet', action='store_true',
                        help='Only log warnings — use when benchmarking, since '
                             'a log line per message costs more than the write.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    broker = Broker(host=args.host, port=args.port, data_dir=args.data_dir)
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        # Ctrl-C unwinds out of asyncio.run before start()'s cleanup runs,
        # so close the partition handles here too. Losing buffered writes on
        # a deliberate shutdown would be an embarrassing way to lose data.
        broker._close_partitions()
        log.info("Broker stopped (interrupted).")


if __name__ == '__main__':
    main()
