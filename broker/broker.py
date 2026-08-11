"""
CONCEPT: The Broker — TCP Server
----------------------------------
The broker is a long-running server that:
  1. Listens on a TCP port (default 9092, same as real Kafka)
  2. Accepts connections from producers and consumers
  3. Routes each request to the right partition
  4. Sends back a response

We use Python's asyncio library which lets ONE thread handle MANY
concurrent connections efficiently using an event loop.

Without asyncio: you'd need one OS thread per client connection.
With asyncio:    one thread handles thousands of connections by
                 switching between them whenever one is waiting for I/O.

                 ┌─────────────────────────────────┐
                 │           BROKER                │
                 │                                 │
    Producer A ──┤── handle_client() ──┐           │
    Producer B ──┤── handle_client()   ├─► route() │
    Consumer  ──┤── handle_client() ──┘           │
                 │              │                  │
                 │         partitions{}            │
                 │    orders-0.log  orders-1.log   │
                 └─────────────────────────────────┘

Message flow for a PRODUCE request:
  1. Client sends 12-byte header
  2. Broker reads header, learns body length
  3. Broker reads body bytes
  4. Broker decodes JSON payload
  5. Broker calls partition.append(value)
  6. Broker sends ACK with assigned offset

Message flow for a FETCH request:
  1. Client sends 12-byte header + body with {topic, partition, offset}
  2. Broker calls partition.fetch(start_offset)
  3. Broker sends ACK with list of messages
"""

import asyncio
import sys
import os
import logging

# Make sure Python can find sibling packages
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (
    HEADER_SIZE, MAGIC,
    MSG_PRODUCE, MSG_FETCH, MSG_ACK,
    encode_message, decode_header, decode_body
)
from broker.partition import Partition

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


class Broker:
    def __init__(self, host: str = 'localhost', port: int = 9092,
                 data_dir: str = './data'):
        self.host     = host
        self.port     = port
        self.data_dir = data_dir

        # CONCEPT: Partition registry
        # partitions is a dict mapping (topic, partition_id) → Partition object
        # It's created lazily — the first PRODUCE to a new topic auto-creates it
        # Key:   ("orders", 0)
        # Value: Partition object managing orders-0.log
        self.partitions: dict[tuple, Partition] = {}

    # ── Partition management ───────────────────────────────────

    def _get_partition(self, topic: str, partition_id: int) -> Partition:
        """
        Get existing partition or create a new one on first use.

        CONCEPT: Lazy creation
          In real Kafka, partitions are created explicitly by an admin.
          Here we auto-create them on first write to keep Phase 1 simple.
        """
        key = (topic, partition_id)
        if key not in self.partitions:
            self.partitions[key] = Partition(topic, partition_id, self.data_dir)
        return self.partitions[key]

    # ── Request routing ────────────────────────────────────────

    async def _route(self, msg_type: int, payload: dict) -> dict:
        """
        CONCEPT: Request dispatcher
          The broker receives a msg_type integer and decides what to do.
          This is the core logic of the broker — everything else is plumbing.

          MSG_PRODUCE → write message, return offset
          MSG_FETCH   → read messages, return list
        """

        if msg_type == MSG_PRODUCE:
            # ── Handle PRODUCE ─────────────────────────────
            # Producer sends: {"topic": "orders", "partition": 0, "value": "...", "key": "..."}
            topic      = payload.get('topic', 'default')
            part_id    = payload.get('partition', 0)
            value      = payload.get('value', '')
            key        = payload.get('key')

            partition = self._get_partition(topic, part_id)

            # run_in_executor: partition.append() does file I/O (blocking)
            # We run it in a thread pool so the event loop isn't blocked
            # This is the asyncio pattern for calling blocking code
            loop   = asyncio.get_event_loop()
            offset = await loop.run_in_executor(None, partition.append, value, key)

            log.info(f"PRODUCE → topic={topic} partition={part_id} "
                     f"offset={offset} value={value!r}")

            return {
                'status'   : 'ok',
                'offset'   : offset,
                'topic'    : topic,
                'partition': part_id
            }

        elif msg_type == MSG_FETCH:
            # ── Handle FETCH ───────────────────────────────
            # Consumer sends: {"topic": "orders", "partition": 0, "offset": 3, "max": 10}
            topic        = payload.get('topic', 'default')
            part_id      = payload.get('partition', 0)
            start_offset = payload.get('offset', 0)
            max_msgs     = payload.get('max', 10)

            partition = self._get_partition(topic, part_id)

            loop     = asyncio.get_event_loop()
            messages = await loop.run_in_executor(
                None, partition.fetch, start_offset, max_msgs
            )

            log.info(f"FETCH  ← topic={topic} partition={part_id} "
                     f"from_offset={start_offset} returned={len(messages)} msgs")

            return {
                'status'  : 'ok',
                'messages': messages,
                'count'   : len(messages)
            }

        else:
            # Unknown message type — return an error response
            log.warning(f"Unknown msg_type={msg_type}")
            return {'status': 'error', 'reason': f'unknown msg_type {msg_type}'}

    # ── Connection handler ─────────────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter):
        """
        CONCEPT: Per-connection coroutine
          Each time a client connects, asyncio calls this function.
          It runs as a coroutine — it can 'await' I/O without blocking
          other connections.

          The loop keeps reading requests until the client disconnects.
          One client can send many requests on the same connection.

          Flow:
            while client connected:
              1. read 12-byte header   (await reader.readexactly)
              2. validate magic number
              3. read body bytes       (await reader.readexactly)
              4. decode JSON
              5. route to handler
              6. send response         (writer.write + await drain)
        """
        addr = writer.get_extra_info('peername')
        log.info(f"Client connected: {addr}")

        try:
            while True:
                # ── Step 1: Read the fixed-size header ─────
                # readexactly(n) waits until exactly n bytes arrive
                # If client disconnects, raises IncompleteReadError
                try:
                    header_bytes = await reader.readexactly(HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    break   # client disconnected cleanly

                # ── Step 2: Parse header ────────────────────
                magic, body_length, msg_type = decode_header(header_bytes)

                # Validate the magic number — reject garbage packets
                if magic != MAGIC:
                    log.warning(f"Bad magic from {addr}: {magic:#x}")
                    break

                # ── Step 3: Read the body ───────────────────
                body_bytes = await reader.readexactly(body_length)

                # ── Step 4: Decode JSON ─────────────────────
                payload = decode_body(body_bytes)

                # ── Step 5: Route to handler ────────────────
                response = await self._route(msg_type, payload)

                # ── Step 6: Send response ───────────────────
                response_bytes = encode_message(MSG_ACK, response)
                writer.write(response_bytes)
                await writer.drain()   # flush the send buffer

        except Exception as e:
            log.error(f"Error handling client {addr}: {e}")

        finally:
            # Always close the connection when done
            writer.close()
            log.info(f"Client disconnected: {addr}")

    # ── Server startup ─────────────────────────────────────────

    async def start(self):
        """
        Start the TCP server and listen forever.

        asyncio.start_server() calls handle_client() each time
        a new TCP connection arrives.
        """
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        log.info(f"Broker listening on {self.host}:{self.port}")
        log.info(f"Data directory: {os.path.abspath(self.data_dir)}")
        log.info("Waiting for producers and consumers...")

        async with server:
            await server.serve_forever()


# ── Entry point ────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Kafka-clone broker')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=9092)
    parser.add_argument('--data-dir', default='./data')
    args = parser.parse_args()

    broker = Broker(host=args.host, port=args.port, data_dir=args.data_dir)

    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        log.info("Broker stopped.")
