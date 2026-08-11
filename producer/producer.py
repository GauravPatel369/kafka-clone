"""
CONCEPT: The Producer — TCP Client (Write Side)
-------------------------------------------------
The producer is the entity that creates messages and sends them
to the broker for storage.

In Phase 1, the producer is simple:
  - Connects to one broker
  - Sends all messages to partition 0 of the given topic
  - Gets back the offset assigned to each message

Phase 2 will add: multiple partitions, key-based routing,
                  load balancing across multiple brokers.

Producer flow:
  1. Open a TCP connection to the broker
  2. Build a PRODUCE message: encode_message(MSG_PRODUCE, payload)
  3. Send it over the socket
  4. Read the 12-byte header of the response
  5. Read the body bytes
  6. Parse and return the offset

We use a context manager (with Producer(...) as p:) so the
TCP connection is always properly closed.
"""

import socket
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (
    HEADER_SIZE, MAGIC,
    MSG_PRODUCE,
    encode_message, decode_header, decode_body
)


class Producer:
    def __init__(self, host: str = 'localhost', port: int = 9092,
                 topic: str = 'default'):
        self.host  = host
        self.port  = port
        self.topic = topic
        self._sock = None

    # ── Connection management ──────────────────────────────────

    def connect(self):
        """
        Open a persistent TCP connection to the broker.

        CONCEPT: Persistent connection
          We keep one TCP connection open for the lifetime of the producer.
          This is more efficient than opening a new connection per message
          (TCP handshake overhead would dominate for small messages).
        """
        self._sock = socket.create_connection((self.host, self.port))
        print(f"[Producer] Connected to broker at {self.host}:{self.port}")

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ── Core operation: send one message ──────────────────────

    def send(self, value: str, key: str = None, partition: int = 0) -> int:
        """
        Send one message to the broker and return the assigned offset.

        Args:
            value     : the message content (any string)
            key       : optional routing key (used in Phase 2 for partitioning)
            partition : which partition to write to (always 0 in Phase 1)

        Returns:
            The offset assigned to this message by the broker.

        Example:
            offset = producer.send("order placed", key="user-42")
            print(f"Stored at offset {offset}")
        """
        if not self._sock:
            raise RuntimeError("Not connected. Call connect() first.")

        # Build payload dict
        payload = {
            'topic'    : self.topic,
            'partition': partition,
            'value'    : value,
            'key'      : key
        }

        # Encode to wire format: 12-byte header + JSON body
        message_bytes = encode_message(MSG_PRODUCE, payload)

        # ── Send ────────────────────────────────────────────────
        # sendall() keeps sending until all bytes are delivered
        # (socket.send() might only send part of the data)
        self._sock.sendall(message_bytes)

        # ── Receive response ────────────────────────────────────
        response = self._recv_response()
        offset   = response.get('offset', -1)

        print(f"[Producer] SENT value={value!r} key={key!r} "
              f"→ topic={self.topic} partition={partition} offset={offset}")
        return offset

    # ── Helper: receive one response from broker ──────────────

    def _recv_response(self) -> dict:
        """
        Read exactly one response from the broker.

        CONCEPT: Length-prefixed protocol
          We always read the fixed 12-byte header first to find out
          how many bytes the body is. Then we read exactly that many
          bytes. This avoids the "where does one message end and the
          next begin?" problem of stream-based TCP.
        """
        # Read exactly 12 bytes (the header)
        header_bytes = self._recvexactly(HEADER_SIZE)
        magic, body_length, msg_type = decode_header(header_bytes)

        if magic != MAGIC:
            raise ValueError(f"Invalid magic number in response: {magic:#x}")

        # Read exactly body_length bytes
        body_bytes = self._recvexactly(body_length)
        return decode_body(body_bytes)

    def _recvexactly(self, n: int) -> bytes:
        """
        CONCEPT: recv() is not guaranteed to return n bytes
          TCP is a stream — socket.recv(n) may return fewer than n bytes
          if the data hasn't fully arrived yet.
          We loop until we have exactly n bytes.
        """
        data = b''
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Broker closed connection unexpectedly")
            data += chunk
        return data


# ── Quick test ─────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Kafka-clone producer')
    parser.add_argument('--host',  default='localhost')
    parser.add_argument('--port',  type=int, default=9092)
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--count', type=int, default=5,
                        help='Number of test messages to send')
    args = parser.parse_args()

    with Producer(args.host, args.port, args.topic) as p:
        for i in range(args.count):
            p.send(f"message-{i}", key=f"key-{i % 3}")

    print(f"\nDone. Sent {args.count} messages to topic '{args.topic}'.")
