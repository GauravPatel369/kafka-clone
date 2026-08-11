"""
CONCEPT: The Consumer — TCP Client (Read Side)
------------------------------------------------
The consumer reads messages from the broker partition by partition,
tracking its own position using an offset.

Key insight: THE BROKER DOESN'T TRACK WHAT YOU'VE READ.
  The consumer is responsible for remembering its offset.
  If the consumer restarts, it reads its last saved offset from
  a local file and resumes from exactly where it left off.

Consumer flow:
  1. Load last saved offset from disk (or start from 0)
  2. Connect to broker
  3. poll() in a loop:
     a. Send FETCH request with current offset
     b. Receive list of messages from broker
     c. Process each message
     d. Update offset to (last_message_offset + 1)
     e. Save offset to disk
     f. Sleep briefly, then repeat

CONCEPT: Consumer offset file
  The offset is saved as a plain text file:
      ./offsets/orders-0-group1.txt
  Contents: just the integer offset, e.g. "42"

  Why save to disk?
    If the consumer process crashes at offset 42 and restarts,
    it reads "42" from the file and continues from message 42.
    Without this, it would start from 0 and reprocess everything.
"""

import socket
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (
    HEADER_SIZE, MAGIC,
    MSG_FETCH,
    encode_message, decode_header, decode_body
)


class Consumer:
    def __init__(self, host: str = 'localhost', port: int = 9092,
                 topic: str = 'default', partition: int = 0,
                 group_id: str = 'group-1', offset_dir: str = './offsets'):
        self.host      = host
        self.port      = port
        self.topic     = topic
        self.partition = partition
        self.group_id  = group_id

        # Load the last committed offset from disk
        # If first run, starts from 0
        os.makedirs(offset_dir, exist_ok=True)
        self._offset_file = os.path.join(
            offset_dir,
            f"{topic}-{partition}-{group_id}.txt"
        )
        self.offset = self._load_offset()

        self._sock = None
        print(f"[Consumer] group={group_id} topic={topic} "
              f"partition={partition} starting_offset={self.offset}")

    # ── Connection management ──────────────────────────────────

    def connect(self):
        self._sock = socket.create_connection((self.host, self.port))
        print(f"[Consumer] Connected to broker at {self.host}:{self.port}")

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ── Core operation: fetch messages ─────────────────────────

    def poll(self, max_messages: int = 10) -> list:
        """
        Fetch the next batch of messages from the broker.

        CONCEPT: Pull-based consumption
          Consumers PULL from the broker (they ask "give me messages").
          This is different from push-based systems where the broker
          PUSHES messages to consumers.

          Pull is better because:
          - Consumer controls its own pace (no buffer overflow)
          - Consumer can batch-process efficiently
          - Simple to implement fault tolerance (just retry the same offset)

        Returns a list of message dicts. Empty list = no new messages yet.
        """
        if not self._sock:
            raise RuntimeError("Not connected. Call connect() first.")

        # Build FETCH request
        payload = {
            'topic'    : self.topic,
            'partition': self.partition,
            'offset'   : self.offset,    # "give me messages from here"
            'max'      : max_messages
        }

        self._sock.sendall(encode_message(MSG_FETCH, payload))

        # Receive response
        response = self._recv_response()
        messages = response.get('messages', [])

        if messages:
            # CONCEPT: Advance the offset
            # After receiving messages, move our offset past the last one
            # so the next poll() starts from where we left off
            last_offset = messages[-1]['offset']
            self.offset = last_offset + 1

            # Save offset to disk immediately
            # This is called "committing" the offset
            self._save_offset()

            print(f"[Consumer] Received {len(messages)} messages | "
                  f"new offset: {self.offset}")
        else:
            print(f"[Consumer] No new messages at offset {self.offset}")

        return messages

    def consume_loop(self, process_fn, poll_interval: float = 1.0):
        """
        Continuously poll for messages and call process_fn on each one.

        CONCEPT: Polling loop
          This is the typical consumer pattern — run forever, checking
          for new messages every poll_interval seconds.

        Args:
            process_fn   : function called with each message dict
            poll_interval: seconds to wait between polls when no messages

        Example:
            def handle(msg):
                print(f"Got: {msg['value']}")

            consumer.consume_loop(handle)
        """
        print(f"[Consumer] Starting consume loop (poll every {poll_interval}s)...")
        while True:
            messages = self.poll()

            for msg in messages:
                process_fn(msg)

            if not messages:
                # No new messages — wait before polling again
                # (avoids hammering the broker with empty requests)
                time.sleep(poll_interval)

    # ── Offset persistence ─────────────────────────────────────

    def _load_offset(self) -> int:
        """Read last saved offset from disk. Return 0 if first run."""
        try:
            with open(self._offset_file, 'r') as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return 0   # first run, start from the beginning

    def _save_offset(self):
        """Write current offset to disk for crash recovery."""
        with open(self._offset_file, 'w') as f:
            f.write(str(self.offset))

    # ── Helpers ────────────────────────────────────────────────

    def _recv_response(self) -> dict:
        header_bytes = self._recvexactly(HEADER_SIZE)
        magic, body_length, _ = decode_header(header_bytes)
        if magic != MAGIC:
            raise ValueError(f"Invalid magic: {magic:#x}")
        body_bytes = self._recvexactly(body_length)
        return decode_body(body_bytes)

    def _recvexactly(self, n: int) -> bytes:
        data = b''
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Broker closed connection")
            data += chunk
        return data


# ── Quick test ─────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Kafka-clone consumer')
    parser.add_argument('--host',      default='localhost')
    parser.add_argument('--port',      type=int, default=9092)
    parser.add_argument('--topic',     default='orders')
    parser.add_argument('--partition', type=int, default=0)
    parser.add_argument('--group',     default='group-1')
    parser.add_argument('--loop',      action='store_true',
                        help='Run forever in polling loop')
    args = parser.parse_args()

    def handle_message(msg):
        print(f"  → offset={msg['offset']} key={msg['key']!r} "
              f"value={msg['value']!r} ts={msg['timestamp']}")

    with Consumer(args.host, args.port, args.topic,
                  args.partition, args.group) as c:
        if args.loop:
            c.consume_loop(handle_message, poll_interval=1.0)
        else:
            # One-shot: fetch whatever is available right now
            messages = c.poll(max_messages=100)
            print(f"\nFetched {len(messages)} messages:")
            for msg in messages:
                handle_message(msg)
