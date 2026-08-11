"""
CONCEPT: Append-Only Partition Log
------------------------------------
This is the heart of Kafka. A partition is just a file on disk where
every message is written at the end — never modified, never deleted.

The file looks like this on disk (each line = one message):
    {"offset": 0, "key": null, "value": "order placed", "timestamp": 1700000000}
    {"offset": 1, "key": "user-42", "value": "payment done", "timestamp": 1700000001}
    {"offset": 2, "key": null, "value": "shipped", "timestamp": 1700000002}

Why one JSON object per line?
  - Easy to append: open file in 'a' mode, write one line
  - Easy to read from offset N: scan lines, skip first N, return rest
  - Human readable: you can open the .log file in any text editor

Key properties:
  - next_offset  : the offset the NEXT message will get (starts at 0)
  - threading.Lock: prevents two producers writing at the same time
    (a "race condition" — without the lock, two threads could both
    read next_offset=5, both write offset 5, corrupting the log)
"""

import os
import json
import threading
import time


class Partition:
    def __init__(self, topic: str, partition_id: int, data_dir: str = './data'):
        self.topic        = topic
        self.partition_id = partition_id
        self.data_dir     = data_dir

        # File paths — one .log file per partition
        # e.g. ./data/orders-0.log, ./data/orders-1.log
        os.makedirs(data_dir, exist_ok=True)
        self.log_path = os.path.join(data_dir, f"{topic}-{partition_id}.log")

        # Lock: ensures only one thread appends at a time
        # Without this: two producers could get the same offset number
        self._lock = threading.Lock()

        # Figure out what the next offset should be
        # (in case we're restarting and the log already has messages)
        self.next_offset = self._recover_offset()

        print(f"[Partition] {topic}-{partition_id} ready | "
              f"log: {self.log_path} | next_offset: {self.next_offset}")

    # ── Write ──────────────────────────────────────────────────

    def append(self, value: str, key: str = None) -> int:
        """
        Write one message to the end of the log file.

        CONCEPT: This is the only write operation. We:
          1. Acquire the lock (block any other thread from writing)
          2. Assign the current next_offset to this message
          3. Increment next_offset for the next call
          4. Write the JSON line to disk
          5. Release the lock

        Returns the offset assigned to this message.

        Example:
            partition.append("order placed")   → returns 0
            partition.append("order shipped")  → returns 1
        """
        with self._lock:           # ← only one thread inside here at a time
            offset = self.next_offset

            # Build the message record
            record = {
                "offset"   : offset,
                "key"      : key,
                "value"    : value,
                "timestamp": int(time.time())
            }

            # Append to file — 'a' mode means "open at end, never overwrite"
            with open(self.log_path, 'a') as f:
                f.write(json.dumps(record) + '\n')   # one JSON object per line

            self.next_offset += 1
            return offset

    # ── Read ───────────────────────────────────────────────────

    def fetch(self, start_offset: int, max_messages: int = 10) -> list:
        """
        Read messages starting from start_offset.

        CONCEPT: Sequential scan
          We open the file and read line by line. Any line whose
          offset < start_offset is skipped. We collect up to
          max_messages lines after that point.

          This is O(n) where n = number of messages before start_offset.
          Real Kafka uses an index file to jump directly to the right
          position — we skip that for Phase 1 to keep it simple.

        Example:
            # Log has offsets 0,1,2,3,4
            partition.fetch(start_offset=2, max_messages=2)
            → [{"offset":2,...}, {"offset":3,...}]
        """
        messages = []

        # If the log file doesn't exist yet, return empty list
        if not os.path.exists(self.log_path):
            return messages

        with open(self.log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue   # skip any corrupted line

                # Skip messages before the requested offset
                if record['offset'] < start_offset:
                    continue

                messages.append(record)

                # Stop once we have enough messages
                if len(messages) >= max_messages:
                    break

        return messages

    # ── Helpers ────────────────────────────────────────────────

    def _recover_offset(self) -> int:
        """
        CONCEPT: Crash recovery
          If the broker restarts, we need to know what offset to
          assign next. We read the last line of the log file and
          return last_offset + 1.

          If the file is empty or doesn't exist, start from 0.
        """
        if not os.path.exists(self.log_path):
            return 0

        last_offset = -1
        with open(self.log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        last_offset = record['offset']
                    except json.JSONDecodeError:
                        pass

        return last_offset + 1   # next message gets last+1

    def size(self) -> int:
        """Return total number of messages stored in this partition."""
        return self.next_offset

    def __repr__(self):
        return f"Partition(topic={self.topic}, id={self.partition_id}, messages={self.next_offset})"
