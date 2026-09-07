"""
CONCEPT: A Partition — Identity, Concurrency, and a Log
---------------------------------------------------------
A partition is the thing the broker addresses: `("orders", 0)`. It owns a name,
a lock, and a `Log` that does the actual storage.

    Partition("orders", 0)          identity + the write lock
        └── Log(data/orders-0/)     segments, index, retention, recovery
              ├── Segment base=0    sealed
              └── Segment base=5193 ACTIVE


WHY THE SPLIT (new in Phase 2)
--------------------------------
Through Phase 1 this class did both jobs: it was the addressable unit AND the
on-disk format — a single JSON-lines file, scanned from byte zero on every
read.

Those two responsibilities change for different reasons. The storage format
changed completely in Phase 2 (segments, a sparse index, checksums); the idea
of "a named, concurrency-controlled thing the broker routes to" did not change
at all. So they separated:

    broker/log/         HOW bytes are stored. Knows nothing about topics,
                        brokers, locks, or the network.
    broker/partition.py WHAT is being stored and who may write to it.

The public API here — `append`, `fetch`, `close`, `next_offset`,
`log_start_offset` — is deliberately unchanged, so `broker.py` and the existing
tests did not move a line while the entire storage engine was replaced beneath
them. **A storage rewrite that forces edits everywhere was not encapsulated to
begin with**, and that is a useful thing to be able to check.


WHY THE LOCK
--------------
The broker hands `append` to a thread POOL (`run_in_executor`), so two
producers really can call it at the same instant. Without the lock:

    thread A reads next_offset  → 41
    thread B reads next_offset  → 41      ← both got 41
    thread A writes record 41, sets next_offset = 42
    thread B writes record 41, sets next_offset = 42

Two messages now claim offset 41, and offset 42 never exists. Every guarantee
in the system — ordering, replay, deduplication, replication — rests on **an
offset naming exactly one message**. The lock is what makes that true.

`fetch` takes no lock on purpose: holding it would block every writer for the
duration of a read. Sealed segments are immutable and the active one is only
ever appended to, so a reader can never see a record change underneath it. The
one risk — reading a record still being written — is now caught by its CRC
rather than merely being unlikely. That is the upgrade Phase 2 bought: "probably
fine" became "provably detected".
"""

import os
import threading

from broker.log import Log, LogConfig
from common.config import DEFAULT_DATA_DIR


class Partition:
    """One addressable partition: `(topic, partition_id)` plus its log."""

    def __init__(self, topic: str, partition_id: int,
                 data_dir: str = DEFAULT_DATA_DIR, config: LogConfig = None):
        self.topic = topic
        self.partition_id = partition_id
        self.data_dir = data_dir

        # One DIRECTORY per partition now, not one file. Phase 0/1 wrote
        # `data/orders-0.log`; a partition is now `data/orders-0/` holding
        # several segments and their indexes.
        #
        # This is a breaking change with no migration path, which is a
        # deliberate luxury of a project with no deployed data. Real Kafka
        # cannot do this, which is exactly why it versions its record format
        # and ships converters.
        self.dir_path = os.path.join(data_dir, f"{topic}-{partition_id}")

        self._lock = threading.Lock()
        self.log = Log(self.dir_path, config)

    # ── Write ──────────────────────────────────────────────────

    def append(self, value: str, key: str = None) -> int:
        """Append one message. Returns the offset assigned by the broker."""
        with self._lock:
            return self.log.append(value, key)

    def append_replicated(self, records: list) -> int:
        """
        Append records fetched from this partition's leader, offsets intact.

        Takes the same lock as `append`, and that is not merely tidiness: a
        broker can be a follower for a partition and, an instant later, its
        leader. Two writers on one log is exactly what the lock exists for,
        and the window where both paths are live is precisely a failover —
        the moment least able to survive a duplicated offset.
        """
        with self._lock:
            return self.log.append_replicated(records)

    # ── Read ───────────────────────────────────────────────────

    def fetch(self, start_offset: int, max_messages: int = 10,
              limit_offset: int = None) -> list:
        """
        Return up to `max_messages` records with offset >= start_offset.

        No longer a scan from byte zero. The `Log` binary-searches its segments
        and then its sparse index, seeks, and reads a few KB — so this costs
        the same whether the partition holds a thousand records or a billion.
        That change is the entire point of Phase 2, and the benchmark's seek
        profile is where you can see it.

        `limit_offset` (Phase 6) is the high watermark: records at or after it
        exist locally but are not yet on every replica, so a consumer must not
        see them. A follower passes None — it is the one reader that must be
        able to read past the watermark, because its doing so is what advances
        it.
        """
        return self.log.read(start_offset, max_messages, limit_offset)

    # ── State ──────────────────────────────────────────────────

    @property
    def next_offset(self) -> int:
        """Offset the next message will receive; also the high watermark."""
        return self.log.next_offset

    @property
    def log_start_offset(self) -> int:
        """
        Oldest offset still readable.

        Was hardcoded to 0 through Phase 1 because nothing was ever deleted.
        Now retention deletes whole segments off the front and this rises —
        which is what finally makes OFFSET_OUT_OF_RANGE reachable, meaning
        "you fell so far behind that your data is gone."
        """
        return self.log.log_start_offset

    def size(self) -> int:
        """Messages currently retained."""
        return self.log.next_offset - self.log.log_start_offset

    def bytes_on_disk(self) -> int:
        return self.log.total_bytes()

    @property
    def corrupt_bytes_recovered(self) -> int:
        """
        Bytes truncated at startup because they failed their checksum.

        Non-zero means this partition had a torn tail — the broker died
        mid-write, or the disk lost bytes. Surfaced by the admin CLI: silently
        repairing damage without reporting it is how you lose data and never
        find out.
        """
        return self.log.corrupt_bytes_recovered

    # ── Maintenance ────────────────────────────────────────────

    def delete_old_segments(self) -> list:
        """Apply retention. Returns the base offsets deleted."""
        with self._lock:
            return self.log.delete_old_segments()

    def truncate_to(self, offset: int) -> int:
        """Drop everything at or above `offset`. Used from Phase 6."""
        with self._lock:
            return self.log.truncate_to(offset)

    def verify(self) -> dict:
        """Full scan checking every checksum. For the admin CLI."""
        return self.log.verify()

    def close(self) -> None:
        with self._lock:
            self.log.close()

    def __repr__(self) -> str:
        return (f"Partition({self.topic}-{self.partition_id}, "
                f"offsets={self.log_start_offset}..{self.next_offset - 1}, "
                f"segments={len(self.log.segments)}, "
                f"bytes={self.bytes_on_disk()})")
