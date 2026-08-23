"""
CONCEPT: The Log — An Ordered Series of Segments
--------------------------------------------------
`Log` owns the segments of one partition and everything that spans them:
which segment holds an offset, when to roll a new one, when to delete old ones,
and how to repair the log after a crash.

    Log (data/orders-0/)
     ├── Segment base=0      sealed    offsets 0..5192
     ├── Segment base=5193   sealed    offsets 5193..9470
     └── Segment base=9471   ACTIVE    offsets 9471..9612   ← appends land here

    log_start_offset = 0        (rises as retention deletes from the front)
    next_offset      = 9613     (one past the newest record)


THE READ PATH — THE POINT OF THE WHOLE PHASE
----------------------------------------------
    read(offset):
      1. binary search the segment base offsets   → which file?
      2. binary search that segment's index       → roughly where in it?
      3. seek + scan forward a few KB             → the record

Every step is O(log n) or O(1). **Nothing in that path depends on how much data
the log holds.** Phase 1 read from byte zero every time and got measurably
slower as the log grew; this does not.


WHY THE ACTIVE SEGMENT IS SPECIAL
-----------------------------------
Exactly one segment takes writes. Every other segment is sealed and never
modified again. Almost everything convenient follows from that:

  - Only the active segment can have a torn tail, so **startup scans one file
    rather than the whole log.** On a 500 GB partition that is the difference
    between seconds and hours.
  - Sealed segments need no locking against writers.
  - Retention deletes sealed segments only — you never delete the file
    someone is appending to.


DURABILITY POLICY
-------------------
Every append does `flush()` — Python buffer to OS page cache — so readers see
the record immediately. That survives `kill -9` of the broker.

`fsync()` — page cache to physical disk — is OFF by default. It survives power
loss and costs ~1-10 ms, roughly 1000x more than a flush.

Real Kafka defaults the same way, and the justification is replication: with
three copies on three machines, losing one node's page cache loses nothing.
**Kafka buys durability with replicas rather than with fsync**, which only
becomes a defensible position once Phase 6 exists. Until then it is a known
weakness, and `fsync_messages` / `fsync_ms` let you measure the cost yourself.
"""

import os
import time
from dataclasses import dataclass

from broker.log.record import iter_records
from broker.log.segment import SEGMENT_NAME_WIDTH, Segment
from common.config import (
    DEFAULT_FSYNC_MESSAGES, DEFAULT_FSYNC_MS, DEFAULT_INDEX_INTERVAL_BYTES,
    DEFAULT_RETENTION_BYTES, DEFAULT_RETENTION_MS, DEFAULT_SEGMENT_BYTES,
    DEFAULT_SEGMENT_MS,
)


@dataclass
class LogConfig:
    """
    Storage tuning for one log. Defaults live in common/config.py.

    Every field here is a real Kafka setting under a similar name, so the
    reasoning transfers even though the values do not (our segments are 1 MB
    rather than 1 GB, so rolling is observable in a demo instead of after a
    week).
    """
    segment_bytes: int = DEFAULT_SEGMENT_BYTES
    segment_ms: float = DEFAULT_SEGMENT_MS
    index_interval_bytes: int = DEFAULT_INDEX_INTERVAL_BYTES
    retention_ms: float = DEFAULT_RETENTION_MS
    retention_bytes: int = DEFAULT_RETENTION_BYTES      # -1 = unlimited
    fsync_messages: int = DEFAULT_FSYNC_MESSAGES        # 0 = never
    fsync_ms: float = DEFAULT_FSYNC_MS                  # 0 = never


class Log:
    """
    The segmented, indexed, self-repairing log for one partition.

    Not thread-safe. `Partition` owns the lock.
    """

    def __init__(self, directory: str, config: LogConfig = None):
        self.directory = directory
        self.config = config or LogConfig()

        os.makedirs(directory, exist_ok=True)

        self.segments: list = []        # ascending by base_offset
        self.corrupt_bytes_recovered = 0

        self._messages_since_fsync = 0
        self._last_fsync = time.time()

        self._load()

    # ── Startup ────────────────────────────────────────────────

    def _load(self) -> None:
        """
        Discover segments on disk and recover the active one.

        The 20-digit zero-padded names mean lexical order IS numeric order, so
        a plain sort gives us the segments in offset order with no parsing.
        """
        base_offsets = sorted(
            int(name[:SEGMENT_NAME_WIDTH])
            for name in os.listdir(self.directory)
            if name.endswith('.log') and len(name) > SEGMENT_NAME_WIDTH
        )

        if not base_offsets:
            self.segments = [Segment.create(self.directory, 0,
                                            self.config.index_interval_bytes)]
            return  

        for i, base_offset in enumerate(base_offsets):
            segment = Segment(self.directory, base_offset,
                              self.config.index_interval_bytes)
            is_active = (i == len(base_offsets) - 1)

            if is_active:
                # Only this one can have been mid-write when we died, so only
                # this one is scanned. That is what keeps startup fast.
                rebuild = not os.path.exists(segment.index_path)
                self.corrupt_bytes_recovered += segment.recover(rebuild_index=rebuild)
                segment.open_for_append()
            else:
                # A sealed segment is immutable, so its size is just the file
                # size and its end is the next segment's start — both free.
                segment.size_bytes = os.path.getsize(segment.log_path)
                segment.next_offset = base_offsets[i + 1]
                if os.path.exists(segment.index_path):
                    segment.index.load()
                else:
                    # An index is derived data and can always be rebuilt from
                    # the log. (The reverse is not true — which is why the log
                    # is the only thing that has to survive.)
                    segment.recover(rebuild_index=True)
                    segment.next_offset = base_offsets[i + 1]

            self.segments.append(segment)

    # ── Properties ─────────────────────────────────────────────

    @property
    def active_segment(self) -> Segment:
        return self.segments[-1]

    @property
    def next_offset(self) -> int:
        """Offset the next appended record will receive."""
        return self.active_segment.next_offset

    @property
    def log_start_offset(self) -> int:
        """
        Oldest offset still readable.

        0 until retention deletes a segment, then it jumps to the base offset
        of the oldest surviving one. A consumer asking below this gets
        OFFSET_OUT_OF_RANGE — "you fell so far behind that your data is gone",
        the failure that consumer lag monitoring exists to prevent.
        """
        return self.segments[0].base_offset

    def total_bytes(self) -> int:
        return sum(segment.size_bytes for segment in self.segments)

    # ── Writing ────────────────────────────────────────────────

    def append(self, value: str, key: str = None) -> int:
        """Append one record and return the offset assigned to it."""
        self._maybe_roll()

        record = {
            'offset': self.next_offset,
            'key': key,
            'value': value,
            # Wall clock. Fine for retention and display, useless for ordering:
            # clocks on different machines disagree and go backwards when NTP
            # corrects them. THE OFFSET is the ordering, always.
            'timestamp': int(time.time()),
        }

        offset = self.active_segment.append(record)
        self._maybe_fsync()
        return offset

    def _maybe_roll(self) -> None:
        """
        Seal the active segment and start a new one if it is full or old.

        Rolled on the append that would exceed the limit, so a segment can end
        slightly under `segment_bytes` but never meaningfully over. Size is the
        usual trigger; `segment_ms` exists so a low-traffic topic still rolls
        eventually — otherwise its single segment could never be sealed, and
        therefore never deleted by retention, and the topic would keep data
        forever regardless of policy.
        """
        active = self.active_segment
        too_big = active.size_bytes >= self.config.segment_bytes
        too_old = (self.config.segment_ms > 0
                   and active.size_bytes > 0
                   and (time.time() - active.created_at) * 1000 >= self.config.segment_ms)

        if not (too_big or too_old):
            return

        active.seal()
        self.segments.append(Segment.create(
            self.directory, active.next_offset, self.config.index_interval_bytes))

    def _maybe_fsync(self) -> None:
        """Apply the flush policy. Both triggers are off by default."""
        self._messages_since_fsync += 1

        by_count = (self.config.fsync_messages > 0
                    and self._messages_since_fsync >= self.config.fsync_messages)
        by_time = (self.config.fsync_ms > 0
                   and (time.time() - self._last_fsync) * 1000 >= self.config.fsync_ms)

        if by_count or by_time:
            self.active_segment.flush_to_disk()
            self._messages_since_fsync = 0
            self._last_fsync = time.time()

    # ── Reading ────────────────────────────────────────────────

    def read(self, start_offset: int, max_messages: int = 10) -> list:
        """
        Return up to `max_messages` records with offset >= start_offset.

        Reads across a segment boundary when a batch spans one — a caller
        asking for 100 messages should get 100, not "however many happened to
        be left in that file".
        """
        if max_messages <= 0:
            return []

        # Below the log start means retention has deleted that data. Return
        # nothing rather than the oldest surviving records.
        #
        # This guard is load-bearing, not defensive noise: without it, asking
        # for offset 0 after retention deleted through offset 295 quietly
        # returns records starting at 295 — the consumer believes it read from
        # the beginning, and 295 messages vanish with no error anywhere. The
        # broker turns this empty result into OFFSET_OUT_OF_RANGE, which is
        # the honest answer: "you fell so far behind that your data is gone."
        if start_offset < self.log_start_offset:
            return []

        index = self._segment_index_containing(start_offset)
        if index is None:
            return []

        messages = []
        for segment in self.segments[index:]:
            messages.extend(segment.read(start_offset, max_messages - len(messages)))
            if len(messages) >= max_messages:
                break

        return messages

    def _segment_index_containing(self, offset: int):
        """
        Index of the segment holding `offset`, or None if it is past the end.

        Binary search by hand rather than via `bisect`: segment i covers
        [base_i, base_{i+1}), and expressing that as a bisect key is less clear
        than the loop.

        Callers must already have rejected offsets below `log_start_offset` —
        this returns segment 0 for those, which would then serve the oldest
        surviving records rather than the ones asked for. `read()` guards it.
        """
        if offset >= self.next_offset:
            return None

        low, high = 0, len(self.segments) - 1
        result = 0
        while low <= high:
            mid = (low + high) // 2
            if self.segments[mid].base_offset <= offset:
                result = mid
                low = mid + 1
            else:
                high = mid - 1
        return result

    # ── Retention ──────────────────────────────────────────────

    def delete_old_segments(self) -> list:
        """
        Apply retention. Returns the base offsets deleted.

        CONCEPT: delete files, never records.
          Removing individual records would mean rewriting a file, which
          destroys the sequential-append property the whole design rests on.
          Whole-segment deletion is a single cheap filesystem operation.

          The cost of that simplicity is granularity: retention is only as
          precise as one segment. A record can outlive `retention_ms` by up to
          however long its segment took to fill. Real Kafka has exactly this
          behaviour, and it surprises people who expect per-record expiry.

        The active segment is never deleted — you do not delete the file you
        are writing to. This means a topic with a single unfilled segment keeps
        everything regardless of policy, which is what `segment_ms` is for.
        """
        deleted = []

        # By age. Only sealed segments are candidates, hence [:-1].
        if self.config.retention_ms > 0:
            cutoff = time.time() - (self.config.retention_ms / 1000.0)
            while len(self.segments) > 1:
                oldest = self.segments[0]
                if oldest.largest_timestamp() >= cutoff:
                    break
                oldest.delete()
                deleted.append(self.segments.pop(0).base_offset)

        # By total size.
        if self.config.retention_bytes > 0:
            while (len(self.segments) > 1
                   and self.total_bytes() > self.config.retention_bytes):
                oldest = self.segments[0]
                oldest.delete()
                deleted.append(self.segments.pop(0).base_offset)

        return deleted

    # ── Truncation (Phase 6) ───────────────────────────────────

    def truncate_to(self, offset: int) -> int:
        """
        Remove every record with offset >= `offset`. Returns bytes removed.

        Unused until Phase 6, where a follower whose log has diverged from a
        new leader truncates to the high watermark and re-fetches from there.
        Building it with the storage layer costs little; retrofitting it under
        working replication would be miserable.
        """
        if offset >= self.next_offset:
            return 0
        if offset <= self.log_start_offset:
            offset = self.log_start_offset

        removed = 0

        # Drop whole segments that start at or above the cut.
        while len(self.segments) > 1 and self.segments[-1].base_offset >= offset:
            segment = self.segments.pop()
            removed += segment.size_bytes
            segment.delete()

        # Then cut inside the new last segment.
        last = self.segments[-1]
        was_sealed = last._fh is None
        removed += last.truncate_to(offset)
        if was_sealed:
            last.open_for_append()

        return removed

    # ── Lifecycle ──────────────────────────────────────────────

    def close(self) -> None:
        for segment in self.segments:
            segment.close()

    def verify(self) -> dict:
        """
        Read every record and check every checksum. For the admin CLI.

        Full scan, so it is an explicit operation rather than something the
        read path does. Reports where the valid data ends per segment, so a
        gap between that and the file size means damage.
        """
        report = {'segments': [], 'records': 0, 'bytes': 0, 'damaged_segments': 0}

        for segment in self.segments:
            count = 0
            valid_end = 0
            with open(segment.log_path, 'rb') as fh:
                for position, size, _record in iter_records(fh):
                    count += 1
                    valid_end = position + size

            file_size = os.path.getsize(segment.log_path)
            damaged = file_size - valid_end
            if damaged:
                report['damaged_segments'] += 1

            report['segments'].append({
                'base_offset': segment.base_offset,
                'records': count,
                'bytes': file_size,
                'valid_bytes': valid_end,
                'damaged_bytes': damaged,
                'index_entries': len(segment.index),
            })
            report['records'] += count
            report['bytes'] += file_size

        return report

    def __repr__(self) -> str:
        return (f"Log({self.directory}, segments={len(self.segments)}, "
                f"offsets={self.log_start_offset}..{self.next_offset - 1}, "
                f"bytes={self.total_bytes()})")
