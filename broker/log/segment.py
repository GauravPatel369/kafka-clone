"""
CONCEPT: A Segment — One .log File and Its .index
---------------------------------------------------
A partition's log is not one file. It is an ordered series of segments, each
named by the offset of its first record:

    00000000000000000000.log     base offset 0        sealed
    00000000000000000000.index
    00000000000000005193.log     base offset 5193     sealed
    00000000000000005193.index
    00000000000000009471.log     base offset 9471     ACTIVE — appends land here
    00000000000000009471.index

Exactly one segment is *active*. When it reaches `segment_bytes` (or
`segment_ms`), it is sealed and a new one begins.


WHY 20 DIGITS, ZERO-PADDED?
-----------------------------
So that **lexical order equals numeric order**. `sorted(os.listdir(...))`
returns the segments in offset order with no parsing, no int conversion, no
sort key. Without padding, "10000.log" sorts before "9471.log" and the whole
structure silently breaks.

20 digits because a signed 64-bit offset maxes out at 19 digits, so it can
never overflow the field. Real Kafka uses exactly this.


WHY SEGMENTS ARE THE ENABLING IDEA
------------------------------------
Splitting one file into many looks like bookkeeping. It is what makes four
separate features possible:

  Fast lookup   Binary search the *file names* to find which segment holds an
                offset, before even opening a file.

  Retention     Deleting old data becomes deleting a FILE — one cheap
                operation. Deleting records from inside one file would mean
                rewriting it, destroying the sequential-append property that
                the entire design rests on.

  Recovery      Only the active segment can be torn, because sealed segments
                are never written again. Startup scans one file, not the whole
                log — the difference between a second and an hour.

  Compaction    A sealed segment can be rewritten in the background without
                touching the one taking writes.


SEALED SEGMENTS ARE IMMUTABLE, AND THAT IS LOAD-BEARING
---------------------------------------------------------
Once sealed, a segment is never modified. Readers need no locks against
writers, backup tools can copy it safely, and replication can ship it whole.
Immutability is not a side effect of the append-only design — it is most of the
value of it.
"""

import os
import time

from broker.log.index import Index
from broker.log.record import encode_record, iter_records

# 20 digits: a signed 64-bit offset is at most 19, so this can never overflow.
SEGMENT_NAME_WIDTH = 20


def segment_name(base_offset: int, suffix: str) -> str:
    return f"{base_offset:0{SEGMENT_NAME_WIDTH}d}{suffix}"


class Segment:
    """
    One segment: a `.log` of records and a sparse `.index` beside it.

    Not thread-safe on its own. `Partition` holds the lock; `Log` assumes calls
    are already serialised.
    """

    def __init__(self, directory: str, base_offset: int,
                 index_interval_bytes: int = 4096):
        self.directory = directory
        self.base_offset = base_offset
        self.index_interval_bytes = index_interval_bytes

        self.log_path = os.path.join(directory, segment_name(base_offset, '.log'))
        self.index_path = os.path.join(directory, segment_name(base_offset, '.index'))

        self.index = Index(self.index_path, base_offset)

        self.size_bytes = 0             # bytes of VALID data
        self.next_offset = base_offset  # offset the next record will get
        self.created_at = time.time()

        self._fh = None                 # append handle; None while sealed/closed
        self._bytes_since_index = 0
        self._cached_largest_timestamp = None

    # ── Opening ────────────────────────────────────────────────

    def open_for_append(self) -> 'Segment':
        """Open the write handle. Only the active segment does this."""
        self._fh = open(self.log_path, 'ab')
        return self

    @classmethod
    def create(cls, directory: str, base_offset: int,
               index_interval_bytes: int = 4096) -> 'Segment':
        os.makedirs(directory, exist_ok=True)
        segment = cls(directory, base_offset, index_interval_bytes)
        # 'a' rather than 'w': creating a segment must never truncate an
        # existing file. If one is already there we have a bug elsewhere, and
        # silently erasing data would hide it in the worst possible way.
        open(segment.log_path, 'ab').close()
        return segment.open_for_append()

    # ── Writing ────────────────────────────────────────────────

    def append(self, record: dict) -> int:
        """
        Append one record. Returns its offset.

        The index entry is added only every `index_interval_bytes`. That
        counter is what makes the index sparse, and it lives here because this
        is the only place that knows how many bytes have actually been written.
        """
        if self._fh is None:
            raise RuntimeError(f"{self!r} is not open for append")

        offset = self.next_offset
        position = self.size_bytes
        encoded = encode_record(record)

        self._fh.write(encoded)
        # flush() pushes to the OS page cache so READERS — which open their own
        # handle — can see the record immediately. It is not fsync(): the data
        # is not on a platter yet. See Log.append for the durability policy.
        self._fh.flush()

        # Index the FIRST record of each interval, before advancing the
        # counter. Indexing after would leave the interval's first record
        # sitting before the entry that is supposed to lead to it.
        # len(), not entries(): entries() builds a whole list, and this runs on
        # every single append. An O(n) call on the hot write path is exactly
        # the kind of thing that looks harmless and turns the append cost
        # quadratic in the segment size.
        if len(self.index) == 0 or self._bytes_since_index >= self.index_interval_bytes:
            self.index.append(offset, position)
            self._bytes_since_index = 0

        self.size_bytes += len(encoded)
        self._bytes_since_index += len(encoded)
        self.next_offset += 1
        return offset

    def flush_to_disk(self) -> None:
        """Force an fsync. Expensive on purpose — see Log's flush policy."""
        if self._fh is not None:
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def seal(self) -> None:
        """
        Stop accepting appends and release the write handle.

        After this the segment is immutable, which is what lets readers,
        backups, and (from Phase 6) replication treat it as a stable artefact.
        """
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None
        self.index.close()

    # ── Reading ────────────────────────────────────────────────

    def read(self, start_offset: int, max_messages: int) -> list:
        """
        Return up to `max_messages` records with offset >= start_offset.

        The whole point of the phase, in four lines: ask the index roughly
        where to start, seek there, scan forward a little.
        """
        if not os.path.exists(self.log_path):
            return []

        position = self.index.lookup(start_offset)
        messages = []

        with open(self.log_path, 'rb') as fh:
            for _pos, _size, record in iter_records(fh, position):
                # The index lands at or BEFORE the target, so a few records
                # here may still be below it. That short scan is exactly what
                # sparseness trades away.
                if record['offset'] < start_offset:
                    continue
                messages.append(record)
                if len(messages) >= max_messages:
                    break

        return messages

    def last_offset(self) -> int:
        """Offset of the newest record, or base_offset - 1 if empty."""
        return self.next_offset - 1

    def largest_timestamp(self) -> float:
        """
        Timestamp of the newest record, for time-based retention.

        Falls back to the segment's creation time for an empty segment, so a
        segment that never received a record still ages out rather than living
        forever.

        Cached once the segment is sealed. Retention runs on a timer over every
        segment, and this is a full scan — recomputing it for immutable files
        on every sweep would make the cleaner's cost proportional to the whole
        log, which is precisely what segments exist to avoid. A sealed segment
        cannot change, so the cached answer cannot go stale.
        """
        if not os.path.exists(self.log_path) or self.size_bytes == 0:
            return self.created_at

        if self._cached_largest_timestamp is not None:
            return self._cached_largest_timestamp

        newest = self.created_at
        with open(self.log_path, 'rb') as fh:
            for _pos, _size, record in iter_records(fh):
                newest = max(newest, record.get('timestamp', 0))

        if self._fh is None:            # sealed → immutable → safe to cache
            self._cached_largest_timestamp = newest
        return newest

    # ── Recovery ───────────────────────────────────────────────

    def recover(self, rebuild_index: bool = False) -> int:
        """
        Scan the segment, repair a torn tail, and set size/next_offset.

        Returns the number of bytes truncated.

        THIS IS WHERE THE CRC EARNS ITS PLACE. `iter_records` stops at the
        first record whose length is impossible or whose checksum fails.
        Everything after that point is unrecoverable *by definition* — the
        length field that would tell us where the next record begins is exactly
        what we cannot trust — so it is cut off rather than left in the file
        forever.

        Phase 0 could only skip a bad line and leave it in place. Now the file
        is actually repaired.

        `rebuild_index` regenerates the index from scratch. Safe at any time:
        an index holds no information the log does not.
        """
        if not os.path.exists(self.log_path):
            self.size_bytes = 0
            self.next_offset = self.base_offset
            return 0

        if rebuild_index:
            self.index.clear()
        else:
            self.index.load()

        file_size = os.path.getsize(self.log_path)
        valid_end = 0
        last_offset = self.base_offset - 1
        bytes_since_index = 0

        with open(self.log_path, 'rb') as fh:
            for position, size, record in iter_records(fh):
                valid_end = position + size
                last_offset = record['offset']

                if rebuild_index:
                    if len(self.index) == 0 or bytes_since_index >= self.index_interval_bytes:
                        self.index.append(record['offset'], position)
                        bytes_since_index = 0
                    bytes_since_index += size

        truncated = file_size - valid_end
        if truncated > 0:
            # Reopen in 'r+b' to truncate: 'ab' cannot shorten a file.
            with open(self.log_path, 'r+b') as fh:
                fh.truncate(valid_end)
            # An index entry pointing past the new end would send a reader
            # seeking beyond EOF, so it goes too.
            self.index.truncate_after(valid_end)

        self.size_bytes = valid_end
        self.next_offset = last_offset + 1
        self._bytes_since_index = bytes_since_index
        return truncated

    def truncate_to(self, offset: int) -> int:
        """
        Drop every record with offset >= `offset`. Returns bytes removed.

        Nothing calls this until Phase 6, where a follower that has diverged
        from a new leader truncates to the high watermark and re-fetches.
        Building it alongside the storage layer is far easier than bolting it
        on once replication is depending on it.
        """
        if not os.path.exists(self.log_path):
            return 0

        cut_at = self.size_bytes
        new_next = self.next_offset

        with open(self.log_path, 'rb') as fh:
            for position, _size, record in iter_records(fh):
                if record['offset'] >= offset:
                    cut_at = position
                    new_next = record['offset']
                    break

        if cut_at >= self.size_bytes:
            return 0

        was_open = self._fh is not None
        if was_open:
            self._fh.close()
            self._fh = None

        removed = self.size_bytes - cut_at
        with open(self.log_path, 'r+b') as fh:
            fh.truncate(cut_at)

        self.index.truncate_after(cut_at)
        self.size_bytes = cut_at
        self.next_offset = new_next

        if was_open:
            self.open_for_append()
        return removed

    # ── Lifecycle ──────────────────────────────────────────────

    def delete(self) -> None:
        """Remove both files. Retention deletes whole segments, never records."""
        self.seal()
        self.index.delete()
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

    def close(self) -> None:
        self.seal()

    def __repr__(self) -> str:
        state = 'active' if self._fh is not None else 'sealed'
        return (f"Segment(base={self.base_offset}, "
                f"offsets={self.base_offset}..{self.last_offset()}, "
                f"bytes={self.size_bytes}, index={len(self.index)}, {state})")
