"""
CONCEPT: The Sparse Offset Index
----------------------------------
The fix for the O(n) read.

Without an index, finding offset 5,000,000 means parsing five million records
you do not want. With one, it means two seeks.

    .index  (8 bytes per entry)                  .log
  ┌────────────┬────────────┐
  │ rel_off  0 │ pos      0 │ ──────────────────► record 9471
  │ rel_off 47 │ pos   4096 │ ──────────────────► record 9518
  │ rel_off 93 │ pos   8192 │ ──────────────────► record 9564
  └────────────┴────────────┘

Looking up offset 9540:
  1. binary search the index → the largest entry at or below it: (47, 4096)
  2. seek(4096) in the .log , O(1) for seek
  3. scan forward a few records until offset >= 9540

Two binary searches and a short sequential read. **Nothing in that path depends
on how much data the log holds.**


WHY SPARSE, NOT DENSE
-----------------------
A dense index — one entry per record — gives O(1) lookup with no scan at all.
It also costs 8 bytes per record: a billion records is 8 GB of index, which
must be held in memory to be useful and rebuilt entirely after a crash.

Sparse indexes one entry per `index_interval_bytes` (4 KB, matching real
Kafka). That is roughly 0.1% of the log size, and the price is scanning up to
4 KB forward after the seek.

The trade is deliberate and asymmetric in our favour: **we give up a small
amount of sequential reading — the thing disks are best at — to avoid a large
amount of memory and random seeking.** Sparse indexing is not a compromise
version of dense indexing; it is the better design for this access pattern.


WHY RELATIVE OFFSETS
----------------------
Entries store `offset - base_offset`, not the absolute offset. With a segment
capped at 1 GB, the relative offset always fits in 4 bytes, where an absolute
offset would need 8. That halves an index that is memory-mapped and hot on a
real broker.

It also makes a segment self-contained: its index is meaningful without knowing
anything outside the file name.


WHY IT IS SAFE TO DELETE AN INDEX
-----------------------------------
An index holds no information the log does not. Delete it and it is rebuilt by
scanning the segment once. That asymmetry is worth noticing: the log is the
source of truth, and everything else is a derived accelerator that can be
thrown away. The reverse is not true — you cannot rebuild a log from an index.

That principle is why recovery is simple. Anything derived can be recomputed;
only the log itself has to survive.
"""

import bisect
import os
import struct

# '!' network byte order — an index file sits next to a log file and moves with
# it, so it gets the same portability treatment.
INDEX_ENTRY_FORMAT = '!I I'                         # relative_offset, position
INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FORMAT)          # 8


class Index:
    """
    A sparse offset→position index for one segment.

    Held entirely in memory as two parallel lists and appended to disk as it
    grows. Real Kafka memory-maps the file so lookups are plain memory reads
    with no syscall; we keep it simpler at the cost of some speed.
    """

    def __init__(self, path: str, base_offset: int):
        self.path = path
        self.base_offset = base_offset

        # Two parallel lists rather than a list of tuples, so `bisect` can
        # binary-search the offsets directly with no key function and no
        # per-comparison tuple unpacking.
        self._offsets: list = []        # relative offsets, ascending
        self._positions: list = []      # byte positions in the .log

        self._fh = None                 # opened lazily, only if we append

    # ── Loading ────────────────────────────────────────────────

    def load(self) -> None:
        """
        Read an existing index file into memory.

        A truncated final entry is ignored rather than treated as an error. An
        index is derived data: a torn one costs a longer scan, never
        correctness, so there is nothing to fail about.
        """
        if not os.path.exists(self.path):
            return

        with open(self.path, 'rb') as fh:
            data = fh.read()

        usable = len(data) - (len(data) % INDEX_ENTRY_SIZE)
        for i in range(0, usable, INDEX_ENTRY_SIZE):
            rel_offset, position = struct.unpack(
                INDEX_ENTRY_FORMAT, data[i:i + INDEX_ENTRY_SIZE])
            self._offsets.append(rel_offset)
            self._positions.append(position)

    # ── Writing ────────────────────────────────────────────────

    def append(self, offset: int, position: int) -> None:
        """
        Record that `offset` starts at byte `position`.

        The caller decides *when* to call this — every `index_interval_bytes`
        — which is what makes the index sparse. This method just records what
        it is told.
        """
        relative = offset - self.base_offset
        if relative < 0:
            raise ValueError(
                f"offset {offset} is below this segment's base {self.base_offset}")

        # Entries must stay ascending or the binary search is meaningless.
        # Silently accepting an out-of-order entry would produce a lookup that
        # returns a position past the record we wanted — and a scan forward
        # from there would skip it entirely, reporting messages as missing.
        if self._offsets and relative <= self._offsets[-1]:
            return

        self._offsets.append(relative)
        self._positions.append(position)

        if self._fh is None:
            self._fh = open(self.path, 'ab')
        self._fh.write(struct.pack(INDEX_ENTRY_FORMAT, relative, position))
        self._fh.flush()

    # ── Lookup ─────────────────────────────────────────────────

    def lookup(self, offset: int) -> int:
        """
        Return a byte position at or before `offset`. Never past it.

        "At or before" is the contract that makes a sparse index correct: the
        caller scans forward from here, so landing early costs a few
        microseconds while landing late would silently skip records. When in
        doubt, this returns 0 and the caller scans the whole segment — slow,
        but never wrong.
        """
        relative = offset - self.base_offset
        if relative <= 0 or not self._offsets:
            return 0

        # bisect_right gives the insertion point; the entry before it is the
        # largest one <= our target.
        i = bisect.bisect_right(self._offsets, relative) - 1
        return self._positions[i] if i >= 0 else 0

    # ── Maintenance ────────────────────────────────────────────

    def truncate_after(self, position: int) -> None:
        """
        Drop entries pointing at or past `position`, and rewrite the file.

        Needed after recovery cuts a torn tail off a log, and after
        `Log.truncate_to` (Phase 6 replication). An index entry pointing past
        the end of its log would send a reader seeking beyond EOF.
        """
        keep = sum(1 for p in self._positions if p < position)
        if keep == len(self._positions):
            return

        self._offsets = self._offsets[:keep]
        self._positions = self._positions[:keep]

        self.close()
        with open(self.path, 'wb') as fh:
            for rel_offset, pos in zip(self._offsets, self._positions):
                fh.write(struct.pack(INDEX_ENTRY_FORMAT, rel_offset, pos))

    def clear(self) -> None:
        """Forget everything, in memory and on disk. Used before a rebuild."""
        self._offsets.clear()
        self._positions.clear()
        self.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def delete(self) -> None:
        self.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    # ── Introspection ──────────────────────────────────────────

    def __len__(self) -> int:
        return len(self._offsets)

    def entries(self) -> list:
        """Absolute (offset, position) pairs. For the admin CLI and tests."""
        return [(self.base_offset + o, p)
                for o, p in zip(self._offsets, self._positions)]

    def __repr__(self) -> str:
        return f"Index({os.path.basename(self.path)}, entries={len(self._offsets)})"
