"""
CONCEPT: The Append-Only Partition Log
----------------------------------------
This is the heart of Kafka. A partition is a file on disk where every message
is written at the end — never modified, never deleted (until retention deletes
whole files, in Phase 2).

The file looks like this on disk, one JSON object per line:

    {"offset": 0, "key": null,      "value": "order placed",  "timestamp": 1700000000}
    {"offset": 1, "key": "user-42", "value": "payment done",  "timestamp": 1700000001}
    {"offset": 2, "key": null,      "value": "shipped",       "timestamp": 1700000002}


WHY APPEND-ONLY? THE ANSWER IS PHYSICS.
----------------------------------------
A spinning disk must physically move a head to reach a location: a random
write costs ~10 ms of seek time. Writing to the end of a file you already have
open costs no seek at all — the head is already there.

    Random writes on a 7200rpm disk:   ~100 per second
    Sequential writes on the same disk: ~100,000 per second

That is a factor of a thousand, from nothing but *where* you write. SSDs
narrow the gap but do not close it (sequential writes still avoid
write-amplification and let the controller use full erase blocks).

So Kafka made a decision that looks almost lazy: **never update anything, only
append.** No in-place edits, no B-tree rebalancing, no "find the record and
change it". Every design consequence flows from that one choice:

  - Messages are immutable → many consumers can read the same bytes safely,
    with no locking, because nothing ever changes underneath them.
  - Offsets are dense and monotonic → position N is just "the Nth record",
    which is what makes an offset a meaningful, permanent address.
  - Deletion is not per-message → you delete a whole old file at once, which
    is again sequential, again fast.


WHY ONE JSON OBJECT PER LINE?
------------------------------
    +  You can `cat` the file and read it. For learning, that is worth a lot.
    +  No schema, no codegen, no dependencies.
    -  ~3x more bytes than a binary encoding.
    -  Parsing is slow: json.loads is the hot path in `fetch()`.
    -  No corruption detection — a torn write produces a line that either
       fails to parse (fine) or parses into something wrong (not fine).

Phase 2 replaces this with a length-prefixed binary record carrying a CRC32,
and adds segments plus an index so reads stop being O(n). For now, readable
beats fast.


WHAT IS STILL WRONG WITH THIS FILE (deliberately, until Phase 2)
-----------------------------------------------------------------
  1. `fetch()` scans from byte zero every time. Reading offset 1,000,000
     means parsing a million JSON lines first. This is the single biggest
     performance flaw in the project and Phase 2 exists to fix it.
  2. The log grows forever. No retention, no segments, no cleanup.
  3. Readers take no lock (see `fetch`), so a reader can in principle observe
     a half-flushed line. We tolerate it by skipping unparseable lines.
"""

import json
import os
import threading
import time

# How many bytes to read from the end of the log when recovering the next
# offset at startup. 64 KB holds hundreds of records, so in practice the very
# first line we examine is the one we want. If it is not (a pathologically
# long record, or a corrupt tail), `_recover_offset` falls back to a full scan.
TAIL_READ_BYTES = 64 * 1024


class Partition:
    """
    One append-only log file, plus the offset counter that goes with it.

    Thread-safety: `append()` is serialised by `self._lock`. `fetch()` takes
    no lock on purpose — see the comment on that method.
    """

    def __init__(self, topic: str, partition_id: int, data_dir: str = './data'):
        self.topic = topic
        self.partition_id = partition_id
        self.data_dir = data_dir

        os.makedirs(data_dir, exist_ok=True)
        self.log_path = os.path.join(data_dir, f"{topic}-{partition_id}.log")

        # CONCEPT: Why a lock at all?
        #
        # The broker hands `append` to a thread pool (see Broker._route), so
        # two producers really can call it at the same moment. Without the
        # lock:
        #
        #     thread A reads next_offset  → 5
        #     thread B reads next_offset  → 5      ← both got 5
        #     thread A writes record 5, sets next_offset = 6
        #     thread B writes record 5, sets next_offset = 6
        #
        # Two different messages now claim offset 5, and offset 6 never
        # exists. Every guarantee in the system — ordering, replay,
        # deduplication, replication — is built on "an offset names exactly
        # one message". The lock is what makes that true.
        self._lock = threading.Lock()

        # How many unparseable lines we have skipped. Surfaced by the admin
        # CLI: a non-zero count means the log has been damaged, and silently
        # skipping damage is how you lose data without ever noticing.
        self.corrupt_lines = 0

        # The oldest offset still readable. Always 0 today, because nothing is
        # ever deleted. Phase 2's retention will delete whole segments off the
        # front, at which point this becomes non-zero — and a consumer asking
        # for an offset below it gets OFFSET_OUT_OF_RANGE, meaning "you fell
        # so far behind that your data is gone". That is the failure consumer
        # lag monitoring exists to prevent.
        #
        # Defined now so LIST_OFFSETS (Phase 1) reports it, rather than
        # hardcoding a 0 that later has to be hunted down.
        self.log_start_offset = 0

        self.next_offset = self._recover_offset()

        # CONCEPT: Open the file ONCE and keep the handle.
        #
        # The original code did `with open(path, 'a')` inside append(), which
        # paid for an open() and a close() syscall on every single message —
        # in a system whose entire premise is that appending is cheap.
        #
        # Two details in this open() call matter:
        #
        #   'a'            The kernel guarantees every write goes to the
        #                  current end of file, even if something else
        #                  appended in between. We never seek.
        #
        #   newline='\n'   Without this, Python's text mode on Windows
        #                  silently translates '\n' into '\r\n' on write.
        #                  The file would then differ byte-for-byte between
        #                  Windows and Linux, byte offsets would not match,
        #                  and Phase 2's index — which stores byte positions —
        #                  would be wrong on one platform only. That is a
        #                  miserable bug to find later, so we forbid it now.
        self._fh = open(self.log_path, 'a', encoding='utf-8', newline='\n')
        self._closed = False

    # ── Write path ─────────────────────────────────────────────

    def append(self, value: str, key: str = None) -> int:
        """
        Append one message to the end of the log. Returns its offset.

        The offset is assigned *by the broker*, not the client. That is what
        makes it authoritative: two producers writing concurrently get
        distinct offsets and a definite order, decided in one place.
        """
        if self._closed:
            raise RuntimeError(f"{self!r} is closed")

        with self._lock:
            offset = self.next_offset

            record = {
                "offset":    offset,
                "key":       key,
                "value":     value,
                # Wall-clock seconds. Fine for display, useless for ordering:
                # clocks on different machines disagree, and can go backwards
                # when NTP corrects them. THE OFFSET is the ordering, always.
                "timestamp": int(time.time()),
            }

            self._fh.write(json.dumps(record) + '\n')

            # CONCEPT: flush() is not fsync(), and the difference is 1000x.
            #
            #   (nothing)     data sits in Python's buffer. Another process
            #                 cannot see it. Even THIS process's own reader
            #                 cannot see it.
            #   flush()       Python buffer → OS page cache. Now every reader
            #                 on this machine sees it. Survives our process
            #                 being killed. Costs ~1 microsecond.
            #   os.fsync()    page cache → physical platter/flash. Survives
            #                 power loss. Costs ~1-10 MILLISECONDS.
            #
            # We flush. We do not fsync. That means: `kill -9` the broker and
            # nothing is lost; pull the power cord and the last few messages
            # are gone.
            #
            # This is exactly the trade real Kafka makes, and its
            # justification is replication: with three copies on three
            # machines, one node losing its page cache loses nothing, because
            # two other machines still have the data. Kafka buys durability
            # with *replicas*, not with fsync — which is only a defensible
            # position once Phase 6 exists. Until then this is a known,
            # documented weakness. Phase 2 makes the policy configurable so
            # you can measure the cost yourself.
            self._fh.flush()

            self.next_offset += 1
            return offset

    # ── Read path ──────────────────────────────────────────────

    def fetch(self, start_offset: int, max_messages: int = 10) -> list:
        """
        Return up to `max_messages` records with offset >= start_offset.

        CONCEPT: Sequential scan — and why it is a problem.
          We open the file and read from the top, skipping every record
          whose offset is below the one asked for. Reading from offset
          1,000,000 means parsing a million records we are going to throw
          away.

          Cost is O(n) in the number of records BEFORE the one you want, so
          fetch latency grows linearly with how much data the partition
          holds. A broker that gets slower the longer it runs is not a
          broker you can operate.

          Real Kafka splits the log into segment files and keeps a sparse
          index of offset → byte position, so it binary-searches to roughly
          the right place and scans a few KB. That is Phase 2, and the
          benchmark will show the difference clearly.

        CONCEPT: Why this takes no lock.
          Holding `self._lock` here would block every writer for the whole
          duration of a scan — turning a slow read into a stalled broker.
          Since records are immutable and only ever appended, a reader can
          never see a record change underneath it. The only risk is reading
          a line that is still being flushed, and we handle that by skipping
          anything that will not parse. Phase 2's length-prefix + CRC turns
          that from "probably fine" into "provably detected".
        """
        messages = []

        if not os.path.exists(self.log_path):
            return messages

        with open(self.log_path, 'r', encoding='utf-8', newline='\n') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    # A partially-flushed final line, or genuine corruption.
                    # We count it rather than swallowing it silently.
                    self.corrupt_lines += 1
                    continue

                if record.get('offset', -1) < start_offset:
                    continue

                messages.append(record)
                if len(messages) >= max_messages:
                    break

        return messages

    # ── Startup recovery ───────────────────────────────────────

    def _recover_offset(self) -> int:
        """
        Work out what offset the next message should get, after a restart.

        CONCEPT: The broker keeps no separate metadata about the log.
          The log IS the metadata. The next offset is simply "the last
          record's offset, plus one". Nothing else needs to be persisted,
          and there is nothing that can fall out of sync with the data —
          because there is no second copy of the truth.

        The original version read the entire file to find its last line.
        With a million records, restarting meant parsing a million JSON
        objects to learn one integer. Instead we seek to the end and read
        the last 64 KB, then walk backwards to the first line that parses.

        Fallback matters: if the tail is unparseable (a record longer than
        the window, or a badly damaged file) we scan the whole thing rather
        than guess. Guessing here would re-issue offsets that already exist,
        which corrupts the log permanently.
        """
        if not os.path.exists(self.log_path):
            return 0

        size = os.path.getsize(self.log_path)
        if size == 0:
            return 0

        # Binary mode: we are doing byte arithmetic with seek(), and text
        # mode's newline translation would make the numbers lie.
        with open(self.log_path, 'rb') as f:
            window = min(TAIL_READ_BYTES, size)
            f.seek(size - window)
            tail = f.read(window)

        for raw in reversed(tail.split(b'\n')):
            raw = raw.strip()
            if not raw:
                continue
            try:
                record = json.loads(raw.decode('utf-8'))
                return int(record['offset']) + 1
            except (ValueError, KeyError, TypeError):
                # Could be the truncated first line of our window (expected,
                # since we sliced mid-file), or real damage. Either way, keep
                # walking backwards.
                continue

        return self._recover_offset_full_scan()

    def _recover_offset_full_scan(self) -> int:
        """Last resort: read every line and take the highest offset seen."""
        last_offset = -1
        with open(self.log_path, 'r', encoding='utf-8', newline='\n') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    last_offset = max(last_offset, int(record['offset']))
                except (ValueError, KeyError, TypeError):
                    self.corrupt_lines += 1
        return last_offset + 1

    # ── Lifecycle ──────────────────────────────────────────────

    def close(self) -> None:
        """
        Close the write handle.

        Needed now that the handle outlives a single append: without an
        explicit close, a long-running broker leaks one file descriptor per
        partition it ever touched, and on shutdown the last writes might not
        be flushed. `Broker.shutdown()` calls this for every partition.
        """
        if not self._closed and self._fh is not None:
            with self._lock:
                try:
                    self._fh.flush()
                finally:
                    self._fh.close()
                    self._closed = True

    def size(self) -> int:
        """Number of messages stored (== the next offset, since we start at 0)."""
        return self.next_offset

    def bytes_on_disk(self) -> int:
        try:
            return os.path.getsize(self.log_path)
        except OSError:
            return 0

    def __repr__(self) -> str:
        return (f"Partition({self.topic}-{self.partition_id}, "
                f"messages={self.next_offset}, "
                f"bytes={self.bytes_on_disk()})")
