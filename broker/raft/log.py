"""
CONCEPT: The Raft Log — Where Decisions Live Before They Are Decisions
------------------------------------------------------------------------
Every change to cluster metadata — a broker registering, a topic being created,
partitions being placed — becomes an **entry** here first. Nothing is decided
by the leader alone.

    index    1      2         3        4         5
    term     1      1         2        2         2
    command  REG    CREATE    REG      ASSIGN    UNREG
             b1     orders    b2       orders    b1
                              ▲
                    commit_index — everything up to here is on a MAJORITY
                    of nodes and can never be lost. Beyond it is a proposal.

An entry is **committed** once a majority store it. Only committed entries are
applied to the state machine, and a committed entry is permanent: any future
leader is guaranteed to have it, because a candidate whose log is behind cannot
win an election.


TWO INVARIANTS THAT DO ALL THE WORK
-------------------------------------
Raft's Log Matching Property:

  1. If two logs contain an entry with the same INDEX and TERM, those entries
     hold the same command.
  2. If two logs contain an entry with the same index and term, the logs are
     IDENTICAL in every preceding entry.

The second is remarkable: matching one (index, term) pair proves agreement on
everything before it. That is what makes `AppendEntries` cheap — a leader sends
one `(prev_log_index, prev_log_term)` pair, and a follower that matches it
knows its whole history agrees.

`matches()` below is that check, and `truncate_from()` is what happens when it
fails.


INDEXING IS 1-BASED, DELIBERATELY
-----------------------------------
Raft's paper indexes from 1, and index 0 is a sentinel meaning "before the
beginning" with term 0. Keeping that convention makes the code checkable
against the paper line by line — worth more than the small awkwardness of a
list offset by one. Off-by-one errors in Raft are not cosmetic; they are
silent data loss.


THE SENTINEL MOVES; IT IS NEVER DELETED
-----------------------------------------
Once snapshotting exists, the log no longer starts at index 1. It starts just
after whatever the last snapshot folded in:

    snapshot @ index 7, term 2
                     │
    _entries    [ sentinel(7,t2) │ entry 8 │ entry 9 ]
    position          0              1         2

    log index  =  position + snapshot_last_index

So `_entries[i]` is no longer the entry at log index `i`, and every accessor
goes through `_pos()`. That one broken identity is the entire difficulty of
compaction — see `docs/phase-5/raft-snapshots.md`.

The sentinel's MEANING changed (from "before time began, term 0" to "the last
entry I folded into the snapshot") but its JOB did not: be the thing a
`prev_log_index` can legally point at when a follower is caught up from the
oldest entry we still hold. Delete it rather than relocating it and the first
`AppendEntries` after any compaction has no valid `prev_log_term` to send.


STORAGE IS BORROWED FROM PHASE 2
----------------------------------
Entries are persisted with `encode_record` / `iter_records` — the same
length-prefix + CRC32 framing built for partition data.

That reuse was not planned; it fits because the requirement is identical:
append records durably, and detect a torn tail after a crash. A half-written
Raft entry is truncated by exactly the code that truncates a half-written
message.
"""

import os

from broker.log.record import encode_record, iter_records


class LogEntry:
    """One command, stamped with the term in which the leader proposed it."""

    __slots__ = ('index', 'term', 'command')

    def __init__(self, index: int, term: int, command: dict):
        self.index = index
        self.term = term
        self.command = command

    def as_dict(self) -> dict:
        return {'index': self.index, 'term': self.term, 'command': self.command}

    @classmethod
    def from_dict(cls, raw: dict) -> 'LogEntry':
        return cls(int(raw['index']), int(raw['term']), raw['command'])

    def __repr__(self) -> str:
        kind = self.command.get('type', '?') if isinstance(self.command, dict) else '?'
        return f"LogEntry(i={self.index}, t={self.term}, {kind})"


class RaftLog:
    """
    The durable, ordered sequence of proposed commands.

    Held in memory and appended to disk. Small enough to keep resident: a
    metadata log records topic and membership changes, not message traffic, so
    it grows in events per hour rather than per second.

    Everything at or before `snapshot_last_index` has been folded into a
    snapshot and is no longer here. `first_index` is the oldest entry we can
    still serve.
    """

    FILENAME = 'raft-log'

    def __init__(self, directory: str, snapshot_last_index: int = 0,
                 snapshot_last_term: int = 0):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, self.FILENAME)

        # The snapshot point. Zero means "nothing compacted", which makes the
        # sentinel LogEntry(0, 0) — exactly the paper's index-0 sentinel, so
        # the uncompacted case needs no special handling anywhere.
        self.snapshot_last_index = int(snapshot_last_index)
        self.snapshot_last_term = int(snapshot_last_term)

        # Position 0 is always the sentinel: the entry immediately BEFORE the
        # oldest one we hold. Storing it makes `term_at(first_index - 1)` a
        # total function rather than a special case, which removes a whole
        # class of boundary bug from the RPC handlers.
        self._entries = [LogEntry(self.snapshot_last_index,
                                  self.snapshot_last_term,
                                  {'type': 'SENTINEL'})]

        self._fh = None
        self._load()

    # ── Loading ────────────────────────────────────────────────

    def _load(self) -> None:
        """
        Replay the log from disk, skipping anything the snapshot already holds.

        `iter_records` stops at the first torn or corrupt record, so a crash
        mid-append costs the partial entry and nothing before it. That entry
        was never acknowledged to a leader, so no majority can have counted it
        — losing it is safe by construction.

        The skip is not defensive padding; it handles a specific crash. A
        snapshot is fsynced BEFORE the log is truncated, so a crash between
        those two steps leaves a fresh snapshot beside a log that still
        contains the entries it covers. Reading them back in would place them
        after the sentinel at the wrong offsets and corrupt every index in the
        file. Skipping makes that crash window a no-op instead.
        """
        if os.path.exists(self.path):
            with open(self.path, 'rb') as fh:
                for _pos, _size, record in iter_records(fh):
                    entry = LogEntry.from_dict(record)
                    if entry.index <= self.snapshot_last_index:
                        continue                # already in the snapshot
                    self._entries.append(entry)

        self._fh = open(self.path, 'ab')

    # ── Reading ────────────────────────────────────────────────

    @property
    def last_index(self) -> int:
        return self._entries[-1].index

    @property
    def last_term(self) -> int:
        return self._entries[-1].term

    @property
    def first_index(self) -> int:
        """The oldest index we can still serve. Everything before it is in the snapshot."""
        return self.snapshot_last_index + 1

    def _pos(self, index: int) -> int:
        """Log index → list position. The one translation compaction introduces."""
        return index - self.snapshot_last_index

    def term_at(self, index: int) -> int:
        """
        Term of the entry at `index`, or 0 if we do not have it.

        "Do not have it" now covers two different situations that must not be
        conflated by the caller: an index in the future (nobody has it) and an
        index inside the snapshot (it exists, we discarded our copy). Both
        return 0 here; `matches` is where the distinction is drawn.
        """
        position = self._pos(index)
        if 0 <= position < len(self._entries):
            return self._entries[position].term
        return 0

    def entry_at(self, index: int):
        position = self._pos(index)
        if 0 <= position < len(self._entries):
            return self._entries[position]
        return None

    def entries_from(self, index: int) -> list:
        """Every entry at or after `index`, for replication."""
        if index < self.first_index:
            index = self.first_index
        return self._entries[self._pos(index):]

    def matches(self, index: int, term: int) -> bool:
        """
        Do we have an entry at `index` with exactly `term`?

        The Log Matching Property in one line. A follower answering True here
        is asserting that its entire history up to `index` is identical to the
        leader's — which is why `AppendEntries` need only carry one such pair
        rather than a checksum of everything.

        `index == snapshot_last_index` is answered from the sentinel, which is
        what lets a leader resume replication across a compaction point.
        Anything OLDER than that we genuinely cannot verify, and we say so
        rather than guessing: an unverifiable claim answered True is how a
        divergent log gets silently accepted.
        """
        if index > self.last_index:
            return False
        if index < self.snapshot_last_index:
            return False                        # discarded; cannot verify
        return self.term_at(index) == term

    def is_up_to_date(self, other_last_index: int, other_last_term: int) -> bool:
        """
        Is a candidate's log at least as up to date as ours?

        **THE ELECTION RESTRICTION.** This is what makes committed entries
        permanent: a candidate missing committed data cannot win, so every new
        leader already holds every committed entry.

        Compare the last TERM first, and only then length. A longer log is not
        automatically better — a node can accumulate many uncommitted entries
        in a term that never committed, while a shorter log from a later term
        holds the entries a majority actually agreed on.

        Delete this method and Raft still elects leaders and still passes every
        happy-path test, while quietly losing committed data. It is the most
        expensive four lines in the file.
        """
        if other_last_term != self.last_term:
            return other_last_term > self.last_term
        return other_last_index >= self.last_index

    # ── Writing ────────────────────────────────────────────────

    def append(self, term: int, command: dict) -> LogEntry:
        """Append one command (leader only). Returns the new entry."""
        entry = LogEntry(self.last_index + 1, term, command)
        self._entries.append(entry)
        self._persist(entry)
        return entry

    def append_entries(self, entries: list) -> None:
        """
        Append entries received from a leader, truncating on conflict.

        Rule 3 of AppendEntries: *if an existing entry conflicts with a new one
        (same index, different term), delete it and everything after it.*

        The conflict check is per entry rather than blanket-truncating from
        `prev_log_index`, and that matters: a retransmitted `AppendEntries`
        commonly repeats entries we already hold. Truncating on a duplicate
        would delete entries the leader believes are safely stored, and can
        remove a *committed* one — turning a redundant network retry into data
        loss.
        """
        for entry in entries:
            if entry.index <= self.snapshot_last_index:
                # Already folded into our snapshot, so we hold its effect and
                # it is committed — which makes it unconditionally identical to
                # what the leader is sending. Skipping is not an optimisation:
                # without it, a routine retransmission of an old entry would
                # reach `truncate_from` and raise, turning a harmless network
                # retry into a crash.
                continue

            existing = self.entry_at(entry.index)

            if existing is not None:
                if existing.term == entry.term:
                    continue                    # already have it, identical
                self.truncate_from(entry.index)  # genuine conflict

            self._entries.append(entry)
            self._persist(entry)

    def truncate_from(self, index: int) -> int:
        """
        Delete the entry at `index` and everything after it. Returns how many.

        Only ever called on a follower whose log diverged from the leader's.
        Those entries were never committed — a committed entry is on a majority
        and is held by every possible future leader — so discarding them is
        safe, and is how Raft converges divergent logs.

        Which is exactly why cutting into the SNAPSHOT is not safe and raises.
        Everything in the snapshot was applied, and applied only ever happens
        to committed entries. A request to roll one back means a committed
        entry is being contradicted, which Raft guarantees cannot happen — so
        it is a bug in us, and the useful response to it is a loud crash rather
        than a quiet obedience that diverges the cluster.
        """
        if index <= self.snapshot_last_index:
            raise ValueError(
                f"refusing to truncate from index {index}: at or before the "
                f"snapshot point {self.snapshot_last_index}, so those entries "
                f"are committed and cannot be rolled back")

        if index > self.last_index:
            return 0

        position = self._pos(index)
        removed = len(self._entries) - position
        self._entries = self._entries[:position]
        self._rewrite()
        return removed

    # ── Compaction ─────────────────────────────────────────────

    def compact_to(self, index: int) -> int:
        """
        Discard every entry at or before `index`. Returns how many were dropped.

        The caller must have persisted a snapshot covering `index` FIRST — see
        `SnapshotStore.save`. This method is the destructive half of that pair
        and has no way to check the other half happened, so the ordering lives
        in `RaftNode.maybe_snapshot`, which owns both.

        Note there is no fsync here beyond the one `_rewrite` already does, and
        no need for one: losing this truncation on a crash costs disk space and
        nothing else. The snapshot is already durable, `_load` skips whatever
        the snapshot covers, and the node comes up in exactly the same state.
        A compaction that half-happens is harmless — which is the point of
        doing it in this order.
        """
        if index <= self.snapshot_last_index:
            return 0                            # already compacted past here
        if index > self.last_index:
            raise ValueError(
                f"cannot compact to index {index}: log ends at {self.last_index}")

        # Read the term BEFORE moving the snapshot point — `term_at` translates
        # through the offset we are about to change.
        term = self.term_at(index)
        kept = self._entries[self._pos(index) + 1:]
        removed = len(self._entries) - 1 - len(kept)

        self.snapshot_last_index = index
        self.snapshot_last_term = term
        self._entries = [LogEntry(index, term, {'type': 'SENTINEL'})] + kept

        self._rewrite()
        return removed

    def reset_to_snapshot(self, index: int, term: int) -> int:
        """
        Discard the ENTIRE log and restart indexing at `index`. Returns how
        many entries were dropped.

        The wholesale version of `compact_to`, used only by
        `handle_install_snapshot` — a follower so far behind that catching it
        up entry by entry is impossible, because the leader no longer holds the
        entries it would need to send.

        Two differences from `compact_to`, both deliberate:

        - It is allowed to move the snapshot point BACKWARDS, and to discard
          entries this node still held. `compact_to` refuses both. The
          difference is the caller: `handle_install_snapshot` has just replaced
          the state machine wholesale, so the log must be made to agree with it
          rather than the other way round. (It guards against an OLD snapshot
          before calling — moving backwards is legal here, but only when the
          caller has already established it is the right thing to do.)

        - It keeps nothing. The paper's optimisation retains the tail when the
          follower already holds a matching entry at `(index, term)`; we
          re-replicate those few entries instead. One path is one thing to get
          right, and this path only runs when a node is badly behind already.
        """
        removed = len(self._entries) - 1

        self.snapshot_last_index = int(index)
        self.snapshot_last_term = int(term)
        self._entries = [LogEntry(self.snapshot_last_index,
                                  self.snapshot_last_term,
                                  {'type': 'SENTINEL'})]

        self._rewrite()
        return removed

    # ── Persistence ────────────────────────────────────────────

    def _persist(self, entry: LogEntry) -> None:
        """
        Append one entry and fsync.

        fsync here, unlike partition data. Kafka can flush partition writes to
        the page cache and rely on replicas for durability — but this log IS
        the replication mechanism, so it has nothing behind it. A node that
        acknowledges an entry and loses it on a power cut breaks the promise a
        majority was counted on.

        The cost is real (~ms per entry) and affordable: metadata changes are
        rare. That asymmetry is the whole reason the two logs are tuned
        differently.
        """
        self._fh.write(encode_record(entry.as_dict()))
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def _rewrite(self) -> None:
        """
        Rewrite the whole file after a truncation or a compaction.

        Rare — divergence or a snapshot — and simplest to do wholesale via
        temp-and-rename, so a crash mid-rewrite leaves the previous log intact
        rather than a half-truncated one.
        """
        if self._fh is not None:
            self._fh.close()

        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'wb') as fh:
            for entry in self._entries[1:]:      # skip the sentinel
                fh.write(encode_record(entry.as_dict()))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.path)

        self._fh = open(self.path, 'ab')

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __len__(self) -> int:
        return self.last_index

    def __repr__(self) -> str:
        compacted = (f", snapshot@{self.snapshot_last_index}"
                     if self.snapshot_last_index else '')
        return (f"RaftLog(last={self.last_index}@term{self.last_term}, "
                f"{len(self._entries) - 1} entries held{compacted})")
