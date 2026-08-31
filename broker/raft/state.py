"""
CONCEPT: Raft's Persistent State — Two Fields That Cannot Be Lost
-------------------------------------------------------------------
Raft requires three things to survive a restart:

    currentTerm     the latest term this node has seen
    votedFor        who it voted for in that term (or nobody)
    log[]           the replicated log itself  (see log.py)

This file owns the first two. They are small — a couple of integers — and
losing either turns Raft's safety proof into a coin flip.


WHY currentTerm MUST BE DURABLE
---------------------------------
A term is Raft's logical clock, and the rule that makes it work is:

    a message carrying a higher term than mine makes me a follower

If a node restarts and forgets it had reached term 7, it comes back at term 0.
A stale leader still on term 4 now looks *newer* than us, and we accept it —
along with any log it wants to overwrite. The node has effectively travelled
backwards in time, and nothing else in the protocol can detect that.


WHY votedFor MUST BE DURABLE — the sharpest one
-------------------------------------------------
Raft guarantees at most one leader per term because each node votes at most
once per term. That is the ENTIRE argument. It rests on this one field.

    node 3 votes for candidate 1 in term 5
    node 3 crashes and restarts, forgetting the vote
    node 3 votes for candidate 2 in term 5

With five nodes, candidate 1 can now reach three votes and candidate 2 can
reach three votes — **two leaders in the same term**, which is precisely the
split-brain that quorums exist to make impossible.

One `fsync` of a two-field file, on a path taken a handful of times a second at
most, is what stands between "provably safe" and "usually fine".


WHY WRITE BEFORE REPLYING
---------------------------
Persist first, then send the vote. Reply first and crash before the write, and
you have granted a vote nobody remembers giving — the same double-vote, with an
extra step. "Durable before observable" is the ordering rule, and it is the
same one the consumer's offset commit follows in Phase 0.
"""

import json
import os


class PersistentState:
    """
    `currentTerm` and `votedFor`, written durably on every change.

    Stored as one small JSON file, replaced atomically. Not append-only: there
    is exactly one current value of each field and no history worth keeping —
    which is the opposite of the log next door, and the reason the two are
    stored differently.
    """

    FILENAME = 'raft-state.json'

    def __init__(self, directory: str):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, self.FILENAME)

        self.current_term = 0
        self.voted_for = None

        self._load()

    # ── Loading ────────────────────────────────────────────────

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding='utf-8') as fh:
                raw = json.load(fh)
            self.current_term = int(raw.get('current_term', 0))
            voted = raw.get('voted_for')
            self.voted_for = int(voted) if voted is not None else None
        except (OSError, ValueError, TypeError):
            # An unreadable state file is NOT recoverable by guessing. Starting
            # at term 0 would let a stale leader win, which is the exact
            # failure this file exists to prevent.
            #
            # Refusing to start is the safe answer: a node that cannot prove
            # what it has already promised must not make new promises.
            raise RuntimeError(
                f"Raft state at {self.path} is unreadable. This node cannot "
                f"safely rejoin the quorum: it cannot prove which term it has "
                f"reached or whom it has already voted for. Restore the file "
                f"or wipe this node's data directory to rejoin as a fresh "
                f"member.")

    # ── Mutation, always durable ───────────────────────────────

    def save(self) -> None:
        """
        Write both fields and fsync. Called before any message that depends
        on them.

        Same temp-write-fsync-rename as the consumer offset file and the topic
        metadata: opening the real file with 'w' truncates it to zero bytes
        first, and a crash in that window leaves an EMPTY state file — which
        would read back as term 0 and put this node right back into the failure
        described in the module docstring.
        """
        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as fh:
            json.dump({'current_term': self.current_term,
                       'voted_for': self.voted_for}, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.path)

    def set_term(self, term: int, voted_for=None) -> None:
        """
        Move to a new term. Clears the vote unless one is supplied.

        Clearing is required, not tidiness: `votedFor` is scoped to a term, and
        carrying an old term's vote forward would refuse a legitimate candidate
        in the new one — a node that silently never votes again.
        """
        self.current_term = term
        self.voted_for = voted_for
        self.save()

    def record_vote(self, candidate_id: int) -> None:
        """Record a granted vote. Must be durable BEFORE the reply is sent."""
        self.voted_for = candidate_id
        self.save()

    def can_vote_for(self, candidate_id: int) -> bool:
        """
        Have we already voted for someone else this term?

        Voting for the SAME candidate again is allowed and necessary: a
        RequestVote can be retransmitted after a dropped reply, and refusing
        the retry would stall an election that should have succeeded.
        """
        return self.voted_for is None or self.voted_for == candidate_id

    def __repr__(self) -> str:
        return (f"PersistentState(term={self.current_term}, "
                f"voted_for={self.voted_for})")
