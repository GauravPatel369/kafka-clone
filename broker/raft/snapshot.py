"""
CONCEPT: The Snapshot — The Answer, Without the Journal
---------------------------------------------------------
A Raft log is a journal of decisions. A state machine is the *result* of
applying them. Once the result is durable, the journal ahead of it is
archaeology:

    REGISTER b1  +  UNREGISTER b1  =  "b1 is not here"

Two fsynced, majority-replicated entries whose combined meaning is identical to
what an empty dict says for free. Keeping them costs disk forever and costs
restart time in proportion to the node's whole uptime.

So each node periodically writes a snapshot — the state machine, plus the index
and term of the last entry folded into it — and throws away everything at or
before that index.


THE INDEX AND TERM ARE NOT DECORATION
---------------------------------------
    {'last_index': 7, 'last_term': 2, 'state': {...}}

`last_index` says where the log now starts. `last_term` is what a leader must
send as `prev_log_term` when catching up a follower from the very first entry
we still hold — see the relocated sentinel in `RaftLog`. Lose either and
replication cannot resume across the compaction point.


SNAPSHOTTING NEEDS NO CONSENSUS
---------------------------------
This is worth stating plainly because it looks like it should. Every node has
the same committed prefix — that is the guarantee Raft provides — so every node
can compact its own copy whenever it likes, with no RPC and no agreement. Two
nodes holding snapshots at different indices are perfectly consistent with each
other; they have simply chosen different amounts of history to keep.


ORDERING IS THE WHOLE SAFETY ARGUMENT
---------------------------------------
    1. write the snapshot, and FSYNC it
    2. only then truncate the log

Reverse those two and a crash in between destroys both copies of the same
entries. The general rule for replacing A with B on disk: **B must be durable
before A is destroyed.** `save()` returns only once step 1 is complete, so the
caller physically cannot get it wrong.
"""

import os

from broker.log.record import encode_record, iter_records


class SnapshotStore:
    """
    The newest snapshot, in one file, replaced atomically.

    One file rather than a numbered series: a snapshot is a complete
    replacement, so there is nothing an older one can tell us that the newer
    does not. Real systems keep a couple for forensics; we keep the answer.
    """

    FILENAME = 'raft-snapshot'

    def __init__(self, directory: str):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, self.FILENAME)

    def save(self, last_index: int, last_term: int, state: dict) -> None:
        """
        Persist a snapshot durably. Returns only once it is on the platter.

        Temp-and-rename, like `PersistentState` and `RaftLog._rewrite`: a crash
        mid-write leaves the previous snapshot intact rather than a truncated
        one. `os.replace` is atomic on both POSIX and Windows, which is the
        property the whole pattern rests on.
        """
        record = {'last_index': int(last_index),
                  'last_term': int(last_term),
                  'state': state}

        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'wb') as fh:
            fh.write(encode_record(record))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.path)

    def load(self):
        """
        Read the snapshot, or None if there is not a usable one.

        A corrupt snapshot is treated as *absent*, not fatal. `iter_records`
        stops at the first bad record, so a torn file yields nothing and the
        node falls back to replaying its log from the beginning — slower, and
        completely correct. The log is the authority; the snapshot is only an
        optimisation, and an optimisation that fails should cost speed rather
        than availability.
        """
        if not os.path.exists(self.path):
            return None

        with open(self.path, 'rb') as fh:
            for _pos, _size, record in iter_records(fh):
                if 'last_index' in record and 'state' in record:
                    return {'last_index': int(record['last_index']),
                            'last_term': int(record.get('last_term', 0)),
                            'state': record['state']}
        return None

    def __repr__(self) -> str:
        snapshot = self.load()
        if snapshot is None:
            return 'SnapshotStore(empty)'
        return (f"SnapshotStore(@{snapshot['last_index']}"
                f"/term{snapshot['last_term']})")
