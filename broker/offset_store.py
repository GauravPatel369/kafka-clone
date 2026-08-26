"""
CONCEPT: `__consumer_offsets` — Committed Positions, Stored as Messages
--------------------------------------------------------------------------
Through Phase 3 a consumer's position lived in `./offsets/orders-0-mygroup.txt`
on the consumer's own machine. That is a category error: the position belongs
to the GROUP, not to whichever container happened to be running a member of it.
Move the consumer and it starts from zero.

Kafka's answer is elegant enough to be worth stating plainly:

    Kafka needed a durable, ordered, replicated key-value store for offsets.
    It already had one. So offsets are just messages in a topic.

`__consumer_offsets` is a real topic, written through the same `Log` as
everything else. It gets durability, ordering, crash recovery, and (from Phase
6) replication for free, because it is not special.

    key   = "group|topic|partition"
    value = the committed offset

The coordinator keeps the current value of every key in memory — that is what
`OFFSET_FETCH` reads — and rebuilds that map by replaying the log at startup.
The log is the durable truth; the map is a derived accelerator, exactly like
the sparse index in Phase 2.


WHY REPLAY WORKS
------------------
The log is append-only, so a key committed five times appears five times. Replay
from the beginning applying every record in order, and the last one wins. Order
is guaranteed because it is one partition — which is the entire reason this
works, and why `__consumer_offsets` uses a single partition here.


THE GAP WE ARE NOT CLOSING
----------------------------
Real Kafka **compacts** this topic: a background cleaner keeps only the latest
record per key, so the log stays proportional to the number of live keys rather
than to the number of commits ever made.

We did not implement compaction in Phase 2, so this log grows forever. A
consumer committing once a second adds ~86,000 records a day, and every one of
them is replayed at startup.

That is a real limitation, stated rather than hidden. Everything else here is
faithful; this one thing is not, and `kfk segments --topic __consumer_offsets`
will show you the cost.
"""

import threading

from broker.log import LogConfig
from broker.partition import Partition

# The leading underscore marks it internal, exactly as real Kafka does. It is a
# normal topic in every other respect — which is the point.
OFFSETS_TOPIC = '__consumer_offsets'

# One partition, deliberately. Ordering is what makes "replay and let the last
# record win" correct, and ordering only exists WITHIN a partition. Real Kafka
# uses 50 partitions and hashes the group id to one of them, so different
# groups spread across the cluster while each group's own commits stay ordered.
OFFSETS_PARTITIONS = 1


def offset_key(group_id: str, topic: str, partition: int) -> str:
    """
    The compaction key for one committed position.

    A single string rather than a tuple because it has to survive JSON, and
    because compaction (if we had it) matches on exact key equality. The pipe
    is safe: topic names are restricted to letters, digits, `.`, `_` and `-`
    by `broker/metadata.py`, so it cannot appear inside one and split the key.
    """
    return f"{group_id}|{topic}|{partition}"


class OffsetStore:
    """
    Committed offsets: durable in a log, current values in memory.

    Thread-safe. The broker's executor threads write here while the
    coordinator reads.
    """

    def __init__(self, data_dir: str, log_config: LogConfig = None):
        self._lock = threading.Lock()

        # (group, topic, partition) -> offset. The map the coordinator serves
        # reads from; the log below is what makes it survive a restart.
        self._offsets = {}

        self._partition = Partition(OFFSETS_TOPIC, 0, data_dir, log_config)
        self._replay()

    # ── Startup ────────────────────────────────────────────────

    def _replay(self) -> None:
        """
        Rebuild the in-memory map by reading the log from the beginning.

        Last record per key wins. That is correct only because this is ONE
        partition and therefore strictly ordered — with two partitions, two
        commits for the same key could be replayed in either order and the
        loser might win.

        This is also where the missing compaction hurts: every commit ever
        made is read at startup, not just the live ones.
        """
        batch_size = 1000
        offset = 0

        while True:
            records = self._partition.fetch(offset, batch_size)
            if not records:
                break

            for record in records:
                key = record.get('key')
                if not key:
                    continue
                try:
                    group_id, topic, partition = key.split('|')
                    self._offsets[(group_id, topic, int(partition))] = int(record['value'])
                except (ValueError, KeyError):
                    # A malformed record is skipped rather than fatal. Failing
                    # to start because one offset record is bad would take the
                    # whole broker down over recoverable state — every
                    # consumer would simply re-commit within seconds.
                    continue

            offset = records[-1]['offset'] + 1

    # ── Commit and fetch ───────────────────────────────────────

    def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        """
        Durably record a group's position.

        Order matters: append to the log FIRST, then update the map. If the
        broker dies between them, replay recovers the committed value — the
        map is derived, so a lost update to it costs nothing.

        Doing it the other way round would let the map report a position the
        log never recorded, and a restart would silently rewind the group.
        """
        with self._lock:
            self._partition.append(str(offset),
                                   key=offset_key(group_id, topic, partition))
            self._offsets[(group_id, topic, partition)] = offset

    def fetch(self, group_id: str, topic: str, partition: int) -> int:
        """
        Return the committed offset, or -1 if this group has never committed.

        -1 rather than 0 because they mean different things: "never committed"
        is a consumer that must apply its `auto.offset.reset` policy, while 0
        is a consumer that has genuinely committed the beginning of the log.
        Collapsing the two would make a brand-new group silently replay
        everything — or silently skip it — depending on which default you
        happened to pick.
        """
        with self._lock:
            return self._offsets.get((group_id, topic, partition), -1)

    def fetch_group(self, group_id: str) -> dict:
        """Every committed position for one group: {(topic, partition): offset}."""
        with self._lock:
            return {(topic, partition): offset
                    for (group, topic, partition), offset in self._offsets.items()
                    if group == group_id}

    def groups(self) -> list:
        """Every group that has ever committed. Used by `kfk groups list`."""
        with self._lock:
            return sorted({group for group, _t, _p in self._offsets})

    def close(self) -> None:
        with self._lock:
            self._partition.close()

    def __repr__(self) -> str:
        return f"OffsetStore({len(self._offsets)} committed positions)"
