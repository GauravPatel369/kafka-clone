"""
Tests for Raft snapshotting — Step A: compaction and restart.

Two layers, deliberately:

  1. **Unit tests on `RaftLog`**, which is where compaction is genuinely hard.
     Dropping the log prefix breaks the identity `_entries[i] is the entry at
     index i` that every accessor in that file rested on, and a broken
     translation there is silent data loss rather than a crash.

  2. **Cluster tests with the threshold set to 4**, so real elections and real
     replication run across real compactions.

The second layer is not optional padding. The production threshold is 1000
against a log that reaches double digits, so with default settings this feature
would never execute once — and this project has already shipped a
cluster-fatal bug that every test agreed with, because the test harness
modelled the failure wrongly. Code that is never run is not tested; it is
merely present.
"""

import asyncio

import pytest

from broker.raft import LogEntry, MetadataStateMachine, RaftLog
from broker.raft.snapshot import SnapshotStore

from test_raft import LEADER, async_test, build_cluster, leaders, wait_for


# ── The snapshot file ──────────────────────────────────────────

def test_a_snapshot_round_trips(tmp_path):
    store = SnapshotStore(str(tmp_path))
    assert store.load() is None

    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC',
                                  'name': 'orders', 'partitions': 4}))
    machine.apply(LogEntry(2, 1, {'type': 'REGISTER_BROKER', 'node_id': 7,
                                  'host': 'h', 'port': 9092,
                                  'registered_at': 1234}))

    store.save(2, 1, machine.snapshot())
    loaded = store.load()

    assert loaded['last_index'] == 2
    assert loaded['last_term'] == 1

    restored = MetadataStateMachine()
    restored.restore(loaded['state'])
    assert restored.snapshot() == machine.snapshot()


def test_restore_replaces_rather_than_merges(tmp_path):
    """
    A snapshot is the WHOLE truth as of its index, not an update to apply.

    A node restoring one had better forget what it thought it knew: anything
    the snapshot does not mention was either never real or has since been
    removed by an entry a majority agreed to. Merging would resurrect it, and
    nothing downstream would ever notice.
    """
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'REGISTER_BROKER', 'node_id': 1,
                                  'host': 'a', 'port': 1}))
    machine.apply(LogEntry(2, 1, {'type': 'CREATE_TOPIC',
                                  'name': 'stale', 'partitions': 2}))

    machine.restore({'brokers': [], 'topics': {'fresh': 1},
                     'partitions': [], 'last_applied': 9})

    assert machine.brokers == {}
    assert 'stale' not in machine.topics
    assert machine.topics == {'fresh': 1}
    assert machine.last_applied == 9


def test_a_corrupt_snapshot_is_treated_as_absent(tmp_path):
    """
    The log is the authority; the snapshot is an optimisation.

    So a snapshot that fails its CRC costs startup time — replay everything —
    and never costs availability. Refusing to start would turn a recoverable
    disk problem into an outage, which is the wrong trade for a cache.
    """
    store = SnapshotStore(str(tmp_path))
    store.save(5, 2, {'brokers': [], 'topics': {}, 'partitions': [],
                      'last_applied': 5})

    with open(store.path, 'r+b') as fh:
        fh.seek(12)
        fh.write(b'\xff\xff\xff\xff')

    assert store.load() is None


# ── Offset indexing in the log ─────────────────────────────────

def _fill(log_, count, term=1):
    for i in range(count):
        log_.append(term, {'type': 'CREATE_TOPIC', 'name': f't{i}',
                           'partitions': 1})


def test_compaction_moves_the_sentinel_rather_than_deleting_it(tmp_path):
    """
    The single most important invariant compaction has to preserve.

    After compacting to 5, `term_at(5)` must still answer — from the relocated
    sentinel — because that is the `prev_log_term` a leader sends when catching
    a follower up from the oldest entry we still hold. Delete the sentinel and
    the first AppendEntries after any compaction is unbuildable.
    """
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 8, term=3)

    assert log_.compact_to(5) == 5
    assert log_.first_index == 6
    assert log_.last_index == 8

    assert log_.term_at(5) == 3                 # the sentinel still answers
    assert log_.matches(5, 3) is True
    assert log_.matches(5, 2) is False


def test_every_accessor_translates_through_the_offset(tmp_path):
    """
    The identity `_entries[i] is entry i` dies at the first compaction. This is
    the test that each accessor learned that, rather than five of six.
    """
    log_ = RaftLog(str(tmp_path))
    for i in range(1, 9):
        log_.append(2, {'type': 'MARKER', 'n': i})
    log_.compact_to(4)

    assert log_.entry_at(6).command['n'] == 6           # not _entries[6]
    assert log_.term_at(7) == 2
    assert [e.index for e in log_.entries_from(5)] == [5, 6, 7, 8]
    assert [e.index for e in log_.entries_from(1)] == [5, 6, 7, 8]   # floored

    assert log_.entry_at(3) is None                     # inside the snapshot
    assert log_.entry_at(99) is None
    assert log_.matches(3, 2) is False                  # cannot verify


def test_truncating_into_the_snapshot_raises(tmp_path):
    """
    Divergence only ever happens in the UNCOMMITTED tail. A request to roll
    back a snapshotted entry means something committed is being contradicted,
    which Raft guarantees cannot happen — so it is a bug in us, and a loud
    crash beats a quiet divergence.
    """
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 6)
    log_.compact_to(4)

    assert log_.truncate_from(6) == 1            # in the tail: fine
    with pytest.raises(ValueError):
        log_.truncate_from(4)
    with pytest.raises(ValueError):
        log_.truncate_from(2)


def test_replayed_entries_below_the_snapshot_are_skipped(tmp_path):
    """
    A leader retransmitting old entries must not be able to crash a follower.

    `append_entries` skips anything at or before the snapshot point. Without
    that skip the entry would reach `truncate_from` — or worse, be appended at
    a negative list position — turning a routine network retry into corruption.
    """
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 6, term=1)
    log_.compact_to(4)

    log_.append_entries([LogEntry(2, 1, {'type': 'MARKER'}),
                         LogEntry(3, 1, {'type': 'MARKER'}),
                         LogEntry(7, 1, {'type': 'MARKER'})])

    assert log_.first_index == 5
    assert log_.last_index == 7
    assert [e.index for e in log_.entries_from(5)] == [5, 6, 7]


def test_a_compacted_log_reloads_from_disk_correctly(tmp_path):
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 10, term=4)
    log_.compact_to(6)
    log_.close()

    reopened = RaftLog(str(tmp_path), snapshot_last_index=6, snapshot_last_term=4)
    assert reopened.first_index == 7
    assert reopened.last_index == 10
    assert reopened.term_at(6) == 4
    assert [e.index for e in reopened.entries_from(7)] == [7, 8, 9, 10]


def test_a_crash_between_snapshot_and_truncation_is_harmless(tmp_path):
    """
    The ordering rule, tested directly.

    `save()` fsyncs the snapshot BEFORE `compact_to` touches the log, so the
    crash window leaves a fresh snapshot beside a log that still holds the
    entries it covers. Reloading must skip them — reading them back in would
    place them after the sentinel at the wrong offsets and corrupt every index
    in the file.
    """
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 8, term=2)
    log_.close()                        # log on disk still has entries 1..8

    # ...and the snapshot for index 5 was already durable when we died.
    reopened = RaftLog(str(tmp_path), snapshot_last_index=5, snapshot_last_term=2)

    assert reopened.first_index == 6
    assert reopened.last_index == 8
    assert [e.index for e in reopened.entries_from(6)] == [6, 7, 8]


# ── The node, under real elections ─────────────────────────────

@async_test
async def test_a_node_snapshots_and_keeps_committing(tmp_path):
    """Compaction must be invisible to the thing it is compacting under."""
    network, nodes = await build_cluster(tmp_path, 3, snapshot_threshold=4)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]

        for i in range(12):
            assert await leader.propose({'type': 'CREATE_TOPIC',
                                         'name': f'topic-{i}', 'partitions': 1})

        assert leader.log.snapshot_last_index >= 4, "no snapshot was ever taken"
        assert leader.log.first_index > 1, "the log prefix was never dropped"
        assert leader.machine.partition_count('topic-11') == 1

        # And the followers compacted too, off their own applied prefix, with
        # no coordination of any kind.
        assert await wait_for(
            lambda: all(n.log.snapshot_last_index >= 4 for n in nodes))
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_state_survives_a_restart_from_the_snapshot(tmp_path):
    """
    The payoff: recovery is snapshot + tail, not a replay of all history.

    The restarted node is rebuilt from disk alone — new object, same directory
    — and must come up holding metadata whose log entries no longer exist
    anywhere on it.
    """
    network, nodes = await build_cluster(tmp_path, 1, snapshot_threshold=3)
    try:
        node = nodes[0]
        assert await wait_for(lambda: node.role == LEADER)

        for i in range(9):
            assert await node.propose({'type': 'CREATE_TOPIC',
                                       'name': f'topic-{i}', 'partitions': 2})

        assert node.log.snapshot_last_index >= 3
        committed = node.commit_index
    finally:
        for n in nodes:
            await n.stop()

    _network2, nodes2 = await build_cluster(tmp_path, 1, snapshot_threshold=3)
    try:
        restarted = nodes2[0]

        assert restarted.last_applied == committed
        assert restarted.commit_index == committed
        assert restarted.machine.partition_count('topic-0') == 2, \
            "an entry inside the snapshot was lost across the restart"
        assert restarted.machine.partition_count('topic-8') == 2
        assert restarted.log.first_index > 1, \
            "the log was replayed from the beginning, not from the snapshot"
    finally:
        for n in nodes2:
            await n.stop()


@async_test
async def test_a_follower_catches_up_across_a_compaction_point(tmp_path):
    """
    The relocated sentinel, exercised end to end.

    A follower kept out of the loop, then healed, must be caught up by a leader
    whose log now STARTS after a compaction. Every AppendEntries the leader
    sends it will carry a `prev_log_index` at or after the snapshot point —
    which only works because the sentinel still answers for it.

    Setting this up takes care, and getting it wrong the first time was
    itself instructive. Partition the follower from the start and it needs
    index 1, which the leader has long since discarded — so the test lands in
    Step B's territory and could not pass until `InstallSnapshot` existed (it
    is now `test_a_follower_beyond_the_snapshot_is_sent_one`, below). The Step
    A case is narrower:
    the follower must be behind the leader's LAST index but at or after its
    SNAPSHOT index. So it stays connected across two compactions, then misses
    only enough entries to stay short of triggering a third.
    """
    network, nodes = await build_cluster(tmp_path, 3, snapshot_threshold=5)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]
        follower = next(n for n in nodes if n is not leader)

        for i in range(10):
            assert await leader.propose({'type': 'CREATE_TOPIC',
                                         'name': f'early-{i}', 'partitions': 1})

        assert await wait_for(
            lambda: follower.commit_index >= leader.commit_index)
        compaction_point = leader.log.snapshot_last_index
        assert compaction_point >= 5, "the leader never compacted"

        network.partition({follower.node_id},
                          {n.node_id for n in nodes if n is not follower})

        # Fewer than the threshold, so no THIRD compaction runs past the
        # follower while it is away.
        for i in range(3):
            assert await leader.propose({'type': 'CREATE_TOPIC',
                                         'name': f'late-{i}', 'partitions': 1})

        assert leader.log.snapshot_last_index == compaction_point
        assert follower.commit_index < leader.commit_index

        network.heal()

        assert await wait_for(
            lambda: follower.commit_index >= leader.commit_index, timeout=10.0)
        assert follower.machine.partition_count('late-2') == 1
        assert follower.machine.partition_count('early-0') == 1
    finally:
        for node in nodes:
            await node.stop()


# ── Step B: InstallSnapshot ────────────────────────────────────

@async_test
async def test_a_follower_beyond_the_snapshot_is_sent_one(tmp_path):
    """
    The case Step A could not handle, now handled.

    The follower is cut off before it replicates anything, so it needs index 1
    — which the leader compacts away while it is gone. No amount of backing up
    `next_index` can help: the entries do not exist any more. Only a snapshot
    can move it forward.
    """
    network, nodes = await build_cluster(tmp_path, 3, snapshot_threshold=4)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]
        follower = next(n for n in nodes if n is not leader)

        network.partition({follower.node_id},
                          {n.node_id for n in nodes if n is not follower})

        for i in range(10):
            assert await leader.propose({'type': 'CREATE_TOPIC',
                                         'name': f'topic-{i}', 'partitions': 3})

        assert leader.log.first_index > 1, "the leader never compacted"
        assert follower.log.last_index == 0, "the follower replicated anyway"

        network.heal()

        assert await wait_for(
            lambda: follower.commit_index >= leader.commit_index, timeout=10.0)

        # It holds metadata whose log entries exist nowhere in the cluster.
        assert follower.machine.partition_count('topic-0') == 3
        assert follower.machine.partition_count('topic-9') == 3
        assert follower.log.snapshot_last_index >= leader.log.snapshot_last_index
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_stale_leaders_snapshot_is_refused(tmp_path):
    """
    Guard 1. The only place a node discards committed state on a peer's word,
    so the term check is a fence around a destructive operation rather than
    routine validation.
    """
    network, nodes = await build_cluster(tmp_path, 3, snapshot_threshold=4)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        node = next(n for n in nodes if n.role != LEADER)
        before = node.commit_index

        response = await node.handle_install_snapshot({
            'term': node.current_term - 1,          # a leader from the past
            'leader_id': 99,
            'last_index': 500,
            'last_term': 1,
            'state': {'brokers': [], 'topics': {'ghost': 1},
                      'partitions': [], 'last_applied': 500},
        })

        assert response['success'] is False
        assert node.commit_index == before
        assert 'ghost' not in node.machine.topics
    finally:
        for n in nodes:
            await n.stop()


@async_test
async def test_an_old_snapshot_cannot_rewind_a_follower(tmp_path):
    """
    Guard 2, and the subtler of the two.

    A snapshot older than what we already hold arrives legitimately — a
    retransmission, or a slow message overtaken by the AppendEntries behind
    it. Applying it would roll back committed state. Success is the right
    answer: we hold everything it contains and more.
    """
    network, nodes = await build_cluster(tmp_path, 1, snapshot_threshold=3)
    try:
        node = nodes[0]
        assert await wait_for(lambda: node.role == LEADER)

        for i in range(8):
            assert await node.propose({'type': 'CREATE_TOPIC',
                                       'name': f'topic-{i}', 'partitions': 1})

        committed = node.commit_index
        assert committed >= 8

        response = await node.handle_install_snapshot({
            'term': node.current_term,
            'leader_id': node.node_id,
            'last_index': 2,                        # far behind where we are
            'last_term': 1,
            'state': {'brokers': [], 'topics': {'topic-0': 1},
                      'partitions': [], 'last_applied': 2},
        })

        assert response['success'] is True
        assert node.commit_index == committed, "an old snapshot rewound us"
        assert node.machine.partition_count('topic-7') == 1
    finally:
        for n in nodes:
            await n.stop()


@async_test
async def test_installing_a_snapshot_defers_the_election_timer(tmp_path):
    """
    A follower being caught up must not campaign against the leader helping it.

    Nothing fails loudly without this — which is exactly why it gets left out.
    The symptom appears only when the snapshot takes longer to arrive than an
    election timeout, and then it looks like an unrelated leadership flap.
    """
    network, nodes = await build_cluster(tmp_path, 3, snapshot_threshold=4)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        node = next(n for n in nodes if n.role != LEADER)

        node._deadline = 0.0                        # about to campaign
        await node.handle_install_snapshot({
            'term': node.current_term,
            'leader_id': leaders(nodes)[0].node_id,
            'last_index': node.commit_index + 50,
            'last_term': node.current_term,
            'state': {'brokers': [], 'topics': {}, 'partitions': [],
                      'last_applied': node.commit_index + 50},
        })

        assert node._deadline > asyncio.get_running_loop().time()
        assert node.role == 'follower'
    finally:
        for n in nodes:
            await n.stop()


def test_reset_to_snapshot_discards_everything(tmp_path):
    log_ = RaftLog(str(tmp_path))
    _fill(log_, 6, term=1)

    assert log_.reset_to_snapshot(40, 6) == 6
    assert log_.first_index == 41
    assert log_.last_index == 40
    assert log_.term_at(40) == 6
    assert log_.matches(40, 6) is True
    assert log_.entries_from(41) == []

    log_.append(6, {'type': 'MARKER'})
    assert log_.last_index == 41

    log_.close()
    reopened = RaftLog(str(tmp_path), snapshot_last_index=40, snapshot_last_term=6)
    assert [e.index for e in reopened.entries_from(41)] == [41]
