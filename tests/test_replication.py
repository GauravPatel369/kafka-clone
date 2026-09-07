"""
Tests for Phase 6 — replication.

Six steps, each with its own section below:

    1. PLACEMENT   the controller decides who should hold a copy, and commits
                   that through Raft. No data moves.
    2. COPYING     followers fetch from their leader and append with the
                   leader's offsets intact.
    3. VISIBILITY  the high watermark hides records that are not yet on every
                   replica from consumers — but not from followers.
    4. THE ISR     a replica that stops fetching is ejected, so one sick
                   machine cannot freeze a partition's watermark.
    5. DURABILITY  `acks=all` waits for the watermark; `min.insync.replicas`
                   refuses a write that cannot be made durable.
    6. FAILOVER    promotion from the ISR, and a leader epoch that fences a
                   broker which has not heard it was replaced.

The properties worth pinning down are the ones that are easy to get wrong in a
way nothing notices. A replica set naming the same broker twice looks like
redundancy and provides none. An assignment that piles every leader onto one
machine replicates correctly while destroying the load balancing Phase 3
existed to create. A follower that re-derives offsets instead of copying them
produces two logs that agree until the first divergence and disagree silently
forever after.

**These tests are not the primary evidence.** Journal #29 records eleven green
unit tests over a cluster that placed one copy of everything, because each one
handed the algorithm its inputs and so could not notice that the inputs never
arrived. Every step here was checked end to end on three real brokers first;
these pin the behaviour down afterwards.
"""

import pytest

from broker.cluster import ClusterState
from broker.log import Log, LogConfig
from broker.raft import LogEntry, MetadataStateMachine
from common.cluster_config import BrokerAddress, ClusterConfig


def _machine(topics: dict) -> MetadataStateMachine:
    machine = MetadataStateMachine()
    for name, partitions in topics.items():
        machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC',
                                      'name': name, 'partitions': partitions}))
    return machine


# ── The shape of a replica set ─────────────────────────────────

def test_a_replica_set_never_names_one_broker_twice():
    """
    Two "copies" on one machine is one copy that costs twice as much.

    The failure this guards against is silent in exactly the wrong way: the
    cluster reports replication factor 3, the admin CLI shows three entries,
    and losing one machine loses two of them.
    """
    machine = _machine({'orders': 6})

    for item in machine.compute_assignment([1, 2, 3], replication_factor=3):
        assert len(set(item['replicas'])) == len(item['replicas']), \
            f"{item['topic']}-{item['partition']} has a duplicate replica"


def test_replication_factor_is_clamped_to_the_cluster_size():
    """
    Asking for more copies than machines is not a smaller cluster with extra
    redundancy. Clamping keeps the assignment honest about how many failures
    it survives rather than listing a broker twice to hit a number.
    """
    machine = _machine({'orders': 3})

    for item in machine.compute_assignment([1, 2], replication_factor=5):
        assert len(item['replicas']) == 2
        assert set(item['replicas']) == {1, 2}


def test_the_leader_is_the_first_replica():
    machine = _machine({'orders': 6})

    for item in machine.compute_assignment([1, 2, 3], replication_factor=2):
        assert item['leader'] == item['replicas'][0]


def test_leadership_still_spreads_across_brokers():
    """
    Replication must not undo Phase 3's load balancing.

    Taking the same N brokers for every partition would replicate perfectly
    and give one machine every leadership — durable, and unbalanced. Walking
    the ring from a rotating start is what prevents it, and this is the test
    that would notice if that rotation were removed.
    """
    machine = _machine({'orders': 9})
    assignment = machine.compute_assignment([1, 2, 3], replication_factor=3)

    counts = {}
    for item in assignment:
        counts[item['leader']] = counts.get(item['leader'], 0) + 1

    assert counts == {1: 3, 2: 3, 3: 3}


def test_every_broker_carries_a_share_of_the_copies():
    """Followership spreads too, not just leadership."""
    machine = _machine({'orders': 9})
    assignment = machine.compute_assignment([1, 2, 3], replication_factor=2)

    load = {}
    for item in assignment:
        for node in item['replicas']:
            load[node] = load.get(node, 0) + 1

    assert load == {1: 6, 2: 6, 3: 6}


def test_assignment_is_deterministic():
    """
    Same inputs, same answer — or leadership churns on every recomputation and
    the controller proposes an entry a second forever.
    """
    machine = _machine({'orders': 6, 'payments': 3})

    first = machine.compute_assignment([3, 1, 2], replication_factor=2)
    second = machine.compute_assignment([1, 2, 3], replication_factor=2)

    assert first == second


def test_replication_factor_can_differ_per_topic():
    """
    A callable, because factors are a per-topic setting the state machine has
    no way to know — it must stay a pure function of its arguments.
    """
    machine = _machine({'critical': 2, 'scratch': 2})
    factors = {'critical': 3, 'scratch': 1}

    assignment = machine.compute_assignment(
        [1, 2, 3], replication_factor=lambda topic: factors[topic])

    by_topic = {}
    for item in assignment:
        by_topic.setdefault(item['topic'], []).append(len(item['replicas']))

    assert by_topic['critical'] == [3, 3]
    assert by_topic['scratch'] == [1, 1]


# ── Committing the decision ────────────────────────────────────

def test_replica_sets_survive_a_commit_and_a_snapshot_round_trip():
    """
    Replica sets go through the Raft log like every other decision, so they
    have to survive both `apply()` and the snapshot/restore path. A field that
    round-trips through one but not the other is lost on the next restart.
    """
    machine = _machine({'orders': 2})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0,
                     'leader': 1, 'replicas': [1, 2, 3]},
                    {'topic': 'orders', 'partition': 1,
                     'leader': 2, 'replicas': [2, 3, 1]}],
    }))

    assert machine.replicas_for('orders', 0) == [1, 2, 3]

    restored = MetadataStateMachine()
    restored.restore(machine.snapshot())

    assert restored.replicas_for('orders', 0) == [1, 2, 3]
    assert restored.replicas_for('orders', 1) == [2, 3, 1]
    assert restored.leader_for('orders', 1) == 2


def test_an_old_assignment_does_not_wipe_replica_sets():
    """
    Entries written before Phase 6 carry no `replicas` key. Replaying one must
    leave the existing set alone: omitted means "unchanged", never "empty".
    Treating it as empty would silently un-replicate every partition the first
    time an old log was replayed.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0,
                     'leader': 1, 'replicas': [1, 2, 3]}]}))

    machine.apply(LogEntry(3, 1, {          # Phase 5 shape: leader only
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0, 'leader': 2}]}))

    assert machine.leader_for('orders', 0) == 2
    assert machine.replicas_for('orders', 0) == [1, 2, 3]


def test_an_unreplicated_partition_reports_its_leader_as_its_only_copy():
    """
    A Phase 5 partition has exactly one copy, and saying so is the truth about
    it rather than a placeholder. Returning an empty list would read as "no
    copies anywhere", which is a different and much more alarming claim.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0, 'leader': 3}]}))

    assert machine.replicas_for('orders', 0) == [3]
    assert machine.replicas_for('orders', 99) == []


def test_the_replication_factor_is_committed_with_the_topic():
    """
    The bug every unit test above sailed past.

    `compute_assignment` was correct, the replica-set arithmetic was correct,
    and a real three-broker cluster still placed every RF=3 partition with one
    copy — because `CREATE_TOPIC` carried the partition count and not the
    factor. The factor lived only in the store of whichever broker served the
    request, and placement runs on the controller, which need not be that
    broker.

    The lesson is the same one Phase 5 kept teaching: a unit test that calls
    the algorithm directly cannot see a value that never reaches it.
    """
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC', 'name': 'orders',
                                  'partitions': 6, 'replication_factor': 3}))

    assert machine.replication_factor('orders') == 3

    assignment = machine.compute_assignment(
        [1, 2, 3], replication_factor=machine.replication_factor)
    assert all(len(item['replicas']) == 3 for item in assignment)

    # And it must survive a snapshot, or the first restart silently
    # un-replicates every topic in the cluster.
    restored = MetadataStateMachine()
    restored.restore(machine.snapshot())
    assert restored.replication_factor('orders') == 3


def test_a_topic_created_before_phase_6_defaults_to_one_copy():
    """Omitted means one, not zero — a Phase 5 topic has exactly one copy."""
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC',
                                  'name': 'legacy', 'partitions': 2}))

    assert machine.replication_factor('legacy') == 1
    assert machine.replication_factor('never-heard-of-it') == 1


def test_a_replica_move_counts_as_a_change():
    """
    `assignment_differs` gates whether anything is proposed at all. Comparing
    only leaders would decide that moving where the COPIES live is a no-op —
    so the data would never be replicated to the new machine, and nothing
    would report an error.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0,
                     'leader': 1, 'replicas': [1, 2]}]}))

    same = [{'topic': 'orders', 'partition': 0, 'leader': 1,
             'replicas': [1, 2], 'isr': [1, 2]}]
    moved = [{'topic': 'orders', 'partition': 0, 'leader': 1,
              'replicas': [1, 3], 'isr': [1, 3]}]
    shrunk = [{'topic': 'orders', 'partition': 0, 'leader': 1,
               'replicas': [1, 2], 'isr': [1]}]

    assert machine.assignment_differs(same) is False
    assert machine.assignment_differs(moved) is True
    # An ISR shrink is a change too: it alters the high watermark and who may
    # be elected leader, even though the replica set is untouched.
    assert machine.assignment_differs(shrunk) is True


# ── Step 2: copying records without re-deciding their offsets ──

def _records(*pairs):
    return [{'offset': offset, 'key': None, 'value': value, 'timestamp': 1700}
            for offset, value in pairs]


def test_a_follower_preserves_the_leaders_offsets(tmp_path):
    """
    The rule the whole phase rests on.

    `append` stamps its own `next_offset`, which is right for a leader —
    deciding a message is number 41 is the leader's job. A follower doing the
    same would be re-deciding it, and the instant the two logs differ by one
    record every later offset names a different message on the two machines.
    """
    log = Log(str(tmp_path / 'orders-0'))
    assert log.append_replicated(_records((0, 'a'), (1, 'b'), (2, 'c'))) == 3

    assert log.next_offset == 3
    assert [(m['offset'], m['value']) for m in log.read(0, 10)] == \
        [(0, 'a'), (1, 'b'), (2, 'c')]


def test_records_already_held_are_skipped_not_duplicated(tmp_path):
    """
    A follower re-fetching after a restart legitimately receives records it
    already has. Appending them again would put two messages at one offset —
    which is the exact failure the partition lock exists to prevent, arriving
    through a different door.
    """
    log = Log(str(tmp_path / 'orders-0'))
    log.append_replicated(_records((0, 'a'), (1, 'b')))

    assert log.append_replicated(_records((0, 'a'), (1, 'b'), (2, 'c'))) == 1
    assert log.next_offset == 3
    assert [m['value'] for m in log.read(0, 10)] == ['a', 'b', 'c']


def test_a_gap_is_refused_rather_than_written(tmp_path):
    """
    The fetcher always asks from its own log end, so a gap means the leader
    answered a question nobody asked. A log with a hole in it silently breaks
    the sparse index, the high watermark, and every read that crosses it — so
    this raises rather than writing something unreadable.
    """
    log = Log(str(tmp_path / 'orders-0'))
    log.append_replicated(_records((0, 'a')))

    with pytest.raises(ValueError, match='replication gap'):
        log.append_replicated(_records((5, 'f')))

    assert log.next_offset == 1


def test_the_leaders_timestamp_is_copied_not_restamped(tmp_path):
    """
    A follower stamping its own clock would make one message carry different
    times on different machines, and retention would then delete it from one
    replica while another kept it.
    """
    log = Log(str(tmp_path / 'orders-0'))
    log.append_replicated([{'offset': 0, 'key': 'k', 'value': 'v',
                            'timestamp': 1234567}])

    record = log.read(0, 1)[0]
    assert record['timestamp'] == 1234567
    assert record['key'] == 'k'


def test_replicated_records_survive_a_reopen(tmp_path):
    """Replication writes through the same storage engine, so recovery is free."""
    directory = str(tmp_path / 'orders-0')
    log = Log(directory)
    log.append_replicated(_records(*[(i, f"v{i}") for i in range(50)]))
    log.close()

    reopened = Log(directory)
    assert reopened.next_offset == 50
    assert [m['value'] for m in reopened.read(48, 10)] == ['v48', 'v49']


def test_replication_rolls_segments_like_any_other_append(tmp_path):
    """
    A follower must not end up with a different segment layout from its
    leader's — it uses the same rolling policy, through the same code path.
    """
    config = LogConfig(segment_bytes=512)
    log = Log(str(tmp_path / 'orders-0'), config)
    log.append_replicated(_records(*[(i, f"value-{i}" * 4) for i in range(60)]))

    assert len(log.segments) > 1, "60 records at 512-byte segments must roll"
    assert log.next_offset == 60
    assert [m['value'] for m in log.read(59, 1)] == ['value-59' * 4]


# ── Step 3: the high watermark ─────────────────────────────────

def _cluster(node_id=1, replicas=None):
    config = ClusterConfig([BrokerAddress(i, '127.0.0.1', 9090 + i)
                            for i in (1, 2, 3)])
    state = ClusterState(node_id, config, 3000)
    state.set_placement(replicas or [
        {'topic': 'orders', 'partition': 0, 'leader': 1, 'replicas': [1, 2, 3]}], 1)
    return state


def test_the_watermark_is_the_slowest_replica():
    """
    The minimum across the set, not the average and not the leader's own end.
    One lagging replica holds the whole partition's watermark back, which is
    the point: a record is safe when EVERY copy has it.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)
    state.record_replica_position('orders', 0, 3, 60)

    assert state.high_watermark('orders', 0, 120) == 60


def test_an_unheard_of_replica_is_excluded_rather_than_counted_at_zero():
    """
    Step 4 changed this deliberately, and the change is the whole feature.

    Before the ISR, a replica we had never heard from counted at offset 0 and
    pinned the watermark there. Safe — and fragile: one broker that never
    starts leaves the partition permanently unreadable while the leader keeps
    accepting writes, with nothing anywhere reporting an error.

    Now a replica that is not fetching is not in-sync, so it is not waited
    for. The replica set is unchanged; it is re-admitted the moment it starts
    keeping up. What changed is whether anyone blocks on it.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)

    # Broker 3 has never fetched, so it is out of sync and excluded. Broker 2
    # is in sync at 100, and we are the leader at 120.
    assert state.in_sync_replicas('orders', 0, 1000) == [1, 2]
    assert state.high_watermark('orders', 0, 120, 1000) == 100


def test_the_leader_counts_at_its_own_log_end():
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)
    state.record_replica_position('orders', 0, 3, 100)

    assert state.high_watermark('orders', 0, 80) == 80


def test_an_unreplicated_partition_has_no_clamp():
    """
    RF=1 makes the watermark the leader's own log end, so the clamp is a
    no-op. Load-bearing rather than a happy accident: every single-broker run
    in Phases 0-5 depends on it, and a watermark that required a SECOND
    replica would leave a one-broker cluster unable to serve its own writes.
    """
    state = _cluster(replicas=[{'topic': 'orders', 'partition': 0,
                                'leader': 1, 'replicas': [1]}])

    assert state.high_watermark('orders', 0, 500) == 500


def test_a_replica_position_never_moves_backwards():
    """
    A retry or a reordered request can carry an older offset. Letting the
    watermark move back would un-commit records a consumer has already been
    allowed to read — and a consumer cannot un-process them.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)
    state.record_replica_position('orders', 0, 2, 40)

    assert state.replica_positions('orders', 0)[2] == 100


def test_reading_is_bounded_by_the_limit_offset(tmp_path):
    """The clamp itself, at the storage layer."""
    log = Log(str(tmp_path / 'orders-0'))
    log.append_replicated(_records(*[(i, f"v{i}") for i in range(10)]))

    assert [m['offset'] for m in log.read(0, 100, limit_offset=4)] == [0, 1, 2, 3]
    assert log.read(4, 100, limit_offset=4) == []
    assert log.read(7, 100, limit_offset=4) == []
    assert len(log.read(0, 100, limit_offset=None)) == 10


def test_the_limit_caps_rather_than_filters(tmp_path):
    """
    Applied as a cap on how many records to read, not as a filter afterwards.
    Filtering would make a consumer sitting at the watermark read a full batch
    off disk on every poll and throw all of it away.
    """
    log = Log(str(tmp_path / 'orders-0'))
    log.append_replicated(_records(*[(i, f"v{i}") for i in range(100)]))

    assert len(log.read(0, 50, limit_offset=10)) == 10
    assert len(log.read(0, 5, limit_offset=90)) == 5


# ── Step 4: the in-sync set ────────────────────────────────────

def test_a_replica_that_stops_fetching_leaves_the_isr():
    """
    In-sync is measured in TIME, not in records behind.

    Kafka shipped `replica.lag.max.messages` and removed it in 0.9: a count
    threshold cannot tell a broken replica from a healthy one during a traffic
    spike, so a producer burst would eject the whole ISR at exactly the moment
    durability matters most.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)
    state.record_replica_position('orders', 0, 3, 100)

    assert state.in_sync_replicas('orders', 0, 10_000) == [1, 2, 3]

    # Broker 3 goes quiet. Rewinding its last-seen stamp is how we age it out
    # without sleeping: a wall-clock test would have to wait out a real window,
    # and `time.monotonic()` on Windows has ~15 ms resolution, so a
    # zero-length window is not reliably "already expired" — `now - seen`
    # rounds to exactly 0.0 and compares equal. The ejection itself is proven
    # against real brokers; this pins the rule.
    state._replica_seen[('orders', 0)][3] -= 10.0

    assert state.in_sync_replicas('orders', 0, 5_000) == [1, 2]
    assert 3 in state.replicas_for('orders', 0), \
        "ejection changes the ISR, never the replica set"


def test_a_caught_up_replica_asking_for_the_same_offset_stays_in_sync():
    """
    Liveness is about ASKING, not about advancing. A follower with nothing new
    to fetch asks for the same offset forever and is perfectly in sync —
    treating a static position as lag would eject every idle partition's
    replicas.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 50)
    state.record_replica_position('orders', 0, 2, 50)
    state.record_replica_position('orders', 0, 2, 50)

    assert 2 in state.in_sync_replicas('orders', 0, 10_000)


def test_the_watermark_ignores_a_replica_that_left_the_isr():
    """
    The point of Step 4. Before the ISR, one follower with a dying disk pinned
    the watermark forever and the partition silently stopped serving new
    records while the leader kept accepting writes.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 100)
    state.record_replica_position('orders', 0, 3, 100)

    assert state.high_watermark('orders', 0, 100, 10_000) == 100
    # With nobody in sync but us, our own log end is the watermark.
    assert state.high_watermark('orders', 0, 100, 0) == 100


def test_the_isr_survives_a_commit_and_a_snapshot():
    """
    Committed rather than leader-local, because Step 6's leader election reads
    it: the set has to outlive the leader that computed it.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0, 'leader': 1,
                     'replicas': [1, 2, 3], 'isr': [1, 2]}]}))

    assert machine.isr_for('orders', 0) == [1, 2]

    restored = MetadataStateMachine()
    restored.restore(machine.snapshot())
    assert restored.isr_for('orders', 0) == [1, 2]


def test_an_isr_defaults_to_the_whole_replica_set():
    """
    A fresh partition must be writable. Treating an unrecorded ISR as empty
    would fail every `acks=all` write on a healthy cluster.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {
        'type': 'ASSIGN_PARTITIONS',
        'leaders': [{'topic': 'orders', 'partition': 0, 'leader': 1,
                     'replicas': [1, 2, 3]}]}))

    assert machine.isr_for('orders', 0) == [1, 2, 3]


def test_only_a_partitions_leader_may_report_its_isr():
    """
    An ISR report is an observation of a leader's OWN followers. A broker has
    no standing to describe a partition it does not serve, and accepting such
    a report would let any broker shrink any partition's ISR.
    """
    state = _cluster()

    state.record_reported_isr(1, [{'topic': 'orders', 'partition': 0,
                                   'isr': [1, 2]}])
    assert state.reported_isr('orders', 0) == [1, 2]

    state.record_reported_isr(3, [{'topic': 'orders', 'partition': 0,
                                   'isr': [3]}])
    assert state.reported_isr('orders', 0) == [1, 2], \
        "broker 3 does not lead orders-0 and must not be believed about it"


# ── Step 5: acks=all and min.insync.replicas ───────────────────

def test_acks_all_is_satisfied_by_the_watermark_alone():
    """
    `acks=all` counts nothing. The high watermark ALREADY means "every in-sync
    replica has this", so waiting for `hw > offset` is the whole
    implementation — no per-follower bookkeeping, no acknowledgement protocol,
    no second mechanism to keep consistent with the first.
    """
    state = _cluster()
    state.record_replica_position('orders', 0, 2, 40)
    state.record_replica_position('orders', 0, 3, 40)

    # Leader wrote up to 49; followers hold through 39.
    assert state.high_watermark('orders', 0, 50, 10_000) == 40
    # So a write at offset 39 is acknowledged and one at 40 is not.
    assert state.high_watermark('orders', 0, 50, 10_000) > 39
    assert not state.high_watermark('orders', 0, 50, 10_000) > 40


def test_min_insync_counts_the_isr_not_the_replica_set():
    """
    Three replicas of which one is dead is TWO in-sync, and pretending
    otherwise is exactly the lie this check exists to prevent.
    """
    state = _cluster()
    state.set_isr('orders', 0, [1, 2])

    assert state.replicas_for('orders', 0) == [1, 2, 3]
    assert state.isr_for('orders', 0) == [1, 2]


def test_a_partitioned_leader_cannot_rely_on_the_committed_isr():
    """
    The bug this guards against, found by isolating a leader that was also the
    controller.

    A leader cut off from the quorum cannot COMMIT anything — including the
    news that its followers have stopped fetching. Trusting the committed set
    alone, it kept acknowledging `acks=all` writes claiming three copies while
    holding the only one. Acceptance therefore uses the intersection of
    committed and observed, which can only ever be smaller: a leader may
    refuse a write it could have taken, and may never take one it should
    refuse.
    """
    state = _cluster()
    state.set_isr('orders', 0, [1, 2, 3])       # last thing the quorum agreed
    state.record_replica_position('orders', 0, 2, 10)
    state.record_replica_position('orders', 0, 3, 10)
    state._replica_seen[('orders', 0)][2] -= 10.0
    state._replica_seen[('orders', 0)][3] -= 10.0

    committed = state.isr_for('orders', 0)
    observed = state.in_sync_replicas('orders', 0, 5_000)

    assert committed == [1, 2, 3], "the stale belief is still there"
    assert observed == [1], "but we can see nobody is fetching"
    assert [n for n in committed if n in observed] == [1], \
        "and acceptance must use the intersection"


# ── Step 6: promotion from the ISR, and the leader epoch ───────

def test_the_leader_epoch_only_bumps_when_the_leader_changes():
    """
    Incrementing on every recomputation would fence brokers that are perfectly
    current, and would make the number mean nothing.
    """
    machine = _machine({'orders': 1})

    machine.apply(LogEntry(2, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 1,
         'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 1}]}))
    assert machine.leader_epoch('orders', 0) == 1

    # Same leader, re-proposed: the epoch must not move.
    machine.apply(LogEntry(3, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 1,
         'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 1}]}))
    assert machine.leader_epoch('orders', 0) == 1

    machine.apply(LogEntry(4, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 2,
         'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 2}]}))
    assert machine.leader_epoch('orders', 0) == 2


def test_the_leader_epoch_never_goes_backwards():
    """
    A stale proposal must not be able to un-fence a broker that was already
    fenced. Monotonic in the state machine, not merely in the caller.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 1,
         'replicas': [1], 'isr': [1], 'leader_epoch': 7}]}))

    machine.apply(LogEntry(3, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 1,
         'replicas': [1], 'isr': [1], 'leader_epoch': 3}]}))

    assert machine.leader_epoch('orders', 0) == 7


def test_the_epoch_survives_a_snapshot():
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 2,
         'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 5}]}))

    restored = MetadataStateMachine()
    restored.restore(machine.snapshot())
    assert restored.leader_epoch('orders', 0) == 5


def test_an_epoch_change_counts_as_an_assignment_change():
    """
    `assignment_differs` gates whether anything is proposed. If it ignored the
    epoch, a leadership change that happened to keep the same replica set and
    ISR would never reach the log — and nothing would ever be fenced.
    """
    machine = _machine({'orders': 1})
    machine.apply(LogEntry(2, 1, {'type': 'ASSIGN_PARTITIONS', 'leaders': [
        {'topic': 'orders', 'partition': 0, 'leader': 1,
         'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 1}]}))

    same = [{'topic': 'orders', 'partition': 0, 'leader': 1,
             'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 1}]
    bumped = [{'topic': 'orders', 'partition': 0, 'leader': 1,
               'replicas': [1, 2], 'isr': [1, 2], 'leader_epoch': 2}]

    assert machine.assignment_differs(same) is False
    assert machine.assignment_differs(bumped) is True


def test_cluster_state_keeps_the_epoch_monotonic():
    """
    A delayed CONTROLLER_UPDATE carrying an older epoch must not un-fence us.
    The version check catches most of this; the epoch floor catches the rest
    without depending on it.
    """
    state = _cluster()
    state.set_placement([{'topic': 'orders', 'partition': 0, 'leader': 1,
                          'replicas': [1, 2, 3], 'isr': [1, 2, 3],
                          'leader_epoch': 4}], 2)
    assert state.leader_epoch('orders', 0) == 4

    state.set_placement([{'topic': 'orders', 'partition': 0, 'leader': 1,
                          'replicas': [1, 2, 3], 'isr': [1, 2, 3],
                          'leader_epoch': 1}], 3)
    assert state.leader_epoch('orders', 0) == 4


def test_an_unfenced_partition_reports_epoch_zero():
    """A partition whose leader has never changed. Zero, not None."""
    state = _cluster()
    assert state.leader_epoch('orders', 0) == 0
    assert state.leader_epoch('never-heard-of-it', 3) == 0
