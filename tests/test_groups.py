"""
Tests for consumer groups: assignment, rebalancing, fencing, and offsets.

The invariant everything rests on: **each partition is assigned to exactly one
member**. Assign one twice and two consumers process the same messages; assign
it zero times and it silently stops being read. Neither raises an error
anywhere, so the tests check the invariant directly.
"""

import asyncio
import threading
import time

import pytest

from broker.broker import Broker
from common import errors
from common.errors import IllegalGenerationError, UnknownMemberIdError
from common.net import SyncConnection
from common.protocol import (
    API_FIND_COORDINATOR, API_HEARTBEAT, API_JOIN_GROUP, API_OFFSET_COMMIT,
    API_OFFSET_FETCH, API_SYNC_GROUP,
)
from consumer.assignor import range_assign, round_robin_assign
from consumer.consumer import Consumer
from producer.producer import Producer


@pytest.fixture
def group_broker(tmp_path):
    """A broker with 4-partition topics and a short session timeout."""
    instance = Broker(host='127.0.0.1', port=0, data_dir=str(tmp_path / 'data'),
                      default_partitions=4, retention_check_ms=10 ** 7,
                      group_reap_ms=0.3)
    thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                              daemon=True)
    thread.start()
    assert instance.ready.wait(timeout=5)
    yield instance
    instance.stop()
    thread.join(timeout=5)


def _produce(port, topic, count, partitions=4):
    with Producer('127.0.0.1', port, topic, quiet=True) as p:
        for i in range(count):
            p.send(f"m{i}", partition=i % partitions)
        p.flush()


# ── Assignors ──────────────────────────────────────────────────

def test_range_gives_contiguous_runs():
    result = range_assign({'A': ['orders'], 'B': ['orders'], 'C': ['orders']},
                          {'orders': 7})
    assert result['A'] == [('orders', 0), ('orders', 1), ('orders', 2)]
    assert result['B'] == [('orders', 3), ('orders', 4)]
    assert result['C'] == [('orders', 5), ('orders', 6)]


def test_range_co_partitions_related_topics():
    """
    THE reason range is the default. With two topics partitioned the same way,
    one member gets `orders-0` AND `payments-0` — so if you are joining those
    streams, the data you need is on the same consumer.
    """
    subs = {'A': ['orders', 'payments'], 'B': ['orders', 'payments']}
    result = range_assign(subs, {'orders': 4, 'payments': 4})

    for member, partitions in result.items():
        orders = {p for t, p in partitions if t == 'orders'}
        payments = {p for t, p in partitions if t == 'payments'}
        assert orders == payments, f"{member} should hold matching partitions"


def test_round_robin_balances_better_across_topics():
    subs = {'A': ['t1', 't2'], 'B': ['t1', 't2'], 'C': ['t1', 't2']}
    result = round_robin_assign(subs, {'t1': 3, 't2': 2})

    sizes = sorted(len(v) for v in result.values())
    assert max(sizes) - min(sizes) <= 1, "round-robin skew is at most one"


@pytest.mark.parametrize('assignor', [range_assign, round_robin_assign])
@pytest.mark.parametrize('members,partitions', [(1, 4), (3, 4), (4, 4), (5, 4), (3, 7)])
def test_every_partition_assigned_exactly_once(assignor, members, partitions):
    """
    The invariant. Twice means duplicate processing; zero times means a
    partition silently stops being read. Neither raises anywhere.
    """
    subs = {f"m{i}": ['orders'] for i in range(members)}
    result = assignor(subs, {'orders': partitions})

    assigned = [tp for v in result.values() for tp in v]
    assert sorted(assigned) == [('orders', p) for p in range(partitions)]
    assert len(assigned) == len(set(assigned)), "no partition assigned twice"


def test_more_members_than_partitions_leaves_some_idle():
    """
    THE most common surprise for people new to Kafka: the partition count is
    the ceiling on consumer parallelism. A fifth consumer on a four-partition
    topic does nothing at all, forever.
    """
    subs = {f"m{i}": ['orders'] for i in range(5)}
    result = range_assign(subs, {'orders': 4})

    idle = [m for m, partitions in result.items() if not partitions]
    assert len(idle) == 1


def test_members_with_different_subscriptions_are_handled():
    """A group may legitimately hold members reading different topics."""
    subs = {'A': ['orders'], 'B': ['payments'], 'C': ['orders', 'payments']}
    result = round_robin_assign(subs, {'orders': 2, 'payments': 2})

    for member, partitions in result.items():
        for topic, _p in partitions:
            assert topic in subs[member], (
                f"{member} was assigned {topic}, which it did not subscribe to")


# ── Coordinator protocol ───────────────────────────────────────

def test_find_coordinator_answers_with_this_broker(group_broker):
    """
    Always us — there is one broker. The API exists so Phase 5 adds a CLUSTER
    rather than a protocol change: real Kafka hashes the group id to spread
    coordination across brokers.
    """
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        response = conn.request(API_FIND_COORDINATOR, {'group_id': 'g'})
    assert response['error_code'] == errors.NONE
    assert response['node_id'] == 0
    assert response['port'] == group_broker.port


def test_single_member_join_completes_immediately(group_broker):
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        response = conn.request(API_JOIN_GROUP, {
            'group_id': 'solo', 'member_id': '', 'topics': ['orders']})

    assert response['error_code'] == errors.NONE
    assert response['is_leader'], "the only member is the leader"
    assert len(response['members']) == 1
    assert response['generation_id'] == 1
    assert response['partition_counts'] == {'orders': 4}


def test_generation_increments_on_every_rebalance(group_broker):
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        first = conn.request(API_JOIN_GROUP, {
            'group_id': 'gen', 'member_id': '', 'topics': ['orders']})
        again = conn.request(API_JOIN_GROUP, {
            'group_id': 'gen', 'member_id': first['member_id'],
            'topics': ['orders']})

    assert again['generation_id'] > first['generation_id']


def test_stale_generation_is_fenced(group_broker):
    """
    THE zombie fence, tested directly.

    A consumer that paused past its session timeout was evicted and its
    partitions reassigned. If its late commit were accepted it would rewind the
    group behind the replacement's progress and those messages would be
    processed twice, with nothing reporting it.
    """
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        joined = conn.request(API_JOIN_GROUP, {
            'group_id': 'fenced', 'member_id': '', 'topics': ['orders']})
        member_id = joined['member_id']
        conn.request(API_SYNC_GROUP, {
            'group_id': 'fenced', 'member_id': member_id,
            'generation_id': joined['generation_id'],
            'assignments': {member_id: [['orders', 0]]}})

        # Rejoin, bumping the generation. The old one is now stale.
        conn.request(API_JOIN_GROUP, {
            'group_id': 'fenced', 'member_id': member_id, 'topics': ['orders']})

        stale = conn.request(API_OFFSET_COMMIT, {
            'group_id': 'fenced', 'member_id': member_id,
            'generation_id': joined['generation_id'],
            'offsets': [{'topic': 'orders', 'partition': 0, 'offset': 99}]})

    assert stale['error_code'] == errors.ILLEGAL_GENERATION
    with pytest.raises(IllegalGenerationError):
        errors.raise_for_error(stale)


def test_unknown_member_is_rejected(group_broker):
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        conn.request(API_JOIN_GROUP, {
            'group_id': 'strangers', 'member_id': '', 'topics': ['orders']})
        response = conn.request(API_HEARTBEAT, {
            'group_id': 'strangers', 'member_id': 'never-joined',
            'generation_id': 1})

    assert response['error_code'] == errors.UNKNOWN_MEMBER_ID
    with pytest.raises(UnknownMemberIdError):
        errors.raise_for_error(response)


def test_heartbeat_reports_a_rebalance_in_progress(group_broker):
    """
    How a member LEARNS about a rebalance it did not cause. Without this, a
    member whose peer just died would keep consuming partitions already
    reassigned to somebody else — two consumers on one partition, which is
    exactly what a group prevents.
    """
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as first, \
            SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as second:
        a = first.request(API_JOIN_GROUP, {
            'group_id': 'notify', 'member_id': '', 'topics': ['orders']})
        first.request(API_SYNC_GROUP, {
            'group_id': 'notify', 'member_id': a['member_id'],
            'generation_id': a['generation_id'],
            'assignments': {a['member_id']: [['orders', 0]]}})

        assert first.request(API_HEARTBEAT, {
            'group_id': 'notify', 'member_id': a['member_id'],
            'generation_id': a['generation_id']})['error_code'] == errors.NONE

        # A second member joins. That request is HELD until A rejoins, so send
        # it without waiting for the reply.
        second.send_request(API_JOIN_GROUP, {
            'group_id': 'notify', 'member_id': '', 'topics': ['orders']})
        time.sleep(0.2)

        beat = first.request(API_HEARTBEAT, {
            'group_id': 'notify', 'member_id': a['member_id'],
            'generation_id': a['generation_id']})

    assert beat['error_code'] == errors.REBALANCE_IN_PROGRESS


# ── Offset storage ─────────────────────────────────────────────

def test_offsets_survive_a_broker_restart(tmp_path):
    """
    The whole reason offsets moved off the consumer's disk. `__consumer_offsets`
    is a real log, so committed positions are rebuilt by replaying it.
    """
    data_dir = str(tmp_path / 'data')

    def run(instance):
        thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                                  daemon=True)
        thread.start()
        assert instance.ready.wait(timeout=5)
        return thread

    first = Broker(host='127.0.0.1', port=0, data_dir=data_dir,
                   retention_check_ms=10 ** 7)
    thread = run(first)
    with SyncConnection('127.0.0.1', first.port, timeout=5.0) as conn:
        conn.request(API_OFFSET_COMMIT, {
            'group_id': 'durable',
            'offsets': [{'topic': 'orders', 'partition': 1, 'offset': 4242}]})
    first.stop()
    thread.join(timeout=5)

    second = Broker(host='127.0.0.1', port=0, data_dir=data_dir,
                    retention_check_ms=10 ** 7)
    thread = run(second)
    try:
        with SyncConnection('127.0.0.1', second.port, timeout=5.0) as conn:
            response = conn.request(API_OFFSET_FETCH, {
                'group_id': 'durable',
                'partitions': [{'topic': 'orders', 'partition': 1}]})
        assert response['offsets'][0]['offset'] == 4242
    finally:
        second.stop()
        thread.join(timeout=5)


def test_never_committed_returns_minus_one_not_zero(group_broker):
    """
    -1 and 0 mean different things. "Never committed" means apply
    `auto.offset.reset`; 0 means "I have genuinely committed the beginning".
    Collapsing them makes a new group silently replay everything, or silently
    skip it, depending on which default you picked.
    """
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        response = conn.request(API_OFFSET_FETCH, {
            'group_id': 'brand-new',
            'partitions': [{'topic': 'orders', 'partition': 0}]})
    assert response['offsets'][0]['offset'] == -1


def test_the_last_commit_wins_on_replay(group_broker):
    with SyncConnection('127.0.0.1', group_broker.port, timeout=5.0) as conn:
        for offset in (10, 20, 30):
            conn.request(API_OFFSET_COMMIT, {
                'group_id': 'overwrite',
                'offsets': [{'topic': 'orders', 'partition': 0, 'offset': offset}]})
        response = conn.request(API_OFFSET_FETCH, {
            'group_id': 'overwrite',
            'partitions': [{'topic': 'orders', 'partition': 0}]})
    assert response['offsets'][0]['offset'] == 30


# ── End-to-end group behaviour ─────────────────────────────────

def test_one_consumer_gets_every_partition(group_broker):
    _produce(group_broker.port, 'solo-topic', 40)

    with Consumer('127.0.0.1', group_broker.port, group_id='g1', quiet=True) as c:
        c.subscribe(['solo-topic'])
        assert len(c.assignment) == 4
        assert len(c.poll(max_messages=100)) == 40


def test_two_consumers_divide_the_partitions(group_broker):
    """
    The read side of load balancing. Each partition goes to exactly one member
    — they divide the work rather than duplicating it, which is the difference
    from two separate groups.
    """
    _produce(group_broker.port, 'shared', 40)

    first = Consumer('127.0.0.1', group_broker.port, group_id='pair', quiet=True,
                     heartbeat_interval_ms=100, session_timeout_ms=2000)
    second = Consumer('127.0.0.1', group_broker.port, group_id='pair', quiet=True,
                      heartbeat_interval_ms=100, session_timeout_ms=2000)

    try:
        first.connect()
        second.connect()

        # `first` joins alone and immediately owns all four partitions — there
        # is nobody else yet.
        first.subscribe(['shared'])
        assert len(first.assignment) == 4

        # `second` joining triggers a rebalance, and its JOIN_GROUP is HELD
        # until `first` rejoins. So it has to run on another thread.
        thread = threading.Thread(target=lambda: second.subscribe(['shared']))
        thread.start()

        # CONCEPT: a consumer rejoins inside poll(), not spontaneously.
        #   The heartbeat thread only DISCOVERS the rebalance and sets a flag;
        #   the rejoin itself happens on the next poll. A consumer that stops
        #   polling therefore stops participating — which is real Kafka
        #   behaviour, and the reason `max.poll.interval.ms` exists.
        deadline = time.time() + 10
        while time.time() < deadline and len(first.assignment) == 4:
            first.poll(max_messages=1)
            time.sleep(0.05)

        thread.join(timeout=10)

        assert set(first.assignment) | set(second.assignment) == {
            ('shared', p) for p in range(4)}
        assert not (set(first.assignment) & set(second.assignment)), (
            "no partition may be assigned to both")
        assert first.assignment and second.assignment
        assert first.generation_id == second.generation_id, (
            "both members must end up in the same generation")
    finally:
        first.close()
        second.close()


def test_committed_offsets_are_shared_by_the_group(group_broker):
    """
    A restarted consumer resumes where the GROUP left off, not where that
    process left off — which is the point of moving offsets to the broker.
    """
    _produce(group_broker.port, 'resume-group', 40)

    with Consumer('127.0.0.1', group_broker.port, group_id='shared-pos',
                  quiet=True) as c:
        c.subscribe(['resume-group'])
        c.poll(max_messages=20)
        c.commit()
        first_positions = dict(c.position)

    with Consumer('127.0.0.1', group_broker.port, group_id='shared-pos',
                  quiet=True) as c:
        c.subscribe(['resume-group'])
        assert c.position == first_positions, "resumed from the group's position"


def test_a_different_group_still_gets_everything(group_broker):
    """The log-not-queue property, still holding with real groups."""
    _produce(group_broker.port, 'fanout', 40)

    for group in ('analytics', 'billing'):
        with Consumer('127.0.0.1', group_broker.port, group_id=group,
                      quiet=True) as c:
            c.subscribe(['fanout'])
            c.poll(max_messages=100)
            c.commit()
            assert sum(c.position.values()) == 40, f"{group} saw everything"


def test_manual_assign_skips_the_group_entirely(group_broker):
    """
    No heartbeats, no rebalance, no generation. The right choice for a
    partition-repair tool — and it gives up failover and fencing to get it.
    """
    _produce(group_broker.port, 'manual', 40)

    with Consumer('127.0.0.1', group_broker.port, group_id='manual-g',
                  quiet=True) as c:
        c.assign([('manual', 2)])
        assert c.generation_id == -1, "never joined a group"
        assert c._heartbeat_thread is None, "no heartbeat thread"

        messages = c.poll(max_messages=100)
        assert messages and all(m['offset'] >= 0 for m in messages)
        c.commit()


def test_a_dead_member_is_evicted_and_its_partitions_reassigned(group_broker):
    """
    Failure handling for consumers. A member that stops heartbeating is evicted
    by the reaper, and the survivors take over its partitions.

    Note the ambiguity the coordinator cannot resolve: a paused process and a
    dead process look identical from outside. That is why eviction is paired
    with generation fencing — we cannot know if it will come back, so we make
    it harmless if it does.
    """
    _produce(group_broker.port, 'failover', 40)

    survivor = Consumer('127.0.0.1', group_broker.port, group_id='ha',
                        quiet=True, session_timeout_ms=600,
                        heartbeat_interval_ms=150)
    victim = Consumer('127.0.0.1', group_broker.port, group_id='ha',
                      quiet=True, session_timeout_ms=600,
                      heartbeat_interval_ms=150)
    try:
        survivor.connect()
        victim.connect()
        thread = threading.Thread(target=lambda: survivor.subscribe(['failover']))
        thread.start()
        time.sleep(0.2)
        victim.subscribe(['failover'])
        thread.join(timeout=10)

        assert victim.assignment, "victim should own something first"

        # Kill the victim: stop heartbeating without leaving cleanly, which is
        # what a `kill -9` looks like to the coordinator.
        victim._stop_heartbeat()
        victim._cluster.close()

        deadline = time.time() + 10
        while time.time() < deadline:
            survivor.poll(max_messages=1)
            if len(survivor.assignment) == 4:
                break
            time.sleep(0.2)

        assert set(survivor.assignment) == {('failover', p) for p in range(4)}, (
            "the survivor should have taken over every partition")
    finally:
        survivor.close()
        try:
            victim.close()
        except Exception:
            pass
