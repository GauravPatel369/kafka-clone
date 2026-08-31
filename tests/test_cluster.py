"""
Tests for the cluster: membership, election, placement, and client routing.

The theme: a broker refuses to serve a partition it does not lead, and clients
find the right one without being told. Those two together are the whole of
Phase 5.
"""

import asyncio
import threading
import time

import pytest

from broker.broker import Broker
from broker.cluster import ClusterState
from broker.controller import Controller
from common import errors
from common.cluster_client import ClusterClient
from common.net import BrokerConnectionError
from common.cluster_config import BrokerAddress, ClusterConfig, ClusterConfigError
from common.net import SyncConnection
from common.protocol import (
    API_CREATE_TOPICS, API_DESCRIBE_CLUSTER, API_FETCH, API_METADATA, API_PRODUCE,
)
from consumer.consumer import Consumer
from producer.producer import Producer


# ── Roster ─────────────────────────────────────────────────────

def test_duplicate_node_ids_are_rejected():
    """
    Two brokers with one id is catastrophic and silent: leadership, heartbeats
    and metadata all key on node_id, so the cluster would believe two machines
    are the same machine.
    """
    with pytest.raises(ClusterConfigError, match='duplicate'):
        ClusterConfig([BrokerAddress(1, 'h', 1), BrokerAddress(1, 'h', 2)])


def test_peers_excludes_self():
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2, 3)])
    assert [p.node_id for p in config.peers(2)] == [1, 3]


def test_a_node_is_only_controller_if_the_quorum_says_so():
    """
    `ClusterState` no longer decides leadership — it records what Raft decided.

    The `None` case is the safety property: during an election, or inside a
    minority that cannot elect, there is no controller. Falling back to
    "probably me" is exactly the guess that produced split-brain in Phase 5.
    """
    config = ClusterConfig.single_node(7, 'h', 9092)
    state = ClusterState(7, config)

    assert state.live_brokers() == [7]

    state.set_controller(7)
    assert state.is_controller

    state.set_controller(9)
    assert not state.is_controller

    state.set_controller(None)
    assert not state.is_controller, (
        "no quorum leader means NOBODY is controller, including us")


# ── Liveness and election ──────────────────────────────────────

def test_liveness_and_leadership_are_separate_questions():
    """
    Heartbeats still say who is UP; Raft says who may DECIDE. Merging the two
    is what made the Phase 5 design unsafe, so they stay apart: a broker can be
    perfectly alive and not be the controller, and the controller can be a node
    we have not heard a heartbeat from recently.
    """
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2, 3)])
    state = ClusterState(2, config, session_timeout_ms=1000)

    state.mark_alive(1)
    state.mark_alive(3)
    assert state.live_brokers() == [1, 2, 3]

    # Being alive says nothing about being in charge.
    state.set_controller(1)
    assert not state.is_controller
    state.mark_down(1)
    assert state.live_brokers() == [2, 3]
    assert state.controller_id == 1, (
        "liveness does not silently revoke leadership — only Raft does")


def test_a_peer_that_stops_heartbeating_goes_down():
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2)])
    state = ClusterState(1, config, session_timeout_ms=100)

    state.mark_alive(2)
    assert 2 in state.live_brokers()

    time.sleep(0.2)
    assert 2 not in state.live_brokers()
    assert state.live_brokers() == [1], "we are always alive to ourselves"


def test_stale_controller_updates_are_ignored():
    """
    A network-delayed update arriving after a newer one would silently roll the
    cluster's view backwards — two brokers would then disagree about who leads
    what, which is exactly what one controller is supposed to prevent.
    """
    state = ClusterState(1, ClusterConfig.single_node(1, 'h', 9092))

    assert state.set_leaders({('t', 0): 1}, version=5)
    assert not state.set_leaders({('t', 0): 2}, version=4), "older is refused"
    assert not state.set_leaders({('t', 0): 3}, version=5), "equal is refused"
    assert state.leader_for('t', 0) == 1
    assert state.set_leaders({('t', 0): 9}, version=6)
    assert state.leader_for('t', 0) == 9


# ── Placement ──────────────────────────────────────────────────

class _FakeMetadata:
    def __init__(self, topics):
        self._topics = topics

    def list_topics(self):
        return list(self._topics)

    def get(self, name):
        return type('C', (), {'partitions': self._topics[name]})() \
            if name in self._topics else None


def test_placement_is_round_robin_and_even():
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2, 3)])
    state = ClusterState(1, config, session_timeout_ms=10_000)
    for peer in (2, 3):
        state.mark_alive(peer)

    controller = Controller(state, _FakeMetadata({'orders': 6}))
    leaders = controller.assign_leaders()

    assert len(leaders) == 6
    counts = {}
    for node in leaders.values():
        counts[node] = counts.get(node, 0) + 1
    assert counts == {1: 2, 2: 2, 3: 2}


def test_placement_is_deterministic():
    """
    Same inputs, same answer — so an inspector can verify the controller's
    arithmetic without trusting it, and a recomputation does not churn
    leadership for no reason.
    """
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2, 3)])

    def compute():
        state = ClusterState(1, config, session_timeout_ms=10_000)
        state.mark_alive(2)
        state.mark_alive(3)
        return Controller(state, _FakeMetadata({'a': 4, 'b': 3})).assign_leaders()

    assert compute() == compute()


def test_placement_uses_only_live_brokers():
    config = ClusterConfig([BrokerAddress(i, 'h', 9000 + i) for i in (1, 2, 3)])
    state = ClusterState(1, config, session_timeout_ms=10_000)
    state.mark_alive(3)                       # broker 2 never heard from

    leaders = Controller(state, _FakeMetadata({'orders': 4})).assign_leaders()
    assert set(leaders.values()) == {1, 3}


def test_recompute_reports_no_change_when_nothing_changed():
    """
    Recomputing on every tick would otherwise append an identical entry to the
    Raft log several times a second, and force a majority round trip to record
    that nothing happened. Only *changes* are events.

    `raft=None` skips the proposal step, so this stays a unit test of the
    placement logic rather than a consensus test.
    """
    async def scenario():
        state = ClusterState(1, ClusterConfig.single_node(1, 'h', 9092))
        controller = Controller(state, _FakeMetadata({'orders': 2}))

        assert await controller.recompute() is True
        assert await controller.recompute() is False
        assert state.metadata_version == 1

    asyncio.run(scenario())


# ── Leadership refusal, over the wire ──────────────────────────

def _free_port() -> int:
    """
    Reserve a free port by binding and releasing it.

    A cluster roster has to name ports BEFORE the brokers bind them, so the
    `port=0` trick used everywhere else does not work here — the roster would
    have to be written after startup, which is backwards.

    There is a small race (something else could claim the port in between).
    Accepted deliberately: the alternative is hardcoded ports, which collide
    with a dev broker and with a parallel test run — a certainty rather than a
    small chance.
    """
    import socket
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def _start_cluster(tmp_path, count: int):
    """Start `count` real brokers that know about each other. Returns them."""
    ports = [_free_port() for _ in range(count)]
    config = ClusterConfig([BrokerAddress(i + 1, '127.0.0.1', ports[i])
                            for i in range(count)])

    brokers, threads = [], []
    for i in range(count):
        instance = Broker(
            host='127.0.0.1', port=ports[i], node_id=i + 1,
            data_dir=str(tmp_path / f"b{i + 1}"), cluster_config=config,
            default_partitions=4, retention_check_ms=10 ** 7,
            broker_heartbeat_ms=100, broker_session_timeout_ms=500)
        thread = threading.Thread(target=lambda b=instance: asyncio.run(b.start()),
                                  daemon=True)
        thread.start()
        assert instance.ready.wait(timeout=5)
        brokers.append(instance)
        threads.append(thread)

    # Wait for membership AND for Raft to actually elect.
    #
    # Checking `len({controller_id}) == 1` alone is not enough: before any
    # election every broker holds None, so the set is {None} — size one, and
    # the check passes while there is no controller at all. Requiring a real
    # leader is the difference between "they agree" and "they agree on
    # somebody".
    deadline = time.time() + 15
    while time.time() < deadline:
        views = [b.cluster.live_brokers() for b in brokers]
        controllers = {b.cluster.controller_id for b in brokers}
        elected = any(b.raft.is_leader for b in brokers)
        if (all(len(v) == count for v in views)
                and len(controllers) == 1
                and None not in controllers
                and elected):
            break
        time.sleep(0.05)

    return brokers, threads


@pytest.fixture
def cluster(tmp_path):
    """A real three-broker cluster."""
    brokers, threads = _start_cluster(tmp_path, 3)
    yield brokers
    for instance in brokers:
        instance.stop()
    for thread in threads:
        thread.join(timeout=5)


@pytest.fixture
def two_brokers(tmp_path):
    """
    One live broker whose roster names a second that never starts.

    Useful for the alone-in-a-cluster cases: broker 1 is the only live member
    and therefore the controller, and broker 2 is permanently down.
    """
    config = ClusterConfig([BrokerAddress(1, '127.0.0.1', 19191),
                            BrokerAddress(2, '127.0.0.1', 19192)])
    instance = Broker(host='127.0.0.1', port=0, data_dir=str(tmp_path / 'b1'),
                      node_id=1, cluster_config=config, default_partitions=4,
                      retention_check_ms=10 ** 7, broker_heartbeat_ms=200,
                      broker_session_timeout_ms=600,
                      # A single-voter quorum, deliberately.
                      #
                      # The roster has two brokers so broker 2 shows as DOWN,
                      # but a 2-VOTER quorum would need both to elect anything
                      # — it tolerates zero failures. Making broker 1 the only
                      # voter lets it be controller on its own, which is what
                      # these tests are actually about.
                      quorum_voters=[1])
    thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                              daemon=True)
    thread.start()
    assert instance.ready.wait(timeout=5)
    yield instance
    instance.stop()
    thread.join(timeout=5)


def test_a_broker_leads_everything_when_alone(two_brokers):
    with SyncConnection('127.0.0.1', two_brokers.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'solo', 'partitions': 4})
        response = conn.request(API_METADATA, {'topics': ['solo']})

    leaders = {p['leader'] for p in response['topics'][0]['partitions']}
    assert leaders == {1}, "the only live broker leads every partition"


def test_partitions_are_spread_across_the_cluster(cluster):
    """
    The point of the phase: six partitions over three brokers, two each. Each
    broker holds ONLY the partitions it leads.
    """
    with SyncConnection('127.0.0.1', cluster[0].port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'spread', 'partitions': 6})

    deadline = time.time() + 10
    while time.time() < deadline:
        owners = [cluster[0].cluster.leader_for('spread', p) for p in range(6)]
        if all(o is not None for o in owners) and len(set(owners)) == 3:
            break
        time.sleep(0.1)

    owners = {p: cluster[0].cluster.leader_for('spread', p) for p in range(6)}
    assert set(owners.values()) == {1, 2, 3}, f"expected all three, got {owners}"

    counts = {}
    for node in owners.values():
        counts[node] = counts.get(node, 0) + 1
    assert counts == {1: 2, 2: 2, 3: 2}

    # And every broker eventually agrees — one controller decided it and
    # broadcast it. "Eventually" is the honest word: the push happens in the
    # background so a client's CREATE_TOPICS is not held hostage by an
    # unreachable peer, which means peers converge shortly after, not
    # instantly. A peer that has not caught up yet answers NOT_LEADER, and the
    # client refreshes — the gap is covered, it is not hidden.
    def all_agree():
        return all(instance.cluster.leader_for('spread', partition) == node
                   for instance in cluster
                   for partition, node in owners.items())

    deadline = time.time() + 10
    while time.time() < deadline and not all_agree():
        time.sleep(0.1)

    assert all_agree(), "every broker must converge on the controller's map"


def test_a_broker_refuses_a_partition_it_does_not_lead(cluster):
    """
    THE Phase 5 guard. Without it a broker would create an empty local log and
    serve zero messages — the client would see an EMPTY TOPIC rather than an
    error. Silent wrongness, which is the failure mode this project keeps
    trying to avoid.
    """
    with SyncConnection('127.0.0.1', cluster[0].port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'refuse', 'partitions': 6})

    deadline = time.time() + 10
    while time.time() < deadline:
        if all(cluster[0].cluster.leader_for('refuse', p) is not None
               for p in range(6)):
            break
        time.sleep(0.1)

    mine = [p for p in range(6) if cluster[0].cluster.leader_for('refuse', p) == 1]
    theirs = [p for p in range(6) if cluster[0].cluster.leader_for('refuse', p) != 1]
    assert mine and theirs, "the topic must be split across brokers"

    with SyncConnection('127.0.0.1', cluster[0].port, timeout=5.0) as conn:
        ok = conn.request(API_PRODUCE,
                          {'topic': 'refuse', 'partition': mine[0], 'value': 'x'})
        assert ok['error_code'] == errors.NONE

        refused = conn.request(API_PRODUCE,
                               {'topic': 'refuse', 'partition': theirs[0],
                                'value': 'x'})
        assert refused['error_code'] == errors.NOT_LEADER_OR_FOLLOWER

        fetched = conn.request(API_FETCH,
                               {'topic': 'refuse', 'partition': theirs[0]})
        assert fetched['error_code'] == errors.NOT_LEADER_OR_FOLLOWER


def test_produce_v1_reports_wrong_leader_per_partition(cluster):
    """
    One request can span partitions on different brokers, so leadership — like
    the error codes — has to be per partition. A partition that lives elsewhere
    must not fail the ones that do not.
    """
    with SyncConnection('127.0.0.1', cluster[0].port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'perpart', 'partitions': 6})

    deadline = time.time() + 10
    while time.time() < deadline:
        if all(cluster[0].cluster.leader_for('perpart', p) is not None
               for p in range(6)):
            break
        time.sleep(0.1)

    mine = [p for p in range(6) if cluster[0].cluster.leader_for('perpart', p) == 1]
    theirs = [p for p in range(6) if cluster[0].cluster.leader_for('perpart', p) != 1]

    with SyncConnection('127.0.0.1', cluster[0].port, timeout=5.0) as conn:
        response = conn.request(API_PRODUCE, {
            'topic': 'perpart', 'acks': 1,
            'partitions': [
                {'partition': mine[0], 'records': [{'key': None, 'value': 'ok'}]},
                {'partition': theirs[0], 'records': [{'key': None, 'value': 'no'}]},
            ],
        }, api_version=1)

    results = {e['partition']: e for e in response['partitions']}
    assert results[mine[0]]['error_code'] == errors.NONE
    assert results[theirs[0]]['error_code'] == errors.NOT_LEADER_OR_FOLLOWER


def test_not_leader_is_retriable():
    """
    It must be retriable or client-side failover cannot work: the client
    refreshes metadata and tries the real leader.
    """
    assert errors.is_retriable(errors.NOT_LEADER_OR_FOLLOWER)


def test_unknown_topic_beats_not_leader(two_brokers):
    """
    A topic that does not exist is not a topic somebody else leads. Answering
    NOT_LEADER would send a client off refreshing metadata forever over a typo;
    the more specific of two true statements is the useful one.
    """
    with SyncConnection('127.0.0.1', two_brokers.port, timeout=5.0) as conn:
        from common.protocol import API_LIST_OFFSETS
        response = conn.request(API_LIST_OFFSETS, {'topic': 'nonexistent'})
    assert response['error_code'] == errors.UNKNOWN_TOPIC_OR_PARTITION


def test_describe_cluster_reports_membership(two_brokers):
    with SyncConnection('127.0.0.1', two_brokers.port, timeout=5.0) as conn:
        response = conn.request(API_DESCRIBE_CLUSTER, {})

    assert response['node_id'] == 1
    assert response['controller_id'] == 1
    assert response['live_brokers'] == [1]
    assert response['down_brokers'] == [2]
    assert {b['node_id']: b['alive'] for b in response['brokers']} == {
        1: True, 2: False}


# ── Client routing ─────────────────────────────────────────────

def test_cluster_client_caches_and_routes(two_brokers):
    with ClusterClient([('127.0.0.1', two_brokers.port)], timeout=5.0) as client:
        client.request_any(API_CREATE_TOPICS,
                           {'topic': 'routed', 'partitions': 4})
        client.invalidate()

        assert client.leader_for('routed', 0) == 1
        assert client.partition_count('routed') == 4

        response = client.request_for_partition(
            'routed', 0, API_PRODUCE,
            {'topic': 'routed', 'partition': 0, 'value': 'hello'})
        assert response['error_code'] == errors.NONE


def test_client_refreshes_and_retries_on_not_leader(two_brokers):
    """
    Client-side failover, complete: the broker says "not me", the client
    refreshes and tries again. Here the refresh restores the true map (broker 1
    really does lead it), so the retry succeeds.
    """
    with ClusterClient([('127.0.0.1', two_brokers.port)], timeout=5.0) as client:
        client.request_any(API_CREATE_TOPICS, {'topic': 'stale', 'partitions': 2})
        client.refresh(['stale'])

        # Poison the cache: claim broker 2 leads partition 0. Broker 2 is in
        # the roster but never started, so the attempt fails and the client
        # must refresh to discover the truth.
        client._leaders[('stale', 0)] = 2
        client._brokers[2] = ('127.0.0.1', 19192)

        response = client.request_for_partition(
            'stale', 0, API_PRODUCE,
            {'topic': 'stale', 'partition': 0, 'value': 'x'})

        assert response['error_code'] == errors.NONE
        assert client.leader_for('stale', 0) == 1, "the cache was corrected"


def test_routing_gives_up_after_one_refresh(two_brokers):
    """
    Two attempts, not a loop. An unbounded retry would spin forever against a
    genuinely broken cluster; the caller already has backoff at a higher level
    via RetriableError.
    """
    with ClusterClient([('127.0.0.1', two_brokers.port)], timeout=1.0) as client:
        client.request_any(API_CREATE_TOPICS, {'topic': 'gone', 'partitions': 2})

        # Pin the client to a broker that will never answer, and stop the
        # refresh from correcting it. That is the situation the bound exists
        # for: a cluster where retrying cannot possibly help.
        client._brokers[99] = ('127.0.0.1', 19199)
        client._leaders[('gone', 0)] = 99

        attempts = []
        original = client.connection_for

        def counting(node_id):
            attempts.append(node_id)
            return original(node_id)

        client.connection_for = counting
        client._ensure_fresh = lambda *a, **k: None      # refresh cannot help

        # Either type is legitimate and the distinction is meaningful:
        # BrokerConnectionError means "could not reach it", LeaderNotAvailable
        # means "reached the cluster, no leader known". The producer's retry
        # loop catches both — `except (RetriableError, BrokerConnectionError)`
        # — precisely because a caller usually wants to treat them the same
        # while still being able to tell them apart in a log.
        with pytest.raises((errors.KafkaError, BrokerConnectionError)):
            client.request_for_partition(
                'gone', 0, API_PRODUCE,
                {'topic': 'gone', 'partition': 0, 'value': 'x'})

        assert len(attempts) <= 2, (
            f"routing must give up after one refresh, tried {len(attempts)} times")


# ── End to end through the clients ─────────────────────────────

def test_producer_and_consumer_work_against_a_cluster(two_brokers):
    with Producer('127.0.0.1', two_brokers.port, 'e2e', quiet=True) as p:
        offsets = p.send_many([f"m{i}" for i in range(40)])
    assert len(offsets) == 40

    with Consumer('127.0.0.1', two_brokers.port, group_id='g', quiet=True) as c:
        c.subscribe(['e2e'])
        messages = c.poll(max_messages=100)
    assert len(messages) == 40
