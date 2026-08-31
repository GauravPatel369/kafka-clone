"""
Tests for Raft: election, replication, persistence, and the safety rules.

Driven entirely in memory through an injected transport — no sockets. That is
what makes it possible to partition the network deterministically and assert
the property that matters: **a minority cannot elect a leader.**

The tests are organised by which Raft rule they pin down, because the rules are
what make it correct and each one is individually easy to omit without any
happy-path test noticing.
"""

import asyncio
import functools

import pytest

from broker.raft import (
    CANDIDATE, FOLLOWER, LEADER, LogEntry, MetadataStateMachine,
    PersistentState, RaftLog, RaftNode, registration_command,
)


def async_test(fn):
    """
    Run an async test without pytest-asyncio.

    The project's rule is that pytest is the only dependency, and this is five
    lines against a whole package. Each test gets a fresh event loop, which is
    also stricter than the plugin's default: no state can leak between tests
    through a shared loop.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))
    return wrapper


# ── A tiny in-memory network ───────────────────────────────────

class Network:
    """
    Routes RPCs between nodes, and can cut them.

    `partition({1,2}, {3,4,5})` makes every message across the divide vanish —
    which is what a network partition looks like from inside a node: not an
    error, just silence.
    """

    def __init__(self):
        self.nodes = {}
        self.blocked = set()        # {(from, to)} — one-directional
        self.dropped = 0

        # Nodes that are DOWN rather than merely unreachable, and the delay
        # their callers pay.
        #
        # CONCEPT: "returns None" and "returns None after a timeout" are not
        # the same failure, and the difference is where the bugs live.
        #
        #   A `blocked` message vanishes instantly, so a caller that waits for
        #   every peer is indistinguishable from one that does not. That made
        #   this whole test file agree with an implementation which, on a real
        #   network, could not keep a leader alive for two consecutive seconds
        #   after one broker died.
        #
        # A dead peer is the SLOWEST peer, always — it costs the full RPC
        # timeout, every time. Any code that waits on all peers therefore runs
        # at the pace of the worst one. Modelling that delay is what makes
        # these tests able to fail.
        self.down = set()
        self.down_delay = 0.5

    def add(self, node: RaftNode) -> None:
        self.nodes[node.node_id] = node

    def partition(self, left: set, right: set) -> None:
        for a in left:
            for b in right:
                self.blocked.add((a, b))
                self.blocked.add((b, a))

    def heal(self) -> None:
        self.blocked.clear()

    def isolate(self, node_id: int) -> None:
        others = set(self.nodes) - {node_id}
        self.partition({node_id}, others)

    def kill(self, node_id: int) -> None:
        """Stop a node and make every RPC to it time out rather than vanish."""
        self.down.add(node_id)

    async def send(self, sender: int, target: int, api: str, payload: dict):
        if target in self.down:
            self.dropped += 1
            await asyncio.sleep(self.down_delay)    # the corpse's timeout
            return None
        if (sender, target) in self.blocked or target not in self.nodes:
            self.dropped += 1
            return None
        node = self.nodes[target]
        if api == 'REQUEST_VOTE':
            return await node.handle_request_vote(payload)
        if api == 'APPEND_ENTRIES':
            return await node.handle_append_entries(payload)
        if api == 'INSTALL_SNAPSHOT':
            return await node.handle_install_snapshot(payload)
        return None


async def build_cluster(tmp_path, size: int, election_timeout_ms=200.0,
                        heartbeat_ms=40.0, snapshot_threshold=0):
    """
    Start `size` Raft nodes wired to a shared in-memory network.

    `snapshot_threshold` defaults to 0 — compaction OFF — so these tests pin
    down election and replication in isolation. `test_raft_snapshot.py` turns
    it on and re-runs the same properties across a compaction.
    """
    network = Network()
    voters = list(range(1, size + 1))
    nodes = []

    for node_id in voters:
        def transport(target, api, payload, _self=node_id):
            return network.send(_self, target, api, payload)

        node = RaftNode(node_id, voters, str(tmp_path / f"n{node_id}"),
                        transport, election_timeout_ms=election_timeout_ms,
                        heartbeat_interval_ms=heartbeat_ms,
                        snapshot_threshold=snapshot_threshold)
        network.add(node)
        nodes.append(node)

    for node in nodes:
        await node.start()
    return network, nodes


async def wait_for(predicate, timeout=8.0, interval=0.02) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def leaders(nodes) -> list:
    return [n for n in nodes if n.role == LEADER]


@async_test
async def test_a_dead_peer_does_not_prevent_the_survivors_electing(tmp_path):
    """
    Kill the leader of three and the other two must elect — and STAY elected.

    This is the test that was missing, and its absence hid a cluster-fatal
    bug through an entire phase of green runs.

    Every earlier test modelled failure as silence: a blocked message returns
    None immediately, so waiting for all peers costs nothing and the
    implementation looked correct. A real dead broker is not silent, it is
    SLOW — it costs a full connect timeout — and three separate places waited
    on the slowest peer:

      1. the vote count, which tallied only after every RequestVote returned,
         so a granted vote arrived stale and was discarded;
      2. the leader's heartbeat, paced by `gather` over all followers, so
         150 ms beats became 2 s ones and healthy followers deposed a healthy
         leader;
      3. the election itself, awaited on the timer loop, so a node that won in
         milliseconds sent nothing for two more seconds.

    Each is invisible with an instant-failure transport, and together they
    left two live brokers out of three permanently unable to keep a leader.

    Hence the second half of the assertion. Electing once is easy; the bug was
    that leadership then flapped forever, so the test insists the SAME node
    still leads several election timeouts later.
    """
    network, nodes = await build_cluster(tmp_path, 3, election_timeout_ms=200.0,
                                         heartbeat_ms=40.0)
    # A dead peer answers no faster than a real one: several heartbeats' worth.
    network.down_delay = 0.4

    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        victim = leaders(nodes)[0]

        await victim.stop()
        network.kill(victim.node_id)
        survivors = [n for n in nodes if n is not victim]

        assert await wait_for(lambda: len(leaders(survivors)) == 1), \
            "two of three voters are a majority and must be able to elect"
        elected = leaders(survivors)[0]
        term = elected.current_term

        # Stay up for well over an election timeout without being deposed.
        await asyncio.sleep(1.5)

        assert leaders(survivors) == [elected], \
            "leadership flapped: the survivor was deposed by its own peer"
        assert elected.current_term == term, \
            f"term climbed {term} → {elected.current_term}: elections still running"
        assert elected.leader_id == elected.node_id
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_lone_voter_elects_itself(tmp_path):
    """
    A one-node quorum is its own majority and must not wait for anybody.

    Worth its own test because it is the case with no peers at all, and so the
    case that incremental vote counting can silently skip: if promotion only
    happens when a RequestVote reply lands, a lone voter is never promoted and
    the broker never becomes a controller. Every single-broker test in the
    project fails at once — which is how this was caught, but only because
    single-broker tests exist. Quorum sizes of one are real: it is what a
    development cluster and most of this test suite run.
    """
    network, nodes = await build_cluster(tmp_path, 1, election_timeout_ms=100.0,
                                         heartbeat_ms=20.0)
    try:
        assert await wait_for(lambda: nodes[0].role == LEADER)
        assert nodes[0].votes_needed == 1
        assert await nodes[0].propose({'type': 'CREATE_TOPIC',
                                       'name': 'solo', 'partitions': 1})
    finally:
        for node in nodes:
            await node.stop()


# ── Persistence ────────────────────────────────────────────────

def test_term_and_vote_survive_restart(tmp_path):
    """
    Both fields must be durable or the safety proof collapses. A node that
    forgets its term accepts a stale leader; one that forgets its vote can vote
    twice in a term, and two candidates both reach a majority.
    """
    state = PersistentState(str(tmp_path))
    state.set_term(7, voted_for=3)

    reloaded = PersistentState(str(tmp_path))
    assert reloaded.current_term == 7
    assert reloaded.voted_for == 3


def test_a_new_term_clears_the_vote(tmp_path):
    """
    `votedFor` is scoped to a term. Carrying it forward would make the node
    refuse a legitimate candidate in the new term — a node that silently never
    votes again.
    """
    state = PersistentState(str(tmp_path))
    state.set_term(5, voted_for=2)
    state.set_term(6)

    assert state.voted_for is None
    assert state.can_vote_for(9)


def test_a_node_will_not_vote_twice_in_one_term(tmp_path):
    state = PersistentState(str(tmp_path))
    state.set_term(4)
    state.record_vote(1)

    assert state.can_vote_for(1), "re-voting for the SAME candidate is allowed"
    assert not state.can_vote_for(2), "voting for a second candidate is not"


def test_unreadable_state_refuses_to_start(tmp_path):
    """
    Guessing term 0 would let a stale leader win. A node that cannot prove what
    it has already promised must not make new promises.
    """
    state = PersistentState(str(tmp_path))
    state.set_term(3, voted_for=1)
    with open(state.path, 'w', encoding='utf-8') as fh:
        fh.write('{ not json')

    with pytest.raises(RuntimeError, match='cannot safely rejoin'):
        PersistentState(str(tmp_path))


def test_log_entries_survive_restart(tmp_path):
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(1, {'type': 'A'})
    raft_log.append(1, {'type': 'B'})
    raft_log.append(2, {'type': 'C'})
    raft_log.close()

    reloaded = RaftLog(str(tmp_path))
    assert reloaded.last_index == 3
    assert reloaded.last_term == 2
    assert reloaded.entry_at(2).command['type'] == 'B'


def test_a_torn_log_entry_is_dropped(tmp_path):
    """
    Reuses Phase 2's CRC framing, so a crash mid-append costs the partial entry
    and nothing before it. Safe by construction: an unacknowledged entry cannot
    have been counted toward any majority.
    """
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(1, {'type': 'GOOD'})
    raft_log.close()

    with open(raft_log.path, 'ab') as fh:
        fh.write(b'\x00\x00\x00\x40 half a record that never finished')

    reloaded = RaftLog(str(tmp_path))
    assert reloaded.last_index == 1
    assert reloaded.entry_at(1).command['type'] == 'GOOD'


# ── The election restriction ───────────────────────────────────

def test_a_stale_log_is_not_up_to_date(tmp_path):
    """
    THE rule that makes committed entries permanent: a candidate missing
    committed data cannot win, so every new leader already holds every
    committed entry.

    Term is compared BEFORE length. A longer log is not automatically better —
    it may be full of uncommitted entries from a term that never committed.
    """
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(2, {'type': 'A'})
    raft_log.append(2, {'type': 'B'})       # last = (index 2, term 2)

    assert raft_log.is_up_to_date(2, 2), "identical is up to date"
    assert raft_log.is_up_to_date(3, 2), "longer, same term"
    assert raft_log.is_up_to_date(1, 3), "SHORTER but a later term wins"
    assert not raft_log.is_up_to_date(1, 2), "shorter, same term"
    assert not raft_log.is_up_to_date(9, 1), "much longer but an older term"


def test_log_matching_detects_divergence(tmp_path):
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(1, {'type': 'A'})
    raft_log.append(2, {'type': 'B'})

    assert raft_log.matches(2, 2)
    assert not raft_log.matches(2, 1), "right index, wrong term"
    assert not raft_log.matches(5, 1), "past the end"


def test_conflicting_entries_are_truncated(tmp_path):
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(1, {'type': 'A'})
    raft_log.append(1, {'type': 'B'})
    raft_log.append(1, {'type': 'C'})

    # A leader from term 2 says index 2 is something else.
    raft_log.append_entries([LogEntry(2, 2, {'type': 'B-PRIME'})])

    assert raft_log.last_index == 2
    assert raft_log.entry_at(2).command['type'] == 'B-PRIME'


def test_duplicate_entries_do_not_truncate(tmp_path):
    """
    A retransmitted AppendEntries commonly repeats entries we already hold.
    Blanket-truncating on a duplicate would delete entries the leader believes
    are safely stored — possibly a COMMITTED one — turning a redundant network
    retry into data loss.
    """
    raft_log = RaftLog(str(tmp_path))
    raft_log.append(1, {'type': 'A'})
    raft_log.append(1, {'type': 'B'})

    raft_log.append_entries([LogEntry(1, 1, {'type': 'A'}),
                             LogEntry(2, 1, {'type': 'B'}),
                             LogEntry(3, 1, {'type': 'C'})])

    assert raft_log.last_index == 3
    assert [raft_log.entry_at(i).command['type'] for i in (1, 2, 3)] == \
        ['A', 'B', 'C']


# ── Election ───────────────────────────────────────────────────

@async_test
async def test_a_cluster_elects_exactly_one_leader(tmp_path):
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        assert len(leaders(nodes)) == 1
        elected = leaders(nodes)[0]
        assert await wait_for(
            lambda: all(n.leader_id == elected.node_id for n in nodes))
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_new_leader_is_elected_when_the_old_one_is_isolated(tmp_path):
    network, nodes = await build_cluster(tmp_path, 5)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        old = leaders(nodes)[0]

        network.isolate(old.node_id)

        survivors = [n for n in nodes if n.node_id != old.node_id]
        assert await wait_for(lambda: len(leaders(survivors)) == 1, timeout=10)

        new = leaders(survivors)[0]
        assert new.node_id != old.node_id
        assert new.current_term > old.current_term, "a new term for a new leader"
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_minority_cannot_elect(tmp_path):
    """
    **THE Phase 6 property, and the whole reason for this phase.**

    Phase 5's `min(live_broker_ids)` gave two controllers under this exact
    partition. Raft gives one, because a majority of five is three and two
    disjoint majorities of the same set cannot exist.

    The minority is not merely slower — it is mathematically unable to elect,
    no matter how long it tries or what it believes about the others.
    """
    network, nodes = await build_cluster(tmp_path, 5)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)

        minority_ids = {1, 2}
        majority_ids = {3, 4, 5}
        network.partition(minority_ids, majority_ids)

        minority = [n for n in nodes if n.node_id in minority_ids]
        majority = [n for n in nodes if n.node_id in majority_ids]

        # Give both sides plenty of time to campaign.
        assert await wait_for(lambda: len(leaders(majority)) == 1, timeout=10)
        await asyncio.sleep(1.5)

        assert len(leaders(majority)) == 1, "the majority side elects"
        assert leaders(minority) == [], (
            "the MINORITY must not elect — this is the entire point")

        # Not-leader is the property; follower-vs-candidate is a timing
        # detail. A minority node oscillates: it campaigns, fails to reach a
        # majority, times out, campaigns again — and a node just demoted by
        # check-quorum is briefly a follower before its timer fires. Asserting
        # one specific state would be testing the clock, not the algorithm.
        assert all(n.role != LEADER for n in minority)

        # And it must be *trying*: terms climb as it campaigns. A minority that
        # went quiet would also pass the check above while being broken in a
        # completely different way.
        assert any(n.current_term > 1 for n in minority), (
            "the minority should still be campaigning, just never winning")
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_the_minority_rejoins_and_steps_down_after_healing(tmp_path):
    network, nodes = await build_cluster(tmp_path, 5)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        network.partition({1, 2}, {3, 4, 5})
        majority = [n for n in nodes if n.node_id in {3, 4, 5}]
        assert await wait_for(lambda: len(leaders(majority)) == 1, timeout=10)
        await asyncio.sleep(1.0)

        network.heal()

        # Wait for term convergence, not just for a leader to exist.
        #
        # These are different moments, and asserting the second implies the
        # first only by luck. A leader is elected as soon as a majority votes;
        # the rejoining minority does not learn the new term until the leader's
        # next heartbeat reaches it. Checking immediately after
        # `len(leaders) == 1` was testing a race, and it lost.
        converged = await wait_for(
            lambda: len({n.current_term for n in nodes}) == 1
            and len(leaders(nodes)) == 1, timeout=15)

        terms = {n.current_term for n in nodes}
        assert converged, f"terms did not converge: {sorted(terms)}"
        assert len(leaders(nodes)) == 1, "exactly one leader after healing"
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_higher_term_makes_a_leader_step_down(tmp_path):
    """
    The rule that outranks every other rule. Phase 5's controller had no
    equivalent — a stale controller had no way to learn it was stale, which is
    exactly how split-brain persisted.
    """
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]

        await leader.handle_append_entries({
            'term': leader.current_term + 5, 'leader_id': 99,
            'prev_log_index': 0, 'prev_log_term': 0,
            'entries': [], 'leader_commit': 0,
        })

        assert leader.role == FOLLOWER
        assert leader.leader_id == 99
    finally:
        for node in nodes:
            await node.stop()


# ── Replication and commit ─────────────────────────────────────

@async_test
async def test_a_committed_command_reaches_every_node(tmp_path):
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]

        assert await leader.propose(
            registration_command(7, '127.0.0.1', 9099))

        assert await wait_for(
            lambda: all(7 in n.machine.brokers for n in nodes), timeout=8)
        for node in nodes:
            assert node.machine.brokers[7]['port'] == 9099
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_non_leader_refuses_to_propose(tmp_path):
    """
    Returning False rather than forwarding is deliberate: the caller knows the
    leader (`leader_id`) and can redirect, and a silent forward would hide the
    extra hop.
    """
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        follower = next(n for n in nodes if n.role != LEADER)
        assert await follower.propose({'type': 'CREATE_TOPIC',
                                       'name': 'x', 'partitions': 1}) is False
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_minority_cannot_commit(tmp_path):
    """
    Commitment needs a majority too, not just election. A partitioned leader
    that keeps accepting proposals would be exactly the split-brain we are
    preventing — one step later in the pipeline.
    """
    network, nodes = await build_cluster(tmp_path, 5)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]

        network.isolate(leader.node_id)
        before = leader.commit_index

        committed = await leader.propose({'type': 'CREATE_TOPIC',
                                          'name': 'orphan', 'partitions': 1},
                                         timeout=1.5)

        assert committed is False
        assert leader.commit_index == before, "nothing commits without a majority"
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_a_lagging_follower_catches_up(tmp_path):
    """
    A follower isolated during several commits must receive all of them when it
    returns — the leader backs `next_index` up until the logs match, then ships
    the difference.
    """
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]
        lagging = next(n for n in nodes if n.node_id != leader.node_id)

        network.isolate(lagging.node_id)

        for i in range(4):
            await leader.propose({'type': 'CREATE_TOPIC',
                                  'name': f"t{i}", 'partitions': 2})

        assert lagging.log.last_index < leader.log.last_index

        network.heal()
        assert await wait_for(
            lambda: lagging.machine.topics == leader.machine.topics, timeout=10)
    finally:
        for node in nodes:
            await node.stop()


@async_test
async def test_the_commit_rule_requires_an_entry_from_the_current_term(tmp_path):
    """
    Figure 8 of the Raft paper, and the rule most often omitted.

    A leader may only commit by counting replicas for an entry from its OWN
    term. Without that check, an entry replicated to a majority can still be
    overwritten by a later leader — committed data disappearing.
    """
    network, nodes = await build_cluster(tmp_path, 3)
    try:
        assert await wait_for(lambda: len(leaders(nodes)) == 1)
        leader = leaders(nodes)[0]

        # Force the term forward so an "earlier term" definitely exists.
        # Depending on which term the election happened to land in would make
        # this test skip itself — and a safety test that silently does not run
        # is worse than no test, because the suite still reports green.
        old_term = leader.current_term
        leader.state.set_term(old_term + 3, voted_for=leader.node_id)

        # Hand-place an entry from that OLD term, replicated to everyone.
        leader.log.append(old_term, {'type': 'STALE'})
        index = leader.log.last_index
        for voter in leader.voters:
            leader.match_index[voter] = index

        before = leader.commit_index
        leader._advance_commit_index()

        assert leader.commit_index == before, (
            "an old-term entry must NOT be committed by counting replicas")
    finally:
        for node in nodes:
            await node.stop()


# ── The state machine ──────────────────────────────────────────

def test_applying_is_deterministic_and_ordered():
    """
    Every node applies the same commands in the same order and reaches the same
    state. That is the whole construction — and it is why `apply()` may not
    read a clock, a file, or anything local.
    """
    commands = [
        registration_command(1, 'h', 9001),
        {'type': 'CREATE_TOPIC', 'name': 'orders', 'partitions': 4},
        registration_command(2, 'h', 9002),
        {'type': 'ASSIGN_PARTITIONS',
         'leaders': [{'topic': 'orders', 'partition': 0, 'leader': 1},
                     {'topic': 'orders', 'partition': 1, 'leader': 2}]},
        {'type': 'UNREGISTER_BROKER', 'node_id': 1},
    ]

    first, second = MetadataStateMachine(), MetadataStateMachine()
    for i, command in enumerate(commands, start=1):
        entry = LogEntry(i, 1, command)
        first.apply(entry)
        second.apply(entry)

    assert first.snapshot() == second.snapshot()
    assert first.brokers == {2: {'host': 'h', 'port': 9002,
                                 'registered_at': first.brokers[2]['registered_at']}}
    assert first.topics == {'orders': 4}
    assert first.leader_for('orders', 1) == 2


def test_unknown_commands_are_ignored_not_fatal():
    """
    A newer node may write a command an older one does not understand.
    Refusing to apply the rest of the log would drop the older node out of the
    quorum during a rolling upgrade.
    """
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'FROM_THE_FUTURE', 'x': 1}))
    machine.apply(LogEntry(2, 1, {'type': 'CREATE_TOPIC',
                                  'name': 'ok', 'partitions': 2}))
    assert machine.topics == {'ok': 2}
    assert machine.last_applied == 2


def test_topic_width_only_ever_grows():
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC',
                                  'name': 't', 'partitions': 6}))
    machine.apply(LogEntry(2, 1, {'type': 'CREATE_TOPIC',
                                  'name': 't', 'partitions': 2}))
    assert machine.topics['t'] == 6, "a narrowing must be refused"


def test_placement_is_computed_by_the_leader_and_is_deterministic():
    """
    The DECISION is replicated, not the inputs. Recomputing during apply()
    would let two nodes with different views of liveness reach different states
    from the same log.
    """
    machine = MetadataStateMachine()
    machine.apply(LogEntry(1, 1, {'type': 'CREATE_TOPIC',
                                  'name': 'orders', 'partitions': 6}))
    for node_id in (1, 2, 3):
        machine.apply(LogEntry(node_id + 1, 1,
                               registration_command(node_id, 'h', 9000 + node_id)))

    first = machine.compute_assignment()
    second = machine.compute_assignment()
    assert first == second

    counts = {}
    for item in first:
        counts[item['leader']] = counts.get(item['leader'], 0) + 1
    assert counts == {1: 2, 2: 2, 3: 2}
