"""
CONCEPT: The Raft Node — Three States and Two RPCs
-----------------------------------------------------
                    ┌──────────────┐
        starts up   │              │  discovers a leader
       ───────────► │   FOLLOWER   │ ◄──────────────────────┐
                    │              │                        │
                    └──────┬───────┘                        │
                           │ no heartbeat within            │
                           │ election_timeout               │
                           ▼                                │
                    ┌──────────────┐   sees a higher term   │
                    │  CANDIDATE   │ ───────────────────────┤
                    └──────┬───────┘                        │
                           │ votes from a MAJORITY          │
                           ▼                                │
                    ┌──────────────┐                        │
                    │    LEADER    │ ───────────────────────┘
                    └──────────────┘

Two RPCs drive all of it: `RequestVote` and `AppendEntries`. `AppendEntries`
with no entries is the heartbeat — replication and liveness are the same
message, so a leader cannot appear alive while failing to replicate.


THE RULE THAT OUTRANKS EVERY OTHER RULE
-----------------------------------------
    If any message carries a term higher than mine — request or response —
    I adopt that term and become a follower, whatever I was doing.

It is checked first in every handler. A leader from term 4 that wakes into
term 7 discovers it from the first message it sees and steps down immediately.
Phase 5's controller had no equivalent: a stale controller had no way to learn
it was stale, which is exactly how split-brain persisted.


WHY A MINORITY CANNOT ELECT
-----------------------------
    votes_needed = len(voters) // 2 + 1

A **majority of all voters**, not of those reachable. Two disjoint majorities
of the same set cannot exist, so two leaders in one term cannot exist. With
five voters, a partitioned side holding two can never reach three no matter
what it believes about the others.

That is arithmetic, not best effort, and it is the entire difference from
Phase 5.

The price is availability: the minority side stops serving even though its
machines are healthy. **Raft chooses consistency**, and `chaos/split_brain.py`
now shows the minority going quiet instead of inventing a second controller.


WHY ELECTION TIMEOUTS ARE RANDOMISED
--------------------------------------
If every node used the same timeout they would all become candidates at the
same instant, split the vote evenly, and time out together — forever. A
randomised timeout means somebody almost always goes first and wins before the
others wake. It is a probabilistic fix to a real liveness problem, and Raft
depends on it.
"""

import asyncio
import logging
import random

from broker.raft.log import LogEntry, RaftLog
from broker.raft.machine import MetadataStateMachine
from broker.raft.snapshot import SnapshotStore
from broker.raft.state import PersistentState
from common.config import DEFAULT_RAFT_SNAPSHOT_THRESHOLD

log = logging.getLogger(__name__)

FOLLOWER = 'follower'
CANDIDATE = 'candidate'
LEADER = 'leader'


class RaftNode:
    """
    One participant in the controller quorum.

    Transport is injected: `send_rpc(node_id, api_key, payload) -> dict | None`
    returning None if the peer is unreachable. Keeping the network out of this
    class is what lets the tests drive elections and partitions in memory,
    deterministically, with no sockets at all.
    """

    def __init__(self, node_id: int, voters: list, directory: str, send_rpc,
                 election_timeout_ms: float = 1000.0,
                 heartbeat_interval_ms: float = 150.0,
                 snapshot_threshold: int = DEFAULT_RAFT_SNAPSHOT_THRESHOLD):
        self.node_id = node_id
        # Every voter, including ourselves. The majority is computed from this
        # fixed set — never from who happens to be reachable.
        self.voters = sorted(voters)
        self.send_rpc = send_rpc

        self.election_timeout_ms = election_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.snapshot_threshold = snapshot_threshold

        self.state = PersistentState(directory)
        self.machine = MetadataStateMachine()

        self.role = FOLLOWER
        self.leader_id = None

        # Volatile: rebuilt from the log after a restart, because the log is
        # the durable part and these are derived from it.
        self.commit_index = 0
        self.last_applied = 0

        # CONCEPT: the snapshot loads BEFORE the log, and that order is forced.
        #
        # The log cannot construct itself without knowing where its indexing
        # starts, and the only record of that is inside the snapshot. So
        # recovery reads the answer first and replays only the tail — which is
        # why real systems restart in seconds instead of in proportion to how
        # long they have been up.
        self.snapshots = SnapshotStore(directory)
        snapshot = self.snapshots.load()
        snapshot_index = snapshot['last_index'] if snapshot else 0
        snapshot_term = snapshot['last_term'] if snapshot else 0

        if snapshot is not None:
            self.machine.restore(snapshot['state'])
            # Safe by construction: a snapshot only ever covers APPLIED
            # entries, entries are only applied once COMMITTED, and committed
            # is permanent. So an index inside the snapshot is committed, and
            # saying so costs us nothing we would otherwise re-derive.
            self.commit_index = snapshot_index
            self.last_applied = snapshot_index
            log.info("Node %s restored snapshot at index %d (term %d)",
                     node_id, snapshot_index, snapshot_term)

        self.log = RaftLog(directory, snapshot_last_index=snapshot_index,
                           snapshot_last_term=snapshot_term)

        # Leader-only, reset on election.
        self.next_index = {}
        self.match_index = {}

        self._deadline = 0.0
        self._votes = set()

        # When each peer last answered us. Used by check-quorum: a leader that
        # cannot reach a majority demotes itself instead of insisting it is
        # still in charge.
        self._last_contact = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._tasks = set()
        # Peers with an AppendEntries still in flight. A slow or dead follower
        # must not accumulate a queue of identical retries behind itself.
        self._inflight = set()

    # ── Properties ─────────────────────────────────────────────

    @property
    def current_term(self) -> int:
        return self.state.current_term

    @property
    def is_leader(self) -> bool:
        return self.role == LEADER

    @property
    def votes_needed(self) -> int:
        return len(self.voters) // 2 + 1

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._reset_election_timer()
        self._spawn(self._run())

    async def stop(self) -> None:
        self._running = False
        for task in list(self._tasks):
            task.cancel()
        self._tasks.clear()
        self.log.close()

    def _spawn(self, coro) -> None:
        # asyncio holds only a weak reference to tasks, so a fire-and-forget
        # task with no strong reference can be collected mid-flight.
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run(self) -> None:
        """The single timer loop: election timeout when idle, heartbeat when leader."""
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)

                if self.role == LEADER:
                    await self._heartbeat_tick()
                elif asyncio.get_running_loop().time() >= self._deadline:
                    await self._begin_election()

            except asyncio.CancelledError:
                return
            except Exception:
                # A bug in one tick must not kill the node's participation in
                # the quorum — a silently stopped Raft node looks exactly like
                # a network partition to everyone else.
                log.exception("Raft tick failed on node %s", self.node_id)

    def _reset_election_timer(self) -> None:
        """
        Arm the election timeout with a random offset.

        The randomisation is not decoration: identical timeouts make every node
        become a candidate simultaneously, split the vote, and time out
        together — a livelock that repeats indefinitely.
        """
        timeout = self.election_timeout_ms * (1.0 + random.random())
        self._deadline = asyncio.get_running_loop().time() + timeout / 1000.0

    # ── Term handling — the rule above all others ──────────────

    def _observe_term(self, term: int) -> bool:
        """
        If `term` is newer than ours, adopt it and step down. True if we did.

        Called at the top of every handler and on every response. This is the
        mechanism by which a stale leader discovers it is stale.
        """
        if term > self.current_term:
            was = self.role
            self.state.set_term(term)          # durable BEFORE we act on it
            self.role = FOLLOWER
            self.leader_id = None
            self._votes.clear()
            if was != FOLLOWER:
                log.info("Node %s stepped down: saw term %d (was %s)",
                         self.node_id, term, was)
            return True
        return False

    # ── Election ───────────────────────────────────────────────

    async def _begin_election(self) -> None:
        """Become a candidate for the next term and canvass every voter."""
        async with self._lock:
            self.role = CANDIDATE
            self.leader_id = None
            # Increment the term and vote for ourselves — durably, before
            # asking anyone else. Sending a RequestVote we have not recorded
            # would let a crash-restart vote again in the same term.
            self.state.set_term(self.current_term + 1, voted_for=self.node_id)
            self._votes = {self.node_id}
            self._reset_election_timer()

            term = self.current_term
            payload = {
                'term': term,
                'candidate_id': self.node_id,
                'last_log_index': self.log.last_index,
                'last_log_term': self.log.last_term,
            }

        log.info("Node %s starting election for term %d (needs %d of %d votes)",
                 self.node_id, term, self.votes_needed, len(self.voters))

        # A one-voter quorum is its own majority, so our own vote already
        # settles it and there is nobody to canvass. Not a special case in the
        # arithmetic — `votes_needed` is 1 — but it does have to be CHECKED
        # here, because the only other place that checks is the response
        # handler, and a single voter never receives a response.
        async with self._lock:
            if self.role == CANDIDATE and self.current_term == term \
                    and len(self._votes) >= self.votes_needed:
                self._become_leader()
                return

        # CONCEPT: count each vote AS IT ARRIVES. Never wait for all of them.
        #
        # This was a real bug, and it was invisible until a broker actually
        # died. The first version gathered every RequestVote and tallied the
        # results together — which sounds equivalent and is not, because
        # `gather` finishes at the pace of the SLOWEST peer, and a dead peer
        # is the slowest possible peer: it costs the full RPC timeout.
        #
        # Kill the leader of three brokers and both survivors would sit for
        # two seconds inside `gather`, waiting on a corpse, while their own
        # election timers (1–2s) expired underneath them. The other survivor
        # would then campaign for a higher term, and when `gather` finally
        # returned, `self.current_term != term` threw away a vote that had
        # been GRANTED. Both nodes did this to each other, in lockstep, for as
        # long as you cared to watch: terms climbing, no leader, ever.
        #
        # Note that the cluster was not merely slow — it was permanently
        # leaderless with a healthy majority available. The single most
        # dangerous kind of bug: correct on the happy path, correct in every
        # unit test with an in-memory transport where nothing is slow, and
        # fatal the first time real hardware failed.
        # And do not await the canvass either. `_collect_vote` promotes us the
        # moment a majority exists, so there is nothing left to wait FOR — and
        # waiting is actively harmful, because this runs on the timer loop.
        #
        # That was the last hiding place of the same bug. Even after votes were
        # counted incrementally, the gather here kept `_run` parked for the
        # dead peer's full RPC timeout. The node would win the election in
        # ~5 ms and then not send its first heartbeat for two more seconds,
        # by which time the follower had timed out and started its own
        # election. Leadership flapped once a second forever, with a healthy
        # majority and no node at fault — and every single unit test passed,
        # because an in-memory transport has no dead peers and no timeouts.
        for peer in self.voters:
            if peer != self.node_id:
                self._spawn(self._collect_vote(peer, payload, term))

    async def _collect_vote(self, peer: int, payload: dict, term: int) -> None:
        """Ask one voter, and promote ourselves the instant a majority exists."""
        response = await self.send_rpc(peer, 'REQUEST_VOTE', payload)
        if not isinstance(response, dict):
            return

        async with self._lock:
            if self.role != CANDIDATE or self.current_term != term:
                return                          # superseded while we canvassed
            if self._observe_term(int(response.get('term', 0))):
                return                          # somebody is ahead of us
            if not response.get('vote_granted'):
                return

            self._votes.add(peer)
            if len(self._votes) >= self.votes_needed:
                self._become_leader()

    def _check_quorum(self) -> None:
        """
        Step down if a majority has not answered within an election timeout.

        CONCEPT: basic Raft is SAFE here but not honest, and the difference
        matters to an operator.

          Nothing in the Raft paper makes a partitioned leader step down. It
          stays leader until it hears a higher term — which never arrives,
          because it cannot hear anything. That is perfectly safe: it can no
          longer COMMIT, since commitment needs a majority, so it cannot
          corrupt anything.

          But it still *reports* itself as leader. Which is precisely the
          confusing symptom Phase 5 had: a node insisting it is the controller
          while a real one exists elsewhere. Safe, and indistinguishable from
          broken when you are looking at a dashboard at 3am.

        Check-quorum closes that gap: a leader that cannot reach a majority
        demotes itself. etcd and Kafka's KRaft both do this, and it is why
        `chaos/split_brain.py` can now assert that the minority side goes
        quiet rather than merely failing to commit.

        Note what it does NOT change: safety was never at risk. This buys
        clarity, not correctness — which is a legitimate thing to spend code
        on, and worth being clear about.
        """
        if self.role != LEADER:
            return

        now = asyncio.get_running_loop().time()
        window = self.election_timeout_ms / 1000.0

        reachable = 1                           # ourselves, trivially
        for peer in self.voters:
            if peer == self.node_id:
                continue
            if now - self._last_contact.get(peer, 0.0) <= window:
                reachable += 1

        if reachable < self.votes_needed:
            log.info("Node %s stepping down: only %d of %d voters reachable, "
                     "needs %d", self.node_id, reachable, len(self.voters),
                     self.votes_needed)
            self.role = FOLLOWER
            self.leader_id = None
            self._reset_election_timer()

    def _become_leader(self) -> None:
        self.role = LEADER
        self.leader_id = self.node_id

        # Start the contact clock now, or check-quorum would demote us on the
        # very first tick for never having heard from anyone yet.
        now = asyncio.get_running_loop().time()
        self._last_contact = {v: now for v in self.voters}

        # Optimistic: assume every follower matches us, and back off on
        # rejection. The alternative (assume nothing) would make every new
        # leader re-send the entire log to everyone.
        self.next_index = {v: self.log.last_index + 1 for v in self.voters}
        self.match_index = {v: 0 for v in self.voters}
        self.match_index[self.node_id] = self.log.last_index

        log.info("Node %s became LEADER for term %d with %d votes",
                 self.node_id, self.current_term, len(self._votes))

        # Heartbeat immediately, so followers stop their election timers before
        # one of them decides to challenge us.
        self._spawn(self._replicate_to_all())

    # ── RPC handlers ───────────────────────────────────────────

    async def handle_request_vote(self, payload: dict) -> dict:
        """Grant a vote only if the term is current AND the log is current."""
        async with self._lock:
            term = int(payload['term'])
            candidate = int(payload['candidate_id'])

            if term < self.current_term:
                return {'term': self.current_term, 'vote_granted': False}

            self._observe_term(term)

            up_to_date = self.log.is_up_to_date(int(payload['last_log_index']),
                                                int(payload['last_log_term']))
            granted = self.state.can_vote_for(candidate) and up_to_date

            if granted:
                # Durable BEFORE replying. Reply first and crash before the
                # write, and we have granted a vote nobody remembers — which is
                # a double vote with an extra step.
                self.state.record_vote(candidate)
                self._reset_election_timer()

            return {'term': self.current_term, 'vote_granted': granted}

    async def handle_append_entries(self, payload: dict) -> dict:
        """
        Heartbeat and replication. Also how a follower learns who leads.

        `match_index` is returned on success so the leader can advance
        `next_index` exactly, rather than guessing from what it sent — which
        matters when a retransmission overlaps entries the follower already had.
        """
        async with self._lock:
            term = int(payload['term'])

            if term < self.current_term:
                return {'term': self.current_term, 'success': False,
                        'match_index': 0}

            self._observe_term(term)

            # A live leader for our term: stand down if we were campaigning,
            # and stop our own election timer.
            self.role = FOLLOWER
            self.leader_id = int(payload['leader_id'])
            self._reset_election_timer()

            prev_index = int(payload['prev_log_index'])
            prev_term = int(payload['prev_log_term'])

            if prev_index > 0 and not self.log.matches(prev_index, prev_term):
                # Our histories diverge before this point. Rejecting makes the
                # leader back up and try an earlier index until they meet — the
                # Log Matching Property doing the work.
                return {'term': self.current_term, 'success': False,
                        'match_index': 0, 'last_index': self.log.last_index}

            entries = [LogEntry.from_dict(e) for e in payload.get('entries', [])]
            if entries:
                self.log.append_entries(entries)

            leader_commit = int(payload.get('leader_commit', 0))
            if leader_commit > self.commit_index:
                # min(): never claim to have committed past what we actually
                # hold. The leader may be ahead of the entries it just sent us.
                self.commit_index = min(leader_commit, self.log.last_index)
                self._apply_committed()

            return {'term': self.current_term, 'success': True,
                    'match_index': self.log.last_index}

    # ── Replication ────────────────────────────────────────────

    async def _heartbeat_tick(self) -> None:
        """
        The leader's periodic beat, which must NOT wait for anybody.

        CONCEPT: heartbeat cadence has to be a property of the LEADER, never
        of its slowest follower.

          `_replicate_to_all` awaits every peer, and that is right when
          `propose` needs to know whether an entry committed. It is wrong on
          the timer, because one unreachable follower then costs the full RPC
          timeout every tick — so a 150 ms heartbeat silently becomes a 2 s
          heartbeat, the surviving followers' 1–2 s election timers expire,
          and they depose a leader that is perfectly healthy.

          The symptom is nasty: leadership flapping every couple of seconds
          with no node at fault, in a cluster that has a clear majority. Raft
          stays SAFE throughout — never two leaders — but nothing gets
          committed, because each new leader is deposed before it finishes.

        So the timer fires replication off and returns immediately, and a peer
        with a request still in flight is skipped rather than queued behind
        itself.
        """
        for peer in self.voters:
            if peer == self.node_id or peer in self._inflight:
                continue
            self._spawn(self._replicate_to(peer))

        async with self._lock:
            self._check_quorum()
            self._advance_commit_index()

    async def _replicate_to_all(self) -> None:
        """Replicate and WAIT — used by `propose`, which needs the outcome."""
        peers = [v for v in self.voters if v != self.node_id]
        if peers:
            await asyncio.gather(*(self._replicate_to(p) for p in peers),
                                 return_exceptions=True)
        async with self._lock:
            self._check_quorum()
            self._advance_commit_index()

    async def _replicate_to(self, peer: int) -> None:
        if peer in self._inflight:
            return
        self._inflight.add(peer)
        try:
            await self._replicate_once(peer)
        finally:
            self._inflight.discard(peer)

    async def _replicate_once(self, peer: int) -> None:
        async with self._lock:
            if self.role != LEADER:
                return
            next_index = self.next_index.get(peer, self.log.last_index + 1)

            # CONCEPT: compaction creates a failure that could not exist before
            # — a follower needing an entry we have already thrown away.
            #
            #   leader    snapshot @ 40 │ 41 42 43 44
            #   follower                       ▲ next_index = 12
            #
            # We cannot build a legal message for index 12: `prev_log_index`
            # 11 is inside our snapshot, and there is no `prev_log_term` to
            # send. The follower would reject, we would back up, and the two
            # of us would loop forever.
            #
            # So we stop sending history and send the RESULT instead. One
            # round trip turns an unrecoverable follower into an ordinary one:
            # it adopts the state, its next_index lands back inside our log,
            # and the next AppendEntries is a normal one.
            needs_snapshot = next_index < self.log.first_index
            prev_index = next_index - 1
            payload = {
                'term': self.current_term,
                'leader_id': self.node_id,
                'prev_log_index': prev_index,
                'prev_log_term': self.log.term_at(prev_index),
                'entries': [e.as_dict() for e in self.log.entries_from(next_index)],
                'leader_commit': self.commit_index,
            }
            term = self.current_term

        if needs_snapshot:
            await self._send_snapshot(peer, term)
            return

        response = await self.send_rpc(peer, 'APPEND_ENTRIES', payload)
        if not isinstance(response, dict):
            return                              # unreachable; retry next tick

        async with self._lock:
            if self._observe_term(int(response.get('term', 0))):
                return
            if self.role != LEADER or self.current_term != term:
                return

            # Any answer at all proves the peer is reachable, success or not.
            # Only counting successes would demote a leader whose follower is
            # merely behind — which is the normal case during catch-up.
            self._last_contact[peer] = asyncio.get_running_loop().time()

            if response.get('success'):
                self.match_index[peer] = int(response.get('match_index', 0))
                self.next_index[peer] = self.match_index[peer] + 1
            else:
                # Back up and retry. Decrementing by one is the paper's simple
                # version; jumping to the follower's reported last index
                # converges in one round instead of many when logs are far
                # apart, and is safe because we still re-check prev_log_term.
                reported = int(response.get('last_index', 0))
                self.next_index[peer] = max(
                    1, min(self.next_index.get(peer, 1) - 1, reported + 1))

    async def _send_snapshot(self, peer: int, term: int) -> None:
        """
        Send our persisted snapshot to a follower that has fallen off the log.

        We send the SAVED snapshot rather than building a fresh one from the
        current state machine, and the reason is that the two must agree with
        the log. `snapshots.save` and `log.compact_to` are called as a pair, so
        the stored snapshot is exactly the prefix our log no longer holds.
        Serialising the live state machine instead would produce a snapshot at
        `last_applied`, ahead of the compaction point, and the follower's
        `next_index` would land in a gap we never sent it.
        """
        async with self._lock:
            snapshot = self.snapshots.load()
            if snapshot is None:
                # `next_index < first_index` implies a compaction happened,
                # which implies a snapshot was saved first. Reaching here means
                # that pairing broke, and quietly retrying forever would hide
                # it — so say so.
                log.error("Node %s: peer %s needs a snapshot but none is "
                          "stored; the log was compacted without one",
                          self.node_id, peer)
                return

            payload = {
                'term': self.current_term,
                'leader_id': self.node_id,
                'last_index': snapshot['last_index'],
                'last_term': snapshot['last_term'],
                'state': snapshot['state'],
            }

        log.info("Node %s sending snapshot @%d to peer %s",
                 self.node_id, payload['last_index'], peer)

        response = await self.send_rpc(peer, 'INSTALL_SNAPSHOT', payload)
        if not isinstance(response, dict):
            return

        async with self._lock:
            if self._observe_term(int(response.get('term', 0))):
                return
            if self.role != LEADER or self.current_term != term:
                return

            self._last_contact[peer] = asyncio.get_running_loop().time()
            if not response.get('success'):
                return

            # The follower now holds everything through the snapshot, so its
            # next_index lands back inside our log and ordinary replication
            # resumes on the next tick.
            installed = int(response.get('match_index', payload['last_index']))
            self.match_index[peer] = max(self.match_index.get(peer, 0), installed)
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit_index()

    async def handle_install_snapshot(self, payload: dict) -> dict:
        """
        Adopt a leader's snapshot, discarding our own log.

        CONCEPT: this is the ONLY place in the system where a node throws away
        committed local state because a peer told it to. Every other
        destructive path — `truncate_from`, a vote, a term change — either
        touches uncommitted data or adds rather than removes.

        That is why the two guards below are not routine input validation.
        They are the entire fence around a destructive operation, and each one
        removed turns a duplicate or stale packet into permanent, silent
        divergence that only a comparison of two nodes would ever reveal.
        """
        async with self._lock:
            term = int(payload['term'])

            # Guard 1: the rule that outranks every other rule. A stale
            # leader's snapshot is refused exactly like its AppendEntries.
            if term < self.current_term:
                return {'term': self.current_term, 'success': False}

            self._observe_term(term)

            self.role = FOLLOWER
            self.leader_id = int(payload['leader_id'])
            # An InstallSnapshot is contact from a live leader, exactly like a
            # heartbeat. Forget this and a follower being caught up — which by
            # definition takes a while — starts an election against the leader
            # currently helping it.
            self._reset_election_timer()

            last_index = int(payload['last_index'])
            last_term = int(payload['last_term'])

            # Guard 2: never move backwards. An older snapshot can arrive
            # legitimately — a retransmission, or a slow message overtaken by
            # the AppendEntries behind it — and applying it would roll back
            # committed state. Answering success is correct: we hold
            # everything it contains and more.
            if last_index <= self.commit_index:
                return {'term': self.current_term, 'success': True,
                        'match_index': self.log.last_index}

            # Durable first, destructive second — the same ordering rule as
            # `_maybe_snapshot`, for the same reason.
            self.snapshots.save(last_index, last_term, payload['state'])
            self.machine.restore(payload['state'])
            dropped = self.log.reset_to_snapshot(last_index, last_term)

            self.commit_index = last_index
            self.last_applied = last_index

            log.info("Node %s installed snapshot @%d from leader %s, "
                     "dropped %d log entries",
                     self.node_id, last_index, self.leader_id, dropped)

            return {'term': self.current_term, 'success': True,
                    'match_index': last_index}

    def _advance_commit_index(self) -> None:
        """
        Commit the highest index replicated to a majority — **from our term**.

        The term check is the rule most often left out, and leaving it out is
        not a small bug: it is Figure 8 of the Raft paper, where an entry
        replicated to a majority is later overwritten by a different leader.

        A leader may only commit an entry from its OWN term by counting
        replicas. Earlier-term entries become committed indirectly, carried
        along when a later same-term entry commits.
        """
        if self.role != LEADER:
            return

        self.match_index[self.node_id] = self.log.last_index

        for index in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(index) != self.current_term:
                continue                        # ← the rule
            replicas = sum(1 for v in self.voters
                           if self.match_index.get(v, 0) >= index)
            if replicas >= self.votes_needed:
                self.commit_index = index
                self._apply_committed()
                return

    def _apply_committed(self) -> None:
        """Apply everything newly committed, in order, exactly once."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry_at(self.last_applied)
            if entry is not None:
                self.machine.apply(entry)

        self._maybe_snapshot()

    # ── Snapshotting ───────────────────────────────────────────

    def _maybe_snapshot(self) -> None:
        """
        Fold the applied prefix into a snapshot once enough has accumulated.

        CONCEPT: this needs no consensus, and that surprises people.

          Every node holds the same committed prefix — that is the guarantee
          Raft exists to provide — so every node can compact its own copy
          whenever it likes, with no RPC and no agreement. Two nodes carrying
          snapshots at different indices are not inconsistent; they have merely
          chosen to keep different amounts of history.

        `last_applied`, never `commit_index`. A snapshot is a picture of the
        state machine, and the state machine has only seen entries up to
        `last_applied`. Compacting to `commit_index` would claim to have
        applied things we have not, and the entries proving otherwise would be
        the ones we just deleted.

        Called under `self._lock` from `_apply_committed`.
        """
        if self.snapshot_threshold <= 0:
            return
        if self.last_applied - self.log.snapshot_last_index < self.snapshot_threshold:
            return

        index = self.last_applied
        term = self.log.term_at(index)

        # ORDER IS THE SAFETY ARGUMENT: durable snapshot first, destroy the log
        # prefix second. Reversed, a crash between the two loses both copies of
        # the same entries. `save()` returns only once it has fsynced, so the
        # second line cannot run early.
        self.snapshots.save(index, term, self.machine.snapshot())
        removed = self.log.compact_to(index)

        log.info("Node %s snapshotted at index %d (term %d), dropped %d entries",
                 self.node_id, index, term, removed)

    # ── Proposing ──────────────────────────────────────────────

    async def propose(self, command: dict, timeout: float = 5.0) -> bool:
        """
        Append a command and wait for it to commit. False if we are not leader.

        Returning False rather than forwarding is deliberate: the caller knows
        who the leader is (`leader_id`) and can redirect, and a silent forward
        would hide from the caller that its request took an extra hop.
        """
        async with self._lock:
            if self.role != LEADER:
                return False
            entry = self.log.append(self.current_term, command)
            self.match_index[self.node_id] = entry.index
            target = entry.index

        # A single-node quorum commits immediately: it is its own majority.
        # Not a special case in the arithmetic — votes_needed is 1 — but it
        # does mean there is nobody to wait for.
        await self._replicate_to_all()

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if self.commit_index >= target:
                return True
            if self.role != LEADER:
                return False                    # lost leadership mid-flight
            await asyncio.sleep(0.02)
            await self._replicate_to_all()

        return self.commit_index >= target

    # ── Introspection ──────────────────────────────────────────

    def status(self) -> dict:
        return {
            'node_id': self.node_id,
            'role': self.role,
            'term': self.current_term,
            'leader_id': self.leader_id,
            'voters': self.voters,
            'votes_needed': self.votes_needed,
            'log_last_index': self.log.last_index,
            'log_last_term': self.log.last_term,
            'log_first_index': self.log.first_index,
            'snapshot_last_index': self.log.snapshot_last_index,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'match_index': dict(self.match_index) if self.is_leader else {},
        }

    def __repr__(self) -> str:
        return (f"RaftNode({self.node_id}, {self.role}, term={self.current_term}, "
                f"log={self.log.last_index}, commit={self.commit_index})")
