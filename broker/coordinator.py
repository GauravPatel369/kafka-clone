"""
CONCEPT: The Group Coordinator
--------------------------------
The coordinator owns three things and deliberately not a fourth:

    membership     who is in this group right now
    generations    a counter that fences anyone who fell out of it
    liveness       heartbeats, and evicting members that stop sending them

    NOT the assignment. A client computes that.

That last line is the interesting design decision. The coordinator never
decides which member gets which partition — the group LEADER, an ordinary
client, computes it and uploads it. So you can ship a custom assignment
strategy (rack-aware, weighted, sticky) with your application and never touch a
broker.

**Membership and fencing are the broker's problem; policy is the client's.**
That split is why Kafka has many assignors and one coordinator.


THE STATE MACHINE
-------------------
        EMPTY
          │  first JOIN_GROUP
          ▼
   PREPARING_REBALANCE ◄──── a member joins, leaves, or times out
          │  every known member has rejoined, or the rebalance timeout fires
          ▼
   COMPLETING_REBALANCE
          │  the leader's SYNC_GROUP arrives with assignments
          ▼
        STABLE  ───────────► heartbeats are accepted here


WHY JOIN_GROUP WAITS
----------------------
A rebalance is a rendezvous: the coordinator cannot answer the first member
until it knows who else is joining. So `JOIN_GROUP` is HELD OPEN until every
known member has rejoined or `rebalance_timeout_ms` expires.

Real Kafka calls this holding area "purgatory". It is also the clearest
justification for an asyncio broker in this whole project: an awaited
`asyncio.Event` costs a few hundred bytes, while a thread-per-request server
would have one OS thread parked per member of every rebalancing group.


WHY GENERATIONS EXIST — the zombie problem
--------------------------------------------
    generation 7 : consumer A owns p0, commits offset 100
    consumer A pauses for a 40-second GC
    coordinator times A out → generation 8 → p0 reassigned to consumer B
    consumer B reads from 100, processes, commits 150
    consumer A wakes and commits 120, still believing it is generation 7
                                            ↑
                       REJECTED: ILLEGAL_GENERATION

Without that rejection, A's stale commit rewinds the group from 150 to 120 and
30 messages are silently reprocessed. The generation number is what makes "you
are no longer who you think you are" expressible at all — and a consumer cannot
detect this about itself, because from inside a GC pause no time has passed.
"""

import asyncio
import time
import uuid

from common import errors

# Group states
EMPTY = 'Empty'
PREPARING_REBALANCE = 'PreparingRebalance'
COMPLETING_REBALANCE = 'CompletingRebalance'
STABLE = 'Stable'

DEFAULT_SESSION_TIMEOUT_MS = 10_000
DEFAULT_REBALANCE_TIMEOUT_MS = 5_000


class Member:
    """One consumer in a group."""

    __slots__ = ('member_id', 'topics', 'session_timeout_ms',
                 'last_heartbeat', 'assignment')

    def __init__(self, member_id: str, topics: list, session_timeout_ms: int):
        self.member_id = member_id
        self.topics = list(topics)
        self.session_timeout_ms = session_timeout_ms
        # monotonic, not wall clock: this measures an ELAPSED interval, and
        # wall clock can jump backwards when NTP corrects it — which would make
        # a member look like it heartbeated in the future and never time out.
        self.last_heartbeat = time.monotonic()
        self.assignment = []            # [(topic, partition), ...]

    def is_expired(self, now: float) -> bool:
        return (now - self.last_heartbeat) * 1000 > self.session_timeout_ms

    def touch(self) -> None:
        self.last_heartbeat = time.monotonic()

    def __repr__(self) -> str:
        return f"Member({self.member_id[:8]}, {len(self.assignment)} partitions)"


class Group:
    """One consumer group's membership and state."""

    def __init__(self, group_id: str):
        self.group_id = group_id
        self.state = EMPTY

        # Starts at 0 and increments on every rebalance. A member holding
        # generation 7 in a generation-8 group is a zombie, by definition.
        self.generation_id = 0

        self.leader_id = None
        self.members = {}               # member_id -> Member
        self.protocol = 'range'

        # Who has actually re-sent JOIN_GROUP in THIS generation. Cleared at
        # the start of every rebalance.
        #
        # This has to be tracked separately from `members`: a member is still
        # a member while it is being waited for, so "have they all rejoined?"
        # cannot be answered by looking at the membership dict. Getting this
        # wrong makes the join complete on the first member, and the leader
        # then computes an assignment for a group of one — silently leaving
        # every other consumer idle.
        self.joined_members = set()

        # Fires when the join phase completes, releasing every held
        # JOIN_GROUP. Recreated per rebalance so a late waiter on the previous
        # round cannot be woken by the next one.
        self.join_complete = asyncio.Event()
        self.sync_complete = asyncio.Event()

        self.rebalance_started_at = 0.0

    # ── State transitions ──────────────────────────────────────

    def begin_rebalance(self) -> None:
        """
        Move to PREPARING_REBALANCE. **The generation does not change yet.**

        That ordering matters, and getting it wrong produces a subtly worse
        protocol. If the generation bumped here, an existing member's next
        heartbeat would carry a now-stale generation and be answered with
        ILLEGAL_GENERATION — technically actionable, but it says "you are a
        zombie" when the truth is "the group is reorganising and you should
        rejoin". Two different situations collapsed into one code.

        Bumping at `complete_join` instead means members in PREPARING_REBALANCE
        still hold the current generation, so they get REBALANCE_IN_PROGRESS —
        the informative answer, and what real Kafka does.

        Fencing does not suffer, because it never depended on this: an EVICTED
        member is removed from `members` entirely and fails the membership
        check regardless of generation. The generation fences the other case —
        a member still present whose assignment has been redrawn — and that is
        settled by the time the join completes.
        """
        if self.state == PREPARING_REBALANCE:
            return

        self.state = PREPARING_REBALANCE
        self.rebalance_started_at = time.monotonic()
        self.joined_members = set()
        self.join_complete = asyncio.Event()
        self.sync_complete = asyncio.Event()

        # Eager (stop-the-world) rebalancing: everyone gives everything up
        # before anyone receives anything. Simple and correct, and it means
        # ALL consumption stops for the duration. Cooperative incremental
        # rebalancing revokes only what must move; it needs several rounds and
        # much more state, and Kafka added it years later.
        for member in self.members.values():
            member.assignment = []

    def complete_join(self) -> None:
        # The generation advances HERE, once membership for this round is
        # settled. Everyone who joined learns the new number in their join
        # response; anyone still holding the old one is, from now on, stale.
        self.generation_id += 1
        self.state = COMPLETING_REBALANCE
        # The leader is simply the first member to have joined. Nothing about
        # it is special — it does not own data or coordinate anything. It is
        # only "the client we asked to compute the assignment", and any member
        # would do.
        if self.leader_id not in self.members:
            self.leader_id = next(iter(self.members), None)
        self.join_complete.set()

    def complete_sync(self) -> None:
        self.state = STABLE
        self.sync_complete.set()

    def all_topics(self) -> list:
        topics = set()
        for member in self.members.values():
            topics.update(member.topics)
        return sorted(topics)

    def __repr__(self) -> str:
        return (f"Group({self.group_id}, {self.state}, gen={self.generation_id}, "
                f"{len(self.members)} members)")


class GroupCoordinator:
    """
    Every group this broker coordinates.

    With one broker that is all of them. Real Kafka hashes the group id to pick
    which broker coordinates it, so groups spread across the cluster — which is
    what `FIND_COORDINATOR` is for, and why that API exists here already even
    though its answer is always "me".
    """

    def __init__(self, node_id: int = 0):
        self.node_id = node_id
        self.groups = {}                # group_id -> Group

        # One asyncio.Lock, not a threading.Lock: every coordinator method runs
        # on the event loop, and the sections below AWAIT. A threading.Lock
        # held across an await would block the entire loop — every other
        # client, not just this group — for the whole rebalance timeout.
        self._lock = asyncio.Lock()

    def _group(self, group_id: str) -> Group:
        group = self.groups.get(group_id)
        if group is None:
            group = Group(group_id)
            self.groups[group_id] = group
        return group

    # ── Fencing ────────────────────────────────────────────────

    def _check_member(self, group: Group, member_id: str, generation_id: int) -> None:
        """
        Reject anyone who is no longer who they think they are.

        Every commit and every heartbeat passes through here. Four lines, and
        they are the entire reason a timed-out consumer cannot rewind a group
        that has already moved on without it.
        """
        if member_id not in group.members:
            raise errors.UnknownMemberIdError(
                f"member {member_id} is not in group {group.group_id} — "
                f"it was evicted, or the group was rebalanced without it",
                group=group.group_id)
        if generation_id != group.generation_id:
            raise errors.IllegalGenerationError(
                f"generation {generation_id} is stale; the group is at "
                f"{group.generation_id}",
                group=group.group_id)

    def _expire_members(self, group: Group) -> list:
        """
        Evict members whose heartbeats stopped. Returns the evicted ids.

        THE liveness mechanism. A consumer that is killed, or merely stuck in a
        long GC pause, stops heartbeating and loses its partitions to the
        survivors.

        Note the ambiguity that cannot be resolved from here: a paused process
        and a dead process look identical from outside. That is why the
        eviction has to be paired with generation fencing — we cannot know
        whether it will come back, so we make it harmless if it does.
        """
        now = time.monotonic()

        # A member with a request PARKED IN THE COORDINATOR is alive by
        # definition — we are holding its connection open right now.
        #
        # This is not a special case, it is the correct rule, and missing it
        # was a real bug: the heartbeat thread cannot start until the join
        # completes, so a member waiting in the rendezvous sends nothing for
        # the whole rebalance. With a session timeout shorter than the
        # rebalance timeout, the reaper evicted members that were doing exactly
        # what the protocol told them to do — and they came back with
        # UNKNOWN_MEMBER_ID from a join they were still inside.
        #
        # Real Kafka avoids it the same way: an in-purgatory request counts as
        # liveness.
        parked = (group.joined_members
                  if group.state in (PREPARING_REBALANCE, COMPLETING_REBALANCE)
                  else frozenset())

        expired = [m.member_id for m in group.members.values()
                   if m.member_id not in parked and m.is_expired(now)]
        for member_id in expired:
            del group.members[member_id]
        return expired

    # ── JOIN_GROUP ─────────────────────────────────────────────

    async def join_group(self, group_id: str, member_id: str, topics: list,
                         session_timeout_ms: int = DEFAULT_SESSION_TIMEOUT_MS,
                         rebalance_timeout_ms: int = DEFAULT_REBALANCE_TIMEOUT_MS
                         ) -> dict:
        """
        Join (or rejoin) a group. **Blocks until the join phase completes.**

        The wait is the point: the coordinator cannot answer the first member
        until it knows who else is coming, because the leader needs the full
        member list to compute an assignment.
        """
        async with self._lock:
            group = self._group(group_id)
            self._expire_members(group)

            if not member_id:
                # A new member gets an id from us. Real Kafka builds it from
                # the client id plus a UUID so a human can tell members apart
                # in a log; we keep just the UUID.
                member_id = str(uuid.uuid4())

            existing = group.members.get(member_id)
            if existing is not None:
                existing.topics = list(topics)
                existing.touch()
            else:
                group.members[member_id] = Member(member_id, topics,
                                                  session_timeout_ms)
                if group.leader_id is None:
                    group.leader_id = member_id

            group.begin_rebalance()
            group.joined_members.add(member_id)
            group.members[member_id].touch()

            # Everyone we know about has rejoined, so stop waiting. In the
            # common case — a group forming for the first time, or a
            # single-member group — this fires on the last member's join and
            # nobody waits at all.
            if group.joined_members >= set(group.members) \
                    and group.state == PREPARING_REBALANCE:
                group.complete_join()

            event = group.join_complete

        if not event.is_set():
            try:
                await asyncio.wait_for(event.wait(),
                                       timeout=rebalance_timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                # A member we were waiting for never came back. Proceed with
                # whoever did — waiting forever for an absent member would let
                # one dead consumer freeze the entire group.
                async with self._lock:
                    group = self._group(group_id)
                    if group.state == PREPARING_REBALANCE:
                        # Drop the members that did not rejoin. Leaving them in
                        # would have the leader assign partitions to a consumer
                        # that is not listening — those partitions would simply
                        # stop being read, with nothing reporting it.
                        for absent in set(group.members) - group.joined_members:
                            del group.members[absent]
                        if group.leader_id not in group.members:
                            group.leader_id = next(iter(group.members), None)
                        group.complete_join()

        async with self._lock:
            group = self._group(group_id)
            if member_id not in group.members:
                raise errors.UnknownMemberIdError(
                    f"member {member_id} was evicted during the rebalance",
                    group=group_id)

            is_leader = group.leader_id == member_id
            return {
                'member_id': member_id,
                'generation_id': group.generation_id,
                'leader_id': group.leader_id,
                'is_leader': is_leader,
                'protocol': group.protocol,
                # Only the leader gets the member list — it is the only one
                # that needs it, and sending it to everyone would be pure
                # bandwidth. Real Kafka does exactly this.
                'members': [
                    {'member_id': m.member_id, 'topics': m.topics}
                    for m in group.members.values()
                ] if is_leader else [],
            }

    # ── SYNC_GROUP ─────────────────────────────────────────────

    async def sync_group(self, group_id: str, member_id: str,
                         generation_id: int, assignments: dict = None,
                         timeout_ms: int = DEFAULT_REBALANCE_TIMEOUT_MS) -> dict:
        """
        Upload assignments (leader) or collect your own (everyone else).

        `assignments` maps member_id -> [[topic, partition], ...].
        """
        async with self._lock:
            group = self._group(group_id)
            self._check_member(group, member_id, generation_id)

            if group.leader_id == member_id and assignments is not None:
                for target_id, partitions in assignments.items():
                    member = group.members.get(target_id)
                    if member is not None:
                        member.assignment = [tuple(p) for p in partitions]
                group.complete_sync()

            group.members[member_id].touch()
            event = group.sync_complete
            if event.is_set():
                return {'assignment': group.members[member_id].assignment,
                        'generation_id': group.generation_id}

        # A follower arrived before the leader uploaded. Wait for it.
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_ms / 1000.0)
        except asyncio.TimeoutError:
            # The leader never uploaded — it died mid-rebalance. Force another
            # round rather than leaving followers with no assignment forever.
            async with self._lock:
                group = self._group(group_id)
                group.begin_rebalance()
            raise errors.RebalanceInProgressError(
                f"the leader of group {group_id} did not send an assignment; "
                f"rejoining", group=group_id)

        async with self._lock:
            group = self._group(group_id)
            self._check_member(group, member_id, generation_id)
            return {'assignment': group.members[member_id].assignment,
                    'generation_id': group.generation_id}

    # ── HEARTBEAT ──────────────────────────────────────────────

    async def heartbeat(self, group_id: str, member_id: str,
                        generation_id: int) -> dict:
        """
        Prove liveness, and find out whether a rebalance has started.

        A heartbeat is how a member LEARNS about a rebalance it did not cause:
        the coordinator answers REBALANCE_IN_PROGRESS, and the member rejoins.
        Without that, a member whose peer just died would keep consuming
        partitions that have already been reassigned to someone else — two
        consumers on one partition, which is exactly what a group exists to
        prevent.
        """
        async with self._lock:
            group = self._group(group_id)
            self._check_member(group, member_id, generation_id)
            group.members[member_id].touch()

            if group.state in (PREPARING_REBALANCE, COMPLETING_REBALANCE):
                raise errors.RebalanceInProgressError(
                    f"group {group_id} is rebalancing; rejoin", group=group_id)

            return {'generation_id': group.generation_id, 'state': group.state}

    # ── LEAVE_GROUP ────────────────────────────────────────────

    async def leave_group(self, group_id: str, member_id: str) -> dict:
        """
        Depart cleanly.

        Worth having as an explicit API: without it, a consumer shutting down
        normally would hold its partitions until its session timed out — up to
        ten seconds of unnecessary stall on every deploy. Leaving tells the
        coordinator immediately so the rebalance starts at once.
        """
        async with self._lock:
            group = self._group(group_id)
            if member_id in group.members:
                del group.members[member_id]
                if group.leader_id == member_id:
                    group.leader_id = next(iter(group.members), None)

                if group.members:
                    # Only BEGIN the rebalance. The survivors have not rejoined
                    # yet, and completing here would hand the leader a member
                    # list nobody has confirmed.
                    #
                    # They find out on their next heartbeat, which answers
                    # REBALANCE_IN_PROGRESS — that is precisely what the
                    # heartbeat is for, beyond proving liveness.
                    group.begin_rebalance()
                else:
                    group.state = EMPTY
                    group.leader_id = None
            return {'group_id': group_id}

    # ── Introspection ──────────────────────────────────────────

    async def describe(self, group_id: str) -> dict:
        async with self._lock:
            group = self.groups.get(group_id)
            if group is None:
                raise errors.UnknownMemberIdError(
                    f"no such group {group_id!r}", group=group_id)

            self._expire_members(group)
            return {
                'group_id': group_id,
                'state': group.state,
                'generation_id': group.generation_id,
                'leader_id': group.leader_id,
                'protocol': group.protocol,
                'members': [
                    {'member_id': m.member_id, 'topics': m.topics,
                     'assignment': [list(p) for p in m.assignment]}
                    for m in group.members.values()
                ],
            }

    async def list_groups(self) -> list:
        async with self._lock:
            return sorted(self.groups)

    async def reap_expired(self) -> dict:
        """
        Sweep every group for dead members. Called on a timer by the broker.

        Needed because eviction otherwise only happens when SOMEONE calls in —
        and if every member of a group dies at once, nobody ever calls in
        again. The group would sit forever holding partitions on behalf of
        processes that no longer exist.
        """
        evicted = {}
        async with self._lock:
            for group_id, group in self.groups.items():
                expired = self._expire_members(group)
                if expired:
                    evicted[group_id] = expired
                    if group.leader_id not in group.members:
                        group.leader_id = next(iter(group.members), None)
                    if group.members:
                        # Begin only — survivors learn via heartbeat and rejoin.
                        group.begin_rebalance()
                    else:
                        group.state = EMPTY
                        group.leader_id = None
        return evicted
