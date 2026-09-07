"""
CONCEPT: The Controller — Who Leads What
------------------------------------------
Exactly one broker in a cluster is the controller. It does one job:

    decide which broker leads which partition, and tell everyone

Why must it be one? Because placement has to be *consistent*. Two brokers
independently deciding they both lead `orders-0` is precisely the failure a
controller exists to prevent — and it would be silent, because each would
happily accept writes into its own local log.


THIS FILE DOES NOT ELECT
--------------------------
Election lives in `ClusterState.elect_controller` — lowest live broker id,
computed independently by every broker from its own liveness view. This class
is what the *winner* then does.

That split is worth keeping straight, because they fail differently:

    election fails    → two controllers (split-brain, under a partition)
    placement fails   → partitions unassigned or double-assigned


PLACEMENT IS ROUND-ROBIN AND DETERMINISTIC
--------------------------------------------
    leaders[(topic, p)] = live[index % len(live)]

Six partitions over three live brokers gives each two. The result depends only
on `(sorted live ids, topic, partition count)`, which means anyone can verify
the controller's arithmetic without trusting it — a useful property when the
controller is the component you most need to be able to reason about.

Sorting is load-bearing. Iterating a set of broker ids unsorted would produce a
different assignment from identical inputs on every recomputation, churning
leadership for no reason.


WHAT THIS DELIBERATELY DOES NOT DO
------------------------------------
**It does not move data.** When a partition's leader changes, the new leader
starts an *empty* log. The old leader's messages are still on the old leader's
disk and are simply not reachable through the new one.

So Phase 5 is not fault tolerance: losing a broker loses its partitions' data.
What it buys is capacity and routing. Phase 6 adds replicas, and then a new
leader already has the data — at which point leadership change becomes
survivable rather than merely detectable.
"""

import logging

log = logging.getLogger(__name__)


class Controller:
    """
    Placement logic for whichever broker is currently the controller.

    Stateless apart from its reference to `ClusterState`: it recomputes from
    live membership rather than persisting decisions. Real Kafka writes its
    decisions to a metadata log so a new controller resumes rather than
    recomputing — which also preserves *why* a partition moved. We lose that
    history, and gain not having to implement a replicated log to hold it.
    """

    def __init__(self, cluster_state, metadata_store, raft=None,
                 replica_lag_time_max_ms: float = None):
        self.cluster = cluster_state
        self.metadata = metadata_store
        # The quorum. Placement decisions are PROPOSED to it and only take
        # effect once a majority has stored them.
        self.raft = raft
        # How long a replica may go without fetching before it is ejected from
        # the ISR. None means "use the broker session timeout", which keeps a
        # single knob for a single question: how long is silence tolerated?
        self.replica_lag_time_max_ms = replica_lag_time_max_ms

    # ── Placement ──────────────────────────────────────────────

    def assign_leaders(self) -> dict:
        """
        Compute `(topic, partition) → node_id` for every declared partition.

        Iterates topics in sorted order and partitions in numeric order so the
        assignment is reproducible. `live` is sorted for the same reason.
        """
        live = self.cluster.live_brokers()
        if not live:
            return {}

        leaders = {}
        index = 0
        for topic in sorted(self.metadata.list_topics()):
            config = self.metadata.get(topic)
            if config is None:
                continue
            for partition in range(config.partitions):
                leaders[(topic, partition)] = live[index % len(live)]
                index += 1
        return leaders

    def assign_partitions(self) -> list:
        """
        Compute the full placement — leader AND replica set — for every
        partition, in the wire form `ASSIGN_PARTITIONS` carries.

        CONCEPT: one placement algorithm, not two.

          Through Phase 5 this file had its own round-robin loop while
          `MetadataStateMachine.compute_assignment` had another, and only this
          one was ever called. Two implementations of the same rule is one
          implementation and one liability — the unused copy drifts, and it
          drifts silently because nothing runs it.

          Phase 6 needs replica sets, which meant changing the rule. So rather
          than changing it twice, placement now lives in ONE place: the state
          machine, which is also where the result is applied. This file
          supplies the inputs — who is alive, which topics exist, how many
          copies each wants — and the machine turns them into a decision.

        The topic list comes from the LOCAL metadata store, and that is worth
        being uncomfortable about. It ought to be the committed set — a
        placement should only concern topics a majority has agreed exist. But
        auto-creation writes a topic into the local store from inside a
        PRODUCE, without proposing anything, so the committed set is missing
        exactly those topics. Placing only committed topics would leave every
        auto-created partition unassigned and therefore unservable.

        The honest fix is to propose auto-creations too. Until then this reads
        the store, which is the union of "committed" and "invented locally" —
        and that union is what the broker is actually serving.

        Each topic's replication factor is resolved HERE, from the local
        metadata store, and the resolved answer is what gets replicated. The
        state machine must not read that store itself: `apply()` has to be a
        pure function of committed state, and a node consulting its own local
        files during apply is how two nodes reach different answers from the
        same log.
        """
        live = self.cluster.live_brokers()
        if not live or self.raft is None:
            return []

        widths = {}
        for topic in self.metadata.list_topics():
            config = self.metadata.get(topic)
            if config is not None:
                widths[topic] = config.partitions

        assignment = self.raft.machine.compute_assignment(
            live, replication_factor=self._factor_for, topics=widths)

        for item in assignment:
            item['isr'] = self._observed_isr(item)
            self._elect_leader(item, live)
            self._stamp_epoch(item)

        return assignment

    def _elect_leader(self, item: dict, live: list) -> None:
        """
        Prefer a leader that already holds the data. Mutates `item` in place.

        CONCEPT: the ring is right for placement and wrong for FAILOVER.

          `compute_assignment` walks the broker ring, which spreads leadership
          evenly across a healthy cluster — exactly what you want when a
          partition is first created. But when a leader dies, the ring hands
          the partition to whichever broker comes next, and that broker may
          hold nothing at all.

          So election asks a different question first: **who is in the ISR?**
          An in-sync replica holds everything below the high watermark by
          definition, so every record a consumer was ever allowed to read is
          already on it. Promoting outside the ISR throws that guarantee away.

        When no in-sync replica is live, we keep the existing leader rather
        than electing an out-of-sync one. That is `unclean.leader.election
        .enable=false`, and it means the partition goes UNAVAILABLE until an
        in-sync replica returns. Kafka offers the other option; we do not, and
        it is worth being explicit that refusing is a choice — the partition
        going dark is the visible cost of not silently losing data.
        """
        topic, partition = item['topic'], item['partition']

        # The committed ISR, not the one being proposed: this decides who is
        # ELIGIBLE, and eligibility has to rest on what the quorum agreed
        # rather than on what this controller currently believes.
        committed_isr = self.raft.machine.isr_for(topic, partition)
        eligible = [n for n in item['replicas']
                    if n in committed_isr and n in live]

        if not eligible:
            # Nobody in sync is up. Refuse to elect: leave whoever the quorum
            # last agreed on, even if that broker is down. A partition with a
            # dead leader is unavailable; a partition with an out-of-sync
            # leader is silently missing data, which is worse.
            previous = self.raft.machine.leader_for(topic, partition)
            if previous is not None:
                item['leader'] = previous
            return

        # Keep the current leader if it is still eligible. Leadership should
        # move because it has to, never because the ring rotated — every
        # unnecessary move costs a metadata refresh on every client.
        current = self.raft.machine.leader_for(topic, partition)
        item['leader'] = current if current in eligible else eligible[0]

    def _stamp_epoch(self, item: dict) -> None:
        """
        Bump this partition's leader epoch if its leader changed.

        Only on an actual change. Incrementing on every recomputation would
        fence brokers that are perfectly current, and would grow the number
        without it meaning anything.
        """
        topic, partition = item['topic'], item['partition']
        epoch = self.raft.machine.leader_epoch(topic, partition)

        if self.raft.machine.leader_for(topic, partition) != item['leader']:
            epoch += 1

        item['leader_epoch'] = epoch

    def _observed_isr(self, item: dict) -> list:
        """
        The in-sync set to propose for one partition.

        CONCEPT: only the LEADER of a partition can observe its lag, and the
        controller is usually not it.

          Lag is measured from fetch requests, which arrive at the partition's
          leader and nowhere else. So there are three sources, in order of
          authority:

            1. our own observation, for partitions WE lead
            2. what that partition's leader reported on its heartbeat
            3. the last committed value, if nobody has said anything yet

          Getting this wrong is not theoretical. The first version had only
          (1) and (3), and froze: controller 2, partition led by broker 1, ISR
          stuck at `[1]` from the instant before the followers first fetched —
          so every `acks=all` write failed `min.insync.replicas` on a perfectly
          healthy cluster, permanently.

        Whatever the source, the answer is constrained to the replica set being
        proposed. A partition that just moved must not carry an ISR naming
        brokers that no longer hold a copy.
        """
        topic, partition = item['topic'], item['partition']
        replicas = item['replicas']

        if item['leader'] == self.cluster.node_id:
            source = self.cluster.in_sync_replicas(
                topic, partition, self.replica_lag_time_max_ms)
        else:
            source = self.cluster.reported_isr(topic, partition)
            if source is None:
                source = self.raft.machine.isr_for(topic, partition)

        # The leader is always in its own ISR — it holds the log by definition,
        # and an ISR that excluded it could elect a successor over a replica
        # that is strictly behind.
        kept = [n for n in replicas if n in source or n == item['leader']]
        return kept or [item['leader']]

    def _factor_for(self, topic: str) -> int:
        """How many copies this topic wants. Unknown topics get one."""
        config = self.metadata.get(topic)
        return config.replication_factor if config else 1

    async def recompute(self) -> bool:
        """
        Reassign, COMMIT through Raft, then apply locally. True if it changed.

        CONCEPT: propose, then apply — never the other way round.
          The placement is decided here (round-robin over live brokers, exactly
          as before) but it is not *true* until a majority of the quorum has
          stored it. Applying first and replicating afterwards would let a
          partitioned leader hand out a map nobody agreed to — the Phase 5
          failure, with extra steps.

          So the order is: compute -> propose -> wait for commit -> apply ->
          push to peers. `CONTROLLER_UPDATE` therefore carries only committed
          metadata. It is a DELIVERY mechanism, not a second source of truth.

        The change comparison matters: recomputing on every tick would
        otherwise append an identical entry to the Raft log several times a
        second and force a majority round trip to record that nothing happened.
        Only *changes* are events.
        """
        if self.raft is None:
            # No quorum wired up (single-node tests): fall back to the
            # leader-only placement, which is what Phase 5 did throughout.
            leaders = self.assign_leaders()
            if leaders == self.cluster.all_leaders():
                return False
            self.cluster.set_leaders(leaders, self.cluster.metadata_version + 1)
            return True

        assignment = self.assign_partitions()
        if not assignment or not self.raft.machine.assignment_differs(assignment):
            return False

        committed = await self.raft.propose({
            'type': 'ASSIGN_PARTITIONS',
            'leaders': assignment,
        })
        if not committed:
            # We are not the leader any more, or we could not reach a
            # majority. Either way this decision is void — and NOT applying
            # it is the entire point. A leader that applied anyway would be
            # a minority controller acting unilaterally.
            log.warning("Controller %s could not commit a placement; "
                        "discarding it", self.cluster.node_id)
            return False

        version = self.cluster.metadata_version + 1
        self.cluster.set_placement(assignment, version)

        log.info("Controller %s committed %d partition(s) across %s (v%d)",
                 self.cluster.node_id, len(assignment),
                 self.cluster.live_brokers(), version)
        return True

    # ── Introspection ──────────────────────────────────────────

    def placement_summary(self) -> dict:
        """node_id → [(topic, partition), ...]. For logs and the admin CLI."""
        summary = {}
        for (topic, partition), node_id in sorted(self.cluster.all_leaders().items()):
            summary.setdefault(node_id, []).append((topic, partition))
        return summary

    def __repr__(self) -> str:
        return (f"Controller(node={self.cluster.node_id}, "
                f"active={self.cluster.is_controller})")
