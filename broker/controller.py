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

    def __init__(self, cluster_state, metadata_store, raft=None):
        self.cluster = cluster_state
        self.metadata = metadata_store
        # The quorum. Placement decisions are PROPOSED to it and only take
        # effect once a majority has stored them.
        self.raft = raft

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
        leaders = self.assign_leaders()
        if leaders == self.cluster.all_leaders():
            return False

        if self.raft is not None:
            committed = await self.raft.propose({
                'type': 'ASSIGN_PARTITIONS',
                'leaders': [{'topic': t, 'partition': p, 'leader': n}
                            for (t, p), n in sorted(leaders.items())],
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
        self.cluster.set_leaders(leaders, version)

        log.info("Controller %s committed %d partition(s) across %s (v%d)",
                 self.cluster.node_id, len(leaders),
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
