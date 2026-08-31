"""
CONCEPT: Membership and the Local View of the Cluster
--------------------------------------------------------
Every broker keeps two things:

    who is alive       from heartbeats it exchanges with its peers
    who leads what     the partition→broker map, decided by the controller

This class owns both, plus the one question every request handler asks:
**do I lead this partition?**


LIVENESS IS A LOCAL OPINION, NOT A FACT
-----------------------------------------
Broker 1 believes broker 3 is down when it has not heard from broker 3 within
`broker_session_timeout_ms`. That is *broker 1's* belief. Broker 2 may believe
otherwise — and in a network partition it certainly will.

There is no way around this. "Is that machine alive?" is not answerable
remotely; only "have I heard from it lately?" is. Every distributed system is
built on that substitution, and every consequence in this phase follows from
it, including split-brain.


HOW THE CONTROLLER IS CHOSEN — AND HOW THAT CHANGED
-----------------------------------------------------
The first version of this file elected the controller as:

    controller = min(live_broker_ids)

Ten lines, no messages, deterministic — *given the same view of who is alive*.
And wrong under a network partition, because each side has a different view and
therefore elects a different controller. `chaos/split_brain.py` demonstrated
exactly that: two controllers, six partitions each assigned differently, no
error anywhere.

**Leadership now comes from a Raft quorum instead** (`broker/raft/`). A
candidate needs votes from a majority of ALL voters, not of those it can reach,
and two disjoint majorities of one set cannot exist. The minority side is
mathematically unable to elect, so it stands down.

This class keeps everything else it always did — liveness, the leader map, the
`leads()` check every request handler asks. Only the *source* of leadership
changed: `elect_controller()` now reports what Raft decided rather than
computing an opinion of its own.

LIVENESS AND LEADERSHIP ARE NOW DIFFERENT QUESTIONS, and keeping them separate
matters: heartbeats still tell us which brokers are up (needed for placement),
while Raft tells us who may decide. Merging them is what made the old design
unsafe.
"""

import threading
import time


class ClusterState:
    """
    One broker's view of the cluster: liveness, controller, leader map.

    Thread-safe. The heartbeat loop writes it from the event loop while request
    handlers read it, and Phase 6's replica fetcher will read it from an
    executor thread.
    """

    def __init__(self, node_id: int, config, session_timeout_ms: float = 3000.0):
        self.node_id = node_id
        self.config = config
        self.session_timeout_ms = session_timeout_ms

        self._lock = threading.Lock()

        # node_id -> monotonic timestamp of the last successful contact.
        # We are trivially always alive to ourselves.
        self._last_seen = {node_id: time.monotonic()}

        # (topic, partition) -> node_id. Empty until the controller decides.
        self._leaders = {}

        # Monotonic counter stamped by the controller on every push. A peer
        # ignores an update older than the one it already has — the cheap
        # version of fencing, and enough to stop out-of-order network delivery
        # from resurrecting a stale map.
        self.metadata_version = 0

        self.controller_id = node_id

    # ── Liveness ───────────────────────────────────────────────

    def mark_alive(self, node_id: int) -> None:
        """Record contact. Called on a successful heartbeat, either direction."""
        with self._lock:
            self._last_seen[node_id] = time.monotonic()

    def mark_down(self, node_id: int) -> None:
        """
        Forget a peer immediately, rather than waiting for its timeout.

        Used when a connection attempt fails outright: a refused connection is
        stronger evidence than silence, so there is no reason to keep believing
        for another few seconds.
        """
        with self._lock:
            self._last_seen.pop(node_id, None)

    def live_brokers(self) -> list:
        """Sorted ids we have heard from recently. Always includes ourselves."""
        now = time.monotonic()
        with self._lock:
            return sorted(
                node_id for node_id, seen in self._last_seen.items()
                if node_id == self.node_id
                or (now - seen) * 1000 <= self.session_timeout_ms
            )

    def is_alive(self, node_id: int) -> bool:
        return node_id in self.live_brokers()

    # ── Controller election ────────────────────────────────────

    def set_controller(self, node_id) -> int:
        """
        Record who Raft says the controller is.

        Called from the membership loop with `raft.leader_id`. This class no
        longer *decides* anything about leadership — it only remembers the
        quorum's answer, so that request handlers and `DESCRIBE_CLUSTER` have
        somewhere local to read it from.

        `None` means Raft has no leader right now — an election is in progress,
        or we are in a minority that cannot elect. That is a real state and it
        is reported honestly rather than being papered over by falling back to
        ourselves, which is precisely the guess that produced split-brain.
        """
        with self._lock:
            self.controller_id = node_id
        return node_id

    @property
    def is_controller(self) -> bool:
        """
        True only if the QUORUM says so.

        `controller_id` is None during an election or inside a minority
        partition, and `None == self.node_id` is False — so a node with no
        quorum never believes it is in charge. That fallthrough is deliberate
        and is the whole safety property in one comparison.
        """
        return (self.controller_id is not None
                and self.controller_id == self.node_id)

    # ── Leadership ─────────────────────────────────────────────

    def set_leaders(self, leaders: dict, version: int) -> bool:
        """
        Accept a leader map from the controller. Returns True if applied.

        Rejects anything not newer than what we hold. Without that check, an
        update delayed by the network could arrive after a newer one and
        silently roll the cluster's view backwards.
        """
        with self._lock:
            if version <= self.metadata_version:
                return False
            self._leaders = {(t, int(p)): int(n) for (t, p), n in leaders.items()}
            self.metadata_version = version
            return True

    def set_leaders_from_wire(self, entries: list, version: int) -> bool:
        """Same, from the JSON form: [{'topic', 'partition', 'leader'}, ...]."""
        leaders = {(e['topic'], int(e['partition'])): int(e['leader'])
                   for e in entries}
        return self.set_leaders(leaders, version)

    def leader_for(self, topic: str, partition: int):
        """Which broker leads this partition, or None if unassigned."""
        with self._lock:
            return self._leaders.get((topic, partition))

    def leads(self, topic: str, partition: int) -> bool:
        """
        Do *I* lead this partition?

        The question every PRODUCE, FETCH and LIST_OFFSETS handler asks. An
        unassigned partition (no entry yet) returns False, so a broker refuses
        to serve a partition nobody has been given rather than inventing one —
        the client gets a retriable error and comes back once the controller
        has decided.
        """
        with self._lock:
            return self._leaders.get((topic, partition)) == self.node_id

    def my_partitions(self) -> list:
        with self._lock:
            return sorted(tp for tp, node in self._leaders.items()
                          if node == self.node_id)

    def all_leaders(self) -> dict:
        with self._lock:
            return dict(self._leaders)

    def leader_entries(self) -> list:
        """Wire form, for CONTROLLER_UPDATE and DESCRIBE_CLUSTER."""
        with self._lock:
            return [{'topic': t, 'partition': p, 'leader': n}
                    for (t, p), n in sorted(self._leaders.items())]

    # ── Introspection ──────────────────────────────────────────

    def snapshot(self) -> dict:
        live = self.live_brokers()
        return {
            'node_id': self.node_id,
            'controller_id': self.controller_id,
            'live_brokers': live,
            'known_brokers': self.config.node_ids(),
            'down_brokers': [n for n in self.config.node_ids() if n not in live],
            'metadata_version': self.metadata_version,
            'partitions': self.leader_entries(),
        }

    def __repr__(self) -> str:
        return (f"ClusterState(node={self.node_id}, "
                f"controller={self.controller_id}, "
                f"live={self.live_brokers()}, v={self.metadata_version})")
