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

        # (topic, partition) -> [node_id, ...], leader first (Phase 6). Who
        # should hold a COPY — as opposed to `_leaders`, which is who serves
        # it. Through Phase 5 these were the same set of one.
        self._replicas = {}

        # (topic, partition) -> {node_id: end offset}. What each follower has
        # proved it holds, learned from the offsets it asks for. Volatile on
        # purpose: a leader that restarts has no business remembering claims it
        # cannot re-verify, and the followers will re-prove them within one
        # fetch interval.
        self._replica_positions = {}

        # (topic, partition) -> {node_id: monotonic time of last fetch}.
        # Feeds the ISR: a replica is in-sync while it is still ASKING, which
        # is a different question from how far behind it is. See `in_sync`.
        self._replica_seen = {}

        # (topic, partition) -> [node_id, ...]. The committed in-sync set,
        # delivered by the controller. A leader OBSERVES lag; only the
        # controller may record it, because leader election reads this set and
        # it must outlive the leader that computed it.
        self._isr = {}

        # (topic, partition) -> int. How many times this partition's leader has
        # changed, per the controller. Compared against a request's epoch to
        # fence a broker that has not heard it was replaced (Phase 6 step 6).
        self._leader_epochs = {}

        # (topic, partition) -> [node_id, ...], as reported by that partition's
        # LEADER on its heartbeat. Only the leader can see who is fetching, so
        # a controller that leads none of a topic's partitions learns the ISR
        # from here rather than first-hand.
        self._reported_isr = {}

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
            # A leader-only map means one copy per partition. Leaving stale
            # replica sets behind would have the fetcher chasing partitions
            # this placement never mentioned.
            self._replicas = {key: [node] for key, node in self._leaders.items()}
            self._isr = {key: [node] for key, node in self._leaders.items()}
            self.metadata_version = version
            return True

    def set_placement(self, entries: list, version: int) -> bool:
        """
        Accept a full placement — leaders AND replica sets — from the
        controller. The Phase 6 form of `set_leaders`.

        Replicas are stored beside the leader map rather than replacing it,
        because the two are asked about at different moments: every PRODUCE
        asks "do I lead this?", while only the replica fetcher asks "should I
        be following this?". Keeping them separate keeps the hot question a
        single dict lookup.
        """
        with self._lock:
            if version <= self.metadata_version:
                return False
            self._leaders = {(e['topic'], int(e['partition'])): int(e['leader'])
                             for e in entries}
            self._replicas = {
                (e['topic'], int(e['partition'])):
                    [int(n) for n in (e.get('replicas') or [e['leader']])]
                for e in entries}
            self._isr = {
                (e['topic'], int(e['partition'])):
                    [int(n) for n in (e.get('isr')
                                      or e.get('replicas') or [e['leader']])]
                for e in entries}

            # Monotonic per partition, never lowered by an update. A delayed
            # message carrying an older epoch must not un-fence us — the
            # version check above catches most of that, and this catches the
            # rest without depending on it.
            for entry in entries:
                key = (entry['topic'], int(entry['partition']))
                epoch = int(entry.get('leader_epoch') or 0)
                if epoch > self._leader_epochs.get(key, 0):
                    self._leader_epochs[key] = epoch

            self.metadata_version = version
            return True

    def set_leaders_from_wire(self, entries: list, version: int) -> bool:
        """
        Same, from the JSON form: [{'topic', 'partition', 'leader'}, ...].

        Kept as the name `CONTROLLER_UPDATE` handling already uses. Entries
        may or may not carry `replicas` depending on the sender's phase, and
        `set_placement` treats an absent list as "the leader alone" — which is
        exactly what a pre-Phase-6 broker means by it.
        """
        return self.set_placement(entries, version)

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

    # ── Replication progress (Phase 6) ─────────────────────────

    def record_replica_position(self, topic: str, partition: int,
                                node_id: int, offset: int) -> None:
        """
        Note that `node_id` holds everything below `offset` of this partition.

        CONCEPT: the position comes from the REQUEST, not from a reply.

          A follower asking for offset 150 has, by asking, proved it holds
          0..149. That is the whole accounting mechanism — no acknowledgement
          message, no separate reporting API, no state to reconcile. The
          request that advances a follower is also the report that it
          advanced.

          The consequence is that the leader always learns a position one
          round trip late: the follower already holds whatever it appended
          from the previous response, and will not say so until it asks again.
          So the high watermark is permanently a little conservative, which is
          the correct direction for a safety bound to be wrong in.

        Monotonic per replica. A retry or a reordered request can carry an
        older offset, and letting the watermark move backwards would un-commit
        records a consumer has already been allowed to read.
        """
        with self._lock:
            key = (topic, partition)
            positions = self._replica_positions.setdefault(key, {})
            if offset > positions.get(node_id, -1):
                positions[node_id] = offset

            # Timestamped on every fetch, including one that returns nothing.
            # A caught-up follower asking for the same offset forever is
            # perfectly in sync, so liveness is about ASKING, not about
            # advancing — see `in_sync`.
            self._replica_seen.setdefault(key, {})[node_id] = time.monotonic()

    def in_sync_replicas(self, topic: str, partition: int,
                         lag_time_max_ms: float = None) -> list:
        """
        Which replicas are keeping up, as this leader currently sees it.

        CONCEPT: in-sync is measured in TIME, not in records behind.

          A replica is out of sync when it has not FETCHED for
          `lag_time_max_ms` — not when it is more than N records behind. Kafka
          shipped `replica.lag.max.messages` and removed it in 0.9 for a
          reason worth understanding: a count-based rule cannot tell a broken
          replica from a healthy one during a traffic spike. A producer burst
          puts every follower thousands of records behind for a moment, and a
          count threshold would eject the whole ISR at precisely the moment
          durability matters most.

          Time asks a better question — *is this replica still talking to me?*
          — and a burst does not change the answer.

        This is an OBSERVATION, not the truth. The committed ISR is whatever
        the controller has recorded (`isr_for`); this is what a leader would
        propose. Keeping the two apart is what stops a partitioned leader from
        acting on a belief nobody else shares.
        """
        window = ((lag_time_max_ms if lag_time_max_ms is not None
                   else self.session_timeout_ms) / 1000.0)

        with self._lock:
            key = (topic, partition)
            replicas = self._replicas.get(key)
            if not replicas:
                return [self.node_id]

            now = time.monotonic()
            seen = self._replica_seen.get(key, {})
            return [node_id for node_id in replicas
                    if node_id == self.node_id                  # always ourselves
                    or now - seen.get(node_id, 0.0) <= window]

    def set_isr(self, topic: str, partition: int, nodes: list) -> None:
        """Adopt a committed in-sync set from the controller."""
        with self._lock:
            self._isr[(topic, partition)] = [int(n) for n in nodes]

    def record_reported_isr(self, node_id: int, entries: list) -> None:
        """
        Take a partition leader's word for its own ISR.

        CONCEPT: only a partition's LEADER can observe its lag, and the
        controller is usually not it.

          Lag is measured from fetch requests, which arrive at the partition's
          leader and nowhere else. A controller that leads none of a topic's
          partitions has no way to see who is keeping up — so if it can only
          report first-hand, it carries the last committed value forward
          forever and the ISR freezes at whatever it was when the partition
          was first placed.

          That was a real bug: controller 2, partition led by 1, ISR stuck at
          `[1]` while all three replicas were healthy — so every `acks=all`
          write failed `min.insync.replicas` on a cluster with nothing wrong
          with it.

        So leaders report, on the heartbeat they were already sending, and the
        controller aggregates. A report is only accepted from the broker that
        actually leads the partition: this is an observation of *its own*
        followers, and a broker has no standing to describe a partition it
        does not serve.
        """
        with self._lock:
            for entry in entries:
                key = (entry['topic'], int(entry['partition']))
                if self._leaders.get(key) != int(node_id):
                    continue                    # not yours to report on
                self._reported_isr[key] = [int(n) for n in entry['isr']]

    def reported_isr(self, topic: str, partition: int):
        """The latest ISR its leader told us about, or None."""
        with self._lock:
            value = self._reported_isr.get((topic, partition))
            return list(value) if value is not None else None

    def isr_for(self, topic: str, partition: int) -> list:
        """
        The committed in-sync set. Falls back to the whole replica set.

        The fallback is what makes a fresh partition writable: before the
        controller has recorded anything, treating the ISR as empty would make
        every `acks=all` write fail `min.insync.replicas` on a perfectly
        healthy cluster.
        """
        with self._lock:
            key = (topic, partition)
            if key in self._isr:
                return list(self._isr[key])
            replicas = self._replicas.get(key)
            if replicas:
                return list(replicas)
            leader = self._leaders.get(key)
            return [leader] if leader is not None else []

    def high_watermark(self, topic: str, partition: int,
                       leader_end_offset: int,
                       lag_time_max_ms: float = None) -> int:
        """
        The highest offset every IN-SYNC replica holds.

        The minimum across the in-sync set, with the leader counted at its own
        log end. A replica we have never heard from counts as 0 — it may hold
        nothing, and assuming otherwise would publish records that could
        vanish in a failover.

        CONCEPT: why in-sync rather than the whole replica set.

          Step 3 took the minimum over every replica, which is safe and
          fragile: one follower with a dying disk stops the watermark
          advancing, so consumers on that partition stop seeing new records
          while the leader keeps accepting writes. Nothing errors. The
          partition is simply, silently dead.

          Excluding a replica that has stopped fetching frees the watermark
          without waiting for a machine that is not coming back. The replica
          set is unchanged — that broker is still SUPPOSED to hold a copy, and
          is re-admitted the moment it catches up. What changed is whether
          anyone waits for it.

        With RF=1 the set is the leader alone, so this returns the leader's log
        end and the clamp becomes a no-op. That is load-bearing rather than a
        happy accident: every single-broker run in Phases 0–5 depends on it,
        and a watermark that needed a SECOND replica would leave a one-broker
        cluster unable to serve its own writes.
        """
        in_sync = self.in_sync_replicas(topic, partition, lag_time_max_ms)

        with self._lock:
            if not self._replicas.get((topic, partition)):
                return leader_end_offset

            positions = self._replica_positions.get((topic, partition), {})
            watermark = leader_end_offset
            for node_id in in_sync:
                if node_id == self.node_id:
                    continue                    # us: our own log end
                watermark = min(watermark, positions.get(node_id, 0))
            return watermark

    def replica_positions(self, topic: str, partition: int) -> dict:
        """node_id → last known end offset. For the admin CLI and tests."""
        with self._lock:
            return dict(self._replica_positions.get((topic, partition), {}))

    def leader_epoch(self, topic: str, partition: int) -> int:
        """How many times this partition's leader has changed. 0 if never."""
        with self._lock:
            return self._leader_epochs.get((topic, partition), 0)

    def replicas_for(self, topic: str, partition: int) -> list:
        """Which brokers should hold a copy. The leader alone if unassigned."""
        with self._lock:
            key = (topic, partition)
            if key in self._replicas:
                return list(self._replicas[key])
            leader = self._leaders.get(key)
            return [leader] if leader is not None else []

    def follower_partitions(self) -> list:
        """
        Partitions I should REPLICATE but do not lead.

        The replica fetcher's work list. Note the exclusion of partitions we
        lead: a leader replicating from itself would be a loop that slowly
        doubled its own log.
        """
        with self._lock:
            return sorted(
                (topic, partition)
                for (topic, partition), nodes in self._replicas.items()
                if self.node_id in nodes
                and self._leaders.get((topic, partition)) != self.node_id)

    def leader_entries(self) -> list:
        """Wire form, for CONTROLLER_UPDATE and DESCRIBE_CLUSTER."""
        with self._lock:
            return [{'topic': t, 'partition': p, 'leader': n,
                     'replicas': list(self._replicas.get((t, p), [n])),
                     'isr': list(self._isr.get((t, p),
                                               self._replicas.get((t, p), [n]))),
                     'leader_epoch': self._leader_epochs.get((t, p), 0)}
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
