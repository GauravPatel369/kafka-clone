"""
CONCEPT: The State Machine — What Committed Entries Mean
----------------------------------------------------------
Raft replicates an ordered log. It has no opinion about what the entries say.
This file is the part that does: it applies committed commands to produce the
cluster's metadata.

    log entry                        effect
    ────────────────────────────────────────────────────────────────
    REGISTER_BROKER   {id,host,port}  a broker joins — this is DISCOVERY
    UNREGISTER_BROKER {id}            a broker's lease expired; fence it
    CREATE_TOPIC      {name,parts}    a topic exists, at that width
    ASSIGN_PARTITIONS {leaders}       who leads what


THE PROPERTY THAT MAKES THIS WORK
-----------------------------------
**Every node applies the same commands in the same order, so every node reaches
the same state.** That is a deterministic state machine over a replicated log —
the standard construction, and the reason Raft is useful rather than merely
interesting.

Determinism is a real constraint on what may go in here. No clocks, no random
numbers, no reading local files, no "whatever this node happens to think".
Two nodes applying entry 47 must agree exactly, or they diverge with nothing to
detect it.

Note what that rules out: the *placement decision* — round-robin over live
brokers — is made by the LEADER, before proposing, and the result is written
into the entry. Recomputing it during `apply()` would let two nodes with
slightly different views of liveness compute different answers from the same
log. **Decide once, replicate the decision, not the inputs.**


WHY DISCOVERY LIVES HERE
--------------------------
Phase 5 kept broker membership in each node's memory, derived from heartbeats.
Two consequences, both bad: it did not survive a controller change, and every
node had its own opinion of it.

As a log entry, registration is durable, ordered, and agreed by a majority
before it counts. That is the whole of the discovery service — no second
system, just entries in the log we already needed.
"""

import time


class MetadataStateMachine:
    """
    Cluster metadata derived from the committed prefix of the Raft log.

    Read by the broker to answer METADATA and to decide whether it leads a
    partition. Written only by `apply()`, only from committed entries, only in
    log order.
    """

    def __init__(self):
        self.brokers = {}          # node_id -> {'host', 'port', 'registered_at'}
        self.topics = {}           # name    -> partition count
        self.leaders = {}          # (topic, partition) -> node_id

        # name -> how many copies of each partition (Phase 6). Separate from
        # `topics` so that replaying a pre-Phase-6 CREATE_TOPIC leaves it
        # alone rather than resetting a factor to 1.
        self.replication_factors = {}

        # (topic, partition) -> int. Bumped every time THIS PARTITION's leader
        # changes (Phase 6 step 6).
        #
        # CONCEPT: the Raft term is NOT the leader epoch, and conflating them
        # is a subtle bug rather than a shortcut.
        #
        #   The term increments when the CONTROLLER changes. The epoch must
        #   increment when a PARTITION's leader changes. One controller
        #   reassigns many partitions across many terms, and one term spans
        #   many reassignments — so reusing the term would fence a partition
        #   whose leadership never moved, and fail to fence one that moved
        #   twice inside a single term.
        self.leader_epochs = {}

        # (topic, partition) -> [node_id, ...]. The in-sync subset of the
        # replica set. Committed rather than leader-local because Step 6's
        # leader election reads it: a new leader must come from the in-sync
        # set, so that set has to outlive the leader that computed it.
        self.isr = {}

        # (topic, partition) -> [node_id, ...], leader first (Phase 6).
        #
        # The replica SET, not the in-sync set. This is who is supposed to hold
        # a copy; the ISR is who currently does, and it changes far more often.
        # Keeping them apart matters because the replica set is a placement
        # decision (rare, deliberate) while the ISR is a health observation
        # (frequent, automatic) — routing one through the other would put disk
        # hiccups in the Raft log.
        self.replicas = {}

        # The index of the last entry applied. Lets a caller tell "the state
        # machine is caught up" from "the log is short", which are different
        # situations that look identical from the outside.
        self.last_applied = 0

    # ── Applying ───────────────────────────────────────────────

    def apply(self, entry) -> None:
        """
        Apply one committed entry. Must be deterministic and idempotent-safe.

        Unknown command types are ignored rather than fatal. A newer node may
        write a command an older one does not understand, and refusing to apply
        the rest of the log would take the older node out of the quorum during
        a rolling upgrade — the exact scenario per-API versioning exists to
        survive.
        """
        command = entry.command or {}
        kind = command.get('type')

        if kind == 'REGISTER_BROKER':
            self.brokers[int(command['node_id'])] = {
                'host': command['host'],
                'port': int(command['port']),
                # From the entry, NOT from time.time(). Applying must be
                # deterministic: reading the local clock here would give every
                # node a different answer for the same entry.
                'registered_at': command.get('registered_at', 0),
            }

        elif kind == 'UNREGISTER_BROKER':
            self.brokers.pop(int(command['node_id']), None)

        elif kind == 'CREATE_TOPIC':
            name = command['name']
            partitions = int(command['partitions'])
            # Widen only. A narrowing would strand the data in the dropped
            # partitions and remap every key; no legitimate flow produces one,
            # so treat it as a bug rather than obeying it.
            if self.topics.get(name, 0) < partitions:
                self.topics[name] = partitions

            # How many copies this topic wants. Committed with the topic
            # because it is an input to PLACEMENT, and placement runs on
            # whichever broker is controller — which need not be the one that
            # served the CREATE_TOPICS request. Kept in a separate dict so an
            # entry written before Phase 6 leaves it untouched.
            if 'replication_factor' in command:
                self.replication_factors[name] = int(
                    command['replication_factor'])

        elif kind == 'ASSIGN_PARTITIONS':
            for item in command.get('leaders', []):
                key = (item['topic'], int(item['partition']))
                self.leaders[key] = int(item['leader'])
                # Absent on entries written before Phase 6. An older command
                # replayed from the log must not wipe a replica set, and a
                # leader-only assignment is a legitimate thing to apply — so
                # omitted means "unchanged", never "empty".
                if 'replicas' in item:
                    self.replicas[key] = [int(n) for n in item['replicas']]
                if 'isr' in item:
                    self.isr[key] = [int(n) for n in item['isr']]
                if 'leader_epoch' in item:
                    # Monotonic per partition. Never decreases, even if an
                    # entry proposes a lower one — a stale proposal must not
                    # be able to un-fence a broker that was already fenced.
                    epoch = int(item['leader_epoch'])
                    if epoch > self.leader_epochs.get(key, 0):
                        self.leader_epochs[key] = epoch

        self.last_applied = entry.index

    # ── Reading ────────────────────────────────────────────────

    def live_broker_ids(self) -> list:
        return sorted(self.brokers)

    def broker_address(self, node_id: int):
        entry = self.brokers.get(node_id)
        return (entry['host'], entry['port']) if entry else None

    def leader_for(self, topic: str, partition: int):
        return self.leaders.get((topic, partition))

    def partition_count(self, topic: str) -> int:
        return self.topics.get(topic, 0)

    def replication_factor(self, topic: str) -> int:
        """How many copies this topic wants. One if it never said."""
        return self.replication_factors.get(topic, 1)

    def broker_list(self) -> list:
        # `registered_at` is carried even though no reader needs it, because
        # this list is what `snapshot()` persists. A field dropped here is a
        # field lost on the next restart, which would leave a node that
        # restored from a snapshot holding subtly different state from one that
        # replayed the log — divergence with nothing to detect it.
        return [{'node_id': node_id, 'host': entry['host'],
                 'port': entry['port'],
                 'registered_at': entry.get('registered_at', 0)}
                for node_id, entry in sorted(self.brokers.items())]

    def replicas_for(self, topic: str, partition: int) -> list:
        """
        Which brokers should hold a copy. Falls back to the leader alone.

        The fallback keeps every Phase 5 code path working unchanged: a
        partition assigned before replica sets existed has exactly one copy,
        which is the truth about it rather than a placeholder.
        """
        key = (topic, partition)
        if key in self.replicas:
            return list(self.replicas[key])
        leader = self.leaders.get(key)
        return [leader] if leader is not None else []

    def isr_for(self, topic: str, partition: int) -> list:
        """The committed in-sync set. Falls back to the whole replica set."""
        key = (topic, partition)
        if key in self.isr:
            return list(self.isr[key])
        return self.replicas_for(topic, partition)

    def leader_epoch(self, topic: str, partition: int) -> int:
        """How many times this partition's leader has changed. 0 if never."""
        return self.leader_epochs.get((topic, partition), 0)

    def leader_entries(self) -> list:
        return [{'topic': topic, 'partition': partition, 'leader': node,
                 'replicas': self.replicas_for(topic, partition),
                 'isr': self.isr_for(topic, partition),
                 'leader_epoch': self.leader_epoch(topic, partition)}
                for (topic, partition), node in sorted(self.leaders.items())]

    # ── Placement, computed by the leader before proposing ─────

    def compute_assignment(self, broker_ids: list = None,
                           replication_factor: int = 1,
                           topics: dict = None) -> list:
        """
        Round-robin every declared partition over the live brokers, with
        `replication_factor` copies of each.

        Called by the LEADER to build an `ASSIGN_PARTITIONS` command. The
        result is written into the log entry, so every node applies the same
        decision rather than recomputing it — see the module docstring on
        determinism.

        Sorted inputs, sorted iteration: the same inputs must give the same
        answer, or leadership churns for no reason on every recomputation.

        CONCEPT: replicas are the brokers FOLLOWING the leader in the ring.

            live = [1, 2, 3],  RF = 3
            orders-0  leader 1  replicas [1, 2, 3]
            orders-1  leader 2  replicas [2, 3, 1]
            orders-2  leader 3  replicas [3, 1, 2]

        Two properties this has to satisfy, both easy to break:

          - **No broker appears twice in one replica set.** Two "copies" on one
            machine is one copy that costs twice as much, and it would make a
            replication factor of 3 survive only one failure.
          - **Leadership spreads.** Taking the same N brokers for every
            partition would replicate correctly and pile every leader onto one
            machine, so the cluster would be durable and unbalanced.

        Walking the ring from a rotating start gives both for free, which is
        why real Kafka does the same thing.

        `replication_factor` is an int, or a callable taking a topic name and
        returning one — because factors are per topic, and the caller (the
        controller) is the only thing that knows them. Passing a function keeps
        that lookup out of here: this method must stay a pure function of its
        arguments, so that the caller and every applier can be shown to agree.

        `topics` likewise defaults to our own committed set but may be passed
        in, and the reason is a real hole rather than flexibility for its own
        sake: **auto-created topics never reach the Raft log.** A topic born
        inside a PRODUCE exists in the broker's local metadata store and
        nowhere else, so a placement computed only from committed state would
        leave every auto-created partition unassigned and unservable. The
        controller therefore supplies the union it actually holds.
        """
        live = sorted(broker_ids if broker_ids is not None else self.brokers)
        if not live:
            return []

        widths = self.topics if topics is None else topics
        factor_for = (replication_factor if callable(replication_factor)
                      else lambda _topic: replication_factor)

        assignment = []
        index = 0
        for topic in sorted(widths):
            # More copies than machines is not extra redundancy — it is the
            # same machine listed twice. Clamp, so the assignment stays honest
            # about how many failures it actually survives.
            factor = max(1, min(int(factor_for(topic)), len(live)))

            for partition in range(widths[topic]):
                start = index % len(live)
                replicas = [live[(start + offset) % len(live)]
                            for offset in range(factor)]
                assignment.append({'topic': topic, 'partition': partition,
                                   'leader': replicas[0],
                                   'replicas': replicas})
                index += 1
        return assignment

    def assignment_differs(self, proposed: list) -> bool:
        """
        Would this assignment change anything?

        Guards against proposing an identical `ASSIGN_PARTITIONS` on every
        tick, which would grow the log without bound and force a majority
        round trip to record that nothing happened.

        Compares the replica set as well as the leader. A move that only
        changes *where the copies live* is a real change — data has to be
        replicated to a new machine — and comparing leaders alone would decide
        it was a no-op and never propose it.
        """
        def index(entries):
            return {(item['topic'], int(item['partition'])):
                    (int(item['leader']),
                     tuple(int(n) for n in item.get('replicas') or ()),
                     tuple(sorted(int(n) for n in item.get('isr') or ())),
                     int(item.get('leader_epoch') or 0))
                    for item in entries}

        return index(self.leader_entries()) != index(proposed)

    def snapshot(self) -> dict:
        """
        The complete state, serialisable, with no reference to the log.

        Written verbatim into a Raft snapshot, so it must contain *everything*
        `apply()` can change. Anything omitted here is silently reset the next
        time a node restarts from a snapshot instead of from the log.
        """
        return {
            'brokers': self.broker_list(),
            'topics': dict(sorted(self.topics.items())),
            'replication_factors': dict(sorted(self.replication_factors.items())),
            'partitions': self.leader_entries(),
            'last_applied': self.last_applied,
        }

    def restore(self, state: dict) -> None:
        """
        Replace this state machine's contents with a snapshot. The inverse of
        `snapshot()`.

        REPLACE, not merge. A node restoring a snapshot is being told "this is
        the entire truth as of index N" — anything it held that the snapshot
        does not mention was either never real or was since removed, and
        keeping it would resurrect an entry that a majority already agreed to
        delete. Clearing first is the difference between restoring state and
        accumulating it.
        """
        self.brokers = {}
        self.topics = {}
        self.leaders = {}
        self.replicas = {}
        self.isr = {}
        self.leader_epochs = {}
        self.replication_factors = {}

        for entry in state.get('brokers', []):
            self.brokers[int(entry['node_id'])] = {
                'host': entry['host'],
                'port': int(entry['port']),
                'registered_at': entry.get('registered_at', 0),
            }

        for name, partitions in state.get('topics', {}).items():
            self.topics[name] = int(partitions)

        for name, factor in state.get('replication_factors', {}).items():
            self.replication_factors[name] = int(factor)

        for item in state.get('partitions', []):
            key = (item['topic'], int(item['partition']))
            self.leaders[key] = int(item['leader'])
            if item.get('replicas'):
                self.replicas[key] = [int(n) for n in item['replicas']]
            if item.get('isr'):
                self.isr[key] = [int(n) for n in item['isr']]
            if item.get('leader_epoch'):
                self.leader_epochs[key] = int(item['leader_epoch'])

        self.last_applied = int(state.get('last_applied', 0))

    def __repr__(self) -> str:
        return (f"MetadataStateMachine(applied={self.last_applied}, "
                f"{len(self.brokers)} brokers, {len(self.topics)} topics)")


def registration_command(node_id: int, host: str, port: int) -> dict:
    """Build a REGISTER_BROKER command. The timestamp is fixed at proposal."""
    return {'type': 'REGISTER_BROKER', 'node_id': node_id,
            'host': host, 'port': port, 'registered_at': int(time.time())}
