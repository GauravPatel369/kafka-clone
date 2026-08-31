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

        elif kind == 'ASSIGN_PARTITIONS':
            for item in command.get('leaders', []):
                self.leaders[(item['topic'], int(item['partition']))] = \
                    int(item['leader'])

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

    def leader_entries(self) -> list:
        return [{'topic': topic, 'partition': partition, 'leader': node}
                for (topic, partition), node in sorted(self.leaders.items())]

    # ── Placement, computed by the leader before proposing ─────

    def compute_assignment(self, broker_ids: list = None) -> list:
        """
        Round-robin every declared partition over the live brokers.

        Called by the LEADER to build an `ASSIGN_PARTITIONS` command. The
        result is written into the log entry, so every node applies the same
        decision rather than recomputing it — see the module docstring on
        determinism.

        Sorted inputs, sorted iteration: the same inputs must give the same
        answer, or leadership churns for no reason on every recomputation.
        """
        live = sorted(broker_ids if broker_ids is not None else self.brokers)
        if not live:
            return []

        assignment = []
        index = 0
        for topic in sorted(self.topics):
            for partition in range(self.topics[topic]):
                assignment.append({'topic': topic, 'partition': partition,
                                   'leader': live[index % len(live)]})
                index += 1
        return assignment

    def assignment_differs(self, proposed: list) -> bool:
        """
        Would this assignment change anything?

        Guards against proposing an identical `ASSIGN_PARTITIONS` on every
        tick, which would grow the log without bound and force a majority
        round trip to record that nothing happened.
        """
        current = {(item['topic'], int(item['partition'])): int(item['leader'])
                   for item in self.leader_entries()}
        incoming = {(item['topic'], int(item['partition'])): int(item['leader'])
                    for item in proposed}
        return current != incoming

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

        for entry in state.get('brokers', []):
            self.brokers[int(entry['node_id'])] = {
                'host': entry['host'],
                'port': int(entry['port']),
                'registered_at': entry.get('registered_at', 0),
            }

        for name, partitions in state.get('topics', {}).items():
            self.topics[name] = int(partitions)

        for item in state.get('partitions', []):
            self.leaders[(item['topic'], int(item['partition']))] = \
                int(item['leader'])

        self.last_applied = int(state.get('last_applied', 0))

    def __repr__(self) -> str:
        return (f"MetadataStateMachine(applied={self.last_applied}, "
                f"{len(self.brokers)} brokers, {len(self.topics)} topics)")


def registration_command(node_id: int, host: str, port: int) -> dict:
    """Build a REGISTER_BROKER command. The timestamp is fixed at proposal."""
    return {'type': 'REGISTER_BROKER', 'node_id': node_id,
            'host': host, 'port': port, 'registered_at': int(time.time())}
