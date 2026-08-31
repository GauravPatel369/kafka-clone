"""
CHAOS EXPERIMENT: A follower that fell off the back of the log.

    python -m chaos.lagging_follower

Snapshotting buys a bounded log and a fast restart, and it introduces a failure
that could not previously exist: **a follower needing an entry the leader has
already thrown away.**

Before compaction, "behind" was one state and always recoverable. The leader
backed `next_index` up until the two logs met, and they always met, because
index 1 was always there. After compaction, "behind" splits in two:

    next_index >= first_index    behind, recoverable      AppendEntries
    next_index <  first_index    behind, STUCK            InstallSnapshot

This script drives a follower into the second row and watches one
`InstallSnapshot` do what any number of `AppendEntries` could not.

The snapshot threshold is forced down to 4. The production default is 1000,
which this cluster's metadata log never approaches — the honest consequence
being that without this script the whole mechanism would never run outside its
unit tests. See docs/phase-5/raft-snapshots.md §6.
"""

import asyncio
import logging
import socket
import tempfile
import threading
import time

from broker.broker import Broker
from common.cluster_config import BrokerAddress, ClusterConfig
from common.net import SyncConnection
from common.protocol import API_CREATE_TOPICS

BROKERS = 3
SNAPSHOT_THRESHOLD = 4
TOPICS = 12


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for(predicate, timeout=25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.15)
    return False


def show_logs(brokers: dict, note: str = '') -> None:
    print(f"    {'BROKER':<9}{'ROLE':<12}{'LOG HOLDS':<16}{'SNAPSHOT':<11}"
          f"{'COMMIT':<9}TOPICS KNOWN")
    for node_id in sorted(brokers):
        raft = brokers[node_id].raft
        held = (f"{raft.log.first_index}..{raft.log.last_index}"
                if raft.log.last_index >= raft.log.first_index else 'nothing')
        snapshot = ('none' if not raft.log.snapshot_last_index
                    else f"@{raft.log.snapshot_last_index}")
        print(f"    {node_id:<9}{raft.role:<12}{held:<16}{snapshot:<11}"
              f"{raft.commit_index:<9}{len(raft.machine.topics)}")
    if note:
        print(note)


def main() -> None:
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-lag-')
    ports = {i: free_port() for i in range(1, BROKERS + 1)}
    config = ClusterConfig([BrokerAddress(i, '127.0.0.1', ports[i])
                            for i in range(1, BROKERS + 1)])

    brokers = {}
    for node_id in range(1, BROKERS + 1):
        instance = Broker(
            host='127.0.0.1', port=ports[node_id], node_id=node_id,
            data_dir=f"{workdir}/b{node_id}", cluster_config=config,
            default_partitions=2, retention_check_ms=10 ** 7,
            broker_heartbeat_ms=120, broker_session_timeout_ms=600,
            raft_snapshot_threshold=SNAPSHOT_THRESHOLD)
        threading.Thread(target=lambda b=instance: asyncio.run(b.start()),
                         daemon=True).start()
        instance.ready.wait(timeout=5)
        brokers[node_id] = instance

    try:
        rule('SETUP — three brokers, snapshot threshold 4')
        if not wait_for(lambda: all(len(b.cluster.live_brokers()) == BROKERS
                                    for b in brokers.values())
                        and any(b.raft.is_leader for b in brokers.values())):
            raise SystemExit("cluster never converged; nothing to demonstrate")

        leader_id = next(i for i, b in brokers.items() if b.raft.is_leader)
        victim_id = next(i for i in brokers if i != leader_id)
        show_logs(brokers)
        print(f"\n    Broker {leader_id} leads the quorum. Nothing has been"
              f" proposed yet, so every\n    log is empty and every node"
              f" agrees on that.")

        # ── Cut one follower off ───────────────────────────────
        rule(f'PARTITION — broker {victim_id} is cut off from the quorum')

        others = set(brokers) - {victim_id}
        for node_id, instance in brokers.items():
            instance.partitioned_from = ({victim_id} if node_id in others
                                         else others)

        print(f"  Broker {victim_id} is still RUNNING. It simply cannot hear"
              f" anyone,\n  and nobody can hear it. The other two are a"
              f" majority of three, so\n  the cluster keeps working without"
              f" it.\n")

        # ── Push the log past it ───────────────────────────────
        rule('WORK CONTINUES — and the leader compacts past the absentee')

        with SyncConnection('127.0.0.1', ports[leader_id], timeout=5.0) as conn:
            for i in range(TOPICS):
                response = conn.request(API_CREATE_TOPICS,
                                        {'topic': f'orders-{i}',
                                         'partitions': 2})
                if response.get('error_code'):
                    raise SystemExit(
                        f"CREATE_TOPICS failed: {response}")

        leader = brokers[leader_id]
        wait_for(lambda: leader.raft.log.snapshot_last_index >= SNAPSHOT_THRESHOLD)

        show_logs(brokers)
        first = leader.raft.log.first_index
        print(f"""
    Broker {leader_id}'s log now STARTS at index {first}. Everything before it
    was applied, folded into a snapshot, and deleted.

    Broker {victim_id} is still waiting for index 1 — which no longer exists
    anywhere in the cluster. No amount of backing up `next_index` can help;
    the leader cannot build a message it has no `prev_log_term` for. This is
    the state that compaction created and `AppendEntries` cannot leave.""")

        # ── Heal ───────────────────────────────────────────────
        rule('HEAL — one InstallSnapshot, and it is an ordinary follower again')

        for instance in brokers.values():
            instance.partitioned_from = set()

        converged = wait_for(
            lambda: brokers[victim_id].raft.commit_index >= leader.raft.commit_index)

        show_logs(brokers)
        victim = brokers[victim_id]
        point = victim.raft.log.snapshot_last_index
        topics = len(victim.raft.machine.topics)
        print(f"""
    Converged: {converged}. Broker {victim_id} knows about {topics} topics
    whose log entries exist NOWHERE — not on it, not on the leader, not on
    disk. It has the *result* without the history, which is the whole point.

    Look at its SNAPSHOT column: @{point}, the same as everyone else's. It did
    not replay {point} entries to get there. `reset_to_snapshot` threw its
    entire log away and restarted indexing at {point}, and the ordinary
    AppendEntries that followed carried {point + 1} onward — verified against
    the relocated sentinel, which is the one thing compaction must never
    delete.

    Two round trips, where `AppendEntries` alone had no path at all.""")

        rule('WHAT JUST HAPPENED')
        print(f"""
  Three mechanisms, each visible above:

    compaction       a node folds its APPLIED prefix into a snapshot and
                     deletes it. No consensus needed — every node holds the
                     same committed prefix, so each compacts its own copy
                     whenever it likes. Two nodes at different snapshot
                     indices are not inconsistent.

    the new failure  compaction is what makes a follower unrecoverable by
                     replication alone. The mechanism that bounds the log is
                     the same one that opens the hole.

    InstallSnapshot  the only place in this system where a node discards
                     committed local state because a peer told it to. That is
                     why the term check and the "never move backwards" guard
                     are a fence around a destructive operation rather than
                     routine validation — remove either and a stale or
                     duplicated message rewinds a follower silently.

  And one honest note: with the production threshold of 1000, none of this
  would have run. A metadata log here reaches tens of entries. This script
  exists because code that never executes is not tested — it is merely
  present, and this project has already shipped a cluster-fatal bug that
  every test agreed with.""")

    finally:
        for instance in brokers.values():
            instance.partitioned_from = set()
        time.sleep(0.3)


if __name__ == '__main__':
    main()
