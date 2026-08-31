"""
CHAOS EXPERIMENT: The same partition that broke Phase 5, under Raft.

    python -m chaos.split_brain

Phase 5 elected its controller as `min(live_broker_ids)` — ten lines, no
messages, deterministic **given the same view of who is alive**. Partition the
network and each side gets a different view, so each side elects a different
controller. This script used to print two of them, with every partition
assigned differently and no error anywhere.

Leadership now comes from a Raft quorum. A candidate needs votes from a
majority of ALL voters, not of those it can reach — and two disjoint majorities
of one set cannot exist. So the minority side cannot elect, full stop.

Run it and count the controllers.
"""

import asyncio
import shutil
import socket
import tempfile
import threading
import time

from broker.broker import Broker
from common.cluster_config import BrokerAddress, ClusterConfig
from common.net import SyncConnection
from common.protocol import API_CREATE_TOPICS

TOPIC = 'split-orders'
PARTITIONS = 6
BROKERS = 5


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for(predicate, timeout=20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.15)
    return False


def show_views(brokers: dict) -> None:
    print(f"    {'BROKER':<9}{'SEES LIVE':<16}{'RAFT ROLE':<12}"
          f"{'TERM':<7}{'CONTROLLER':<13}LEADS")
    for node_id in sorted(brokers):
        broker = brokers[node_id]
        state, raft = broker.cluster, broker.raft
        live = ','.join(str(n) for n in state.live_brokers())
        controller = ('none' if state.controller_id is None
                      else str(state.controller_id))
        marker = '  ◄── in charge' if state.is_controller else ''
        print(f"    {node_id:<9}{live:<16}{raft.role:<12}{raft.current_term:<7}"
              f"{controller:<13}{len(state.my_partitions())}{marker}")


def controllers(brokers: dict) -> list:
    return sorted({b.cluster.controller_id for b in brokers.values()
                   if b.cluster.controller_id is not None})


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-split-')
    ports = {i: free_port() for i in range(1, BROKERS + 1)}
    config = ClusterConfig([BrokerAddress(i, '127.0.0.1', ports[i])
                            for i in range(1, BROKERS + 1)])

    brokers, threads = {}, {}
    for node_id in range(1, BROKERS + 1):
        instance = Broker(
            host='127.0.0.1', port=ports[node_id], node_id=node_id,
            data_dir=f"{workdir}/b{node_id}", cluster_config=config,
            default_partitions=PARTITIONS, retention_check_ms=10 ** 7,
            broker_heartbeat_ms=120, broker_session_timeout_ms=600)
        thread = threading.Thread(target=lambda b=instance: asyncio.run(b.start()),
                                  daemon=True)
        thread.start()
        instance.ready.wait(timeout=5)
        brokers[node_id] = instance
        threads[node_id] = thread

    try:
        rule('SETUP — a healthy five-broker cluster')
        wait_for(lambda: all(len(b.cluster.live_brokers()) == BROKERS
                             for b in brokers.values())
                 and any(b.raft.is_leader for b in brokers.values()))

        leader_id = next(i for i, b in brokers.items() if b.raft.is_leader)
        with SyncConnection('127.0.0.1', ports[leader_id], timeout=5.0) as conn:
            conn.request(API_CREATE_TOPICS,
                         {'topic': TOPIC, 'partitions': PARTITIONS})

        wait_for(lambda: all(
            b.cluster.leader_for(TOPIC, p) is not None
            for b in brokers.values() for p in range(PARTITIONS)))

        show_views(brokers)
        print(f"""
    One controller — broker {leader_id} — elected by a majority of 5, which is 3.
    The topic was created through the Raft log, so all five brokers agree on
    which partitions exist and who leads each one.""")

        # ── The partition ──────────────────────────────────────
        rule('PARTITION THE NETWORK — {1,2} cannot reach {3,4,5}')

        left, right = {1, 2}, {3, 4, 5}
        for node_id, instance in brokers.items():
            instance.partitioned_from = right if node_id in left else left

        print("  brokers 1,2 and 3,4,5 can no longer hear each other.")
        print("  Both halves are still RUNNING and still accepting connections.\n")

        minority = {i: brokers[i] for i in left}
        majority = {i: brokers[i] for i in right}

        wait_for(lambda: (any(b.raft.is_leader for b in majority.values())
                          and not any(b.raft.is_leader
                                      for b in minority.values())), timeout=25)
        time.sleep(2.0)
        show_views(brokers)

        found = controllers(brokers)
        minority_controllers = [i for i, b in minority.items()
                                if b.cluster.is_controller]

        print(f"""
    **{len(found)} controller{'s' if len(found) != 1 else ''}: {found}.**

    The minority side {sorted(left)} has {len(minority_controllers)} controllers.
    It is not slow, or unlucky, or still trying — it is ARITHMETICALLY unable
    to elect. Two voters cannot reach three, and no amount of time changes
    that.

    Note the TERM column: the minority keeps campaigning, so its term climbs
    while it never wins. That is the correct behaviour, not a bug — it is a
    node saying "I would like to lead" and being refused, repeatedly.""")

        # ── The comparison ─────────────────────────────────────
        rule('THE MAPS NOW DIFFER — BUT ONLY ONE SIDE MAY CHANGE THEM')

        left_view = {p: brokers[1].cluster.leader_for(TOPIC, p)
                     for p in range(PARTITIONS)}
        right_view = {p: brokers[3].cluster.leader_for(TOPIC, p)
                      for p in range(PARTITIONS)}
        conflicts = sum(1 for p in range(PARTITIONS)
                        if left_view[p] != right_view[p])

        print(f"    {'PARTITION':<12}{'per broker 1':<18}per broker 3")
        for partition in range(PARTITIONS):
            mark = '' if left_view[partition] == right_view[partition] \
                else '   ◄── CONFLICT'
            print(f"    p{partition:<11}{str(left_view[partition]):<18}"
                  f"{right_view[partition]}{mark}")

        print(f"""
    The two sides disagree about {conflicts} of {PARTITIONS} partitions — and that
    number alone looks identical to Phase 5. It is not the same situation, and
    the difference is the whole phase:

      PHASE 5   two live controllers, both ASSIGNING. Broker 1 decided one
                map, broker 3 decided another, and both kept deciding. Two
                authorities, both acting.

      NOW       one controller (broker {found[0]}) legitimately reassigned the
                partitions after losing brokers 1 and 2. The minority holds the
                OLD map and cannot propose a new one — it has no controller at
                all. One authority, one stale copy.

    A stale map is a recoverable state; two authorities is not. When the
    partition heals, the minority simply catches up to the committed log. In
    Phase 5 there was no log to catch up to, and no rule saying whose writes
    survived.

    THE HONEST REMAINING GAP: brokers 1 and 2 still show partitions under
    LEADS, and would still accept a write for them. Raft fixed the CONTROL
    plane — who may decide — and not the DATA plane. Stopping a stale leader
    from accepting writes needs leader-epoch fencing on the partition itself,
    which is Phase 6, and which uses the Raft term you can see in the TERM
    column above.""")

        # ── Healing ────────────────────────────────────────────
        rule('HEALING THE PARTITION')
        for instance in brokers.values():
            instance.partitioned_from = set()

        wait_for(lambda: len(controllers(brokers)) == 1
                 and len({b.raft.current_term for b in brokers.values()}) == 1,
                 timeout=25)
        time.sleep(0.5)
        show_views(brokers)
        print("""
    Everyone converges on one term and one controller. If the minority's term
    climbed above the majority leader's while it was campaigning, that leader
    steps down and a fresh election runs — which is Raft's "a higher term
    always wins" rule doing its job, and the reason a stale leader can never
    persist.""")

        # ── The lesson ─────────────────────────────────────────
        rule('WHAT CHANGED, AND WHAT IT COST')
        print("""
  ONE LINE OF ALGORITHM:

      Phase 5   controller = min(live_broker_ids)        ← a local opinion
      now       controller = whoever won a majority      ← a quorum decision

  Three properties came with it:

    QUORUM   a candidate needs a majority of ALL voters, so two disjoint
             majorities cannot exist, so two leaders cannot exist.

    TERMS    every election increments a number carried on every message. A
             leader that wakes into a higher term steps down on the first
             message it sees. Phase 5's controller had no way to learn it was
             stale.

    LOG      placement is proposed, replicated, and only applied once a
             majority stores it. CONTROLLER_UPDATE now delivers committed
             metadata rather than being a second source of truth.

  AND THE PRICE, which is not small:

    The minority side is UNAVAILABLE. Brokers 1 and 2 are healthy, running,
    and reachable by their own clients — and they refuse to act. Under
    lowest-live-id they would have carried on serving, and produced data that
    could never be reconciled.

    That is the CAP choice, made explicit. Raft picks consistency. Kafka picks
    Raft because a messaging system that silently accepts irreconcilable
    writes is worse than one that briefly refuses them.

  What this does NOT fix: the partitions on brokers 1 and 2 are still
  unreachable, and their DATA still does not move. Leadership is now safe;
  durability is Phase 6.""")

    finally:
        for instance in brokers.values():
            instance.partitioned_from = set()
            instance.stop()
        for thread in threads.values():
            thread.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
