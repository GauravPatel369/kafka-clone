"""
CHAOS EXPERIMENT: Kill the controller, and then partition the network.

    python -m chaos.kill_controller

  EXPERIMENT 1 — three brokers, partitions spread across them
  EXPERIMENT 2 — kill the controller; the surviving quorum elects a new one
  EXPERIMENT 3 — what a leadership change costs BEFORE replication exists

The third is the one that matters, and it is uncomfortable. Phase 5 makes the
cluster *route* around a dead broker. It does not make the data survive.
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
from common.protocol import API_CREATE_TOPICS, API_FETCH
from producer.producer import Producer

TOPIC = 'cluster-orders'
PARTITIONS = 6
BROKERS = 3


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def show_placement(brokers: dict, topic: str) -> None:
    alive = [b for b in brokers.values() if b is not None]
    if not alive:
        return
    view = alive[0].cluster

    by_broker = {}
    for partition in range(PARTITIONS):
        leader = view.leader_for(topic, partition)
        by_broker.setdefault(leader, []).append(partition)

    print(f"    {'BROKER':<10}{'STATE':<12}{'LEADS':<26}CONTROLLER")
    for node_id in sorted(brokers):
        instance = brokers[node_id]
        running = instance is not None
        parts = by_broker.get(node_id, [])
        label = ', '.join(f"p{p}" for p in parts) or '(none)'
        marker = '  ◄── controller' if running and instance.cluster.is_controller else ''
        print(f"    {node_id:<10}{'up' if running else 'DOWN':<12}{label:<26}{marker}")

    unassigned = by_broker.get(None, [])
    if unassigned:
        print(f"\n    ⚠ unassigned: {['p%d' % p for p in unassigned]}")


def wait_for(predicate, timeout=15.0, interval=0.15) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-cluster-')
    ports = {i: free_port() for i in range(1, BROKERS + 1)}
    config = ClusterConfig([BrokerAddress(i, '127.0.0.1', ports[i])
                            for i in range(1, BROKERS + 1)])

    brokers, threads = {}, {}

    def start(node_id):
        instance = Broker(
            host='127.0.0.1', port=ports[node_id], node_id=node_id,
            data_dir=f"{workdir}/b{node_id}", cluster_config=config,
            default_partitions=PARTITIONS, retention_check_ms=10 ** 7,
            broker_heartbeat_ms=150, broker_session_timeout_ms=700)
        thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                                  daemon=True)
        thread.start()
        instance.ready.wait(timeout=5)
        brokers[node_id] = instance
        threads[node_id] = thread

    try:
        rule('SETUP')
        for node_id in range(1, BROKERS + 1):
            start(node_id)

        # Wait for BOTH: everyone alive, AND a Raft leader elected.
        #
        # Waiting only for liveness was a real bug in this script. Creating a
        # topic is now a quorum decision, so with no leader yet elected the
        # broker answered LEADER_NOT_AVAILABLE — and because nothing checked
        # the response, the run continued and reported "0 messages stored"
        # with six unassigned partitions and no error anywhere. Exactly the
        # mistake the unchecked-futures comment below warns about, one layer
        # up: a control-plane reply nobody reads is a failure nobody sees.
        wait_for(lambda: all(len(b.cluster.live_brokers()) == BROKERS
                             for b in brokers.values() if b is not None)
                 and any(b.raft.is_leader for b in brokers.values()), timeout=25)
        print(f"  Started {BROKERS} brokers on ports {sorted(ports.values())}")

        with SyncConnection('127.0.0.1', ports[1], timeout=5.0) as conn:
            created = conn.request(API_CREATE_TOPICS,
                                   {'topic': TOPIC, 'partitions': PARTITIONS})
        if created.get('error_code'):
            raise SystemExit(f"  CREATE_TOPICS failed: {created}")

        if not wait_for(lambda: all(
                brokers[1].cluster.leader_for(TOPIC, p) is not None
                for p in range(PARTITIONS)), timeout=15):
            raise SystemExit("  placement never converged")

        # ── EXPERIMENT 1 ───────────────────────────────────────
        rule('EXPERIMENT 1 — partitions spread across the cluster')
        show_placement(brokers, TOPIC)
        print(f"""
    Six partitions over three brokers, two each. The controller — elected by
    a Raft majority, which for three voters is two — decided this, committed
    it to the metadata log, and only then pushed it out. All three agree
    because they agree on the *log*, not because they each guessed the same.

    A client does not need to be told any of it: it asks any broker for
    METADATA, learns the map, and connects to each partition's leader
    directly. No proxy on the data path.""")

        rule('  producing 600 messages, routed by the client')
        with Producer('127.0.0.1', ports[1], TOPIC, quiet=True) as producer:
            futures = [producer.send(f"m{i}", key=f"key-{i}") for i in range(600)]
            producer.flush()

        # CHECK THE FUTURES. An earlier version of this script did not, and
        # silently reported "600 messages produced, 0 stored" while every
        # single send had failed. A future nobody inspects is an error nobody
        # sees — which is the whole reason `get()` re-raises.
        failures = 0
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception:
                failures += 1
        if failures:
            print(f"    ⚠ {failures}/600 sends FAILED")

        counts = {}
        for node_id, instance in brokers.items():
            local = sum(p.size() for (t, _p), p in instance.partitions.items()
                        if t == TOPIC)
            counts[node_id] = local
        print("    messages stored per broker: " +
              "  ".join(f"b{n}={c}" for n, c in sorted(counts.items())))
        print(f"    total: {sum(counts.values())}")
        print("""
    Each broker holds ONLY the partitions it leads. That is the capacity win:
    three disks, three event loops, three NICs.""")

        # ── EXPERIMENT 2 ───────────────────────────────────────
        rule('EXPERIMENT 2 — kill the controller')
        controller_id = brokers[1].cluster.controller_id
        old_term = brokers[controller_id].raft.current_term
        print(f"  broker {controller_id} is the controller, elected in term {old_term}")
        print(f"  killing broker {controller_id}...\n")

        victim = brokers.pop(controller_id)
        victim.stop()
        threads[controller_id].join(timeout=5)
        brokers[controller_id] = None

        survivors = {n: b for n, b in brokers.items() if b is not None}

        # Do NOT assert min(survivors) here. Raft does not promise WHICH node
        # wins, only that at most one can — the winner is whoever's randomised
        # election timer fired first among those with an up-to-date log. A
        # test that pins the identity would be testing the timer, not the
        # algorithm.
        # The leader must be a SURVIVOR, not merely agreed upon. Checking only
        # for agreement passed instantly: immediately after the kill both
        # survivors still named the dead broker, which is one consistent
        # non-None answer and therefore looks exactly like success. A
        # convergence check has to exclude the state it started in.
        elected = wait_for(lambda: len({b.raft.leader_id
                                        for b in survivors.values()}) == 1
                           and all(b.raft.leader_id in survivors
                                   for b in survivors.values()), timeout=25)

        wait_for(lambda: all(b.cluster.controller_id in survivors
                             for b in survivors.values()), timeout=10)
        probe = next(iter(survivors.values()))
        new_controller = probe.cluster.controller_id
        new_term = probe.raft.current_term
        wait_for(lambda: all(b.cluster.leader_for(TOPIC, p) != controller_id
                             for b in survivors.values()
                             for p in range(PARTITIONS)), timeout=15)

        print(f"  controller is now broker {new_controller}, "
              f"term {old_term} → {new_term} (converged: {elected})\n")
        show_placement(brokers, TOPIC)
        print(f"""
    Two of the three voters remain, and two is a majority of three — so the
    survivors could elect. Note the term: every election increments it, which
    is how a returning broker {controller_id} would learn it is stale rather
    than resuming as though nothing happened.

    Which survivor won is deliberately not predictable. Raft guarantees at
    most ONE leader per term, never a particular one; the winner is whoever's
    randomised timer fired first. Had a third broker also died, the last one
    standing would elect nothing at all — one voter cannot reach two.

    The new controller then proposed a placement without the dead broker,
    waited for it to commit, and pushed the committed map.""")

        # ── EXPERIMENT 3 ───────────────────────────────────────
        rule('EXPERIMENT 3 — what that reassignment actually cost')

        reachable = sorted(survivors)[0]
        recovered = 0
        with SyncConnection('127.0.0.1', ports[reachable], timeout=5.0) as conn:
            for partition in range(PARTITIONS):
                leader = survivors[reachable].cluster.leader_for(TOPIC, partition)
                if leader != reachable:
                    continue
                response = conn.request(API_FETCH, {
                    'topic': TOPIC, 'partition': partition, 'offset': 0,
                    'max': 1000})
                recovered += len(response.get('messages', []))

        print(f"  broker {reachable} now leads "
              f"{len([p for p in range(PARTITIONS) if survivors[reachable].cluster.leader_for(TOPIC, p) == reachable])} "
              f"partitions and can serve {recovered} messages from them\n")
        print(f"""    Before the kill, broker {controller_id} held {counts[controller_id]} messages.
    Those partitions now belong to a survivor — and that survivor's log for
    them is EMPTY. The data is still on the dead broker's disk, unreachable.

    **Phase 5 is not fault tolerance.** It gives you capacity and routing:
    a client automatically finds the new leader, and the cluster keeps
    accepting writes. What it does not give you is the old data, because
    leadership moved and the bytes did not.

    That is exactly what Phase 6 fixes. With replicas, the partition already
    exists on another broker, so promoting it is a change of role rather than
    a change of address — and `acks=all` plus the high watermark make sure the
    promoted replica is not missing anything a client was told was committed.""")

        rule('WHAT JUST HAPPENED')
        print("""
  Three mechanisms, each visible above:

    membership   heartbeats every 150 ms; a peer unheard from for 700 ms is
                 down. Shorter detects faster and turns a network blip into a
                 leadership change; longer leaves partitions unserved. The
                 same dilemma as a consumer session timeout, one level up.

    election     a Raft majority. It costs RequestVote/AppendEntries traffic,
                 a durable term and vote, and a replicated log — where the
                 first version of this phase cost ten lines. What that buys
                 is safety under a network partition, which no amount of
                 local cleverness can provide. Run `chaos/split_brain.py`.

    placement    the controller assigns round-robin over live brokers,
                 PROPOSES the result, and applies it only once a majority has
                 stored it. CONTROLLER_UPDATE then delivers committed
                 metadata; it is no longer a source of truth of its own.

  And one honest gap: leadership moved, data did not. Phase 6.""")

    finally:
        for node_id, instance in brokers.items():
            if instance is not None:
                instance.stop()
        for thread in threads.values():
            thread.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
