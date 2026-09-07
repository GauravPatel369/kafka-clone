"""
CHAOS EXPERIMENT: Kill a partition leader mid-produce. Twice.

    python -m chaos.kill_leader

This is the demonstration the whole of Phase 6 was built for, and it is the
same shape as `chaos/kill_consumer_midprocess.py`: run the identical crash
twice, change ONE setting, and watch two different outcomes.

    acks=1     the leader acknowledges as soon as IT has the record
    acks=all   the leader acknowledges once every in-sync replica has it

Both runs kill the same broker at the same point. Both producers are told
their writes succeeded. Only one of them is telling the truth.

WHAT TO WATCH
---------------
The number that matters is not how many messages were produced — it is how
many the producer was **acknowledged** for, compared with how many survive the
failover. A message that was acknowledged and then vanished is the worst
outcome a messaging system can have, because the producer has already moved on
and nothing anywhere reports an error.

Phase 5 lost EVERYTHING on this test: leadership moved and the bytes did not.
Steps 1-5 made copies. Step 6 is what promotes a copy that actually has the
data, which is why `acks=all` can now reconcile exactly.
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
from common.protocol import API_CREATE_TOPICS, API_FETCH, API_PRODUCE

TOPIC = 'orders'
BROKERS = 3
COUNT = 400
LAG_MS = 1500


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_for(predicate, timeout=30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.15)
    return False


def run_experiment(acks, label: str) -> dict:
    """Produce with `acks`, kill the leader halfway, count what survives."""
    workdir = tempfile.mkdtemp(prefix='kafka-killleader-')
    ports = {i: free_port() for i in range(1, BROKERS + 1)}
    config = ClusterConfig([BrokerAddress(i, '127.0.0.1', ports[i])
                            for i in range(1, BROKERS + 1)])

    brokers = {}
    for node_id in range(1, BROKERS + 1):
        instance = Broker(
            host='127.0.0.1', port=ports[node_id], node_id=node_id,
            data_dir=f"{workdir}/b{node_id}", cluster_config=config,
            default_partitions=1, retention_check_ms=10 ** 7,
            broker_heartbeat_ms=120, broker_session_timeout_ms=600,
            replica_lag_time_max_ms=LAG_MS, min_insync_replicas=2)
        threading.Thread(target=lambda b=instance: asyncio.run(b.start()),
                         daemon=True).start()
        instance.ready.wait(timeout=5)
        brokers[node_id] = instance

    if not wait_for(lambda: all(len(b.cluster.live_brokers()) == BROKERS
                                for b in brokers.values())
                    and any(b.raft.is_leader for b in brokers.values())):
        raise SystemExit("cluster never converged")

    controller = next(i for i, b in brokers.items() if b.raft.is_leader)
    with SyncConnection('127.0.0.1', ports[controller], timeout=5.0) as conn:
        created = conn.request(API_CREATE_TOPICS, {
            'topic': TOPIC, 'partitions': 1, 'replication_factor': 3})
        if created.get('error_code'):
            raise SystemExit(f"CREATE_TOPICS failed: {created}")

    wait_for(lambda: len(brokers[controller].cluster.replicas_for(TOPIC, 0)) == 3)
    wait_for(lambda: len(brokers[controller].cluster.isr_for(TOPIC, 0)) == 3, 20)

    leader = brokers[controller].cluster.leader_for(TOPIC, 0)
    replicas = brokers[controller].cluster.replicas_for(TOPIC, 0)
    survivors = [n for n in brokers if n != leader]

    print(f"  {label}: orders-0 leader={leader} replicas={replicas} "
          f"ISR={brokers[controller].cluster.isr_for(TOPIC, 0)}")

    # ── Produce, killing the leader halfway ────────────────────
    acknowledged = []
    killed_at = None

    with SyncConnection('127.0.0.1', ports[leader], timeout=20.0) as conn:
        for i in range(COUNT):
            if i == COUNT // 2:
                # Kill it the way a machine dies: stop answering, without
                # telling anyone. `partitioned_from` on every side is the
                # closest this harness gets to pulling the power cable.
                killed_at = len(acknowledged)
                for node_id, instance in brokers.items():
                    if node_id == leader:
                        instance.partitioned_from = set(survivors)
                    else:
                        instance.partitioned_from = {leader}

            try:
                response = conn.request(
                    API_PRODUCE, {'topic': TOPIC, 'partition': 0,
                                  'value': f"msg-{i}", 'acks': acks})
            except Exception:
                continue                    # connection died with the broker

            # ONLY count a message the broker said yes to. That is the promise
            # being tested — everything else is a message the producer knows
            # it must retry.
            if response.get('error_code') == 0:
                acknowledged.append(i)

    # ── Wait for the survivors to elect a new leader ───────────
    elected = wait_for(
        lambda: brokers[survivors[0]].cluster.leader_for(TOPIC, 0) in survivors,
        timeout=30)
    new_leader = brokers[survivors[0]].cluster.leader_for(TOPIC, 0)

    # ── Count what a consumer can actually read ────────────────
    readable = set()
    if new_leader in survivors:
        with SyncConnection('127.0.0.1', ports[new_leader], timeout=20.0) as conn:
            offset = 0
            while True:
                response = conn.request(API_FETCH, {
                    'topic': TOPIC, 'partition': 0,
                    'offset': offset, 'max': 500})
                messages = response.get('messages') or []
                if not messages:
                    break
                for message in messages:
                    readable.add(int(message['value'].split('-')[1]))
                offset = messages[-1]['offset'] + 1

    lost = [i for i in acknowledged if i not in readable]

    for instance in brokers.values():
        instance.partitioned_from = set()
    time.sleep(0.3)

    return {
        'acks': label, 'old_leader': leader, 'new_leader': new_leader,
        'elected': elected, 'acknowledged': len(acknowledged),
        'killed_after': killed_at, 'readable': len(readable),
        'lost': len(lost), 'epoch': brokers[survivors[0]].cluster.leader_epoch(TOPIC, 0),
    }


def main() -> None:
    logging.disable(logging.CRITICAL)

    rule('EXPERIMENT 1 — acks=all')
    print("  The leader answers only once every in-sync replica holds the\n"
          "  record. A message that was acknowledged is, by definition, on\n"
          "  more than one machine.\n")
    strong = run_experiment('all', 'acks=all')

    rule('EXPERIMENT 2 — acks=1')
    print("  The leader answers as soon as ITS OWN log has the record. The\n"
          "  followers will catch up shortly — unless the leader dies first.\n")
    weak = run_experiment(1, 'acks=1  ')

    rule('RESULTS')
    print(f"    {'SETTING':<12}{'ACKED':<9}{'READABLE':<11}{'LOST':<8}"
          f"{'FAILOVER':<12}EPOCH")
    for result in (strong, weak):
        print(f"    {result['acks']:<12}{result['acknowledged']:<9}"
              f"{result['readable']:<11}{result['lost']:<8}"
              f"{result['old_leader']} → {result['new_leader']:<8}"
              f"{result['epoch']}")

    rule('WHAT JUST HAPPENED')
    print(f"""
  Both runs killed the leader of orders-0 halfway through producing {COUNT}
  messages. Both producers were told their writes succeeded. One was right.

    acks=all   {strong['lost']} of {strong['acknowledged']} acknowledged messages lost
    acks=1     {weak['lost']} of {weak['acknowledged']} acknowledged messages lost

  The {weak['lost']} missing messages in the second run were in the dead leader's log
  and had not reached a follower when it died. The producer has already
  moved on. Nothing anywhere reports an error. That is not a bug — it is
  precisely what acks=1 means, and it is why the setting exists.

  THREE MECHANISMS MADE THE FIRST RUN POSSIBLE

    replication    the surviving brokers had already fetched the data. In
                   Phase 5 this test lost EVERYTHING, because leadership
                   moved and the bytes did not.

    the ISR        the controller promoted a replica from the in-sync set.
                   An in-sync replica holds everything below the high
                   watermark by definition, so every record a consumer was
                   allowed to read was already on the new leader. Promoting
                   outside the ISR would throw that guarantee away.

    the epoch      leadership moved from broker {strong['old_leader']} to {strong['new_leader']}, and the
                   partition's leader epoch became {strong['epoch']}. A client that has
                   refreshed its metadata now carries epoch {strong['epoch']}, and the old
                   leader — still believing it is at epoch {strong['epoch'] - 1} — refuses that
                   request with FENCED_LEADER_EPOCH rather than writing into
                   a log nobody will ever read.

  TWO HONEST LIMITS

    1. THE EPOCH DOES NOT SAVE acks=1 HERE, and it is worth being exact
       about why. Fencing needs SOMEBODY to know the newer number. A client
       that has not refreshed sends no epoch, and an isolated leader cannot
       learn one — so for the {weak['lost']} messages above, nothing in the system was
       in a position to object. Short of a leader refusing to serve when it
       cannot reach its own ISR, no mechanism stops that; what stops it is
       acks=all, which the isolated leader physically could not satisfy.

       The epoch fences the case where knowledge EXISTS somewhere: a
       refreshed client, or a returning broker. That is a narrower promise
       than "stale leaders cannot write", and the narrower promise is the
       true one.

    2. acks=all is at-least-once, not exactly-once. A producer whose request
       times out cannot know whether the write landed, so it must retry —
       and the retry may duplicate. Kafka closes that with an idempotent
       producer: a producer ID plus a per-partition sequence number, with
       the broker dropping a sequence it has already seen. We do not have
       one.""")


if __name__ == '__main__':
    main()
