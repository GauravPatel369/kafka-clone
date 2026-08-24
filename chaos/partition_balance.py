"""
EXPERIMENT: How partitioning distributes load, and what it costs.

    python -m chaos.partition_balance

Not a failure-injection script like the others — this one makes the *routing*
decision visible, because it is the one that decides both your throughput
ceiling and your ordering guarantees.

  EXPERIMENT 1 — keyless messages spread
      The load-balancing case. Sticky partitioning fills one partition's batch
      at a time, so balance appears over batches rather than per message.

  EXPERIMENT 2 — keyed messages pin, and stay ordered
      The ordering case. Every event for a customer lands in one partition and
      is strictly ordered there.

  EXPERIMENT 3 — one key for everything
      The accidental bottleneck. Perfect ordering, and the throughput of a
      single partition forever.

  EXPERIMENT 4 — what changing the partition count does
      The sharpest consequence of key routing, and why partition counts are a
      decision rather than a tuning knob.
"""

import asyncio
import shutil
import tempfile
import threading
from collections import Counter

from broker.broker import Broker
from producer.partitioner import partition_for_key
from producer.producer import Producer

PARTITIONS = 4
MESSAGES = 4000


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def show_spread(counts: Counter, num_partitions: int) -> None:
    total = sum(counts.values())
    widest = max(counts.values()) if counts else 1
    ideal = total / num_partitions

    for partition in range(num_partitions):
        count = counts.get(partition, 0)
        bar = '█' * int(count / widest * 40)
        print(f"    p{partition}  {count:>6}  {bar}")

    worst = max(abs(counts.get(p, 0) - ideal) for p in range(num_partitions))
    print(f"\n    ideal {ideal:,.0f} per partition; worst deviation "
          f"{worst:,.0f} ({worst / ideal:.1%})")


def experiment_keyless(port: int) -> None:
    rule('EXPERIMENT 1 — keyless messages (load balancing)')
    with Producer('127.0.0.1', port, 'keyless', quiet=True, batch_size=2048) as p:
        futures = [p.send('x' * 60) for _ in range(MESSAGES)]
        p.flush()

    print(f"  {MESSAGES:,} messages, no key, {PARTITIONS} partitions\n")
    show_spread(Counter(f.partition for f in futures), PARTITIONS)
    print("""
    Sticky partitioning fills ONE partition's batch, sends it, then moves on.
    Round-robin per message would balance just as well and ruin batching: with
    4 partitions each batch would fill 4x more slowly, so you would send four
    small requests where one large one would do.""")


def experiment_keyed(port: int) -> None:
    rule('EXPERIMENT 2 — keyed messages (ordering per key)')
    keys = [f"cust-{i}" for i in range(500)]

    with Producer('127.0.0.1', port, 'keyed', quiet=True) as p:
        futures = [(key, p.send(f"event-{i}", key=key))
                   for i in range(MESSAGES)
                   for key in [keys[i % len(keys)]]]
        p.flush()

    print(f"  {MESSAGES:,} messages across {len(keys)} keys, "
          f"{PARTITIONS} partitions\n")
    show_spread(Counter(f.partition for _key, f in futures), PARTITIONS)

    # Every message for one key must be in one partition, in ascending order.
    by_key = {}
    for key, future in futures:
        by_key.setdefault(key, []).append((future.partition, future.get()))

    single_partition = sum(1 for entries in by_key.values()
                           if len({p for p, _o in entries}) == 1)
    ordered = sum(1 for entries in by_key.values()
                  if [o for _p, o in entries] == sorted(o for _p, o in entries))

    print(f"\n    keys confined to one partition : {single_partition}/{len(by_key)}")
    print(f"    keys whose offsets ascend      : {ordered}/{len(by_key)}")
    print("""
    That is the guarantee: every event for one customer is in one log, in
    order. Events for DIFFERENT customers interleave freely — and you almost
    never care, which is what makes the trade worth taking.""")


def experiment_one_hot_key(port: int) -> None:
    rule('EXPERIMENT 3 — one key for everything (the accidental bottleneck)')
    with Producer('127.0.0.1', port, 'hotkey', quiet=True) as p:
        futures = [p.send(f"m{i}", key='ALL') for i in range(1000)]
        p.flush()

    print("  1,000 messages, all with key 'ALL'\n")
    show_spread(Counter(f.partition for f in futures), PARTITIONS)
    print("""
    Total ordering, and the throughput of ONE partition no matter how many you
    provisioned. Three partitions sit idle.

    This is a real and common production incident: someone keys by something
    with almost no cardinality — a region code, an environment name, a
    tenant ID where one tenant dominates — and the topic silently stops
    scaling. The cure is a key with enough distinct values to spread across
    the partitions you have.""")


def experiment_repartitioning() -> None:
    rule('EXPERIMENT 4 — what happens when you change the partition count')
    keys = [f"cust-{i}" for i in range(5000)]

    print("  Where do 5,000 keys go, before and after adding one partition?\n")
    for old, new in ((3, 4), (4, 8), (4, 5)):
        moved = sum(1 for k in keys
                    if partition_for_key(k, old) != partition_for_key(k, new))
        print(f"    {old} → {new} partitions:  {moved:,}/{len(keys)} keys "
              f"move ({moved / len(keys):.0%})")

    print("""
    `murmur2(key) % n` depends on n, so changing n remaps most keys.

    A moved key now has its history in one log and its future in another.
    Nothing reads those two in order ever again, and a consumer that assumed
    per-key ordering is silently wrong from that moment on.

    This is why Kafka lets you INCREASE partitions (sometimes you must) and
    forbids DECREASING them (that would also strand data), and why the real
    advice is to over-provision partitions up front. We forbid changes
    entirely rather than ship a foot-gun this project has no need for.

    Note the 4 → 8 row: doubling still moves ~50% of keys. There is no
    "safe" resize. Consistent hashing would reduce the churn, at the cost of
    the simple, portable `hash % n` that lets a Java and a Python producer
    agree without sharing any state.""")


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-balance-')
    broker = Broker(host='127.0.0.1', port=0, data_dir=workdir,
                    default_partitions=PARTITIONS, retention_check_ms=10 ** 7)
    thread = threading.Thread(target=lambda: asyncio.run(broker.start()), daemon=True)
    thread.start()
    if not broker.ready.wait(timeout=5):
        raise SystemExit("Broker failed to start")

    try:
        experiment_keyless(broker.port)
        experiment_keyed(broker.port)
        experiment_one_hot_key(broker.port)
        experiment_repartitioning()

        rule('THE TRADE-OFF, IN ONE PLACE')
        print("""
    key = None        perfect balance, no ordering guarantee at all
    key = customer    ordered per customer, balanced across customers
    key = constant    total ordering, one partition's throughput forever

  Scalability and ordering trade against each other, and the partition key is
  the dial. Kafka does not choose for you, because the right answer depends on
  what your data means — and that is knowledge only the application has.""")
    finally:
        broker.stop()
        thread.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
