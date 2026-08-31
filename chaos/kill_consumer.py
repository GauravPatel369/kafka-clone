"""
CHAOS EXPERIMENT: Kill a consumer and watch its partitions get taken over.

    python -m chaos.kill_consumer

Phase 0's chaos script showed what happens when a consumer crashes *while
processing* — the delivery-semantics question. This one shows what happens when
a consumer crashes *and never comes back* — the availability question, which
did not exist before Phase 4 because there was nobody to take over from.

  EXPERIMENT 1 — three consumers divide four partitions
  EXPERIMENT 2 — kill one; the survivors pick up its partitions
  EXPERIMENT 3 — the zombie returns and is fenced

The third is the one worth watching. A consumer that was merely PAUSED — not
dead — wakes up believing it still owns partitions that now belong to somebody
else. Its stale commit would rewind the group. It is rejected.
"""

import asyncio
import shutil
import tempfile
import threading
import time

from broker.broker import Broker
from common.errors import IllegalGenerationError, UnknownMemberIdError
from consumer.consumer import Consumer
from producer.producer import Producer

TOPIC = 'chaos-orders'
PARTITIONS = 4
MESSAGES = 400
GROUP = 'chaos-group'

SESSION_TIMEOUT_MS = 800
HEARTBEAT_MS = 150


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def show_assignments(runners: dict) -> None:
    print(f"    {'CONSUMER':<12}{'PARTITIONS':<22}GENERATION")
    for name, runner in runners.items():
        partitions = sorted(p for _t, p in runner.consumer.assignment)
        label = ', '.join(f"p{p}" for p in partitions) or '(none)'
        print(f"    {name:<12}{label:<22}{runner.consumer.generation_id}")

    owned = [p for r in runners.values() for _t, p in r.consumer.assignment]
    print(f"\n    covered: {sorted(set(owned))}   "
          f"duplicates: {len(owned) - len(set(owned))}")


class ConsumerRunner(threading.Thread):
    """
    One consumer, in its own thread, subscribing and polling forever.

    CONCEPT: why each consumer needs a thread of its own.
      This script originally drove every consumer from one loop, polling them
      in sequence. It deadlocked, and the reason is worth keeping:

        JOIN_GROUP BLOCKS. The coordinator holds it until every member has
        rejoined. So a driver that polls alice and then bob will sit inside
        alice's rejoin — waiting for bob — and never reach bob. The rendezvous
        cannot complete, the rebalance times out, and every member ends up
        alone in its own generation believing it owns everything.

      Real consumers never hit this, because each one is its own process or
      thread and polls independently. A rendezvous protocol requires genuinely
      concurrent participants; simulating them sequentially cannot work, and
      that constraint is a property of the protocol, not of the test harness.
    """

    def __init__(self, name: str, consumer):
        super().__init__(name=f"runner-{name}", daemon=True)
        self.label = name
        self.consumer = consumer
        self.stopped = threading.Event()
        self.consumed = 0

    def run(self) -> None:
        try:
            self.consumer.subscribe([TOPIC])
        except Exception:
            return
        while not self.stopped.is_set():
            try:
                self.consumed += len(self.consumer.poll(max_messages=5))
            except (IllegalGenerationError, UnknownMemberIdError):
                pass
            except Exception:
                return
            time.sleep(0.05)

    def stop(self) -> None:
        self.stopped.set()


def converge(runners: dict, want_members: int, timeout: float = 15.0) -> bool:
    """Wait until every partition is owned by exactly one live member."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        owned = [tp for r in runners.values() for tp in r.consumer.assignment]
        active = sum(1 for r in runners.values() if r.consumer.assignment)
        generations = {r.consumer.generation_id for r in runners.values()}
        if (len(set(owned)) == PARTITIONS and len(owned) == PARTITIONS
                and active == min(want_members, PARTITIONS)
                and len(generations) == 1):
            return True
        time.sleep(0.1)
    return False


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-rebalance-')
    broker = Broker(host='127.0.0.1', port=0, data_dir=workdir,
                    default_partitions=PARTITIONS, retention_check_ms=10 ** 7,
                    group_reap_ms=0.2)
    thread = threading.Thread(target=lambda: asyncio.run(broker.start()), daemon=True)
    thread.start()
    if not broker.ready.wait(timeout=5):
        raise SystemExit("Broker failed to start")

    runners = {}

    def start_consumer(name):
        consumer = Consumer('127.0.0.1', broker.port, group_id=GROUP, quiet=True,
                            session_timeout_ms=SESSION_TIMEOUT_MS,
                            heartbeat_interval_ms=HEARTBEAT_MS)
        consumer.connect()
        runner = ConsumerRunner(name, consumer)
        runners[name] = runner
        runner.start()
        return runner

    try:
        rule('SETUP')
        with Producer('127.0.0.1', broker.port, TOPIC, quiet=True) as producer:
            for i in range(MESSAGES):
                producer.send(f"m{i}", partition=i % PARTITIONS)
            producer.flush()
        print(f"  Produced {MESSAGES} messages across {PARTITIONS} partitions "
              f"of {TOPIC!r}")

        # ── EXPERIMENT 1 ───────────────────────────────────────
        rule('EXPERIMENT 1 — three consumers join one group')

        start_consumer('alice')
        converge(runners, want_members=1)
        print("  alice joined alone and took everything:\n")
        show_assignments(runners)

        for name in ('bob', 'carol'):
            start_consumer(name)
            converge(runners, want_members=len(runners))

        print("\n  after bob and carol joined:\n")
        show_assignments(runners)
        print("""
    Four partitions, three members — so one member holds two. The partition
    count is the CEILING on consumer parallelism: a fifth consumer here would
    sit idle forever.

    Note every member is in the SAME generation. That is what a converged
    group looks like, and members stranded in different generations is what a
    broken rebalance looks like.""")

        # ── EXPERIMENT 2 ───────────────────────────────────────
        rule('EXPERIMENT 2 — kill bob (no clean shutdown)')

        victim_runner = runners.pop('bob')
        victim = victim_runner.consumer
        victim_partitions = sorted(p for _t, p in victim.assignment)
        print(f"  bob owned {['p%d' % p for p in victim_partitions]}")
        print("  killing bob: stop the poll loop, stop heartbeating, drop the")
        print("  socket, no LEAVE_GROUP — what `kill -9` looks like to the")
        print("  coordinator\n")

        victim_runner.stop()
        victim._stop_heartbeat()
        victim._cluster.close()
        victim._heartbeat_conn.close()

        started = time.time()
        ok = converge(runners, want_members=2)
        elapsed = time.time() - started

        print(f"  after ~{elapsed:.1f}s (session timeout is "
              f"{SESSION_TIMEOUT_MS}ms):\n")
        show_assignments(runners)

        recovered = sorted(p for r in runners.values() for _t, p in r.consumer.assignment)
        print(f"""
    bob's partitions {['p%d' % p for p in victim_partitions]} were picked up by
    the survivors. All {PARTITIONS} partitions covered: {recovered == list(range(PARTITIONS))}.

    Nothing was lost, because bob's committed offsets live on the BROKER, in
    __consumer_offsets — not on bob's disk. Whoever inherits a partition
    resumes from where the GROUP got to, which is the whole reason offsets
    moved server-side in this phase.""")

        # ── EXPERIMENT 3 ───────────────────────────────────────
        rule('EXPERIMENT 3 — the zombie returns')
        print("""  bob was not dead, only paused — a long GC, a suspended
  container, a laptop lid closing. It wakes up still believing it owns
  partitions and tries to commit.\n""")

        victim._cluster.refresh()
        try:
            victim.position = {(TOPIC, p): 1 for p in victim_partitions}
            victim.committed = {}
            victim.commit()
            print("    ✗ THE COMMIT WAS ACCEPTED — this would be a bug")
        except (IllegalGenerationError, UnknownMemberIdError) as exc:
            print(f"    ✓ REJECTED: {type(exc).__name__}")
            print(f"      {exc}")

        print("""
    Without that rejection, bob's stale commit would rewind the group to
    offset 1 on partitions somebody else has already read past — and those
    messages would be processed a second time, with nothing reporting it.

    Note what bob could NOT have done: detected this itself. From inside a
    pause, no time has passed. Only the coordinator knows the world moved on,
    which is exactly why the generation number is checked there and not here.""")

        # ── Verdict ────────────────────────────────────────────
        rule('WHAT JUST HAPPENED')
        print(f"""
  Consumer groups turn "a consumer died" from an outage into a pause.

    detection    a member that stops heartbeating is evicted after
                 session_timeout_ms ({SESSION_TIMEOUT_MS}ms here). Shorter
                 detects faster and turns every GC pause into a spurious
                 rebalance; longer is stable and leaves partitions unread for
                 longer. There is no right answer, which is why it is a knob.

    handover     the survivors take the dead member's partitions and resume
                 from the GROUP's committed offsets, not their own.

    safety       if the "dead" member comes back, its generation is stale and
                 its commits are refused. Eviction alone would not be safe —
                 a paused process and a dead one are indistinguishable from
                 outside, so the design has to make the wrong guess harmless.

  The cost, paid in full and visible above: EAGER rebalancing stops ALL
  consumption while it runs. Every member revokes everything before anyone
  receives anything. Kafka's cooperative incremental rebalancing moves only
  the partitions that must move; it needs several rounds and much more state,
  and we did not implement it.""")

    finally:
        for runner in runners.values():
            runner.stop()
        for runner in runners.values():
            try:
                runner.consumer.close()
            except Exception:
                pass
        broker.stop()
        thread.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
