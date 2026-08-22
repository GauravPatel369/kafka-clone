"""
CHAOS EXPERIMENT: Watch at-most-once delivery lose your data.
---------------------------------------------------------------

    python -m chaos.kill_consumer_midprocess

Correctness tests prove a system works when nothing goes wrong. Distributed
systems are only interesting when something does. These scripts break things
on purpose so you can watch the failure mode instead of reading about it.

THE EXPERIMENT
---------------
We produce 10 messages, then consume them with a processor that crashes
halfway through — the ordinary case of a consumer hitting a bug, an OOM kill,
or a deploy at the wrong moment.

We run that scenario twice. The ONLY difference is where `commit()` is called:

    RUN A — commit first (at-most-once)      RUN B — commit last (at-least-once)

      messages = consumer.poll()               messages = consumer.poll()
      consumer.commit()   ◄── HERE             for m in messages:
      for m in messages:                           process(m)  ✗ crash
          process(m)  ✗ crash                  consumer.commit()   ◄── HERE

Then we restart the consumer and see what it gets.

That one line is the entire difference between "silently lost three
messages" and "did three messages twice". It is the most consequential line
of code in any consumer you will ever write.
"""

import asyncio
import shutil
import tempfile
import threading

from broker.broker import Broker
from consumer.consumer import Consumer
from producer.producer import Producer

TOPIC = 'chaos-orders'
MESSAGE_COUNT = 10
CRASH_AT_OFFSET = 5      # the processor blows up when it reaches this one


class SimulatedCrash(Exception):
    """Stands in for an OOM kill, a bug, or a deploy at the wrong moment."""


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def run_scenario(port: int, offsets_dir: str, group: str,
                 commit_before_processing: bool) -> list:
    """
    Consume one batch, crash partway through, and report what got processed.

    Returns the list of message values that were fully processed before the
    crash.
    """
    processed = []

    with Consumer('127.0.0.1', port, TOPIC, 0, group,
                  offset_dir=offsets_dir, quiet=True) as consumer:
        messages = consumer.poll(max_messages=MESSAGE_COUNT)
        print(f"    fetched {len(messages)} messages "
              f"(offsets {messages[0]['offset']}..{messages[-1]['offset']})")

        if commit_before_processing:
            consumer.commit()
            print(f"    committed offset {consumer.committed} "
                  f"BEFORE doing any work")

        try:
            for message in messages:
                if message['offset'] == CRASH_AT_OFFSET:
                    raise SimulatedCrash(
                        f"consumer died while processing offset {message['offset']}")
                processed.append(message['value'])
                print(f"    processed {message['value']}")
        except SimulatedCrash as exc:
            print(f"    ✗ CRASH: {exc}")
            # Note what does NOT happen here: no commit. The process is gone.
            return processed

        consumer.commit()
        return processed


def restart_and_see_what_arrives(port: int, offsets_dir: str, group: str) -> list:
    """A fresh consumer in the same group — what does it get?"""
    with Consumer('127.0.0.1', port, TOPIC, 0, group,
                  offset_dir=offsets_dir, quiet=True) as consumer:
        print(f"    restarted; resuming from committed offset {consumer.committed}")
        return [m['value'] for m in consumer.poll(max_messages=MESSAGE_COUNT)]


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-chaos-')
    offsets_dir = f"{workdir}/offsets"

    broker = Broker(host='127.0.0.1', port=0, data_dir=f"{workdir}/data")
    thread = threading.Thread(target=lambda: asyncio.run(broker.start()), daemon=True)
    thread.start()
    if not broker.ready.wait(timeout=5):
        raise SystemExit("Broker failed to start")

    try:
        rule('SETUP')
        with Producer('127.0.0.1', broker.port, TOPIC, quiet=True) as producer:
            for i in range(MESSAGE_COUNT):
                producer.send(f"msg-{i}")
        print(f"  Produced {MESSAGE_COUNT} messages to {TOPIC!r}: "
              f"msg-0 .. msg-{MESSAGE_COUNT - 1}")
        print(f"  The consumer will crash when it reaches offset {CRASH_AT_OFFSET}.")

        # ── RUN A ──────────────────────────────────────────────
        rule('RUN A — commit BEFORE processing   (at-most-once)')
        print("  First run:")
        processed_a = run_scenario(broker.port, offsets_dir, 'group-at-most-once',
                                   commit_before_processing=True)
        print("\n  After restart:")
        redelivered_a = restart_and_see_what_arrives(
            broker.port, offsets_dir, 'group-at-most-once')

        handled_a = set(processed_a) | set(redelivered_a)
        lost = [f"msg-{i}" for i in range(MESSAGE_COUNT)
                if f"msg-{i}" not in handled_a]

        print(f"\n  processed before crash : {processed_a}")
        print(f"  redelivered after      : {redelivered_a}")
        print(f"  ✗ NEVER PROCESSED      : {lost}")

        # ── RUN B ──────────────────────────────────────────────
        rule('RUN B — commit AFTER processing   (at-least-once)')
        print("  First run:")
        processed_b = run_scenario(broker.port, offsets_dir, 'group-at-least-once',
                                   commit_before_processing=False)
        print("\n  After restart:")
        redelivered_b = restart_and_see_what_arrives(
            broker.port, offsets_dir, 'group-at-least-once')

        duplicates = sorted(set(processed_b) & set(redelivered_b))
        handled_b = set(processed_b) | set(redelivered_b)
        lost_b = [f"msg-{i}" for i in range(MESSAGE_COUNT)
                  if f"msg-{i}" not in handled_b]

        print(f"\n  processed before crash : {processed_b}")
        print(f"  redelivered after      : {redelivered_b}")
        print(f"  ⚠ PROCESSED TWICE      : {duplicates}")
        print(f"  ✓ never processed      : {lost_b}")

        # ── Verdict ────────────────────────────────────────────
        rule('WHAT JUST HAPPENED')
        print(f"""
  Same crash, same data, one line moved.

    RUN A (commit first)   {len(lost)} messages LOST FOREVER: {lost}
                           The committed offset said 'done' before any work
                           happened, so the restart skipped straight past
                           them. Nothing logged an error. Nothing retried.
                           You would find out from a customer.

    RUN B (commit last)    0 messages lost, {len(duplicates)} processed twice:
                           {duplicates}
                           The crash left the old offset on disk, so the whole
                           uncommitted batch came back — including the work
                           already done.

  Neither is 'correct'. You are choosing which failure you can live with:

    lost data      is silent, permanent, and usually unrecoverable.
    duplicate work is loud, visible, and has a standard cure — make the
                   processing idempotent, so doing it twice has the same
                   effect as doing it once (UPSERT instead of INSERT, a
                   dedup key, a check-before-act).

  That is why this project defaults to at-least-once, and why real Kafka
  does too. Note also that RUN B's duplicate window is the whole batch: a
  smaller batch or a more frequent commit shrinks it, at the cost of more
  disk writes. Phase 4 puts that dial in your hands.

  What neither run gives you is exactly-once. Getting that requires the
  processing and the commit to be ONE atomic operation -- a transaction
  spanning your database and the broker. Expensive, narrow, and the reason
  people say 'exactly-once delivery is impossible, exactly-once processing
  is achievable'.
""")
    finally:
        broker.stop()
        thread.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
