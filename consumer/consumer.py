"""
CONCEPT: The Consumer — the Read Side
---------------------------------------
The consumer reads messages from a partition, tracking its own position with
an integer called the offset.

The single most important sentence in this file:

    THE BROKER DOES NOT TRACK WHAT YOU HAVE READ.

In a traditional queue the broker remembers, per message per consumer, whether
it was delivered and acknowledged. That bookkeeping is what makes queue brokers
expensive. Kafka pushes the responsibility to the consumer, which then only has
to remember ONE INTEGER per partition. That is the whole scalability story.

The price is that the consumer now owns a correctness problem: *when* it
records that integer decides whether crashes lose messages or duplicate them.


PULL, NOT PUSH
---------------
The consumer asks for messages; the broker never sends unsolicited data.

  ┌─────────┬──────────────────────────────────────────────────────────────┐
  │ PUSH    │ Broker sends as soon as data exists. Lowest latency. But the │
  │         │ broker must track each consumer's speed, and a slow consumer │
  │         │ gets overwhelmed — you need a flow-control protocol to stop  │
  │         │ the broker drowning it.                                      │
  ├─────────┼──────────────────────────────────────────────────────────────┤
  │ PULL    │ Consumer asks when ready, so it can never be overwhelmed —   │
  │  ← ours │ flow control is automatic. It can batch as much as it likes. │
  │         │ Recovery is trivial: re-ask for the same offset. Cost: when  │
  │         │ idle you either poll and waste requests, or wait and add     │
  │         │ latency. (Real Kafka's long-poll gets both; see config.py.)  │
  └─────────┴──────────────────────────────────────────────────────────────┘


THE COMMIT IS EXPLICIT, AND THAT IS THE POINT
-----------------------------------------------
The original version committed the offset inside `poll()`, before the caller
had processed anything. That is **at-most-once** delivery: crash while
processing and those messages are gone forever, because the saved offset
already claims they are done.

    fetch ──► commit ──► process        fetch ──► process ──► commit
                    ▲                                     ▲
                 crash = LOST                          crash = REDELIVERED
              (at-most-once)                           (at-least-once)

`poll()` now only fetches and advances an in-memory position. `commit()` is a
separate call. You choose the semantics by choosing where you call it — which
is exactly how the real Kafka consumer API works, and why `enable.auto.commit`
is the most consequential one-line config in Kafka.

We default to at-least-once. Duplicates have a cure (make your processing
idempotent); silently lost data does not.

Note that at-least-once is NOT achieved by committing "after processing" in
some vague sense — it is achieved because a crash before the commit leaves the
*previous* offset on disk, so the next run re-fetches from there. The commit is
the only durable statement the consumer ever makes.
"""

import argparse
import os
import time

from common.config import (
    DEFAULT_HOST, DEFAULT_MAX_MESSAGES, DEFAULT_OFFSET_DIR,
    DEFAULT_POLL_INTERVAL, DEFAULT_PORT,
)
from common.errors import UnknownTopicOrPartitionError, raise_for_error
from common.net import SyncConnection
from common.protocol import (
    API_FETCH, API_LIST_OFFSETS, TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
)


class Consumer:
    """
    Reads one partition of one topic, remembering its position on local disk.

    "One partition" is a real Phase 0 limitation. A consumer *group* that
    splits many partitions across many members — and reassigns them when a
    member dies - is Phase 4. Here, `group_id` only namespaces the offset
    file, so two consumers in the same group both read everything and fight
    over the same file.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 topic: str = 'default', partition: int = 0,
                 group_id: str = 'group-1',
                 offset_dir: str = DEFAULT_OFFSET_DIR,
                 quiet: bool = False):
        self.host = host
        self.port = port
        self.topic = topic
        self.partition = partition
        self.group_id = group_id
        self.quiet = quiet

        os.makedirs(offset_dir, exist_ok=True)
        self._offset_file = os.path.join(
            offset_dir, f"{topic}-{partition}-{group_id}.txt")

        # TWO positions, and the gap between them is where crash-safety lives.
        #
        #   position  — the next offset to FETCH. In memory only. Advances on
        #               every poll.
        #   committed — the last offset durably written to disk. This is what
        #               a restart resumes from.
        #
        # position >= committed always. Everything in between is "fetched but
        # not yet declared done", and is exactly what gets redelivered after
        # a crash. The size of that gap is your duplicate window.
        self.committed = self._load_offset()
        self.position = self.committed

        # Last high watermark the broker reported: one past the newest message
        # in the partition. Used by lag(). Starts equal to our position, so
        # lag reads as 0 until we have actually asked the broker anything.
        self.high_watermark = self.position

        self._conn = SyncConnection(host, port)

        self._log(f"group={group_id} {topic}-{partition} "
                  f"starting at offset {self.position}")

    # ── Connection lifecycle ───────────────────────────────────

    def connect(self) -> 'Consumer':
        self._conn.connect()
        self._log(f"Connected to broker at {self.host}:{self.port}")
        return self

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> 'Consumer':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Reading ────────────────────────────────────────────────

    def poll(self, max_messages: int = DEFAULT_MAX_MESSAGES) -> list:
        """
        Fetch the next batch of messages. Returns [] when caught up.

        Advances `position` in memory. **Commits nothing.** Call `commit()`
        once you have actually processed what you got back.
        """
        response = self._conn.request(API_FETCH, {
            'topic':     self.topic,
            'partition': self.partition,
            'offset':    self.position,
            'max':       max_messages,
        })

        # Raises the typed exception matching the numeric error code. The one
        # worth handling here is OffsetOutOfRangeError: it means our saved
        # position no longer exists in the log — usually because retention
        # deleted it while we were behind. See seek_to_beginning/seek_to_end,
        # which are the two possible recoveries (Kafka's `auto.offset.reset`).
        raise_for_error(response, topic=self.topic, partition=self.partition,
                        offset=self.position)

        messages = response.get('messages', [])
        self.high_watermark = response.get('high_watermark', self.position)

        if messages:
            # Advance past the last message we received.
            #
            # We use the last record's own offset rather than
            # `position + len(messages)`: the two are the same today, but they
            # stop being the same the moment Phase 2 adds compaction (which
            # leaves gaps in the offset sequence). Trusting the data over an
            # assumption about the data is the habit worth building.
            self.position = messages[-1]['offset'] + 1
            self._log(f"fetched {len(messages)} messages, position now {self.position}")
        else:
            self._log(f"no new messages at offset {self.position}")

        return messages

    def lag(self) -> int:
        """
        How many messages exist that this consumer has not fetched.

        THE number to alarm on in production. Steady lag means the consumer
        keeps up. Growing lag means it does not, and at some point retention
        will delete data the consumer has not read yet — silent loss that
        looks like a monitoring problem right up until it is a data problem.
        """
        return max(0, self.high_watermark - self.position)

    # ── Committing ─────────────────────────────────────────────

    def commit(self) -> int:
        """
        Durably record the current position. Returns the committed offset.

        This is the consumer's only promise to the world: "everything below
        this offset is handled." Call it AFTER processing for at-least-once,
        BEFORE for at-most-once. There is no third option.
        """
        if self.position != self.committed:
            self._save_offset(self.position)
            self.committed = self.position
            self._log(f"committed offset {self.committed}")
        return self.committed

    def seek(self, offset: int) -> None:
        """
        Jump to an arbitrary offset. Does not commit — commit if you mean it.

        CONCEPT: This is Kafka's superpower, and it exists only because the
        log is not destructive. Messages are still on disk after being read,
        so "reprocess the last three days with the fixed code" is a one-line
        change of an integer. In a queue, consumed messages are gone and that
        recovery is simply impossible.
        """
        if offset < 0:
            raise ValueError(f"Offset must be >= 0, got {offset}")
        self.position = offset
        self._log(f"seek → {offset}")

    def seek_to_beginning(self) -> None:
        """
        Move to the oldest offset the broker still holds.

        Asks the broker rather than assuming 0: once Phase 2's retention
        starts deleting old segments, the earliest surviving offset is no
        longer 0, and seeking to 0 would raise OFFSET_OUT_OF_RANGE.
        """
        self.seek(self._list_offset(TIMESTAMP_EARLIEST))

    def seek_to_end(self) -> None:
        """
        Skip everything currently stored and read only new messages.

        CONCEPT: the two halves of Kafka's `auto.offset.reset`.
          When a consumer has no valid position — first run, or its saved
          offset aged out — it must choose:

            earliest  process the whole history. Correct for anything that
                      must not miss data; expensive, and it reprocesses.
            latest    start from now. Correct for live dashboards and
                      metrics; SILENTLY SKIPS everything that arrived while
                      you were away.

          There is no safe default, which is why Kafka makes you pick and why
          picking `latest` is behind a surprising number of "we lost a day of
          events" incidents.

        Uses LIST_OFFSETS rather than fetching-and-discarding: the broker
        answers from metadata in constant time, with no data transferred.
        """
        self.seek(self._list_offset(TIMESTAMP_LATEST))

    def _list_offset(self, timestamp: int) -> int:
        """
        Ask the broker for the earliest or latest offset in this partition.

        LIST_OFFSETS is a query, so — unlike PRODUCE and FETCH — it does not
        auto-create the partition; a topic nobody has written to yet is
        genuinely unknown. For a seek that is not worth failing over: an empty
        partition starts at 0 whichever end you asked for, so we fall back
        rather than making the caller handle a distinction with no consequence.
        """
        response = self._conn.request(API_LIST_OFFSETS, {
            'topic':     self.topic,
            'partition': self.partition,
            'timestamp': timestamp,
        })
        try:
            raise_for_error(response, topic=self.topic, partition=self.partition)
        except UnknownTopicOrPartitionError:
            self._log(f"{self.topic}-{self.partition} does not exist yet; "
                      f"starting at 0")
            return 0
        return response['offset']

    # ── The loop ───────────────────────────────────────────────

    def consume_loop(self, process_fn,
                     poll_interval: float = DEFAULT_POLL_INTERVAL,
                     max_messages: int = DEFAULT_MAX_MESSAGES,
                     auto_commit: bool = True) -> None:
        """
        Poll forever, calling `process_fn(message)` on everything received.

        The ordering here IS the delivery guarantee:

            messages = poll()          # fetch
            for m in messages:
                process_fn(m)          # do the work
            commit()                   # only now say it is done

        If `process_fn` raises, we do not commit, and the next run re-fetches
        from the last committed offset. Those messages are redelivered rather
        than lost — at-least-once, by construction.

        Set `auto_commit=False` when the caller wants to commit on its own
        schedule (say, once per 1000 messages instead of once per batch:
        fewer disk writes, but a bigger window of possible duplicates — the
        same trade-off, with a dial on it).
        """
        self._log(f"consuming (poll every {poll_interval}s, "
                  f"auto_commit={auto_commit}) — Ctrl-C to stop")
        while True:
            messages = self.poll(max_messages=max_messages)

            for message in messages:
                process_fn(message)

            if messages and auto_commit:
                self.commit()

            if not messages:
                # Nothing new. Sleep rather than spin — see config.py for why
                # this simple approach costs latency, and what real Kafka
                # does instead (long polling).
                time.sleep(poll_interval)

    # ── Offset persistence ─────────────────────────────────────

    def _load_offset(self) -> int:
        """Read the committed offset. Returns 0 on a first run."""
        try:
            with open(self._offset_file, 'r', encoding='utf-8') as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return 0
        except ValueError:
            # An empty or garbled file. With the atomic write below this
            # should now be unreachable — it was reachable before, and it
            # silently restarted the consumer from zero, reprocessing the
            # entire topic. We warn loudly instead of pretending it is normal.
            print(f"[Consumer] WARNING: offset file {self._offset_file} is "
                  f"unreadable; restarting from 0. This should not happen.")
            return 0

    def _save_offset(self, offset: int) -> None:
        """
        Write the offset to disk atomically.

        CONCEPT: Why not just open(path, 'w')?
          Opening with 'w' TRUNCATES the file to zero bytes, and only then
          writes. Between those two steps the file on disk is EMPTY. Crash in
          that window — microseconds wide, but 3am is long — and the consumer
          restarts from offset 0 and reprocesses everything.

          The fix is the write-temp-then-rename dance:

            1. write the new content to a temporary file
            2. flush + fsync it, so the BYTES are physically on disk
            3. atomically rename it over the real file

          A rename is atomic at the filesystem level: any reader sees either
          the complete old file or the complete new one. There is no instant
          at which a partial file is visible.

          Step 2 is not optional. Without the fsync, the rename can reach
          disk before the data does, leaving a correctly-named file full of
          zeros after a power cut — the exact failure we were preventing.

          `os.replace`, not `os.rename`: on Windows, rename fails if the
          destination exists. replace overwrites on every platform.

        This pattern is how editors, databases, and config managers all write
        small files safely. Learn it once, use it everywhere.
        """
        tmp_path = self._offset_file + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(str(offset))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._offset_file)

    # ── Internals ──────────────────────────────────────────────

    def _log(self, message: str) -> None:
        if not self.quiet:
            print(f"[Consumer] {message}")

    def __repr__(self) -> str:
        return (f"Consumer(group={self.group_id!r}, "
                f"{self.topic}-{self.partition}, "
                f"position={self.position}, committed={self.committed})")


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone consumer  (run with: python -m consumer.consumer)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--partition', type=int, default=0)
    parser.add_argument('--group', default='group-1')
    parser.add_argument('--loop', action='store_true',
                        help='Keep polling forever instead of fetching once')
    parser.add_argument('--from-beginning', action='store_true',
                        help='Ignore the committed offset and replay from 0')
    args = parser.parse_args()

    def handle(message):
        print(f"  → offset={message['offset']} key={message['key']!r} "
              f"value={message['value']!r} ts={message['timestamp']}")

    with Consumer(args.host, args.port, args.topic,
                  args.partition, args.group) as consumer:
        if args.from_beginning:
            consumer.seek_to_beginning()

        if args.loop:
            try:
                consumer.consume_loop(handle)
            except KeyboardInterrupt:
                # Commit what we have processed before exiting. Without this,
                # a clean Ctrl-C would replay the last batch on restart — not
                # incorrect (at-least-once permits it) but needlessly untidy.
                consumer.commit()
                print("\n[Consumer] stopped.")
        else:
            messages = consumer.poll()
            print(f"\nFetched {len(messages)} messages "
                  f"(lag now {consumer.lag()}):")
            for message in messages:
                handle(message)
            consumer.commit()


if __name__ == '__main__':
    main()
