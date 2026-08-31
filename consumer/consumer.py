"""
CONCEPT: The Consumer — Group Membership, and the Read Side of Load Balancing
--------------------------------------------------------------------------------
The consumer reads messages and tracks its position. Two sentences have
governed it since Phase 0 and still do:

    THE BROKER DOES NOT TRACK WHAT YOU HAVE READ.
    A queue broker remembers per-message delivery state; a log broker
    remembers one integer per consumer.

What changed in Phase 4 is *where that integer lives* and *who decides which
partitions you read*.


TWO MODES, AND THEY ARE NOT RANKED
------------------------------------
    consumer.subscribe(['orders'])       group-managed
        The coordinator decides your assignment. Partitions are divided among
        the group's members, one member each. A member that dies has its
        partitions taken over. Costs: heartbeats, and a rebalance pause
        whenever membership changes.

    consumer.assign([('orders', 2)])     manual
        You choose. No group, no coordination, no heartbeats, no rebalance —
        and no failover and no zombie fencing either.

`assign()` is not a lesser `subscribe()`. It is the right choice for a
partition-repair tool, a debugging reader, or anything that must read one
specific partition and must not be reassigned. You are trading protection for
control, deliberately.


THE REBALANCE, FROM THE CLIENT'S SIDE
---------------------------------------
    JOIN_GROUP   ─►  "here I am, I want these topics"
                 ◄─  member_id, generation, and (if leader) the member list
      │
      │  the LEADER — an ordinary client — computes the assignment
      │
    SYNC_GROUP   ─►  leader uploads it; everyone else sends nothing
                 ◄─  your partitions
      │
    HEARTBEAT    ─►  every few seconds, forever
                 ◄─  NONE, or REBALANCE_IN_PROGRESS → rejoin

The heartbeat runs on its own THREAD with its own CONNECTION. Both matter:

  own thread      so a slow `process_fn` does not look like a dead process.
                  (Real Kafka splits this further with `max.poll.interval.ms`,
                  which we do not implement — so a consumer wedged inside
                  processing keeps heartbeating and holds its partitions.)

  own connection  because a request and its response are a pair, and two
                  threads sharing one socket eventually read each other's
                  replies. That is the exact bug Phase 3 hit in the producer;
                  here we avoid it by not sharing at all.


OFFSETS MOVED TO THE BROKER
-----------------------------
Through Phase 3 the position lived in `./offsets/orders-0-mygroup.txt`, on the
consumer's own machine. That is a category error: the position belongs to the
GROUP, and storing it on one machine means it dies with that machine.

It now goes to `__consumer_offsets` on the broker, via OFFSET_COMMIT. Move the
consumer, restart it, run it in a fresh container — it resumes where the group
left off.


THE COMMIT IS STILL EXPLICIT, AND STILL THE WHOLE BALLGAME
------------------------------------------------------------
    fetch ──► commit ──► process        fetch ──► process ──► commit
                    ▲                                     ▲
               crash = LOST                        crash = REDELIVERED
             (at-most-once)                        (at-least-once) ← default

Unchanged since Phase 0, and unchanged by the offsets moving. Where you call
`commit()` still decides which failure you get, and there is still no third
option.
"""

import argparse
import logging
import threading
import time
from contextlib import contextmanager

from common.cluster_client import ClusterClient
from common.config import (
    DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HOST, DEFAULT_MAX_MESSAGES,
    DEFAULT_POLL_INTERVAL, DEFAULT_PORT, DEFAULT_SESSION_TIMEOUT_MS,
)
from common.errors import (
    IllegalGenerationError, KafkaError, RebalanceInProgressError,
    UnknownMemberIdError, UnknownTopicOrPartitionError, raise_for_error,
)
from common.net import BrokerConnectionError, SyncConnection  # noqa: F401
from common.protocol import (
    API_FETCH, API_HEARTBEAT, API_JOIN_GROUP, API_LEAVE_GROUP,
    API_LIST_OFFSETS, API_METADATA, API_OFFSET_COMMIT, API_OFFSET_FETCH,
    API_SYNC_GROUP, TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
)
from consumer.assignor import assign as compute_assignment

log = logging.getLogger(__name__)


class Consumer:
    """
    Reads from a topic, either as a group member or by manual assignment.

        with Consumer(port=9092, group_id='billing') as c:
            c.subscribe(['orders'])
            for message in c.poll():
                handle(message)
            c.commit()
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 topic: str = None, partition: int = 0,
                 group_id: str = 'group-1',
                 offset_dir: str = None,
                 quiet: bool = False,
                 session_timeout_ms: int = DEFAULT_SESSION_TIMEOUT_MS,
                 heartbeat_interval_ms: int = DEFAULT_HEARTBEAT_INTERVAL_MS,
                 auto_offset_reset: str = 'earliest',
                 assignor: str = 'range'):
        self.host = host
        self.port = port
        self.group_id = group_id
        self.quiet = quiet
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.assignor = assignor

        # What to do when this group has never committed a position for a
        # partition. There is no safe default, which is why Kafka makes you
        # choose:
        #   'earliest'  process the whole history. Never misses data; expensive,
        #               and it reprocesses.
        #   'latest'    start from now. SILENTLY SKIPS everything that arrived
        #               while you were away — behind a surprising number of
        #               "we lost a day of events" incidents.
        self.auto_offset_reset = auto_offset_reset

        if offset_dir is not None:
            # Phase 4 moved committed positions to the broker. Accepting and
            # ignoring the old argument would be worse than saying so.
            self._log("note: offset_dir is ignored — offsets now live in "
                      "__consumer_offsets on the broker")

        self.subscription = []          # topics, when group-managed
        self.assignment = []            # [(topic, partition), ...]

        # position  — next offset to FETCH, in memory, per partition
        # committed — last offset durably recorded on the BROKER
        #
        # position >= committed always. Everything between them is "fetched but
        # not yet declared done", and is exactly what gets redelivered after a
        # crash. That gap is your duplicate window, and its size is a choice.
        self.position = {}
        self.committed = {}
        self.high_watermark = {}

        self.member_id = ''
        self.generation_id = -1
        self.is_leader = False

        # From Phase 5 the consumer talks to a CLUSTER. Fetches must go to
        # whichever broker leads each assigned partition; group and offset APIs
        # can go to any broker.
        self._cluster = ClusterClient([(host, port)], timeout=30.0)

        # A SECOND connection, for the heartbeat thread only. Sharing one
        # socket between threads means eventually reading each other's replies
        # and desynchronising the stream permanently — the bug Phase 3 hit in
        # the producer. Two connections avoid it outright, and also stop a
        # heartbeat queueing behind a slow fetch, which would defeat the point
        # of a separate thread.
        self._heartbeat_conn = SyncConnection(host, port)
        self._heartbeat_thread = None
        self._heartbeat_stop = threading.Event()

        self._need_rejoin = threading.Event()
        self._state_lock = threading.Lock()
        self._next_partition_index = 0

        # CONCEPT: a Consumer is NOT thread-safe, and says so loudly.
        #
        # Two threads calling poll()/commit()/subscribe() on one Consumer share
        # `self._conn`, and a request plus its response is a pair that must not
        # be interleaved. Interleave them and each thread reads the other's
        # reply; from then on the stream is desynchronised permanently and the
        # error is "Correlation mismatch: sent 2, got 1" — a long way from the
        # cause.
        #
        # This guard converts that into an immediate, accurate exception at the
        # point of misuse. Real Kafka's consumer does the same thing and calls
        # it ConcurrentModificationException.
        #
        # Note it is NOT a fix that makes concurrent use work: serialising
        # would hide a design error, because two threads sharing a consumer
        # also share its position and assignment. The heartbeat thread is the
        # one legitimate second user, and it has its own connection precisely
        # so it never needs this lock.
        self._access_lock = threading.Lock()

        # The Phase 0-3 constructor took a topic and a partition and read that
        # one partition. Keep it working as an implicit manual assignment —
        # `assign()` is exactly what it always meant.
        self._initial = (topic, partition)

    # ── Lifecycle ──────────────────────────────────────────────

    def connect(self) -> 'Consumer':
        self._cluster.refresh()
        topic, partition = self._initial
        if topic is not None:
            self.assign([(topic, partition)])
        return self

    def close(self) -> None:
        """
        Leave the group cleanly, stop heartbeating, close both connections.

        LEAVE_GROUP matters more than it looks: without it a consumer shutting
        down normally holds its partitions until its session times out — up to
        ten seconds of stalled consumption on every deploy. Announcing the
        departure starts the rebalance immediately.
        """
        self._stop_heartbeat()

        if self.subscription and self.member_id:
            try:
                self._cluster.request_any(API_LEAVE_GROUP, {
                    'group_id': self.group_id, 'member_id': self.member_id})
            except (KafkaError, BrokerConnectionError):
                # Best effort. Failing to leave cleanly costs a session
                # timeout, not correctness, and raising here would mask
                # whatever the caller was actually shutting down for.
                pass

        self._cluster.close()
        self._heartbeat_conn.close()

    def __enter__(self) -> 'Consumer':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Single-thread guard ────────────────────────────────────

    @contextmanager
    def _exclusive(self, operation: str):
        """Refuse, clearly, if another thread is already inside this consumer."""
        if not self._access_lock.acquire(blocking=False):
            raise RuntimeError(
                f"Consumer is not thread-safe: {operation}() was called while "
                f"another thread was still using this consumer. Give each "
                f"thread its own Consumer. (Sharing one would desynchronise "
                f"the connection and corrupt the position state.)")
        try:
            yield
        finally:
            self._access_lock.release()

    # ── Mode selection ─────────────────────────────────────────

    def subscribe(self, topics: list) -> None:
        """
        Join a group and let the coordinator assign partitions.

        Triggers a rebalance: every existing member is told to rejoin, and the
        partitions are redivided. That pause is the cost of eager
        (stop-the-world) rebalancing, and the chaos script makes it visible.
        """
        with self._exclusive('subscribe'):
            self.subscription = list(topics)
            self.assignment = []
            self._need_rejoin.set()
            self._ensure_joined()
        self._start_heartbeat()

    def assign(self, partitions: list) -> None:
        """
        Read exactly these partitions. No group, no coordination.

        No heartbeats, no rebalance — and therefore no failover if this
        consumer dies, and no generation fencing on its commits. A deliberate
        trade of protection for control.
        """
        with self._exclusive('assign'):
            self.subscription = []
            self.assignment = [tuple(tp) for tp in partitions]
            self._refresh_positions()

    # ── Group membership ───────────────────────────────────────

    def _ensure_joined(self) -> None:
        """Run the join → assign → sync dance if a rebalance is pending."""
        if not self.subscription or not self._need_rejoin.is_set():
            return

        response = raise_for_error(self._cluster.request_any(API_JOIN_GROUP, {
            'group_id': self.group_id,
            'member_id': self.member_id,
            'topics': self.subscription,
            'session_timeout_ms': self.session_timeout_ms,
        }))

        with self._state_lock:
            self.member_id = response['member_id']
            self.generation_id = response['generation_id']
            self.is_leader = response['is_leader']

        sync_payload = {
            'group_id': self.group_id,
            'member_id': self.member_id,
            'generation_id': self.generation_id,
        }

        if self.is_leader:
            # THE LEADER IS A CLIENT, and this is the line that proves it. The
            # coordinator never computes an assignment — so a custom strategy
            # ships with your application and never touches a broker.
            subscriptions = {m['member_id']: m['topics']
                             for m in response['members']}
            counts = response.get('partition_counts', {})
            for topic in self.subscription:
                counts.setdefault(topic, self._cluster.partition_count(topic))
            assignment = compute_assignment(self.assignor, subscriptions, counts)
            sync_payload['assignments'] = {
                member_id: [list(tp) for tp in partitions]
                for member_id, partitions in assignment.items()
            }
            self._log(f"leader: assigned "
                      f"{sum(len(v) for v in assignment.values())} partitions "
                      f"across {len(assignment)} member(s)")

        sync = raise_for_error(self._cluster.request_any(API_SYNC_GROUP, sync_payload))

        with self._state_lock:
            self.assignment = [tuple(tp) for tp in sync['assignment']]
            self._need_rejoin.clear()

        self._refresh_positions()
        self._log(f"joined group {self.group_id} gen={self.generation_id} "
                  f"assignment={self.assignment}")

    def _refresh_positions(self) -> None:
        """
        Fetch committed offsets for the current assignment and set positions.

        An offset of -1 means this group has never committed that partition, so
        `auto_offset_reset` applies. That is a genuinely different case from a
        committed 0, and collapsing them would make a brand-new group either
        silently replay everything or silently skip it.
        """
        if not self.assignment:
            self.position, self.committed, self.high_watermark = {}, {}, {}
            return

        response = raise_for_error(self._cluster.request_any(API_OFFSET_FETCH, {
            'group_id': self.group_id,
            'partitions': [{'topic': t, 'partition': p} for t, p in self.assignment],
        }))

        committed = {(entry['topic'], entry['partition']): entry['offset']
                     for entry in response['offsets']}

        position, resolved = {}, {}
        for tp in self.assignment:
            offset = committed.get(tp, -1)
            if offset < 0:
                offset = (self._list_offset(tp, TIMESTAMP_LATEST)
                          if self.auto_offset_reset == 'latest'
                          else self._list_offset(tp, TIMESTAMP_EARLIEST))
                resolved[tp] = -1       # still uncommitted
            else:
                resolved[tp] = offset
            position[tp] = offset

        self.position = position
        self.committed = resolved
        self.high_watermark = {tp: position[tp] for tp in self.assignment}

    # ── Heartbeats ─────────────────────────────────────────────

    def _start_heartbeat(self) -> None:
        if self._heartbeat_thread is not None:
            return
        self._heartbeat_conn.connect()
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name='consumer-heartbeat', daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_thread is None:
            return
        self._heartbeat_stop.set()
        self._heartbeat_thread.join(timeout=5)
        self._heartbeat_thread = None

    def _heartbeat_loop(self) -> None:
        """
        Prove liveness on a fixed schedule, independent of the poll loop.

        Also how a member LEARNS about a rebalance it did not cause: the
        coordinator answers REBALANCE_IN_PROGRESS and we set the rejoin flag.
        Without that, a member whose peer just died would keep consuming
        partitions already reassigned to somebody else — two consumers on one
        partition, which is precisely what a group exists to prevent.
        """
        interval = self.heartbeat_interval_ms / 1000.0

        while not self._heartbeat_stop.wait(interval):
            with self._state_lock:
                member_id, generation_id = self.member_id, self.generation_id
            if not member_id:
                continue

            try:
                raise_for_error(self._heartbeat_conn.request(API_HEARTBEAT, {
                    'group_id': self.group_id,
                    'member_id': member_id,
                    'generation_id': generation_id,
                }))
            except (RebalanceInProgressError, IllegalGenerationError,
                    UnknownMemberIdError):
                # The group moved on without us: rejoin at the next poll.
                #
                # BUT only if this answer is still current. A heartbeat sent
                # before a rejoin completed comes back saying "rebalance in
                # progress" — which was true when it was sent and is stale by
                # the time it lands.
                #
                # Acting on a stale notice is catastrophic rather than merely
                # wasteful: the rejoin triggers a BRAND NEW rebalance on a
                # group that had just stabilised, which makes every other
                # member's heartbeat report a rebalance, which makes them
                # rejoin, and so on. The group never converges. It showed up as
                # members sitting in generations 7, 8 and 9, each believing it
                # owned every partition.
                #
                # Comparing the generation we ASKED with the one we now hold
                # tells us whether the answer is about the current world.
                with self._state_lock:
                    stale = generation_id != self.generation_id
                if not stale:
                    self._need_rejoin.set()
            except (KafkaError, BrokerConnectionError) as exc:
                # A transient failure must not kill the heartbeat thread — if
                # it dies silently we stop heartbeating, get evicted, and the
                # symptom looks like an unexplained rebalance.
                self._log(f"heartbeat failed: {exc}")

    # ── Reading ────────────────────────────────────────────────

    def poll(self, max_messages: int = DEFAULT_MAX_MESSAGES) -> list:
        """
        Fetch from the assigned partitions. Returns [] when caught up.

        Advances `position` in memory. **Commits nothing** — call `commit()`
        once you have processed what you got back.

        Partitions are visited round-robin, resuming where the last poll
        stopped. Always starting at partition 0 would starve later partitions
        whenever the first one has enough messages to fill `max_messages` —
        a consumer that silently never reads half its assignment.
        """
        with self._exclusive('poll'):
            if self._need_rejoin.is_set():
                self._ensure_joined()

            with self._state_lock:
                assignment = list(self.assignment)

            if not assignment:
                return []

            messages = []
            for i in range(len(assignment)):
                if len(messages) >= max_messages:
                    break
                index = (self._next_partition_index + i) % len(assignment)
                topic, partition = assignment[index]
                messages.extend(self._fetch_one(topic, partition,
                                                max_messages - len(messages)))

            self._next_partition_index = (
                self._next_partition_index + 1) % len(assignment)
            return messages

    def _fetch_one(self, topic: str, partition: int, max_messages: int) -> list:
        tp = (topic, partition)
        # Routed to the LEADER of this partition, with refresh-and-retry on
        # NOT_LEADER_OR_FOLLOWER. From Phase 5 a consumer's assigned partitions
        # can live on different brokers, so "the broker" is no longer a thing.
        response = self._cluster.request_for_partition(
            topic, partition, API_FETCH, {
                'topic': topic, 'partition': partition,
                'offset': self.position.get(tp, 0), 'max': max_messages,
            })
        raise_for_error(response, topic=topic, partition=partition,
                        offset=self.position.get(tp, 0))

        self.high_watermark[tp] = response.get('high_watermark',
                                               self.position.get(tp, 0))
        messages = response.get('messages', [])
        if messages:
            # Trust the data over an assumption about the data: use the last
            # record's own offset rather than position + len(messages). They
            # are the same today and stop being the same the moment compaction
            # leaves gaps in the sequence.
            self.position[tp] = messages[-1]['offset'] + 1
        return messages

    def _list_offset(self, tp: tuple, timestamp: int) -> int:
        topic, partition = tp
        response = self._cluster.request_for_partition(
            topic, partition, API_LIST_OFFSETS,
            {'topic': topic, 'partition': partition, 'timestamp': timestamp})
        try:
            raise_for_error(response, topic=topic, partition=partition)
        except UnknownTopicOrPartitionError:
            # A query does not create the topic, so a partition nobody has
            # written to yet is genuinely unknown. An empty partition starts at
            # 0 whichever end you asked for, so fall back rather than making
            # the caller handle a distinction with no consequence.
            return 0
        return response['offset']

    # ── Committing ─────────────────────────────────────────────

    def commit(self) -> dict:
        """
        Durably record the current positions on the BROKER.

        The group's only promise to the world: everything below these offsets
        is handled. Call it AFTER processing for at-least-once, BEFORE for
        at-most-once. There is no third option.

        Fenced by generation when group-managed: a consumer that was evicted
        while paused is rejected with ILLEGAL_GENERATION rather than being
        allowed to rewind the group behind its replacement's progress.
        """
        with self._exclusive('commit'):
            return self._commit_locked()

    def _commit_locked(self) -> dict:
        pending = [{'topic': t, 'partition': p, 'offset': offset}
                   for (t, p), offset in self.position.items()
                   if offset != self.committed.get((t, p), -1)]
        if not pending:
            return dict(self.committed)

        payload = {'group_id': self.group_id, 'offsets': pending}
        if self.subscription and self.member_id:
            payload['member_id'] = self.member_id
            payload['generation_id'] = self.generation_id

        try:
            raise_for_error(self._cluster.request_any(API_OFFSET_COMMIT, payload))
        except (IllegalGenerationError, UnknownMemberIdError):
            # We are a zombie: evicted, and our partitions belong to someone
            # else now. Rejoin rather than retrying the commit — the work we
            # were about to claim credit for has already been redone.
            self._need_rejoin.set()
            raise

        for entry in pending:
            self.committed[(entry['topic'], entry['partition'])] = entry['offset']

        self._log(f"committed {len(pending)} partition offset(s)")
        return dict(self.committed)

    # ── Positioning ────────────────────────────────────────────

    def seek(self, topic: str, partition: int, offset: int) -> None:
        """Jump to an offset. Does not commit — commit if you mean it."""
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")
        self.position[(topic, partition)] = offset

    def seek_to_beginning(self) -> None:
        """
        Rewind every assigned partition to the oldest offset still retained.

        Asks the broker rather than assuming 0: once retention has deleted old
        segments the earliest surviving offset is not 0, and seeking to 0 would
        raise OFFSET_OUT_OF_RANGE.

        This is Kafka's superpower and it exists only because reads are not
        destructive: "redeploy the fixed consumer and reprocess yesterday" is a
        change to an integer. In a queue, consumed messages are gone.
        """
        for tp in self.assignment:
            self.position[tp] = self._list_offset(tp, TIMESTAMP_EARLIEST)

    def seek_to_end(self) -> None:
        """Skip everything currently stored; read only new messages."""
        for tp in self.assignment:
            self.position[tp] = self._list_offset(tp, TIMESTAMP_LATEST)

    def lag(self) -> int:
        """
        Total unread messages across the assignment.

        THE number to alarm on. Steady lag means the consumer keeps up; growing
        lag means it does not, and eventually retention deletes data nobody has
        read — silent loss that looks like a monitoring problem right up until
        it is a data problem.
        """
        return sum(max(0, self.high_watermark.get(tp, 0) - self.position.get(tp, 0))
                   for tp in self.assignment)

    def lag_by_partition(self) -> dict:
        return {tp: max(0, self.high_watermark.get(tp, 0) - self.position.get(tp, 0))
                for tp in self.assignment}

    # ── The loop ───────────────────────────────────────────────

    def consume_loop(self, process_fn,
                     poll_interval: float = DEFAULT_POLL_INTERVAL,
                     max_messages: int = DEFAULT_MAX_MESSAGES,
                     auto_commit: bool = True) -> None:
        """
        Poll forever, calling `process_fn(message)` on everything received.

        The ordering IS the delivery guarantee:

            messages = poll()      # fetch
            for m: process_fn(m)   # do the work
            commit()               # only now say it is done

        If `process_fn` raises we do not commit, so the next run re-fetches from
        the last committed offset. Redelivered rather than lost — at-least-once,
        by construction.
        """
        self._log(f"consuming (poll every {poll_interval}s, "
                  f"auto_commit={auto_commit}) — Ctrl-C to stop")
        while True:
            messages = self.poll(max_messages=max_messages)

            for message in messages:
                process_fn(message)

            if messages and auto_commit:
                try:
                    self.commit()
                except (IllegalGenerationError, UnknownMemberIdError):
                    # Evicted mid-batch. The work was redone by whoever took
                    # our partitions; rejoin on the next poll and carry on.
                    self._log("commit rejected — we were evicted; rejoining")

            if not messages:
                time.sleep(poll_interval)

    # ── Internals ──────────────────────────────────────────────

    def _log(self, message: str) -> None:
        if not self.quiet:
            print(f"[Consumer] {message}")

    def __repr__(self) -> str:
        mode = 'subscribed' if self.subscription else 'assigned'
        return (f"Consumer(group={self.group_id!r}, {mode}, "
                f"gen={self.generation_id}, partitions={len(self.assignment)})")


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone consumer  (run with: python -m consumer.consumer)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--group', default='group-1')
    parser.add_argument('--partition', type=int, default=None,
                        help='Read exactly this partition (manual assign, no group)')
    parser.add_argument('--assignor', default='range', choices=['range', 'roundrobin'])
    parser.add_argument('--reset', default='earliest', choices=['earliest', 'latest'])
    parser.add_argument('--loop', action='store_true')
    parser.add_argument('--from-beginning', action='store_true')
    parser.add_argument('--session-timeout-ms', type=int,
                        default=DEFAULT_SESSION_TIMEOUT_MS)
    args = parser.parse_args()

    def handle(message):
        print(f"  → offset={message['offset']:<8} key={str(message['key']):<10} "
              f"{message['value']!r}")

    consumer = Consumer(args.host, args.port, group_id=args.group,
                        auto_offset_reset=args.reset, assignor=args.assignor,
                        session_timeout_ms=args.session_timeout_ms)
    with consumer:
        if args.partition is not None:
            consumer.assign([(args.topic, args.partition)])
        else:
            consumer.subscribe([args.topic])

        if args.from_beginning:
            consumer.seek_to_beginning()

        if args.loop:
            try:
                consumer.consume_loop(handle)
            except KeyboardInterrupt:
                consumer.commit()
                print("\n[Consumer] stopped.")
        else:
            messages = consumer.poll()
            print(f"\nFetched {len(messages)} messages "
                  f"from {consumer.assignment} (lag now {consumer.lag()}):")
            for message in messages:
                handle(message)
            consumer.commit()


if __name__ == '__main__':
    main()
