"""
CONCEPT: The Replica Fetcher — A Follower Is Just a Consumer
---------------------------------------------------------------
For every partition this broker should hold a copy of but does not lead, a
loop runs:

    read my own log end  →  FETCH from the leader  →  append what came back

That is the whole of replication. The interesting part is what it does NOT
have: no push, no per-follower queues on the leader, no separate replication
protocol. A follower issues the **same `FETCH` a consumer issues**, with a
`replica_id` attached so the leader knows to answer differently.


WHY FOLLOWERS PULL
--------------------
The leader could push. Kafka's followers pull, and so do ours, for three
reasons that compound:

  - **The leader stays stateless per follower.** It holds a position integer
    and nothing else. A slow follower costs the leader no memory, no retry
    timers, and no backpressure logic — it simply asks less often.
  - **Restart is free.** A follower that comes back resumes from its own log
    end. Nothing has to remember where it got to, because the log *is* the
    record of where it got to.
  - **The code already exists.** This file is thin because the read path,
    the wire protocol, and the storage engine were all built already. A
    replication design that needed none of them would be a second system.

The cost is one round trip of latency: a record is not on a follower until the
follower asks. `acks=all` is what makes a producer wait for that, and Step 5
is where it starts to.


OFFSETS ARE COPIED, NEVER RECOMPUTED
--------------------------------------
`Log.append` assigns `next_offset` to each record, which is right for a leader
— deciding that a message is number 41 is the leader's job. A follower doing
the same would be *re-deciding* it. `append_replicated` copies the number
instead, and refuses a record that would leave a gap.


WHAT THIS STEP DOES NOT YET DO
--------------------------------
Data moves; nothing depends on it having moved. There is no high watermark
(Step 3), so a consumer can still read records that only the leader holds; no
ISR (Step 4), so a dead follower is simply a fetcher that keeps failing; and
`acks=all` (Step 5) does not wait for anybody. Those are what turn a copy into
a guarantee.
"""

import asyncio
import logging

from common import errors
from common.protocol import (API_FETCH, HEADER_SIZE, decode_body,
                             decode_header, encode_message)

log = logging.getLogger(__name__)

# How many records to ask for in one FETCH. Large enough that a follower
# catching up after a restart does not need thousands of round trips; small
# enough that one response stays well inside the message size limit.
REPLICA_FETCH_MAX_RECORDS = 500

# How long the LEADER may hold an empty fetch open before answering nothing.
#
# This is the mechanism that decouples `acks=all` latency from the follower's
# poll schedule. Without it a caught-up follower sleeps, and a producer waiting
# for replication waits for that sleep to expire — measured at 200 ms per
# message, an `acks=all` cost of 358x rather than the ~2x a round trip
# justifies. Holding the request costs the leader one parked coroutine and
# returns the instant a record lands.
REPLICA_FETCH_MAX_WAIT_MS = 500

# How long to wait before asking again after an EMPTY long-poll — i.e. the
# leader held the request open and still had nothing. Short, because at that
# point the partition is genuinely idle and another request costs almost
# nothing.
REPLICA_FETCH_IDLE_MS = 50

# ...and after a failure. Longer, because the common cause is a leader that is
# down or moving, and hammering it changes nothing.
REPLICA_FETCH_BACKOFF_MS = 500


class ReplicaFetcher:
    """
    Keeps this broker's follower copies up to date with their leaders.

    One task per followed partition, started and stopped as the placement
    changes. The set of partitions to follow comes from `ClusterState`, which
    gets it from the controller through Raft — so a fetcher only ever exists
    for a copy a majority agreed this broker should hold.
    """

    def __init__(self, broker):
        self.broker = broker
        self._tasks = {}                # (topic, partition) -> asyncio.Task
        self._running = False

        # Per-partition end offsets, for logging and for Step 3's high
        # watermark. `-1` means "not yet known".
        self.fetch_positions = {}

    # ── Lifecycle ──────────────────────────────────────────────

    def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False
        for task in list(self._tasks.values()):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    def reconcile(self) -> None:
        """
        Start fetchers for newly assigned copies, stop those we no longer hold.

        Called on every membership tick rather than only on change, because it
        is cheap and idempotent — and because "the placement changed" is
        exactly the event most likely to be missed. Comparing sets here is
        more robust than trusting a notification to fire.

        A partition we have been given LEADERSHIP of stops being fetched. That
        is failover, from this side: the loop that was copying the partition
        exits, and the same broker starts serving writes into the same log.
        """
        if not self._running:
            return

        wanted = set(self.broker.cluster.follower_partitions())
        current = set(self._tasks)

        for key in current - wanted:
            task = self._tasks.pop(key)
            task.cancel()
            self.fetch_positions.pop(key, None)
            log.info("Stopped replicating %s-%s", *key)

        for key in wanted - current:
            self._tasks[key] = asyncio.create_task(self._follow(*key))
            log.info("Started replicating %s-%s from broker %s",
                     key[0], key[1], self.broker.cluster.leader_for(*key))

    # ── One partition's loop ───────────────────────────────────

    async def _follow(self, topic: str, partition: int) -> None:
        """Fetch from this partition's leader forever, until reassigned."""
        while self._running:
            try:
                appended = await self._fetch_once(topic, partition)
                # Only pause when there was nothing to get. A follower that is
                # behind should keep asking at full speed, or catching up after
                # a restart would take one idle interval per batch.
                if appended == 0:
                    await asyncio.sleep(REPLICA_FETCH_IDLE_MS / 1000.0)

            except asyncio.CancelledError:
                return
            except Exception:
                # A fetcher that dies is a partition that silently stops being
                # replicated, and nothing else in the system would notice —
                # the copy just quietly falls further behind forever.
                log.exception("Replica fetch failed for %s-%s", topic, partition)
                await asyncio.sleep(REPLICA_FETCH_BACKOFF_MS / 1000.0)

    async def _fetch_once(self, topic: str, partition: int) -> int:
        """One round trip. Returns how many records were appended."""
        leader_id = self.broker.cluster.leader_for(topic, partition)
        if leader_id is None or leader_id == self.broker.node_id:
            await asyncio.sleep(REPLICA_FETCH_BACKOFF_MS / 1000.0)
            return 0

        local = self.broker._get_partition(topic, partition)
        start = local.next_offset
        self.fetch_positions[(topic, partition)] = start

        response = await self._send_fetch(leader_id, topic, partition, start)
        if response is None:
            await asyncio.sleep(REPLICA_FETCH_BACKOFF_MS / 1000.0)
            return 0

        code = response.get('error_code', errors.NONE)
        if code != errors.NONE:
            # NOT_LEADER_OR_FOLLOWER is the normal, expected case during a
            # placement change: we are asking a broker that has just stopped
            # leading. The next membership tick will point us at the new one,
            # so this is a wait rather than a problem.
            if code != errors.NOT_LEADER_OR_FOLLOWER:
                log.warning("Replica fetch %s-%s from broker %s: %s",
                            topic, partition, leader_id,
                            response.get('error_message', code))
            await asyncio.sleep(REPLICA_FETCH_BACKOFF_MS / 1000.0)
            return 0

        records = response.get('messages') or []
        if not records:
            return 0

        written = await self.broker._run_blocking(
            local.append_replicated, records)

        self.fetch_positions[(topic, partition)] = local.next_offset
        return written

    async def _send_fetch(self, leader_id: int, topic: str, partition: int,
                          offset: int):
        """
        One FETCH to the partition's leader, marked as a replica.

        `replica_id` is what separates this from a consumer's fetch. The leader
        uses it to answer past the high watermark — a follower must be able to
        read records that are not yet safe for consumers, because it is the
        act of that follower reading them that MAKES them safe. (Step 3.)
        """
        peer = self.broker.cluster_config.get(leader_id)
        if peer is None:
            return None

        if getattr(self.broker, 'partitioned_from', None) and \
                leader_id in self.broker.partitioned_from:
            return None                         # test hook, as for heartbeats

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.host, peer.port), timeout=2.0)
        except (OSError, asyncio.TimeoutError):
            return None

        try:
            writer.write(encode_message(API_FETCH, {
                'topic': topic,
                'partition': partition,
                'offset': offset,
                'max': REPLICA_FETCH_MAX_RECORDS,
                'replica_id': self.broker.node_id,
                'max_wait_ms': REPLICA_FETCH_MAX_WAIT_MS,
            }, correlation_id=1))
            await writer.drain()

            # Must exceed the long-poll window, or we would abandon a request
            # the leader is legitimately still holding open for us — and then
            # reconnect and ask again, turning long-polling into a slower
            # version of short-polling.
            header = decode_header(await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE),
                timeout=REPLICA_FETCH_MAX_WAIT_MS / 1000.0 + 5.0))
            return decode_body(await reader.readexactly(header.body_length))
        except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError,
                ValueError):
            return None
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    # ── Introspection ──────────────────────────────────────────

    def status(self) -> dict:
        return {f"{topic}-{partition}": offset
                for (topic, partition), offset
                in sorted(self.fetch_positions.items())}

    def __repr__(self) -> str:
        return f"ReplicaFetcher(following {len(self._tasks)} partition(s))"
