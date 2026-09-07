"""
CONCEPT: The Broker — A TCP Server
------------------------------------
The broker is the long-running server process at the centre of the system. It:

  1. Listens on a TCP port (9092, matching real Kafka)
  2. Accepts connections from producers and consumers
  3. Routes each request to the right partition
  4. Sends back a response

                 ┌─────────────────────────────────────┐
                 │             BROKER                  │
    Producer A ──┤── handle_client() ──┐               │
    Producer B ──┤── handle_client()   ├─► _route() ──┐│
    Consumer   ──┤── handle_client() ──┘              ││
                 │                                    ▼│
                 │                      partitions{}   │
                 │              orders-0.log  orders-1.log
                 └─────────────────────────────────────┘


WHY ASYNCIO AND NOT THREADS?
------------------------------
A broker's job is almost entirely waiting. Waiting for a request to arrive,
waiting for a disk write, waiting for a socket to drain. Actual computation is
a rounding error.

  ┌──────────────────┬────────────────────────────────────────────────────┐
  │ Model            │ Trade-off                                          │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Thread per       │ Dead simple — blocking code just works. But each   │
  │ connection       │ thread costs ~8 MB of stack and a context switch.  │
  │                  │ Falls over somewhere around 1,000 clients.         │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Process per      │ True parallelism and crash isolation. Heavier      │
  │ connection       │ still, and processes cannot share the partition    │
  │                  │ registry without IPC.                              │
  ├──────────────────┼────────────────────────────────────────────────────┤
  │ Event loop       │ One thread, one small coroutine per connection     │
  │ (asyncio)        │ (~KB). Tens of thousands of clients. Cost: ONE     │
  │  ← WE USE THIS   │ blocking call freezes EVERY connection, and you    │
  │                  │ get one CPU core.                                  │
  └──────────────────┴────────────────────────────────────────────────────┘

That "one blocking call freezes everything" cost is not hypothetical — our
storage layer does blocking file I/O. So `_route` hands those calls to a
thread pool via `run_in_executor`: the loop stays free while a worker blocks
on the disk. Mixing the two models is the standard asyncio pattern, and
forgetting to is the standard asyncio bug.

(Real Kafka uses network threads plus I/O threads — same idea, different
plumbing. The JVM has cheap threads; Python does not.)


PHASE 1: WHAT CHANGED IN HERE
-------------------------------
The routing logic is the same shape, but every answer is now structured:

  * Requests carry an `api_key` and `api_version`; unsupported combinations
    are rejected with UNSUPPORTED_VERSION instead of being half-handled.
  * Every response carries a numeric `error_code`, so clients decide what to
    retry by asking the code rather than by matching English.
  * A body length larger than MAX_REQUEST_BYTES is refused BEFORE allocating.
  * Two new read-only APIs, LIST_OFFSETS and METADATA, so clients and the
    admin CLI can ask about state instead of inferring it.
"""

import argparse
import asyncio
import json
import logging
import os
import threading

from broker.cluster import ClusterState
from broker.controller import Controller
from broker.replica import ReplicaFetcher
from broker.coordinator import GroupCoordinator
from broker.raft import RaftNode
from broker.log import LogConfig
from broker.metadata import (
    InvalidTopicError, MetadataStore, validate_topic_name)
from broker.offset_store import OffsetStore
from broker.partition import Partition
from common import errors
from common.cluster_config import ClusterConfig, ClusterConfigError
from common.config import (
    DEFAULT_ACKS_ALL_TIMEOUT_MS, DEFAULT_BROKER_HEARTBEAT_MS,
    DEFAULT_BROKER_SESSION_TIMEOUT_MS, DEFAULT_MIN_INSYNC_REPLICAS,
    DEFAULT_REPLICA_LAG_TIME_MAX_MS,
    DEFAULT_DATA_DIR, DEFAULT_GROUP_REAP_MS, DEFAULT_HOST, DEFAULT_PORT,
    DEFAULT_RETENTION_CHECK_MS,
)
from common.protocol import (
    API_BROKER_HEARTBEAT, API_CONTROLLER_UPDATE, API_CREATE_TOPICS,
    API_DESCRIBE_CLUSTER, API_DESCRIBE_GROUPS, API_FETCH, API_FIND_COORDINATOR,
    API_HEARTBEAT, API_JOIN_GROUP, API_LEAVE_GROUP, API_LIST_GROUPS,
    API_LIST_OFFSETS, API_METADATA, API_OFFSET_COMMIT, API_OFFSET_FETCH,
    API_APPEND_ENTRIES, API_INSTALL_SNAPSHOT, API_PRODUCE, API_REQUEST_VOTE,
    API_SYNC_GROUP,
    HEADER_SIZE, MAGIC, MAX_REQUEST_BYTES,
    TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
    api_name, decode_body, decode_header, encode_message, is_supported,
)

log = logging.getLogger(__name__)


class Broker:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 data_dir: str = DEFAULT_DATA_DIR, node_id: int = 0,
                 log_config: LogConfig = None,
                 retention_check_ms: float = DEFAULT_RETENTION_CHECK_MS,
                 auto_create_topics: bool = True,
                 default_partitions: int = 1,
                 group_reap_ms: float = DEFAULT_GROUP_REAP_MS,
                 cluster_config: ClusterConfig = None,
                 broker_heartbeat_ms: float = DEFAULT_BROKER_HEARTBEAT_MS,
                 broker_session_timeout_ms: float = DEFAULT_BROKER_SESSION_TIMEOUT_MS,
                 quorum_voters: list = None,
                 raft_snapshot_threshold: int = None,
                 replica_lag_time_max_ms: float = DEFAULT_REPLICA_LAG_TIME_MAX_MS,
                 min_insync_replicas: int = DEFAULT_MIN_INSYNC_REPLICAS,
                 acks_all_timeout_ms: float = DEFAULT_ACKS_ALL_TIMEOUT_MS):
        self.host = host
        self.port = port          # rewritten with the real port if port == 0
        self.data_dir = data_dir

        # Every broker in a cluster needs a stable identity: partition
        # assignments, leadership, and replica sets are all expressed as
        # broker IDs, not host:port (which can change when a machine is
        # replaced). With one broker it is always 0 — but METADATA already
        # reports it, so Phase 5 adds peers rather than a new concept.
        self.node_id = node_id

        # Storage tuning applied to every partition this broker creates.
        # Real Kafka allows per-topic overrides; we have nowhere to hang them
        # yet, so it is broker-wide.
        self.log_config = log_config or LogConfig()
        self.retention_check_ms = retention_check_ms
        self.group_reap_ms = group_reap_ms

        # Which topics exist and how many partitions each has. A partition
        # count has to be DECLARED rather than inferred from which directories
        # happen to exist, because the producer needs it to route a message
        # before any of those partitions contain anything.
        self.metadata = MetadataStore(data_dir, auto_create=auto_create_topics,
                                      default_partitions=default_partitions)

        # Consumer groups: membership and fencing here, committed positions in
        # `__consumer_offsets`. Note what the coordinator does NOT do — it
        # never computes an assignment. A client does.
        self.coordinator = GroupCoordinator(node_id=node_id)
        self.offsets = OffsetStore(data_dir, self.log_config)

        # Cluster membership and placement. A one-broker "cluster" is NOT a
        # special case: it behaves exactly like a three-broker cluster with two
        # brokers down, so every single-broker test exercises the same
        # membership and election code rather than a separate branch that could
        # rot unnoticed.
        self.cluster_config = cluster_config or ClusterConfig.single_node(
            node_id, host, port)
        self.cluster = ClusterState(node_id, self.cluster_config,
                                    broker_session_timeout_ms)
        self.replica_lag_time_max_ms = replica_lag_time_max_ms
        self.min_insync_replicas = min_insync_replicas
        self.acks_all_timeout_ms = acks_all_timeout_ms

        self.controller = Controller(
            self.cluster, self.metadata,
            replica_lag_time_max_ms=replica_lag_time_max_ms)
        self.broker_heartbeat_ms = broker_heartbeat_ms

        # The controller quorum. Leadership comes from here now, not from
        # min(live_brokers) — see broker/cluster.py for what changed and why.
        #
        # CONCEPT: the quorum is not necessarily the whole cluster.
        #
        # By default every broker in `cluster.json` votes — Kafka's combined
        # `process.roles=broker,controller` mode, and normal for small
        # clusters. But the two sets are genuinely different things, and Kafka
        # keeps them apart as `controller.quorum.voters`:
        #
        #   the CLUSTER   every broker that stores partitions
        #   the QUORUM    the subset that votes on metadata
        #
        # Separating them matters because quorum size has its own arithmetic.
        # A 2-voter quorum needs BOTH voters for a majority, so it tolerates
        # zero failures — strictly worse than a single voter, which at least
        # tolerates none while staying available. Quorums are sized 1, 3, 5 for
        # that reason, while a cluster can be any size at all.
        self.quorum_voters = sorted(quorum_voters
                                    or self.cluster_config.node_ids())
        self.raft = RaftNode(
            node_id=node_id,
            voters=self.quorum_voters,
            directory=os.path.join(data_dir, 'raft'),
            send_rpc=self._raft_rpc,
            # An election timeout several times the heartbeat interval, so a
            # single dropped heartbeat cannot start a pointless election.
            election_timeout_ms=broker_session_timeout_ms,
            heartbeat_interval_ms=broker_heartbeat_ms,
            # Overridable so `chaos/lagging_follower.py` can force compaction
            # to actually happen. The default is far above anything a real run
            # of this cluster reaches, which keeps snapshots off the hot path
            # and, less comfortably, keeps them out of every normal run —
            # see docs/phase-5/raft-snapshots.md §6.
            **({} if raft_snapshot_threshold is None
               else {'snapshot_threshold': raft_snapshot_threshold}),
        )
        # Constructed after the RaftNode, so the controller can propose to it.
        self.controller.raft = self.raft

        # Follower copies. One fetch loop per partition this broker should
        # hold but does not lead — see broker/replica.py.
        self.replicas = ReplicaFetcher(self)

        # Strong references to fire-and-forget tasks. asyncio only keeps a weak
        # reference, so a task nobody holds can be collected mid-flight.
        self._background = set()

        # Peers with a heartbeat still in flight — see `_heartbeat_peers`.
        self._heartbeats_inflight = set()

        # CONCEPT: The partition registry.
        #   (topic, partition_id) → Partition
        # Keyed by the pair because a topic is only a *name*; the partition is
        # what holds data. Everything in Kafka is ultimately addressed by
        # (topic, partition).
        self.partitions: "dict[tuple, Partition]" = {}
        self._registry_lock = threading.Lock()

        # Set once bound and accepting. Tests wait on this instead of sleeping
        # and hoping. threading.Event, not asyncio.Event, because the waiter
        # is a different thread than the loop.
        self.ready = threading.Event()

        self._loop = None
        self._shutdown = None     # asyncio.Event, created inside start()

    # ── Partition management ───────────────────────────────────

    def _get_partition(self, topic: str, partition_id: int) -> Partition:
        """
        Return the partition, creating it on first use.

        CONCEPT: Lazy auto-creation — convenient, and a genuine footgun.
          Producing to "oders" instead of "orders" raises no error. It
          silently creates a new topic and your messages go somewhere nobody
          reads. Real Kafka makes this configurable
          (`auto.create.topics.enable`) and most production clusters turn it
          OFF for exactly this reason. Phase 3 adds explicit CREATE_TOPICS.

        Note the deliberate split introduced in Phase 1: **writes create,
        queries do not.** PRODUCE and FETCH auto-create; LIST_OFFSETS and
        METADATA use `_find_partition` and report
        UNKNOWN_TOPIC_OR_PARTITION instead. A query that conjures the thing it
        was asking about makes it impossible to ask "does this exist?".

        The lock: `_get_partition` runs on the loop thread today, so nothing
        can interleave. It is held anyway because Phase 6's replication
        fetcher touches this registry from another thread, and a registry that
        is only accidentally safe is a bug waiting for a future phase.
        """
        key = (topic, partition_id)
        with self._registry_lock:
            if key not in self.partitions:
                partition = Partition(topic, partition_id, self.data_dir,
                                      self.log_config)
                self.partitions[key] = partition
                log.info("Created partition %s-%s", topic, partition_id)

                # Report repaired damage loudly. A torn tail means the broker
                # died mid-write or the disk lost bytes; silently fixing that
                # without saying so is how you lose data and never find out.
                if partition.corrupt_bytes_recovered:
                    log.warning(
                        "Recovered %s-%s: truncated %d corrupt bytes from the "
                        "tail of the active segment",
                        topic, partition_id, partition.corrupt_bytes_recovered)
            return self.partitions[key]

    def _find_partition(self, topic: str, partition_id: int):
        """Look up a partition without creating it. Returns None if absent."""
        with self._registry_lock:
            return self.partitions.get((topic, partition_id))

    async def _run_blocking(self, fn, *args):
        """
        Run blocking file I/O on the executor so the event loop stays free.

        CONCEPT: the escape hatch for blocking code.
          `partition.append()` writes to a file, which blocks. Calling it
          directly would stop the loop dead — every other connected client
          waits for this one disk write. `run_in_executor` hands it to a
          worker thread and returns an awaitable, so other connections keep
          being served.

          This is also precisely why Partition needs its threading.Lock: the
          executor is a POOL, so two appends really can run at the same
          instant on different threads.
        """
        return await asyncio.get_running_loop().run_in_executor(None, fn, *args)

    # ── Request routing ────────────────────────────────────────

    async def _route(self, api_key: int, api_version: int, payload: dict) -> dict:
        """
        Dispatch one request and return the response body.

        Every return value carries `error_code`. Success is
        `errors.NONE` (0) — sent explicitly rather than omitted, so a client
        never has to distinguish "no error" from "the field is missing
        because an older broker did not set it".
        """
        if not is_supported(api_key, api_version):
            # Two different failures share this branch, and the message
            # distinguishes them: an api_key we do not implement at all, and
            # one we implement at other versions only. Real Kafka separates
            # them; for us the client fix is the same either way.
            return _error(
                errors.UNSUPPORTED_VERSION,
                f"{api_name(api_key)} v{api_version} is not supported by this broker")

        if api_key == API_PRODUCE:
            # The one place a version actually branches. v0 is Phase 1's
            # single-record shape; v1 is Phase 3's batched, multi-partition
            # shape. Serving both is what a rolling upgrade looks like.
            if api_version >= 1:
                return await self._handle_produce_v1(payload)
            return await self._handle_produce(payload)
        if api_key == API_CREATE_TOPICS:
            return await self._handle_create_topics(payload)
        if api_key == API_FETCH:
            return await self._handle_fetch(payload)
        if api_key == API_LIST_OFFSETS:
            return await self._handle_list_offsets(payload)
        if api_key == API_METADATA:
            return await self._handle_metadata(payload)
        if api_key == API_REQUEST_VOTE:
            return await self._handle_request_vote(payload)
        if api_key == API_APPEND_ENTRIES:
            return await self._handle_append_entries(payload)
        if api_key == API_INSTALL_SNAPSHOT:
            return await self._handle_install_snapshot(payload)
        if api_key == API_BROKER_HEARTBEAT:
            return await self._handle_broker_heartbeat(payload)
        if api_key == API_CONTROLLER_UPDATE:
            return await self._handle_controller_update(payload)
        if api_key == API_DESCRIBE_CLUSTER:
            return await self._handle_describe_cluster(payload)

        # ── Consumer group APIs ────────────────────────────────
        # Every one of these can raise a KafkaError from the coordinator —
        # ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS — so
        # they share one translation layer rather than each repeating a
        # try/except. Those errors are normal protocol outcomes here, not
        # failures: a rebalance is a thing that happens, and telling the client
        # about it with a code is the whole mechanism.
        group_handlers = {
            API_FIND_COORDINATOR: self._handle_find_coordinator,
            API_JOIN_GROUP: self._handle_join_group,
            API_SYNC_GROUP: self._handle_sync_group,
            API_HEARTBEAT: self._handle_heartbeat,
            API_LEAVE_GROUP: self._handle_leave_group,
            API_OFFSET_COMMIT: self._handle_offset_commit,
            API_OFFSET_FETCH: self._handle_offset_fetch,
            API_LIST_GROUPS: self._handle_list_groups,
            API_DESCRIBE_GROUPS: self._handle_describe_group,
        }
        handler = group_handlers.get(api_key)
        if handler is not None:
            try:
                return await handler(payload)
            except errors.KafkaError as exc:
                return _error(exc.error_code, str(exc))

        # Unreachable: is_supported() already rejected unknown keys. Kept as a
        # belt-and-braces guard, because "unreachable" and "unreached" differ.
        return _error(errors.INVALID_REQUEST, f"unhandled api_key {api_key}")

    async def _handle_create_topics(self, payload: dict) -> dict:
        """
        Create a topic explicitly, with a chosen partition count.

        CONCEPT: why this API had to exist.
          Auto-creation always makes ONE partition, because a lazily-created
          topic has nobody to ask how wide it should be. So relying on it does
          not merely risk a typo — it silently costs you all the parallelism
          that Phase 3 was built to provide.

          Real Kafka calls the flag `auto.create.topics.enable`, and most
          production clusters turn it off for exactly these two reasons.
        """
        name = payload.get('topic')
        partitions = payload.get('partitions')
        replication_factor = payload.get('replication_factor', 1)

        if not name:
            return _error(errors.INVALID_REQUEST, "'topic' is required")

        # CONCEPT: creating a topic is a METADATA DECISION, so it goes through
        # the quorum like every other one.
        #
        # Writing it only to the local store was a real bug: if the broker you
        # asked was not the controller, the controller never learned the topic
        # existed and never placed its partitions. Every request for them was
        # answered NOT_LEADER, forever. It passed by luck whenever the broker
        # you happened to ask was also the leader.
        #
        # A non-leader forwards rather than refusing, so `kfk topics create`
        # can talk to any broker and does not need to know who is in charge.
        # `raft.is_leader`, not `cluster.is_controller`: the quorum is the
        # authority and the cluster view is a cache of it. Asking the cache
        # here made a freshly elected leader refuse its own topic creations
        # for one heartbeat interval — it did not yet know it had won.
        if not self.raft.is_leader:
            forwarded = await self._forward_to_controller(
                API_CREATE_TOPICS, payload)
            if forwarded is not None:
                return forwarded
            return _error(errors.LEADER_NOT_AVAILABLE,
                          "no controller is available to create topics right now")

        try:
            partitions = (partitions if partitions is not None
                          else self.metadata.default_partitions)
            if self.metadata.exists(name):
                raise InvalidTopicError(f"topic {name!r} already exists")
            validate_topic_name(name)
        except InvalidTopicError as exc:
            return _error(errors.INVALID_REQUEST, str(exc))

        # The replication factor is committed WITH the topic, not applied
        # locally afterwards. It is an input to placement, and placement runs
        # on whichever broker is controller — which may not be this one by the
        # time the next tick fires. A factor that lived only in the local
        # store would be invisible to the node that actually needs it, and the
        # partition would be silently created with one copy.
        committed = await self.raft.propose({
            'type': 'CREATE_TOPIC', 'name': name,
            'partitions': int(partitions),
            'replication_factor': int(replication_factor)})
        if not committed:
            return _error(errors.LEADER_NOT_AVAILABLE,
                          "could not commit the topic to the quorum")

        self._sync_metadata_from_raft()
        config = self.metadata.get(name)

        log.info("Created topic %s with %d partition(s) via the quorum",
                 name, config.partitions)
        await self._refresh_placement()
        return {
            'error_code': errors.NONE,
            'topic': name,
            'partitions': config.partitions,
            'replication_factor': config.replication_factor,
        }

    def _enough_replicas(self, topic: str, part_id: int):
        """
        Refuse an `acks=all` write when the ISR is too small. None if fine.

        CONCEPT: this is the CAP choice, made out loud.

          The broker is healthy, the disk is fine, the producer is connected —
          and the write is refused, because accepting it would mean
          acknowledging data that one machine failure could erase. A system
          that chooses consistency has to be willing to say no while looking
          perfectly well.

        Checked BEFORE the append, deliberately. Writing first and failing the
        acknowledgement afterwards would leave the record in the leader's log,
        where a later ISR recovery would replicate it — so a producer told
        FAILED would find its message in the topic anyway. Refusing up front
        is what makes the error mean what it says.

        It counts the ISR, not the replica set: three replicas of which one is
        dead is two in-sync, and pretending otherwise is exactly the lie this
        check exists to prevent.

        CONCEPT: committed ISR and OBSERVED ISR answer different questions, and
        this one needs both.

          The committed set (agreed through Raft) is what leader election must
          read — it has to outlive the leader that computed it. But a leader
          cut off from the quorum cannot commit anything, including the news
          that its followers have stopped fetching. Trusting the committed set
          alone, it would keep acknowledging `acks=all` writes claiming three
          copies while holding the only one.

          That was a real bug, found by isolating a leader that was also the
          controller: `ISR [1, 2, 3]` with `observed [1]`, and every write
          accepted.

          So acceptance uses the INTERSECTION. It can only ever be smaller than
          the committed set, which is the safe direction: a leader may refuse a
          write it could have taken, and may never take one it should refuse.
        """
        committed = self.cluster.isr_for(topic, part_id)
        observed = self.cluster.in_sync_replicas(
            topic, part_id, self.replica_lag_time_max_ms)
        in_sync = [n for n in committed if n in observed]

        if len(in_sync) >= self.min_insync_replicas:
            return None

        return _error(
            errors.NOT_ENOUGH_REPLICAS,
            f"{topic}-{part_id} has {len(in_sync)} in-sync replica(s) "
            f"{in_sync}, needs {self.min_insync_replicas}")

    async def _await_replication(self, topic: str, part_id: int,
                                 offset: int) -> bool:
        """
        Block until every in-sync replica holds `offset`. False on timeout.

        CONCEPT: `acks=all` does not count acknowledgements.

          The high watermark ALREADY means "every in-sync replica has this".
          So waiting for `hw > offset` is the whole implementation — no
          per-follower bookkeeping, no acknowledgement protocol, no second
          mechanism to keep consistent with the first. Step 3 built this; Step
          5 just consults it.

        Polling rather than an event, because the watermark advances inside
        another request's handler (a follower's FETCH) and a condition variable
        shared between them would buy microseconds at the cost of a lock this
        broker does not otherwise need.
        """
        partition = self._get_partition(topic, part_id)
        deadline = (asyncio.get_running_loop().time()
                    + self.acks_all_timeout_ms / 1000.0)

        while asyncio.get_running_loop().time() < deadline:
            watermark = self.cluster.high_watermark(
                topic, part_id, partition.next_offset,
                self.replica_lag_time_max_ms)
            if watermark > offset:
                return True
            await asyncio.sleep(0.002)

        return False

    async def _handle_produce_v1(self, payload: dict) -> dict:
        """
        Batched produce: many records, across many partitions, in one request.

        CONCEPT: per-partition error codes.
          One request now touches several partitions, so one top-level error
          code cannot describe the outcome. If partition 3 is out of range,
          partitions 0 and 1 must still succeed and say so — otherwise a single
          bad partition fails a batch of nine good ones and the client cannot
          tell which to resend.

          This is the shape Phase 1's Exercise 7 asked you to design, and it is
          why the error taxonomy put codes in the BODY rather than the header.

        `base_offset` is returned once per partition rather than once per
        record: offsets within a batch are contiguous, so the client resolves
        record i as `base_offset + i`. Saving n-1 integers per batch is a small
        thing on its own and exactly the kind of saving that makes batching
        cheaper than the sum of its parts.
        """
        topic = payload.get('topic')
        acks = payload.get('acks', 1)
        partitions = payload.get('partitions', [])

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")
        if not isinstance(partitions, list):
            return _error(errors.INVALID_REQUEST, "'partitions' must be a list")

        try:
            self.metadata.get_or_create(topic)
        except errors.KafkaError as exc:
            return _error(exc.error_code, str(exc))
        await self._refresh_placement()

        results = []
        total = 0

        for entry in partitions:
            part_id = entry.get('partition', 0)
            records = entry.get('records', [])

            try:
                self.metadata.validate_partition(topic, part_id)
            except errors.KafkaError as exc:
                # Per-partition, so the others in this request still proceed.
                results.append({'partition': part_id,
                                'error_code': exc.error_code,
                                'error_message': str(exc),
                                'base_offset': -1, 'count': 0})
                continue

            # Leadership is checked per partition too, for the same reason the
            # error codes are: one request may span several partitions, and
            # some may have moved while others did not. The epoch is
            # per-partition for exactly the same reason.
            wrong_leader = self._not_leader(
                topic, part_id, entry.get('leader_epoch'))
            if wrong_leader is not None:
                results.append(dict(wrong_leader, partition=part_id,
                                    base_offset=-1, count=0))
                continue

            if not records:
                results.append({'partition': part_id, 'error_code': errors.NONE,
                                'base_offset': -1, 'count': 0})
                continue

            if acks == 'all':
                short = self._enough_replicas(topic, part_id)
                if short is not None:
                    results.append(dict(short, partition=part_id,
                                        base_offset=-1, count=0))
                    continue

            partition = self._get_partition(topic, part_id)
            base_offset = await self._run_blocking(
                _append_records, partition, records)

            if acks == 'all':
                last = base_offset + len(records) - 1
                if not await self._await_replication(topic, part_id, last):
                    # The records ARE in the leader's log and will replicate
                    # once the followers recover. Saying so honestly beats
                    # claiming either success or a clean failure: the producer
                    # should retry, and a retry may duplicate — which is what
                    # at-least-once means and why Kafka has an idempotent
                    # producer that we do not.
                    results.append({
                        'partition': part_id,
                        'error_code': errors.REQUEST_TIMED_OUT,
                        'error_message': (
                            f"wrote {topic}-{part_id} at {base_offset} but it "
                            f"did not reach every in-sync replica in time"),
                        'base_offset': base_offset, 'count': len(records)})
                    total += len(records)
                    continue

            results.append({'partition': part_id, 'error_code': errors.NONE,
                            'base_offset': base_offset, 'count': len(records)})
            total += len(records)

        log.info("PRODUCE v1 → %s: %d records across %d partition(s), acks=%s",
                 topic, total, len(partitions), acks)

        # acks=0 means the client is not reading a reply, so this response is
        # written into a socket nobody will drain. Returning None tells
        # handle_client to send nothing at all — see the note there.
        if acks == 0:
            return None

        return {'error_code': errors.NONE, 'topic': topic, 'partitions': results}

    async def _handle_produce(self, payload: dict) -> dict:
        """PRODUCE v0 — one record, one partition. Phase 1's shape, still served."""
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        value = payload.get('value', '')
        key = payload.get('key')

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")

        try:
            self.metadata.get_or_create(topic)
            self.metadata.validate_partition(topic, part_id)
        except errors.KafkaError as exc:
            return _error(exc.error_code, str(exc))
        await self._refresh_placement()

        wrong_leader = self._not_leader(
            topic, part_id, payload.get('leader_epoch'))
        if wrong_leader is not None:
            return wrong_leader

        acks = payload.get('acks', 1)
        if acks == 'all':
            short = self._enough_replicas(topic, part_id)
            if short is not None:
                return short

        partition = self._get_partition(topic, part_id)
        offset = await self._run_blocking(partition.append, value, key)

        if acks == 'all' and not await self._await_replication(
                topic, part_id, offset):
            return _error(
                errors.REQUEST_TIMED_OUT,
                f"wrote {topic}-{part_id} at {offset} but it did not reach "
                f"every in-sync replica in time")

        log.info("PRODUCE -> %s-%s offset=%s value=%r", topic, part_id, offset, value)

        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'offset': offset,
        }

    async def _handle_fetch(self, payload: dict) -> dict:
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        start_offset = payload.get('offset', 0)
        max_msgs = payload.get('max', 10)

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")
        if start_offset < 0:
            return _error(errors.OFFSET_OUT_OF_RANGE,
                          f"offset {start_offset} is negative")

        try:
            self.metadata.get_or_create(topic)
        except errors.KafkaError as exc:
            return _error(exc.error_code, str(exc))
        await self._refresh_placement()

        wrong_leader = self._not_leader(
            topic, part_id, payload.get('leader_epoch'))
        if wrong_leader is not None:
            return wrong_leader

        partition = self._get_partition(topic, part_id)

        # Asking beyond the end is NOT an error — it is what every caught-up
        # consumer does on every poll. It returns an empty list, and the
        # consumer waits. Only asking below `log_start_offset` (data aged out
        # by retention, from Phase 2) is genuinely out of range.
        if start_offset < partition.log_start_offset:
            return _error(
                errors.OFFSET_OUT_OF_RANGE,
                f"offset {start_offset} is below the log start "
                f"{partition.log_start_offset}; that data has been deleted")

        # `replica_id` marks a FOLLOWER's fetch rather than a consumer's, and
        # it changes two things.
        replica_id = payload.get('replica_id')

        if replica_id is not None:
            # A follower asking for offset N has, by asking, proved it holds
            # everything below N. The request IS the progress report.
            self.cluster.record_replica_position(
                topic, part_id, int(replica_id), start_offset)

        watermark = self.cluster.high_watermark(
            topic, part_id, partition.next_offset,
            self.replica_lag_time_max_ms)

        # CONCEPT: consumers are clamped to the high watermark; followers are
        # not. The asymmetry is the whole idea, and it is circular in a way
        # that takes a moment to see.
        #
        #   The act of a follower READING a record is what makes that record
        #   safe for consumers. Clamp the follower too and nothing is ever
        #   replicated past the watermark, so the watermark never advances, so
        #   nothing is ever readable. The cluster would sit at offset 0 looking
        #   perfectly healthy.
        #
        # And why clamp consumers at all: a consumer that reads offset 9 from a
        # leader that then dies has acted on a record no survivor holds. The
        # new leader's log ends at 8, and its next write becomes offset 9 — a
        # DIFFERENT message wearing an offset somebody already processed. That
        # is worse than losing the record; it breaks the one-offset-one-message
        # guarantee everything else is built on.
        limit = None if replica_id is not None else watermark
        messages = await self._run_blocking(
            partition.fetch, start_offset, max_msgs, limit)

        # CONCEPT: long-polling. An empty answer is held, not sent.
        #
        # Without this, a caught-up follower sleeps between fetches and only
        # discovers a new record when it next wakes — so `acks=all` waits for
        # the follower's POLL INTERVAL rather than for the network. Measured:
        # 200 ms per message, an `acks=all` cost of 358x rather than the ~2x
        # the round trip actually justifies. The producer was paying for the
        # follower's sleep schedule.
        #
        # Polling harder would trade that for constant idle chatter. Holding
        # the request instead costs nothing while idle and returns the instant
        # data lands, which is why Kafka has `fetch.max.wait.ms` and why a
        # replica fetch sets it.
        max_wait_ms = float(payload.get('max_wait_ms') or 0)
        if not messages and max_wait_ms > 0:
            deadline = asyncio.get_running_loop().time() + max_wait_ms / 1000.0
            while asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.002)

                watermark = self.cluster.high_watermark(
                    topic, part_id, partition.next_offset,
                    self.replica_lag_time_max_ms)
                limit = None if replica_id is not None else watermark
                messages = await self._run_blocking(
                    partition.fetch, start_offset, max_msgs, limit)
                if messages:
                    break

        log.info("FETCH  ← %s-%s from=%s returned=%s hw=%s%s",
                 topic, part_id, start_offset, len(messages), watermark,
                 f" (replica {replica_id})" if replica_id is not None else '')

        # `high_watermark` no longer means "everything written". It now means
        # "everything REPLICATED" — which is what makes consumer lag a measure
        # of durable backlog rather than of disk backlog.
        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'messages': messages,
            'count': len(messages),
            'high_watermark': watermark,
            'log_end_offset': partition.next_offset,
            'log_start_offset': partition.log_start_offset,
        }

    async def _handle_list_offsets(self, payload: dict) -> dict:
        """
        Answer "where does this partition start / end?" without reading data.

        CONCEPT: why this deserves its own API.
          A consumer that wants to start at the newest message could fetch
          everything and throw it away — absurd for a large log. LIST_OFFSETS
          answers from metadata the broker already holds, in constant time and
          zero bytes of payload.

          The timestamp field uses Kafka's sentinels: -1 (LATEST) and
          -2 (EARLIEST). Real Kafka also accepts an actual millisecond
          timestamp and answers "the first offset at or after that time",
          which is how "replay from 9am" works. We support only the
          sentinels — the general form needs the time index that Phase 2's
          storage engine introduces.
        """
        topic = payload.get('topic')
        part_id = payload.get('partition', 0)
        timestamp = payload.get('timestamp', TIMESTAMP_LATEST)

        if not topic:
            return _error(errors.INVALID_REQUEST, "'topic' is required")

        # Existence is checked BEFORE leadership, and the order is deliberate.
        #
        # A topic that does not exist is not a topic somebody else leads — it is
        # a topic nobody leads. Answering NOT_LEADER would send the client off
        # to refresh its metadata and retry, forever, for a name that is simply
        # a typo. UNKNOWN_TOPIC_OR_PARTITION is the honest answer, and the more
        # specific of two true statements is the useful one.
        if not self.metadata.exists(topic):
            return _error(errors.UNKNOWN_TOPIC_OR_PARTITION,
                          f"no topic {topic!r} on this broker")

        wrong_leader = self._not_leader(topic, part_id)
        if wrong_leader is not None:
            return wrong_leader

        # A query, so it does not create. See _get_partition's docstring.
        partition = self._find_partition(topic, part_id)
        if partition is None:
            return _error(errors.UNKNOWN_TOPIC_OR_PARTITION,
                          f"no partition {topic}-{part_id} on this broker")

        if timestamp == TIMESTAMP_EARLIEST:
            offset = partition.log_start_offset
        elif timestamp == TIMESTAMP_LATEST:
            offset = partition.next_offset
        else:
            return _error(
                errors.INVALID_REQUEST,
                f"timestamp {timestamp} not supported; use "
                f"{TIMESTAMP_EARLIEST} (earliest) or {TIMESTAMP_LATEST} (latest). "
                f"Lookup by real timestamp needs the time index from Phase 2.")

        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'timestamp': timestamp,
            'offset': offset,
        }

    async def _handle_metadata(self, payload: dict) -> dict:
        """
        Describe the cluster and its topics.

        CONCEPT: this is the API that makes Kafka clients "thick".
          A client does not send data to some load balancer that figures out
          where it goes. It asks for metadata, learns which broker leads which
          partition, caches that, and connects to the right broker directly —
          no proxy hop, no extra network round trip on the data path.

          The cost is that the cache goes stale exactly when it matters (a
          leader just failed). The cure is NOT_LEADER_OR_FOLLOWER: the wrong
          broker says so, the client refreshes, and retries. Metadata plus
          that one error code is the whole failover mechanism, and both exist
          now, before there is a cluster to use them.

        Request: {'topics': ['orders']} — or omit/None for every topic.
        """
        requested = payload.get('topics')

        # CONCEPT: a metadata request can CREATE a topic, and it has to.
        #
        # A producer must know the partition count before it can route its
        # first message — `murmur2(key) % count` needs the count. But for a
        # brand-new topic, asking produces "does not exist", the client assumes
        # 1, and every keyed message goes to partition 0. The topic is then
        # auto-created with its real width on the first PRODUCE, and the
        # producer's cached 1 is now wrong.
        #
        # The fix is the one real Kafka uses: the client asks metadata to
        # create the topic if needed, so the answer it caches is the real one.
        # Off by default, because a plain query must NOT conjure the thing it
        # is asking about — that would make "does this exist?" unanswerable.
        if requested and payload.get('allow_auto_topic_creation') \
                and self.metadata.auto_create:
            for name in requested:
                try:
                    self.metadata.get_or_create(name)
                except (errors.KafkaError, InvalidTopicError):
                    pass        # reported per topic below, as usual
            await self._refresh_placement()

        # Partition lists now come from the METADATA STORE, not from which
        # directories happen to exist. That distinction is the whole reason
        # Phase 3 needed a metadata store: a 4-partition topic must report four
        # partitions the moment it is created, before any of them hold data —
        # otherwise a producer asking "how wide is this topic?" would get an
        # answer that grows as messages arrive, and key routing would change
        # underneath it.
        names = requested if requested else self.metadata.list_topics()
        missing = [name for name in names if not self.metadata.exists(name)]

        topics = []
        for name in sorted(n for n in names if self.metadata.exists(n)):
            config = self.metadata.get(name)
            partition_entries = []

            for part_id in config.partition_ids():
                partition = self._find_partition(name, part_id)
                leader_id = self.cluster.leader_for(name, part_id)
                partition_entries.append({
                    'partition': part_id,
                    # The real leader, from the controller's map. -1 when
                    # unassigned, so a client can tell "not placed yet" from
                    # "placed on broker 0" — the same -1-vs-0 distinction that
                    # committed offsets needed in Phase 4.
                    'leader': leader_id if leader_id is not None else -1,
                    # Real replica sets and a real ISR since Phase 6. With one
                    # broker these collapse to a single trivially-in-sync copy,
                    # which is why the SHAPE was right from Phase 3 and clients
                    # written against it never had to change.
                    'replicas': self.cluster.replicas_for(name, part_id),
                    'isr': self.cluster.isr_for(name, part_id),
                    # Handed out so a client can send it BACK on produce and
                    # fetch. That round trip is what lets a broker recognise a
                    # request aimed at a leadership it no longer holds.
                    'leader_epoch': self.cluster.leader_epoch(name, part_id),
                    # A declared-but-never-written partition has no log yet.
                    # It is still a real partition — reporting 0/0 is honest,
                    # and omitting it would under-report the topic's width.
                    'high_watermark': partition.next_offset if partition else 0,
                    'log_start_offset': partition.log_start_offset if partition else 0,
                })

            topics.append({'name': name, 'error_code': errors.NONE,
                           'partitions': partition_entries})

        # A topic that was asked for but does not exist is reported per topic,
        # not as a failure of the whole request — one unknown name must not
        # hide the nine that were fine.
        topics.extend({
            'name': name,
            'error_code': errors.UNKNOWN_TOPIC_OR_PARTITION,
            'partitions': [],
        } for name in sorted(missing))

        return {
            'error_code': errors.NONE,
            # EVERY broker in the cluster, not just this one.
            #
            # This is what makes a client able to reach the whole cluster from
            # a single bootstrap address: it connects to any broker, learns the
            # full roster plus the leader map, and then talks to each leader
            # directly. Reporting only ourselves would leave a client able to
            # route in theory and unable to connect in practice — which is
            # exactly the bug this line replaced, and it failed silently
            # because the producer's futures were never inspected.
            'brokers': self.cluster_config.as_list(),
            'controller_id': self.node_id,
            'topics': topics,
        }

    # ── Consumer group handlers ────────────────────────────────

    async def _handle_find_coordinator(self, payload: dict) -> dict:
        """
        Which broker coordinates this group?

        Always us, because there is only one broker. The API exists now anyway
        so that Phase 5 adds a CLUSTER rather than a protocol change — real
        Kafka hashes the group id to pick a coordinator, spreading groups
        across brokers so no single node owns every group's state.
        """
        if not payload.get('group_id'):
            return _error(errors.INVALID_REQUEST, "'group_id' is required")
        return {'error_code': errors.NONE, 'node_id': self.node_id,
                'host': self.host, 'port': self.port}

    async def _handle_join_group(self, payload: dict) -> dict:
        """
        Join a group. **This request is held open** until the join phase ends.

        That is not a quirk — the coordinator cannot answer the first member
        until it knows who else is joining, because the leader needs the full
        member list to compute an assignment. Real Kafka calls the holding
        area "purgatory".

        Holding a request is also the clearest justification for asyncio in
        this project: an awaited Event costs a few hundred bytes, where a
        thread-per-request server would park an OS thread per member of every
        rebalancing group.
        """
        group_id = payload.get('group_id')
        topics = payload.get('topics', [])

        if not group_id:
            return _error(errors.INVALID_REQUEST, "'group_id' is required")
        if not topics:
            return _error(errors.INVALID_REQUEST, "'topics' must not be empty")

        result = await self.coordinator.join_group(
            group_id,
            payload.get('member_id', ''),
            topics,
            payload.get('session_timeout_ms', 10_000),
            payload.get('rebalance_timeout_ms', 5_000),
        )

        # The leader needs partition counts to compute an assignment. Sending
        # them with the join response saves it a separate METADATA round trip
        # at the exact moment the whole group is stalled waiting for it.
        if result['is_leader']:
            counts = {}
            for topic in {t for m in result['members'] for t in m['topics']}:
                config = self.metadata.get(topic)
                if config is None:
                    config = self.metadata.get_or_create(topic)
                counts[topic] = config.partitions
            result['partition_counts'] = counts

        log.info("JOIN_GROUP %s member=%s gen=%s leader=%s",
                 group_id, result['member_id'][:8], result['generation_id'],
                 result['is_leader'])

        result['error_code'] = errors.NONE
        return result

    async def _handle_sync_group(self, payload: dict) -> dict:
        """Leader uploads assignments; everyone else collects theirs."""
        group_id = payload.get('group_id')
        member_id = payload.get('member_id')
        if not group_id or not member_id:
            return _error(errors.INVALID_REQUEST,
                          "'group_id' and 'member_id' are required")

        result = await self.coordinator.sync_group(
            group_id, member_id, payload.get('generation_id', -1),
            payload.get('assignments'))
        result['error_code'] = errors.NONE
        return result

    async def _handle_heartbeat(self, payload: dict) -> dict:
        result = await self.coordinator.heartbeat(
            payload.get('group_id'), payload.get('member_id'),
            payload.get('generation_id', -1))
        result['error_code'] = errors.NONE
        return result

    async def _handle_leave_group(self, payload: dict) -> dict:
        result = await self.coordinator.leave_group(
            payload.get('group_id'), payload.get('member_id'))
        result['error_code'] = errors.NONE
        return result

    async def _handle_offset_commit(self, payload: dict) -> dict:
        """
        Record a group's positions, fenced by generation.

        The fencing check is the point. A consumer that paused past its session
        timeout was evicted and its partitions reassigned; if its late commit
        were accepted it would rewind the group behind the replacement's
        progress, and those messages would be processed twice with nothing
        reporting it.

        `member_id` may be omitted for a consumer using `assign()` rather than
        `subscribe()` — no group membership means nothing to fence against,
        which is a real trade: manual assignment gives up the protection along
        with the coordination.
        """
        group_id = payload.get('group_id')
        offsets = payload.get('offsets', [])
        if not group_id:
            return _error(errors.INVALID_REQUEST, "'group_id' is required")

        member_id = payload.get('member_id')
        if member_id:
            async with self.coordinator._lock:
                group = self.coordinator._group(group_id)
                self.coordinator._check_member(
                    group, member_id, payload.get('generation_id', -1))
                group.members[member_id].touch()

        results = []
        for entry in offsets:
            topic = entry.get('topic')
            partition = entry.get('partition', 0)
            offset = entry.get('offset', 0)
            if not topic:
                results.append({'topic': topic, 'partition': partition,
                                'error_code': errors.INVALID_REQUEST})
                continue

            await self._run_blocking(self.offsets.commit, group_id, topic,
                                     partition, offset)
            results.append({'topic': topic, 'partition': partition,
                            'error_code': errors.NONE})

        return {'error_code': errors.NONE, 'offsets': results}

    async def _handle_offset_fetch(self, payload: dict) -> dict:
        """
        Read back a group's committed positions.

        An offset of -1 means "this group has never committed this partition",
        which is different from 0. The consumer applies its reset policy
        (earliest or latest) rather than assuming either — collapsing the two
        would make a brand-new group silently replay everything, or silently
        skip it, depending on which default you happened to pick.
        """
        group_id = payload.get('group_id')
        if not group_id:
            return _error(errors.INVALID_REQUEST, "'group_id' is required")

        requested = payload.get('partitions')
        if requested:
            offsets = [
                {'topic': entry['topic'], 'partition': entry.get('partition', 0),
                 'offset': self.offsets.fetch(group_id, entry['topic'],
                                              entry.get('partition', 0))}
                for entry in requested
            ]
        else:
            offsets = [
                {'topic': topic, 'partition': partition, 'offset': offset}
                for (topic, partition), offset
                in sorted(self.offsets.fetch_group(group_id).items())
            ]

        return {'error_code': errors.NONE, 'group_id': group_id,
                'offsets': offsets}

    async def _handle_list_groups(self, payload: dict) -> dict:
        """
        Every group this broker knows about.

        Includes groups that only exist in `__consumer_offsets` — a group whose
        members have all gone away still has committed positions, and hiding it
        would make "where did my consumer get to?" unanswerable after a
        shutdown. That is precisely when you want to look.
        """
        live = {}
        for group_id in await self.coordinator.list_groups():
            described = await self.coordinator.describe(group_id)
            live[group_id] = {
                'group_id': group_id,
                'state': described['state'],
                'members': len(described['members']),
                'generation_id': described['generation_id'],
            }

        for group_id in self.offsets.groups():
            live.setdefault(group_id, {
                'group_id': group_id, 'state': 'Empty (has offsets)',
                'members': 0, 'generation_id': -1})

        return {'error_code': errors.NONE,
                'groups': [live[g] for g in sorted(live)]}

    async def _handle_describe_group(self, payload: dict) -> dict:
        group_id = payload.get('group_id')
        if not group_id:
            return _error(errors.INVALID_REQUEST, "'group_id' is required")

        try:
            described = await self.coordinator.describe(group_id)
        except errors.UnknownMemberIdError:
            # A group with committed offsets but no live members is a real and
            # normal state — every consumer shut down. Report it rather than
            # erroring, so its lag is still inspectable.
            if group_id in self.offsets.groups():
                described = {'group_id': group_id, 'state': 'Empty',
                             'generation_id': -1, 'leader_id': None,
                             'protocol': 'range', 'members': []}
            else:
                raise

        described['error_code'] = errors.NONE
        return described

    async def _refresh_placement(self) -> None:
        """
        Reassign partitions after the set of topics changed, and push.

        CONCEPT: placement reacts to TWO kinds of event, not one.
          The membership loop recomputes when brokers come and go. But a topic
          created a moment ago has partitions nobody has been assigned — and
          until the controller places them, every request for them is answered
          NOT_LEADER.

          Leaving it to the next heartbeat tick would "work" (within
          `broker_heartbeat_ms`) and be wrong in the way that matters: a
          `CREATE_TOPICS` immediately followed by a `PRODUCE` — which is what
          every client and every test does — would fail. Correctness that
          depends on a timer being fast enough is not correctness.

        A no-op on non-controller brokers: they receive the map by push
        instead. `recompute()` returns False when nothing changed, so the
        common case costs one dict comparison.
        """
        if self.cluster.is_controller and await self.controller.recompute():
            # Broadcast in the BACKGROUND, not inline.
            #
            # Awaiting the push here would make a client's CREATE_TOPICS wait
            # on every peer — including unreachable ones, each costing a full
            # connect timeout. One dead broker would then slow down every
            # metadata change for every client, which is a dead broker causing
            # a cluster-wide latency problem rather than a local one.
            #
            # The push is a broadcast the controller owes its peers, not part
            # of the requester's transaction. A peer that misses it stays stale
            # until the next change, and NOT_LEADER corrects any client that
            # reaches the wrong broker meanwhile.
            #
            # The reference is kept because asyncio only holds a weak one:
            # a task nobody references can be garbage-collected mid-flight.
            task = asyncio.create_task(self._push_controller_update())
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    def _sync_metadata_from_raft(self) -> None:
        """
        Mirror committed topics from the Raft state machine into MetadataStore.

        The Raft machine is the SOURCE OF TRUTH; `MetadataStore` is a local
        mirror that the rest of the broker already reads. Keeping the mirror
        one-way — machine to store, never back — is what stops the two becoming
        independent opinions, which is the same rule `CONTROLLER_UPDATE` now
        follows.
        """
        for name, partitions in self.raft.machine.topics.items():
            try:
                self.metadata.ensure_topic(
                    name, int(partitions),
                    self.raft.machine.replication_factor(name))
            except InvalidTopicError:
                pass

    async def _forward_to_controller(self, api_key: int, payload: dict):
        """
        Send a request on to the controller and return its reply, or None.

        Forwarding rather than refusing keeps clients simple: any broker
        accepts a metadata request. The cost is one extra hop, paid only on
        rare control-plane operations — never on the data path, where clients
        still route themselves to the partition leader directly.
        """
        # Ask RAFT, not the cluster view. `ClusterState.controller_id` is a
        # cache refreshed by the membership loop, so it trails the quorum by up
        # to one heartbeat interval — and during that window a broker would
        # answer "no controller is available" while a healthy leader was
        # already elected and serving. Small window, real refusal: a client
        # that creates a topic immediately after startup hits it every time.
        controller_id = self.raft.leader_id
        if controller_id is None:
            controller_id = self.cluster.controller_id
        if controller_id is None or controller_id == self.node_id:
            return None
        peer = self.cluster_config.get(controller_id)
        if peer is None:
            return None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.host, peer.port), timeout=5.0)
        except (OSError, asyncio.TimeoutError):
            return None
        try:
            writer.write(encode_message(api_key, payload, correlation_id=1))
            await writer.drain()
            header = decode_header(await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE), timeout=5.0))
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

    # ── Raft transport ─────────────────────────────────────────

    async def _raft_rpc(self, node_id: int, api: str, payload: dict):
        """
        Send one Raft RPC to a peer. Returns None if it is unreachable.

        `RaftNode` never imports a socket: the transport is injected so the
        algorithm can be driven in memory by the tests, where a network
        partition is a set membership check rather than an OS-level
        manipulation. Keeping consensus separate from I/O is what makes it
        testable at all.
        """
        peer = self.cluster_config.get(node_id)
        if peer is None:
            return None

        if getattr(self, 'partitioned_from', None) and \
                node_id in self.partitioned_from:
            # The same test hook the Phase 5 heartbeats honour, so
            # chaos/split_brain.py can cut Raft traffic too. Without this the
            # "partition" would only block heartbeats while consensus messages
            # sailed through, and the experiment would prove nothing.
            return None

        api_key = {'REQUEST_VOTE': API_REQUEST_VOTE,
                   'INSTALL_SNAPSHOT': API_INSTALL_SNAPSHOT}.get(
                       api, API_APPEND_ENTRIES)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.host, peer.port), timeout=2.0)
        except (OSError, asyncio.TimeoutError):
            return None

        try:
            writer.write(encode_message(api_key, payload, correlation_id=1))
            await writer.drain()
            header = decode_header(await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE), timeout=2.0))
            body = await reader.readexactly(header.body_length)
            return decode_body(body)
        except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError,
                ValueError):
            return None
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _handle_request_vote(self, payload: dict) -> dict:
        result = await self.raft.handle_request_vote(payload)
        result['error_code'] = errors.NONE
        return result

    async def _handle_append_entries(self, payload: dict) -> dict:
        result = await self.raft.handle_append_entries(payload)
        result['error_code'] = errors.NONE
        return result

    async def _handle_install_snapshot(self, payload: dict) -> dict:
        result = await self.raft.handle_install_snapshot(payload)
        result['error_code'] = errors.NONE
        return result

    # ── Cluster handlers ───────────────────────────────────────

    def _not_leader(self, topic: str, partition: int, epoch: int = None):
        """
        Return an error body if this broker may not serve the partition.

        CONCEPT: refusing is the whole point.
          Without this check a broker asked for a partition it does not lead
          would happily create an empty local log and serve zero messages. The
          client would see an EMPTY TOPIC rather than an error — silent
          wrongness, the failure mode this project keeps trying to avoid.

          Refusing turns it into a retriable error the client already knows how
          to handle: refresh metadata, find the real leader, retry. That is
          client-side failover, and it needed nothing new on the client.

        An UNASSIGNED partition also fails here. A partition the controller has
        not placed yet is not ours to serve, and the client should come back
        once placement has happened.

        CONCEPT: two failures, two error codes (Phase 6 step 6).

          `NOT_LEADER_OR_FOLLOWER` means "I never led this" — routine, and
          happens on every placement change. `FENCED_LEADER_EPOCH` means "I
          thought I led this and I am the stale one", which is the split-brain
          case on the DATA plane: a partitioned broker still accepting writes
          into a log nobody will read, telling producers they succeeded.

          Both are retriable and the client handles them identically. The
          distinction is for the operator: the first is noise, the second is a
          broker that was serving a partition it had already lost.

        `epoch` is what the CALLER believes. A client that has never heard of
        epochs passes None and is not fenced — it will be routed correctly by
        the leadership check anyway, and rejecting it would break every
        pre-Phase-6 client for no safety gain.
        """
        if not self.cluster.leads(topic, partition):
            leader = self.cluster.leader_for(topic, partition)
            return _error(
                errors.NOT_LEADER_OR_FOLLOWER,
                f"broker {self.node_id} does not lead {topic}-{partition}"
                + (f"; broker {leader} does" if leader is not None
                   else "; it has not been assigned yet"))

        if epoch is not None:
            mine = self.cluster.leader_epoch(topic, partition)
            theirs = int(epoch)

            # THE DANGEROUS DIRECTION: the caller knows a NEWER epoch than we
            # do. We still believe we lead this partition, and the only way
            # the caller could have a higher number is that leadership moved
            # and we have not heard.
            #
            # This is the split-brain case on the data plane, and it is the
            # one the whole mechanism exists for. Getting the comparison
            # backwards — refusing the stale CLIENT instead of the stale
            # BROKER — passes every test and fences nothing: verified by
            # producing to an isolated leader with a newer epoch and watching
            # it succeed.
            if theirs > mine:
                return _error(
                    errors.FENCED_LEADER_EPOCH,
                    f"broker {self.node_id} believes it leads "
                    f"{topic}-{partition} at epoch {mine}, but the caller "
                    f"knows epoch {theirs}; this broker is stale")

            # The other direction: the caller's metadata is older than ours.
            # Harmless on its own — we DO lead the partition, which is what
            # the caller wanted — but worth refusing so the caller refreshes
            # rather than continuing to route on a stale map that may be wrong
            # about other partitions too.
            if theirs < mine:
                return _error(
                    errors.FENCED_LEADER_EPOCH,
                    f"{topic}-{partition} is at leader epoch {mine}, "
                    f"request carried {theirs}")

        return None

    async def _handle_broker_heartbeat(self, payload: dict) -> dict:
        """
        A peer telling us it is alive.

        Liveness is bidirectional here: receiving a heartbeat counts as
        evidence just as much as a successful outbound one. That halves the
        time to notice a peer has come back, and costs nothing.
        """
        peer_id = payload.get('node_id')
        if peer_id is None:
            return _error(errors.INVALID_REQUEST, "'node_id' is required")

        self.cluster.mark_alive(int(peer_id))

        # Phase 6: the sender may have told us who is keeping up with the
        # partitions IT leads. Only a leader can see that, so this is the only
        # way a controller learns the ISR of a partition it does not serve.
        report = payload.get('isr_report')
        if report:
            self.cluster.record_reported_isr(int(peer_id), report)

        return {'error_code': errors.NONE, 'node_id': self.node_id,
                'controller_id': self.cluster.controller_id,
                'metadata_version': self.cluster.metadata_version}

    async def _handle_controller_update(self, payload: dict) -> dict:
        """
        The controller telling us the new partition→leader map.

        We accept it only if its version is newer than ours
        (`ClusterState.set_leaders`). Without that check a network-delayed
        update could arrive after a newer one and silently roll our view
        backwards — two brokers would then disagree about who leads what, which
        is exactly what having a single controller is supposed to prevent.

        We do NOT verify the sender is the controller we elected. Under a
        network partition two brokers legitimately believe they are the
        controller, and version comparison is all the fencing this design has.
        Real Kafka stamps a leader epoch and rejects a stale controller's
        writes outright; see the phase doc.
        """
        version = int(payload.get('metadata_version', 0))
        entries = payload.get('partitions', [])

        # Adopt the controller's topic definitions FIRST. The leader map refers
        # to partitions of these topics, so applying the map without them would
        # leave us holding assignments for topics we do not believe in.
        for name, partitions in (payload.get('topics') or {}).items():
            try:
                self.metadata.ensure_topic(name, int(partitions))
            except InvalidTopicError:
                pass

        applied = self.cluster.set_leaders_from_wire(entries, version)

        if applied:
            log.info("Applied controller update v%d from broker %s: "
                     "%d partition(s), %d led locally",
                     version, payload.get('controller_id'), len(entries),
                     len(self.cluster.my_partitions()))

        return {'error_code': errors.NONE, 'applied': applied,
                'metadata_version': self.cluster.metadata_version}

    async def _handle_describe_cluster(self, payload: dict) -> dict:
        """Everything `kfk cluster status` needs, from any broker."""
        snapshot = self.cluster.snapshot()
        snapshot['error_code'] = errors.NONE
        snapshot['brokers'] = [
            dict(entry, alive=entry['node_id'] in snapshot['live_brokers'])
            for entry in self.cluster_config.as_list()
        ]
        return snapshot

    # ── Connection handling ────────────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        """
        One coroutine per connected client, for the life of the connection.

        Reads requests until the client hangs up. One connection carries many
        requests — reconnecting per message would spend more time on TCP
        handshakes than on data.
        """
        addr = writer.get_extra_info('peername')
        log.info("Client connected: %s", addr)

        try:
            while True:
                # Step 1: the fixed 16-byte header. readexactly() is asyncio's
                # version of the loop in common/net.py, for the same reason:
                # TCP delivers a byte stream with no message boundaries.
                try:
                    header_bytes = await reader.readexactly(HEADER_SIZE)
                except asyncio.IncompleteReadError:
                    break                     # clean disconnect, not an error

                header = decode_header(header_bytes)

                # Step 2: validate framing before trusting anything in it.
                if header.magic != MAGIC:
                    # Not our protocol, or the stream desynchronised. With no
                    # delimiters there is no way to resynchronise, so hanging
                    # up is the only correct move.
                    log.warning("Bad magic from %s: %#x — closing",
                                addr, header.magic)
                    break

                # Step 3: refuse absurd lengths BEFORE allocating for them.
                #
                # `body_length` is an unsigned 32-bit integer straight off the
                # network. Trusting it means a client can claim 4 GB and have
                # us try to allocate it — a denial of service that needs no
                # credentials, only a socket. We answer, then close: the
                # connection cannot continue, because the bytes it claimed are
                # still queued behind us and would be read as a header.
                if header.body_length > MAX_REQUEST_BYTES:
                    log.warning("Oversized request from %s: %s bytes",
                                addr, header.body_length)
                    await _reply(writer, header, _error(
                        errors.MESSAGE_TOO_LARGE,
                        f"request is {header.body_length:,} bytes, limit is "
                        f"{MAX_REQUEST_BYTES:,}"))
                    break

                try:
                    body_bytes = await reader.readexactly(header.body_length)
                except asyncio.IncompleteReadError:
                    log.warning("Client %s vanished mid-body", addr)
                    break

                # Step 4: decode. A bad body is the CLIENT's fault, so answer
                # with an error and keep the connection alive. Phase 0's broad
                # `except Exception` killed the connection here and buried
                # real bugs in the same handler.
                try:
                    payload = decode_body(body_bytes)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    log.warning("Malformed body from %s: %s", addr, exc)
                    await _reply(writer, header, _error(
                        errors.INVALID_REQUEST, f"malformed body: {exc}"))
                    continue

                if not isinstance(payload, dict):
                    await _reply(writer, header, _error(
                        errors.INVALID_REQUEST,
                        f"body must be a JSON object, got {type(payload).__name__}"))
                    continue

                # Step 5: handle it.
                try:
                    response = await self._route(
                        header.api_key, header.api_version, payload)
                except Exception:
                    # A bug in a handler must not take down the connection or
                    # leave the client waiting forever for a reply that never
                    # comes. Log the traceback for us; return a generic code
                    # to them. UNKNOWN_SERVER_ERROR is non-retriable, which is
                    # right: hammering a broker that is throwing exceptions
                    # helps nobody.
                    log.exception("Handler crashed on %s from %s",
                                  api_name(header.api_key), addr)
                    response = _error(errors.UNKNOWN_SERVER_ERROR,
                                      "internal broker error")

                # Step 6: answer, echoing api_key and correlation_id.
                #
                # A None response means acks=0: the client is not going to read
                # a reply, so writing one would leave bytes in the socket that
                # the NEXT response gets read as. Sending nothing is what keeps
                # the stream in sync — and it is the one case where this
                # otherwise strictly request/response protocol has no response.
                if response is not None:
                    await _reply(writer, header, response)

        except (ConnectionResetError, BrokenPipeError):
            # The client died or the network dropped. Extremely normal; not
            # worth a stack trace.
            log.info("Client %s disconnected abruptly", addr)
        except Exception:
            log.exception("Unhandled error serving %s", addr)
        finally:
            writer.close()
            # CONCEPT: close() only *schedules* the close.
            #   Buffered response bytes may not have reached the socket, and
            #   the file descriptor is not released until it completes.
            #   Without wait_closed(), a busy broker leaks descriptors and
            #   clients occasionally see truncated replies.
            #
            #   The try/except is required: if the peer already vanished,
            #   wait_closed() raises — and an exception from a finally block
            #   replaces whatever real error we were handling.
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            log.info("Client disconnected: %s", addr)

    # ── Lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        """
        Bind the port and serve until `stop()` is called.

        We await a shutdown event rather than `serve_forever()` so shutdown is
        orderly: stop accepting, then close every partition's file handle.
        Tests need this, and so does anyone who would rather not leave the
        last few messages sitting in a buffer at exit.
        """
        self._loop = asyncio.get_running_loop()
        self._shutdown = asyncio.Event()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        # port=0 means "OS, pick any free port". Read back what it chose so
        # tests can connect — which is why the suite never collides with a dev
        # broker already sitting on 9092.
        self.port = server.sockets[0].getsockname()[1]

        # The roster was built in __init__, BEFORE we bound — so a broker
        # started with port=0 recorded port 0 for itself. METADATA hands that
        # roster to clients, which would then dutifully dial port 0 and hang
        # until their socket timeout.
        #
        # Correcting it here is the only place we can: the real port does not
        # exist until the listener is bound. A value that is only knowable
        # after construction cannot be captured during it.
        own = self.cluster_config.get(self.node_id)
        if own is not None:
            own.port = self.port
            own.host = self.host

        log.info("Broker %s listening on %s:%s", self.node_id, self.host, self.port)
        log.info("Data directory: %s", os.path.abspath(self.data_dir))
        self.ready.set()

        await self.raft.start()

        # A single-node quorum elects itself immediately — it IS its own
        # majority — so a standalone broker is usable at once. A real quorum
        # takes an election timeout to agree, during which requests are
        # answered NOT_LEADER and clients retry. That pause is the honest cost
        # of consensus, and it did not exist with min(live_brokers).
        if len(self.quorum_voters) == 1:
            await asyncio.sleep(0)
            await self.raft._begin_election()
            self.cluster.set_controller(self.raft.leader_id)
            await self.controller.recompute()

        self.replicas.start()
        self.replicas.reconcile()

        cleaner = asyncio.create_task(self._retention_loop())
        reaper = asyncio.create_task(self._group_reap_loop())
        membership = asyncio.create_task(self._membership_loop())

        async with server:
            await self._shutdown.wait()

        cleaner.cancel()
        reaper.cancel()
        membership.cancel()
        await self.replicas.stop()
        await self.raft.stop()
        self.offsets.close()
        self._close_partitions()
        log.info("Broker stopped.")

    async def _membership_loop(self) -> None:
        """
        Heartbeat every peer, recompute liveness, elect, and place.

        The whole cluster mechanism in one loop:

            1. heartbeat each peer            (who answers?)
            2. elect: lowest live id          (computed locally, no messages)
            3. if I am the controller and the map changed, push it

        Step 2 is the interesting one. **Every broker runs it against its own
        liveness view**, so agreement is free when the views agree — and absent
        when they do not. A network partition gives each side a different view
        and therefore a different controller. That is the documented cost of
        lowest-live-id election, demonstrated by `chaos/split_brain.py`.
        """
        interval = self.broker_heartbeat_ms / 1000.0
        while True:
            try:
                await asyncio.sleep(interval)
                await self._heartbeat_peers()

                # Leadership is whatever the QUORUM decided. We do not compute
                # an opinion of our own any more — that is the change that
                # fixes split-brain, and it is one line.
                # Followers learn about topics by applying the Raft log, so
                # mirror what has been committed into the local store.
                self._sync_metadata_from_raft()

                previous = self.cluster.controller_id
                elected = self.cluster.set_controller(self.raft.leader_id)
                if elected != previous:
                    log.info("Controller is now broker %s (was %s); "
                             "raft term %s, live=%s",
                             elected, previous, self.raft.current_term,
                             self.cluster.live_brokers())

                if self.cluster.is_controller and await self.controller.recompute():
                    await self._push_controller_update()

                # Start and stop follower loops to match the placement we now
                # hold. Runs every tick, not only on change: it is cheap and
                # idempotent, and "the placement changed" is exactly the event
                # most likely to be missed.
                self.replicas.reconcile()

            except asyncio.CancelledError:
                return
            except Exception:
                # A bug here must not take down a broker that is otherwise
                # serving reads and writes perfectly well.
                log.exception("Membership tick failed; will retry")

    async def _heartbeat_peers(self) -> None:
        """
        Contact every peer CONCURRENTLY, and do not wait for the round.

        Concurrently, not in sequence: with five peers and a slow timeout, one
        unreachable broker would otherwise stall the loop and make every other
        peer look dead too — a single failure cascading into a total one.

        CONCEPT: concurrent was not enough. `gather` finishes with the SLOWEST
        peer, so awaiting the round still paced this loop at the dead broker's
        connect timeout — 2 s, against a 700 ms session timeout. A perfectly
        healthy peer was therefore refreshed only every 2 s and spent most of
        its life "expired": `live_brokers` flapped between {1} and {1,2} once
        a second, on a cluster where nothing was wrong with either node.

        That flapping was not cosmetic. Membership changes drive placement, so
        the controller re-proposed the partition map on every flap, and each
        proposal took its own 2 s waiting on the same corpse. One dead broker
        out of three was enough to keep the survivors permanently busy
        rediscovering each other.

        So the round is fired off and the loop moves on. Each peer records its
        own result when it lands, and a peer with a heartbeat still in flight
        is skipped rather than queued behind itself.
        """
        peers = self.cluster_config.peers(self.node_id)
        if not peers:
            return

        for peer in peers:
            if peer.node_id in self._heartbeats_inflight:
                continue
            self._heartbeats_inflight.add(peer.node_id)
            self._spawn(self._heartbeat_and_record(peer))

    def _spawn(self, coro) -> None:
        """Fire-and-forget, with a strong reference so it is not collected."""
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _heartbeat_and_record(self, peer) -> None:
        try:
            if await self._heartbeat_one(peer):
                self.cluster.mark_alive(peer.node_id)
        finally:
            self._heartbeats_inflight.discard(peer.node_id)

    def _isr_report(self) -> list:
        """
        Who is keeping up, for every partition THIS broker leads.

        Only the partitions we lead: an ISR is an observation of our own
        followers' fetches, and we have no standing to describe a partition we
        do not serve. The receiver enforces the same rule, because a report is
        only as trustworthy as the check on it.
        """
        report = []
        for topic, partition in self.cluster.my_partitions():
            report.append({
                'topic': topic,
                'partition': partition,
                'isr': self.cluster.in_sync_replicas(
                    topic, partition, self.replica_lag_time_max_ms),
            })
        return report

    async def _heartbeat_one(self, peer) -> bool:
        """One heartbeat. Returns True if the peer answered."""
        if getattr(self, 'partitioned_from', None) and \
                peer.node_id in self.partitioned_from:
            # Test hook: simulate a network partition without touching the OS.
            # Used by chaos/split_brain.py, which needs one side of the cluster
            # to be unreachable from the other while both keep running.
            return False

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.host, peer.port), timeout=2.0)
        except (OSError, asyncio.TimeoutError):
            # A refused connection is stronger evidence than silence, so drop
            # the peer now rather than waiting out its session timeout.
            self.cluster.mark_down(peer.node_id)
            return False

        try:
            writer.write(encode_message(
                API_BROKER_HEARTBEAT,
                {'node_id': self.node_id,
                 'controller_id': self.cluster.controller_id,
                 # Piggy-backed on a message that was already being sent at
                 # exactly the right cadence. A separate ISR-report API would
                 # be a second timer to tune and a second thing to go quiet
                 # independently of liveness — and "this broker is alive" and
                 # "here is what its followers are doing" are answers to
                 # nearly the same question.
                 'isr_report': self._isr_report()},
                correlation_id=1))
            await writer.drain()

            header = decode_header(await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE), timeout=2.0))
            await reader.readexactly(header.body_length)
            return True
        except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            return False
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _push_controller_update(self) -> None:
        """Broadcast the new leader map to every peer."""
        payload = {
            'controller_id': self.node_id,
            'metadata_version': self.cluster.metadata_version,
            'partitions': self.cluster.leader_entries(),
            # The topic list travels WITH the leader map. A peer that does not
            # know a topic exists — or thinks it is narrower than it is —
            # cannot use the map, and would recompute a wrong one if it later
            # became controller.
            'topics': {name: self.metadata.get(name).partitions
                       for name in self.metadata.list_topics()
                       if self.metadata.get(name) is not None},
        }
        await asyncio.gather(
            *(self._push_one(peer, payload)
              for peer in self.cluster_config.peers(self.node_id)),
            return_exceptions=True)

    async def _push_one(self, peer, payload: dict) -> None:
        if getattr(self, 'partitioned_from', None) and \
                peer.node_id in self.partitioned_from:
            return

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.host, peer.port), timeout=2.0)
        except (OSError, asyncio.TimeoutError):
            # A peer that missed this push stays stale until the next change.
            # Real Kafka retries until acknowledged; we accept the gap and let
            # NOT_LEADER correct any client that reaches the wrong broker.
            return
        try:
            writer.write(encode_message(API_CONTROLLER_UPDATE, payload,
                                        correlation_id=1))
            await writer.drain()
            header = decode_header(await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE), timeout=2.0))
            await reader.readexactly(header.body_length)
        except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _group_reap_loop(self) -> None:
        """
        Periodically evict group members that stopped heartbeating.

        CONCEPT: why a timer is required, not just lazy checks.
          Eviction otherwise happens only when SOMEBODY calls in — and if every
          member of a group dies at once, nobody ever calls in again. The group
          would sit forever in `Stable`, holding partitions on behalf of
          processes that no longer exist, and `kfk groups describe` would show
          a healthy group that is consuming nothing.

        Guarded like the retention sweep: a bug here must not take down a
        broker that is otherwise serving reads and writes perfectly well.
        """
        interval = self.group_reap_ms / 1000.0
        while True:
            try:
                await asyncio.sleep(interval)
                evicted = await self.coordinator.reap_expired()
                for group_id, members in evicted.items():
                    log.info("Group %s: evicted %d member(s) that stopped "
                             "heartbeating; rebalance triggered",
                             group_id, len(members))
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("Group reap failed; will retry next interval")

    async def _retention_loop(self) -> None:
        """
        Periodically delete segments that have aged out or pushed the log over
        its size budget.

        CONCEPT: retention is a background job, not part of the write path.
          Checking on every append would add a cost to the hot path for a
          decision that changes on the order of days. Running it on a timer
          keeps writes fast and makes deletion coarse — a record can outlive
          `retention_ms` by up to one check interval plus however long its
          segment took to fill. Real Kafka behaves the same way, and it
          surprises people expecting per-record expiry.

        The whole loop is guarded: a bug in the cleaner must never take down a
        broker that is otherwise happily serving reads and writes. Deleting old
        data is the least urgent thing this process does.
        """
        interval = self.retention_check_ms / 1000.0
        while True:
            try:
                await asyncio.sleep(interval)
                with self._registry_lock:
                    partitions = list(self.partitions.values())

                for partition in partitions:
                    deleted = await self._run_blocking(partition.delete_old_segments)
                    if deleted:
                        log.info("Retention: deleted %d segment(s) from %s-%s, "
                                 "log now starts at offset %s",
                                 len(deleted), partition.topic,
                                 partition.partition_id, partition.log_start_offset)
            except asyncio.CancelledError:
                return                  # normal shutdown
            except Exception:
                log.exception("Retention sweep failed; will retry next interval")

    def stop(self) -> None:
        """
        Ask the broker to shut down. Safe to call from any thread.

        `call_soon_threadsafe` is the ONLY sanctioned way to touch an asyncio
        loop from outside its thread. Setting the event directly is a race:
        the loop may be blocked in select() and never notice.
        """
        if self._loop is not None and self._shutdown is not None:
            self._loop.call_soon_threadsafe(self._shutdown.set)

    def _close_partitions(self) -> None:
        with self._registry_lock:
            for partition in self.partitions.values():
                try:
                    partition.close()
                except OSError as exc:
                    log.warning("Error closing %r: %s", partition, exc)


# ── Response helpers ───────────────────────────────────────────

def _append_records(partition, records: list) -> int:
    """
    Append a batch and return the offset of its FIRST record.

    Runs on an executor thread. Kept as a module function rather than a lambda
    so `run_in_executor` gets something picklable-shaped and readable in a
    traceback.

    The records are appended one at a time under the partition's lock, so their
    offsets are contiguous — which is what lets the client resolve record i as
    `base_offset + i` from a single returned integer.
    """
    base_offset = -1
    for record in records:
        offset = partition.append(record.get('value', ''), record.get('key'))
        if base_offset < 0:
            base_offset = offset
    return base_offset


def _error(error_code: int, message: str) -> dict:
    """
    Build an error body.

    Both fields matter and they serve different readers: `error_code` is for
    the client's code (retry or not), `error_message` is for the human reading
    the log afterwards. Sending only the code makes debugging miserable;
    sending only the message is Phase 0's mistake.
    """
    return {'error_code': error_code, 'error_message': message}


async def _reply(writer: asyncio.StreamWriter, header, body: dict) -> None:
    """
    Send a response echoing the request's api_key and correlation_id.

    The echo is what lets a client match replies to requests and therefore
    pipeline them. Getting it wrong is invisible under sequential load and
    catastrophic under concurrent load, which is why there is a test pinning it.
    """
    writer.write(encode_message(header.api_key, body,
                                header.correlation_id, header.api_version))
    # drain() applies BACKPRESSURE: if this client is not reading fast enough
    # it pauses us, instead of letting the send buffer grow without bound.
    # Skip it and one slow consumer can drive the broker out of memory.
    await writer.drain()


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone broker  (run with: python -m broker.broker)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--node-id', type=int, default=0,
                        help='This broker\'s id. Must match an entry in --cluster.')
    parser.add_argument('--cluster', default=None,
                        help='Path to cluster.json. Omit to run standalone.')
    parser.add_argument('--quiet', action='store_true',
                        help='Only log warnings — use when benchmarking, since '
                             'a log line per message costs more than the write.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    cluster_config = None
    if args.cluster:
        try:
            cluster_config = ClusterConfig.from_file(args.cluster)
        except ClusterConfigError as exc:
            raise SystemExit(f"cluster config: {exc}")
        if args.node_id not in cluster_config:
            # Starting with an id the roster does not contain would produce a
            # broker nobody heartbeats and nobody assigns partitions to — it
            # would run happily and do nothing. Refusing is far kinder than a
            # cluster that is quietly one broker short.
            raise SystemExit(
                f"--node-id {args.node_id} is not in {args.cluster} "
                f"(it has {cluster_config.node_ids()})")

    broker = Broker(host=args.host, port=args.port,
                    data_dir=args.data_dir, node_id=args.node_id,
                    cluster_config=cluster_config)
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        # Ctrl-C unwinds out of asyncio.run before start()'s cleanup runs, so
        # close the partition handles here too. Losing buffered writes on a
        # deliberate shutdown would be an embarrassing way to lose data.
        broker._close_partitions()
        log.info("Broker stopped (interrupted).")


if __name__ == '__main__':
    main()
