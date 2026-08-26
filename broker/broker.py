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

from broker.coordinator import GroupCoordinator
from broker.log import LogConfig
from broker.metadata import InvalidTopicError, MetadataStore
from broker.offset_store import OffsetStore
from broker.partition import Partition
from common import errors
from common.config import (
    DEFAULT_DATA_DIR, DEFAULT_GROUP_REAP_MS, DEFAULT_HOST, DEFAULT_PORT,
    DEFAULT_RETENTION_CHECK_MS,
)
from common.protocol import (
    API_CREATE_TOPICS, API_DESCRIBE_GROUPS, API_FETCH, API_FIND_COORDINATOR,
    API_HEARTBEAT, API_JOIN_GROUP, API_LEAVE_GROUP, API_LIST_GROUPS,
    API_LIST_OFFSETS, API_METADATA, API_OFFSET_COMMIT, API_OFFSET_FETCH,
    API_PRODUCE, API_SYNC_GROUP,
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
                 group_reap_ms: float = DEFAULT_GROUP_REAP_MS):
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

        try:
            config = self.metadata.create_topic(name, partitions, replication_factor)
        except InvalidTopicError as exc:
            return _error(errors.INVALID_REQUEST, str(exc))

        log.info("Created topic %s with %d partition(s)", name, config.partitions)
        return {
            'error_code': errors.NONE,
            'topic': name,
            'partitions': config.partitions,
            'replication_factor': config.replication_factor,
        }

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

            if not records:
                results.append({'partition': part_id, 'error_code': errors.NONE,
                                'base_offset': -1, 'count': 0})
                continue

            partition = self._get_partition(topic, part_id)
            base_offset = await self._run_blocking(
                _append_records, partition, records)

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

        partition = self._get_partition(topic, part_id)
        offset = await self._run_blocking(partition.append, value, key)

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

        messages = await self._run_blocking(partition.fetch, start_offset, max_msgs)

        log.info("FETCH  ← %s-%s from=%s returned=%s",
                 topic, part_id, start_offset, len(messages))

        # `high_watermark` is one past the last message. A consumer subtracts
        # its position from it to get LAG — the most-watched number in any
        # Kafka deployment, because unbounded lag means retention will delete
        # data nobody has read. In Phase 6 this stops meaning "everything
        # written" and starts meaning "everything REPLICATED", which is a much
        # more interesting number.
        return {
            'error_code': errors.NONE,
            'topic': topic,
            'partition': part_id,
            'messages': messages,
            'count': len(messages),
            'high_watermark': partition.next_offset,
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
                partition_entries.append({
                    'partition': part_id,
                    # With one broker, every partition is led by us and has
                    # exactly one replica, which is trivially in-sync. Phase 5
                    # assigns real leaders; Phase 6 real replicas and a real
                    # ISR. The SHAPE is right now, so clients written against
                    # it keep working.
                    'leader': self.node_id,
                    'replicas': [self.node_id],
                    'isr': [self.node_id],
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
            'brokers': [{'node_id': self.node_id,
                         'host': self.host, 'port': self.port}],
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

        log.info("Broker %s listening on %s:%s", self.node_id, self.host, self.port)
        log.info("Data directory: %s", os.path.abspath(self.data_dir))
        self.ready.set()

        cleaner = asyncio.create_task(self._retention_loop())
        reaper = asyncio.create_task(self._group_reap_loop())

        async with server:
            await self._shutdown.wait()

        cleaner.cancel()
        reaper.cancel()
        self.offsets.close()
        self._close_partitions()
        log.info("Broker stopped.")

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
                        help='Broker id. Only meaningful from Phase 5 onward.')
    parser.add_argument('--quiet', action='store_true',
                        help='Only log warnings — use when benchmarking, since '
                             'a log line per message costs more than the write.')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    broker = Broker(host=args.host, port=args.port,
                    data_dir=args.data_dir, node_id=args.node_id)
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
