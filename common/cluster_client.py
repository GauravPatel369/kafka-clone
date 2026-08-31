"""
CONCEPT: The Client Side of a Cluster
---------------------------------------
Once partitions live on different brokers, every client needs three things it
did not need before:

    a connection per broker      one socket each, kept open
    a metadata cache             (topic, partition) → which broker leads it
    refresh-and-retry            because that cache goes stale

This class holds all three, so producer and consumer share one implementation
of routing and failover instead of two that drift apart.


WHY THE CLIENT ROUTES, AND NOT A PROXY
----------------------------------------
    Producer ──── orders-0 ───────────► Broker 1
             ──── orders-1 ───────────► Broker 2

There is no load balancer in front of a Kafka cluster. The client holds the map
and connects to the leader **directly** — no extra network hop on the data
path, which is worth a great deal when the data path is the entire product.

The price is that clients are "thick": every one needs routing logic, a cache,
and the discipline to handle that cache being wrong. This file is that price.


THE CACHE IS WRONG EXACTLY WHEN IT MATTERS
--------------------------------------------
A cached leader is correct right up until the moment leadership moves — which
is precisely when something has gone wrong and you most need to reach the right
broker.

Polling for freshness would not fix it (you would still have a window) and
would cost a metadata request per interval per client. So Kafka does the
opposite: **assume the cache is right, and let the broker tell you when it is
not.**

    1. send to the cached leader
    2. broker replies NOT_LEADER_OR_FOLLOWER
    3. refresh metadata, retry

The error code is the invalidation signal. It has existed since Phase 1,
unraised, waiting for this.
"""

import threading
import time

from common import errors
from common.net import BrokerConnectionError, SyncConnection
from common.protocol import API_METADATA

# How long a cached map is trusted without a NOT_LEADER prompt.
#
# This is a backstop, not the mechanism. NOT_LEADER_OR_FOLLOWER is what
# actually keeps the cache honest; the TTL only covers changes that happen
# while a client is idle, so it can be generous.
DEFAULT_METADATA_TTL_MS = 30_000


class ClusterClient:
    """
    Connections, metadata cache, and leader routing for one client.

    NOT thread-safe for concurrent requests on the same broker connection —
    `SyncConnection` is not, and this does not add a lock. Callers that share
    one across threads must serialise themselves; `Producer` holds a lock and
    `Consumer` gives its heartbeat thread a separate client, for the reasons
    those files explain.
    """

    def __init__(self, bootstrap: list, timeout: float = 10.0,
                 metadata_ttl_ms: float = DEFAULT_METADATA_TTL_MS):
        # `bootstrap` is [(host, port), ...] — any subset of the cluster. We
        # only need to reach ONE broker to learn about all of them, which is
        # why it is a list: if the first is down, try the next.
        self.bootstrap = list(bootstrap)
        self.timeout = timeout
        self.metadata_ttl_ms = metadata_ttl_ms

        self._connections = {}          # node_id -> SyncConnection
        self._bootstrap_conn = None     # used before we know any node_id

        self._brokers = {}              # node_id -> (host, port)
        self._leaders = {}              # (topic, partition) -> node_id
        self._partition_counts = {}     # topic -> count
        self._fetched_at = 0.0

        self._lock = threading.RLock()

    # ── Lifecycle ──────────────────────────────────────────────

    def close(self) -> None:
        with self._lock:
            for conn in self._connections.values():
                conn.close()
            self._connections.clear()
            if self._bootstrap_conn is not None:
                self._bootstrap_conn.close()
                self._bootstrap_conn = None

    def __enter__(self) -> 'ClusterClient':
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Connections ────────────────────────────────────────────

    def connection_for(self, node_id: int) -> SyncConnection:
        """
        An open connection to one broker, created on first use and reused.

        Reused because a TCP handshake is a full round trip before any payload
        moves — reconnecting per request would spend more time saying hello
        than sending data.
        """
        with self._lock:
            conn = self._connections.get(node_id)
            if conn is not None and conn.connected:
                return conn

            address = self._brokers.get(node_id)
            if address is None:
                raise errors.LeaderNotAvailableError(
                    f"no address known for broker {node_id}; metadata may be stale")

            conn = SyncConnection(address[0], address[1], timeout=self.timeout)
            conn.connect()
            self._connections[node_id] = conn
            return conn

    def _any_connection(self) -> SyncConnection:
        """
        Any broker that answers — for requests that are not partition-specific.

        Tries known brokers first, then the bootstrap list. A cluster is only
        unreachable if *every* address fails, which is the distinction between
        "one broker is down" and "there is no cluster".
        """
        with self._lock:
            for node_id in sorted(self._connections):
                if self._connections[node_id].connected:
                    return self._connections[node_id]

            for node_id in sorted(self._brokers):
                try:
                    return self.connection_for(node_id)
                except (BrokerConnectionError, errors.KafkaError):
                    continue

            if self._bootstrap_conn is not None and self._bootstrap_conn.connected:
                return self._bootstrap_conn

            last_error = None
            for host, port in self.bootstrap:
                try:
                    conn = SyncConnection(host, port, timeout=self.timeout)
                    conn.connect()
                    self._bootstrap_conn = conn
                    return conn
                except BrokerConnectionError as exc:
                    last_error = exc

            raise BrokerConnectionError(
                f"no broker reachable from bootstrap {self.bootstrap}: {last_error}")

    # ── Metadata ───────────────────────────────────────────────

    def refresh(self, topics: list = None, allow_auto_create: bool = False) -> dict:
        """Fetch METADATA from any reachable broker and replace the cache."""
        payload = {}
        if topics:
            payload['topics'] = list(topics)
            payload['allow_auto_topic_creation'] = allow_auto_create

        response = errors.raise_for_error(
            self._any_connection().request(API_METADATA, payload))

        with self._lock:
            for entry in response.get('brokers', []):
                self._brokers[int(entry['node_id'])] = (entry['host'],
                                                        int(entry['port']))
            for topic in response.get('topics', []):
                if topic.get('error_code', 0) != 0:
                    continue
                name = topic['name']
                partitions = topic['partitions']
                self._partition_counts[name] = len(partitions)
                for part in partitions:
                    leader = part.get('leader')
                    if leader is not None and leader >= 0:
                        self._leaders[(name, int(part['partition']))] = int(leader)
            self._fetched_at = time.monotonic()

        return response

    def invalidate(self) -> None:
        """
        Mark the cache stale so the next lookup refetches.

        We clear only the timestamp, not the map. The stale entries stay usable
        as a fallback if a refresh fails — a slightly wrong leader is a better
        starting point than none, because NOT_LEADER will correct it anyway.
        """
        with self._lock:
            self._fetched_at = 0.0

    def _ensure_fresh(self, topic: str = None, allow_auto_create: bool = False) -> None:
        with self._lock:
            age_ms = (time.monotonic() - self._fetched_at) * 1000
            fresh = self._fetched_at > 0 and age_ms < self.metadata_ttl_ms
            known = topic is None or topic in self._partition_counts
        if not fresh or not known:
            self.refresh([topic] if topic else None, allow_auto_create)

    def leader_for(self, topic: str, partition: int) -> int:
        """Which broker leads this partition. Refreshes if we do not know."""
        self._ensure_fresh(topic)
        with self._lock:
            leader = self._leaders.get((topic, partition))
        if leader is None:
            self.invalidate()
            self._ensure_fresh(topic)
            with self._lock:
                leader = self._leaders.get((topic, partition))
        if leader is None:
            raise errors.LeaderNotAvailableError(
                f"no leader known for {topic}-{partition}",
                topic=topic, partition=partition)
        return leader

    def partition_count(self, topic: str, allow_auto_create: bool = True) -> int:
        self._ensure_fresh(topic, allow_auto_create)
        with self._lock:
            return self._partition_counts.get(topic, 1)

    def brokers(self) -> dict:
        self._ensure_fresh()
        with self._lock:
            return dict(self._brokers)

    # ── The routing primitive ──────────────────────────────────

    def request_for_partition(self, topic: str, partition: int, api_key: int,
                              payload: dict, api_version: int = 0) -> dict:
        """
        Send a request to whichever broker leads this partition.

        On NOT_LEADER_OR_FOLLOWER — or a dead connection — refresh the cache
        once and retry. **This is client-side failover, complete.**

        Two attempts, not a loop. One refresh is enough for a leader that has
        moved; an unbounded loop would spin against a genuinely broken cluster,
        and the caller already has backoff via `RetriableError` at a higher
        level. Bounding retries where the information does not change between
        attempts is the difference between resilience and a busy-wait.
        """
        last_error = None

        for attempt in range(2):
            try:
                node_id = self.leader_for(topic, partition)
                response = self.connection_for(node_id).request(
                    api_key, payload, api_version)
            except (BrokerConnectionError, errors.LeaderNotAvailableError) as exc:
                # The broker we believed in is unreachable. That is evidence
                # our map is stale, not just that one request failed.
                last_error = exc
                self._drop_connection_for(topic, partition)
                self.invalidate()
                continue

            if response.get('error_code') == errors.NOT_LEADER_OR_FOLLOWER:
                # The broker is up and told us we asked the wrong one — the
                # cleanest possible invalidation signal.
                last_error = errors.NotLeaderError(
                    response.get('error_message', ''), topic=topic,
                    partition=partition)
                self.invalidate()
                continue

            return response

        raise last_error or errors.LeaderNotAvailableError(
            f"could not reach a leader for {topic}-{partition}",
            topic=topic, partition=partition)

    def request_any(self, api_key: int, payload: dict, api_version: int = 0) -> dict:
        """
        For requests with no partition: METADATA, CREATE_TOPICS, group APIs.

        Any broker will do. Group APIs work because each broker coordinates the
        groups that talk to it — a simplification noted in the phase doc, where
        real Kafka hashes the group id to pick one coordinator per group.
        """
        return self._any_connection().request(api_key, payload, api_version)

    def _drop_connection_for(self, topic: str, partition: int) -> None:
        with self._lock:
            node_id = self._leaders.get((topic, partition))
            conn = self._connections.pop(node_id, None)
        if conn is not None:
            conn.close()

    def __repr__(self) -> str:
        return (f"ClusterClient({len(self._brokers)} brokers, "
                f"{len(self._leaders)} partitions cached)")
