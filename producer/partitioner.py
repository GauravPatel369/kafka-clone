"""
CONCEPT: Choosing a Partition — This Is the Load Balancer
------------------------------------------------------------
There is no load balancer in front of a Kafka cluster. The producer decides
where each message goes, and that decision is the entire load-balancing
mechanism of the write path.

Two cases, and they are opposites:

    key is None      spread messages evenly           → maximum throughput
    key is set       always the same partition        → ordering for that key

Ordering is guaranteed within a partition and nowhere else. So the key is how
YOU choose what must be ordered:

    key = customer_id   every event for a customer is strictly ordered;
                        different customers interleave, and you don't care.

    key = None          perfect balance, no ordering guarantees at all.

    key = "constant"    everything in one partition: total ordering, and the
                        throughput of a single machine forever. This is the
                        classic accidental bottleneck.

> **Scalability and ordering trade against each other, and the partition key is
> the dial.**


WHY MURMUR2 AND NOT PYTHON'S hash()
-------------------------------------
`hash("cust-42")` looks like the obvious choice. It is actively wrong, and the
way it fails is worth remembering.

**Python randomises `hash()` for str on every process start** (PYTHONHASHSEED),
as a defence against hash-collision denial-of-service attacks. So:

    producer process A:  hash("cust-42") % 4  →  1
    producer process B:  hash("cust-42") % 4  →  3

The same key goes to different partitions depending on which process sent it.
Per-key ordering is silently destroyed.

And notice how it fails: **every test you would naturally write still passes**,
because a single test process has a single seed. It works on your machine, it
works in CI, and it breaks in production the moment you run two producers.

murmur2 is deterministic across processes, machines, and languages — so a
Python producer and a Java producer put the same key in the same partition.
That cross-language agreement is not incidental; it is why Kafka specifies the
hash rather than leaving it to each client.

md5 or sha256 would also be deterministic, but they are ~20x slower on a hot
path, to defend against an attacker who is not here. **Match the mechanism to
the actual requirement:** we need determinism and spread, not cryptography.
"""

import itertools
import random
import threading


# ── murmur2 ────────────────────────────────────────────────────

_M = 0x5bd1e995
_R = 24
_SEED = 0x9747b28c
_MASK32 = 0xFFFFFFFF


def _i32(value: int) -> int:
    """
    Force a Python int into signed 32-bit, the way Java's `int` behaves.

    Python integers are arbitrary precision; Java's are 32 bits and wrap on
    overflow. murmur2 depends on that wrapping, so reproducing Kafka's exact
    output means reproducing the overflow — mask to 32 bits, then interpret the
    top bit as a sign.

    Skip this and the hash still "works" (it is deterministic and spreads
    fine), but it no longer matches Kafka. A Java producer and this one would
    disagree about where a key belongs, which is the one property we chose
    murmur2 to get.
    """
    value &= _MASK32
    return value - 0x100000000 if value & 0x80000000 else value


def murmur2(data: bytes) -> int:
    """
    Kafka's murmur2, byte-for-byte compatible with its Java implementation.

    Returns a signed 32-bit integer, exactly as Java does.
    """
    length = len(data)
    h = _i32(_SEED ^ length)

    # Four bytes at a time, little-endian.
    for i in range(length // 4):
        i4 = i * 4
        k = (data[i4] & 0xff) | ((data[i4 + 1] & 0xff) << 8) \
            | ((data[i4 + 2] & 0xff) << 16) | ((data[i4 + 3] & 0xff) << 24)
        k = _i32(k * _M)
        k ^= (k & _MASK32) >> _R       # Java's >>> : logical, not arithmetic
        k = _i32(k * _M)
        h = _i32(h * _M)
        h = _i32(h ^ k)

    # The trailing 1-3 bytes. The fall-through in Java's switch is deliberate:
    # each case adds its byte and continues into the next.
    tail = length & ~3
    remainder = length % 4
    if remainder >= 3:
        h = _i32(h ^ ((data[tail + 2] & 0xff) << 16))
    if remainder >= 2:
        h = _i32(h ^ ((data[tail + 1] & 0xff) << 8))
    if remainder >= 1:
        h = _i32(h ^ (data[tail] & 0xff))
        h = _i32(h * _M)

    h = _i32(h ^ ((h & _MASK32) >> 13))
    h = _i32(h * _M)
    h = _i32(h ^ ((h & _MASK32) >> 15))
    return h


def partition_for_key(key, num_partitions: int) -> int:
    """
    Map a key to a partition, the way Kafka's default partitioner does.

    The `& 0x7fffffff` masks off the sign bit, and it is not cosmetic:
    murmur2 returns a SIGNED integer, and `-7 % 3` is `2` in Python but `-1` in
    Java. Masking first makes the value non-negative in both languages, so the
    two agree. Using Python's naturally-positive modulo instead would quietly
    put keys in different partitions than a Java producer does — the exact
    incompatibility murmur2 was chosen to avoid.
    """
    if isinstance(key, str):
        key = key.encode('utf-8')
    return (murmur2(key) & 0x7fffffff) % num_partitions


# ── Partitioners ───────────────────────────────────────────────

class StickyPartitioner:
    """
    For keyless messages: fill ONE partition's batch, then switch.

    CONCEPT: why not round-robin?
      Round-robin per message gives perfect balance and ruins batching. With 10
      partitions, consecutive messages go to 10 different buffers, so each one
      fills 10x more slowly — you end up sending ten small requests where one
      large one would do, and batching was the whole point.

      Sticky sends a run of messages to one partition until its batch is ready,
      then picks another. Balance over time is the same; batch sizes are ~10x
      better.

      This is Kafka's KIP-480, added in 2.4 — a change made specifically
      because the "obviously fair" policy was defeating a feature added later.
      Two features interacting is exactly the kind of thing that is invisible
      until you measure it.

    Thread-safe: the sender thread and caller threads both touch this.
    """

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
        self._lock = threading.Lock()
        self._current = random.randrange(num_partitions) if num_partitions > 1 else 0

    def next_partition(self) -> int:
        with self._lock:
            return self._current

    def on_batch_full(self) -> int:
        """
        Called when the current partition's batch is sent. Picks a new one.

        Advances round-robin rather than choosing randomly, so that over any
        window of `num_partitions` batches every partition gets exactly one —
        random choice would leave visible imbalance for a long time.
        """
        with self._lock:
            if self.num_partitions > 1:
                self._current = (self._current + 1) % self.num_partitions
            return self._current


class RoundRobinPartitioner:
    """
    One message per partition, in turn.

    Kept because it is the honest baseline for the sticky comparison, and
    because it is genuinely what you want when messages are large enough that
    each is its own batch anyway.
    """

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
        self._counter = itertools.count()
        self._lock = threading.Lock()

    def next_partition(self) -> int:
        with self._lock:
            return next(self._counter) % self.num_partitions

    def on_batch_full(self) -> int:
        return self.next_partition()


class DefaultPartitioner:
    """
    Kafka's default routing policy:

        explicit partition given  →  use it
        key is not None           →  murmur2(key) % num_partitions
        otherwise                 →  sticky

    Holds one sticky state per topic, because the partition counts differ and
    a sticky cursor for a 4-partition topic is meaningless for a 12-partition
    one.
    """

    def __init__(self, sticky: bool = True):
        self.sticky = sticky
        self._per_topic = {}
        self._lock = threading.Lock()

    def _keyless(self, topic: str, num_partitions: int):
        with self._lock:
            state = self._per_topic.get(topic)
            if state is None or state.num_partitions != num_partitions:
                cls = StickyPartitioner if self.sticky else RoundRobinPartitioner
                state = cls(num_partitions)
                self._per_topic[topic] = state
            return state

    def partition(self, topic: str, key, num_partitions: int,
                  explicit: int = None) -> int:
        if explicit is not None:
            return explicit
        if key is not None:
            return partition_for_key(key, num_partitions)
        return self._keyless(topic, num_partitions).next_partition()

    def on_batch_full(self, topic: str, num_partitions: int) -> None:
        """Tell the sticky state to move on. No-op for keyed messages."""
        self._keyless(topic, num_partitions).on_batch_full()
