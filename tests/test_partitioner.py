"""
Tests for partition routing — the load-balancing decision.

The properties here are the ones an application actually relies on: the same
key always lands in the same partition (so per-key ordering exists at all), and
keyless messages spread evenly (so partitions are worth having).
"""

import statistics
from collections import Counter

import pytest

from producer.partitioner import (
    DefaultPartitioner, RoundRobinPartitioner, StickyPartitioner,
    murmur2, partition_for_key,
)


# ── murmur2 ────────────────────────────────────────────────────

def test_murmur2_returns_a_signed_32_bit_int():
    """
    Kafka's murmur2 returns a Java `int`, which is signed and wraps on
    overflow. Python integers do neither, so the implementation emulates it —
    and if that emulation were dropped the hash would still look fine (it
    would be deterministic and spread well) while no longer matching Kafka.
    """
    values = [murmur2(f"key-{i}".encode()) for i in range(500)]

    assert all(-2 ** 31 <= v < 2 ** 31 for v in values)
    assert any(v < 0 for v in values), "signed means negatives must occur"


def test_murmur2_is_deterministic():
    """
    THE property that Python's built-in `hash()` does not have. `hash()` on a
    str is randomised per process by PYTHONHASHSEED, so two producer processes
    would route the same key to different partitions — silently destroying
    per-key ordering, while passing every single-process test.
    """
    assert murmur2(b'cust-42') == murmur2(b'cust-42')
    assert murmur2(b'') == murmur2(b'')
    assert murmur2(b'a') != murmur2(b'b')


@pytest.mark.parametrize('length', [0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 100])
def test_murmur2_handles_every_tail_length(length):
    """
    murmur2 consumes four bytes at a time and then handles 0-3 trailing bytes
    with a deliberate switch fall-through. Each remainder is a separate branch,
    so each needs exercising — an error in one would only show for inputs of
    that length mod 4.
    """
    value = murmur2(b'x' * length)
    assert -2 ** 31 <= value < 2 ** 31


# ── Key routing ────────────────────────────────────────────────

def test_the_same_key_always_maps_to_the_same_partition():
    """
    The guarantee everything else rests on. Without it, "all events for a
    customer are ordered" is not true, and keys are pointless.
    """
    for num_partitions in (1, 3, 4, 12, 64):
        target = partition_for_key('cust-42', num_partitions)
        assert all(partition_for_key('cust-42', num_partitions) == target
                   for _ in range(50))
        assert 0 <= target < num_partitions


def test_partition_is_always_in_range_even_for_negative_hashes():
    """
    murmur2 returns signed values, and about half are negative. The `&
    0x7fffffff` mask is what keeps the modulo in range — and, equally
    importantly, what makes Python agree with Java, where `-7 % 3` is `-1`
    rather than `2`.
    """
    for i in range(2000):
        assert 0 <= partition_for_key(f"key-{i}", 7) < 7


def test_keys_spread_evenly_across_partitions():
    """
    An even spread is what makes partitions worth having: a hash that piled
    80% of keys into one partition would give you the ordering cost of keys
    with none of the parallelism benefit.

    The tolerance is derived from expected variance, not guessed. With n keys
    over p partitions the count per partition is binomial with
    sd = sqrt(n * (1/p) * (1 - 1/p)); 4 standard deviations is a bound a
    healthy hash will not cross by chance, but a genuinely skewed one will.
    """
    for num_partitions in (3, 4, 12):
        n = 24000
        counts = Counter(partition_for_key(f"cust-{i}", num_partitions)
                         for i in range(n))

        assert set(counts) == set(range(num_partitions)), "every partition used"

        expected = n / num_partitions
        sd = (n * (1 / num_partitions) * (1 - 1 / num_partitions)) ** 0.5
        worst = max(abs(c - expected) for c in counts.values())
        assert worst < 4 * sd, (
            f"{num_partitions} partitions: worst deviation {worst:.0f} "
            f"exceeds 4 sd ({4 * sd:.0f})")


def test_changing_the_partition_count_remaps_most_keys():
    """
    The sharpest consequence of key routing, and the reason partition counts
    are painful to change: `hash(key) % n` depends on n.

    Going from 3 to 4 partitions moves roughly three keys in four. Every moved
    key now has its history in one log and its future in another, and nothing
    reads them in order again. This is why Kafka forbids DECREASING partitions
    and why increasing them is a decision, not a tuning knob.
    """
    keys = [f"cust-{i}" for i in range(4000)]
    moved = sum(1 for k in keys
                if partition_for_key(k, 3) != partition_for_key(k, 4))

    fraction = moved / len(keys)
    assert 0.6 < fraction < 0.85, (
        f"expected roughly 3 in 4 keys to move, got {fraction:.0%}")


# ── Keyless routing ────────────────────────────────────────────

def test_sticky_partitioner_stays_put_until_told_otherwise():
    """
    The whole point of stickiness: consecutive keyless messages go to ONE
    partition so its batch fills quickly. Round-robin would spread them across
    every partition and make each batch fill n times more slowly — sending n
    small requests where one large one would do.
    """
    sticky = StickyPartitioner(num_partitions=4)

    first = sticky.next_partition()
    assert all(sticky.next_partition() == first for _ in range(100))

    second = sticky.on_batch_full()
    assert second != first
    assert all(sticky.next_partition() == second for _ in range(100))


def test_sticky_partitioner_visits_every_partition_in_turn():
    """
    Balance over time. Advancing round-robin on each batch — rather than
    choosing randomly — guarantees that any window of n batches covers all n
    partitions exactly once.
    """
    sticky = StickyPartitioner(num_partitions=5)
    visited = {sticky.next_partition()}
    for _ in range(4):
        visited.add(sticky.on_batch_full())
    assert visited == set(range(5))


def test_round_robin_moves_every_message():
    rr = RoundRobinPartitioner(num_partitions=3)
    assert [rr.next_partition() for _ in range(6)] == [0, 1, 2, 0, 1, 2]


def test_single_partition_topics_are_handled():
    """An off-by-one in the sticky advance would divide by zero or loop here."""
    sticky = StickyPartitioner(num_partitions=1)
    assert sticky.next_partition() == 0
    assert sticky.on_batch_full() == 0
    assert partition_for_key('anything', 1) == 0


# ── DefaultPartitioner ─────────────────────────────────────────

def test_explicit_partition_overrides_everything():
    partitioner = DefaultPartitioner()
    assert partitioner.partition('t', 'some-key', 8, explicit=5) == 5
    assert partitioner.partition('t', None, 8, explicit=0) == 0


def test_keyed_and_keyless_take_different_paths():
    partitioner = DefaultPartitioner()

    keyed = [partitioner.partition('t', 'cust-1', 4) for _ in range(20)]
    assert len(set(keyed)) == 1, "keyed messages pin to one partition"

    keyless = [partitioner.partition('t', None, 4) for _ in range(20)]
    assert len(set(keyless)) == 1, "sticky keeps keyless messages together too"

    partitioner.on_batch_full('t', 4)
    assert partitioner.partition('t', None, 4) != keyless[0]


def test_sticky_state_is_per_topic():
    """
    A sticky cursor for a 4-partition topic is meaningless for a 12-partition
    one. Sharing it would route messages to partitions that do not exist.
    """
    partitioner = DefaultPartitioner()
    partitioner.partition('narrow', None, 2)
    wide = partitioner.partition('wide', None, 16)
    assert 0 <= wide < 16


def test_partition_count_change_rebuilds_sticky_state():
    """A stale cursor could point past the end after a topic is re-created."""
    partitioner = DefaultPartitioner()
    partitioner.partition('t', None, 16)
    assert 0 <= partitioner.partition('t', None, 2) < 2
