"""
Tests for the Partition facade: identity, concurrency, and delegation.

The storage format itself is tested in test_log.py. This file covers what
`Partition` actually owns — the lock, the directory layout, and the public API
the broker depends on.

That split is deliberate and worth noticing: when Phase 2 replaced the entire
storage engine, the tests that broke were exactly the ones that had reached
past this API into file internals. Tests that only used `append`/`fetch`
survived untouched. **Tests coupled to implementation detail are a cost you pay
on every refactor.**
"""

import os
import threading

import pytest

from broker.log import LogConfig
from broker.partition import Partition


@pytest.fixture
def partition(tmp_path):
    p = Partition('test-topic', 0, data_dir=str(tmp_path))
    yield p
    p.close()


# ── Offsets ────────────────────────────────────────────────────

def test_offsets_start_at_zero_and_increment_by_one(partition):
    assert [partition.append(f"m{i}") for i in range(5)] == [0, 1, 2, 3, 4]
    assert partition.size() == 5
    assert partition.next_offset == 5


def test_appended_records_are_readable_immediately(partition):
    """
    Writes go through a buffered handle, so `append` must flush to the OS page
    cache — otherwise a reader opening its own handle sees an empty file.
    """
    partition.append('hello', key='k1')
    messages = partition.fetch(0)

    assert len(messages) == 1
    assert messages[0]['value'] == 'hello'
    assert messages[0]['key'] == 'k1'
    assert messages[0]['offset'] == 0
    assert isinstance(messages[0]['timestamp'], int)


# ── Reading ────────────────────────────────────────────────────

def test_fetch_returns_messages_from_the_requested_offset(partition):
    for i in range(10):
        partition.append(f"m{i}")
    assert [m['offset'] for m in partition.fetch(7)] == [7, 8, 9]


def test_fetch_respects_max_messages(partition):
    for i in range(100):
        partition.append(f"m{i}")

    result = partition.fetch(0, max_messages=10)
    assert len(result) == 10
    assert result[-1]['offset'] == 9


def test_fetch_past_the_end_returns_empty(partition):
    """
    A caught-up consumer asks for an offset that does not exist yet. That is
    the normal steady state of a healthy system, not an error.
    """
    partition.append('only-one')
    assert partition.fetch(99) == []


def test_fetch_on_a_partition_with_no_data_returns_empty(tmp_path):
    p = Partition('never-written', 0, data_dir=str(tmp_path))
    try:
        assert p.fetch(0) == []
        assert p.next_offset == 0
        assert p.log_start_offset == 0
    finally:
        p.close()


# ── Layout ─────────────────────────────────────────────────────

def test_a_partition_is_a_directory_of_segments(tmp_path):
    """
    Phase 0/1 wrote one flat file, `data/orders-0.log`. A partition is now a
    DIRECTORY holding segments and their indexes — which is what makes
    retention (delete a file) and fast lookup (binary search the names)
    possible at all.
    """
    p = Partition('orders', 3, data_dir=str(tmp_path))
    try:
        p.append('x')
        assert p.dir_path == os.path.join(str(tmp_path), 'orders-3')
        assert os.path.isdir(p.dir_path)

        files = sorted(os.listdir(p.dir_path))
        assert '00000000000000000000.log' in files
        assert '00000000000000000000.index' in files
    finally:
        p.close()


def test_log_start_offset_rises_with_retention(tmp_path):
    """
    Hardcoded to 0 through Phase 1 because nothing was ever deleted. Now it is
    real — and it is what finally makes OFFSET_OUT_OF_RANGE reachable, meaning
    "you fell so far behind that your data is gone".
    """
    p = Partition('aging', 0, data_dir=str(tmp_path),
                  config=LogConfig(segment_bytes=512, retention_bytes=1000,
                                   index_interval_bytes=128))
    try:
        for i in range(300):
            p.append('x' * 40)
        assert p.log_start_offset == 0

        p.delete_old_segments()

        assert p.log_start_offset > 0
        assert p.fetch(0) == [], "data below the log start is gone, not hidden"
    finally:
        p.close()


# ── Restart ────────────────────────────────────────────────────

def test_offsets_continue_after_restart(tmp_path):
    """
    The log IS the metadata — nothing else is persisted, so the next offset has
    to be derivable from the log alone. Otherwise a restart would reissue
    offsets that already exist and permanently corrupt the partition.
    """
    first = Partition('restart-topic', 0, data_dir=str(tmp_path))
    for i in range(5):
        first.append(f"before-{i}")
    first.close()

    second = Partition('restart-topic', 0, data_dir=str(tmp_path))
    try:
        assert second.next_offset == 5
        assert second.append('after-restart') == 5
        assert [m['offset'] for m in second.fetch(0)] == [0, 1, 2, 3, 4, 5]
    finally:
        second.close()


def test_restart_is_cheap_with_many_segments(tmp_path):
    """
    Only the ACTIVE segment is scanned at startup, because sealed segments are
    immutable and cannot have been mid-write. On a large partition that is the
    difference between seconds and hours.
    """
    config = LogConfig(segment_bytes=512, index_interval_bytes=128)
    first = Partition('many-segments', 0, data_dir=str(tmp_path), config=config)
    for i in range(500):
        first.append(f"m{i}" + 'x' * 30)
    segment_count = len(first.log.segments)
    first.close()

    assert segment_count > 5, "test needs several segments to be meaningful"

    second = Partition('many-segments', 0, data_dir=str(tmp_path), config=config)
    try:
        assert second.next_offset == 500
        assert len(second.log.segments) == segment_count
        assert second.corrupt_bytes_recovered == 0
    finally:
        second.close()


def test_torn_tail_is_repaired_on_restart(tmp_path):
    """
    Phase 1 could only *skip* a damaged record and leave it in the file
    forever, with the next append landing after it. Now the file is repaired
    and the damage is reported rather than hidden.
    """
    p = Partition('torn-topic', 0, data_dir=str(tmp_path))
    for i in range(3):
        p.append(f"good-{i}")
    active_path = p.log.active_segment.log_path
    p.close()

    with open(active_path, 'ab') as fh:
        fh.write(b'\x00\x00\x00\x99 half a record that never finished')

    recovered = Partition('torn-topic', 0, data_dir=str(tmp_path))
    try:
        assert recovered.corrupt_bytes_recovered > 0, "damage must be counted, not hidden"
        assert recovered.next_offset == 3
        assert len(recovered.fetch(0)) == 3
        assert recovered.append('after-repair') == 3
    finally:
        recovered.close()


# ── Concurrency ────────────────────────────────────────────────

def test_concurrent_appends_never_duplicate_an_offset(partition):
    """
    The broker runs appends in a thread POOL (`run_in_executor`), so this
    really happens.

    Without the lock, two threads read the same `next_offset`, both write it,
    and one message is silently lost while an offset goes missing. Every
    guarantee in the system — ordering, replay, deduplication, replication —
    rests on "one offset names exactly one message". This is the most important
    test in the file.
    """
    threads_count, per_thread = 8, 50
    seen = []
    seen_lock = threading.Lock()

    def writer(thread_id):
        local = [partition.append(f"t{thread_id}-m{i}") for i in range(per_thread)]
        with seen_lock:
            seen.extend(local)

    threads = [threading.Thread(target=writer, args=(t,)) for t in range(threads_count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = threads_count * per_thread
    assert len(seen) == total
    assert sorted(seen) == list(range(total)), "offsets must be unique and dense"
    assert partition.size() == total

    # And the log must contain every one of them, exactly once — read back
    # through the public API rather than by parsing files, so this test
    # survives the next storage change too.
    stored = partition.fetch(0, max_messages=total * 2)
    assert [m['offset'] for m in stored] == list(range(total))
    assert len({m['value'] for m in stored}) == total, "no message lost or duplicated"


# ── Lifecycle ──────────────────────────────────────────────────

def test_close_is_idempotent(tmp_path):
    p = Partition('closed-topic', 0, data_dir=str(tmp_path))
    p.close()
    p.close()      # must not raise


def test_verify_reports_a_clean_partition(partition):
    for i in range(50):
        partition.append(f"m{i}")

    report = partition.verify()
    assert report['records'] == 50
    assert report['damaged_segments'] == 0
