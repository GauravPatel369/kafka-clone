"""
Tests for the storage layer.

The partition is the foundation everything else stands on. If offsets are ever
wrong here, then replication, consumer groups, and replay are all wrong too —
and the symptom will show up three phases away from the cause.
"""

import json
import threading

import pytest

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


def test_appended_records_are_readable_immediately(partition):
    """
    The write handle is buffered and long-lived, so `append` must flush.
    Without the flush the data sits in Python's buffer and `fetch` — which
    opens its own read handle — sees an empty file.
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

    result = partition.fetch(start_offset=7)
    assert [m['offset'] for m in result] == [7, 8, 9]


def test_fetch_respects_max_messages(partition):
    for i in range(100):
        partition.append(f"m{i}")

    result = partition.fetch(start_offset=0, max_messages=10)
    assert len(result) == 10
    assert result[-1]['offset'] == 9


def test_fetch_past_the_end_returns_empty(partition):
    """A caught-up consumer asks for an offset that does not exist yet. That
    is the normal steady state, not an error."""
    partition.append('only-one')
    assert partition.fetch(start_offset=99) == []


def test_fetch_on_a_partition_with_no_file_returns_empty(tmp_path):
    p = Partition('never-written', 0, data_dir=str(tmp_path))
    try:
        assert p.fetch(0) == []
    finally:
        p.close()


# ── Recovery ───────────────────────────────────────────────────

def test_offsets_continue_after_restart(tmp_path):
    """
    The log IS the metadata. Nothing else is persisted, so the next offset has
    to be derivable from the log alone — otherwise a restart would reissue
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


def test_recovery_uses_the_tail_not_a_full_scan(tmp_path):
    """
    Recovery reads the last 64 KB rather than the whole file. With enough
    records to exceed that window, the fast path is what runs — and it must
    still produce the right answer.
    """
    first = Partition('big-topic', 0, data_dir=str(tmp_path))
    for i in range(2000):
        first.append(f"padding-value-{i}-{'x' * 60}")
    first.close()

    assert first.bytes_on_disk() > 64 * 1024, "test needs a log bigger than the tail window"

    second = Partition('big-topic', 0, data_dir=str(tmp_path))
    try:
        assert second.next_offset == 2000
    finally:
        second.close()


def test_recovery_falls_back_to_full_scan_when_the_tail_is_unusable(tmp_path, monkeypatch):
    """
    Shrink the tail window to a size that cannot contain a whole record. Every
    line in the window is then a fragment, so the fast path finds nothing and
    must fall back to scanning — rather than guessing 0 and reissuing offsets
    that already exist.
    """
    first = Partition('fallback-topic', 0, data_dir=str(tmp_path))
    for i in range(20):
        first.append(f"value-{i}")
    first.close()

    monkeypatch.setattr('broker.partition.TAIL_READ_BYTES', 8)

    second = Partition('fallback-topic', 0, data_dir=str(tmp_path))
    try:
        assert second.next_offset == 20
    finally:
        second.close()


def test_recovery_survives_a_torn_final_line(tmp_path):
    """
    Simulates a crash mid-write: the last line is truncated JSON. Recovery
    must skip it and resume from the last COMPLETE record. It must not throw,
    and it must not reset to 0.
    """
    p = Partition('torn-topic', 0, data_dir=str(tmp_path))
    for i in range(3):
        p.append(f"good-{i}")
    p.close()

    with open(p.log_path, 'a', encoding='utf-8', newline='\n') as f:
        f.write('{"offset": 3, "key": null, "value": "trunc')   # no closing brace

    recovered = Partition('torn-topic', 0, data_dir=str(tmp_path))
    try:
        assert recovered.next_offset == 3
        assert len(recovered.fetch(0)) == 3
        assert recovered.corrupt_lines >= 1, "damage should be counted, not hidden"
    finally:
        recovered.close()


# ── Concurrency ────────────────────────────────────────────────

def test_concurrent_appends_never_duplicate_an_offset(partition):
    """
    The broker runs appends in a thread pool, so this really happens.

    Without the lock in `append`, two threads read the same `next_offset`,
    both write it, and one message is silently lost while an offset goes
    missing. Every guarantee in Kafka rests on "one offset names exactly one
    message", so this is the most important test in the file.
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

    # And the file must contain every one of them, exactly once.
    with open(partition.log_path, encoding='utf-8', newline='\n') as f:
        offsets_on_disk = [json.loads(line)['offset'] for line in f if line.strip()]
    assert sorted(offsets_on_disk) == list(range(total))


# ── Lifecycle ──────────────────────────────────────────────────

def test_append_after_close_raises(tmp_path):
    p = Partition('closed-topic', 0, data_dir=str(tmp_path))
    p.close()
    with pytest.raises(RuntimeError):
        p.append('nope')


def test_close_is_idempotent(tmp_path):
    p = Partition('closed-topic', 0, data_dir=str(tmp_path))
    p.close()
    p.close()      # must not raise
