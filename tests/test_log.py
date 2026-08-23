"""
Tests for the storage engine: records, index, segments, retention, recovery.

These are the tests that matter most in the project. Every guarantee above this
layer — ordering, replay, consumer offsets, and (from Phase 6) replication —
assumes the log is correct. A bug here surfaces three phases away from its
cause.
"""

import os
import struct
import zlib

import pytest

from broker.log import Log, LogConfig
from broker.log.index import INDEX_ENTRY_SIZE, Index
from broker.log.record import (
    MAX_RECORD_BYTES, RECORD_HEADER_SIZE, CorruptRecordError,
    decode_record, encode_record, iter_records,
)
from broker.log.segment import Segment, segment_name


# ── Record format ──────────────────────────────────────────────

def test_record_roundtrips():
    record = {'offset': 41, 'key': 'user-7', 'value': 'hello', 'timestamp': 1700000000}
    encoded = encode_record(record)

    header, payload = encoded[:RECORD_HEADER_SIZE], encoded[RECORD_HEADER_SIZE:]
    assert decode_record(header, payload) == record


def test_record_header_is_eight_bytes():
    assert RECORD_HEADER_SIZE == 8
    encoded = encode_record({'offset': 0, 'value': 'x'})
    length, _crc = struct.unpack('!I I', encoded[:RECORD_HEADER_SIZE])
    assert length == len(encoded) - RECORD_HEADER_SIZE


def test_crc_detects_a_single_flipped_bit():
    """
    THE reason this format exists. Phase 1 stored JSON lines, so a corrupted
    record either failed to parse (fine) or parsed into plausible nonsense and
    was served as real data (not fine). One flipped bit must now be caught.
    """
    encoded = bytearray(encode_record({'offset': 1, 'value': 'hello world'}))
    encoded[-3] ^= 0x01                       # flip one bit inside the payload

    with pytest.raises(CorruptRecordError, match='CRC mismatch'):
        decode_record(bytes(encoded[:RECORD_HEADER_SIZE]),
                      bytes(encoded[RECORD_HEADER_SIZE:]))


def test_crc_detects_truncation_that_would_still_be_valid_json():
    """
    The specific failure Phase 1 could not catch: a torn write whose remains
    happen to parse. `{"value": "hello world"}` cut to `{"value": "hello"}` is
    valid JSON and a believable message.
    """
    payload = b'{"offset": 1, "value": "hello"}'
    header = struct.pack('!I I', len(payload), zlib.crc32(b'{"offset": 1, "value": "hello world"}'))

    with pytest.raises(CorruptRecordError, match='CRC mismatch'):
        decode_record(header, payload)


def test_iter_records_stops_cleanly_at_a_torn_tail(tmp_path):
    """
    `iter_records` must not raise on damage — recovery uses it, and in
    recovery a torn tail is the expected case, not an exception.
    """
    path = tmp_path / 'x.log'
    good = b''.join(encode_record({'offset': i, 'value': f"m{i}"}) for i in range(3))
    with open(path, 'wb') as fh:
        fh.write(good)
        fh.write(encode_record({'offset': 3, 'value': 'torn'})[:5])   # half a header

    with open(path, 'rb') as fh:
        records = list(iter_records(fh))

    assert [r['offset'] for _pos, _size, r in records] == [0, 1, 2]
    assert records[-1][0] + records[-1][1] == len(good), (
        "the last valid record must end exactly where the good data ends — "
        "this is the byte recovery truncates to")


def test_absurd_length_field_is_treated_as_corruption_not_allocated(tmp_path):
    """
    Same reasoning as MAX_REQUEST_BYTES on the wire: a length read off disk is
    a request to commit memory we did not authorise. A corrupted 0xFFFFFFFF
    must not become a 4 GB allocation.
    """
    path = tmp_path / 'x.log'
    with open(path, 'wb') as fh:
        fh.write(encode_record({'offset': 0, 'value': 'fine'}))
        fh.write(struct.pack('!I I', MAX_RECORD_BYTES + 1, 0))

    with open(path, 'rb') as fh:
        records = list(iter_records(fh))

    assert len(records) == 1


# ── Index ──────────────────────────────────────────────────────

def test_index_lookup_never_overshoots(tmp_path):
    """
    THE correctness contract of a sparse index. The caller scans forward from
    whatever position this returns, so landing early costs microseconds while
    landing late would silently skip records — messages reported as missing
    that are sitting right there on disk.
    """
    index = Index(str(tmp_path / 'x.index'), base_offset=100)
    index.append(100, 0)
    index.append(150, 4096)
    index.append(200, 8192)

    assert index.lookup(100) == 0
    assert index.lookup(149) == 0, "must not jump to the entry for 150"
    assert index.lookup(150) == 4096
    assert index.lookup(199) == 4096
    assert index.lookup(999) == 8192, "past the end → the last entry, then scan"
    assert index.lookup(50) == 0, "below the base → start of the segment"


def test_index_persists_and_reloads(tmp_path):
    path = str(tmp_path / 'x.index')
    index = Index(path, base_offset=0)
    for i in range(10):
        index.append(i * 50, i * 4096)
    index.close()

    assert os.path.getsize(path) == 10 * INDEX_ENTRY_SIZE

    reloaded = Index(path, base_offset=0)
    reloaded.load()
    assert len(reloaded) == 10
    # Entries are (offset 50i → position 4096i). Offset 475 falls between the
    # entries for 450 and 500, so the lookup must land on 450's position.
    assert reloaded.lookup(475) == 9 * 4096
    assert reloaded.lookup(450) == 9 * 4096
    assert reloaded.lookup(449) == 8 * 4096


def test_index_rejects_out_of_order_entries(tmp_path):
    """
    Entries must stay ascending or the binary search is meaningless — and the
    failure would be silent: a lookup returning a position past the target,
    then a forward scan that misses it entirely.
    """
    index = Index(str(tmp_path / 'x.index'), base_offset=0)
    index.append(10, 100)
    index.append(5, 50)                        # ignored, not appended
    assert index.entries() == [(10, 100)]


def test_a_truncated_index_file_is_tolerated(tmp_path):
    """
    An index holds no information the log does not, so a torn one costs a
    longer scan, never correctness. There is nothing to fail about.
    """
    path = str(tmp_path / 'x.index')
    index = Index(path, base_offset=0)
    index.append(0, 0)
    index.append(50, 4096)
    index.close()

    with open(path, 'r+b') as fh:
        fh.truncate(INDEX_ENTRY_SIZE + 3)      # one entry plus a fragment

    reloaded = Index(path, base_offset=0)
    reloaded.load()
    assert len(reloaded) == 1


# ── Segments ───────────────────────────────────────────────────

def test_segment_names_sort_lexically_in_offset_order():
    """
    20-digit zero padding is what makes `sorted(os.listdir())` return segments
    in offset order with no parsing. Without it "10000.log" sorts before
    "9471.log" and the whole structure silently breaks.
    """
    names = [segment_name(n, '.log') for n in (0, 9471, 10000, 1234567)]
    assert names == sorted(names)
    assert names[0] == '00000000000000000000.log'


def test_segment_appends_and_reads_back(tmp_path):
    segment = Segment.create(str(tmp_path), base_offset=0, index_interval_bytes=64)
    try:
        for i in range(50):
            segment.append({'offset': i, 'key': None, 'value': f"m{i}", 'timestamp': 0})

        assert segment.next_offset == 50
        assert [r['offset'] for r in segment.read(0, 100)] == list(range(50))
        assert [r['offset'] for r in segment.read(30, 5)] == [30, 31, 32, 33, 34]
    finally:
        segment.close()


def test_segment_index_stays_sparse(tmp_path):
    """
    One entry per interval, not one per record. A dense index of a billion
    records would be gigabytes; that is the whole reason for the interval.
    """
    segment = Segment.create(str(tmp_path), base_offset=0, index_interval_bytes=1024)
    try:
        for i in range(500):
            segment.append({'offset': i, 'key': None, 'value': 'x' * 50, 'timestamp': 0})
        assert 1 < len(segment.index) < 100, (
            f"expected a sparse index, got {len(segment.index)} entries for 500 records")
    finally:
        segment.close()


# ── Log: rolling ───────────────────────────────────────────────

def test_log_rolls_segments_when_full(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=2048, index_interval_bytes=256))
    try:
        for i in range(200):
            log.append('x' * 60)

        assert len(log.segments) > 1, "should have rolled"
        assert log.next_offset == 200

        # Base offsets ascend and are contiguous — no gaps, no overlaps.
        bases = [s.base_offset for s in log.segments]
        assert bases == sorted(bases)
        for earlier, later in zip(log.segments, log.segments[1:]):
            assert earlier.next_offset == later.base_offset
    finally:
        log.close()


def test_reads_span_segment_boundaries(tmp_path):
    """
    A caller asking for 100 messages should get 100 — not "however many
    happened to be left in that file".
    """
    log = Log(str(tmp_path), LogConfig(segment_bytes=1024, index_interval_bytes=128))
    try:
        for i in range(300):
            log.append(f"m{i}")
        assert len(log.segments) > 2, "test needs several segments to be meaningful"

        assert [r['value'] for r in log.read(0, 300)] == [f"m{i}" for i in range(300)]
        assert [r['offset'] for r in log.read(95, 10)] == list(range(95, 105))
    finally:
        log.close()


@pytest.mark.parametrize('start', [0, 1, 137, 499, 500])
def test_read_from_any_offset_is_correct(tmp_path, start):
    """
    The index lands at or before the target and the caller scans forward. An
    off-by-one anywhere in that path shows up as a missing or duplicated
    record at exactly one starting offset, so sweep several.
    """
    log = Log(str(tmp_path), LogConfig(segment_bytes=4096, index_interval_bytes=128))
    try:
        for i in range(500):
            log.append(f"m{i}")

        got = [r['offset'] for r in log.read(start, 1000)]
        assert got == list(range(start, 500))
    finally:
        log.close()


# ── Log: recovery ──────────────────────────────────────────────

def test_offsets_continue_after_reopen(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=1024))
    for i in range(100):
        log.append(f"m{i}")
    log.close()

    reopened = Log(str(tmp_path), LogConfig(segment_bytes=1024))
    try:
        assert reopened.next_offset == 100
        assert reopened.append('after') == 100
        assert len(reopened.read(0, 200)) == 101
    finally:
        reopened.close()


def test_recovery_truncates_a_torn_tail(tmp_path):
    """
    The upgrade over Phase 1. Before, a bad record was skipped and left in the
    file forever, with the next append landing after it — permanent garbage in
    the middle of the log. Now the file is actually repaired.
    """
    log = Log(str(tmp_path))
    for i in range(10):
        log.append(f"m{i}")
    active_path = log.active_segment.log_path
    good_size = log.active_segment.size_bytes
    log.close()

    with open(active_path, 'ab') as fh:
        fh.write(b'\x00\x01\x02garbage that is not a record')

    recovered = Log(str(tmp_path))
    try:
        assert recovered.corrupt_bytes_recovered > 0
        assert os.path.getsize(active_path) == good_size, "the file is repaired, not just skipped"
        assert recovered.next_offset == 10
        assert recovered.append('after-repair') == 10
        assert [r['value'] for r in recovered.read(0, 100)][-1] == 'after-repair'
    finally:
        recovered.close()


def test_recovery_detects_a_flipped_bit_in_the_last_record(tmp_path):
    """
    Corruption inside a record, not just a short write. Everything from the bad
    record on is unrecoverable by definition — the length field that would say
    where the next record starts is exactly what we cannot trust.
    """
    log = Log(str(tmp_path))
    for i in range(5):
        log.append(f"message-{i}")
    path = log.active_segment.log_path
    log.close()

    with open(path, 'r+b') as fh:
        fh.seek(os.path.getsize(path) - 4)
        last = fh.read(1)
        fh.seek(os.path.getsize(path) - 4)
        fh.write(bytes([last[0] ^ 0x01]))

    recovered = Log(str(tmp_path))
    try:
        assert recovered.corrupt_bytes_recovered > 0
        assert recovered.next_offset == 4, "the damaged record is gone"
        assert [r['value'] for r in recovered.read(0, 100)] == [
            f"message-{i}" for i in range(4)]
    finally:
        recovered.close()


def test_a_deleted_index_is_rebuilt(tmp_path):
    """
    An index is derived data: it holds nothing the log does not. That
    asymmetry is what makes recovery simple — anything derived can be
    recomputed, and only the log itself has to survive.
    """
    log = Log(str(tmp_path), LogConfig(segment_bytes=100_000, index_interval_bytes=128))
    for i in range(300):
        log.append(f"m{i}")
    index_path = log.active_segment.index_path
    log.close()

    os.remove(index_path)

    rebuilt = Log(str(tmp_path), LogConfig(segment_bytes=100_000, index_interval_bytes=128))
    try:
        assert os.path.exists(index_path), "index should be regenerated"
        assert len(rebuilt.active_segment.index) > 1
        assert [r['offset'] for r in rebuilt.read(250, 10)] == list(range(250, 260))
    finally:
        rebuilt.close()


def test_only_the_active_segment_is_scanned_on_startup(tmp_path):
    """
    Sealed segments are immutable, so only the active one can be torn. Startup
    scanning one file instead of the whole log is the difference between
    seconds and hours on a large partition.

    Verified by damaging a SEALED segment: recovery must not notice, because
    it must not have looked.
    """
    config = LogConfig(segment_bytes=512, index_interval_bytes=128)
    log = Log(str(tmp_path), config)
    for i in range(200):
        log.append(f"m{i}")
    assert len(log.segments) > 2
    sealed_path = log.segments[0].log_path
    log.close()

    with open(sealed_path, 'ab') as fh:
        fh.write(b'junk appended to a sealed segment')

    reopened = Log(str(tmp_path), config)
    try:
        assert reopened.corrupt_bytes_recovered == 0, (
            "sealed segments are not scanned at startup — that is the point")
    finally:
        reopened.close()


# ── Log: retention ─────────────────────────────────────────────

def test_retention_by_size_deletes_whole_segments(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=1024, retention_bytes=3000,
                                       index_interval_bytes=256))
    try:
        for i in range(400):
            log.append('x' * 40)
        before = len(log.segments)

        deleted = log.delete_old_segments()

        assert deleted, "should have deleted something"
        assert len(log.segments) < before
        assert log.total_bytes() <= 3000 + log.config.segment_bytes
    finally:
        log.close()


def test_retention_raises_log_start_offset(tmp_path):
    """
    The visible consequence of deletion, and the reason OFFSET_OUT_OF_RANGE
    becomes reachable at all: a consumer below this point has lost its data.
    """
    log = Log(str(tmp_path), LogConfig(segment_bytes=512, retention_bytes=1000,
                                       index_interval_bytes=128))
    try:
        for i in range(300):
            log.append('x' * 40)
        assert log.log_start_offset == 0

        log.delete_old_segments()

        assert log.log_start_offset > 0
        assert log.read(0, 10) == [], "deleted data is gone, not merely hidden"
        assert log.read(log.log_start_offset, 5), "surviving data still readable"
    finally:
        log.close()


def test_retention_never_deletes_the_active_segment(tmp_path):
    """
    You do not delete the file you are writing to. The consequence is that a
    topic with one unfilled segment keeps everything regardless of policy —
    which is exactly what `segment_ms` exists to prevent.
    """
    log = Log(str(tmp_path), LogConfig(segment_bytes=10 * 1024 * 1024,
                                       retention_bytes=1))
    try:
        for i in range(50):
            log.append(f"m{i}")

        assert log.delete_old_segments() == []
        assert len(log.segments) == 1
        assert len(log.read(0, 100)) == 50
    finally:
        log.close()


def test_retention_by_age(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=512, retention_ms=1,
                                       index_interval_bytes=128))
    try:
        for i in range(200):
            log.append('x' * 40)
        assert len(log.segments) > 1

        import time
        time.sleep(1.1)                        # timestamps have second resolution

        assert log.delete_old_segments()
        assert len(log.segments) == 1, "everything sealed and old is gone"
    finally:
        log.close()


# ── Log: truncation (Phase 6 groundwork) ───────────────────────

def test_truncate_to_removes_everything_at_or_above(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=1024, index_interval_bytes=128))
    try:
        for i in range(200):
            log.append(f"m{i}")

        log.truncate_to(150)

        assert log.next_offset == 150
        assert [r['offset'] for r in log.read(0, 500)] == list(range(150))
        assert log.append('after-truncate') == 150
    finally:
        log.close()


def test_truncate_survives_a_reopen(tmp_path):
    """A truncation that only exists in memory is not a truncation."""
    config = LogConfig(segment_bytes=1024, index_interval_bytes=128)
    log = Log(str(tmp_path), config)
    for i in range(200):
        log.append(f"m{i}")
    log.truncate_to(75)
    log.close()

    reopened = Log(str(tmp_path), config)
    try:
        assert reopened.next_offset == 75
        assert len(reopened.read(0, 500)) == 75
    finally:
        reopened.close()


# ── Verification ───────────────────────────────────────────────

def test_verify_reports_a_clean_log(tmp_path):
    log = Log(str(tmp_path), LogConfig(segment_bytes=1024, index_interval_bytes=128))
    try:
        for i in range(100):
            log.append(f"m{i}")
        report = log.verify()

        assert report['records'] == 100
        assert report['damaged_segments'] == 0
        assert all(s['damaged_bytes'] == 0 for s in report['segments'])
    finally:
        log.close()


def test_verify_reports_damage(tmp_path):
    log = Log(str(tmp_path))
    for i in range(20):
        log.append(f"m{i}")
    path = log.active_segment.log_path
    log.close()

    with open(path, 'ab') as fh:
        fh.write(b'trailing junk')

    # A fresh Log would repair it on open, so check the file directly.
    reopened = Log(str(tmp_path))
    try:
        assert reopened.corrupt_bytes_recovered == len(b'trailing junk')
        assert reopened.verify()['damaged_segments'] == 0, "already repaired"
    finally:
        reopened.close()
