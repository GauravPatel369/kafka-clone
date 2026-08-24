"""
Tests for topic metadata, multi-partition produce, and PRODUCE v1.

The theme: a topic's partition count has to be DECLARED and stable, because
producers route on `hash(key) % count`. A count inferred from whichever
directories happen to exist would change as data arrived, and key routing would
shift underneath running clients.
"""

import asyncio
import threading

import pytest

from broker.broker import Broker
from broker.metadata import InvalidTopicError, MetadataStore
from common import errors
from common.net import SyncConnection
from common.protocol import API_CREATE_TOPICS, API_METADATA, API_PRODUCE
from consumer.consumer import Consumer
from producer.producer import Producer


@pytest.fixture
def multi_broker(tmp_path):
    """A broker with auto-created topics defaulting to 4 partitions."""
    instance = Broker(host='127.0.0.1', port=0, data_dir=str(tmp_path / 'data'),
                      default_partitions=4, retention_check_ms=10 ** 7)
    thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                              daemon=True)
    thread.start()
    assert instance.ready.wait(timeout=5)
    yield instance
    instance.stop()
    thread.join(timeout=5)


# ── MetadataStore ──────────────────────────────────────────────

def test_topics_persist_across_restart(tmp_path):
    """
    A partition count that vanished on restart would be catastrophic: the
    broker would fall back to 1, and every keyed message would start routing
    somewhere new.
    """
    store = MetadataStore(str(tmp_path))
    store.create_topic('orders', partitions=6)

    reloaded = MetadataStore(str(tmp_path))
    assert reloaded.partition_count('orders') == 6


def test_creating_a_duplicate_topic_is_rejected(tmp_path):
    store = MetadataStore(str(tmp_path))
    store.create_topic('orders', partitions=2)
    with pytest.raises(InvalidTopicError, match='already exists'):
        store.create_topic('orders', partitions=8)


@pytest.mark.parametrize('name', ['', '.', '..', 'has space', 'sla/sh', 'a' * 250])
def test_invalid_topic_names_are_rejected(tmp_path, name):
    """
    The length limit traces to the filesystem: a topic name becomes part of a
    directory name, and `topic-partition` has to fit in 255 bytes. `.` and
    `..` are directory entries rather than names, and allowing them would let a
    topic escape the data directory.
    """
    store = MetadataStore(str(tmp_path))
    with pytest.raises(InvalidTopicError):
        store.create_topic(name, partitions=1)


def test_auto_create_can_be_disabled(tmp_path):
    """
    With auto-creation on, producing to a typo'd topic silently succeeds and
    your messages go where nobody reads. Off, it is a visible error — which is
    why most production clusters disable it.
    """
    store = MetadataStore(str(tmp_path), auto_create=False)
    with pytest.raises(errors.UnknownTopicOrPartitionError):
        store.get_or_create('never-declared')


def test_auto_created_topics_get_one_partition_by_default(tmp_path):
    """
    The quieter hazard of auto-creation: it costs you all your parallelism,
    silently, because a lazily-created topic has nobody to ask how wide it
    should be.
    """
    store = MetadataStore(str(tmp_path), auto_create=True)
    assert store.get_or_create('lazy').partitions == 1


def test_partition_validation_catches_out_of_range(tmp_path):
    """
    Without this, producing to partition 9 of a 4-partition topic would create
    `data/orders-9/` — data written where no consumer is ever assigned and no
    metadata response mentions it. It would simply disappear.
    """
    store = MetadataStore(str(tmp_path))
    store.create_topic('orders', partitions=4)

    store.validate_partition('orders', 3)
    with pytest.raises(errors.UnknownTopicOrPartitionError):
        store.validate_partition('orders', 4)
    with pytest.raises(errors.UnknownTopicOrPartitionError):
        store.validate_partition('orders', -1)


def test_metadata_file_survives_being_unreadable(tmp_path):
    """
    Losing the topic list is bad; refusing to start is worse — the partition
    DATA is still on disk and intact. Start empty rather than turning an
    unreadable metadata file into an outage.
    """
    store = MetadataStore(str(tmp_path))
    store.create_topic('orders', partitions=2)

    with open(store.path, 'w', encoding='utf-8') as fh:
        fh.write('{ this is not json')

    assert MetadataStore(str(tmp_path)).list_topics() == []


# ── CREATE_TOPICS over the wire ────────────────────────────────

def test_create_topics_request(multi_broker):
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        response = conn.request(API_CREATE_TOPICS,
                                {'topic': 'explicit', 'partitions': 6})
        assert response['error_code'] == errors.NONE
        assert response['partitions'] == 6

        again = conn.request(API_CREATE_TOPICS,
                             {'topic': 'explicit', 'partitions': 6})
        assert again['error_code'] == errors.INVALID_REQUEST


def test_metadata_reports_declared_partitions_before_any_data(multi_broker):
    """
    THE reason a metadata store had to exist. A 6-partition topic must report
    six partitions the moment it is created — before any of them hold data.

    Counting directories would give an answer that grows as messages arrive, so
    a producer's `hash(key) % count` would change underneath it and keys would
    start landing in different partitions.
    """
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'wide', 'partitions': 6})
        response = conn.request(API_METADATA, {'topics': ['wide']})

    topic = response['topics'][0]
    assert len(topic['partitions']) == 6
    assert [p['partition'] for p in topic['partitions']] == list(range(6))
    assert all(p['high_watermark'] == 0 for p in topic['partitions'])


# ── PRODUCE v1 ─────────────────────────────────────────────────

def test_produce_v1_writes_to_several_partitions_in_one_request(multi_broker):
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'batched', 'partitions': 3})
        response = conn.request(API_PRODUCE, {
            'topic': 'batched', 'acks': 1,
            'partitions': [
                {'partition': 0, 'records': [{'key': None, 'value': f"a{i}"}
                                             for i in range(5)]},
                {'partition': 2, 'records': [{'key': None, 'value': f"c{i}"}
                                             for i in range(3)]},
            ],
        }, api_version=1)

    assert response['error_code'] == errors.NONE
    results = {entry['partition']: entry for entry in response['partitions']}
    assert results[0]['base_offset'] == 0 and results[0]['count'] == 5
    assert results[2]['base_offset'] == 0 and results[2]['count'] == 3


def test_produce_v1_reports_errors_per_partition(multi_broker):
    """
    Why v1 exists at all. One request touches several partitions, so one bad
    partition must not fail the good ones — the client has to be able to tell
    which records to resend.
    """
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'mixed', 'partitions': 2})
        response = conn.request(API_PRODUCE, {
            'topic': 'mixed', 'acks': 1,
            'partitions': [
                {'partition': 0, 'records': [{'key': None, 'value': 'good'}]},
                {'partition': 9, 'records': [{'key': None, 'value': 'bad'}]},
            ],
        }, api_version=1)

    assert response['error_code'] == errors.NONE, "the request itself succeeded"
    results = {entry['partition']: entry for entry in response['partitions']}
    assert results[0]['error_code'] == errors.NONE
    assert results[0]['base_offset'] == 0
    assert results[9]['error_code'] == errors.UNKNOWN_TOPIC_OR_PARTITION


def test_produce_v0_still_works(multi_broker):
    """
    Per-API versioning demonstrated, not just described: the broker serves the
    Phase 1 shape and the Phase 3 shape at the same time. That is exactly what
    lets a cluster upgrade one node at a time while old clients keep working.
    """
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'oldclient', 'partitions': 2})

        v0 = conn.request(API_PRODUCE,
                          {'topic': 'oldclient', 'partition': 1, 'value': 'from-v0'},
                          api_version=0)
        assert v0['error_code'] == errors.NONE
        assert v0['offset'] == 0

        v1 = conn.request(API_PRODUCE, {
            'topic': 'oldclient', 'acks': 1,
            'partitions': [{'partition': 1,
                            'records': [{'key': None, 'value': 'from-v1'}]}],
        }, api_version=1)
        assert v1['partitions'][0]['base_offset'] == 1, (
            "both versions append to the same log")


def test_produce_to_a_partition_that_does_not_exist_is_rejected(multi_broker):
    """
    Silently creating `data/topic-9/` would write data nobody can ever read.
    A visible error is strictly better than invisible loss.
    """
    with SyncConnection('127.0.0.1', multi_broker.port, timeout=5.0) as conn:
        conn.request(API_CREATE_TOPICS, {'topic': 'narrow', 'partitions': 2})
        response = conn.request(API_PRODUCE,
                                {'topic': 'narrow', 'partition': 7, 'value': 'x'},
                                api_version=0)
    assert response['error_code'] == errors.UNKNOWN_TOPIC_OR_PARTITION


# ── Producer end to end ────────────────────────────────────────

def test_keyed_messages_all_land_in_one_partition(multi_broker):
    with Producer('127.0.0.1', multi_broker.port, 'keyed', quiet=True) as p:
        futures = [p.send(f"m{i}", key='cust-42') for i in range(40)]
        p.flush()

    partitions = {f.partition for f in futures}
    assert len(partitions) == 1, "one key must map to exactly one partition"
    assert [f.get() for f in futures] == list(range(40)), "and stay ordered"


def test_keyless_messages_spread_across_partitions(multi_broker):
    """
    Load balancing, demonstrated. Sticky sends runs to one partition at a time,
    so a large enough sample must still touch several.
    """
    with Producer('127.0.0.1', multi_broker.port, 'spread', quiet=True,
                  batch_size=256) as p:
        futures = [p.send('x' * 40) for i in range(400)]
        p.flush()

    used = {f.partition for f in futures}
    assert len(used) > 1, f"expected several partitions, used {used}"


def test_different_keys_reach_different_partitions(multi_broker):
    with Producer('127.0.0.1', multi_broker.port, 'manykeys', quiet=True) as p:
        futures = [p.send(f"m{i}", key=f"cust-{i}") for i in range(200)]
        p.flush()

    assert len({f.partition for f in futures}) == 4


def test_send_returns_a_future_that_resolves_to_an_offset(multi_broker):
    """
    The forced API change: returning an offset would mean waiting for the
    reply, and a message that waits for its own reply was never batched with
    anything.
    """
    with Producer('127.0.0.1', multi_broker.port, 'futures', quiet=True) as p:
        future = p.send('hello', partition=0)
        assert not future.done, "send() must not block"
        p.flush()
        assert future.get() == 0


def test_batching_sends_many_records_in_one_request(multi_broker):
    """
    Verified by counting REQUESTS, not by timing — a timing assertion would be
    flaky and would not actually prove batching happened.
    """
    sent = []
    with Producer('127.0.0.1', multi_broker.port, 'counted', quiet=True,
                  linger_ms=50) as p:
        original = p._send_topic_batches

        def counting(topic, batches):
            sent.append(sum(len(b) for b in batches))
            return original(topic, batches)

        p._send_topic_batches = counting
        for i in range(200):
            p.send(f"m{i}", partition=0)
        p.flush()

    assert sum(sent) == 200
    assert len(sent) < 200, f"expected batching, got {len(sent)} requests"


def test_acks_zero_gets_no_reply_and_no_offset(multi_broker, offsets_dir):
    """
    `acks=0` is implemented faithfully: the broker sends NOTHING back. The
    future resolves with -1 because there is no acknowledgement to report.

    That is the semantic, not an oversight — you asked not to be told. The
    messages still arrive; you simply have no way to know that they did.
    """
    with Producer('127.0.0.1', multi_broker.port, 'fireforget', quiet=True,
                  acks=0) as p:
        futures = [p.send(f"m{i}", partition=0) for i in range(10)]
        p.flush()

    assert all(f.get() == -1 for f in futures), "no offset is reported"

    with Consumer('127.0.0.1', multi_broker.port, 'fireforget', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert len(c.poll()) == 10, "the messages did arrive"


def test_the_connection_survives_mixing_acks_zero_and_one(multi_broker):
    """
    acks=0 sends no reply, so a client that read one would consume the NEXT
    response and desynchronise the stream permanently. Mixing the two on one
    connection is the case that would expose it.
    """
    with Producer('127.0.0.1', multi_broker.port, 'mixed-acks', quiet=True,
                  acks=0) as p:
        p.send('fire-and-forget', partition=0)
        p.flush()
        p.acks = 1
        assert p.send_and_wait('now-i-care', partition=0) == 1
