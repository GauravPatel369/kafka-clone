"""
End-to-end tests: real producer → real broker over TCP → real consumer.

These cover the seams between components, which is where the interesting
failures live — each component can be individually correct and still disagree
with its neighbour about framing, flushing, versioning, or when a message
counts as handled.
"""

import pytest

from common import errors
from common.errors import (
    InvalidRequestError, KafkaError, OffsetOutOfRangeError,
    UnknownTopicOrPartitionError, UnsupportedVersionError,
)
from common.net import BrokerConnectionError, ProtocolError, SyncConnection
from common.protocol import (
    API_FETCH, API_LIST_OFFSETS, API_METADATA, API_PRODUCE,
    HEADER_SIZE, MAX_REQUEST_BYTES, TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
    encode_message,
)
from consumer.consumer import Consumer
from producer.producer import Producer


# ── The happy path ─────────────────────────────────────────────

def test_produced_messages_come_back_in_order(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'orders', quiet=True) as producer:
        for i in range(20):
            producer.send(f"order-{i}", key=f"customer-{i % 3}")

    with Consumer('127.0.0.1', broker.port, 'orders', 0, 'g1',
                  offset_dir=offsets_dir, quiet=True) as consumer:
        messages = consumer.poll(max_messages=100)

    assert [m['value'] for m in messages] == [f"order-{i}" for i in range(20)]
    assert [m['offset'] for m in messages] == list(range(20))


def test_topics_are_isolated_from_each_other(broker, offsets_dir):
    """Different topics are different files with independent offset sequences."""
    with Producer('127.0.0.1', broker.port, 'topic-a', quiet=True) as p:
        p.send('a1')
        p.send('a2')
    with Producer('127.0.0.1', broker.port, 'topic-b', quiet=True) as p:
        assert p.send('b1') == 0, "topic-b has its own offset sequence"

    with Consumer('127.0.0.1', broker.port, 'topic-a', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert [m['value'] for m in c.poll()] == ['a1', 'a2']

    with Consumer('127.0.0.1', broker.port, 'topic-b', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert [m['value'] for m in c.poll()] == ['b1']


def test_two_groups_each_receive_every_message(broker, offsets_dir):
    """
    THE defining property of a log versus a queue: reading is not destructive,
    so independent groups each see the full stream. In a queue the two would
    compete and each message would go to exactly one of them.
    """
    with Producer('127.0.0.1', broker.port, 'events', quiet=True) as p:
        for i in range(5):
            p.send(f"e{i}")

    for group in ('analytics', 'billing'):
        with Consumer('127.0.0.1', broker.port, 'events', 0, group,
                      offset_dir=offsets_dir, quiet=True) as c:
            assert len(c.poll()) == 5, f"{group} should see all five"


# ── Correlation IDs and pipelining (the Phase 1 headline) ──────

def test_responses_echo_the_correlation_id(broker):
    """
    Without an exact echo, a client cannot tell which response answers which
    request. Everything pipelined rests on this one property.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        for expected in range(1, 6):
            correlation_id = conn.send_request(
                API_PRODUCE, {'topic': 'echo', 'value': f"m{expected}"})
            header, body = conn._read_frame()

            assert correlation_id == expected, "IDs start at 1 and increment"
            assert header.correlation_id == correlation_id
            assert header.api_key == API_PRODUCE, "the api_key is echoed too"
            assert body['error_code'] == errors.NONE


def test_pipelining_returns_replies_in_request_order(broker):
    """
    The payoff of Phase 1. All requests go out before any reply is read, and
    `pipeline` puts the answers back in the caller's order by matching
    correlation IDs — not by trusting arrival order.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        responses = conn.pipeline([
            (API_PRODUCE, {'topic': 'pipe', 'partition': 0, 'value': f"m{i}"})
            for i in range(50)
        ])

    assert len(responses) == 50
    assert [r['offset'] for r in responses] == list(range(50)), (
        "offsets ascending in request order proves the replies were matched "
        "correctly, not merely counted")


def test_pipelined_producer_matches_sequential_results(broker, offsets_dir):
    """Same messages, same order, same offsets — only the waiting differs."""
    with Producer('127.0.0.1', broker.port, 'pipe-topic', quiet=True) as p:
        offsets = p.send_pipelined([f"m{i}" for i in range(30)])

    assert offsets == list(range(30))

    with Consumer('127.0.0.1', broker.port, 'pipe-topic', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert [m['value'] for m in c.poll()] == [f"m{i}" for i in range(30)]


def test_correlation_mismatch_is_detected(broker, monkeypatch):
    """
    If the echo were ever wrong, a client would silently return one request's
    answer for another — a corruption bug that no amount of sequential testing
    would reveal. It must be caught loudly.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        # Force the client to expect an ID the broker will never send back.
        real_send = conn.send_request

        def lying_send(*args, **kwargs):
            real_send(*args, **kwargs)
            return 999999

        monkeypatch.setattr(conn, 'send_request', lying_send)

        with pytest.raises(ProtocolError, match='Correlation mismatch'):
            conn.request(API_PRODUCE, {'topic': 'x', 'value': 'y'})


# ── Versioning and error codes ─────────────────────────────────

def test_unsupported_api_version_is_rejected_cleanly(broker):
    """
    A protocol with no version cannot be upgraded without downtime. Rejecting
    an unknown version explicitly — rather than half-handling it — is what
    makes a rolling upgrade survivable.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_PRODUCE, {'topic': 'v', 'value': 'x'},
                                api_version=99)

    assert response['error_code'] == errors.UNSUPPORTED_VERSION
    with pytest.raises(UnsupportedVersionError):
        errors.raise_for_error(response)


def test_unknown_api_key_is_rejected_and_the_connection_survives(broker):
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(9999, {'anything': True})
        assert response['error_code'] == errors.UNSUPPORTED_VERSION

        # A bad request is the client's problem, not a reason to hang up.
        ok = conn.request(API_PRODUCE, {'topic': 'still-alive', 'value': 'yes'})
        assert ok['error_code'] == errors.NONE


def test_missing_required_field_is_an_invalid_request(broker):
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_PRODUCE, {'value': 'no topic here'})

    assert response['error_code'] == errors.INVALID_REQUEST
    with pytest.raises(InvalidRequestError):
        errors.raise_for_error(response)


def test_negative_offset_is_out_of_range(broker):
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_FETCH, {'topic': 't', 'offset': -5})

    assert response['error_code'] == errors.OFFSET_OUT_OF_RANGE
    with pytest.raises(OffsetOutOfRangeError):
        errors.raise_for_error(response)


def test_fetching_past_the_end_is_not_an_error(broker):
    """
    Every caught-up consumer does this on every poll. It must return an empty
    list, not an error — otherwise the steady state of a healthy system would
    be a stream of exceptions.
    """
    with Producer('127.0.0.1', broker.port, 'ahead', quiet=True) as p:
        p.send('only-one')

    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_FETCH, {'topic': 'ahead', 'offset': 500})

    assert response['error_code'] == errors.NONE
    assert response['messages'] == []


def test_oversized_request_is_refused_before_allocation(broker):
    """
    `body_length` arrives from the network and is trusted by nobody: a client
    can claim 4 GB and, unguarded, the broker would try to allocate it. No
    credentials needed — only a socket.

    We send a header claiming far too much and no body at all. The broker must
    answer MESSAGE_TOO_LARGE without waiting for bytes that will never come.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        header = encode_message(API_PRODUCE, {}, correlation_id=1)[:HEADER_SIZE]
        lying = (header[:4]
                 + (MAX_REQUEST_BYTES + 1).to_bytes(4, 'big')
                 + header[8:])
        conn._sock.sendall(lying)

        _hdr, body = conn._read_frame()

    assert body['error_code'] == errors.MESSAGE_TOO_LARGE


def test_malformed_body_gets_an_error_reply_and_the_connection_survives(broker):
    """
    A malformed request should be answered, not punished — and definitely
    should not look identical to a broker bug.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        header = encode_message(API_PRODUCE, {}, correlation_id=1)[:HEADER_SIZE]
        broken = header[:4] + (11).to_bytes(4, 'big') + header[8:] + b'not json!!!'
        conn._sock.sendall(broken)

        _hdr, body = conn._read_frame()
        assert body['error_code'] == errors.INVALID_REQUEST
        assert 'malformed' in body['error_message']

        ok = conn.request(API_PRODUCE, {'topic': 'survived', 'value': 'yes'})
        assert ok['error_code'] == errors.NONE


def test_non_object_body_is_rejected(broker):
    """A JSON array is valid JSON and still not a valid request."""
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        header = encode_message(API_PRODUCE, {}, correlation_id=1)[:HEADER_SIZE]
        payload = b'[1, 2, 3]'
        frame = (header[:4] + len(payload).to_bytes(4, 'big')
                 + header[8:] + payload)
        conn._sock.sendall(frame)

        _hdr, body = conn._read_frame()

    assert body['error_code'] == errors.INVALID_REQUEST


# ── LIST_OFFSETS and METADATA ──────────────────────────────────

def test_list_offsets_reports_both_ends(broker):
    """
    Answers "where does this partition start and end?" from metadata alone —
    constant time, no data transferred. A consumer wanting the newest message
    would otherwise have to fetch everything and throw it away.
    """
    with Producer('127.0.0.1', broker.port, 'bounds', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(7)])

    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        earliest = conn.request(API_LIST_OFFSETS, {
            'topic': 'bounds', 'timestamp': TIMESTAMP_EARLIEST})
        latest = conn.request(API_LIST_OFFSETS, {
            'topic': 'bounds', 'timestamp': TIMESTAMP_LATEST})

    assert earliest['offset'] == 0, "nothing deleted yet — retention is Phase 2"
    assert latest['offset'] == 7, "one past the last message"


def test_list_offsets_does_not_create_the_topic(broker):
    """
    Writes create; queries do not. A query that conjures the thing it asks
    about makes "does this exist?" unanswerable.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_LIST_OFFSETS, {'topic': 'ghost'})
        assert response['error_code'] == errors.UNKNOWN_TOPIC_OR_PARTITION

        # And it really was not created as a side effect.
        metadata = conn.request(API_METADATA, {})
        assert 'ghost' not in [t['name'] for t in metadata['topics']]


def test_list_offsets_rejects_real_timestamps_for_now(broker):
    """
    Real Kafka answers "first offset at or after this time", which is how
    'replay from 9am' works. That needs the time index Phase 2 introduces, so
    for now only the two sentinels are accepted — explicitly, rather than
    silently doing something else.
    """
    with Producer('127.0.0.1', broker.port, 'ts', quiet=True) as p:
        p.send('x')

    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_LIST_OFFSETS,
                                {'topic': 'ts', 'timestamp': 1700000000})

    assert response['error_code'] == errors.INVALID_REQUEST


def test_offset_out_of_range_after_retention_deletes_data(tmp_path):
    """
    The error that only became reachable in Phase 2.

    Through Phase 1 nothing was ever deleted, so `log_start_offset` was always
    0 and no valid offset could be too low. Now retention deletes whole
    segments off the front, and a consumer that fell far enough behind asks for
    data that no longer exists.

    This is the failure consumer lag monitoring exists to prevent, and it is
    deliberately NOT retriable: the data will not come back. The consumer must
    choose earliest or latest — Kafka's `auto.offset.reset`, and a real
    judgement call, since one reprocesses and the other skips.
    """
    import asyncio
    import threading

    from broker.broker import Broker
    from broker.log import LogConfig

    instance = Broker(
        host='127.0.0.1', port=0, data_dir=str(tmp_path / 'data'),
        log_config=LogConfig(segment_bytes=512, retention_bytes=1000,
                             index_interval_bytes=128),
        # Long interval so the background cleaner cannot fire mid-test and
        # make this depend on timing. We trigger retention explicitly instead.
        retention_check_ms=10 ** 7)
    thread = threading.Thread(target=lambda: asyncio.run(instance.start()), daemon=True)
    thread.start()
    assert instance.ready.wait(timeout=5)

    try:
        with Producer('127.0.0.1', instance.port, 'aging', quiet=True) as p:
            p.send_many(['x' * 40 for _ in range(300)])

        partition = instance.partitions[('aging', 0)]
        assert partition.delete_old_segments(), "retention should delete segments"
        start = partition.log_start_offset
        assert start > 0

        with SyncConnection('127.0.0.1', instance.port, timeout=5.0) as conn:
            gone = conn.request(API_FETCH, {'topic': 'aging', 'offset': 0})
            assert gone['error_code'] == errors.OFFSET_OUT_OF_RANGE
            with pytest.raises(OffsetOutOfRangeError) as caught:
                errors.raise_for_error(gone)
            assert not caught.value.retriable, "the data is gone; retrying cannot help"

            # Surviving data is still served normally.
            alive = conn.request(API_FETCH, {'topic': 'aging', 'offset': start})
            assert alive['error_code'] == errors.NONE
            assert alive['messages']
            assert alive['log_start_offset'] == start

        # And LIST_OFFSETS reports the new start, so a client can recover
        # without guessing.
        with SyncConnection('127.0.0.1', instance.port, timeout=5.0) as conn:
            earliest = conn.request(API_LIST_OFFSETS,
                                    {'topic': 'aging', 'timestamp': TIMESTAMP_EARLIEST})
        assert earliest['offset'] == start
    finally:
        instance.stop()
        thread.join(timeout=5)


def test_consumer_recovers_from_lost_data_by_seeking(broker, offsets_dir, tmp_path):
    """
    `seek_to_beginning` asks the broker where the log now starts rather than
    assuming 0 — which is exactly what makes it a valid recovery from
    OFFSET_OUT_OF_RANGE once retention is deleting data.
    """
    with Producer('127.0.0.1', broker.port, 'reset', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(10)])

    with Consumer('127.0.0.1', broker.port, 'reset', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.seek_to_beginning()
        assert c.position == 0, "nothing deleted yet, so the start is still 0"
        assert len(c.poll()) == 10


def test_metadata_describes_brokers_and_partitions(broker):
    """
    The request a real client makes before sending anything: it learns which
    broker leads which partition and connects there directly. Phase 5 makes
    the broker list longer than one; the shape does not change.
    """
    with Producer('127.0.0.1', broker.port, 'meta-a', quiet=True) as p:
        p.send_many(['x', 'y'])
    with Producer('127.0.0.1', broker.port, 'meta-b', quiet=True) as p:
        p.send('z')

    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_METADATA, {})

    assert response['brokers'] == [
        {'node_id': 0, 'host': '127.0.0.1', 'port': broker.port}]
    assert response['controller_id'] == 0

    topics = {t['name']: t for t in response['topics']}
    assert set(topics) == {'meta-a', 'meta-b'}

    partition = topics['meta-a']['partitions'][0]
    assert partition['leader'] == 0
    assert partition['replicas'] == [0], "one copy until Phase 6"
    assert partition['isr'] == [0]
    assert partition['high_watermark'] == 2
    assert partition['log_start_offset'] == 0


def test_metadata_reports_unknown_topics_per_topic(broker):
    """
    One unknown name must not hide the ones that were fine — so the error is
    reported on the topic, not on the whole request.
    """
    with Producer('127.0.0.1', broker.port, 'real', quiet=True) as p:
        p.send('x')

    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(API_METADATA, {'topics': ['real', 'imaginary']})

    assert response['error_code'] == errors.NONE, "the request itself succeeded"
    topics = {t['name']: t['error_code'] for t in response['topics']}
    assert topics['real'] == errors.NONE
    assert topics['imaginary'] == errors.UNKNOWN_TOPIC_OR_PARTITION


def test_seek_to_end_skips_existing_messages(broker, offsets_dir):
    """
    One half of Kafka's `auto.offset.reset`. Convenient for live dashboards,
    and the reason a surprising number of "we lost a day of events" incidents
    happen: `latest` silently skips everything that arrived while you were
    away.
    """
    with Producer('127.0.0.1', broker.port, 'skip', quiet=True) as p:
        p.send_many([f"old-{i}" for i in range(5)])

    with Consumer('127.0.0.1', broker.port, 'skip', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.seek_to_end()
        assert c.position == 5
        assert c.poll() == [], "the five old messages are skipped"

        with Producer('127.0.0.1', broker.port, 'skip', quiet=True) as p:
            p.send('new-one')

        assert [m['value'] for m in c.poll()] == ['new-one']


def test_seek_to_beginning_asks_the_broker(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'rewind', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(4)])

    with Consumer('127.0.0.1', broker.port, 'rewind', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll()
        c.commit()
        c.seek_to_beginning()
        assert c.position == 0
        assert len(c.poll()) == 4


# ── Offset semantics ───────────────────────────────────────────

def test_poll_does_not_commit(broker, offsets_dir):
    """
    The Phase 0 headline fix, still holding. `poll()` advances the in-memory
    position only. If it also committed, a crash during processing would
    silently lose the batch — at-most-once delivery, by accident.
    """
    with Producer('127.0.0.1', broker.port, 'commits', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(5)])

    with Consumer('127.0.0.1', broker.port, 'commits', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll()
        assert c.position == 5, "position advances in memory"
        assert c.committed == 0, "nothing is committed until commit() is called"

    with Consumer('127.0.0.1', broker.port, 'commits', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert c.position == 0
        assert len(c.poll()) == 5, "uncommitted messages are redelivered"


def test_committed_offset_survives_a_restart(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'resume', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(10)])

    with Consumer('127.0.0.1', broker.port, 'resume', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll(max_messages=4)
        c.commit()

    with Consumer('127.0.0.1', broker.port, 'resume', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert c.position == 4
        assert [m['value'] for m in c.poll()] == [f"m{i}" for i in range(4, 10)]


def test_at_least_once_redelivers_when_processing_fails(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'retry', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(6)])

    processed = []

    with Consumer('127.0.0.1', broker.port, 'retry', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        with pytest.raises(ValueError):
            for message in c.poll():
                if message['offset'] == 3:
                    raise ValueError("simulated processing failure")
                processed.append(message['value'])
        # No commit happens — the exception escaped before it.

    assert processed == ['m0', 'm1', 'm2']

    with Consumer('127.0.0.1', broker.port, 'retry', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        redelivered = [m['value'] for m in c.poll()]

    assert redelivered == [f"m{i}" for i in range(6)], (
        "at-least-once: the whole uncommitted batch returns, including the "
        "three already-processed messages — which is exactly why processing "
        "must be idempotent")


def test_lag_reflects_unread_messages(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'lag-topic', quiet=True) as p:
        p.send_many([f"m{i}" for i in range(10)])

    with Consumer('127.0.0.1', broker.port, 'lag-topic', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll(max_messages=4)
        assert c.lag() == 6
        c.poll(max_messages=100)
        assert c.lag() == 0


# ── Connection behaviour ───────────────────────────────────────

def test_one_connection_carries_many_requests(broker):
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        offsets = [
            conn.request(API_PRODUCE,
                         {'topic': 'many', 'partition': 0, 'value': f"m{i}"})['offset']
            for i in range(50)
        ]
    assert offsets == list(range(50))


def test_bad_magic_number_closes_the_connection(broker):
    """
    A wrong magic means the stream desynchronised or the peer is not speaking
    our protocol. With no delimiters there is no way to find the next message
    boundary, so hanging up is the only correct response.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        conn._sock.sendall(b'\xde\xad\xbe\xef' + b'\x00' * 12)
        with pytest.raises(BrokerConnectionError):
            conn._read_frame()


def test_connecting_to_a_dead_broker_raises_a_typed_error():
    """
    Transport failures surface as BrokerConnectionError, not raw OSError.
    Phase 3's retry logic needs to tell 'the connection broke, try again'
    apart from 'the broker answered and said no' (a KafkaError).
    """
    conn = SyncConnection('127.0.0.1', 1, timeout=1.0)   # port 1: nothing listens
    with pytest.raises(BrokerConnectionError):
        conn.connect()


def test_protocol_error_is_deliberately_not_a_connection_error():
    """
    Retrying a ProtocolError reproduces it exactly: the two sides disagree
    about the format, not about the network.
    """
    assert issubclass(ProtocolError, ValueError)
    assert not issubclass(ProtocolError, ConnectionError)
    assert not issubclass(KafkaError, ConnectionError)


def test_client_refuses_to_send_an_oversized_request(broker):
    """
    Checked client-side too — no point pushing 200 MB across the network to
    be told no.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        with pytest.raises(ProtocolError, match='over the'):
            conn.request(API_PRODUCE, {'topic': 't',
                                       'value': 'x' * (MAX_REQUEST_BYTES + 10)})


# ── Restart ────────────────────────────────────────────────────

def test_offsets_continue_across_a_broker_restart(tmp_path):
    """
    Kill the broker, start a new one on the same data directory, and offsets
    must continue rather than restart at 0. This is
    `Partition._recover_offset` working through the whole stack.
    """
    import asyncio
    import threading

    from broker.broker import Broker

    data_dir = str(tmp_path / 'data')

    def run(instance):
        thread = threading.Thread(target=lambda: asyncio.run(instance.start()),
                                  daemon=True)
        thread.start()
        assert instance.ready.wait(timeout=5)
        return thread

    first = Broker(host='127.0.0.1', port=0, data_dir=data_dir)
    thread = run(first)
    with Producer('127.0.0.1', first.port, 'durable', quiet=True) as p:
        p.send_many([f"before-{i}" for i in range(5)])
    first.stop()
    thread.join(timeout=5)

    second = Broker(host='127.0.0.1', port=0, data_dir=data_dir)
    thread = run(second)
    try:
        with Producer('127.0.0.1', second.port, 'durable', quiet=True) as p:
            assert p.send('after-restart') == 5, "offsets must not reset"
    finally:
        second.stop()
        thread.join(timeout=5)
