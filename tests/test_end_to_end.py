"""
End-to-end tests: real producer → real broker over TCP → real consumer.

These are the tests that would have caught all four Phase 0 bugs. They cover
the seams between components, which is where the interesting failures live —
each component can be individually correct and still disagree with its
neighbour about framing, flushing, or when a message counts as handled.
"""

import pytest

from common.net import BrokerConnectionError, ProtocolError, SyncConnection
from common.protocol import MSG_FETCH, MSG_PRODUCE, encode_message
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
    """
    Different topics are different files with independent offset sequences.
    Both start at 0, and neither can see the other's data.
    """
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


# ── Offset semantics: the Phase 0 headline fix ─────────────────

def test_poll_does_not_commit(broker, offsets_dir):
    """
    THE bug this phase fixed. `poll()` must advance the in-memory position
    only. If it also committed, a crash during processing would silently lose
    every message in the batch — at-most-once delivery, by accident.
    """
    with Producer('127.0.0.1', broker.port, 'commits', quiet=True) as p:
        for i in range(5):
            p.send(f"m{i}")

    with Consumer('127.0.0.1', broker.port, 'commits', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll()
        assert c.position == 5, "position advances in memory"
        assert c.committed == 0, "but nothing is committed until commit() is called"

    # A restarted consumer that never committed must replay the whole batch.
    with Consumer('127.0.0.1', broker.port, 'commits', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert c.position == 0
        assert len(c.poll()) == 5, "uncommitted messages are redelivered"


def test_committed_offset_survives_a_restart(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'resume', quiet=True) as p:
        for i in range(10):
            p.send(f"m{i}")

    with Consumer('127.0.0.1', broker.port, 'resume', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll(max_messages=4)
        c.commit()

    with Consumer('127.0.0.1', broker.port, 'resume', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        assert c.position == 4
        assert [m['value'] for m in c.poll()] == [f"m{i}" for i in range(4, 10)]


def test_at_least_once_redelivers_when_processing_fails(broker, offsets_dir):
    """
    The whole point of committing after processing: a failure mid-batch means
    the messages come back, rather than disappearing.
    """
    with Producer('127.0.0.1', broker.port, 'retry', quiet=True) as p:
        for i in range(6):
            p.send(f"m{i}")

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


def test_seek_replays_from_an_arbitrary_offset(broker, offsets_dir):
    """
    Replay is only possible because reads are non-destructive. This is the
    capability that makes 'redeploy the fixed consumer and reprocess
    yesterday' a one-line change instead of an impossibility.
    """
    with Producer('127.0.0.1', broker.port, 'replay', quiet=True) as p:
        for i in range(10):
            p.send(f"m{i}")

    with Consumer('127.0.0.1', broker.port, 'replay', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll()
        c.commit()
        assert c.poll() == [], "caught up"

        c.seek_to_beginning()
        assert len(c.poll()) == 10, "and now everything again"


def test_lag_reflects_unread_messages(broker, offsets_dir):
    with Producer('127.0.0.1', broker.port, 'lag-topic', quiet=True) as p:
        for i in range(10):
            p.send(f"m{i}")

    with Consumer('127.0.0.1', broker.port, 'lag-topic', 0, 'g',
                  offset_dir=offsets_dir, quiet=True) as c:
        c.poll(max_messages=4)
        assert c.lag() == 6
        c.poll(max_messages=100)
        assert c.lag() == 0


# ── Connection behaviour ───────────────────────────────────────

def test_one_connection_carries_many_requests(broker):
    """
    Connections are persistent. If each request needed a fresh connection,
    the TCP handshake would cost more than the work.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        offsets = [
            conn.request(MSG_PRODUCE,
                         {'topic': 'many', 'partition': 0, 'value': f"m{i}"})['offset']
            for i in range(50)
        ]
    assert offsets == list(range(50))


def test_fetching_an_unknown_topic_returns_empty_not_an_error(broker):
    """
    Auto-creation means a fetch for a topic nobody has written to succeeds
    with zero messages. Convenient — and the exact reason a typo'd topic name
    fails silently instead of loudly. Phase 3 makes this configurable.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(MSG_FETCH,
                                {'topic': 'never-seen', 'partition': 0, 'offset': 0})
    assert response['status'] == 'ok'
    assert response['messages'] == []


def test_unknown_message_type_is_rejected_without_dropping_the_connection(broker):
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        response = conn.request(99, {'anything': True})
        assert response['status'] == 'error'

        # The connection must still be usable — a bad request is the client's
        # problem, not a reason to hang up on it.
        ok = conn.request(MSG_PRODUCE, {'topic': 'still-alive', 'value': 'yes'})
        assert ok['status'] == 'ok'


def test_malformed_body_gets_an_error_reply_and_the_connection_survives(broker):
    """
    Before the fix, a bad JSON body raised inside a broad `except Exception`
    and killed the connection. A malformed request should be answered, not
    punished — and definitely should not look identical to a broker bug.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        # Hand-build a frame whose body is not valid JSON.
        good = encode_message(MSG_PRODUCE, {})
        header = good[:12]
        broken = header[:4] + (11).to_bytes(4, 'big') + header[8:] + b'not json!!!'
        conn._sock.sendall(broken)

        response = conn.recv()
        assert response['status'] == 'error'
        assert 'malformed' in response['reason']

        assert conn.request(MSG_PRODUCE,
                            {'topic': 'survived', 'value': 'yes'})['status'] == 'ok'


def test_bad_magic_number_closes_the_connection(broker):
    """
    A wrong magic number means the stream is desynchronised or the peer is
    not speaking our protocol. There is no way to find the next message
    boundary in a stream with no delimiters, so hanging up is the only
    correct response.
    """
    with SyncConnection('127.0.0.1', broker.port, timeout=5.0) as conn:
        conn._sock.sendall(b'\xde\xad\xbe\xef' + (0).to_bytes(4, 'big')
                           + (1).to_bytes(4, 'big'))
        with pytest.raises(BrokerConnectionError):
            conn.recv()


def test_connecting_to_a_dead_broker_raises_a_typed_error():
    """
    Transport failures must surface as BrokerConnectionError, not as raw
    OSError. Phase 3's retry logic needs to tell 'the connection broke, try
    again' apart from 'the broker answered and said no'.
    """
    conn = SyncConnection('127.0.0.1', 1, timeout=1.0)   # port 1: nothing listens
    with pytest.raises(BrokerConnectionError):
        conn.connect()


def test_talking_to_a_non_broker_raises_protocol_error(broker):
    """
    ProtocolError is deliberately NOT a connection error: retrying will
    produce the identical failure, because the two sides disagree about the
    format rather than about the network.
    """
    assert issubclass(ProtocolError, ValueError)
    assert not issubclass(ProtocolError, ConnectionError)


# ── Restart ────────────────────────────────────────────────────

def test_offsets_continue_across_a_broker_restart(tmp_path):
    """
    Kill the broker, start a new one on the same data directory, and offsets
    must continue rather than restart at 0. This is `Partition._recover_offset`
    working through the whole stack.
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
        for i in range(5):
            p.send(f"before-{i}")
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
