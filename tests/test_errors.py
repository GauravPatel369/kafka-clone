"""
Tests for the error taxonomy.

The point of numeric codes is that a client can decide what to do WITHOUT
reading English. These tests pin that property down — particularly the
retriable/non-retriable split, which Phase 3's retry loop and Phase 6's
failover both depend on.
"""

import pytest

from common import errors
from common.errors import (
    KafkaError, NotLeaderError, OffsetOutOfRangeError, RetriableError,
    UnknownServerError, UnknownTopicOrPartitionError,
    error_for_code, is_retriable, raise_for_error,
)


# ── The retriable split ────────────────────────────────────────

RETRIABLE_CODES = [
    errors.UNKNOWN_TOPIC_OR_PARTITION,
    errors.LEADER_NOT_AVAILABLE,
    errors.NOT_LEADER_OR_FOLLOWER,
    errors.REQUEST_TIMED_OUT,
    errors.NOT_ENOUGH_REPLICAS,
    errors.REBALANCE_IN_PROGRESS,
]

NON_RETRIABLE_CODES = [
    errors.UNKNOWN_SERVER_ERROR,
    errors.OFFSET_OUT_OF_RANGE,
    errors.CORRUPT_MESSAGE,
    errors.MESSAGE_TOO_LARGE,
    errors.UNSUPPORTED_VERSION,
    errors.INVALID_REQUEST,
]


@pytest.mark.parametrize('code', RETRIABLE_CODES)
def test_retriable_codes_produce_retriable_exceptions(code):
    """
    Phase 3's retry loop is `except RetriableError`. If a code that should be
    retriable is not, failover silently stops working and the symptom is
    "sometimes messages just fail" — about the worst bug to diagnose.
    """
    assert is_retriable(code)
    assert isinstance(error_for_code(code), RetriableError)


@pytest.mark.parametrize('code', NON_RETRIABLE_CODES)
def test_non_retriable_codes_are_not_retriable(code):
    """
    Equally important in the other direction. Retrying an INVALID_REQUEST
    resends identical bad bytes forever, in a tight loop.
    """
    assert not is_retriable(code)
    assert not isinstance(error_for_code(code), RetriableError)
    assert isinstance(error_for_code(code), KafkaError)


def test_unknown_topic_is_retriable_even_though_it_sounds_permanent():
    """
    Surprising but correct. In a cluster the usual cause is metadata that has
    not propagated yet, not a typo — the topic was created a second ago, or
    this broker has not seen the controller's update. Kafka classifies it
    retriable for the same reason.

    A genuine typo still surfaces: the retry loop exhausts its attempts and
    reports this error, just more slowly.
    """
    assert is_retriable(errors.UNKNOWN_TOPIC_OR_PARTITION)


def test_offset_out_of_range_is_not_retriable():
    """
    The data is gone (retention deleted it) or never existed. Retrying the
    same offset cannot help; the consumer must decide to reset to earliest or
    latest — Kafka's `auto.offset.reset`, and a real judgement call, since one
    choice reprocesses and the other skips.
    """
    assert not is_retriable(errors.OFFSET_OUT_OF_RANGE)


# ── Code → exception mapping ───────────────────────────────────

def test_error_for_code_returns_the_specific_type():
    assert isinstance(error_for_code(errors.NOT_LEADER_OR_FOLLOWER), NotLeaderError)
    assert isinstance(error_for_code(errors.OFFSET_OUT_OF_RANGE), OffsetOutOfRangeError)
    assert isinstance(
        error_for_code(errors.UNKNOWN_TOPIC_OR_PARTITION), UnknownTopicOrPartitionError)


def test_unrecognised_codes_fall_back_to_a_non_retriable_error():
    """
    Not an impossible case: a newer broker can return a code this client has
    never heard of. Falling back to non-retriable is the safe default —
    treating an unrecognised failure as retriable would spin forever on
    something we cannot understand.
    """
    exc = error_for_code(777)
    assert isinstance(exc, UnknownServerError)
    assert not exc.retriable


def test_error_message_names_the_code_and_carries_context():
    """
    Codes are for the program; the message is for the human reading the log
    at 3am. Both are needed, and the context (which topic? which offset?) is
    what turns "it failed" into something actionable.
    """
    exc = error_for_code(errors.NOT_LEADER_OR_FOLLOWER, 'try broker 5',
                         topic='orders', partition=3)
    text = str(exc)

    assert 'NOT_LEADER_OR_FOLLOWER' in text
    assert '6' in text
    assert 'try broker 5' in text
    assert "topic='orders'" in text
    assert 'partition=3' in text
    assert exc.context['partition'] == 3


def test_error_codes_match_real_kafka():
    """
    Deliberate, and the reason these numbers are not 1, 2, 3: read a real
    Kafka stack trace or the protocol spec and the values line up.
    """
    assert errors.NONE == 0
    assert errors.UNKNOWN_SERVER_ERROR == -1
    assert errors.OFFSET_OUT_OF_RANGE == 1
    assert errors.UNKNOWN_TOPIC_OR_PARTITION == 3
    assert errors.NOT_LEADER_OR_FOLLOWER == 6
    assert errors.NOT_ENOUGH_REPLICAS == 19
    assert errors.REBALANCE_IN_PROGRESS == 27
    assert errors.UNSUPPORTED_VERSION == 35


# ── raise_for_error ────────────────────────────────────────────

def test_raise_for_error_passes_successes_through_unchanged():
    """
    Returning the response lets it be used inline, so no call site can forget
    the check:  messages = raise_for_error(conn.request(...))['messages']
    """
    response = {'error_code': errors.NONE, 'offset': 41}
    assert raise_for_error(response) is response


def test_raise_for_error_raises_the_right_type():
    with pytest.raises(NotLeaderError):
        raise_for_error({'error_code': errors.NOT_LEADER_OR_FOLLOWER,
                         'error_message': 'broker 5 leads now'})


def test_raise_for_error_treats_a_missing_code_as_success():
    """
    Defensive: a response without the field is treated as success rather than
    as an unknown failure. The broker always sets it explicitly — precisely so
    a client never has to guess whether a missing field means "fine" or "an
    older broker did not tell me".
    """
    assert raise_for_error({'offset': 1}) == {'offset': 1}
