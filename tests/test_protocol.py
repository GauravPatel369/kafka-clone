"""
Tests for the wire format.

Pure unit tests — no sockets, no files. The protocol is the contract between
every component, so it is tested in isolation where a failure points at
exactly one thing.
"""

import json
import struct

import pytest

from common.protocol import (
    API_FETCH, API_METADATA, API_PRODUCE,
    HEADER_FORMAT, HEADER_SIZE, MAGIC, MAX_REQUEST_BYTES, SUPPORTED_VERSIONS,
    api_name, decode_body, decode_header, encode_message, is_supported,
)


def test_header_is_exactly_sixteen_bytes():
    """
    The header size is load-bearing: both the broker and every client read
    exactly this many bytes before they know anything else. If it silently
    changed, both ends would read from the wrong place and the failure would
    look like data corruption rather than a version mismatch.
    """
    assert HEADER_SIZE == 16
    assert struct.calcsize(HEADER_FORMAT) == 16


def test_encode_then_decode_roundtrips():
    payload = {'topic': 'orders', 'partition': 0, 'value': 'hello', 'key': None}
    wire = encode_message(API_PRODUCE, payload, correlation_id=7, api_version=0)

    header = decode_header(wire[:HEADER_SIZE])

    assert header.magic == MAGIC
    assert header.api_key == API_PRODUCE
    assert header.api_version == 0
    assert header.correlation_id == 7
    assert header.body_length == len(wire) - HEADER_SIZE
    assert decode_body(wire[HEADER_SIZE:]) == payload


def test_correlation_id_survives_the_wire():
    """
    The whole point of Phase 1. Without an exact round trip of this integer,
    a client cannot tell which response answers which request, and pipelining
    is impossible.
    """
    for correlation_id in (1, 42, 65535, 2 ** 32 - 1):
        wire = encode_message(API_FETCH, {}, correlation_id=correlation_id)
        assert decode_header(wire[:HEADER_SIZE]).correlation_id == correlation_id


def test_header_fields_do_not_bleed_into_each_other():
    """
    api_key and api_version are 2-byte fields packed between 4-byte ones.
    An off-by-one in the format string would put api_version's bytes into
    correlation_id and still "work" for small values — so use values whose
    bits would visibly collide.
    """
    wire = encode_message(0xABCD, {}, correlation_id=0x11223344, api_version=0xEF01)
    header = decode_header(wire[:HEADER_SIZE])

    assert header.api_key == 0xABCD
    assert header.api_version == 0xEF01
    assert header.correlation_id == 0x11223344


def test_header_is_a_named_tuple_not_a_bare_tuple():
    """
    Five positional fields is where `header[3]` stops being readable and
    starts being a bug waiting for someone to insert a field. Attribute access
    survives a reordering; indexing does not.
    """
    header = decode_header(encode_message(API_PRODUCE, {}, 1)[:HEADER_SIZE])
    assert header.correlation_id == 1
    assert header._fields == (
        'magic', 'body_length', 'api_key', 'api_version', 'correlation_id')


def test_length_field_counts_bytes_not_characters():
    """
    The length header must be a count of BYTES, not of str characters.

    If it ever counted characters, a non-ASCII message would make the reader
    stop short — leaving the rest of that message in the stream to be misread
    as the next message's header. That desynchronises the connection
    permanently, and the symptom ("bad magic number") looks nothing like the
    cause ("someone sent an emoji").
    """
    text = 'héllo wörld 🎉'
    wire = encode_message(API_PRODUCE, {'value': text})

    header = decode_header(wire[:HEADER_SIZE])
    body = wire[HEADER_SIZE:]

    assert header.body_length == len(body)
    assert header.body_length > len(text)        # bytes, not characters
    assert decode_body(body)['value'] == text


def test_non_ascii_is_escaped_on_the_wire_and_that_costs_bandwidth():
    """
    An observation worth pinning down, because it is easy to assume otherwise.

    `json.dumps` defaults to `ensure_ascii=True`, so 'é' travels as the six
    ASCII bytes `\\u00e9` rather than the two bytes UTF-8 would need. An emoji
    outside the BMP costs TWELVE, sent as a surrogate pair.

    The upside is a pure-ASCII body that survives any middlebox. The downside
    is up to 6x bandwidth on non-English text — which matters for a system
    whose whole job is moving bytes. Phase 2 revisits the body encoding; for
    now we simply know what we are paying.
    """
    body = encode_message(API_PRODUCE, {'value': 'é'})[HEADER_SIZE:]

    assert b'\\u00e9' in body, "non-ASCII is escaped, not sent as UTF-8"
    assert b'\xc3\xa9' not in body, "the raw UTF-8 bytes are NOT on the wire"

    utf8_size = len(json.dumps({'value': 'é'}, ensure_ascii=False).encode('utf-8'))
    assert len(body) > utf8_size


def test_magic_is_big_endian_on_the_wire():
    """
    '!' in the struct format means network byte order (big-endian). Without
    it, struct uses the host's native order — so an x86 client and a
    big-endian server would disagree about every integer in the header.

    Pinning the byte order is what makes a protocol portable, and this test is
    what stops someone "simplifying" the '!' away.
    """
    assert encode_message(API_PRODUCE, {})[:4] == b'\x00\x00\xca\xfe'


def test_empty_payload_is_valid():
    wire = encode_message(API_METADATA, {})
    header = decode_header(wire[:HEADER_SIZE])
    assert decode_body(wire[HEADER_SIZE:HEADER_SIZE + header.body_length]) == {}


def test_decode_header_rejects_wrong_size_input():
    with pytest.raises(struct.error):
        decode_header(b'too short')


# ── Versioning ─────────────────────────────────────────────────

@pytest.mark.parametrize('api_key', sorted(SUPPORTED_VERSIONS))
def test_every_advertised_api_supports_version_zero(api_key):
    assert is_supported(api_key, 0)


def test_unsupported_versions_and_unknown_keys_are_both_rejected():
    """
    Two different failures that a client fixes the same way, so they share a
    code: an api_key we do not implement at all, and one we implement only at
    other versions.
    """
    assert not is_supported(API_PRODUCE, 99), "version beyond our range"
    assert not is_supported(9999, 0), "api_key we never implemented"


def test_api_names_are_available_for_logging():
    assert api_name(API_PRODUCE) == 'PRODUCE'
    assert api_name(API_FETCH) == 'FETCH'
    assert 'UNKNOWN' in api_name(4242)


def test_api_keys_match_real_kafka():
    """
    Deliberate: using Apache Kafka's actual numbering means what you learn
    here maps onto real Kafka logs and the protocol spec. Inventing our own
    would have made this a dead end.
    """
    from common import protocol
    assert protocol.API_PRODUCE == 0
    assert protocol.API_FETCH == 1
    assert protocol.API_LIST_OFFSETS == 2
    assert protocol.API_METADATA == 3
    assert protocol.API_JOIN_GROUP == 11        # reserved for Phase 4
    assert protocol.API_CREATE_TOPICS == 19     # reserved for Phase 3


def test_request_size_limit_is_defined_and_sane():
    """
    The guard exists because `body_length` arrives from the network and is
    otherwise trusted: a client claiming 4 GB would have the broker try to
    allocate it. Large enough for real batches, small enough that one client
    cannot exhaust the broker.
    """
    assert 1024 * 1024 <= MAX_REQUEST_BYTES <= 100 * 1024 * 1024
