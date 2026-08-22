"""
Tests for the wire format.

These are pure unit tests — no sockets, no files. The protocol is the contract
between every component in the system, so it gets tested in isolation where a
failure points at exactly one thing.
"""

import json
import struct

import pytest

from common.protocol import (
    HEADER_FORMAT, HEADER_SIZE, MAGIC,
    MSG_ACK, MSG_FETCH, MSG_PRODUCE,
    decode_body, decode_header, encode_message,
)


def test_header_is_exactly_twelve_bytes():
    """
    The header size is load-bearing: both the broker and every client read
    exactly this many bytes before they know anything else. If it silently
    changed, both ends would read from the wrong place and the failure would
    look like data corruption rather than a version mismatch.
    """
    assert HEADER_SIZE == 12
    assert struct.calcsize(HEADER_FORMAT) == 12


def test_encode_then_decode_roundtrips():
    payload = {'topic': 'orders', 'partition': 0, 'value': 'hello', 'key': None}
    wire = encode_message(MSG_PRODUCE, payload)

    magic, length, msg_type = decode_header(wire[:HEADER_SIZE])

    assert magic == MAGIC
    assert msg_type == MSG_PRODUCE
    assert length == len(wire) - HEADER_SIZE
    assert decode_body(wire[HEADER_SIZE:]) == payload


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
    payload = {'value': text}
    wire = encode_message(MSG_PRODUCE, payload)

    _magic, length, _type = decode_header(wire[:HEADER_SIZE])
    body = wire[HEADER_SIZE:]

    assert length == len(body)
    assert length > len(text)          # the frame is bytes, not characters
    assert decode_body(body)['value'] == text


def test_non_ascii_is_escaped_on_the_wire_and_that_costs_bandwidth():
    """
    An observation worth pinning down, because it is easy to assume otherwise.

    `json.dumps` defaults to `ensure_ascii=True`, so 'é' is transmitted as the
    six ASCII bytes `\\u00e9` rather than the two bytes UTF-8 would need. An
    emoji outside the BMP costs TWELVE bytes, sent as a surrogate pair.

    The upside is that the body is pure ASCII and survives any
    latin-1-assuming middlebox. The downside is up to 6x bandwidth on
    non-English text — which matters for a system whose whole job is moving
    bytes.

    Passing `ensure_ascii=False` would fix it, and this test would then fail
    loudly rather than the change slipping through unnoticed. Phase 1 revisits
    the body encoding; for now we simply know what we are paying.
    """
    wire = encode_message(MSG_PRODUCE, {'value': 'é'})
    body = wire[HEADER_SIZE:]

    assert b'\\u00e9' in body, "non-ASCII is escaped, not sent as UTF-8"
    assert b'\xc3\xa9' not in body, "the raw UTF-8 bytes are NOT on the wire"

    utf8_size = len(json.dumps({'value': 'é'}, ensure_ascii=False).encode('utf-8'))
    assert len(body) > utf8_size


@pytest.mark.parametrize('msg_type', [MSG_PRODUCE, MSG_FETCH, MSG_ACK])
def test_every_message_type_survives_the_wire(msg_type):
    wire = encode_message(msg_type, {'n': 1})
    assert decode_header(wire[:HEADER_SIZE])[2] == msg_type


def test_magic_is_big_endian_on_the_wire():
    """
    '!' in the struct format means network byte order (big-endian). Without
    it, struct uses the host's native order — so an x86 client (little-endian)
    and a big-endian server would disagree about every integer in the header.

    Pinning the byte order is what makes a protocol portable, and this test
    is what stops someone "simplifying" the '!' away.
    """
    wire = encode_message(MSG_PRODUCE, {})
    assert wire[:4] == b'\x00\x00\xca\xfe'


def test_empty_payload_is_valid():
    """Zero-length bodies must work — later phases use them for pings."""
    wire = encode_message(MSG_ACK, {})
    _magic, length, _type = decode_header(wire[:HEADER_SIZE])
    assert decode_body(wire[HEADER_SIZE:HEADER_SIZE + length]) == {}


def test_decode_header_rejects_wrong_size_input():
    with pytest.raises(struct.error):
        decode_header(b'too short')
