"""
CONCEPT: Wire Protocol
----------------------
Every message sent over TCP between producer/consumer and broker
follows a fixed binary format:

    ┌────────────┬────────────┬────────────┬──────────────────┐
    │  magic(4B) │ length(4B) │  type(4B)  │   body (JSON)    │
    └────────────┴────────────┴────────────┴──────────────────┘

- magic   : always 0xCAFE — confirms the packet is valid
- length  : how many bytes the JSON body is
- type    : what kind of message (PRODUCE, FETCH, ACK)
- body    : actual payload as JSON bytes

Why binary header + JSON body?
  - Binary header is fixed size so receiver always knows how many
    bytes to read first (12 bytes), then reads `length` more bytes
  - JSON body is human-readable, easy to debug, no extra libraries
"""

import struct
import json

# ── Header format ─────────────────────────────────────────────
# '!' = network byte order (big-endian, standard for protocols)
# 'I' = unsigned int (4 bytes)
# Three I's = 12 bytes total header
HEADER_FORMAT = '!I I I'
HEADER_SIZE   = struct.calcsize(HEADER_FORMAT)   # always 12

# Magic number — any packet not starting with this is garbage
MAGIC = 0xCAFE

# ── Message types ──────────────────────────────────────────────
# These are just integers on the wire, we give them names for clarity
MSG_PRODUCE  = 1   # producer → broker: "store this message"
MSG_FETCH    = 2   # consumer → broker: "give me messages from offset N"
MSG_ACK      = 3   # broker → client:   "here is my response"


def encode_message(msg_type: int, payload: dict) -> bytes:
    """
    Pack a message into bytes ready to send over TCP.

    Example:
        encode_message(MSG_PRODUCE, {"topic": "orders", "value": "hello"})
        → b'\x00\x00\xCA\xFE' + length + type + b'{"topic":"orders",...}'
    """
    # Convert dict → JSON string → bytes
    body = json.dumps(payload).encode('utf-8')

    # Pack the 12-byte header
    header = struct.pack(HEADER_FORMAT, MAGIC, len(body), msg_type)

    # Header + body = complete wire message
    return header + body


def decode_header(data: bytes) -> tuple[int, int, int]:
    """
    Read just the 12-byte header to learn how many more bytes to read.

    Returns: (magic, body_length, msg_type)
    """
    magic, length, msg_type = struct.unpack(HEADER_FORMAT, data)
    return magic, length, msg_type


def decode_body(data: bytes) -> dict:
    """
    Parse the JSON body bytes into a Python dict.
    """
    return json.loads(data.decode('utf-8'))
