"""
CONCEPT: The Wire Protocol, v2
--------------------------------
Every message between a client and a broker is a fixed 16-byte header
followed by a JSON body:

    ┌──────────┬───────────┬─────────┬─────────┬────────────────┬───────────┐
    │ magic 4B │ length 4B │ api_key │ api_ver │ correlation_id │ body JSON │
    │  0xCAFE  │  of body  │   2B    │   2B    │      4B        │           │
    └──────────┴───────────┴─────────┴─────────┴────────────────┴───────────┘
    └──────────────────────── 16 bytes ───────────────────────────┘

  magic           always 0xCAFE. A sanity check, not security: it catches
                  "you connected to the wrong port" and "the stream
                  desynchronised" immediately, instead of letting garbage be
                  interpreted as a length.
  length          body size IN BYTES. This is what gives a byte stream
                  message boundaries — see common/net.py for why TCP alone
                  cannot.
  api_key         which operation (PRODUCE, FETCH, ...).
  api_version     which version of that operation.
  correlation_id  client-chosen; echoed by the broker.


WHAT CHANGED FROM v1, AND WHY EACH FIELD EARNS ITS BYTES
----------------------------------------------------------
v1 was `magic | length | msg_type` — 12 bytes. Three problems, three fields.

**correlation_id (4B) — the expensive one to have omitted.**
v1 had no way to tell which response answered which request. The only reason
it worked is that the client never sent a second request until the first came
back. Every send therefore cost a full round trip of doing nothing, which
capped Phase 0 at ~4,000 msg/sec with the broker mostly idle.

With an ID, a client can put many requests on the wire and match the replies
as they arrive:

    req#1 ──►         instead of      req ──►
    req#2 ──►                             ◄── res
    req#3 ──►                         req ──►
         ◄── res#1                        ◄── res
         ◄── res#2                    req ──►
         ◄── res#3                        ◄── res

    one round trip                    three round trips

Four bytes buy back the entire idle time of the connection.

**api_version (2B) — the one that makes upgrades possible.**
A protocol with no version cannot change without a flag day: every client and
every broker must restart at the same instant. That is merely annoying on one
machine and impossible on ten, which restart one at a time over an hour and
must interoperate throughout.

Note it is versioned PER API, not once for the whole protocol. Adding a field
to FETCH should not force a new PRODUCE. Real Kafka takes this further with an
`ApiVersions` request (key 18) so a client can ask each broker what it
supports and pick the highest common version per API.

**api_key (2B) — a rename with a purpose.**
v1's `msg_type` conflated "what operation" with "is this a request or a
response" (there was a separate MSG_ACK). Now direction is implicit — a
response echoes the key it is answering — and the key names only the
operation. That is what lets one key have several versions.

Why `H` (uint16) and not `I` for these two? 65,535 operations is already
absurd; real Kafka has ~70. Spending 4 bytes each would waste 4 bytes on every
message forever.


WHY THIS IS A BREAKING CHANGE, AND WHY THAT IS OK EXACTLY ONCE
----------------------------------------------------------------
The header layout changed but MAGIC did not, so a stale v1 client is not
rejected cleanly — its `msg_type` lands where `api_key`/`api_version` now sit,
and it fails with UNSUPPORTED_VERSION rather than "wrong protocol".

That is a lesson, deliberately left in place: **changing a format without
changing its identifier makes the failure confusing.** Real protocols either
bump the identifier (a clean flag day) or negotiate (no flag day at all).

We can break it because every client lives in this repository. That is a fact
about this project, not a general licence, and it is precisely the decision
that becomes unavailable the moment someone else depends on you.
"""

import json
import struct
from collections import namedtuple

# '!' network byte order (big-endian) — pins the layout so a big-endian and a
#     little-endian machine agree. Without it, struct uses the host's order and
#     two architectures silently disagree about every integer.
# 'I' uint32   'H' uint16
HEADER_FORMAT = '!I I H H I'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)      # 16

MAGIC = 0xCAFE

# Largest body we will allocate for, checked BEFORE allocating.
#
# `length` arrives from the network and is trusted by nobody. A client —
# malicious or merely broken — can claim 4 GB, and without this check the
# broker faithfully tries to allocate it. Three such connections end the
# process, and no authentication is needed to send them, only a socket.
#
# A claimed length is a request to commit OUR memory. It must be validated
# before it is honoured, never after.
#
# Real Kafka splits this into `socket.request.max.bytes` (100 MB, the frame)
# and `message.max.bytes` (1 MB, one record) because the two limits protect
# different things: one bounds a single allocation, the other bounds what a
# consumer must be able to buffer.
MAX_REQUEST_BYTES = 10 * 1024 * 1024              # 10 MB


# ── API keys (Apache Kafka's real numbers) ─────────────────────
#
# Not arbitrary. Using Kafka's actual numbering means anything learned here
# maps onto real Kafka logs, stack traces, and the protocol spec. Reserving
# the later ones now, with the right values, means no renumbering when Phases
# 3-6 fill them in.

API_PRODUCE = 0
API_FETCH = 1
API_LIST_OFFSETS = 2
API_METADATA = 3

API_CREATE_TOPICS = 19         # implemented in Phase 3

# Reserved — defined so the numbers are claimed, not yet implemented.
API_OFFSET_COMMIT = 8          # Phase 4
API_OFFSET_FETCH = 9           # Phase 4
API_FIND_COORDINATOR = 10      # Phase 4
API_JOIN_GROUP = 11            # Phase 4
API_HEARTBEAT = 12             # Phase 4
API_LEAVE_GROUP = 13           # Phase 4
API_SYNC_GROUP = 14            # Phase 4

API_NAMES = {
    API_PRODUCE: 'PRODUCE',
    API_FETCH: 'FETCH',
    API_LIST_OFFSETS: 'LIST_OFFSETS',
    API_METADATA: 'METADATA',
    API_OFFSET_COMMIT: 'OFFSET_COMMIT',
    API_OFFSET_FETCH: 'OFFSET_FETCH',
    API_FIND_COORDINATOR: 'FIND_COORDINATOR',
    API_JOIN_GROUP: 'JOIN_GROUP',
    API_HEARTBEAT: 'HEARTBEAT',
    API_LEAVE_GROUP: 'LEAVE_GROUP',
    API_SYNC_GROUP: 'SYNC_GROUP',
    API_CREATE_TOPICS: 'CREATE_TOPICS',
}

# What this broker can actually speak: api_key → (min_version, max_version).
#
# Everything is v0 today. The table exists so that adding v1 to one API later
# is a one-line change here plus a branch in the handler — rather than a
# redesign. An api_key absent from this table is unimplemented; a version
# outside its range gets UNSUPPORTED_VERSION.
SUPPORTED_VERSIONS = {
    # PRODUCE v0 = one record, one partition (Phase 1).
    # PRODUCE v1 = many records across many partitions, with a per-partition
    #              error code (Phase 3).
    #
    # BOTH are served. That is not compatibility theatre — it is the entire
    # point of per-API versioning, demonstrated: a broker that speaks the old
    # and new version at once is what lets a ten-broker cluster upgrade one
    # node at a time while old clients keep working.
    API_PRODUCE: (0, 1),
    API_FETCH: (0, 0),
    API_LIST_OFFSETS: (0, 0),
    API_METADATA: (0, 0),
    API_CREATE_TOPICS: (0, 0),
}

# ── LIST_OFFSETS timestamp sentinels (Kafka's real values) ─────
TIMESTAMP_LATEST = -1     # offset one past the newest record
TIMESTAMP_EARLIEST = -2   # offset of the oldest record still retained


Header = namedtuple('Header', 'magic body_length api_key api_version correlation_id')


# ── Encoding ───────────────────────────────────────────────────

def encode_message(api_key: int, payload: dict, correlation_id: int = 0,
                   api_version: int = 0) -> bytes:
    """
    Pack one message: 16-byte header + JSON body.

    Used for requests and responses alike. A response simply echoes the
    api_key and correlation_id it is answering — so direction is implicit and
    needs no field of its own.
    """
    body = json.dumps(payload).encode('utf-8')
    header = struct.pack(HEADER_FORMAT, MAGIC, len(body),
                         api_key, api_version, correlation_id)
    return header + body


def decode_header(data: bytes) -> Header:
    """
    Parse the 16 header bytes.

    Returns a named tuple rather than a bare 5-tuple: `header.correlation_id`
    survives someone inserting a field, whereas `header[4]` silently becomes
    the wrong value. With five fields, positional unpacking is a bug waiting
    for a refactor.
    """
    return Header(*struct.unpack(HEADER_FORMAT, data))


def decode_body(data: bytes) -> dict:
    """Parse the JSON body. Raises json.JSONDecodeError on malformed input."""
    return json.loads(data.decode('utf-8'))


# ── Helpers ────────────────────────────────────────────────────

def api_name(api_key: int) -> str:
    """Human-readable API name, for logs and error messages."""
    return API_NAMES.get(api_key, f'UNKNOWN({api_key})')


def is_supported(api_key: int, api_version: int) -> bool:
    """Whether this build implements that api_key at that version."""
    bounds = SUPPORTED_VERSIONS.get(api_key)
    return bounds is not None and bounds[0] <= api_version <= bounds[1]
