"""
CONCEPT: The On-Disk Record Format
------------------------------------
Phase 0 and 1 stored one JSON object per line. That was readable — you could
`cat` the file — and it had two flaws that matter more than readability.

**It could not detect corruption.** A torn write produces bytes that either
fail to parse (fine, we skip them) or parse into something *wrong* (not fine,
we return them as data). The second case is real:

    {"offset": 41, "value": "hello world"}      written
    {"offset": 41, "value": "hello"}            torn — and still valid JSON

Nothing anywhere would notice. A storage layer that cannot tell you its data is
intact is one you cannot build replication on top of.

**Record boundaries were implicit.** Finding the 500th record meant parsing 499
you did not want. Skipping a record required understanding it.

So each record now carries an 8-byte header:

    ┌──────────────┬──────────────┬────────────────────────────┐
    │  length 4B   │   crc32 4B   │   payload (JSON bytes)     │
    └──────────────┴──────────────┴────────────────────────────┘
    │◄── header ──►│              │◄──── `length` bytes ──────►│

  length   size of the payload, NOT including this header. Makes boundaries
           explicit: a reader can skip a record with one seek, without
           parsing it. This is what the index scan needs.
  crc32    checksum of the payload. Lets a reader know the bytes are intact
           BEFORE trying to interpret them.
  payload  still JSON. Legible in a hex dump, and no new dependency.

WHAT THIS COSTS
  - 8 bytes per record.
  - You can no longer `cat` a log file. That was a genuine teaching benefit,
    so it is replaced deliberately: `kfk dump-log` reads the format AND
    verifies the checksums, which `cat` never could. Real Kafka made exactly
    this trade and ships `kafka-dump-log.sh` for the same reason.

WHY CRC32 AND NOT A CRYPTOGRAPHIC HASH
  We are defending against *accidents* — a torn write, a bad sector, a flipped
  bit — not against an attacker who can recompute a checksum. CRC32 catches all
  single-bit errors, all double-bit errors, and any burst under 32 bits, at
  roughly the speed of memcpy. SHA-256 would be ~50x slower and defend against
  a threat that does not exist here. Match the tool to the actual failure mode.

  Note what CRC32 does NOT do: an attacker who can write to your disk can also
  fix the checksum. This is integrity against corruption, not against tampering.
"""

import json
import struct
import zlib

# '!' network byte order, so a log file written on one machine is readable on
# another regardless of native endianness. Log files outlive the process that
# wrote them and sometimes move between machines; the cost of pinning the
# layout is zero and the cost of not pinning it is a corrupt-looking file that
# is actually just byte-swapped.
RECORD_HEADER_FORMAT = '!I I'                       # length, crc32
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FORMAT)      # 8

# Sanity bound on a length field read off disk.
#
# The same reasoning as MAX_REQUEST_BYTES in the wire protocol: `length` is
# data we did not compute, and honouring it means committing that much memory.
# A corrupted length of 0xFFFFFFFF would otherwise ask for a 4 GB allocation.
# Anything over this is corruption by definition, and treated as end-of-valid-data.
MAX_RECORD_BYTES = 16 * 1024 * 1024                 # 16 MB


class CorruptRecordError(Exception):
    """
    A record's bytes are not intact: impossible length, or CRC mismatch.

    Callers treat this as "the valid part of the file ends here". We cannot
    recover past a corrupt record, because we no longer know where the next one
    begins — the length field that would tell us is exactly what we cannot
    trust.
    """


def encode_record(record: dict) -> bytes:
    """
    Serialise one record to its on-disk form.

    The CRC covers the payload only, not the header. If the length field itself
    is corrupted we will read the wrong number of bytes and the CRC will almost
    certainly fail anyway — so covering it buys very little. (Real Kafka does
    include some header fields in its CRC; with a batch header carrying much
    more metadata, it is worth more there.)
    """
    payload = json.dumps(record).encode('utf-8')
    header = struct.pack(RECORD_HEADER_FORMAT, len(payload), zlib.crc32(payload))
    return header + payload


def decode_record(header_bytes: bytes, payload: bytes) -> dict:
    """
    Verify and parse one record. Raises CorruptRecordError if damaged.

    Order matters: check the checksum BEFORE parsing. Parsing corrupt bytes can
    succeed and produce plausible nonsense, which is the exact failure this
    format exists to prevent.
    """
    length, expected_crc = struct.unpack(RECORD_HEADER_FORMAT, header_bytes)

    if len(payload) != length:
        raise CorruptRecordError(
            f"record claims {length} bytes, only {len(payload)} available")

    actual_crc = zlib.crc32(payload)
    if actual_crc != expected_crc:
        raise CorruptRecordError(
            f"CRC mismatch: stored {expected_crc:#010x}, computed {actual_crc:#010x}")

    try:
        return json.loads(payload.decode('utf-8'))
    except (ValueError, UnicodeDecodeError) as exc:
        # The CRC passed, so the bytes on disk are exactly what was written.
        # A parse failure here therefore means we wrote something invalid — a
        # bug in US, not disk corruption. Worth distinguishing: one is a
        # hardware story, the other is a code story.
        raise CorruptRecordError(
            f"payload passed CRC but is not valid JSON (a writer bug): {exc}") from exc


def record_size(payload_length: int) -> int:
    """Total bytes a record occupies on disk, header included."""
    return RECORD_HEADER_SIZE + payload_length


def iter_records(fh, start_position: int = 0):
    """
    Walk records from `start_position`, yielding (position, size, record).

    Stops **cleanly** at the first damaged or incomplete record rather than
    raising. That is the whole point: this function is used both for normal
    reads and for crash recovery, and in recovery a torn tail is the expected
    case, not an exception.

    `position` is the byte offset of the record header — which is exactly what
    the index stores, so an index entry can be built while iterating.

    Yielding size as well as position means a caller can compute where the
    valid data ends (`position + size`) without re-deriving it, which is what
    recovery truncates to.
    """
    fh.seek(start_position)
    position = start_position

    while True:
        header = fh.read(RECORD_HEADER_SIZE)
        if len(header) < RECORD_HEADER_SIZE:
            return                      # clean EOF, or a torn header

        length, _crc = struct.unpack(RECORD_HEADER_FORMAT, header)
        if length > MAX_RECORD_BYTES:
            return                      # impossible length: corruption

        payload = fh.read(length)
        if len(payload) < length:
            return                      # torn tail: the write did not finish

        try:
            record = decode_record(header, payload)
        except CorruptRecordError:
            return                      # CRC failed: valid data ends here

        size = RECORD_HEADER_SIZE + length
        yield position, size, record
        position += size
