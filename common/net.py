"""
CONCEPT: Framed Request/Response Over a TCP Stream
---------------------------------------------------
This module is the *client-side transport*: it turns a Python dict into bytes
on a socket and a socket back into a Python dict.

It exists because `producer.py` and `consumer.py` each held a byte-identical
copy of the same twenty lines. Duplicated protocol code is how two components
quietly drift apart and then disagree about the wire format. Now there is one
copy, used by the producer, the consumer, the admin CLI, the benchmark, and
(from Phase 6) the replication fetcher.


THE PROBLEM THIS SOLVES: TCP HAS NO MESSAGE BOUNDARIES
--------------------------------------------------------
This is the most misunderstood thing about network programming, so it is worth
being precise.

TCP guarantees your bytes arrive, and arrive in order. It guarantees NOTHING
about how they are grouped. If you call:

    sock.sendall(b"HELLO")
    sock.sendall(b"WORLD")

the receiver may observe any of:

    recv() -> b"HELLOWORLD"     (glued together)
    recv() -> b"HEL"            (split across reads)
    recv() -> b"LOWOR"
    recv() -> b"LD"
    recv() -> b"HELLO"          (what you naively expected)

TCP is a *byte stream*, like a file. "Where does one message end?" is a
question TCP does not answer, so every protocol on top of it must answer for
itself. That is **framing**, and there are three common ways:

    ┌────────────────────┬──────────────────────────────────────────────────┐
    │ Framing strategy   │ Trade-off                                        │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Delimiter          │ Simple (readline()). But you must escape the      │
    │ (e.g. newline)     │ delimiter if it can appear in the payload, you    │
    │                    │ cannot pre-allocate, and it breaks on binary      │
    │                    │ data. HTTP/1.1 headers work this way.             │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Length prefix      │ Read a fixed header, learn the body length, read  │
    │  ← WE USE THIS     │ exactly that many bytes. Binary-safe, no          │
    │                    │ escaping, receiver allocates once. Kafka, gRPC,   │
    │                    │ and every serious binary protocol do this.        │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Close-to-signal-   │ "Read until the peer hangs up." One message per   │
    │ end                │ connection only. HTTP/1.0 did this, and it is     │
    │                    │ why HTTP/1.0 was slow.                            │
    └────────────────────┴──────────────────────────────────────────────────┘

So every read is a two-step dance:

        1. read EXACTLY 16 bytes    → the header
        2. unpack it                → body_length = 348
        3. read EXACTLY 348 bytes   → the body
        4. json.loads               → a dict

"read EXACTLY n bytes" is `_recv_exactly()` below, and it is the reason this
file exists.


NEW IN PHASE 1: CORRELATION IDS, AND WHAT THEY UNLOCK
-------------------------------------------------------
Every request carries a client-chosen integer; the response echoes it. Two
things follow.

**Detection.** `request()` verifies the echo. A mismatch means the stream has
desynchronised — we are reading from the middle of some other message — and
that is unrecoverable rather than merely unlucky, so it raises immediately
instead of returning a plausible-looking wrong answer.

**Pipelining.** Because replies can be matched rather than merely assumed, the
client no longer has to wait. `pipeline()` puts N requests on the wire and
then collects N replies:

    request()  →   send ─ wait ─ recv ─ send ─ wait ─ recv    (N round trips)
    pipeline() →   send send send ─ wait ─ recv recv recv     (1 round trip)

HOW MUCH IS THAT WORTH? MEASURE, DO NOT ASSUME.
------------------------------------------------
On loopback: **almost nothing.** `bench/bench.py` measures ~1.05x for
pipelined PRODUCE, and that number is worth understanding rather than
explaining away.

Pipelining removes time spent WAITING. The benchmark's cost breakdown shows
the loopback round trip is only ~38 us, while a PRODUCE costs ~253 us — the
other ~215 us is the broker genuinely working (a thread hop to the executor, a
file write, a flush). There is very little idle time to reclaim, because the
broker was never idle.

A second limit: our broker handles one request per connection at a time — read,
route, reply, repeat. Pipelined requests queue and are still served serially.
Pipelining removes the CLIENT's waiting, not the BROKER's serialisation. Real
Kafka does the same per connection; that in-order handling is exactly what
makes its ordering guarantee work.

So the gain scales with the ratio of round trip to work. Projected from the
measured ~215 us of broker work:

    loopback   (0.035 ms)    1.1x        same region (5 ms)     19.9x
    same LAN   (0.5 ms)      2.9x        cross-region (50 ms)  190.4x

Invisible here, decisive at 50 ms. This is the general shape of latency-hiding
optimisations: their value is a property of the *network*, not of the code, so
benchmarking them on localhost measures the one case where they do not matter.

Note what pipelining is NOT: it is not batching. Batching (Phase 3) puts many
*messages* in one *request*, cutting the per-message broker work as well —
which is the ~215 us that pipelining cannot touch. That is why batching, not
pipelining, is what will move the produce number. They compose, and Phase 3
uses both.
"""

import itertools
import socket

from common.config import DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SOCKET_TIMEOUT
from common.protocol import (
    HEADER_SIZE, MAGIC, MAX_REQUEST_BYTES,
    api_name, decode_body, decode_header, encode_message,
)


class BrokerConnectionError(ConnectionError):
    """
    The broker went away mid-conversation, or could not be reached.

    We define our own type rather than letting raw socket errors escape so
    callers can catch every transport failure with one clause. Phase 3's retry
    logic depends on telling "the connection broke, try again" apart from
    "the broker answered, and said no" — which is a `KafkaError`.
    """


class ProtocolError(ValueError):
    """
    Bytes arrived that are not a valid message: bad magic, absurd length,
    a correlation ID we never sent.

    Deliberately NOT a subclass of ConnectionError. A connection error is
    worth retrying; a protocol error means the two sides disagree about the
    format, and retrying reproduces it exactly.
    """


class SyncConnection:
    """
    A blocking, framed connection to one broker.

        with SyncConnection('localhost', 9092) as conn:
            reply = conn.request(API_PRODUCE, {'topic': 'orders', ...})

    "Sync" names a real property rather than leaving it to be discovered: each
    `request()` blocks for a full round trip. `pipeline()` is the escape hatch.

    **NOT THREAD-SAFE, and the failure is nasty.** A request and its response
    are a pair, and that pair is not atomic. Two threads sharing one connection
    will eventually read each other's replies, after which the stream is
    permanently desynchronised — the next read starts mid-message and
    interprets body bytes as a header, giving "Bad magic 0x7b226572"
    (`{"er`...), an error a long way from its cause.

    Callers that share a connection across threads must serialise the whole
    request/response pair themselves; `producer.Producer` holds a lock for
    exactly this reason, since its sender thread and its caller threads both
    use one socket.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 timeout: float = DEFAULT_SOCKET_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout

        # Quoted annotation: on Python 3.9 the `X | None` syntax is invalid at
        # runtime, and annotations on `self.x` targets ARE evaluated. Quoting
        # keeps the file importable on 3.9 while still documenting the type.
        self._sock: "socket.socket | None" = None

        # Correlation IDs start at 1 so that 0 is never a legitimate ID —
        # a zero in a response then reads as "uninitialised" rather than as a
        # valid match. Reserving one sentinel value costs nothing and turns a
        # silent bug into a visible one.
        self._correlation_ids = itertools.count(1)

    # ── Connection lifecycle ───────────────────────────────────

    def connect(self) -> 'SyncConnection':
        """
        Open one TCP connection and keep it for the object's lifetime.

        CONCEPT: Why persistent?
          A TCP connection costs a three-way handshake — one full round trip —
          before a single byte of payload moves. On a LAN that is ~0.5 ms;
          across regions, 50 ms+. Reconnecting per message would spend more
          time saying hello than sending data.

          The cost is a file descriptor held per client on the broker. That is
          the same trade every real server makes, and it is why "max
          connections" is a tuning knob on every broker in existence.
        """
        try:
            self._sock = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
        except OSError as exc:
            raise BrokerConnectionError(
                f"Could not connect to broker at {self.host}:{self.port}: {exc}"
            ) from exc

        # CONCEPT: Nagle's algorithm, and why we turn it off.
        #
        # By default TCP DELAYS small writes up to ~40 ms, hoping to coalesce
        # them into a bigger packet. Excellent for bulk transfer; terrible for
        # request/response, where our 80-byte request would sit in a buffer
        # waiting for company that never arrives.
        #
        # TCP_NODELAY says "send it now": slightly more packet overhead, in
        # exchange for not paying a 40 ms tax per request.
        #
        # Note that pipelining and batching are the RIGHT way to fill packets:
        # they combine messages in the application, which knows what belongs
        # together, rather than letting the kernel guess by stalling.
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return self

    def close(self) -> None:
        """Close the socket. Safe to call twice, and safe if never connected."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def __enter__(self) -> 'SyncConnection':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Request/response ───────────────────────────────────────

    def request(self, api_key: int, payload: dict, api_version: int = 0) -> dict:
        """
        Send one request, wait for its response, return the decoded body.

        Blocks for a full round trip. Verifies that the correlation ID comes
        back — see `_read_frame` for why a mismatch is fatal rather than
        merely surprising.
        """
        correlation_id = self.send_request(api_key, payload, api_version)
        header, body = self._read_frame()

        if header.correlation_id != correlation_id:
            raise ProtocolError(
                f"Correlation mismatch: sent {correlation_id}, got "
                f"{header.correlation_id}. The stream is desynchronised."
            )
        return body

    def send_request(self, api_key: int, payload: dict,
                     api_version: int = 0) -> int:
        """
        Write one request without waiting. Returns its correlation ID.

        The building block for pipelining: call this N times, then read N
        responses and match them by the IDs it returned.
        """
        if self._sock is None:
            raise BrokerConnectionError("Not connected — call connect() first.")

        correlation_id = next(self._correlation_ids)
        frame = encode_message(api_key, payload, correlation_id, api_version)

        if len(frame) - HEADER_SIZE > MAX_REQUEST_BYTES:
            # Fail here rather than letting the broker reject it: no point
            # pushing 200 MB across the network to be told no.
            raise ProtocolError(
                f"Request body is {len(frame) - HEADER_SIZE:,} bytes, over the "
                f"{MAX_REQUEST_BYTES:,} byte limit."
            )

        try:
            # sendall, not send: plain send() returns how many bytes it
            # actually queued, which is fewer than you gave it once the
            # kernel's buffer fills. Ignoring that return value is a classic
            # bug that appears only under load — the worst time to find it.
            self._sock.sendall(frame)
        except OSError as exc:
            raise BrokerConnectionError(f"Send failed: {exc}") from exc

        return correlation_id

    def pipeline(self, requests: list, api_version: int = 0) -> list:
        """
        Send many requests before reading any reply. Returns bodies in the
        order the requests were given.

        `requests` is a list of `(api_key, payload)` pairs.

        This is the whole payoff of adding a correlation ID: without one, the
        replies below would be unmatchable, and the only safe design would be
        the one-at-a-time loop that `request()` implements.

        Ordering: we do not assume replies arrive in request order. Our broker
        happens to answer in order — it handles one request per connection at
        a time — but relying on that would make the client wrong the moment a
        broker gains out-of-order handling. Matching by ID costs one dict and
        is correct either way.

        CAVEAT worth knowing: this can deadlock if you pipeline more data than
        fits in the kernel's socket buffers. We fill our send buffer while the
        broker fills its own with replies nobody is reading, and both block.
        Real clients bound in-flight requests (`max.in.flight.requests`) for
        exactly this reason; keep depth modest.
        """
        correlation_ids = [
            self.send_request(api_key, payload, api_version)
            for api_key, payload in requests
        ]

        by_id = {}
        for _ in correlation_ids:
            header, body = self._read_frame()
            by_id[header.correlation_id] = body

        missing = [cid for cid in correlation_ids if cid not in by_id]
        if missing:
            raise ProtocolError(f"No response for correlation IDs {missing}")

        return [by_id[cid] for cid in correlation_ids]

    # ── Reading ────────────────────────────────────────────────

    def _read_frame(self) -> tuple:
        """
        Read exactly one framed message. Returns (Header, body dict).

        The two-step read from the module docstring: fixed header first to
        learn the length, then exactly that many body bytes.
        """
        header = decode_header(self._recv_exactly(HEADER_SIZE))

        if header.magic != MAGIC:
            # Either we are not talking to our broker, or the stream has
            # desynchronised — we read from the middle of a message and
            # interpreted body bytes as a header. Both are unrecoverable on
            # this connection: with no delimiters there is no way to find the
            # next real message boundary.
            raise ProtocolError(
                f"Bad magic {header.magic:#x} (expected {MAGIC:#x}) — "
                f"not a broker, or the stream is out of sync."
            )

        if header.body_length > MAX_REQUEST_BYTES:
            # The same guard the broker applies to us. A length field is a
            # request to commit OUR memory, and it arrives from the network,
            # so it is validated before it is honoured — never after.
            raise ProtocolError(
                f"Response claims {header.body_length:,} bytes, over the "
                f"{MAX_REQUEST_BYTES:,} byte limit. Refusing to allocate."
            )

        body = self._recv_exactly(header.body_length)
        try:
            return header, decode_body(body)
        except ValueError as exc:      # json.JSONDecodeError subclasses this
            raise ProtocolError(
                f"{api_name(header.api_key)} response body was not valid "
                f"JSON: {exc}"
            ) from exc

    def _recv_exactly(self, n: int) -> bytes:
        """
        Read exactly `n` bytes, looping until we have them all.

        CONCEPT: recv(n) is a MAXIMUM, not a promise.
          `sock.recv(1000)` means "give me up to 1000 bytes, whatever has
          arrived". It routinely returns fewer. Code assuming otherwise works
          perfectly on localhost — where everything arrives at once — and
          fails the moment a real network is in between. This is the single
          most common bug in hand-written network code.

          A return of b'' is special: not "no data yet" but EOF. The peer has
          closed. There will never be more.

        We accumulate into a bytearray rather than `data += chunk` on bytes:
        `bytes` is immutable, so `+=` reallocates and copies the whole buffer
        every time — O(n²) for a large response. bytearray appends in place.
        """
        if n == 0:
            return b''

        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except socket.timeout as exc:
                raise BrokerConnectionError(
                    f"Timed out after {self.timeout}s waiting for {n} bytes "
                    f"(got {len(buf)})."
                ) from exc
            except OSError as exc:
                raise BrokerConnectionError(f"Receive failed: {exc}") from exc

            if not chunk:
                raise BrokerConnectionError(
                    f"Broker closed the connection after {len(buf)} of {n} bytes."
                )
            buf.extend(chunk)

        return bytes(buf)

    def __repr__(self) -> str:
        return (f"SyncConnection({self.host}:{self.port}, "
                f"{'connected' if self.connected else 'closed'})")
