"""
CONCEPT: Framed Request/Response Over a TCP Stream
---------------------------------------------------
This module holds the *client-side transport*: the code that turns a Python
dict into bytes on a socket and a socket back into a Python dict.

It exists because `producer.py` and `consumer.py` each contained a
byte-identical copy of the same twenty lines. Duplicated protocol code is how
two components quietly drift apart and then disagree about the wire format.
Now there is one copy, and everything that talks to a broker uses it: the
producer, the consumer, the admin CLI, the benchmark, and (in Phase 6) the
replication fetcher.


THE PROBLEM THIS SOLVES: TCP has no message boundaries
-------------------------------------------------------
This is the single most misunderstood thing about network programming, so it
is worth being precise.

TCP guarantees your bytes arrive, and arrive in order. It guarantees NOTHING
about how they are grouped. If you call:

    sock.sendall(b"HELLO")
    sock.sendall(b"WORLD")

the receiver may observe any of these:

    recv() -> b"HELLOWORLD"     (both messages glued together)
    recv() -> b"HEL"            (one message split across reads)
    recv() -> b"LOWOR"
    recv() -> b"LD"
    recv() -> b"HELLO"          (what you naively expected)

TCP is a *byte stream*, like a file. "Where does one message end?" is a
question TCP does not answer — so every protocol built on TCP must answer it
itself. That is called **framing**, and there are three common ways:

    ┌────────────────────┬──────────────────────────────────────────────────┐
    │ Framing strategy   │ Trade-off                                        │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Delimiter          │ Simple (readline()). But you must escape the     │
    │ (e.g. newline)     │ delimiter if it can appear in the payload, and   │
    │                    │ you cannot pre-allocate the buffer. Breaks on    │
    │                    │ binary data. This is how HTTP/1.1 headers work.  │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Length prefix      │ Read a fixed-size header, learn the body length, │
    │  ← WE USE THIS     │ read exactly that many bytes. Binary-safe, no    │
    │                    │ escaping, and the receiver can allocate the      │
    │                    │ right buffer up front. Kafka, gRPC, and every    │
    │                    │ serious binary protocol do this.                 │
    ├────────────────────┼──────────────────────────────────────────────────┤
    │ Close-to-signal-   │ "Read until the peer hangs up." Works only for   │
    │ end                │ one message per connection. HTTP/1.0 did this,   │
    │                    │ and it is why HTTP/1.0 was slow.                 │
    └────────────────────┴──────────────────────────────────────────────────┘

Our header (see `common/protocol.py`) is 12 fixed bytes, of which four are the
body length. So every read is a two-step dance:

        ┌──────────────────────────────────────────────────────┐
        │ 1. read EXACTLY 12 bytes    → the header             │
        │ 2. unpack it                → body_length = 348      │
        │ 3. read EXACTLY 348 bytes   → the body               │
        │ 4. json.loads               → a dict                 │
        └──────────────────────────────────────────────────────┘

"read EXACTLY n bytes" is `_recv_exactly()` below. It is the whole point of
this file.


A NOTE ON WHAT THIS CLASS DOES NOT DO
--------------------------------------
`request()` sends one request and waits for one response — strictly
synchronous, one request in flight at a time. That is a real limitation: the
connection sits idle for the entire network round trip.

We cannot fix it yet, because the current protocol has no way to tell which
response belongs to which request. Phase 1 adds a correlation ID to the
header, which is what makes pipelining (several requests in flight, responses
matched up by ID) possible. The limitation is deliberate and temporary.
"""

import socket

from common.protocol import (
    HEADER_SIZE, MAGIC,
    encode_message, decode_header, decode_body,
)
from common.config import DEFAULT_HOST, DEFAULT_PORT, DEFAULT_SOCKET_TIMEOUT


class BrokerConnectionError(ConnectionError):
    """
    Raised when the broker goes away mid-conversation, or speaks nonsense.

    We define our own type (rather than letting the raw socket errors escape)
    so that callers can write `except BrokerConnectionError` and catch every
    transport failure with one clause. Phase 3's producer retry logic depends
    on being able to distinguish "the connection broke, try again" from
    "the broker answered, and said no".
    """


class ProtocolError(ValueError):
    """
    Raised when bytes arrive that are not a valid message — a bad magic
    number, a malformed body.

    Distinct from BrokerConnectionError on purpose: a connection error is
    worth retrying, a protocol error means the two sides disagree about the
    format and retrying will fail exactly the same way.
    """


class SyncConnection:
    """
    A blocking, framed connection to one broker.

    Usage:
        with SyncConnection('localhost', 9092) as conn:
            reply = conn.request(MSG_PRODUCE, {'topic': 'orders', ...})

    "Sync" is in the name because a Phase 3 `AsyncConnection` will sit
    alongside it, and because the synchronous behaviour is a property worth
    naming rather than discovering.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 timeout: float = DEFAULT_SOCKET_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        # Quoted annotation: on Python 3.9 the `X | None` syntax is not valid
        # at runtime, and annotations on `self.x` targets ARE evaluated. The
        # quotes make it a string that nothing evaluates, so the file stays
        # importable on 3.9 while still documenting the type.
        self._sock: "socket.socket | None" = None

    # ── Connection lifecycle ───────────────────────────────────

    def connect(self) -> 'SyncConnection':
        """
        Open one TCP connection and keep it for the object's lifetime.

        CONCEPT: Why a persistent connection?
          A TCP connection costs a three-way handshake — one full network
          round trip — before a single byte of your data moves. On a LAN
          that is ~0.5 ms; across regions it is 50 ms+.

          If you open a new connection per message, that handshake dominates
          everything: you would spend more time saying hello than sending
          data. So we connect once and reuse.

          The cost is that the broker holds an open file descriptor per
          client. That is the trade real servers make too, and it is why
          "max connections" is a tuning knob on every broker on earth.
        """
        try:
            self._sock = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
        except OSError as exc:
            raise BrokerConnectionError(
                f"Could not connect to broker at {self.host}:{self.port}: {exc}"
            ) from exc

        # CONCEPT: Nagle's algorithm and why we turn it off.
        #
        # By default TCP *delays* small writes for up to ~40 ms, hoping to
        # coalesce them into one bigger packet. That is a fine default for
        # bulk transfer and a terrible one for request/response: our 60-byte
        # request would sit in a buffer waiting for company that never comes,
        # adding tens of milliseconds to every single call.
        #
        # TCP_NODELAY says "send it now". We accept slightly more packet
        # overhead in exchange for not paying a 40 ms tax per request.
        # Phase 3's batching is the *right* way to get big packets: batch in
        # the application, where we know what belongs together, rather than
        # letting the kernel guess.
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return self

    def close(self) -> None:
        """Close the socket. Safe to call twice, and safe if never connected."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass          # already dead; nothing useful left to do
            self._sock = None

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def __enter__(self) -> 'SyncConnection':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── The one operation ──────────────────────────────────────

    def request(self, msg_type: int, payload: dict) -> dict:
        """
        Send one request, wait for its response, return the decoded body.

        This is synchronous: the calling thread blocks for a full network
        round trip. See the module docstring for why we cannot do better
        until Phase 1 adds correlation IDs.
        """
        self.send(msg_type, payload)
        return self.recv()

    def send(self, msg_type: int, payload: dict) -> None:
        """
        Write one framed message to the socket.

        `sendall` (not `send`) is required: plain `send()` returns the number
        of bytes it actually queued, which may be fewer than you gave it when
        the kernel's send buffer is full. `sendall` loops for you. Using
        `send()` and ignoring the return value is a classic bug that only
        appears under load, which is the worst time to find it.
        """
        if self._sock is None:
            raise BrokerConnectionError("Not connected — call connect() first.")

        try:
            self._sock.sendall(encode_message(msg_type, payload))
        except OSError as exc:
            raise BrokerConnectionError(f"Send failed: {exc}") from exc

    def recv(self) -> dict:
        """
        Read exactly one framed message and return its decoded body.

        The two-step read described in the module docstring: fixed header
        first to learn the length, then exactly that many body bytes.
        """
        header = self._recv_exactly(HEADER_SIZE)
        magic, body_length, _msg_type = decode_header(header)

        if magic != MAGIC:
            # Either we are talking to something that is not our broker, or
            # the stream has desynchronised — we read from the middle of a
            # message and interpreted body bytes as a header. Both are
            # unrecoverable on this connection: there is no way to find the
            # next real message boundary in a stream with no delimiters.
            raise ProtocolError(
                f"Bad magic {magic:#x} (expected {MAGIC:#x}) — "
                f"not a broker, or the stream is out of sync."
            )

        body = self._recv_exactly(body_length)
        try:
            return decode_body(body)
        except ValueError as exc:      # json.JSONDecodeError subclasses this
            raise ProtocolError(f"Response body was not valid JSON: {exc}") from exc

    # ── The primitive everything rests on ──────────────────────

    def _recv_exactly(self, n: int) -> bytes:
        """
        Read exactly `n` bytes, looping until we have them all.

        CONCEPT: recv(n) is a MAXIMUM, not a promise.
          `sock.recv(1000)` means "give me up to 1000 bytes, whatever has
          arrived". It routinely returns fewer. Code that assumes otherwise
          works perfectly on localhost — where everything arrives at once —
          and fails the moment there is a real network in between. This is
          the single most common bug in hand-written network code.

          A return of b'' is special: it is not "no data yet", it is EOF —
          the peer has closed its end. There will never be more data.

        We accumulate into a bytearray rather than doing `data += chunk` on
        a bytes object: `bytes` is immutable, so `+=` allocates and copies
        the whole accumulated buffer every time. For a large fetch response
        that is O(n²) copying. bytearray appends in place.
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
        state = 'connected' if self.connected else 'closed'
        return f"SyncConnection({self.host}:{self.port}, {state})"
