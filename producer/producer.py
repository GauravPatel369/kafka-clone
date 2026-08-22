"""
CONCEPT: The Producer — the Write Side
----------------------------------------
The producer creates messages and sends them to a broker.

    producer.send("order placed", key="user-42")
        → encode  → TCP → broker → append to log → offset 41
        ← offset 41 ←──────────────────────────────────────┘

In Phase 0 this is as simple as it gets: one connection, one message per
request, wait for the acknowledgement before sending the next.


THE THING TO NOTICE: THIS DESIGN IS SLOW, AND THAT IS THE LESSON
------------------------------------------------------------------
Every `send()` costs a full network round trip, during which the producer
does nothing at all:

    send msg 1 ──────► broker
               ◄────── ack           (wait 0.5 ms)
    send msg 2 ──────► broker
               ◄────── ack           (wait 0.5 ms)

At 0.5 ms per round trip that caps you at ~2,000 messages/second no matter
how fast the broker is. The broker is idle most of that time; so is the
network; so is the producer. The bottleneck is pure latency.

Real producers fix this two ways, and Phase 3 implements both:

  1. **Batching** — accumulate messages in memory for a few milliseconds
     (`linger.ms`) and send them as one request. 100 messages in one round
     trip instead of 100 round trips. Costs a little latency, multiplies
     throughput by ~50x.
  2. **Pipelining** — send the next request without waiting for the previous
     acknowledgement, matching replies to requests by correlation ID
     (Phase 1's protocol change is what makes this possible).

Both are the same insight: **the round trip is the expensive part, so amortise
it.** Keep the Phase 0 number from `bench/bench.py` — the improvement in
Phase 3 is one of the most dramatic in the project.


WHAT THE PRODUCER DECIDES
--------------------------
The producer, not the broker, chooses which partition a message goes to. That
sounds like a detail; it is actually the load-balancing mechanism of the whole
system, and it is what Phase 3 is about. `key` is accepted and stored today
but not yet used for routing — every message goes to partition 0.
"""

import argparse

from common.config import DEFAULT_HOST, DEFAULT_PORT
from common.net import SyncConnection
from common.protocol import MSG_PRODUCE


class Producer:
    """
    A synchronous producer bound to one topic and one broker.

    Use as a context manager so the connection is always closed:

        with Producer(topic='orders') as p:
            p.send('hello')
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 topic: str = 'default', quiet: bool = False):
        self.host = host
        self.port = port
        self.topic = topic

        # `quiet` exists for the benchmark. Printing a line per message costs
        # more than the message does — without this, bench/bench.py would be
        # measuring the speed of your terminal. Any measurement harness needs
        # a way to silence the thing it is measuring.
        self.quiet = quiet

        self._conn = SyncConnection(host, port)

    # ── Connection lifecycle ───────────────────────────────────

    def connect(self) -> 'Producer':
        self._conn.connect()
        self._log(f"Connected to broker at {self.host}:{self.port}")
        return self

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> 'Producer':
        return self.connect()

    def __exit__(self, *exc_info) -> None:
        self.close()

    # ── Sending ────────────────────────────────────────────────

    def send(self, value: str, key: str = None, partition: int = 0) -> int:
        """
        Send one message; block until the broker acknowledges; return the
        offset it was assigned.

        Args:
            value:     the message body.
            key:       routing key. Stored today, used for partition routing
                       from Phase 3 — messages with the same key will land in
                       the same partition, which is how per-key ordering is
                       guaranteed.
            partition: explicit target partition. Always 0 until Phase 3.

        CONCEPT: The returned offset is a receipt.
          Getting an offset back means the broker has the message in its log.
          It does NOT yet mean the message is safe — it is in the OS page
          cache, not on a platter, and there is only one copy of it.

          Phase 3 makes that explicit with `acks` (0 = do not wait, 1 = leader
          has it, all = every replica has it) and Phase 6 makes `acks=all`
          meaningful by adding replicas. Right now every send is effectively
          `acks=1` against a single copy.
        """
        response = self._conn.request(MSG_PRODUCE, {
            'topic':     self.topic,
            'partition': partition,
            'value':     value,
            'key':       key,
        })

        if response.get('status') != 'ok':
            # Phase 1 replaces this string with a numeric error code, which
            # is what lets a client decide *programmatically* whether an
            # error is worth retrying. Matching on error prose is not a plan.
            raise RuntimeError(
                f"Broker rejected the message: {response.get('reason', response)}")

        offset = response['offset']
        self._log(f"SENT value={value!r} key={key!r} → "
                  f"{self.topic}-{partition} offset={offset}")
        return offset

    def send_many(self, values, key: str = None, partition: int = 0) -> list:
        """
        Convenience wrapper: send an iterable of values, return their offsets.

        Note this is NOT batching — it is a loop of individual round trips,
        with all the cost described in the module docstring. Real batching
        arrives in Phase 3, and the benchmark difference between this method
        and that one is the point.
        """
        return [self.send(v, key=key, partition=partition) for v in values]

    # ── Internals ──────────────────────────────────────────────

    def _log(self, message: str) -> None:
        if not self.quiet:
            print(f"[Producer] {message}")

    def __repr__(self) -> str:
        return f"Producer(topic={self.topic!r}, broker={self.host}:{self.port})"


# ── Entry point ────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Kafka-clone producer  (run with: python -m producer.producer)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--topic', default='orders')
    parser.add_argument('--count', type=int, default=5,
                        help='How many test messages to send')
    parser.add_argument('--prefix', default='message',
                        help='Text prefix for generated messages')
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    with Producer(args.host, args.port, args.topic, quiet=args.quiet) as producer:
        for i in range(args.count):
            producer.send(f"{args.prefix}-{i}", key=f"key-{i % 3}")

    print(f"\nDone. Sent {args.count} messages to topic {args.topic!r}.")


if __name__ == '__main__':
    main()
