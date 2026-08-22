"""
kfk — the admin CLI.

    python -m tools.kfk describe --topic orders
    python -m tools.kfk tail     --topic orders --count 5
    python -m tools.kfk dump-log --topic orders
    python -m tools.kfk offsets

CONCEPT: A system you cannot inspect is a system you cannot debug.
--------------------------------------------------------------------
Everything here is also reachable from Python, but that is not the point. When
something is wrong at 2am you want to ask the running system a question
without writing a program first. Real Kafka ships a dozen of these
(kafka-topics.sh, kafka-consumer-groups.sh, kafka-dump-log.sh) and operators
live in them.

There are two ways to inspect a broker, and this tool uses both deliberately:

  OVER THE NETWORK (`describe`, `tail`)
      Ask the running broker. Shows what CLIENTS see, which is the thing that
      actually matters. Requires the broker to be alive and healthy.

  DIRECTLY OFF DISK (`dump-log`, `offsets`)
      Read the files ourselves. Works when the broker is down or wedged — the
      exact moments when you most need to look — and shows the ground truth
      rather than the broker's opinion of it. This is only possible because
      the log is a plain file. It is a real argument for boring formats.

This tool grows every phase: `kfk topics` (Phase 3), `kfk groups` with lag
(Phase 4), `kfk cluster status` (Phase 5), `kfk partitions` with ISR (Phase 6).
"""

import argparse
import json
import os
import sys

from common.config import (
    DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_OFFSET_DIR, DEFAULT_PORT,
)
from common.net import BrokerConnectionError, SyncConnection
from common.protocol import MSG_FETCH


def _fetch(args, offset: int, max_messages: int) -> dict:
    with SyncConnection(args.host, args.port, timeout=10.0) as conn:
        return conn.request(MSG_FETCH, {
            'topic': args.topic,
            'partition': args.partition,
            'offset': offset,
            'max': max_messages,
        })


# ── Commands ───────────────────────────────────────────────────

def cmd_describe(args) -> int:
    """Ask the live broker about a partition."""
    response = _fetch(args, offset=0, max_messages=1)

    high_watermark = response.get('high_watermark', 0)
    log_path = os.path.join(args.data_dir, f"{args.topic}-{args.partition}.log")
    size = os.path.getsize(log_path) if os.path.exists(log_path) else 0

    print(f"Topic     : {args.topic}")
    print(f"Partition : {args.partition}")
    print(f"Broker    : {args.host}:{args.port}")
    print(f"Messages  : {high_watermark}   (offsets 0..{max(high_watermark - 1, 0)})")
    print(f"Log file  : {log_path}")
    print(f"Log size  : {size:,} bytes"
          + (f"   (~{size // high_watermark} bytes/message)" if high_watermark else ""))

    if high_watermark == 0:
        print("\nNote: this partition is empty. Because auto-creation is on, a "
              "\n      typo'd topic name looks exactly like this. Check the spelling.")
    return 0


def cmd_tail(args) -> int:
    """Show the most recent messages, like `tail -n`."""
    high_watermark = _fetch(args, 0, 1).get('high_watermark', 0)
    if high_watermark == 0:
        print(f"{args.topic}-{args.partition} is empty.")
        return 0

    start = max(0, high_watermark - args.count)
    messages = _fetch(args, start, args.count).get('messages', [])

    print(f"Last {len(messages)} of {high_watermark} messages "
          f"in {args.topic}-{args.partition}:\n")
    for message in messages:
        print(f"  offset={message['offset']:<8} key={str(message['key']):<12} "
              f"ts={message['timestamp']}  {message['value']!r}")
    return 0


def cmd_dump_log(args) -> int:
    """
    Read the log file straight off disk — no broker required.

    Also reports unparseable lines. A non-zero corrupt count means the log has
    been damaged (a torn write, a crash mid-flush) and the broker has been
    silently skipping those records. Silence is exactly the problem.
    """
    log_path = os.path.join(args.data_dir, f"{args.topic}-{args.partition}.log")
    if not os.path.exists(log_path):
        print(f"No log file at {log_path}", file=sys.stderr)
        return 1

    total = corrupt = 0
    offsets = []

    print(f"{log_path}\n")
    with open(log_path, 'r', encoding='utf-8', newline='\n') as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                record = json.loads(line)
                offsets.append(record['offset'])
                if args.verbose:
                    print(f"  {record['offset']:<8} {record['value']!r}")
            except (json.JSONDecodeError, KeyError):
                corrupt += 1
                print(f"  line {line_number}: CORRUPT — {line[:60]!r}")

    print(f"\n  records        : {total}")
    print(f"  corrupt lines  : {corrupt}")
    print(f"  bytes          : {os.path.getsize(log_path):,}")

    if offsets:
        expected = list(range(offsets[0], offsets[0] + len(offsets)))
        if offsets != expected:
            # Today this can only mean corruption — offsets are dense. From
            # Phase 2 on, log compaction legitimately creates gaps, and this
            # message will need to learn the difference.
            print("  ⚠ offsets are NOT dense — gaps or duplicates present")
        else:
            print(f"  offsets        : {offsets[0]}..{offsets[-1]} (dense ✓)")
    return 0


def cmd_offsets(args) -> int:
    """
    List every committed consumer offset on this machine.

    These are local files only, because Phase 0 consumers keep their position
    on their own disk. That is a real limitation: the position lives with the
    machine, not with the cluster, so moving a consumer to a new host loses
    it. Phase 4 moves offsets into the broker where they belong.
    """
    if not os.path.isdir(args.offset_dir):
        print(f"No offset directory at {args.offset_dir}")
        return 0

    entries = sorted(f for f in os.listdir(args.offset_dir) if f.endswith('.txt'))
    if not entries:
        print(f"No committed offsets in {args.offset_dir}")
        return 0

    print(f"{'TOPIC-PARTITION-GROUP':<40} {'COMMITTED':>10}")
    for name in entries:
        try:
            with open(os.path.join(args.offset_dir, name), encoding='utf-8') as f:
                value = f.read().strip() or '(empty!)'
        except OSError as exc:
            value = f'(unreadable: {exc})'
        print(f"{name[:-4]:<40} {value:>10}")

    print("\nNote: these are LOCAL files. A consumer moved to another machine "
          "\n      starts from scratch. Phase 4 fixes that.")
    return 0


# ── Entry point ────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        prog='kfk',
        description='Kafka-clone admin CLI  (run with: python -m tools.kfk)')
    parser.add_argument('--host', default=DEFAULT_HOST)
    parser.add_argument('--port', type=int, default=DEFAULT_PORT)
    parser.add_argument('--data-dir', default=DEFAULT_DATA_DIR)
    parser.add_argument('--offset-dir', default=DEFAULT_OFFSET_DIR)

    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('describe', help='partition summary (asks the broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.set_defaults(func=cmd_describe)

    p = sub.add_parser('tail', help='show the newest messages (asks the broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.add_argument('--count', type=int, default=10)
    p.set_defaults(func=cmd_tail)

    p = sub.add_parser('dump-log', help='read the log file directly (no broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.add_argument('--verbose', '-v', action='store_true', help='print every record')
    p.set_defaults(func=cmd_dump_log)

    p = sub.add_parser('offsets', help='list local committed consumer offsets')
    p.set_defaults(func=cmd_offsets)

    args = parser.parse_args()

    try:
        return args.func(args)
    except BrokerConnectionError as exc:
        print(f"Cannot reach the broker at {args.host}:{args.port}: {exc}",
              file=sys.stderr)
        print("Is it running?  python -m broker.broker", file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
