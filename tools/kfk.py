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
import os
import sys

from broker.log.index import INDEX_ENTRY_SIZE
from broker.log.record import iter_records

from common.config import (
    DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_OFFSET_DIR, DEFAULT_PORT,
)
from common.errors import ERROR_NAMES, KafkaError, raise_for_error
from common.net import BrokerConnectionError, SyncConnection
from common.protocol import (
    API_CREATE_TOPICS, API_FETCH, API_LIST_OFFSETS, API_METADATA,
    TIMESTAMP_EARLIEST, TIMESTAMP_LATEST,
)


def _request(args, api_key: int, payload: dict) -> dict:
    with SyncConnection(args.host, args.port, timeout=10.0) as conn:
        return raise_for_error(conn.request(api_key, payload))


def _fetch(args, offset: int, max_messages: int) -> dict:
    return _request(args, API_FETCH, {
        'topic': args.topic,
        'partition': args.partition,
        'offset': offset,
        'max': max_messages,
    })


# ── Commands ───────────────────────────────────────────────────

def cmd_metadata(args) -> int:
    """
    Ask the broker to describe the cluster and every topic on it.

    This is the request a real Kafka client makes before it sends anything:
    it learns which broker leads which partition and connects there directly,
    with no proxy in the data path. Phase 5 makes the broker list longer than
    one; the shape of the answer does not change.
    """
    response = _request(args, API_METADATA,
                        {'topics': [args.topic] if args.topic else None})

    print("BROKERS")
    for broker in response['brokers']:
        marker = '  (controller)' if broker['node_id'] == response['controller_id'] else ''
        print(f"  {broker['node_id']}: {broker['host']}:{broker['port']}{marker}")

    print("\nTOPICS")
    if not response['topics']:
        print("  (none yet — produce something first)")

    for topic in response['topics']:
        if topic['error_code'] != 0:
            print(f"  {topic['name']}: "
                  f"{ERROR_NAMES.get(topic['error_code'], topic['error_code'])}")
            continue
        print(f"  {topic['name']}")
        print(f"    {'PART':<6}{'LEADER':<8}{'REPLICAS':<12}{'ISR':<12}"
              f"{'START':<8}{'END':<8}")
        for part in topic['partitions']:
            print(f"    {part['partition']:<6}{part['leader']:<8}"
                  f"{str(part['replicas']):<12}{str(part['isr']):<12}"
                  f"{part['log_start_offset']:<8}{part['high_watermark']:<8}")
    return 0


def cmd_topics_create(args) -> int:
    """
    Create a topic with a chosen partition count.

    Worth doing explicitly rather than relying on auto-creation, for two
    reasons: a typo'd name auto-creates silently and your messages go where
    nobody reads, and an auto-created topic gets ONE partition — so the
    convenience quietly costs you all your parallelism.
    """
    response = _request(args, API_CREATE_TOPICS,
                        {'topic': args.topic, 'partitions': args.partitions})
    print(f"Created topic {response['topic']!r} with "
          f"{response['partitions']} partition(s).")
    if args.partitions > 1:
        print("\nNote: the partition count is now fixed. Changing it would "
              "\n      remap roughly (1 - old/new) of your keys to different "
              "\n      partitions, splitting each key's history across two logs.")
    return 0


def cmd_topics_list(args) -> int:
    response = _request(args, API_METADATA, {})
    topics = response['topics']

    if not topics:
        print("No topics yet.")
        return 0

    print(f"{'TOPIC':<32}{'PARTITIONS':>11}{'MESSAGES':>11}")
    for topic in topics:
        messages = sum(p['high_watermark'] - p['log_start_offset']
                       for p in topic['partitions'])
        print(f"{topic['name']:<32}{len(topic['partitions']):>11}{messages:>11}")
    return 0


def cmd_topics_describe(args) -> int:
    """
    Show the per-partition message spread — i.e. whether load balancing worked.

    A wildly uneven spread usually means one of two things: every message
    carried the same key (all events pinned to one partition, giving you the
    throughput of a single machine), or too few messages have been sent for the
    sticky partitioner to have cycled through all of them yet.
    """
    response = _request(args, API_METADATA, {'topics': [args.topic]})
    topic = response['topics'][0]

    if topic['error_code'] != 0:
        print(f"{args.topic}: "
              f"{ERROR_NAMES.get(topic['error_code'], topic['error_code'])}",
              file=sys.stderr)
        return 1

    partitions = topic['partitions']
    counts = [p['high_watermark'] - p['log_start_offset'] for p in partitions]
    total = sum(counts)
    widest = max(counts) if counts else 0

    print(f"Topic      : {args.topic}")
    print(f"Partitions : {len(partitions)}")
    print(f"Messages   : {total:,}\n")

    print(f"  {'PART':<6}{'LEADER':<8}{'START':>9}{'END':>9}{'MESSAGES':>10}  SPREAD")
    for part, count in zip(partitions, counts):
        bar = '█' * int(count / widest * 32) if widest else ''
        print(f"  {part['partition']:<6}{part['leader']:<8}"
              f"{part['log_start_offset']:>9}{part['high_watermark']:>9}"
              f"{count:>10}  {bar}")

    if total and len(partitions) > 1:
        ideal = total / len(partitions)
        skew = (max(counts) / ideal) if ideal else 0
        print(f"\n  ideal per partition: {ideal:,.0f}    "
              f"worst partition is {skew:.2f}x that")
    return 0


def cmd_describe(args) -> int:
    """
    Ask the live broker about one partition.

    Phase 0 did this by fetching a message and reading `high_watermark` off
    the response — it worked, but it transferred data to answer a metadata
    question. LIST_OFFSETS answers both ends in constant time and zero bytes
    of payload, which is exactly why the API exists.
    """
    earliest = _request(args, API_LIST_OFFSETS, {
        'topic': args.topic, 'partition': args.partition,
        'timestamp': TIMESTAMP_EARLIEST})['offset']
    latest = _request(args, API_LIST_OFFSETS, {
        'topic': args.topic, 'partition': args.partition,
        'timestamp': TIMESTAMP_LATEST})['offset']

    count = latest - earliest

    # A partition is a DIRECTORY of segments now, not a single file, so the
    # size is the sum of its .log files.
    directory = _partition_dir(args)
    segments = []
    size = 0
    if os.path.isdir(directory):
        segments = sorted(n for n in os.listdir(directory) if n.endswith('.log'))
        size = sum(os.path.getsize(os.path.join(directory, n)) for n in segments)

    print(f"Topic     : {args.topic}")
    print(f"Partition : {args.partition}")
    print(f"Broker    : {args.host}:{args.port}")
    print(f"Offsets   : earliest={earliest}  latest={latest}  ({count} messages)")
    print(f"Directory : {directory}")
    print(f"Segments  : {len(segments)}")
    print(f"Log size  : {size:,} bytes"
          + (f"   (~{size // count} bytes/message)" if count else ""))

    if earliest > 0:
        print(f"\nNote: earliest offset is {earliest}, not 0 — retention has "
              f"\n      deleted older data. A consumer below {earliest} gets "
              f"OFFSET_OUT_OF_RANGE.")
    return 0


def cmd_tail(args) -> int:
    """Show the most recent messages, like `tail -n`."""
    latest = _request(args, API_LIST_OFFSETS, {
        'topic': args.topic, 'partition': args.partition,
        'timestamp': TIMESTAMP_LATEST})['offset']

    if latest == 0:
        print(f"{args.topic}-{args.partition} is empty.")
        return 0

    high_watermark = latest
    start = max(0, high_watermark - args.count)
    messages = _fetch(args, start, args.count).get('messages', [])

    print(f"Last {len(messages)} of {high_watermark} messages "
          f"in {args.topic}-{args.partition}:\n")
    for message in messages:
        print(f"  offset={message['offset']:<8} key={str(message['key']):<12} "
              f"ts={message['timestamp']}  {message['value']!r}")
    return 0


def _partition_dir(args) -> str:
    return os.path.join(args.data_dir, f"{args.topic}-{args.partition}")


def cmd_dump_log(args) -> int:
    """
    Read the segment files straight off disk — no broker required.

    CONCEPT: why a tool replaced `cat`.
      Through Phase 1 a log was JSON lines, so `cat` was a perfectly good
      reader. Phase 2's records carry a length prefix and a CRC32, which makes
      the file binary and `cat` useless.

      That was a real teaching benefit to give up, so it is replaced
      deliberately — and the replacement does something `cat` never could:
      **it verifies the checksums.** Real Kafka made the same trade and ships
      `kafka-dump-log.sh` for the same reason.

    Reading files directly, rather than asking the broker, matters most when
    the broker is down or wedged — which is exactly when you need to look.
    """
    directory = _partition_dir(args)
    if not os.path.isdir(directory):
        print(f"No partition directory at {directory}", file=sys.stderr)
        print("(Phase 2 stores a partition as a DIRECTORY of segments. "
              "Old flat .log files from Phase 0/1 cannot be read.)", file=sys.stderr)
        return 1

    total = 0
    offsets = []
    damaged_total = 0

    print(f"{directory}\n")
    for name in sorted(os.listdir(directory)):
        if not name.endswith('.log'):
            continue

        path = os.path.join(directory, name)
        file_size = os.path.getsize(path)
        count = 0
        valid_end = 0

        print(f"  {name}  ({file_size:,} bytes)")
        with open(path, 'rb') as fh:
            # iter_records stops at the first record that fails its length or
            # CRC check, so anything between where it stops and the end of the
            # file is damage.
            for position, size, record in iter_records(fh):
                count += 1
                valid_end = position + size
                offsets.append(record['offset'])
                if args.verbose:
                    print(f"    {record['offset']:<10} pos={position:<9} "
                          f"key={str(record['key']):<10} {record['value']!r}")

        damaged = file_size - valid_end
        damaged_total += damaged
        note = f"   ⚠ {damaged:,} DAMAGED BYTES at the tail" if damaged else ""
        print(f"    {count} records{note}")

        index_path = path[:-4] + '.index'
        if os.path.exists(index_path):
            entries = os.path.getsize(index_path) // INDEX_ENTRY_SIZE
            ratio = (os.path.getsize(index_path) / file_size * 100) if file_size else 0
            print(f"    index: {entries} entries ({ratio:.2f}% of the log)")
        print()

    print(f"  records        : {total or len(offsets)}")
    print(f"  damaged bytes  : {damaged_total:,}")

    if offsets:
        expected = list(range(offsets[0], offsets[0] + len(offsets)))
        if offsets != expected:
            # Offsets are dense today. Compaction (not yet implemented) would
            # legitimately create gaps, and this check will need to learn the
            # difference when it arrives.
            print("  ⚠ offsets are NOT dense — gaps or duplicates present")
        else:
            print(f"  offsets        : {offsets[0]}..{offsets[-1]} (dense ✓)")

    if damaged_total:
        print("\n  Damage is repaired the next time the broker opens this "
              "\n  partition: recovery truncates back to the last valid record.")
    return 0


def cmd_segments(args) -> int:
    """
    List the segments of a partition, with sizes and index density.

    The clearest way to see that a log is many files, not one — and to check
    that the sparse index really is sparse. If the index is a large fraction of
    the log, `index_interval_bytes` is too small; if there is one entry for a
    huge segment, it is too large and reads are scanning far more than they
    should.
    """
    directory = _partition_dir(args)
    if not os.path.isdir(directory):
        print(f"No partition directory at {directory}", file=sys.stderr)
        return 1

    names = sorted(n for n in os.listdir(directory) if n.endswith('.log'))
    if not names:
        print(f"{directory} has no segments.")
        return 0

    print(f"{args.topic}-{args.partition}   ({len(names)} segment(s))\n")
    print(f"  {'BASE OFFSET':>14}  {'LOG BYTES':>12}  {'INDEX ENTRIES':>14}  "
          f"{'INDEX %':>8}  STATE")

    total_log = total_index = 0
    for i, name in enumerate(names):
        base_offset = int(name[:20])
        log_size = os.path.getsize(os.path.join(directory, name))
        index_path = os.path.join(directory, name[:-4] + '.index')
        index_size = os.path.getsize(index_path) if os.path.exists(index_path) else 0
        entries = index_size // INDEX_ENTRY_SIZE
        ratio = (index_size / log_size * 100) if log_size else 0
        state = 'ACTIVE' if i == len(names) - 1 else 'sealed'

        print(f"  {base_offset:>14}  {log_size:>12,}  {entries:>14}  "
              f"{ratio:>7.2f}%  {state}")
        total_log += log_size
        total_index += index_size

    print(f"\n  total log   : {total_log:,} bytes")
    print(f"  total index : {total_index:,} bytes "
          f"({total_index / total_log * 100:.2f}% overhead)"
          if total_log else "")
    print("\n  Only the ACTIVE segment takes writes. Sealed segments are "
          "immutable,\n  which is why startup only has to scan one file and why "
          "retention\n  can delete whole files instead of rewriting them.")
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

    # `kfk topics <verb>` — a nested subcommand group, matching real Kafka's
    # kafka-topics.sh. Kept separate from `describe`, which is about ONE
    # partition's storage rather than a topic's shape.
    topics = sub.add_parser('topics', help='create, list and describe topics')
    topics_sub = topics.add_subparsers(dest='topics_command', required=True)

    t = topics_sub.add_parser('create', help='create a topic')
    t.add_argument('--topic', required=True)
    t.add_argument('--partitions', type=int, default=1)
    t.set_defaults(func=cmd_topics_create)

    t = topics_sub.add_parser('list', help='list all topics')
    t.set_defaults(func=cmd_topics_list)

    t = topics_sub.add_parser('describe', help='per-partition message spread')
    t.add_argument('--topic', required=True)
    t.set_defaults(func=cmd_topics_describe)

    p = sub.add_parser('metadata', help='cluster + topic overview (asks the broker)')
    p.add_argument('--topic', default=None, help='limit to one topic')
    p.set_defaults(func=cmd_metadata)

    p = sub.add_parser('describe', help='partition summary (asks the broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.set_defaults(func=cmd_describe)

    p = sub.add_parser('tail', help='show the newest messages (asks the broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.add_argument('--count', type=int, default=10)
    p.set_defaults(func=cmd_tail)

    p = sub.add_parser('dump-log',
                       help='read segments directly and verify CRCs (no broker)')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.add_argument('--verbose', '-v', action='store_true', help='print every record')
    p.set_defaults(func=cmd_dump_log)

    p = sub.add_parser('segments', help='list a partition\'s segments and index density')
    p.add_argument('--topic', required=True)
    p.add_argument('--partition', type=int, default=0)
    p.set_defaults(func=cmd_segments)

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
    except KafkaError as exc:
        # The broker answered and said no. Report it as a plain message rather
        # than a traceback: a CLI user asked a question and got an answer, and
        # a stack trace would imply the tool malfunctioned.
        print(f"{exc}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
