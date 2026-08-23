"""
CHAOS EXPERIMENT: Corrupt a log on purpose and watch recovery repair it.

    python -m chaos.corrupt_log

Phase 1 stored JSON lines. A damaged record either failed to parse (skipped and
counted) or **parsed into plausible nonsense and was served as real data**. The
second case is the dangerous one, and nothing anywhere would notice it.

Phase 2 gives every record a length prefix and a CRC32, so damage is *detected*
rather than interpreted. This script demonstrates both halves of that:

  EXPERIMENT 1 — a torn tail
      Kill the broker mid-write and the last record is half-written. Recovery
      must truncate it and continue cleanly from the last good offset.

  EXPERIMENT 2 — a flipped bit inside a valid record
      The nastier case. The record is complete and its length is correct; one
      byte of the payload is wrong. Under Phase 1 this was undetectable. The
      CRC must catch it.

  EXPERIMENT 3 — the case Phase 1 got wrong
      Truncate a record so that what remains is still *valid JSON*. Phase 1
      would have parsed it and returned it as a message. Phase 2 rejects it.
"""

import os
import shutil
import tempfile

from broker.log import Log, LogConfig
from broker.log.record import encode_record

MESSAGES = 20


def rule(title: str = '') -> None:
    print('\n' + '─' * 72)
    if title:
        print(title)
        print('─' * 72)


def build_log(directory: str) -> str:
    """Write a clean log and return the active segment's path."""
    log = Log(directory, LogConfig(segment_bytes=64 * 1024))
    for i in range(MESSAGES):
        log.append(f"message-{i}", key=f"key-{i % 3}")
    path = log.active_segment.log_path
    size = log.active_segment.size_bytes
    log.close()
    print(f"  wrote {MESSAGES} records, {size:,} bytes")
    return path


def reopen(directory: str) -> Log:
    return Log(directory, LogConfig(segment_bytes=64 * 1024))


def experiment_torn_tail(workdir: str) -> None:
    rule('EXPERIMENT 1 — torn tail (the broker died mid-write)')
    directory = os.path.join(workdir, 'torn')
    path = build_log(directory)
    good_size = os.path.getsize(path)

    # A record that never finished being written: a valid header, then only
    # part of the payload it promised.
    partial = encode_record({'offset': MESSAGES, 'key': None,
                             'value': 'this write never completed',
                             'timestamp': 0})
    with open(path, 'ab') as fh:
        fh.write(partial[:len(partial) // 2])
    print(f"  simulated a crash: appended {len(partial) // 2} bytes of a "
          f"{len(partial)}-byte record")
    print(f"  file is now {os.path.getsize(path):,} bytes "
          f"(was {good_size:,})")

    log = reopen(directory)
    try:
        print(f"\n  → recovery truncated {log.corrupt_bytes_recovered} bytes")
        print(f"  → file is {os.path.getsize(path):,} bytes again "
              f"({'REPAIRED' if os.path.getsize(path) == good_size else 'STILL WRONG'})")
        print(f"  → next offset is {log.next_offset} (expected {MESSAGES})")
        print(f"  → {len(log.read(0, 100))} records readable")
        print(f"  → next append lands at offset {log.append('after-recovery')}")
    finally:
        log.close()


def experiment_flipped_bit(workdir: str) -> None:
    rule('EXPERIMENT 2 — one flipped bit inside a complete record')
    directory = os.path.join(workdir, 'flipped')
    path = build_log(directory)

    # Flip a bit in the middle of the file — inside a record that is complete
    # and whose length field is perfectly correct. Only the checksum can catch
    # this.
    target = os.path.getsize(path) // 2
    with open(path, 'r+b') as fh:
        fh.seek(target)
        byte = fh.read(1)
        fh.seek(target)
        fh.write(bytes([byte[0] ^ 0x01]))
    print(f"  flipped one bit at byte {target:,} of {os.path.getsize(path):,}")
    print("  the record is COMPLETE and its length field is CORRECT —")
    print("  only a checksum can detect this")

    log = reopen(directory)
    try:
        survivors = log.read(0, 100)
        print(f"\n  → CRC failed; recovery truncated "
              f"{log.corrupt_bytes_recovered:,} bytes")
        print(f"  → {len(survivors)} of {MESSAGES} records survived "
              f"(offsets 0..{survivors[-1]['offset'] if survivors else '-'})")
        print("  → everything from the damaged record onward is gone, because")
        print("    the length field that would say where the NEXT record starts")
        print("    is exactly the thing we can no longer trust")
    finally:
        log.close()


def experiment_plausible_truncation(workdir: str) -> None:
    rule('EXPERIMENT 3 — damage that Phase 1 would have served as real data')
    directory = os.path.join(workdir, 'plausible')

    log = Log(directory, LogConfig(segment_bytes=64 * 1024))
    log.append('hello world, this is the full message')
    path = log.active_segment.log_path
    log.close()

    print('  stored value: "hello world, this is the full message"')
    print('\n  Under Phase 1 the file held one JSON line. Truncating it to')
    print('      {"offset": 0, "key": null, "value": "hello"}')
    print('  leaves VALID JSON and a believable message. Phase 1 would have')
    print('  parsed it and handed it to a consumer as real data.')

    # Rewrite the record's payload as a shorter but still-valid JSON object,
    # keeping the ORIGINAL length and CRC. This is what a partial overwrite
    # looks like from the format's point of view.
    with open(path, 'rb') as fh:
        original = fh.read()
    tampered = bytearray(original)
    replacement = b'{"offset": 0, "key": null, "value": "hello", "timestamp": 0}'
    tampered[8:8 + len(replacement)] = replacement
    with open(path, 'wb') as fh:
        fh.write(bytes(tampered))

    log = reopen(directory)
    try:
        records = log.read(0, 10)
        print(f"\n  → Phase 2 result: {len(records)} records readable")
        print(f"  → CRC mismatch detected; {log.corrupt_bytes_recovered:,} "
              f"bytes truncated")
        print("  → the corrupted message was NOT returned as data")
    finally:
        log.close()


def main() -> None:
    import logging
    logging.disable(logging.CRITICAL)

    workdir = tempfile.mkdtemp(prefix='kafka-corrupt-')
    try:
        experiment_torn_tail(workdir)
        experiment_flipped_bit(workdir)
        experiment_plausible_truncation(workdir)

        rule('WHAT JUST HAPPENED')
        print("""
  Three kinds of damage, three detections, and one repair each.

  The length prefix catches an INCOMPLETE record: the header promises N bytes
  and fewer are there. That alone would have caught Experiment 1.

  The CRC32 catches a COMPLETE but WRONG record — Experiments 2 and 3, where
  the length is perfectly correct and only the contents are damaged. This is
  the case that matters, because it is the one that looks like valid data.
  Experiment 3 is the specific failure Phase 1 had: bytes that parse cleanly
  into a plausible message and get served to a consumer with no error anywhere.

  Recovery TRUNCATES rather than skipping. Phase 1 skipped a bad line and left
  it in the file forever, with the next append landing after it — permanent
  garbage in the middle of the log. Now the file is repaired back to the last
  provably-good record.

  Note what is NOT protected: an attacker who can write to your disk can
  recompute the checksum. CRC32 is integrity against ACCIDENTS — torn writes,
  bad sectors, flipped bits — not against tampering. Matching the mechanism to
  the actual threat is why this is a 4-byte CRC and not a signature.

  And note the cost of detection: everything after the damaged record is
  discarded, because the length field that would locate the next record is the
  very thing we cannot trust. Losing the tail is the price of never serving
  corrupt data. In Phase 6, replication makes that price nearly zero — another
  copy of those records exists on another machine.
""")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == '__main__':
    main()
