"""
The storage engine: a segmented, indexed, checksummed append-only log.

    record.py    one record on disk: length + CRC32 + JSON payload
    index.py     sparse offset → byte position index
    segment.py   one .log file and its .index
    log.py       the ordered series of segments for one partition

Read them in that order. Each layer only knows about the one above it, and
none of them know anything about topics, brokers, or the network — which is
what let the whole storage format be replaced in Phase 2 without touching
broker.py.
"""

from broker.log.log import Log, LogConfig
from broker.log.record import CorruptRecordError
from broker.log.segment import Segment

__all__ = ['Log', 'LogConfig', 'Segment', 'CorruptRecordError']
