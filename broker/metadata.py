"""
CONCEPT: Topic Metadata — What Exists, and How Wide It Is
------------------------------------------------------------
Through Phase 2 a "topic" was nothing but a directory name. Producing to
`orders` created `data/orders-0/` and that was the whole of it: one partition,
no configuration, no record that the topic was ever meant to exist.

That works until you want a topic to have FOUR partitions. Then somebody has to
know the number — before any of them contain data — because the producer needs
it to compute `hash(key) % num_partitions`. A partition count you infer from
which directories happen to exist is not a partition count; it is a guess that
changes as data arrives.

So topics become real objects with real configuration, persisted to
`data/_metadata.json`:

    {
      "version": 1,
      "topics": {
        "orders":   {"partitions": 4, "replication_factor": 1, "created_at": ...},
        "payments": {"partitions": 1, "replication_factor": 1, "created_at": ...}
      }
    }


WHY AUTO-CREATION BECOMES A FLAG
----------------------------------
Phases 0-2 created a topic on first use. Convenient in a demo, and two distinct
hazards in production:

  1. A typo silently succeeds. Producing to "oders" creates a new topic, and
     your messages go somewhere nobody is reading. No error, ever.
  2. An auto-created topic gets ONE partition. So the convenience silently
     costs you all of your parallelism — the thing this phase exists to add.

Real Kafka calls this `auto.create.topics.enable` and most production clusters
turn it off. We keep it, default it on for demos, and let you turn it off.


WHY THE PARTITION COUNT IS IMMUTABLE HERE
-------------------------------------------
Real Kafka lets you INCREASE partitions and never decrease them. We forbid both,
which is stricter and simpler — and the reason is worth understanding, because
it is the sharpest consequence of key-based routing.

`murmur2(key) % num_partitions` depends on the count. Change 3 to 4 and roughly
three keys in four map somewhere new:

    key "cust-42" → partition 1 of 3        (all its history is here)
    key "cust-42" → partition 3 of 4        (everything new goes here)

The old events and the new events are now in different logs, and **nothing
reads them in order ever again**. Kafka accepts this for increases because
growing a topic is sometimes necessary; it forbids decreases because those
would also strand data. We forbid changes entirely rather than offer a
foot-gun the project has no need for.

`chaos/partition_balance.py` demonstrates exactly how many keys move.
"""

import json
import os
import re
import time
from dataclasses import asdict, dataclass, field

from common import errors

METADATA_FILENAME = '_metadata.json'
METADATA_VERSION = 1

# Kafka's own rule: letters, digits, dot, underscore, hyphen; at most 249
# characters. The limit exists because a topic name becomes part of a
# DIRECTORY NAME, and `topic-partition` has to fit inside the filesystem's
# 255-byte limit with room for the partition suffix.
#
# A validation rule that traces to a filesystem constraint is worth noticing:
# most "arbitrary" limits in real systems have a reason like this behind them.
VALID_TOPIC_NAME = re.compile(r'^[a-zA-Z0-9._-]+$')
MAX_TOPIC_NAME_LENGTH = 249
MAX_PARTITIONS = 1024           # sanity bound; real clusters go higher


class InvalidTopicError(Exception):
    """The topic name or configuration is not acceptable."""


@dataclass
class TopicConfig:
    """
    One topic's configuration.

    `replication_factor` is stored but always 1 until Phase 6 — recorded now so
    that adding replicas is a change to the broker, not a change to the
    on-disk metadata format. Migrating a format is much more annoying than
    carrying an unused field.
    """
    partitions: int
    replication_factor: int = 1
    created_at: float = field(default_factory=time.time)

    def partition_ids(self) -> range:
        return range(self.partitions)


def validate_topic_name(name: str) -> None:
    """Raise InvalidTopicError if the name is unusable."""
    if not name:
        raise InvalidTopicError("topic name must not be empty")
    if len(name) > MAX_TOPIC_NAME_LENGTH:
        raise InvalidTopicError(
            f"topic name is {len(name)} characters, limit is {MAX_TOPIC_NAME_LENGTH}")
    if name in ('.', '..'):
        # These are directory entries, not names. Allowing them would let a
        # topic name escape the data directory.
        raise InvalidTopicError(f"{name!r} is not a valid topic name")
    if not VALID_TOPIC_NAME.match(name):
        raise InvalidTopicError(
            f"topic name {name!r} may only contain letters, digits, "
            f"'.', '_' and '-'")


class MetadataStore:
    """
    The broker's record of which topics exist and how wide they are.

    Held in memory, persisted to one JSON file. Small enough that rewriting the
    whole file on every change is fine — there are hundreds of topics, not
    millions of records. This is the opposite trade from the partition logs,
    and deliberately so: **the right storage design follows from the access
    pattern, not from consistency for its own sake.**
    """

    def __init__(self, data_dir: str, auto_create: bool = True,
                 default_partitions: int = 1):
        self.data_dir = data_dir
        self.auto_create = auto_create
        self.default_partitions = default_partitions

        self.path = os.path.join(data_dir, METADATA_FILENAME)
        self.topics: "dict[str, TopicConfig]" = {}

        os.makedirs(data_dir, exist_ok=True)
        self._load()

    # ── Persistence ────────────────────────────────────────────

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return

        try:
            with open(self.path, encoding='utf-8') as fh:
                raw = json.load(fh)
        except (json.JSONDecodeError, OSError):
            # Losing the topic list is bad, but refusing to start is worse:
            # the partition DATA is still on disk and intact. Start empty and
            # let topics be re-registered rather than making an unreadable
            # metadata file into an outage.
            return

        for name, config in raw.get('topics', {}).items():
            self.topics[name] = TopicConfig(
                partitions=config['partitions'],
                replication_factor=config.get('replication_factor', 1),
                created_at=config.get('created_at', 0.0),
            )

    def _save(self) -> None:
        """
        Persist atomically: write a temp file, fsync it, rename over the target.

        The same pattern as the consumer's offset file, for the same reason.
        Opening the real file with 'w' truncates it to zero bytes first, and a
        crash in that window leaves an EMPTY metadata file — which would read
        back as "no topics exist" while every partition's data sits on disk,
        unreachable because nothing knows the topics are there.

        The fsync must come before the rename, or the rename can reach disk
        first and leave a correctly-named file full of zeros after a power cut.
        """
        payload = {
            'version': METADATA_VERSION,
            'topics': {name: asdict(config) for name, config in self.topics.items()},
        }

        tmp_path = self.path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.path)

    # ── Topic management ───────────────────────────────────────

    def create_topic(self, name: str, partitions: int = None,
                     replication_factor: int = 1) -> TopicConfig:
        """
        Create a topic. Raises InvalidTopicError if it exists or is invalid.

        Note what this does NOT do: create any directories. Partition logs are
        still created lazily on first write. The metadata records that four
        partitions are *supposed* to exist; the files appear when they are
        needed.

        That split matters for a reason worth stating: the producer needs the
        partition COUNT to route a message, and it needs it before any of those
        partitions contain anything. Counting directories would give a count
        that grows as data arrives — so the number has to be declared, not
        observed.
        """
        validate_topic_name(name)

        if name in self.topics:
            raise InvalidTopicError(f"topic {name!r} already exists")

        partitions = partitions if partitions is not None else self.default_partitions
        if partitions < 1:
            raise InvalidTopicError(f"partitions must be >= 1, got {partitions}")
        if partitions > MAX_PARTITIONS:
            raise InvalidTopicError(
                f"partitions must be <= {MAX_PARTITIONS}, got {partitions}")
        if replication_factor != 1:
            raise InvalidTopicError(
                f"replication_factor must be 1 until Phase 6 adds replicas, "
                f"got {replication_factor}")

        config = TopicConfig(partitions=partitions,
                             replication_factor=replication_factor)
        self.topics[name] = config
        self._save()
        return config

    def get_or_create(self, name: str) -> TopicConfig:
        """
        Return a topic's config, auto-creating it if that is enabled.

        Raises UnknownTopicOrPartitionError when auto-creation is off — the
        loud version of the typo hazard, which is what you want in production.
        """
        if name in self.topics:
            return self.topics[name]

        if not self.auto_create:
            raise errors.UnknownTopicOrPartitionError(
                f"topic {name!r} does not exist and auto-creation is disabled",
                topic=name)

        validate_topic_name(name)
        config = TopicConfig(partitions=self.default_partitions)
        self.topics[name] = config
        self._save()
        return config

    def get(self, name: str):
        """Return the config, or None. Never creates."""
        return self.topics.get(name)

    def exists(self, name: str) -> bool:
        return name in self.topics

    def list_topics(self) -> list:
        return sorted(self.topics)

    def partition_count(self, name: str) -> int:
        config = self.topics.get(name)
        if config is None:
            raise errors.UnknownTopicOrPartitionError(
                f"topic {name!r} does not exist", topic=name)
        return config.partitions

    def validate_partition(self, name: str, partition: int) -> None:
        """
        Raise if the partition is out of range for this topic.

        Without this check, producing to partition 9 of a 4-partition topic
        would silently CREATE `data/orders-9/` — data written to a partition no
        consumer will ever be assigned, and which no metadata response
        mentions. It would simply disappear. Validating the range converts
        silent loss into a visible error.
        """
        config = self.topics.get(name)
        if config is None:
            raise errors.UnknownTopicOrPartitionError(
                f"topic {name!r} does not exist", topic=name)
        if not 0 <= partition < config.partitions:
            raise errors.UnknownTopicOrPartitionError(
                f"partition {partition} is out of range for topic {name!r}, "
                f"which has {config.partitions} partition(s) (0..{config.partitions - 1})",
                topic=name, partition=partition)

    def __repr__(self) -> str:
        return f"MetadataStore({len(self.topics)} topics, auto_create={self.auto_create})"
