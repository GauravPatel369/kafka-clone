"""
CONCEPT: One Place for Every Default
-------------------------------------
Before this file existed, `localhost` and `9092` were typed out eight times
across four modules, and `./data` in three. That is fine until you want to
change one of them — then you get to find all eight, and the one you miss
becomes a bug that only shows up on someone else's machine.

Every magic number in this project lives here. Modules import the name, never
the literal.

Why 9092? It is the real Kafka default port. Nothing about it is special; we
match it so that habits transfer.
"""

# ── Network ────────────────────────────────────────────────────
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9092

# How long a client waits on a socket operation before giving up.
#
# Why have a timeout at all? Without one, a client whose broker has silently
# vanished (machine powered off, network cable pulled) blocks forever inside
# recv() — TCP has no way to tell the difference between "the peer is quiet"
# and "the peer is gone". A timeout turns an infinite hang into an error you
# can retry. Phase 3 builds retry logic on top of this.
DEFAULT_SOCKET_TIMEOUT = 10.0    # seconds

# ── Storage ────────────────────────────────────────────────────
DEFAULT_DATA_DIR   = './data'      # broker-side: partition log files
DEFAULT_OFFSET_DIR = './offsets'   # consumer-side: committed offsets
                                   # (moves server-side in Phase 4)

# ── Storage engine (Phase 2) ───────────────────────────────────
#
# Every one of these is a real Kafka setting under a similar name, so the
# reasoning transfers even where the value does not.

# Roll a new segment once the active one reaches this size.
#
# Real Kafka defaults to 1 GB. We use 1 MB so that segment rolling is
# observable in a demo — with 1 GB you would produce for a week before seeing
# a second file, and a mechanism you never watch happen is a mechanism you do
# not understand.
DEFAULT_SEGMENT_BYTES = 1 * 1024 * 1024        # 1 MB

# Roll a segment this old even if it is not full. 7 days, matching Kafka.
#
# Why bother? Retention only ever deletes SEALED segments, so a low-traffic
# topic whose single segment never fills would keep its data forever no matter
# what retention says. This is what stops that.
DEFAULT_SEGMENT_MS = 7 * 24 * 60 * 60 * 1000

# Add an index entry roughly every this many bytes. Kafka's default.
#
# This is the sparseness dial. Smaller = bigger index, shorter scan after the
# seek. Larger = tiny index, longer scan. Set it huge and you have effectively
# removed the index and gone back to Phase 1's O(n) reads.
DEFAULT_INDEX_INTERVAL_BYTES = 4096

# Delete segments whose newest record is older than this. 7 days, matching Kafka.
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

# Delete oldest segments once the log exceeds this total. -1 = unlimited.
DEFAULT_RETENTION_BYTES = -1

# fsync policy — both OFF by default, exactly as real Kafka ships.
#
# Every append already does flush(), pushing data to the OS page cache, which
# survives the broker process being killed. fsync() pushes it to the physical
# disk, which survives power loss, and costs ~1-10 ms instead of ~1 us.
#
# Kafka's position is that durability is bought with REPLICATION, not fsync:
# three copies on three machines survive one node's page cache vanishing, so
# paying 1000x per write to defend against something replicas already cover is
# a bad trade. That argument only becomes valid once Phase 6 exists — until
# then this is a documented weakness, and these dials let you measure what the
# safe version would cost.
DEFAULT_FSYNC_MESSAGES = 0                      # 0 = never
DEFAULT_FSYNC_MS = 0                            # 0 = never

# How often the broker runs the retention cleaner. Kafka's default is 5 minutes.
DEFAULT_RETENTION_CHECK_MS = 60_000             # 1 minute

# ── Consumer groups (Phase 4) ──────────────────────────────────

# How long a member may go without heartbeating before it is evicted and its
# partitions reassigned.
#
# A genuine dilemma with no right answer:
#   short (3s)   dead consumers detected fast; a GC pause or a network blip
#                triggers a SPURIOUS rebalance, stopping consumption for
#                everyone in the group.
#   long (60s)   stable under hiccups; a genuinely dead consumer's partitions
#                go unread for a minute.
#
# Kafka's real answer is two timeouts: `session.timeout.ms` for the heartbeat
# thread (is the PROCESS alive?) and `max.poll.interval.ms` for the poll loop
# (is PROCESSING progressing?). Separating them means slow processing stops
# looking like a dead process. We implement the first only.
DEFAULT_SESSION_TIMEOUT_MS = 10_000

# How often the consumer's background thread heartbeats. Kafka's guidance is
# roughly a third of the session timeout, so two consecutive heartbeats can be
# lost before anyone is evicted — one dropped packet should not cost the whole
# group a rebalance.
DEFAULT_HEARTBEAT_INTERVAL_MS = 3_000

# How long the coordinator waits for every known member to rejoin before
# proceeding without the absentees.
DEFAULT_REBALANCE_TIMEOUT_MS = 5_000

# How often the broker sweeps groups for members that stopped heartbeating.
#
# Needed because eviction otherwise only happens when somebody calls in — and
# if every member of a group dies at once, nobody ever calls in again. The
# group would sit forever holding partitions for processes that no longer exist.
DEFAULT_GROUP_REAP_MS = 5_000

# ── Client behaviour ───────────────────────────────────────────
DEFAULT_MAX_MESSAGES  = 100    # messages per FETCH request
DEFAULT_POLL_INTERVAL = 1.0    # seconds to sleep when a poll returns nothing

# Why sleep at all when there is nothing to read?
#
# Without it, an idle consumer spins as fast as the CPU allows, sending
# thousands of empty FETCH requests per second and burning broker capacity to
# be told "nothing yet" over and over.
#
# The cost is latency: a message that arrives just after a poll waits up to
# DEFAULT_POLL_INTERVAL before anyone sees it. Real Kafka avoids both problems
# with a *long poll* — the broker holds the request open for up to
# `fetch.max.wait.ms` and answers the instant data arrives, giving low latency
# AND no busy-waiting. We use the simple version for now; the trade-off is
# real and worth noticing.
