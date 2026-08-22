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
