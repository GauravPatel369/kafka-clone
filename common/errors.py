"""
CONCEPT: Errors as Data, Not as Prose
---------------------------------------
In Phase 0 a failure looked like this:

    {'status': 'error', 'reason': 'unknown msg_type 99'}

A human can read that. A program cannot act on it. And the question a client
actually needs answered is not "what went wrong" but:

    SHOULD I TRY AGAIN?

Consider two failures a producer meets constantly once there is a cluster:

    "this broker is no longer the leader for orders-3"
        → RETRY. Refresh metadata, find the new leader, resend. The message
          is fine; it just knocked on the wrong door.

    "topic 'oders' does not exist"
        → DO NOT RETRY. Retrying sends the identical request and gets the
          identical answer, forever, in a tight loop.

Both arrive as `status: error` plus a sentence. Deciding between them by
matching English is how you get an outage caused by someone improving an error
message.

So: every failure gets a small stable integer, and every integer is classified
**retriable or not**. The client asks the classification, never the prose.


WHY THESE PARTICULAR NUMBERS
------------------------------
They are Apache Kafka's real error codes. `NOT_LEADER_OR_FOLLOWER` really is
6; `REBALANCE_IN_PROGRESS` really is 27. Nothing forced this on us — we could
have numbered them 1, 2, 3.

Using the real ones means what you learn here transfers directly: read a Kafka
stack trace, a broker log, or the protocol spec, and the numbers line up.
Inventing our own would have made this file a dead end.


CODES CROSS THE WIRE, EXCEPTIONS ARE WHAT YOU CATCH
-----------------------------------------------------
Two representations of the same thing, because they serve different jobs:

    error_code (int)   travels in a JSON body; language-neutral; stable
                       forever, because changing it breaks every client.

    KafkaError         a Python exception; carries a message; and — the point
                       — is arranged in a hierarchy so that the common
                       question is one `except` clause:

                           try:
                               send()
                           except RetriableError:
                               backoff_and_try_again()

That single clause is why the hierarchy is shaped by *what you do about it*
rather than by what went wrong. A taxonomy organised around the caller's
decision is useful; one organised around the failure's biography is trivia.
"""


# ── Error codes (Apache Kafka's real numbers) ──────────────────

NONE = 0                            # success
UNKNOWN_SERVER_ERROR = -1           # a bug on the broker side

OFFSET_OUT_OF_RANGE = 1
CORRUPT_MESSAGE = 2
UNKNOWN_TOPIC_OR_PARTITION = 3
LEADER_NOT_AVAILABLE = 5
NOT_LEADER_OR_FOLLOWER = 6
REQUEST_TIMED_OUT = 7
MESSAGE_TOO_LARGE = 10
NOT_ENOUGH_REPLICAS = 19
ILLEGAL_GENERATION = 22
UNKNOWN_MEMBER_ID = 25
REBALANCE_IN_PROGRESS = 27
UNSUPPORTED_VERSION = 35
INVALID_REQUEST = 42
FENCED_LEADER_EPOCH = 74


ERROR_NAMES = {
    NONE: 'NONE',
    UNKNOWN_SERVER_ERROR: 'UNKNOWN_SERVER_ERROR',
    OFFSET_OUT_OF_RANGE: 'OFFSET_OUT_OF_RANGE',
    CORRUPT_MESSAGE: 'CORRUPT_MESSAGE',
    UNKNOWN_TOPIC_OR_PARTITION: 'UNKNOWN_TOPIC_OR_PARTITION',
    LEADER_NOT_AVAILABLE: 'LEADER_NOT_AVAILABLE',
    NOT_LEADER_OR_FOLLOWER: 'NOT_LEADER_OR_FOLLOWER',
    REQUEST_TIMED_OUT: 'REQUEST_TIMED_OUT',
    MESSAGE_TOO_LARGE: 'MESSAGE_TOO_LARGE',
    NOT_ENOUGH_REPLICAS: 'NOT_ENOUGH_REPLICAS',
    ILLEGAL_GENERATION: 'ILLEGAL_GENERATION',
    UNKNOWN_MEMBER_ID: 'UNKNOWN_MEMBER_ID',
    REBALANCE_IN_PROGRESS: 'REBALANCE_IN_PROGRESS',
    UNSUPPORTED_VERSION: 'UNSUPPORTED_VERSION',
    INVALID_REQUEST: 'INVALID_REQUEST',
    FENCED_LEADER_EPOCH: 'FENCED_LEADER_EPOCH',
}


# ── Exceptions ─────────────────────────────────────────────────

class KafkaError(Exception):
    """Base class for every broker-reported failure."""

    error_code = UNKNOWN_SERVER_ERROR
    retriable = False

    def __init__(self, message: str = '', **context):
        self.context = context           # topic, partition, offset, ...
        detail = ' '.join(f"{k}={v!r}" for k, v in context.items())
        super().__init__(
            f"[{ERROR_NAMES.get(self.error_code, '?')}({self.error_code})] "
            f"{message}" + (f"  ({detail})" if detail else "")
        )


class RetriableError(KafkaError):
    """
    The same request, sent again later, may well succeed.

    "Retriable" is a claim about TIME, not about correctness: the request is
    valid, the cluster is simply not in a state to serve it right now. A
    leader election is in progress, metadata has not propagated, a replica is
    catching up.

    Catch THIS class, not the individual subclasses. New retriable errors
    added in later phases then work with existing retry loops automatically —
    which is the entire reason the hierarchy exists.
    """
    retriable = True


# ── Retriable ──────────────────────────────────────────────────

class UnknownTopicOrPartitionError(RetriableError):
    """
    The broker has never heard of this topic/partition.

    Retriable, which surprises people — surely a missing topic is permanent?

    Usually not, in a cluster. The common cause is that metadata has not
    reached this broker yet: the topic was created a moment ago, or the
    controller's update is still in flight. A refresh-and-retry normally
    succeeds within a second.

    If the name is genuinely a typo, the retry loop gives up after its
    configured attempts and reports this error — which is the right outcome,
    just slower. Kafka makes the same call.
    """
    error_code = UNKNOWN_TOPIC_OR_PARTITION


class LeaderNotAvailableError(RetriableError):
    """No leader for this partition right now — an election is under way.

    Transient by definition; a leader is being chosen. Phase 6."""
    error_code = LEADER_NOT_AVAILABLE


class NotLeaderError(RetriableError):
    """
    This broker holds the partition but is not its leader; write elsewhere.

    THE most important error in the whole system, and the reason failover is
    invisible to applications:

        1. client sends to broker 2, its cached metadata says leader
        2. broker 2 answers NOT_LEADER_OR_FOLLOWER
        3. client refreshes metadata, learns broker 5 now leads
        4. client resends to broker 5 — success

    The application sees a few milliseconds of extra latency and nothing else.
    Defined now, in Phase 1, so that Phase 5 adds a cluster rather than a
    protocol change.
    """
    error_code = NOT_LEADER_OR_FOLLOWER


class RequestTimedOutError(RetriableError):
    """
    The broker did not answer within the deadline.

    The nastiest error in distributed systems, because it tells you nothing
    about what happened: the request may have been lost, or fully applied with
    the response lost. Retrying can therefore DUPLICATE the write.

    That is why at-least-once is the honest default (see
    docs/phase-0-foundation.md), and why real Kafka needs an idempotent
    producer — a sequence number per partition — to make the retry safe.
    """
    error_code = REQUEST_TIMED_OUT


class NotEnoughReplicasError(RetriableError):
    """Fewer in-sync replicas than `min.insync.replicas` — refusing the write.

    Kafka choosing consistency over availability, out loud. Phase 6."""
    error_code = NOT_ENOUGH_REPLICAS


class FencedLeaderEpochError(RetriableError):
    """
    This broker's idea of who leads the partition is out of date.

    Distinct from NOT_LEADER_OR_FOLLOWER, and the distinction is diagnostic
    rather than cosmetic:

        NOT_LEADER_OR_FOLLOWER   "I never led this" — routine, happens on
                                 every placement change
        FENCED_LEADER_EPOCH      "I thought I led this and I am the stale
                                 one" — a broker was serving a partition it
                                 had already lost

    The second is the split-brain case on the DATA plane: a partitioned leader
    that has not yet heard it was replaced, accepting writes into a log nobody
    will read while telling producers they succeeded. Retriable for the client
    (refresh metadata, find the real leader) and a thing an operator should be
    able to grep for.
    """
    error_code = FENCED_LEADER_EPOCH


class RebalanceInProgressError(RetriableError):
    """
    The group is redistributing partitions; rejoin and retry.

    Retriable, and the retry is not a plain resend: the member must rejoin the
    group first. This is how a consumer LEARNS about a rebalance it did not
    cause — its heartbeat comes back with this code.
    """
    error_code = REBALANCE_IN_PROGRESS


class IllegalGenerationError(RetriableError):
    """
    Your generation number is stale: the group rebalanced without you.

    THE zombie fence. A consumer that paused past its session timeout was
    evicted and its partitions reassigned; when it wakes and tries to commit,
    this rejects it. Without that rejection its stale commit would rewind the
    group and silently reprocess everything the replacement had already done.

    Retriable in the sense that rejoining and retrying will work — but the
    consumer must throw away its in-flight work first, because those partitions
    now belong to someone else.
    """
    error_code = ILLEGAL_GENERATION


class UnknownMemberIdError(RetriableError):
    """
    The coordinator has never heard of this member, or evicted it.

    Same remedy as ILLEGAL_GENERATION: rejoin from scratch with an empty
    member id and accept whatever assignment you are given.
    """
    error_code = UNKNOWN_MEMBER_ID


# ── Not retriable ──────────────────────────────────────────────

class UnknownServerError(KafkaError):
    """The broker hit an unexpected exception. A bug, not a condition."""
    error_code = UNKNOWN_SERVER_ERROR


class OffsetOutOfRangeError(KafkaError):
    """
    The requested offset does not exist in this partition.

    Two very different causes, which is why a consumer must handle it:

      - too LOW: retention deleted that data. The consumer fell so far behind
        that the broker gave up waiting. Real data loss, and the reason
        consumer lag is the number everyone alarms on.
      - too HIGH: asking beyond the end, usually after a consumer's saved
        offset outlived the log (a topic recreated under the same name).

    Not retriable: the offset will not come back. The consumer must decide to
    resume from the earliest or the latest offset — Kafka's
    `auto.offset.reset`, and a genuine judgement call, since one choice
    reprocesses and the other skips.
    """
    error_code = OFFSET_OUT_OF_RANGE


class CorruptMessageError(KafkaError):
    """A stored record failed its checksum. Phase 2 adds the CRC that detects it."""
    error_code = CORRUPT_MESSAGE


class MessageTooLargeError(KafkaError):
    """The request exceeds `MAX_REQUEST_BYTES`. Retrying sends the same bytes."""
    error_code = MESSAGE_TOO_LARGE


class UnsupportedVersionError(KafkaError):
    """
    This broker cannot speak the requested api_version.

    Not retriable against this broker — but in a mixed-version cluster mid
    rolling-upgrade, a *different* broker may well support it. Real clients
    negotiate per broker with an ApiVersions request precisely to avoid ever
    seeing this.
    """
    error_code = UNSUPPORTED_VERSION


class InvalidRequestError(KafkaError):
    """Malformed or nonsensical request. Retrying resends the same bad bytes."""
    error_code = INVALID_REQUEST


# ── Code → exception ───────────────────────────────────────────

_BY_CODE = {
    cls.error_code: cls
    for cls in (
        UnknownServerError, OffsetOutOfRangeError, CorruptMessageError,
        UnknownTopicOrPartitionError, LeaderNotAvailableError, NotLeaderError,
        RequestTimedOutError, MessageTooLargeError, NotEnoughReplicasError,
        RebalanceInProgressError, UnsupportedVersionError, InvalidRequestError,
        IllegalGenerationError, UnknownMemberIdError,
    )
}


def error_for_code(error_code: int, message: str = '', **context) -> KafkaError:
    """
    Build the exception for a code. Unknown codes become UnknownServerError.

    Unknown codes are not an impossible case: a newer broker can return a code
    this client has never heard of. Falling back to a generic non-retriable
    error is the safe default — treating an unrecognised failure as retriable
    would spin forever on something we cannot understand.
    """
    cls = _BY_CODE.get(error_code, UnknownServerError)
    return cls(message or ERROR_NAMES.get(error_code, f'code {error_code}'),
               **context)


def is_retriable(error_code: int) -> bool:
    """Whether a bare code is worth retrying, without building an exception."""
    return _BY_CODE.get(error_code, UnknownServerError).retriable


def raise_for_error(response: dict, **context) -> dict:
    """
    Raise if the response carries an error; otherwise return it unchanged.

    Every client call goes through here, so error handling cannot be
    accidentally skipped at one call site — the single most common way error
    handling rots.

    Returning the response lets it be used inline:

        messages = raise_for_error(conn.request(...))['messages']
    """
    error_code = response.get('error_code', NONE)
    if error_code != NONE:
        raise error_for_code(error_code, response.get('error_message', ''),
                             **context)
    return response
