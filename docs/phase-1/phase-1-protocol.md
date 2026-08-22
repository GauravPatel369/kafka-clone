# Phase 1 — Protocol v2: Correlation IDs, API Versions, Real Error Codes

> **Numbering note.** The original prototype's docstrings called themselves
> "Phase 1". In the roadmap, Phase 1 is *this* — the protocol rework. If you
> just finished [the code reading guide](../phase-0/code-reading-guide.md), the system it
> described is what the roadmap calls Phase 0. This document is the next thing
> built.

> **Goal:** turn the wire format from something that works into something that
> can be built on. Nothing here is a user-visible feature. Everything in
> Phases 3–6 depends on it.

---

## 1. The problem

The Phase 0 protocol has three limits, and each one blocks a specific later
phase.

### Limit 1 — one request in flight, forever

There is nothing in the wire format identifying *which* response belongs to
*which* request. The only reason the client can match them up is that it never
sends a second request until the first has been answered.

So every `send()` costs a full round trip during which nothing happens:

```
producer ──req 1──►                 (idle: producer, network, broker)
         ◄──res 1──
producer ──req 2──►                 (idle again)
         ◄──res 2──
```

The Phase 0 benchmark measured this: **3,982 msg/sec with the broker
essentially idle.** The bottleneck is not the broker, the disk, or the CPU. It
is that we insist on waiting.

### Limit 2 — errors are prose

```python
return {'status': 'error', 'reason': f'unknown msg_type {msg_type}'}
```

A client that wants to know *should I retry this?* has to match on English
text. That is not a plan; it is a future outage. Consider the two failures a
producer will meet constantly in Phase 6:

| What happened | Right response |
|---|---|
| "this broker is no longer the leader for that partition" | **Retry** — refresh metadata, send to the new leader |
| "that topic does not exist" | **Do not retry** — retrying will fail identically, forever |

Both arrive as `status: error` with a sentence. Phase 3's retry logic and
Phase 6's failover both need to tell these apart *programmatically*, and a
string cannot do that.

### Limit 3 — no version, so no upgrade

The format is whatever the code does today. Change it, and every client must
be restarted at the same instant as every broker.

That is fine for one machine. It is impossible for a real cluster: ten brokers
restart one at a time over an hour, so old and new coexist for that hour and
must understand each other. **A protocol with no version number cannot be
upgraded without downtime**, and "we'll take downtime" stops being an option
the moment anyone depends on you.

### And one security-shaped bug

```python
body_bytes = await reader.readexactly(body_length)
```

`body_length` is an unsigned 32-bit integer read straight off the wire and
trusted. A client — malicious or merely broken — can claim 4 GB, and the
broker will faithfully try to allocate it. Three such connections take the
process down. No authentication is required to do this, only a socket.

---

## 2. How real Kafka solves it

Kafka's request header, since 2011:

```
api_key (int16) · api_version (int16) · correlation_id (int32) · client_id (string)
```

and the response header is just `correlation_id (int32)`.

Three ideas, and they are worth taking one at a time.

### Correlation IDs

Every request carries a client-chosen integer. The response echoes it. That is
all — and it is enough to send many requests without waiting:

```
   WITHOUT correlation IDs             WITH correlation IDs

   req ──►                             req#1 ──►
       ◄── res                         req#2 ──►
   req ──►                             req#3 ──►
       ◄── res                              ◄── res#1
   req ──►                                  ◄── res#2
       ◄── res                              ◄── res#3

   3 round trips                       1 round trip
```

The client keeps a map of `correlation_id → what I was waiting for`. The
network fills with useful work instead of silence.

> **A subtlety worth knowing.** Kafka *also* guarantees that responses on one
> connection come back in request order. So why have IDs at all? Because
> ordering alone tells you nothing if a response is dropped or you reconnect,
> and because the ID makes debugging tractable — you can point at a packet
> capture and say "that is the answer to that". The ordering guarantee has its
> own famous consequence: with `max.in.flight > 1` and retries enabled, a
> retried request can land *after* one sent later, silently reordering a
> partition. Phase 3 meets that problem properly.

### Per-API versioning

Not one version for "the protocol" — a separate version per API.

Why: adding a field to `Fetch` should not force a new `Produce`. During a
rolling upgrade the client asks each broker what it supports (real Kafka:
`ApiVersions`, api_key 18) and picks the highest version both understand, per
API. A 10-broker cluster upgrades over an hour with no client aware of it.

### Numeric error codes

A small stable integer per failure, with one property that matters more than
the number itself: **each code is classified retriable or not.** The client
does not interpret prose; it asks the code.

---

## 3. Options considered

### How do we match responses to requests?

| Option | Trade-off |
|---|---|
| One request in flight (Phase 0) | Free, trivially correct, wastes the entire round trip. |
| A connection per in-flight request | No bookkeeping at all, but N TCP handshakes and N sockets per client. Solves latency by buying it back in setup cost. |
| Rely on response order only | Free — but forces the broker to answer strictly in order, so one slow request blocks everything behind it (head-of-line blocking). |
| **Correlation ID** ← chosen | One integer on the wire, a counter on the client, a pending map. Enables pipelining without constraining broker ordering. |

### Where does the error code live?

| Option | Trade-off |
|---|---|
| In the header | Fixed cost on every message including successes; and one code cannot describe a response covering five partitions where two failed. |
| **In the body** ← chosen | Free on the wire (JSON already), and nests naturally: a per-request code now, a per-partition code in Phase 3. Matches real Kafka. |

### How do we ship a breaking header change?

| Option | Trade-off |
|---|---|
| Bump the magic number | Old clients get a clean "wrong protocol" rejection. But no interop at all — a flag day. |
| Negotiate (`ApiVersions`) | What real Kafka does. Needs an API that is itself never allowed to change. Overkill while every client lives in this repo. |
| **Break it, and say so** ← chosen | Honest. `MAGIC` stays `0xCAFE`, so a stale Phase 0 client fails with `UNSUPPORTED_VERSION` rather than "bad magic" — a lesson in itself: *changing a layout without changing its identifier makes the failure confusing.* |

We break it because there are no deployed clients. That is a fact about this
project, not a general licence, and it is exactly the decision you cannot make
once someone depends on you.

---

## 4. The decision

### The new header — 16 bytes

```
┌──────────┬───────────┬─────────┬─────────┬────────────────┬───────────┐
│ magic 4B │ length 4B │ api_key │ api_ver │ correlation_id │ body JSON │
│  0xCAFE  │  of body  │   2B    │   2B    │      4B        │           │
└──────────┴───────────┴─────────┴─────────┴────────────────┴───────────┘
└──────────────────── 16 bytes, format '!I I H H I' ───────────────────┘
```

Four bytes bigger than Phase 0's. `api_key` and `api_version` are `H`
(uint16) rather than `I` because 65,535 API types is already absurd — real
Kafka has about 70. Spending 4 bytes there would be waste on every single
message.

Both directions use the same header. The response echoes `api_key` and
`correlation_id`. Real Kafka's response header is smaller (correlation ID
only) because the client already knows what it asked; we echo the key because
it makes a packet capture readable, and 2 bytes is a fair price for that.

### API keys — matching real Kafka's numbers

Not arbitrary. These are the actual numbers from the Kafka protocol, so
anything you learn here transfers:

| Key | API | Status |
|---:|---|---|
| 0 | `PRODUCE` | implemented |
| 1 | `FETCH` | implemented |
| 2 | `LIST_OFFSETS` | implemented — earliest/latest offset |
| 3 | `METADATA` | implemented — brokers and topics |
| 8 / 9 | `OFFSET_COMMIT` / `OFFSET_FETCH` | reserved → Phase 4 |
| 10 | `FIND_COORDINATOR` | reserved → Phase 4 |
| 11–14 | `JOIN_GROUP`, `HEARTBEAT`, `LEAVE_GROUP`, `SYNC_GROUP` | reserved → Phase 4 |
| 19 | `CREATE_TOPICS` | reserved → Phase 3 |

Reserving them now, with the right numbers, means no renumbering later.

### Error codes — and the retriable flag

Also real Kafka's numbers ([`common/errors.py`](../../common/errors.py)):

| Code | Name | Retriable? | Means |
|---:|---|:---:|---|
| 0 | `NONE` | — | success |
| −1 | `UNKNOWN_SERVER_ERROR` | no | a bug on the broker |
| 1 | `OFFSET_OUT_OF_RANGE` | no | asked for data that is aged out or not written |
| 2 | `CORRUPT_MESSAGE` | no | CRC mismatch (Phase 2) |
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | **yes** | see below |
| 5 | `LEADER_NOT_AVAILABLE` | **yes** | election in progress (Phase 6) |
| 6 | `NOT_LEADER_OR_FOLLOWER` | **yes** | **the hinge of Phase 5 + 6** |
| 7 | `REQUEST_TIMED_OUT` | **yes** | broker did not answer in time |
| 10 | `MESSAGE_TOO_LARGE` | no | over `MAX_REQUEST_BYTES` |
| 19 | `NOT_ENOUGH_REPLICAS` | **yes** | ISR too small for `acks=all` (Phase 6) |
| 27 | `REBALANCE_IN_PROGRESS` | **yes** | group is rebalancing (Phase 4) |
| 35 | `UNSUPPORTED_VERSION` | no | this broker cannot speak that api_version |
| 42 | `INVALID_REQUEST` | no | malformed — retrying sends the same bad bytes |

Two rows are worth pausing on.

**Why is `UNKNOWN_TOPIC_OR_PARTITION` retriable?** It looks permanent. But in
a cluster the usual cause is that metadata has not propagated yet — the topic
was created a second ago, or this broker has not received the controller's
update. Retrying with a metadata refresh usually succeeds. Kafka classifies it
retriable for exactly this reason, and it is a good illustration that
"retriable" is a claim about *time*, not about correctness.

**`NOT_LEADER_OR_FOLLOWER` (6)** is defined now although nothing can raise it
until Phase 5. It is the error that makes failover work: the client sends to a
broker that used to lead a partition, gets code 6, refreshes its metadata,
finds the new leader, and retries — all without the application noticing.
Defining it now means Phase 5 adds a cluster, not a protocol change.

### The exception hierarchy

Codes cross the wire; exceptions are what Python code catches. The tree is
shaped so that the *only* question a caller usually asks is answerable with
one `except`:

```
KafkaError
├── RetriableError                     ← except this to retry
│   ├── NotLeaderError                 (6)
│   ├── LeaderNotAvailableError        (5)
│   ├── RequestTimedOutError           (7)
│   ├── NotEnoughReplicasError         (19)
│   ├── RebalanceInProgressError       (27)
│   └── UnknownTopicOrPartitionError   (3)
└── UnknownServerError / OffsetOutOfRangeError / CorruptMessageError
    / MessageTooLargeError / UnsupportedVersionError / InvalidRequestError
```

Phase 3's retry loop becomes three honest lines:

```python
try:
    return self._send_once(...)
except RetriableError:
    backoff_and_try_again()
```

No string matching, no guessing.

### The size guard

```python
MAX_REQUEST_BYTES = 10 * 1024 * 1024      # 10 MB
```

Checked **before** allocating anything. A frame claiming more gets
`MESSAGE_TOO_LARGE` and the connection closes — a claimed length is a
statement about memory *we* are asked to commit, so it must be validated
before it is honoured, not after.

The number is a policy choice with two failure modes: too low and you reject
legitimate large batches; too high and one client can exhaust the broker.
Real Kafka splits it in two — `socket.request.max.bytes` (100 MB) for the
frame and `message.max.bytes` (1 MB) for a record — because those two limits
protect different things.

---

## 5. Code walkthrough

| File | Change |
|---|---|
| [`common/errors.py`](../../common/errors.py) | **New.** Codes, the retriable set, the exception tree, `error_for_code()`, `raise_for_error()`. |
| [`common/protocol.py`](../../common/protocol.py) | Rewritten: 16-byte header, `Header` namedtuple, API keys, version table, `MAX_REQUEST_BYTES`. |
| [`common/net.py`](../../common/net.py) | Correlation ID counter; `request()` verifies the echo; **`pipeline()`** sends many and matches replies. |
| [`broker/broker.py`](../../broker/broker.py) | Version checking, structured error responses, `LIST_OFFSETS` and `METADATA` handlers, size guard. |
| [`producer/producer.py`](../../producer/producer.py) | `raise_for_error`; `send_pipelined()`. |
| [`consumer/consumer.py`](../../consumer/consumer.py) | `raise_for_error`; uses `LIST_OFFSETS` for `seek_to_end()`. |
| [`tools/kfk.py`](../../tools/kfk.py) | `describe` uses `METADATA` + `LIST_OFFSETS` instead of fetching a message to guess. |

### The part that does the work

Correlation matching, in [`net.py`](../../common/net.py):

```python
def request(self, api_key, payload, api_version=0):
    correlation_id = self._next_correlation_id()
    self._send(api_key, api_version, correlation_id, payload)
    header, body = self._read_frame()
    if header.correlation_id != correlation_id:
        raise ProtocolError(...)          # stream desynchronised
    return body
```

and pipelining, which is the same primitive used differently:

```python
def pipeline(self, requests):
    ids = [self._send(...) for each request]      # all out, no waiting
    replies = {}
    for _ in ids:
        header, body = self._read_frame()
        replies[header.correlation_id] = body     # match by ID
    return [replies[i] for i in ids]              # back in caller's order
```

That second function is eleven lines, and it is only possible because of four
bytes in the header.

### What pipelining actually bought — the measurement, not the story

The tidy version of this section would end there. The benchmark says
otherwise, and the real result is the more useful one:

```
PRODUCE sequential                     3,946 msg/sec
PRODUCE pipelined (100 in flight)      4,143 msg/sec     → 1.05x
```

**Pipelining is worth essentially nothing on loopback.** That is not a bug in
the implementation; it is what the numbers should have been predicted to say.
`bench/bench.py` now measures the cost breakdown that explains it:

| Per request | µs |
|---|---:|
| Cheap request (METADATA), sequential | 130.1 |
| Same request, pipelined | 91.9 |
| **→ loopback round-trip wait** | **38.2** ← what pipelining removes |
| PRODUCE, sequential | 253.4 |
| **→ broker work (thread hop + disk)** | **215.2** ← what pipelining cannot remove |

Pipelining removes *waiting*. On loopback the wait is 38 µs out of a 253 µs
request — the other 215 µs is the broker genuinely working. **The broker was
never idle, so there was no idle time to reclaim.**

There is a second limit worth naming: our broker handles one request per
connection at a time (read → route → reply → repeat). Pipelined requests queue
up and are still served serially. Pipelining removes the *client's* waiting,
not the *broker's* serialisation. Real Kafka behaves the same way per
connection — that in-order handling is precisely what makes its ordering
guarantee work.

So when is it worth four bytes? When the round trip dominates the work.
Projected from the measured 215 µs:

| Round trip | Sequential | Pipelined | Speedup |
|---|---:|---:|---:|
| 0.035 ms (loopback) | 3,345/s | 3,788/s | 1.1x |
| 0.5 ms (same LAN) | 1,309/s | 3,788/s | **2.9x** |
| 5 ms (same region) | 190/s | 3,788/s | **19.9x** |
| 50 ms (cross-region) | 20/s | 3,788/s | **190.4x** |

Invisible here, decisive at 50 ms.

Three lessons, and they outlast this phase:

1. **Benchmark the claim, then keep the number that argues against you.** The
   first draft of this document asserted pipelining was "worth several times
   the throughput on loopback". The measurement said 1.05x. The assertion was
   wrong, and finding that out is the entire reason for running the benchmark.
2. **Latency-hiding optimisations have no value on localhost.** Their benefit
   is a property of the network, so testing them on loopback measures the one
   environment where they cannot help. If you only ever benchmark locally, you
   will conclude they are useless — and ship a system that falls over at 50 ms.
3. **Pipelining is not the fix for produce throughput here.** The 215 µs of
   broker work per message is. Cutting *that* needs batching — many messages
   in one request — which is Phase 3, and which is the change that will
   actually move this benchmark.

---

## 6. How real Kafka differs

- **`ApiVersions` (key 18).** A real client's first request asks the broker
  what it supports and negotiates per API. We hardcode v0 everywhere and
  return `UNSUPPORTED_VERSION` otherwise — correct, but it cannot negotiate.
- **`client_id` in the header.** Real requests carry a client name, which is
  what makes broker metrics and quotas per-application. We omit it.
- **Throttling.** Real responses include `throttle_time_ms`, letting the
  broker slow an abusive client without dropping it. We have no quotas.
- **Binary bodies.** Ours are JSON. Real Kafka's are packed binary with
  varints and optional compression — several times smaller. We keep JSON for
  the same reason as always: you can read it.
- **Error codes are per-partition.** A real `Produce` response carries a code
  for every partition it touched. Ours is one code per request, because a
  request touches one partition. Phase 3 changes that.

---

## 7. Exercises

1. **Watch pipelining fail to help.** Run `python -m bench.bench --count 5000`
   and read the cost breakdown. Before looking at the projection table, work
   out for yourself what round-trip time would make pipelining worth 10x.
2. **Prove the diagnosis.** The claim is that PRODUCE is broker-bound while
   METADATA is round-trip-bound. Pipeline METADATA instead of PRODUCE and
   confirm the speedup is larger (it should be roughly 130/92). If it is not,
   the explanation above is wrong — go find out why.
3. **Find the ceiling.** Raise `--pipeline-depth` until throughput stops
   improving, then keep going until it deadlocks. Why does it deadlock, and
   which `max.in.flight`-style bound would have prevented it?
4. **Trigger every error.** Ask for `api_version=99`; ask for offset −5; send
   an 11 MB message. Confirm each raises the right typed exception, and check
   which are `RetriableError`.
5. **Prove the guard matters.** Comment out the `MAX_REQUEST_BYTES` check and
   send a header claiming 4 GB with no body. Watch the broker's memory. Put it
   back.
6. **Break correlation on purpose.** Make the broker echo `correlation_id + 1`.
   Which test fails, and is the error message good enough to diagnose it from?
7. **Design the next one.** Phase 3 needs a `Produce` request carrying batches
   for several partitions at once, each with its own error code. Sketch that
   body. What breaks if you reuse one top-level `error_code`?

---

## 8. Verification

```bash
pytest -q                                  # correctness
python -m chaos.kill_consumer_midprocess   # unchanged behaviour, new protocol
python -m bench.bench --count 5000         # sequential vs pipelined

python -m broker.broker                    # terminal 1
python -m producer.producer --count 20     # terminal 2
python -m tools.kfk metadata               # cluster + topic view
python -m tools.kfk describe --topic orders
```

Record the run in [`benchmarks.md`](../benchmarks.md).

→ Next: **Phase 2 — Storage engine** (segments, sparse index, CRC, retention),
which fixes the O(n) fetch you measured in Phase 0.
