# Phase 5 — The Cluster: Membership, Raft Controller Election, Metadata Routing

> **Goal:** partitions stop living on one machine. Several brokers, one
> controller that decides which broker leads which partition, and clients that
> route themselves to the right one.
>
> Phase 6 makes it *fault-tolerant*. This phase makes it *distributed*, which
> is a different thing and has to come first.

---

## 1. The problem

Everything through Phase 4 runs on one process:

- **One machine's capacity is the ceiling.** Four partitions on one broker
  share one disk, one event loop, one NIC. Adding partitions adds parallelism
  the hardware cannot deliver.
- **One process is a single point of failure.** Kill it and every topic is
  gone. Consumer groups survive a *consumer* dying (Phase 4); nothing survives
  the broker dying.
- **`node_id` has been 0 since Phase 1.** `METADATA` has reported
  `{'node_id': 0, ...}`, `replicas: [0]`, `isr: [0]` for four phases. The shape
  was right; the content was a placeholder.

`FIND_COORDINATOR` has always answered "me". `NOT_LEADER_OR_FOLLOWER` has been
defined since Phase 1 and never raised. This phase is where those become true.

---

## 2. How real Kafka solves it

### Every partition has a leader, and clients go straight to it

```
                    metadata: orders-0 → broker 1
                              orders-1 → broker 2
                              orders-2 → broker 3

   Producer ──── orders-0 ────────────► Broker 1   (leads orders-0)
            ──── orders-1 ────────────► Broker 2   (leads orders-1)
            ──── orders-2 ────────────► Broker 3   (leads orders-2)
```

**There is no proxy.** The client holds the map and connects to the leader
directly. That is what "thick client" means, and it is why `METADATA` was built
in Phase 1: no extra hop on the data path, at the cost of every client needing
routing logic and a cache that can go stale.

### The cure for a stale cache is one error code

```
1. client sends to broker 2      (its cache says broker 2 leads orders-1)
2. broker 2 replies NOT_LEADER_OR_FOLLOWER
3. client refreshes metadata     (learns broker 5 leads it now)
4. client retries to broker 5    → success
```

**Metadata plus that one retriable error is the entire client-side failover
mechanism.** Both have existed since Phase 1, unused, waiting for this.

### A controller decides placement

One broker is the controller. It owns the partition→leader map and reassigns
when membership changes. Every other broker follows.

Why one? Because placement must be *consistent*: two brokers independently
deciding that they both lead `orders-0` is precisely the failure mode a
controller exists to prevent.

---

## 3. Options considered

### Where does the broker list come from?

| Option | Trade-off |
|---|---|
| **Static `cluster.json`** ← chosen | No dependency, no bootstrap problem, trivially auditable. Adding a broker means editing a file everywhere. |
| A discovery service (etcd, Consul) | Dynamic membership. Another system to run, and *it* now needs consensus. |
| Gossip (SWIM) | Fully dynamic, no central list. Considerably more protocol, and eventual-consistency surprises during churn. |

Static is the right size here: the interesting problems in this phase are
*liveness* and *election*, and a fixed roster lets us study those without also
building service discovery.

### How is the controller elected?

| Option | Trade-off |
|---|---|
| Lowest live broker id | Ten lines. Deterministic given the same view of who is alive. **Split-brain-prone under a network partition.** |
| **Quorum consensus (Raft)** ← chosen | Correct under partition: a minority mathematically cannot elect. Costs a log, terms, elections and durable state — the largest single component in this project. |
| External coordinator (ZooKeeper) | What Kafka did until 3.3. Correct, and now you operate a second distributed system whose failure takes yours down. |

**Lowest-live-id was built first, on purpose, and then replaced.** It made the
failure *visible*: `chaos/split_brain.py` printed two controllers, six
partitions each assigned differently, and no error anywhere. Reading about
split-brain is not the same as watching it happen.

Raft replaces exactly one thing — **the election**. Everything else in this
phase stays: `cluster.json`, broker heartbeats and liveness, the round-robin
placement algorithm, `CONTROLLER_UPDATE`, the client metadata cache and
routing. What changes is that leadership is a *quorum decision* rather than a
local opinion, and placement is *committed* before it is applied.

    votes_needed = len(voters) // 2 + 1

A majority of **all** voters, not of those reachable. Two disjoint majorities of
one set cannot exist, so two leaders cannot exist. With five voters a
partitioned side holding two can never reach three — arithmetic, not best
effort.

The price is availability: the minority stops serving even though its machines
are healthy. **Raft chooses consistency**, and `chaos/split_brain.py` now shows
the minority sitting as candidates with no controller instead of inventing one.

### Three pieces, all required

| Piece | Why it is not optional |
|---|---|
| **Leader election** | Terms, randomised timeouts, and the *election restriction* — a candidate whose log is behind cannot win, so every new leader already holds every committed entry. |
| **Log replication** | `AppendEntries`, log matching, conflict truncation, majority commit — including the rule that a leader may only commit an entry from its **own** term (Figure 8 of the Raft paper). |
| **Persistence** | `currentTerm`, `votedFor` and the log, fsynced. Forget `votedFor` and a node can vote twice in one term, so two candidates both reach a majority. |

Skip any one and you get something that looks like Raft and is not — worse than
lowest-live-id, because it *appears* safe.

### The quorum is not the cluster

`cluster.json` lists brokers; `quorum_voters` lists who votes, defaulting to all
of them (Kafka's combined `process.roles=broker,controller`). They are separate
because quorum size has its own arithmetic: a **2-voter quorum needs both
voters**, so it tolerates zero failures — strictly worse than one voter. Quorums
are sized 1, 3, 5 for that reason; clusters can be any size.

### How do the other brokers learn the map?

| Option | Trade-off |
|---|---|
| **Controller pushes on change** ← chosen | One authority, one version number. Peers are simple followers. A missed push leaves a peer stale until the next one. |
| Every broker computes it deterministically | No push at all — but then there is no controller, and any disagreement about *who is alive* silently becomes a disagreement about placement. |
| Peers poll the controller | Simple, but adds latency to every change and load proportional to peers × poll rate. |

Push with a monotonically increasing `metadata_version` means a peer can ignore
an out-of-order update, which is the cheap version of fencing.

### One port or two?

Real Kafka supports separate listeners for client and inter-broker traffic —
useful for putting them on different networks with different security.

**We use one port and add new API keys.** The protocol already carries an
`api_key`, so inter-broker requests are just more requests. A second listener
would be more plumbing and would teach nothing this project has not covered.
Noted as a deviation, not an oversight.

---

## 4. The decision

### `cluster.json`

```json
{
  "brokers": [
    {"node_id": 1, "host": "127.0.0.1", "port": 9092},
    {"node_id": 2, "host": "127.0.0.1", "port": 9093},
    {"node_id": 3, "host": "127.0.0.1", "port": 9094}
  ]
}
```

```bash
python -m broker.broker --node-id 1 --cluster cluster.json --data-dir ./data/b1
```

Each broker needs its **own data directory** — they are separate machines in
production, and sharing one directory locally would have three brokers writing
the same log files.

### Three new API keys

| Key | API | Direction |
|---:|---|---|
| 27 | `BROKER_HEARTBEAT` | broker → peer, "I am alive" |
| 28 | `CONTROLLER_UPDATE` | controller → peer, "here is the new map" |
| 29 | `DESCRIBE_CLUSTER` | client → any broker, for `kfk cluster status` |

Real Kafka's numbers for the first two are different (they belong to KRaft's
internal protocol); ours are chosen to sit clear of the client APIs.

### The membership loop

Each broker, every `broker_heartbeat_ms`:

1. send `BROKER_HEARTBEAT` to every peer in the roster
2. mark peers that answered as seen-now
3. a peer not seen within `broker_session_timeout_ms` is **down**
4. read the controller from Raft (`raft.leader_id`)
5. if *we* are the controller and membership changed → propose the new
   placement, wait for a majority to commit it, **then** apply and push

Note step 4: leadership is **read from Raft**, not computed. `ClusterState`
records what the quorum decided; it no longer has an opinion of its own.
`controller_id` is `None` during an election or inside a minority — and
`None == self.node_id` is False, so a node without a quorum never believes it
is in charge. That one comparison is the safety property.

**Liveness and leadership are now different questions**, and keeping them apart
is the point: heartbeats still say which brokers are *up* (placement needs it),
while Raft says who may *decide*. Merging the two is what made the first design
unsafe.

### A timer must never wait for the thing it is timing

The hardest bugs in this phase were not in the consensus algorithm. They were
all one mistake, in four places, and it is worth stating as a rule:

> **A dead peer is the slowest peer, always.** It costs a full RPC timeout,
> every time. So any loop that waits for all peers runs at the pace of the
> worst one.

Every one of these looked correct, and three of them were even properly
*concurrent* — the RPCs went out in parallel. The bug was in awaiting them:

| Code | Looked like | Actually cost |
|---|---|---|
| the vote tally | "count the votes" | a granted vote discarded as stale |
| awaiting the election on the timer loop | "elect, then continue" | winning in 5 ms, then silent for 2 s |
| the leader's heartbeat | "beat every 150 ms" | a 2 s beat, against a 1–2 s election timeout |
| the membership round | "heartbeat every peer" | membership flapping against a 700 ms session timeout |

Compounded, they meant **two healthy brokers out of three could not keep a
leader elected.** Terms climbed once a second indefinitely; Raft remained
*safe* throughout — never two leaders — but nothing ever committed, because
each new leader was deposed before it finished anything. Safe and useless is a
real failure mode, and it is one that looks like a performance problem.

The fix everywhere is the same shape: **the timer fires work off and returns.**
Results are recorded when they land, and a peer with a request still in flight
is skipped rather than queued behind itself. Waiting is reserved for the one
place that genuinely needs an answer — `propose`, which must know whether an
entry committed.

Why none of this was caught by 24 passing Raft tests: the in-memory test
transport modelled a dead node as **instant silence**. Waiting for a peer that
fails instantly costs nothing, so the tests agreed with an implementation that
could not survive a single real broker dying. Failure has to be modelled the
way it actually fails — dead things are slow, not quiet — which is why the test
network now has a `down_delay`.

### Placement — decided locally, committed globally

```python
leaders[(topic, partition)] = live_brokers[index % len(live_brokers)]
```

Round-robin over the sorted live brokers, unchanged. What changed is what
happens next:

```
compute → propose to Raft → wait for majority commit → apply → CONTROLLER_UPDATE
```

Applying first and replicating afterwards would let a partitioned leader hand
out a map nobody agreed to — the old failure with extra steps. So
**`CONTROLLER_UPDATE` now carries only committed metadata**: it is a delivery
mechanism, not a second source of truth.

`CREATE_TOPICS` goes through the log for the same reason. Writing it only to the
local store was a real bug: ask a broker that is not the controller and the
controller never learns the topic exists, so its partitions are never placed and
every request for them is answered NOT_LEADER forever. A non-leader now
**forwards** to the controller, so clients need not know who that is.

### `NOT_LEADER_OR_FOLLOWER`, finally raised

```python
# every PRODUCE / FETCH / LIST_OFFSETS handler
if not self.cluster.leads(topic, partition):
    return _error(errors.NOT_LEADER_OR_FOLLOWER, ...)
```

A broker refuses to serve a partition it does not lead. Without that refusal it
would happily create an empty local log and serve zero messages — the client
would see an empty topic rather than an error, which is the silent-wrongness
failure this project keeps trying to avoid.

### The client side

`common/cluster_client.py` holds what every client now needs:

- a **connection pool**, one `SyncConnection` per broker
- a **metadata cache**: `(topic, partition) → node_id`, plus a TTL
- **`request_for_partition(...)`** — route to the leader, and on
  `NOT_LEADER_OR_FOLLOWER` refresh once and retry

Producer and consumer both use it, so routing and failover are written once.

---

## 5. Code walkthrough

| File | Role |
|---|---|
| [`common/cluster_config.py`](../../common/cluster_config.py) | Parse and validate `cluster.json`. |
| [`broker/cluster.py`](../../broker/cluster.py) | Membership, heartbeats, liveness, the local view of the leader map. |
| [`broker/raft/`](../../broker/raft/) | The quorum: `state.py` (durable term/vote), `log.py` (the replicated log and the election restriction), `node.py` (follower/candidate/leader), `machine.py` (what committed entries mean). |
| [`broker/controller.py`](../../broker/controller.py) | Placement, proposed through Raft and applied only once committed. |
| [`common/cluster_client.py`](../../common/cluster_client.py) | Client connection pool, metadata cache, leader routing, refresh-and-retry. |
| [`broker/broker.py`](../../broker/broker.py) | Leadership checks on every partition API; three new handlers. |
| [`tools/kfk.py`](../../tools/kfk.py) | `cluster status`. |

### The one that matters — routing with refresh-and-retry

```python
# common/cluster_client.py
def request_for_partition(self, topic, partition, api_key, payload, api_version=0):
    for attempt in range(2):
        node_id = self.leader_for(topic, partition)
        try:
            response = self.connection_for(node_id).request(api_key, payload, api_version)
        except BrokerConnectionError:
            self.invalidate()                 # that broker is gone
            if attempt: raise
            continue
        if response.get('error_code') == errors.NOT_LEADER_OR_FOLLOWER:
            self.invalidate()                 # our map is stale
            continue
        return response
    raise NotLeaderError(...)
```

Two attempts, not a loop: one refresh is enough for a leader that has moved. An
unbounded loop would spin forever against a genuinely broken cluster, and
`RetriableError` already gives the caller a backoff at a higher level.

---

## 6. How real Kafka differs

- **KRaft is pull-based.** Followers *fetch* from the leader, reusing the same
  `Fetch` machinery as partition replication. Ours pushes, like the Raft paper.
  Identical safety; Kafka's choice reuses more code.
- **Snapshots are unchunked, and pushed rather than fetched.** We compact the
  log, restart from snapshot-plus-tail, and catch up a fallen-behind follower
  with `InstallSnapshot` — **[Step A](raft-snapshots.md)** ·
  **[Step B](raft-install-snapshot.md)**. Real KRaft splits a snapshot across
  many messages and has the follower *fetch* it, which matters when the state
  machine is gigabytes rather than kilobytes.
- **Dynamic quorum reconfiguration.** Raft can change its voter set through the
  log itself (joint consensus). Ours is fixed at startup.
- **Epoch fencing on partition data.** Real Kafka stamps a `leader_epoch` so a
  stale leader's *writes* are rejected. We fence metadata by term, but a stale
  broker will still accept a write for a partition it no longer leads — visible
  at the end of `chaos/split_brain.py`. Phase 6 fixes it, using the Raft term
  as the epoch.
- **Coordinators are distributed.** Real Kafka hashes the group id to pick which
  broker coordinates a group. Ours are per-broker, so a group is coordinated by
  whichever broker the consumer happened to ask.
- **No data movement.** When a leader changes here, the *partition* changes
  machine but its **data does not follow** — the new leader starts an empty
  log. That is a serious limitation and it is exactly what Phase 6 fixes: with
  replicas, the new leader already has the data.

> That last point is worth stating loudly: **Phase 5 alone is not fault
> tolerance.** Losing a broker means losing its partitions' data. What this
> phase buys is capacity and routing; durability is Phase 6's job.

---

## 7. Exercises

1. **Watch an election.** `python -m chaos.kill_controller` — kill the
   controller and watch the quorum elect a new one in a new term.
2. **Watch a follower rescued by a snapshot.**
   `python -m chaos.lagging_follower` — cut a broker off, compact past it, heal,
   and watch it acquire twelve topics whose log entries exist nowhere.
3. **Watch a minority fail to elect.** `python -m chaos.split_brain` — the
   partition that produced two controllers now produces one. Count them, then
   read the TERM column: the minority keeps campaigning and keeps losing.
3. **Break the election restriction.** Remove the "log at least as up to date"
   clause from `RaftLog.is_up_to_date`. Every happy-path test still passes.
   Find the one that does not, and work out what it would cost in production.
4. **Break persistence.** Make `votedFor` in-memory only. How would you even
   *detect* the resulting double vote?
3. **Prove routing works.** Start 3 brokers, produce 1,000 keyed messages, then
   `kfk cluster status`. Each broker should hold only the partitions it leads.
4. **Force a stale cache.** Produce, kill the leader of one partition, produce
   again to the same key. Count the `NOT_LEADER_OR_FOLLOWER` responses in the
   logs — that is failover working.
5. **Break placement determinism.** Make `_assign_leaders` iterate an unsorted
   set of broker ids. Restart twice. Why does the assignment change, and what
   would that do to a running cluster?

---

## 8. Verification

```bash
pytest -q
python -m chaos.kill_controller
python -m chaos.split_brain

# three terminals
python -m broker.broker --node-id 1 --cluster cluster.json --port 9092 --data-dir ./data/b1
python -m broker.broker --node-id 2 --cluster cluster.json --port 9093 --data-dir ./data/b2
python -m broker.broker --node-id 3 --cluster cluster.json --port 9094 --data-dir ./data/b3

python -m tools.kfk topics create --topic orders --partitions 6
python -m producer.producer --topic orders --count 2000
python -m tools.kfk cluster status
```

The result is **partitions spread across brokers** and **clients reaching the
right one without being told**.

→ Next: **Phase 6 — Replication**, which makes losing a broker survivable
rather than merely detectable.
