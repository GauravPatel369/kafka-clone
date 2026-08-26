# Phase 4 — Consumer Groups, the Coordinator, and Delivery Semantics

> **Goal:** many consumers sharing a topic's partitions between them,
> reassigning automatically when one dies, with their positions stored on the
> broker instead of a local file.
>
> Phase 3 balanced the **write** side. This is the read side, and it is where
> failure handling for consumers becomes real.

---

## 1. The problem

### `group_id` has been a filename since Phase 0

```python
self._offset_file = os.path.join(offset_dir, f"{topic}-{partition}-{group_id}.txt")
```

That is the whole of it. The broker has never heard of a group. Consequences:

- **Two consumers in the same group both read everything.** They do not divide
  the work; they duplicate it, and then fight over one offset file.
- **Consumption cannot scale.** Phase 3 gave a topic four partitions. Nothing
  can use them in parallel.
- **A dead consumer's partitions stay dead.** Nobody notices, nobody takes over.

### Offsets live on the wrong machine

A consumer's position is in `./offsets/orders-0-mygroup.txt`. Move the consumer
to another host and it starts from zero. Run it in a container and the position
dies with the container. The position belongs to the *group*, not to a machine,
and storing it on a machine is a category error.

### Nothing detects a dead consumer

There is no heartbeat, no timeout, no liveness concept at all. A consumer that
is killed, or merely stuck in a 40-second GC pause, is indistinguishable from
one that is working — and its partitions simply stop being read.

---

## 2. How real Kafka solves it

### A group coordinator, and the partitions get divided

```
   topic "orders", 4 partitions        group "billing", 3 members

   p0 ──────────────────────────────►  consumer A
   p1 ──────────────────────────────►  consumer A
   p2 ──────────────────────────────►  consumer B
   p3 ──────────────────────────────►  consumer C
```

**Each partition is read by exactly one member.** Add a consumer and partitions
redistribute; kill one and its partitions move to the survivors.

> **The partition count is therefore the ceiling on consumer parallelism.** Four
> partitions means at most four useful consumers in a group; a fifth sits idle
> forever. This is the single most common surprise for people new to Kafka, and
> it is why you over-provision partitions.

A *different* group reading the same topic gets its own full copy of the
stream — that is the log-not-queue property from Phase 0, still holding.

### The join dance

Kafka's rebalance protocol is three requests, and the shape is worth learning
because the reasoning behind each step is not obvious:

```
  1. JOIN_GROUP   every member announces itself and its subscriptions.
                  The coordinator collects them, bumps the GENERATION, and
                  picks one member as LEADER.
                  → everyone gets: generation, member_id
                  → the leader also gets: the full member list

  2. (the leader computes the assignment, on the client)

  3. SYNC_GROUP   the leader uploads the assignment for everyone.
                  Other members send nothing and receive their own share.
                  → group becomes STABLE

  4. HEARTBEAT    every few seconds, forever, to prove liveness.
```

**Why does a *client* compute the assignment?** Because then the broker does
not need to know your assignment strategy. You can ship a custom assignor —
rack-aware, weighted, sticky — without changing or restarting a single broker.
The coordinator's job is membership and fencing, not policy. That separation is
why Kafka has many assignors and one coordinator.

### Generation IDs fence zombies

Every rebalance increments a **generation** number. Every request carries it.

```
   generation 7 : consumer A owns p0, commits offset 100
   consumer A pauses for a 40-second GC
   coordinator times it out → generation 8, p0 goes to consumer B
   consumer B reads from 100, processes, commits 150
   consumer A wakes up and commits 120 with generation 7
                                              ↑
                    REJECTED: ILLEGAL_GENERATION. A is a zombie.
```

Without fencing, the zombie's stale commit would rewind the group and cause 30
messages to be reprocessed. The generation number is what makes "you are no
longer who you think you are" expressible.

### Offsets go into a topic

Real Kafka stores committed offsets in `__consumer_offsets`, an internal
**compacted** topic keyed by `(group, topic, partition)`. The coordinator keeps
the current values in memory and replays the log to rebuild that map on
startup.

The elegance is that Kafka needed a durable, replicated, ordered key-value
store — and it already had one. Offsets get replication and durability for free
because they are just messages.

---

## 3. Options considered

### Where do committed offsets live?

| Option | Trade-off |
|---|---|
| Local file (Phases 0–3) | No broker involvement. Position dies with the machine; a group cannot share it; nothing can be reassigned. |
| A dedicated database | Familiar, and now the broker has an external dependency and a second consistency story. |
| **An internal topic** ← chosen | Durable, ordered, and replicated by the same machinery as everything else. Costs: the topic grows without compaction, which we do not implement. |

### Who computes the assignment?

| Option | Trade-off |
|---|---|
| The broker | Simpler clients. But every assignment strategy needs a broker change and a cluster restart to deploy. |
| **The group leader (a client)** ← chosen | Strategy is a client concern, so a custom assignor ships with your application. Cost: a more complicated join protocol, and the leader can be a badly-behaved client. |

### Which assignment strategy?

| Strategy | Trade-off |
|---|---|
| **Range** ← default | Per topic, hand out contiguous runs. Simple, and co-partitioned topics line up — member A gets `orders-0` *and* `payments-0`, which matters if you join them. Skews when partitions do not divide evenly, and the skew is the same member every time. |
| **Round-robin** | Deal all partitions out in turn. Best balance across many topics; loses the co-partitioning property. |
| Sticky | Balanced *and* minimises movement between rebalances. Much more complex; Kafka added it years later. Not implemented. |

### How aggressive should failure detection be?

`session_timeout_ms` is a genuine dilemma with no good answer:

| Setting | Effect |
|---|---|
| Short (3 s) | Dead consumers detected fast. A GC pause or a slow network blip triggers a **spurious rebalance**, which stops consumption for everyone. |
| Long (60 s) | Stable under hiccups. A genuinely dead consumer's partitions go unread for a minute. |

Kafka's answer is two separate timeouts: `session.timeout.ms` for the heartbeat
thread (liveness of the *process*) and `max.poll.interval.ms` for the poll loop
(liveness of the *processing*). Separating them means slow processing no longer
looks like a dead process. We implement the first and document the second.

### Eager or cooperative rebalancing?

| Option | Trade-off |
|---|---|
| **Eager (stop-the-world)** ← chosen | Every member revokes everything, then receives a new assignment. Simple and correct. **All consumption stops** for the duration. |
| Cooperative incremental | Only the partitions that must move are revoked. Kafka's `CooperativeStickyAssignor`. Needs multiple rebalance rounds and much more state. |

We implement eager because it is what makes the mechanism visible — and the
chaos script shows the pause it causes, which is the honest cost.

---

## 4. The decision

### Six new APIs

Kafka's real api_keys, reserved back in Phase 1 and now implemented:

| Key | API | Purpose |
|---:|---|---|
| 8 | `OFFSET_COMMIT` | Store a group's position, fenced by generation |
| 9 | `OFFSET_FETCH` | Read it back |
| 10 | `FIND_COORDINATOR` | Which broker owns this group (always us until Phase 5) |
| 11 | `JOIN_GROUP` | Announce membership; triggers a rebalance |
| 12 | `HEARTBEAT` | Prove liveness; learn a rebalance has started |
| 13 | `LEAVE_GROUP` | Depart cleanly instead of waiting to time out |
| 14 | `SYNC_GROUP` | Leader uploads assignments; members collect theirs |

### The coordinator holds a state machine

```
        EMPTY
          │ first JOIN_GROUP
          ▼
   PREPARING_REBALANCE ◄──── a member joins, leaves, or times out
          │ all members joined, or rebalance timeout
          ▼
   COMPLETING_REBALANCE
          │ leader's SYNC_GROUP arrives
          ▼
        STABLE ─────────────► heartbeats accepted here
```

`JOIN_GROUP` **waits** — the coordinator holds the request open until every
known member has rejoined or the rebalance timeout fires. That is exactly what
real Kafka does (it calls the holding area "purgatory"), and it is why an
asyncio broker is a good fit: an awaited `asyncio.Event` costs nothing while a
thread-per-request server would have one thread blocked per member.

### The consumer gains two modes

```python
consumer.subscribe(['orders'])          # group-managed: coordinator assigns
consumer.assign([('orders', 2)])        # manual: you choose, no group, no rebalance
```

`assign()` is not a lesser version of `subscribe()` — it is the right choice
when you want exactly one specific partition and no coordination (a
partition-repair tool, a debugging consumer). It skips heartbeats entirely.

A background **heartbeat thread** proves liveness on its own schedule, so a
slow `process_fn` does not look like a dead process — that separation is the
whole reason it is a thread and not a piggyback on `poll()`.

### Offsets move server-side

`__consumer_offsets` is a real topic on the broker, written through the same
`Log`. The coordinator keeps `(group, topic, partition) → offset` in memory and
rebuilds it by replaying that log at startup.

**Known gap, stated plainly:** real Kafka *compacts* this topic so it stays
proportional to the number of live keys rather than to the number of commits.
We did not implement compaction in Phase 2, so this log grows forever. A
consumer committing every second adds ~86,000 records a day. Documented rather
than hidden; the fix is Phase 2's cleaner learning a second policy.

---

## 5. Code walkthrough

| File | Role |
|---|---|
| [`broker/coordinator.py`](../../broker/coordinator.py) | Group state machine, generations, membership, timeouts. |
| [`broker/offset_store.py`](../../broker/offset_store.py) | `__consumer_offsets` — durable commits plus the in-memory map. |
| [`consumer/assignor.py`](../../consumer/assignor.py) | Range and round-robin. |
| [`consumer/consumer.py`](../../consumer/consumer.py) | `subscribe`/`assign`, heartbeat thread, multi-partition poll. |
| [`broker/broker.py`](../../broker/broker.py) | The six new handlers. |
| [`tools/kfk.py`](../../tools/kfk.py) | `groups list` / `describe`, with per-partition lag. |

### Fencing, which is four lines and the whole point

```python
def _check_generation(self, group, member_id, generation_id):
    if member_id not in group.members:
        raise UnknownMemberIdError(...)          # you were evicted
    if generation_id != group.generation_id:
        raise IllegalGenerationError(...)        # the world moved on
```

Every commit and every heartbeat goes through it. A consumer that was timed out
and has since been replaced cannot write anything — its generation is stale, so
its commit is rejected rather than silently rewinding the group.

---

## 6. How real Kafka differs

- **No compaction on `__consumer_offsets`**, as above — the one significant gap.
- **`max.poll.interval.ms`.** Real Kafka separates "the process is alive"
  (heartbeat) from "the poll loop is progressing". We only implement the first,
  so a consumer stuck inside `process_fn` keeps heartbeating and holds its
  partitions.
- **Static membership.** `group.instance.id` lets a restarting consumer reclaim
  its partitions without triggering a rebalance. We rebalance every time.
- **Cooperative rebalancing**, as above.
- **The coordinator is per-group and distributed.** Real Kafka picks the
  coordinator broker by hashing the group id, so groups spread across the
  cluster. With one broker, `FIND_COORDINATOR` always answers "me" — the API
  exists now so Phase 5 adds a cluster, not a protocol change.

---

## 7. Exercises

1. **Watch a rebalance.** Run `python -m chaos.kill_consumer` and read the
   assignment tables before and after.
2. **Find the ceiling.** Start five consumers on a four-partition topic. What
   does the fifth do? Now add a partition — does it help immediately?
3. **Cause a spurious rebalance.** Set `session_timeout_ms=1000` and put a
   `time.sleep(3)` in your processing. Watch the group thrash. This is what
   `max.poll.interval.ms` exists to prevent.
4. **Make a zombie.** Pause a consumer past its session timeout, let another
   take over and commit, then let the first commit. Confirm it is rejected with
   `ILLEGAL_GENERATION` and work out what would have happened without fencing.
5. **Compare assignors.** Assign 7 partitions to 3 members with range, then
   round-robin. Which member is over-loaded, and is it the same one each time?
6. **Measure the offsets topic.** Commit 10,000 times and check
   `kfk segments --topic __consumer_offsets`. How many distinct keys does it
   actually hold? That difference is what compaction would reclaim.

---

## 8. Verification

```bash
pytest -q
python -m chaos.kill_consumer

python -m broker.broker                                    # terminal 1
python -m tools.kfk topics create --topic orders --partitions 4
python -m producer.producer --topic orders --count 2000
python -m consumer.consumer --topic orders --group billing --loop   # x3
python -m tools.kfk groups describe --group billing        # assignment + lag
```

The result is the **assignment table** and the **lag column**, and that a
killed consumer's partitions are picked up by the survivors.

→ Next: **Phase 5 — Cluster**, where these partitions stop living on one
machine.
