# Phase 6 — Replication: ISR, the High Watermark, and Surviving a Dead Broker

> Phase 5 made the system **distributed**. This one makes it **fault tolerant**.
> Those are different words and the gap between them is exactly one phase wide.

---

## 1. The problem

`chaos/kill_controller.py` already prints the confession, at the end of a run
that otherwise looks like a success:

```
    broker 1 now leads 3 partitions and can serve 99 messages from them
    Before the kill, broker 3 held 208 messages.
```

Leadership moved. **The bytes did not.** A partition reassigned to a new broker
starts with an empty log, because a partition lives in exactly one place:

```
   broker 1          broker 2          broker 3
   orders-0          orders-1          orders-2
   orders-3          orders-4          orders-5
        │                 │                 │
        └── the ONLY copy of that data, anywhere
```

Kill broker 3 and `orders-2` and `orders-5` are gone. Not unavailable —
**gone.** The controller dutifully assigns them to a survivor, which creates
two empty directories and starts writing at offset 0, and every consumer that
had committed offset 4,000 is now told the log holds 12 messages.

Phase 5 bought capacity and routing. It bought no durability at all, and it is
worth being blunt that a five-broker cluster with no replication is *less*
reliable than a single broker: five machines that can each lose data
independently.

### What Raft did and did not fix

Phase 5 ended with Raft, which is easy to mistake for having solved this. It
did not, and the boundary is the single most important idea in this phase:

| | replicated by Raft | not replicated |
|---|---|---|
| **Control plane** | who leads what, which topics exist | |
| **Data plane** | | **every message anyone ever produced** |

Raft replicates *decisions about* partitions — kilobytes, changing a few times
an hour. It does not replicate partition *data* — gigabytes, changing thousands
of times a second. Running message traffic through Raft would mean an fsync and
a majority round trip **per message**, which is roughly a 100× slowdown to
solve a problem that has a cheaper answer.

That cheaper answer is this phase.

---

## 2. How real Kafka solves it

Every partition gets a **replica set**: one leader and N-1 followers.

```
   orders-2   replicas [3, 1, 2]     leader = 3
   ────────────────────────────────────────────────
   broker 3   ████████████████████   leader, takes all reads and writes
   broker 1   ████████████████████   follower, fetching
   broker 2   ██████████████         follower, behind
```

Four mechanisms, and they only make sense together.

### Followers *pull*; the leader does not push

A follower runs a fetch loop against the leader — the **same `FETCH` API a
consumer uses**, with a `replica_id` set. It reads from its last offset,
appends what it gets to its own local log, and asks again.

Pull rather than push is not an arbitrary choice. It means:

- the leader keeps no per-follower send queues, and a slow follower costs the
  leader nothing but a position integer;
- a follower that restarts resumes from its own log rather than needing the
  leader to know where it got to;
- **the code already exists.** A follower is a consumer with a different id,
  which is why Kafka's replication protocol is a thin layer over its read path
  rather than a second system.

### The high watermark is what a consumer may see

The leader tracks how far each follower has got. The **high watermark** is the
highest offset present on *every* in-sync replica.

```
   leader log    0 1 2 3 4 5 6 7 8 9
   follower A    0 1 2 3 4 5 6 7
   follower B    0 1 2 3 4 5 6 7 8
                               ▲
                        HW = 8 (one past the last fully-replicated offset)

   consumers may read 0..7.   Offsets 8 and 9 exist but are INVISIBLE.
```

Withholding data the leader already has looks perverse until you ask what
happens without it. Suppose a consumer reads offset 9, and the leader then
dies. Neither survivor has offset 9. The new leader's log ends at 8, and the
next message it accepts becomes offset 9 — **a different message with an offset
a consumer has already read and acted on.**

That is not data loss. It is worse: it is two different messages sharing an
offset, which breaks the one guarantee everything else rests on. The high
watermark is the price of an offset naming exactly one message, forever.

Note what this makes true: `high_watermark` in a FETCH response stops meaning
*"everything written"* and starts meaning *"everything safely replicated."* The
lag number every Kafka operator watches becomes a much more interesting number.

### The ISR is the set that is keeping up

A replica is **in-sync** if it has fetched within `replica.lag.time.max.ms`.
Fall behind and you are ejected; catch up and you are re-added.

The ISR exists to stop one sick machine from freezing the cluster. Without it,
`acks=all` waits for *all* replicas, so a single follower with a failing disk
stalls every write to that partition. With it, the slow replica is dropped and
writes continue on the remainder.

**A leader may only be elected from the ISR** — which is why the ISR is a
durable, controller-managed set and not just a leader's private opinion.

### `acks` is where you choose your failure

| `acks` | Leader waits for | Loses data when | Latency |
|---|---|---|---|
| `0` | nothing | anything at all | none |
| `1` | its own log | leader dies before a follower fetches | one disk write |
| `all` | every ISR member | the whole ISR dies | one replication round trip |

With `min.insync.replicas=2`, `acks=all` **refuses the write** when the ISR
shrinks to one. The producer gets `NOT_ENOUGH_REPLICAS` and the data stays
where it can still be retried.

This is the CAP choice, made explicit and configurable per topic:
`acks=all` + `min.insync.replicas=2` picks **consistency** — writes fail during
a partition. `acks=1` picks **availability** — writes succeed, and some of them
evaporate. There is no third option, and any system claiming one is hiding
which of these two it picked.

---

## 3. Options considered

### How does a follower get the data?

| Option | Cost |
|---|---|
| **Follower pulls with `FETCH`** | Reuses the entire read path. Leader is stateless per follower. One extra round trip of latency. |
| Leader pushes | Lower latency. Leader needs per-follower queues, retry state, and backpressure — a second replication system next to Raft's. |
| Run partition data through Raft | One consensus implementation instead of two. An fsync and a majority round trip *per message*: unusable. |

**Follower pulls.** The third option is worth stating precisely because it
sounds elegant. Raft is already there, and reusing it would mean one mechanism
rather than two. It fails on numbers: metadata changes a few times an hour,
messages arrive thousands of times a second, and a design that treats them the
same is priced for the wrong one.

### When is a write acknowledged?

Covered by `acks` above — but the real decision is **what the default should
be.** Kafka's default is `acks=1`, and this clone keeps it. A default that
silently loses data on leader failure is a strange thing to defend, and the
defence is that the alternative is worse: `acks=all` as a default makes every
first-time user's benchmark look broken, and they turn it off without reading
why it was on.

Better to make the trade-off *visible* — which is `chaos/kill_leader.py`'s
entire job — than to pick for them and be ignored.

### Where does the replica assignment come from?

The controller, proposed through Raft, exactly like the leader map today. This
is nearly free: [`controller.py:99`](../../broker/controller.py#L99) already
computes a placement, proposes it, and applies it only on commit. Phase 6
widens the value from `leader` to `{leader, replicas, isr}` and inherits the
whole commit path.

### How does a follower whose log diverged recover?

When a leader dies, a follower may hold uncommitted records the new leader
never saw — exactly Raft's divergence problem, one layer down.

| Option | Cost |
|---|---|
| **Truncate to the high watermark** | Simple. Discards only records that were never visible to a consumer. |
| Truncate to the new leader's log end | Equivalent here, and needs an extra round trip to discover it. |
| Keep both and reconcile | Not possible: offsets are positional, so "both" is a contradiction. |

Truncate to the HW, then start fetching. The follower discards records nobody
was ever allowed to read — which is precisely the definition the high watermark
exists to provide, used a second time.

---

## 4. The decision

Six steps, each one leaving a system that runs.

### Step 1 — Replica sets in the controller ✅

`compute_assignment` returns `{topic, partition, leader, replicas}` instead of
a bare leader. Round-robin the leader as today, then take the next N-1 brokers
for the followers, so replicas of one partition never land on one machine.

Nothing copies data yet. This step is metadata only, and it is the cheap one.

**Done.** A three-broker cluster now places `orders` with RF=3 as:

```
orders-0  leader 1  replicas [1, 2, 3]
orders-1  leader 2  replicas [2, 3, 1]
orders-2  leader 3  replicas [3, 1, 2]
```

— six copies per broker, leadership still 2/2/2, all three brokers agreeing.

Two things this step also cleaned up, both pre-existing:

- **One placement algorithm, not two.** `Controller.assign_leaders` and
  `MetadataStateMachine.compute_assignment` were separate implementations of
  the same rule and only the first was ever called. Placement now lives in the
  state machine; the controller supplies the inputs.
- **`replication_factor` is committed with the topic.** It had been carried
  unused in `TopicConfig` since Phase 3 and gated off in `create_topic`. Not
  putting it in the `CREATE_TOPIC` command cost an hour and became
  [journal #29](../challenges-and-solutions.md) — every unit test passed while
  a real cluster placed one copy of everything.

One hole found and *not* fixed: **auto-created topics never reach the Raft
log.** A topic born inside a PRODUCE exists in one broker's local store and
nowhere else, so placement has to read the local store rather than the
committed set. Worth fixing; not Phase 6's job.

### Step 2 — The fetcher loop (`broker/replica.py`) ✅

A background task per followed partition. Read the local log end, `FETCH` from
the leader with `replica_id`, append what comes back, repeat.

This is where the data actually moves, and where the Phase 2 storage engine
pays off: a follower appends through the same `Log` class a producer does.

**Done.** 450 messages across 3 partitions on a real 3-broker cluster:

```
PARTITION   LEADER  REPLICAS    DEPTH PER BROKER
orders-0    1       [1, 2, 3]   {1: 150, 2: 150, 3: 150}
orders-1    2       [2, 3, 1]   {1: 150, 2: 150, 3: 150}
orders-2    3       [3, 1, 2]   {1: 150, 2: 150, 3: 150}
```

— and the followers' records are byte-for-byte identical to the leader's, not
merely the same count.

The one genuinely new piece of storage code is `Log.append_replicated`, and it
exists for a single rule: **a follower copies offsets, it never assigns them.**
`append()` stamps `next_offset` onto each record, which is exactly right for a
leader — deciding that a message is number 41 is the leader's job. A follower
doing the same would be *re-deciding* it, and the moment the two logs differ by
one record, every later offset names a different message on the two machines.

It also refuses two things rather than papering over them:

- a record **at or before** our end is skipped, because a follower re-fetching
  after a restart legitimately receives records it already holds, and writing
  them again would put two messages at one offset;
- a record that would leave a **gap** raises, because the fetcher always asks
  from its own log end — so a gap means the leader answered a question nobody
  asked, and a log with a hole silently breaks the sparse index, the high
  watermark, and every read that crosses it.

`replica_id` flows end to end but currently only changes a log line. That is
deliberate: the plumbing is verified before Step 3 makes it load-bearing.

### Step 3 — The high watermark ✅

The leader tracks `follower_end_offset` per replica, from the offsets they ask
for. HW is the minimum across the replica set. `_handle_fetch` clamps a
consumer's read to the HW; a `replica_id` fetch is *not* clamped, or followers
could never catch up past it.

#### The position comes from the request, not from a reply

A follower asking for offset 150 has, by asking, proved it holds 0..149. That
is the entire accounting mechanism, and it is worth noticing what it avoids:
no acknowledgement message, no separate reporting API, no state the leader has
to reconcile. **The request that advances a follower is also the report that it
advanced.**

It also means the leader learns a follower's position exactly one round trip
late — the follower has 0..149 *and* whatever it appended from the previous
response, but the leader will not know until the next request. The HW is
therefore always slightly conservative, which is the correct direction for a
safety bound to be wrong in.

#### Why the asymmetry is the whole idea

```
   consumer  FETCH orders-0 from 0        → clamped to HW.  Sees 0..7.
   follower  FETCH orders-0 from 8  +id   → NOT clamped.    Sees 8, 9.
```

The clamp cannot apply to followers, and the reason is circular in a way that
takes a moment to see: **the act of a follower reading a record is what makes
that record safe for consumers.** Clamp the follower and no record ever gets
replicated past the HW, so the HW never advances, so nothing is ever readable.
The system would deadlock at offset 0 while looking perfectly healthy.

#### The bound that makes an offset mean one thing

Withholding data the leader already has looks perverse until you ask what
happens without it:

```
   leader   0 1 2 3 4 5 6 7 8 9      consumer reads offset 9, acts on it
   A        0 1 2 3 4 5 6 7          leader dies
   B        0 1 2 3 4 5 6 7 8
                                     B is elected, its log ends at 8
                                     next produce becomes offset 9
```

Offset 9 now names a *different message* from the one a consumer already read
and acted on. That is worse than data loss: it is two messages sharing an
offset, which breaks the guarantee every other guarantee is built on.

#### An unreplicated partition must not stall

With RF=1 the replica set is the leader alone, so the HW is the leader's own
log end and the clamp is a no-op. This matters more than it sounds: every
Phase 0–5 test, every single-broker run, and `__consumer_offsets` all depend on
it. A high watermark that required a *second* replica would make a one-broker
cluster unable to serve its own writes.

#### Measured

Three brokers, RF=3, one partition. Produce 20 records, then cut both
followers off and produce 15 more:

```
HEALTHY CLUSTER
  log_end=20  hw=20  consumer sees 20 records

FOLLOWERS PARTITIONED (15 more records produced)
  log_end=35  hw=20  consumer sees 20 records      ← CLAMPED
  replica positions: {2: 20, 3: 20}

HEALED
  log_end=35  hw=35  consumer sees 35 records
  replica positions: {2: 35, 3: 35}
```

The leader **holds 35 records and serves 20** for as long as its followers are
unreachable, then releases all 35 the moment they catch up. Nothing is lost and
nothing is exposed early — which is the entire contract.

**Checkpoint reached.** Steps 1–3 give real replication: copies exist, they
stay current, and a consumer cannot see a record that only one machine holds.
Everything after this is about **failure** — what happens when a replica stops
keeping up (Step 4), when a producer wants to wait for the copies (Step 5), and
when the leader dies (Step 6).

### Step 4 — ISR tracking ✅

Eject a replica that has not fetched within `replica.lag.time.max.ms`; re-admit
it when it reaches the HW.

#### Why the watermark alone is not enough

Step 3's high watermark is the minimum across **every** replica. That is safe
and it is fragile: one follower with a dying disk stops the watermark
advancing, and every consumer on that partition stops seeing new records while
the leader happily keeps accepting writes. Nothing is broken, nothing errors,
and the partition is functionally dead.

The ISR is the fix. The watermark becomes the minimum across the **in-sync**
set, and a replica that stops keeping up is removed from it:

```
   replicas [1, 2, 3]        ISR [1, 2, 3]     HW = min(all three)
   broker 3 stops fetching
   ...lag.time.max.ms passes
   replicas [1, 2, 3]        ISR [1, 2]        HW = min(1, 2)   ← moves again
```

The replica set did not change — broker 3 is still *supposed* to hold a copy,
and will be re-admitted when it catches up. What changed is whether anyone
waits for it.

#### Time, not offsets

A replica is out of sync when it has not **fetched** for
`replica.lag.time.max.ms`, not when it is more than N records behind.

That distinction matters and Kafka learned it the hard way — it used to have
`replica.lag.max.messages` and removed it in 0.9. A message-count threshold
cannot tell a broken replica from a healthy one during a traffic spike: a
producer burst puts every follower thousands of records behind for a moment,
and a count-based rule ejects the entire ISR at exactly the moment durability
matters most. A time-based rule asks a different question — *is this replica
still talking to me?* — and a burst does not change the answer.

#### The ISR belongs to the controller, not to the leader

A leader observes lag; only the controller may *record* it, by proposing an
`ASSIGN_PARTITIONS` through Raft.

This is not ceremony. **Leader election reads the ISR** (Step 6): a new leader
must come from the in-sync set, or it may be missing records a producer was
told were committed. If the ISR were a leader's private opinion, a partitioned
leader could shrink it to itself alone and then hand that belief to a successor
that never agreed. Committing it means the set survives the leader that
computed it — which is the entire reason it exists.

The cost is a Raft round trip per ISR change, and a shrink is therefore not
instant. That is the correct trade: ISR changes should be rare, and one that is
worth acting on is worth agreeing on.

#### Measured

Three brokers, RF=3, `replica.lag.time.max.ms` = 1500:

```
HEALTHY            ISR [1, 2, 3]   hw=20  log_end=20
PARTITION b3       ISR [1, 2, 3]   hw=20  log_end=35   ← b3 still counted, HW stuck
...1.5 s later     ISR [1, 2]      hw=35  log_end=35   ← b3 ejected, HW freed
HEAL               ISR [1, 2, 3]   hw=35  log_end=35   ← b3 caught up, re-admitted
```

The middle two lines are the whole feature: the watermark was **stuck at 20
with 15 records withheld**, and ejecting the dead replica released them without
losing anything or waiting for a machine that was not coming back.

### Step 5 — `acks=all` and `min.insync.replicas` ✅

The producer already sends `acks`. The leader now holds the response until the
high watermark passes the written offset, and refuses the write outright when
the ISR is too small.

#### `acks=all` waits for the watermark, not for a count

The implementation is one line of insight: **the high watermark already means
"every in-sync replica has this."** So `acks=all` does not need to count
acknowledgements or track per-follower state — it waits for `hw > offset` and
it is done. Step 3 built the mechanism; Step 5 just consults it.

#### `min.insync.replicas` is where the CAP choice becomes visible

```
   ISR [1, 2, 3]   min.insync=2   → write accepted
   ISR [1, 2]      min.insync=2   → write accepted
   ISR [1]         min.insync=2   → NOT_ENOUGH_REPLICAS, refused
```

That third line is the system **choosing consistency over availability**, out
loud. The broker is healthy, the disk is fine, the producer is connected — and
the write is refused because accepting it would mean acknowledging data that
one machine failure could erase.

The check is deliberately *before* the append, not after. Writing first and
failing the acknowledgement would leave the record in the leader's log, where a
later ISR recovery would replicate it — so a producer that was told `FAILED`
would find its message in the topic anyway. Refusing up front makes the error
mean what it says.

**`min.insync.replicas` counts the ISR, not the replica set.** With RF=3 and
`min.insync=2`, losing one broker is survivable and losing two stops writes.
Setting `min.insync=3` with RF=3 means *any* single failure stops the partition
— which is why the standard advice is `RF=3, min.insync=2`: it tolerates one
failure while still guaranteeing every acknowledged write is on two machines.

#### Measured — and the prediction was wrong

Three brokers, RF=3, `min.insync=2`, 400 sequential messages per run:

| | msg/s | p50 | p99 |
|---|---:|---:|---:|
| `acks=1` | 706–722 | 1.3 ms | 2.9–4.4 ms |
| `acks=all` | 112–127 | 5.0–5.8 ms | 29–34 ms |
| | **≈ 6x slower** | | |

**Prediction 1 said 30–50%. The real cost is ~600%** — wrong by more than an
order of magnitude, and in the direction that matters. Recording why, because
the reasoning error is more useful than the number.

The prediction assumed `acks=all` adds *one follower round trip*, and priced
that round trip at the 0.035 ms loopback RTT measured in Phase 1. Two things
that reasoning missed:

1. **A follower discovers records by asking, not by being told.** So the wait
   is not a round trip — it is *however long until the follower next asks*.
   The first measurement made this brutally clear: **358x slower**, p50 218 ms,
   which is exactly `REPLICA_FETCH_IDLE_MS` of 200 ms paid once per message.
   The producer was waiting for the follower's sleep schedule.

2. **The wait is serial with everything else.** `acks=1` returns after one
   local append; `acks=all` returns after an append, a follower's poll, its
   append, its *next* poll (which is what reports the position), and the
   leader noticing. That is several scheduling hops, and on loopback the
   scheduling dominates because the network is free.

The fix for (1) is **long-polling**, which is what Kafka's `fetch.max.wait.ms`
is for: the leader holds an empty fetch open instead of answering "nothing",
and returns the instant a record lands. That took 358x down to ~6x. The
remaining 6x is (2), and it is real.

> **The lesson, again.** Phase 1 predicted a pipelining win and got 1.05x
> because the broker was never idle. Here the prediction under-counted by 12x
> because it modelled replication as a network cost when it is really a
> *scheduling* cost. Both times, the number was reasoned from an architecture
> diagram rather than from what the code actually waits on.

The honest headline: **`acks=all` costs about 6x throughput on loopback, and
almost all of that is scheduling rather than network.** On a real network with
a 0.5 ms RTT the ratio would *fall*, because `acks=1` gets slower while
`acks=all` barely changes — which is exactly the inversion Phase 1 found for
pipelining, and the reason loopback benchmarks flatter the wrong designs.

### Step 5 — `acks=all` and `min.insync.replicas`

The producer already sends `acks`. The leader now holds the response until the
HW passes the written offset, and returns `NOT_ENOUGH_REPLICAS` when
`len(isr) < min.insync.replicas`.

### Step 6 — Promotion from the ISR, and the leader epoch ✅

The step that turns *"the copies exist"* into *"a broker can die without taking
data with it."* Two mechanisms that only work together.

#### Promotion: prefer a leader that already has the data

Steps 1–5 placed leaders by walking the broker ring. That is correct for a
fresh partition and **wrong for a failover**: the ring hands the partition to
whichever broker comes next, which may be one that holds nothing.

Placement now asks a different question first — *who is in the ISR?* — and only
falls back to the ring when nobody is:

```
   orders-2   replicas [3, 1, 2]   ISR [3, 1]   leader 3
   broker 3 dies
   → live replicas ∩ ISR = [1]     leader 1     ← has the data
```

The ISR is what makes this safe rather than merely convenient. An in-sync
replica holds everything below the high watermark by definition, so **every
record a consumer was ever allowed to read is already on the new leader.**
Promote outside the ISR and that guarantee is gone.

#### What happens when the ISR is empty

Every replica of a partition is down, or none is in sync. Two options, and
Kafka makes it configurable:

| | Result |
|---|---|
| **Refuse to elect** (`unclean.leader.election.enable=false`) | The partition is unavailable until an in-sync replica returns. **No data loss.** |
| Elect anyway | The partition is available immediately, starting from whatever that replica has. **Silent data loss.** |

We refuse, and we do not offer the option. Worth being explicit that **refusing
is a choice, not an absence of one** — the partition going dark is the visible
cost of a guarantee, and an operator staring at an unavailable partition should
know it is protecting them.

#### The epoch: fencing a leader that does not know it was replaced

Promotion alone is not enough, and this is the failure it leaves:

```
   broker 3 leads orders-2, then is network-partitioned
   controller promotes broker 1
   broker 3 STILL BELIEVES it leads orders-2 and accepts writes
```

Broker 3 is not malicious or broken — it simply has not heard. Its writes go
into a log nobody will ever read, and a producer is told they succeeded.

A **leader epoch** closes this: a monotonically increasing number, bumped every
time a partition's leadership changes, committed through Raft with the
placement. Clients receive it in `METADATA` and send it back on produce and
fetch, and **a broker refuses a request whose epoch is newer than its own** —
because the only way a caller can know a higher number is that leadership moved
and this broker has not heard.

#### Getting the comparison backwards fences nothing

The first implementation checked `request_epoch < broker_epoch` — refusing the
stale *client* — which is the harmless direction. Every test passed. Then a
direct check produced to an isolated leader with a newer epoch and it
**succeeded**:

```
after failover: leader=2 epoch=2
  old leader 1 still thinks epoch=1, leads=True
  produce to STALE leader carrying epoch 2 → error_code=0     ← fenced nothing
```

After the fix, the same call returns `error_code=74`, *"broker 1 believes it
leads orders-0 at epoch 1, but the caller knows epoch 2; this broker is
stale."*

Worth dwelling on: the mechanism was present, the number was committed and
propagated correctly, the error code existed, and it protected nothing. A
fencing check pointed the wrong way is indistinguishable from a working one
until something is actually stale.

#### What the epoch does not do

Be exact about the promise, because it is narrower than "a stale leader cannot
write."

Fencing needs **somebody to know the newer number**. A client that has not
refreshed sends no epoch; an isolated leader cannot learn one. So an isolated
leader with an unaware client will keep accepting `acks=1` writes, and nothing
in the system is in a position to object — as `chaos/kill_leader.py`
demonstrates by losing exactly those messages.

What actually protects that case is `acks=all`: the isolated leader physically
cannot satisfy it, because its ISR has collapsed to itself. The epoch protects
the cases where the knowledge exists somewhere — a refreshed client, or a
broker that comes back.

The alternative would be a leader that stops serving when it cannot reach its
own ISR, which is the data-plane analogue of Raft's check-quorum. Kafka does
not do this, and neither do we.

**The Raft term is not the epoch, and that was worth getting right.** The term
increments when the *controller* changes; the epoch must increment when a
*partition's* leader changes. One controller can reassign many partitions
across many terms, and one term can span many reassignments. Reusing the term
would fence a partition whose leadership never moved, and fail to fence one
that moved twice inside a term. So the epoch is per partition, incremented by
the controller when it observes a leader change for that partition.

#### Where fencing is enforced

`_not_leader` already gated every PRODUCE, FETCH and LIST_OFFSETS. It now also
compares epochs, which makes the check answer two different questions with two
different error codes:

| Situation | Code |
|---|---|
| I am not the leader | `NOT_LEADER_OR_FOLLOWER` — retriable, refresh and retry |
| I *think* I am, but my epoch is stale | `FENCED_LEADER_EPOCH` — I am the stale one |

Separating them matters for diagnosis. The first is routine, and happens on
every placement change. The second means a broker was serving a partition it
had lost, and that is a thing an operator should be able to see in a log.

#### Measured

`chaos/kill_leader.py`, the demonstration this phase was built for. Same crash
twice, one setting changed:

```
acks=all   produced 400, acknowledged 400, readable after failover 400   LOSS: 0
acks=1     produced 400, acknowledged 400, readable after failover 387   LOSS: 13
```

Thirteen messages the producer was **told were accepted** are gone in the
second run. They were in the dead leader's log and had not reached a follower
when it died. That is not a bug — it is exactly what `acks=1` means, made
visible.

> The first run is the sentence Phase 5 could not say: **a broker died and took
> no data with it.**

---

## 5. What this will cost

Two predictions, recorded now so they can be checked rather than quietly
revised later — Phase 1 predicted pipelining would raise produce throughput and
was wrong by an order of magnitude ([journal #7](../challenges-and-solutions.md)),
and Phase 5's snapshot ceiling was invented and wrong by 20× ([#28](../challenges-and-solutions.md)).

**Prediction 1: `acks=all` costs 30–50% of produce throughput on loopback.**
Reasoning: a write gains one follower fetch round trip. Loopback RTT measured
0.035 ms in Phase 1, against ~215 µs of broker work per message — so the round
trip is small next to the work, and the loss should be well under half.

**Prediction 2: `acks=1` throughput drops under 10%.** The leader does not wait
for followers, so the only new cost is followers reading from it — which
competes for the same thread pool.

If prediction 1 lands far from 30–50%, the interesting answer is *why*, and it
goes in the journal either way.

---

## 6. How this will differ from real Kafka

Written before the code, so the simplifications are chosen rather than
discovered:

- **No zero-copy.** Real Kafka serves a follower fetch with `sendfile`, moving
  bytes from page cache to socket without entering user space. We decode every
  record into Python objects and re-serialise them. This is already the largest
  performance gap in the project and replication doubles its importance.
- **No throttling.** Real Kafka rate-limits replication so a catching-up
  follower cannot starve consumers.
- **No rack awareness.** Kafka spreads replicas across failure domains; we
  spread them across broker ids and hope those differ.
- **No unclean leader election.** If the ISR empties, the partition stops. Kafka
  offers `unclean.leader.election.enable` to promote an out-of-sync replica —
  trading data loss for uptime. We will not implement the option, but the doc
  should be clear that refusing it *is* a choice, not an absence of one.
- **No `KIP-101` epoch-based truncation.** We truncate to the HW, which can
  discard slightly more than necessary in a rare double-failure. Kafka uses the
  leader-epoch history to truncate to the exact divergence point.

---

## 7. Exercises

1. **Watch the trade-off.** `python -m chaos.kill_leader` — kill a leader
   mid-produce with `acks=all`, then rerun with `acks=1`. One run reconciles;
   the other loses a message the producer was told had been accepted.
2. **Remove the high watermark clamp** in `_handle_fetch`. Every test passes.
   Construct the sequence where a consumer reads an offset that later belongs
   to a different message.
3. **Set `min.insync.replicas=3` on a 3-broker cluster, then kill one broker.**
   Explain to somebody why refusing every write is the *correct* behaviour.
4. **Make the ISR a leader-local set** instead of controller-managed. Find the
   sequence where two brokers disagree about who is in-sync and a partition
   ends up with a leader that is missing committed data.
5. **Measure prediction 1** before reading §5's reasoning, then compare.

---

## 8. Verification

```bash
pytest -q
python -m chaos.kill_leader
python -m bench.bench --count 20000 --acks all
python -m tools.kfk partitions describe --topic orders   # leader / replicas / ISR / HW
```

The result is **a broker can die without taking data with it** — which is the
sentence Phase 5 could not say.

→ Next: **Phase 7 — Capstone**, where a demo kills a broker mid-flight and
reconciles every message produced against every message consumed.
