# Phase 5 (addendum) — Snapshots: Throwing the Log Away Safely

> Step A of two. This document covers **taking a snapshot and discarding the
> log prefix**. Step B — the `InstallSnapshot` RPC, which catches up a follower
> that has fallen off the back of the log — is in
> **[raft-install-snapshot.md](raft-install-snapshot.md)**. §4 below explains
> exactly which hole it fills and why compaction is what opens it.

---

## 1. The problem

`RaftLog` never forgets anything.

```
index    1      2         3        4         5      ...    9,412
term     1      1         2        2         2             7
command  REG    CREATE    REG      ASSIGN    UNREG         ASSIGN
```

Two costs, both unbounded:

- **Disk.** The file grows forever. Nothing ever deletes an entry.
- **Restart time.** [`RaftLog._load`](../../broker/raft/log.py#L116) replays
  every record from byte zero to rebuild the in-memory list, and
  `_apply_committed` then re-applies all of them to the state machine. A node
  that has been running for a year pays for that year every time it restarts.

And here is the thing that makes it *silly* rather than merely expensive: after
applying entry 9,412, the first 9,411 entries tell you **nothing you don't
already know**. The state machine already holds their entire effect. `REGISTER
b1` followed later by `UNREGISTER b1` is two durable, fsynced, majority-
replicated records whose combined meaning is "b1 is not here" — which is also
what an empty dict says, for free.

The log is a *journal*. The state machine is the *answer*. Once you have the
answer, the journal ahead of it is archaeology.

---

## 2. How Raft solves it

Each node independently writes a **snapshot**: a dump of its state machine,
stamped with the index and term of the last entry included in it.

```
   BEFORE                                AFTER
   ┌─┬─┬─┬─┬─┬─┬─┬─┬─┐                   ┌─────────┐┬─┬─┐
   │1│2│3│4│5│6│7│8│9│                   │snapshot ││8│9│
   └─┴─┴─┴─┴─┴─┴─┴─┴─┘                   │ @ 7,t2  ││ │ │
                     ▲                   └─────────┘┴─┴─┘
             last_applied = 7                       ▲
                                             the log now STARTS at 8
```

Three properties make this safe, and all three matter:

1. **You may only snapshot up to `last_applied`.** Not `commit_index`, not
   `last_index`. A snapshot is a picture of the state machine, and the state
   machine has only seen entries up to `last_applied`. Snapshot past it and you
   are claiming to have applied things you have not.
2. **Snapshotting is a local decision.** No consensus, no RPC, no coordination.
   Every node has the same committed prefix, so every node can compact its own
   copy whenever it likes. Two nodes with snapshots at different indices are
   perfectly consistent with each other.
3. **`(last_index, last_term)` must survive the truncation.** This is the
   subtle one — see §4.

---

## 3. Options considered

### What goes in the snapshot?

| Option | Cost |
|---|---|
| **Serialise the state machine** | Must be able to write and re-read the state. Small, fast, simple. |
| Copy-on-write fork of the state | No pause while writing. Needs a persistent data structure or a real fork. |
| Log-structured compaction (per key) | This is Kafka's `__consumer_offsets` model, not Raft's. Keeps the log *as* the store. |

We serialise the state machine.
[`MetadataStateMachine.snapshot()`](../../broker/raft/machine.py#L176) already
existed for the admin CLI and returns exactly the right shape — brokers,
topics, partitions, `last_applied`. All that was missing was its inverse.

Our state is kilobytes. The fancy options exist because real state machines are
gigabytes and you cannot stop the world to write one. We can, and it takes
under a millisecond, so paying for the machinery would be paying for a problem
we do not have.

### When to snapshot?

| Option | Cost |
|---|---|
| **Every N applied entries** | One integer of config. Predictable log length. |
| Every N bytes of log | Truer to the actual cost. Needs byte accounting we do not keep. |
| On a timer | Snapshots an idle node repeatedly for no reason. |
| Only on shutdown | Free until the crash that skips it. |

Every N entries, `raft.snapshot.threshold`, default 1000. Deliberately larger
than this cluster ever reaches — see §6 for why that is a real problem and how
the tests deal with it.

---

## 4. The decision, and the part that is actually hard

Taking the snapshot is easy. **Truncating the log is where the bugs are**, and
they are all the same bug.

Look at the old accessor:

```python
def term_at(self, index):
    if 0 <= index < len(self._entries):
        return self._entries[index].term      # ← list position IS the log index
```

Every method in the file rested on one identity: **`_entries[i]` is the entry
at log index `i`**, held in place by a sentinel `LogEntry(0, 0)` parked at
position 0. Drop a prefix and that identity dies — position 0 is now log index
7, and six methods quietly return the wrong entry.

The fix is one concept applied consistently:

```python
self.snapshot_last_index = 7        # everything at or before this is in the snapshot
self._entries[0] = LogEntry(7, 2, SNAPSHOT_SENTINEL)

def _pos(self, index):
    return index - self.snapshot_last_index
```

**The sentinel is not deleted — it is relocated.** It used to mean "before the
beginning of time, term 0". It now means "the last entry I folded into the
snapshot, term 2". Its *job* never changed: be the thing `prev_log_index` can
point at when a follower is being caught up from the very start of what we
hold.

That is what keeps replication working across a compaction. A leader catching
up a follower from `first_index` sends `prev_log_index = snapshot_last_index`,
and the follower answers `matches(7, 2)` from its own sentinel without needing
the entry itself. Delete the sentinel instead of moving it and the first
`AppendEntries` after any compaction has no valid `prev_log_term` to send.

### The failure this creates

Compaction introduces a state that could not previously exist: **a follower can
need an entry the leader no longer has.**

```
leader   snapshot @ 40 │ 41 42 43 44
follower                        ▲ next_index = 12   ← the leader threw 12 away
```

The leader cannot build the message. `prev_log_index = 11` is inside its
snapshot; `term_at(11)` has nothing to return. The follower rejects, the leader
backs up, and they loop forever.

**This is precisely what `InstallSnapshot` exists to fix.** Step A shipped
without it — the leader logged `peer N needs a snapshot (wants index 12, log
starts at 41)` several times a second and the follower never converged — and
[Step B](raft-install-snapshot.md) closes it. That ordering was deliberate:
seeing the hole open, and watching a real cluster sit in it, is what makes the
RPC something other than a box to tick.

---

## 5. Code walkthrough

### `broker/raft/snapshot.py` — the file on disk

`SnapshotStore.save(last_index, last_term, state)` writes one record —
temp file, fsync, `os.replace` — the same atomic-swap pattern as
[`PersistentState`](../../broker/raft/state.py) and
[`RaftLog._rewrite`](../../broker/raft/log.py#L264). A crash mid-write leaves
the previous snapshot intact, never a half-written one.

**Order matters, and it is counter-intuitive:** write the snapshot *and fsync
it*, and only then truncate the log. Do it the other way and a crash in between
loses both copies of the same entries. The rule is the general one for any
"replace A with B" on disk: **B must be durable before A is destroyed.**

### `MetadataStateMachine.restore()` — the inverse of `snapshot()`

Rebuilds `brokers`, `topics`, `leaders`, `last_applied` from the saved dict.
The one thing it must get right is being a *replacement*, not a merge: a
restoring node clears what it had first, or a stale local entry survives into a
state it was never part of.

### `RaftLog` — offset indexing

| Method | What changed |
|---|---|
| `term_at`, `entry_at` | `self._entries[self._pos(index)]`, guarded at both ends |
| `entries_from` | Floors at `first_index` instead of 1 |
| `matches` | Answers `snapshot_last_index` from the sentinel |
| `truncate_from` | Refuses to cut into the snapshot — those entries are committed |
| `compact_to` | New. Drops the prefix, moves the sentinel, rewrites the file |
| `_load` | Starts from the snapshot point rather than from 0 |

`truncate_from` refusing is worth pausing on. It only ever runs on a follower
whose log diverged from the leader's — and divergence can only happen in the
*uncommitted* tail. An attempt to truncate into the snapshot means something
committed is being rolled back, which is not a recoverable situation. It raises
rather than obeying: better a loud crash than silent divergence.

### `RaftNode` — startup order

```python
self.snapshots = SnapshotStore(directory)
snapshot = self.snapshots.load()
if snapshot:
    self.machine.restore(snapshot['state'])
    self.commit_index = self.last_applied = snapshot['last_index']
self.log = RaftLog(directory, snapshot_last_index=..., snapshot_last_term=...)
```

The snapshot loads **before** the log, because the log needs the snapshot point
to know where its own indexing starts. Restart recovery is now *snapshot, then
replay the tail* — which is what every real system does, and why they restart
in seconds rather than in proportion to their uptime.

Setting `commit_index` from the snapshot is safe by construction: a snapshot
only ever contains applied entries, applied only ever happens to committed
entries, and committed is permanent.

---

## 6. The trap we deliberately avoided

A threshold of 1000 against a log that reaches perhaps 30 entries means **the
entire feature never executes.** Not in a test run, not in a chaos script, not
in a demo. It would sit in the repo looking like coverage.

That is a worse outcome than not having it, and this project has already been
bitten by exactly this shape of mistake: the in-memory test transport modelled
a dead peer as *instant silence* rather than as *slow*, so 260 passing tests
sailed straight over a bug that made the cluster permanently leaderless the
first time a real broker died. Code that is never run is not tested; it is
merely present.

So `snapshot_threshold` is a constructor argument, and
[`tests/test_raft_snapshot.py`](../../tests/test_raft_snapshot.py) sets it to
**4**, driving real elections and real replication across real compactions.

---

## 7. How real Kafka differs

- **KRaft snapshots are a topic-partition**, `__cluster_metadata`, written as a
  normal log segment and fetched with the normal `Fetch` API. Ours is a single
  file with a bespoke reader.
- **Incremental snapshots.** A large state machine is written without stopping
  the world, usually copy-on-write. We serialise a few kilobytes under the lock.
- **Snapshot chunking.** Real snapshots are sent in pieces with `offset`/`done`
  fields. Ours fits in one message — see Step B.
- **A snapshot has a leader epoch.** KRaft stamps the snapshot with the epoch
  that produced it, feeding the same fencing machinery Phase 6 needs.

---

## 8. Exercises

1. **Watch a restart get cheap.** Set the threshold to 4, produce enough
   metadata changes to trigger a few snapshots, restart a broker, and compare
   `log_last_index` with the number of records actually in `raft-log`.
2. **Break the sentinel, two ways, and notice they are not equally dangerous.**
   - *Delete* it — `self._entries = kept` in `compact_to`. **7 tests fail
     immediately**, because position 0 stops being the sentinel and `_pos()`
     is wrong for every index. Loud, obvious, cheap to find.
   - *Keep it but zero its term* — `LogEntry(index, 0, ...)`. Only **3 fail**,
     and only the ones that cross a compaction point: the log's own arithmetic
     is fine, `first_index` is fine, restart is fine. What breaks is exactly
     one thing — a leader can no longer produce a `prev_log_term` a follower
     will accept — and nothing local to the node ever notices.

   The second is the shape of bug this project keeps meeting: correct
   everywhere you look, wrong only where two nodes have to agree.
3. **Snapshot too far.** Change `maybe_snapshot` to compact to `commit_index`
   instead of `last_applied`. Construct the case where a node then serves state
   it never applied.
4. **Reverse the order.** Truncate the log before fsyncing the snapshot, then
   kill the process in between. What is lost, and why is it unrecoverable?
5. **Watch the hole, then watch it close.** `python -m chaos.lagging_follower`
   cuts a broker off, compacts past it, and heals. Then revert
   `_replicate_once` to Step A's behaviour — drop the `needs_snapshot`
   dispatch — and run it again. The follower never converges, and the reason
   is one line.

---

→ Next: **[Step B — the `InstallSnapshot` RPC](raft-install-snapshot.md)**,
which closes the hole §4 opened. Watch it with
`python -m chaos.lagging_follower`.
