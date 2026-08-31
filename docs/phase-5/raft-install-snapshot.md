# Phase 5 (addendum) — `InstallSnapshot`: Catching Up a Follower That Fell Off the Log

> Step B of two. Step A — taking a snapshot and discarding the log prefix — is
> in **[raft-snapshots.md](raft-snapshots.md)**, and this document assumes it.

---

## 1. The problem Step A created

Compaction introduced a state that could not previously exist: **a follower
needing an entry the leader has already thrown away.**

```
   leader     snapshot @ 40 │ 41 42 43 44
                            ▲ first_index = 41
   follower   1 2 3 4 5 6 7 8 9 10 11
                                     ▲ next_index = 12
```

The leader has to send `prev_log_index = 11` so the follower can verify the
match. It no longer holds entry 11. `term_at(11)` has nothing to return, so
there is no legal `AppendEntries` to build.

Before compaction this could not happen. "Behind" was a single state and always
recoverable: back up `next_index` until the logs meet, and they always did,
because index 1 was always there. Now "behind" splits in two, and the line
between them is `first_index`:

| | `next_index >= first_index` | `next_index < first_index` |
|---|---|---|
| | behind, recoverable | behind, **stuck** |
| Fix | `AppendEntries`, backing up | `InstallSnapshot` |

Step A shipped with the second column unhandled — it logged a loud warning and
looped. This closes it.

---

## 2. How Raft solves it

A third RPC. When the leader finds a follower below `first_index`, it stops
trying to send *history* and sends the *result* instead: the whole state
machine, stamped with the index and term it covers.

```
   leader ──── InstallSnapshot {last_index: 40, last_term: 6, state} ───► follower

   follower:  discard the entire log
              adopt the state wholesale
              last_applied = commit_index = 40
              reply {term}

   leader:    match_index[peer] = 40
              next_index[peer]  = 41      ← now inside the log again
```

The next ordinary `AppendEntries` carries 41 onward with `prev_log_index = 40`,
which the follower answers from its freshly relocated sentinel. One round trip
converts an unrecoverable follower into an ordinary one.

### Why this is safe

The alarming part is *a follower deletes its whole log on request*. Three
things make that fine, and all three are worth being explicit about:

1. **The snapshot only ever covers applied entries.** Applied implies
   committed, committed is permanent — so everything in it is agreed by a
   majority and can never be rolled back.
2. **The term check comes first.** A stale leader's `InstallSnapshot` is
   rejected exactly like a stale `AppendEntries`, by the same rule that
   outranks every other rule.
3. **The follower was behind anyway.** Whatever it is deleting is either
   already inside the snapshot or was uncommitted — and uncommitted entries on
   a follower whose log diverged are precisely what Raft discards routinely in
   `truncate_from`.

---

## 3. Options considered

### Chunked or whole?

| Option | Cost |
|---|---|
| **One message** | Bounded by the message size limit. Trivially correct. |
| Chunked, `offset`/`done` | Survives a state machine larger than a message. Needs partial-snapshot state on the follower, and a way to discard it when a chunk stream is abandoned. |

One message. Our state machine is a few kilobytes — brokers, topic widths, and
partition assignments — against a 16 MB message ceiling. The paper chunks
because real state machines are gigabytes and a single RPC would monopolise the
connection; we would be paying for a problem we do not have, in the coin of
partial-transfer state that is itself a source of bugs.

**Where this breaks — measured, not guessed:** the snapshot is JSON inside a
message capped at `MAX_REQUEST_BYTES` (10 MB), and a partition assignment
costs about 54 bytes of it.

| partitions | snapshot |
|---|---|
| 10,000 | 0.50 MB |
| 50,000 | 2.56 MB |
| 100,000 | 5.14 MB |
| 200,000 | **10.38 MB — over the limit** |

So the ceiling is around **190,000 partitions**, at which point a follower
that falls behind can never be caught up and the failure is permanent. Worth
knowing precisely: real Kafka clusters do run six figures of partitions, so
this is a limit one could actually reach — just not with a clone whose
partitions are single-copy JSON files.

### Should the follower keep any of its log?

The paper's optimisation: if the follower already holds an entry matching the
snapshot's `(last_index, last_term)`, keep everything after it rather than
discarding the lot.

Skipped, deliberately. It saves re-replicating a handful of entries in a case
that is already rare — the follower is, by definition, far behind — and it adds
a branch whose failure mode is keeping entries that should have gone. The
unconditional version has one path, and one path is one thing to get right.

---

## 4. The decision

**API key 54.** `REQUEST_VOTE` is 52 (which is KRaft's real `Vote` key) and
`APPEND_ENTRIES` is 53, our own choice; 54 continues that sequence.

**Leader side** — [`node.py::_replicate_once`](../../broker/raft/node.py):

```python
if next_index < self.log.first_index:
    await self._send_snapshot(peer)
    return
```

The Step A warning becomes a dispatch. Note what it replaced: a message that
was *legal but useless*, sent every tick forever.

**Follower side** — `handle_install_snapshot`:

```python
if term < self.current_term:                 # stale leader
    return {'term': self.current_term, 'success': False}
self._observe_term(term)
self.role = FOLLOWER
self._reset_election_timer()                 # this IS contact from a leader

if last_index <= self.commit_index:          # we are already ahead
    return {'term': self.current_term, 'success': True, ...}

self.snapshots.save(last_index, last_term, state)   # durable FIRST
self.machine.restore(state)
self.log.reset_to_snapshot(last_index, last_term)
self.commit_index = self.last_applied = last_index
```

Three details in there that are easy to leave out:

- **Resetting the election timer.** An `InstallSnapshot` is contact from a live
  leader, exactly like a heartbeat. Forget it and a follower being caught up —
  which by definition takes a while — starts an election against the leader
  currently helping it.
- **The `last_index <= commit_index` guard.** A snapshot older than what we
  already have would *move us backwards*. It can arrive legitimately: a
  retransmission, or a slow message overtaken by the AppendEntries that
  followed it. Ignoring it is not defensive padding; without it a duplicate
  packet rolls back committed state.
- **Durability order, again.** Save the snapshot before touching the log, for
  the same reason as Step A: a crash in between must not lose both copies.

**`RaftLog.reset_to_snapshot`** — the wholesale version of `compact_to`. It
drops every entry, relocates the sentinel, and truncates the file to empty.
Where `compact_to` refuses to move backwards, this one is *allowed* to, because
its caller has just replaced the state machine to match.

---

## 5. What it costs

`InstallSnapshot` is the only place in this system where a node **discards
committed local state on the word of a peer**. Everything else — vote, append,
truncate — either adds or removes uncommitted data.

That is why the term check and the `commit_index` guard are not routine input
validation. They are the entire fence around a destructive operation. Delete
either and a stale or duplicated message rewinds a follower's state machine,
which is silent, permanent, and detectable only by comparing two nodes.

---

## 6. How real Kafka differs

- **Chunking**, as above.
- **KRaft fetches snapshots**, it does not receive them pushed: the follower
  asks with `FetchSnapshot`, reusing the same flow as partition replication.
  Pull-based means the follower controls the pace, which matters when the
  snapshot is large.
- **Snapshots are a topic-partition** — `__cluster_metadata` — written as
  ordinary log segments, not a bespoke file.
- **The snapshot carries a leader epoch**, feeding the same fencing machinery
  Phase 6 needs for partition data.

---

## 7. Exercises

1. **Watch it fire.** `python -m chaos.lagging_follower` — partition a follower,
   compact past it, heal, and watch one `InstallSnapshot` do what a hundred
   `AppendEntries` could not.
2. **Remove the term check** in `handle_install_snapshot`. Which test fails, and
   what would the production symptom be?
3. **Remove the `last_index <= commit_index` guard** and deliver the same
   snapshot twice. Construct the case where the second delivery loses data.
4. **Skip the election-timer reset.** Nothing fails immediately. Work out what
   would happen to a follower whose snapshot took longer than an election
   timeout to arrive, and why raising the timeout is not the fix.
5. **Implement the paper's optimisation** — keep the log tail when the follower
   already matches `(last_index, last_term)`. Then work out how you would ever
   know it was correct.

---

→ Back to **[Phase 5 — The Cluster](phase-5-cluster.md)**, or on to Phase 6,
where the Raft term becomes the epoch that fences partition data.
