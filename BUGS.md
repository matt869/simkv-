# Bugs found

Every bug below was found by the simulator, not by reading the code. Each one
reproduces from a seed.

The pattern is worth noting: none of these are mistakes in the Raft state
machine. Elections, log matching and the commit rule were right the first time,
because the paper is clear about them. Every real bug was in the *ordering
between memory and disk* — the part the paper says one sentence about ("persist
before responding") and leaves to you.

---

## 1. A durability claim covering somebody else's failed write

**Severity: data loss.** A leader could commit entries that were never written.

**Found:** sweeping 500 seeds; 3 failed as `durable_entry_lost`.
**Reproduces:** `sim run --seed 18 --duration 8000 --settle 12000 --drain 3000 --check-durability-every 1`

Log writes are pipelined: a batch writes its entries, waits for the write to be
confirmed, issues an `fsync`, and on completion reports how much of the log is
now durable. Batches overlap, so a later one routinely finishes first.

The claim each batch made was `log.last_index()` — the whole prefix, not just
the range it had written. That is normally sound, because an `fsync` covers
every write issued before it. It stops being sound when one of those earlier
writes **failed**: the simulated disk rejects operations, and the rejection
arrives asynchronously. The sequence was:

1. Batch A writes entries 12–14. The disk rejects the write; the error is in
   flight.
2. Batch B, covering only entry 15, syncs and completes first.
3. B reports "14 entries durable". The file has a hole where 12–14 should be.
4. 0.3 ms later A's error arrives and the node takes itself down — long after
   it told the leader that data was safe.

A leader counting that node towards a quorum commits entries that exist nowhere
on stable storage.

**Fix:** a batch now records the first index it wrote, and may only extend the
durable claim if that index is contiguous with what is already confirmed.
Ranges that arrive early are parked and applied when the gap below them closes
— dropping them instead stalled replication badly enough to cut throughput by
94%, which the first attempt at this fix did.

**What caught it:** `durable_claim_unbacked`, a check added while chasing this
bug, which decodes each live node's disk image and compares it against what the
node claims is durable. It located the exact event; the original
`durable_entry_lost` only showed the damage 1.4 seconds later, at the crash.

---

## 2. Announcing a term before it was durable

**Severity: two leaders in one term.**

**Found:** 2 seeds in 5000, as `durable_term_lost`.
**Reproduces (before the fix):** seeds 2878 and 4024 at
`--duration 6000 --settle 10000 --drain 3000`

Every reply that reveals durable state was correctly deferred behind its
`fsync`. But a node also answers *stale* requests immediately — "your term is
old, mine is 18" — and those answers were escaping during the window where term
18 was still in flight to the disk.

Crash in that window and the node comes back at term 17, having already spoken
in 18. A term it has forgotten is a term it can vote in a second time, and two
votes in one term is two leaders in one term.

**Fix:** the node tracks its durable term, and `send` withholds any message
carrying a term above it. Dropping the message is safe — it is indistinguishable
from the network losing it, which it does constantly, and the sender retries.

A narrower instance of the same mistake was fixed alongside it: the
`AppendEntries` rejection path sent its reply immediately even when it had just
bumped its own term.

---

## 3. The history was never recorded in release builds

**Severity: the external correctness check was silently doing nothing.**

**Found:** by the liveness check reporting `no_progress_after_recovery` on a
*fault-free* run, which is not supposed to be possible.

A harness bug, and the worst kind. The client recorded operation completions
like this:

```rust
debug_assert!(history.complete(id, now, outcome.clone()), "...");
```

`debug_assert!` does not evaluate its argument in release builds. Sweeps run in
release. So every operation stayed pending, the history came out empty — and an
empty history is trivially linearizable. The check reported success on a system
it had never looked at.

It only surfaced because a *different* oracle (liveness) noticed that zero
operations had completed. Without that second check, this would have been a
green test suite proving nothing.

**Fix:** call it, then assert on the result.

---

## Checker bugs (correct behaviour reported as failures)

Three checks were stricter than Raft actually promises. Worth recording because
a checker that cries wolf is as useless as one that sleeps — the first sweep
reported all 200 seeds failing, which told me nothing about any of them.

| Check | False positive |
|---|---|
| `commit_regression` | Fired on every restart: a node observed while *down* kept its pre-crash commit index in the tracker, so coming back at zero looked like state going backwards. Commit index is volatile; a crash destroys it by design. |
| `leader_completeness` | Fired on partitioned-away stale leaders. A node still calling itself leader of term 1 has no obligation to hold entries committed later in term 2 — it cannot commit anything anyway. The check now binds only leaders of terms *after* the one an entry was committed in. |
| `recovery_not_a_prefix` | Recovery must bring back what was **synced**, not the whole in-memory log. A truncation whose `set_len` never reached the platter legally resurrects entries the node had already discarded — uncommitted, older-termed, and harmless, since the election restriction stops them winning anything. |

A fourth, `durable_conflict`, was removed outright: a disk holding something
else at a committed index is not a violation on its own, it simply does not
count towards the quorum — which `committed_not_durable` already measures.

---

## Proving the checkers can still see anything

A harness that has never failed is indistinguishable from one that cannot fail.
`--bug NAME` switches on a deliberate defect; `every_injected_bug_is_caught`
asserts each is detected, and `an_unmodified_store_survives_the_same_seeds` is
the control.

| Defect | Detection |
|---|---|
| `no-dedup` — retried requests apply twice | 283 of 300, as `linearizability` |
| `truncate-on-any-append` — trust the leader's length blindly | 300 of 300, as `commit_beyond_log` |
| `ack-before-sync` — acknowledge before `fsync` | 1 in 300, as `leader_completeness` |
| `vote-before-sync` — reveal a vote before it is durable | 266 of 300 (was 5 of 300 before leader-biased faults) |
| `commit-any-term` — Raft Figure 8 | not caught in 5000 seeds at the default batch size; **1 in 300 with `--max-batch 2`** |

### Closing the gap: aim the faults

`commit-any-term` escaped 5000 uniformly-random seeds. Two changes to the fault
model were tried, and the result is a useful lesson about what a fault injector
is actually for.

**Leader-biased crashes** (`leader_bias_ppm`, default 30%) aim crashes and
isolations at whoever currently believes they are leading, rather than at a node
picked uniformly. The interesting windows in a consensus protocol are all around
a leadership change, and uniform faults reach them only by luck.

This did not catch `commit-any-term` -- but it moved `vote-before-sync` from
5 seeds in 300 to **266 in 300**, a fifty-fold improvement, because that defect
also needs a crash inside a leadership window.

It also made `ack-before-sync` *harder* to find, from roughly 1 in 40 to 1 in
300: that defect is a lying **follower**, and crashing leaders more often means
crashing followers less often. Aiming faults is a trade, not a free win. That is
why the bias is a tunable rather than a rule, and why the detection test now
sweeps 400 seeds in parallel instead of asserting against a threshold that sat
just above the observed rate.

**Small replication batches** (`--max-batch 2`) did catch it, at 1 seed in 300.
The reason is precise: with a large batch the leader sends its new no-op
*together* with the older entries, so a follower acknowledges both at once and
the correct and the buggy commit rules agree. Only when entries arrive in small
batches does a follower acknowledge at an old-term index -- which is exactly the
situation Raft's commit rule exists for.

The lesson is that the search was never the limiting factor. The defect was
unreachable under the default configuration and trivially reachable one flag
away; what needed widening was the space of *configurations*, not the number of
seeds.

---

## Open: committed entries overwritten under small batches

**Status: found, not fixed.** Reproduces at
`sim run --seed 388 --max-batch 2 --duration 8000 --settle 12000 --drain 3000`
(1 seed in 1000 at that configuration; the default `--max-batch 64` is clean
over 2000 seeds).

A committed entry is overwritten by one from an *earlier* term:

```
committed index 96 was term 8 Noop, now term 7 Put{k2, c2v22}
committed index 96 (term 8) is on stable storage on 0 of 3 nodes
```

What the trace shows, and what it rules out:

- No `durable_claim_unbacked` fires at any event, so every acknowledgement was
  honestly backed by that node's disk at the moment it was sent.
- No `durable_entry_lost` fires, so no restart discarded anything a node had
  claimed was synced.
- n0 holds the committed entries (`log=97 durable=97 term=9`, undamaged) and
  correctly **refuses** to vote for n2: `granted=false (up_to_date=false)`.
- n1 grants: `granted=true (up_to_date=true)` -- and n1's log is `last=(77,7)`
  against n2's offered `last=(97,7)`. Same term, longer log. **Under Raft's
  election restriction that vote is correct.**

So the bad election is a symptom, not the cause. The real question is how n1
came to be missing entries 78..97 that a quorum had committed: whether it
truncated them while following a stale leader, or a leader counted an
acknowledgement that a follower later walked back. `truncate()` guards against
cutting below the commit index with a `debug_assert!`, which does not run in
release builds, so that path is currently unobserved.

Next step is to promote that assertion into a reported violation and re-run the
seed, which will say directly whether a node is truncating committed entries.

---

## Current status

```
5000 seeds, 3-node cluster, crashes + partitions + clock skew + torn writes
  → 45.2M events, 2.4M operations checked, no failures

1000 seeds, 20s runs           → no failures
400 seeds, 5 servers, 2 keys   → no failures
400 seeds, majority may fail   → no failures (safety only; liveness not expected)
```

Absence of failures is not proof of correctness. It means the bugs that remain
are rarer than roughly one run in five thousand under this fault model — and
that the fault model, not the search, is now the limiting factor.
