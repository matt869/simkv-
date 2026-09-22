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

| Defect | Detection | Caught by |
|---|---|---|
| `ack-before-sync` — acknowledge before `fsync` | 300 of 300 | `ack_beyond_durable` |
| `commit-any-term` — Raft Figure 8 | 156 of 300 | `commit_of_foreign_term` |
| `vote-before-sync` — reveal a vote before it is durable | 268 of 300 | `durable_term_lost` |
| `no-dedup` — retried requests apply twice | 288 of 300 | `linearizability` |
| `truncate-on-any-append` — trust the leader's length | 300 of 300 | `commit_beyond_log` |

There is no longer a defect on this list the harness cannot see, and the test
that asserts it is strict: no known-gap escape hatch.

### Check the rule, not just the damage

Two of these defects were nearly invisible, and the fix for both was the same
realisation.

`commit-any-term` escaped 5000 seeds. `ack-before-sync` was found in 1 seed in
300. Both were being detected only by the *damage* they eventually cause, and
that damage needs a long coincidence: for the premature commit to matter, a
leader has to die inside a narrow window; for the premature acknowledgement to
matter, enough nodes holding the data have to lose it while the leader — which
synced honestly — is also gone.

Two rounds of aiming the fault injector helped, but not enough:

**Leader-biased faults** (`leader_bias_ppm`, 30%) aim crashes and isolations at
whoever believes they are leading. This moved `vote-before-sync` from 5 seeds in
300 to 268, because that defect also needs a crash in a leadership window. It
made `ack-before-sync` *harder* to find, from 1 in 40 to 1 in 300, because that
one is a lying **follower** and crashing leaders more means crashing followers
less. Aiming faults is a trade, not a free win.

**Unsynced-biased crashes** (`unsynced_bias_ppm`, 50%) aim at nodes holding
writes they have not synced — precisely the window a durability bug needs. This
was the right idea and still only moved `ack-before-sync` from 1 in 300 to 2.

What actually worked was checking the **rule** rather than waiting for the
consequence:

- `ack_beyond_durable`: a follower may never acknowledge a `match_index` larger
  than it has ever had durable. Compared against a per-node high-water mark, so
  a crash legitimately lowering the current value cannot cause a false positive.
  1 in 300 → **300 of 300**.
- `commit_of_foreign_term`: a leader may only advance its commit index onto an
  entry from its own term. Not caught in 5000 → **156 of 300**.

Both checks are one line of Raft restated as an assertion, and both fire on the
first offending event rather than seconds later at the wreckage. Aiming faults
widens the space you search; checking rules shortens the distance between a
mistake and a report. The second is worth more.

A caveat recorded honestly: `commit-any-term` had briefly been reachable at
`--max-batch 2`, at 1 seed in 300. Fixing bug 4 removed that path — the
acknowledgement it depended on was itself the bug — and it went back to
undetectable until `commit_of_foreign_term` existed.

---

## 4. Acknowledging entries nobody had compared

**Severity: committed data overwritten.** Found by `--max-batch 2`; seed 388
replaced a committed term-8 entry with a term-7 one.

**Reproduces (before the fix):**
`sim run --seed 388 --max-batch 2 --duration 8000 --settle 12000 --drain 3000`

A follower that accepted an `AppendEntries` replied with `match_index` set to
its **whole durable log**. Raft's rule is narrower: a follower may only
acknowledge `prev_index + entries.len()` -- the prefix this message actually
proved agrees with the leader. Past that point the follower's log can run on
into a suffix from an older term that nobody has compared yet.

With 64-entry batches the leader always sent everything to the end of its log,
so any divergent suffix was compared -- and truncated -- in the same message
before the ack went out. The bug was invisible. With two-entry batches:

1. Node 2, an old leader partitioned away, holds term-7 entries up to 97.
2. Node 0, leader of term 9, sends entries 94–95. They match. Node 2 replies
   "durable through 97" -- its own term-7 97.
3. Node 0 counts node 2 as holding its term-8/9 entries at 96–97, reaches a
   quorum, and commits them.
4. Node 2 later wins an election -- legitimately, against a third node whose log
   was shorter -- and overwrites the committed entries everywhere.

Finding it took three wrong turns, recorded because the process is the point:
the bad election looked like the bug (it was legal under the election
restriction); then truncation of committed entries looked likely (promoting the
release-invisible `debug_assert!` in `truncate()` to a reported violation showed
it never happened); then a lost hard-state write (a real hole, fixed, but not
this one -- the fingerprint did not move). The trace of *who acknowledged what*
is what finally showed it.

**Fix:** every acknowledgement is capped at the verified prefix. The deferred
reply carries it through the persist batch, so an ack that waits for an `fsync`
is capped the same way as one sent immediately. Batch sizes 1, 2 and 4 are now
clean over 1000 seeds each, and seed 388 is a regression test.

**Related, found on the way:** an entry is now only reported durable once the
term that authorised it is durable too, because recovery discards entries ahead
of the hard state. And `truncated_committed` is now a checked invariant in
release builds.

---

## Current status

```
6000 seeds across six configurations → no failures
  (default; --max-batch 1; --max-batch 2; 5 servers + small batches;
   5 servers, 8 clients, 2 keys; and majority-failure allowed)
5000 seeds, 3-node cluster, crashes + partitions + clock skew + torn writes
  → 45.2M events, 2.4M operations checked, no failures

1000 seeds, 20s runs           → no failures
400 seeds, 5 servers, 2 keys   → no failures
400 seeds, majority may fail   → no failures (safety only; liveness not expected)
```

Absence of failures is not proof of correctness. It means the bugs that remain
are rarer than roughly one run in five thousand under this fault model — and
that the fault model, not the search, is now the limiting factor.
