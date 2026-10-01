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
| `commit-any-term` — Raft Figure 8 | 216 of 300 | `commit_of_foreign_term` |
| `vote-before-sync` — reveal a vote before it is durable | 142 of 300 | `vote_beyond_durable` |
| `no-dedup` — retried requests apply twice | 299 of 300 | `linearizability` |
| `truncate-on-any-append` — trust the leader's length | 300 of 300 | `commit_beyond_log` |
| `read-without-quorum` — ReadIndex without confirming leadership | 17 of 300 | `stale_read_index` |
| `read-before-term-commit` — ReadIndex before a current-term commit | 21 of 300 | `stale_read_index` |
| `config-before-term-commit` — change membership before a current-term commit | 31 of 400 (`--reconfig`) | `config_before_term_commit` |

There is no longer a defect on this list the harness cannot see, and the test
that asserts it is strict: no known-gap escape hatch, and — since the
correction below — a stall does not count as a catch.

### Correction: `vote-before-sync` had stopped being a defect

This table used to say `vote-before-sync` was caught in 268 of 300 seeds by
`durable_term_lost`. That was measured before bug 2 was fixed, and was never
measured again. Bug 2's fix is a guard on every send: no message may carry a
term that is not yet durable. The injected defect sent its early vote through
that same guard — which dropped it. From then on the "defect" was an election
that could never finish, and sweeps caught it as `no_progress_after_recovery`
in 231 of 300 seeds, while the bug it claims to model was never exercised at
all. The test only asked that *something* fail, so it went on passing.

Two changes. The defect now goes around the guard, so the vote really does
leave before it is durable; damage-based detection then managed 20 of 300. And
a new rule check, `vote_beyond_durable`, reads the voter's stable storage on
every granted vote it sees — the vote, or a later term, must already be there —
which takes it to 142 of 300. `every_injected_bug_is_caught` now also rejects a
catch that is only a stall: every defect on this list breaks a safety property,
and has to be caught by one.

The leader-bias figures below ("from 5 seeds in 300 to 268") date from before
the guard and are kept as they were measured.

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

## 5. Three bugs in log compaction, found by building it

Adding snapshots meant the recovery path — where every earlier bug lived — had
to handle a log that no longer starts at index 1. Running with compaction turned
up to absurd rates (`--snapshot-threshold 3`, a snapshot every three entries)
found three defects in the new code within minutes.

**A hole in the write-ahead file.** The file is append-only, and a node that
installs a snapshot discards its whole log. It then appended the next entries
*after* the records the snapshot had replaced, leaving a gap where the
superseded entries used to be. Recovery reads the file as one contiguous run, so
it stopped dead at the gap and never reached the entries beyond it — the node
claimed 416 entries durable while its disk would produce 400. Fixed by resetting
the log region when a snapshot supersedes it.

**An older snapshot overwriting a newer one.** Two `InstallSnapshot` messages
arrived out of order, so the snapshot covering index 12 was written *after* the
one covering index 15 and got the higher sequence number. Recovery picks by
sequence, so it chose the older image. Fixed twice over: snapshots are now
ordered by covered index with the sequence number only as a tie-break, and a
node refuses to write a snapshot older than one it has already written.

**A state machine rolled back underneath its own applied index.** Installing a
snapshot replaced the state machine even when the node had already applied
*past* it. Raft went on believing everything through `last_applied` was
reflected in the state, while the state had quietly reverted — which surfaced as
a client reading a value that had been overwritten long before. The image is now
only adopted when it is genuinely ahead of what has been applied.

### Correction: the retain-tail optimisation was never the problem

The first version of this section said Raft section 7's retain-the-tail
optimisation was unsafe here, because disabling it made seed 425 pass. That was
wrong, and the reason it was wrong is worth more than the bug.

## 6. Two snapshot installs sharing one slot

**Severity: stale reads.** Found at `--snapshot-threshold 3`, seed 26.

A snapshot received from the leader was held in a single field on the node
until its `fsync` completed. The leader retries and the network reorders, so two
installs can be in flight at once — and the second overwrote the first's image.
When the first completed, there was no image to restore, but it advanced
`last_applied` anyway. Raft believed the state machine reflected everything up
to that index; the state machine had not moved.

The client saw it as a read returning `c0v70` — a value overwritten four seconds
earlier, by a write whose own window had long closed.

**Fix:** the image travels inside the persist action that completes it, so two
installs cannot see each other's data.

**And the correction:** with this fixed, retain-tail was switched back on and
seed 425 passed, along with 500 seeds at each of thresholds 3, 5, 10 and 400.
Disabling the optimisation had never removed the bug. It changed which entries
got re-sent, which changed the timing, which happened to stop two installs
overlapping on that seed. **A bug going away when you remove a feature is not
evidence that the feature caused it** — it is evidence that the feature is on
the schedule the bug needs. The optimisation is back, with a comment saying so.

---

## 7. A snapshot written over the only durable copy

**Severity: committed data lost.** Found at
`--snapshot-threshold 10 --servers 5 --max-batch 2`, seed 287 — the first sweep
run after fixing bug 6, in a configuration combining all three stress knobs.

Snapshots alternate between two files so that a torn write can only ever damage
one of them. That guarantee has a precondition nobody wrote down: **at most one
snapshot write may be in flight, and it must never target the file holding the
good copy.**

Locally-taken snapshots had an in-flight guard. Installs from the leader did
not. So a node could be installing snapshot 313 into one file while also
taking its own snapshot 313 into the other — the file holding snapshot 303,
its only durable one. That write begins by truncating the file. The crash came
before either write synced:

```
recovered ... snapshot=0@0 damaged=true
```

No snapshot at all. And because the log had already been compacted past 303,
the entries the snapshot had absorbed were in neither place.

**Fix:** one in-flight guard covering both paths (an install that arrives while
a write is landing is dropped, and the leader retries), and the target file is
chosen as *whichever does not hold the newest durable snapshot* — tracked
explicitly, including across restarts — rather than by alternating on a
sequence number that knows nothing about which write actually landed.

Both seeds are now regression tests.

---

## 8. ReadIndex, and a storm no checker could see

Reads used to go through the log: correct, and a disk sync on a majority per
read. ReadIndex (Raft §6.4) answers from the leader's memory instead, after
confirming leadership with a round of probes. Two deliberate defects came with
it, and building it turned up one real bug.

### The workload could not see stale reads

`read-without-quorum` — a leader that serves reads without confirming it is
still leader — went uncaught in 400 seeds by the linearizability check. The
reason was the workload, not the checker. Clients run one operation at a time.
A client stranded on the minority side of a partition sends a write that can
never commit, and waits on it for seconds. The stale reads a deposed leader
serves happen in the first milliseconds of the partition, before the majority
side has even elected anyone, so there is nothing yet for them to be stale
against.

The fix was the lesson from before: **check the rule, not the damage.** A read
must observe everything committed anywhere in the cluster before it arrived.
For a leader that has committed an entry of its own term and then confirmed
itself with a majority, that is guaranteed — any leader that could have
committed past it was elected by nodes that would have refused the
confirmation. `stale_read_index` checks exactly that, and catches both read
defects.

### Bug 8: a message storm

A fault-free sweep took four times longer with ReadIndex than without, despite
running fewer events per seed. One seed was responsible: **580,183 events where
about 24,000 is normal** — 245,000 `AppendEntries` and 245,000 replies.

Installing a snapshot that matched the follower's own log kept the entries above
it (the section 7 optimisation restored in bug 6) but threw away the follower's
parked durability claims for those same entries. Those were claims for data
already on disk, waiting only on a gap below them. With them gone, the
follower's durable index stuck. It acknowledged the same point on every reply;
the leader, seeing it behind, resent immediately; the follower had nothing new,
so it replied immediately. The two went back and forth at network speed for
the rest of the run.

Every value stayed correct and every commit stayed durable, so **no checker
fired. A stopwatch found it.** The fix keeps the claims for entries that
survive the snapshot. And because this whole class of bug is invisible to
safety checks, there is now one that is not: healthy runs cost about 15 events
per completed operation — never more than 30, even in the harshest
configuration — so a run above 40 per operation per server fails as
`message_storm`. With the fix reverted, that check catches seed 200 at 360 per
operation.

---

## 9. Membership changes, and three bugs they shook loose

Single-server membership changes (thesis §4.1) went in with two rule checks of
their own: `config_before_term_commit` (a leader changed the membership before
committing an entry of its own term — the bug in the original single-server
algorithm, fixed on raft-dev in 2015) and `concurrent_config_change` (a second
change proposed while one is still uncommitted). The matching defect,
`config-before-term-commit`, is caught in 31 of 400 reconfiguring seeds.

### A removed server that never finds out

A leader removes a server and commits the change without it — that is the
point of removing it. The removed server never receives the entry, never learns
it is out, times out, and campaigns. Its higher term deposes the working leader;
the cluster elects a new one; the removed server times out again. In seed 2 it
started 36 elections.

The fix is leader stickiness (thesis §4.2.3): a server that has heard from a
leader within the minimum election timeout ignores vote requests, even at
higher terms. `--no-stickiness` turns it off, which makes the effect
measurable: over 400 reconfiguring seeds, 213,320 operations complete without
it and 247,971 with it. Seed 2 alone goes from 331 operations, 44 elections and
5 leaders to 588 operations.

### Bug 9: a term learned from a snapshot never reached disk

Seed 392 (`--reconfig --snapshot-threshold 5 --max-batch 2`): a follower
restarted at term 15, and the first message it got from the term-17 leader was
`InstallSnapshot`. That handler adopted the term in memory with the plain
`step_down`, not `step_down_and_persist` — and nothing in the install path
writes the hard state, because the snapshot goes to its own file. Term 17 was
never durable, so bug 2's send guard withheld every reply the follower owed the
leader, for good. The leader kept retrying, the retries kept the follower's
election timer quiet, and the cluster stalled with a live leader and a live
quorum: `no_progress_after_recovery`, 240 operations instead of 1199.

The same hole was in `AppendEntries`: an append falling entirely below the
snapshot returned early with a reply, skipping the hard-state write that the
rest of the handler does. Both now persist the term first.

This is not a membership bug. It needed a restart, a snapshot as the very first
contact, and a term jump — and adding servers mid-run changed the schedules
enough to produce that combination. Nothing about the code path was new.

### Bug 10: a repeated vote, granted from memory

Found by `vote_beyond_durable` the first time it ran, at seed 480 under
`--reconfig`. A node with term 8 already durable granted n4 its vote and began
writing it. The network duplicated n4's request; the copy arrived while the
write was in flight. By then nothing was "dirty" — the vote was already in
memory — so the handler answered at once, and the send guard, which checks
terms and not votes, let it out. A crash before the sync would bring the node
back with no vote in term 8, free to elect a second leader in it.

Raft now tracks the newest hard state actually on disk, not just its term, and
does not repeat a grant until that vote is there; the write in flight sends the
answer when it lands. No damage-based check had ever reported this, across tens
of thousands of seeds — a duplicate, a crash, and a competing candidate all in
one window is a long coincidence. The rule check saw it on its first sweep.

---

## Current status

```
16500 seeds across eleven configurations → no failures, no false positives
  (default; fault-free; --snapshot-threshold 5 + batch 2; 5 servers +
   threshold 3; 7 servers; and with membership changes: default, 5 servers,
   threshold 5 + batch 2, majority-failure, reads through the log,
   no stickiness)
All eight injected defects caught, every one by a safety rule rather than a
  stall; the test that asserts it has no known-gap escape hatch
3000 seeds adding and removing servers under crashes + partitions + clock
  skew + torn writes → 59.2M events, 2.8M operations checked, no failures
```

Absence of failures is not proof of correctness. It means the bugs that remain
are rarer than roughly one run in five thousand under this fault model — and
that the fault model, not the search, is now the limiting factor.
