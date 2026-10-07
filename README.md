# simkv

Deterministic simulation testing for a Raft-replicated key-value store.
**Every bug is a seed.**

A whole distributed system — network, disks, clocks, crashes — runs inside one
deterministic single-threaded simulator. A seed names a run: the same seed
replays the same message order, the same latencies, the same torn writes, on any
machine, forever. That turns "it deadlocked once in staging last Tuesday" into
`sim replay --seed 41337`.

```
cargo run --release -p harness --bin sim -- demo
```

## What's here

| crate | role |
|---|---|
| `simcore` | the deterministic world: RNG, event scheduler, network, disk, fault injection, tracing |
| `sim-io` | the narrow interface a node is allowed to use — clock, messages, storage — and nothing else |
| `kvstore` | the system under test: Raft plus a key-value state machine |
| `checker` | the oracles: Raft safety invariants, durability, linearizability |
| `harness` | seeds in, verdicts out: workload, sweep, shrink, replay, CLI |

## The simulated world is hostile on purpose

- **Network** — messages are delayed on a long-tailed distribution, dropped,
  duplicated, reordered (naturally, because every copy draws its own latency),
  corrupted a bit at a time, and cut by *directional* partitions — including
  the split that makes stale reads possible, where a server loses its peers but
  keeps its clients.
- **Disk** — a completed write is not a durable write. Each file has a page
  cache and a durable image; only a completed `fsync` moves data between them. A
  crash independently loses, tears, or reorders every unsynced write, which
  leaves zero-filled holes in the middle of a log.
- **Clocks** — every node has its own offset and drift, and the fault injector
  steps them. Nothing may depend on two nodes agreeing about the time.
- **Crashes** — power-loss semantics: volatile state is gone, in-flight timers
  and I/O die, and the disk keeps whatever the crash model left behind.

Faults are confined to a window. Afterwards the world heals and every node comes
back, so a correct cluster has to demonstrably recover — otherwise "wedged" and
"busy" would look the same.

## What is checked

- **Raft safety, continuously** — election safety, leader append-only, log
  matching, leader completeness, state machine safety, and that a restarted node
  never comes back at a lower term than one it already acted in.
- **Durability, against the actual bytes** — everything the cluster considers
  committed must be on stable storage on a majority, decoded with the same
  recovery rules a restarting node uses. An implementation that acknowledged
  before `fsync` passes every in-memory check and fails this one.
- **Linearizability, from outside** — the client history is checked against a
  single-threaded model with the Wing & Gong search: partitioned per key,
  memoised on (linearized set, state), with O(1) backtracking. Operations that
  never got an answer may be placed anywhere after their invocation, or nowhere
  at all, because a client that timed out genuinely does not know.
- **The rules themselves, not just their consequences** — a follower may never
  acknowledge more than it has synced; a vote may never be granted before it is
  on disk; a leader may only commit onto an entry of its own term, and may only
  change the membership once it has, one server at a time; a read may only be
  accepted at an index from the leader's own term, and only answered once a
  quorum has replied to a probe sent after it arrived. Waiting for the *damage* these cause needs a long run of bad luck;
  checking the rule fires on the first offending event.
- **Efficiency** — a message storm breaks no safety property, so nothing above
  can see one. Healthy runs cost about 15 events per operation; a run far above
  that fails as `message_storm`.

## Usage

```
sim run     [--seed N] [--benign] [--trace debug]   one run, reported in full
sim sweep   [--seeds N] [--threads N]               many seeds, hunting failures
sim replay  --seed N [--out trace.txt]              re-run verbosely, verify determinism
sim shrink  --seed N                                cut a failure to a minimal repro
sim bugs    [--seeds N]                             detection rate of every injected defect
sim demo                                            a tour of all of the above
```

Useful knobs for reaching states the defaults do not: `--max-batch N` (small
batches make followers acknowledge at an old-term index), `--snapshot-threshold
N` (low values force constant compaction and snapshot transfer),
`--reads log|index`, `--quorum-loss`, `--reconfig` (add and remove servers
while faults are injected; `--spares N` sets how many slots sit outside the
initial membership), `--no-stickiness`, `--no-prevote`, and `--bug NAME` (eight deliberate
defects, every one of which the checkers catch by a safety rule, not a stall).

Exit status is 0 when nothing failed and 1 when something did, so a sweep drops
straight into CI.

`replay` re-runs the seed several times and compares fingerprints. The
fingerprint is fed only by significant state changes and is independent of the
log level, so a silent run and a fully traced run must agree — if they ever
disagree, tracing is perturbing the system and every seed recorded so far is
suspect.

## Scope

The store implements leader election, log replication with `(client, seq)`
deduplication so a retry cannot apply twice, and log compaction: snapshots go
to two alternating files, and a follower that has fallen behind the compacted
log is caught up with `InstallSnapshot`. The write-ahead file stays
append-only, so compaction reclaims memory but not disk — doing that safely
needs an atomic rename the storage model deliberately does not provide.

Reads use **ReadIndex** (Raft §6.4) by default: the leader records its commit
index, confirms it is still leader with one round of probes to a majority, and
answers from memory once that index is applied — no log entry, no disk sync.
Against putting every read through the log, that makes sweeps 2.1× faster on
fault-free runs and 1.3× faster under faults, with a third fewer log entries.
`--reads log` switches back.

**Membership changes** are single-server (Raft thesis §4.1): a configuration
takes effect as soon as it is in a log, a leader proposes one only after
committing an entry of its own term, and never with a previous change still
uncommitted. A leader that removes itself keeps leading until the change commits,
then steps aside. Snapshots carry the membership in force at their index. With
`--reconfig` the fault injector adds and removes servers mid-run, and every
quorum the checkers compute — durability included — is the quorum of the
configuration that entry was committed under.

**Leader stickiness** (thesis §4.2.3) is on by default: a server that has heard
from a leader within the minimum election timeout ignores vote requests, even
at higher terms. Without it, a removed server that never learns of its removal
times out and disrupts the cluster forever — over 400 reconfiguring seeds,
stickiness completes 16% more operations.

**Pre-vote** (thesis §9.6) is on by default too: before touching its term, a
would-be candidate asks whether it could win, and servers that are still
hearing from a leader say no. A server that has lost touch — partitioned, or
removed and never told — can then time out forever without its term moving.
It completes 7% more operations by default, and 12% more with membership
changes. `--no-prevote` turns it off.

Deliberately **not** implemented: joint consensus, and lease-based reads —
which trade the probe round for a dependence on bounded clock drift, in a
simulator that deliberately skews and steps every clock.

## Bugs found

See [BUGS.md](BUGS.md).
