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
  corrupted a bit at a time, and cut by *directional* partitions.
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

## Usage

```
sim run     [--seed N] [--benign] [--trace debug]   one run, reported in full
sim sweep   [--seeds N] [--threads N]               many seeds, hunting failures
sim replay  --seed N [--out trace.txt]              re-run verbosely, verify determinism
sim shrink  --seed N                                cut a failure to a minimal repro
sim demo                                            a tour of all of the above
```

Exit status is 0 when nothing failed and 1 when something did, so a sweep drops
straight into CI.

`replay` re-runs the seed several times and compares fingerprints. The
fingerprint is fed only by significant state changes and is independent of the
log level, so a silent run and a fully traced run must agree — if they ever
disagree, tracing is perturbing the system and every seed recorded so far is
suspect.

## Scope

The store implements leader election, log replication, and linearizable reads
through the log, with `(client, seq)` deduplication so a retry cannot apply
twice. Deliberately **not** implemented: snapshots, log compaction, and
membership changes.

## Bugs found

See [BUGS.md](BUGS.md).
