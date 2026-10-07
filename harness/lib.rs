//! `harness` -- turning a seed into a verdict.
//!
//! [`SimConfig`] describes a run; [`run`] executes one and judges it; [`sweep`]
//! runs thousands across threads to go looking for trouble; [`shrink`] cuts a
//! failing configuration down to the smallest one that still fails; [`replay`]
//! re-runs a seed with tracing on and proves the simulator is deterministic
//! while it does so.
//!
//! The whole point of the arrangement is that a bug report is a number. A
//! failing seed plus its config reproduces the same run, event for event, on
//! any machine, forever.

pub mod cluster;
pub mod replay;
pub mod shrink;
pub mod sweep;
pub mod workload;

pub use cluster::Cluster;
pub use workload::WorkloadConfig;

use checker::linearizability::{History, Verdict};
use checker::Report;
use kvstore::raft::{InjectedBug, RaftConfig};
use simcore::disk::DiskConfig;
use simcore::faults::FaultConfig;
use simcore::net::NetConfig;
use simcore::trace::{Fingerprint, Level};
use simcore::{Nanos, MILLIS, SECONDS};

/// Everything that determines a run. Same config plus same seed, same run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    /// Servers in the initial membership.
    pub servers: usize,
    /// Extra server slots that start outside the cluster and can be added to
    /// it by a membership change.
    pub spares: usize,
    pub clients: usize,
    /// How long faults are injected for.
    pub duration: Nanos,
    /// Quiet period after the faults stop, in which the cluster must recover.
    pub settle: Nanos,
    /// Final window with the clients stopped, to let in-flight work finish.
    pub drain: Nanos,
    /// Fuel. A run that exceeds this is reported as incomplete rather than
    /// being allowed to spin forever.
    pub max_events: u64,
    /// Events between invariant sweeps. 1 checks after every single event.
    pub check_every: u64,
    /// Events between durability sweeps, which decode every node's disk and
    /// are far more expensive than the in-memory invariants.
    pub check_durability_every: u64,
    pub check_liveness: bool,
    /// Flag runs that burn far more events per operation than any healthy run
    /// does. Catches livelocks and message storms, which break no safety
    /// property and so are invisible to every other check.
    pub check_efficiency: bool,
    pub linearizability_budget: u64,
    pub net: NetConfig,
    pub disk: DiskConfig,
    pub faults: FaultConfig,
    pub raft: RaftConfig,
    pub workload: WorkloadConfig,
    pub trace_level: Level,
}

impl SimConfig {
    /// Switch on a deliberate defect, to check that the checkers can see it.
    pub fn with_bug(mut self, bug: InjectedBug) -> SimConfig {
        self.raft.bug = bug;
        self
    }

    /// Change membership during the run, with `spares` extra servers to add.
    pub fn with_reconfig(mut self, spares: usize) -> SimConfig {
        self.spares = spares;
        self.faults.enable_reconfig = true;
        self
    }

    /// Every server slot in the world, members or not.
    pub fn slots(&self) -> usize {
        self.servers + self.spares
    }
}

impl Default for SimConfig {
    fn default() -> Self {
        let duration = 20 * SECONDS;
        SimConfig {
            seed: 0,
            servers: 3,
            spares: 0,
            clients: 4,
            duration,
            settle: 15 * SECONDS,
            drain: 5 * SECONDS,
            max_events: 4_000_000,
            check_every: 1,
            check_durability_every: 200,
            check_liveness: true,
            check_efficiency: true,
            linearizability_budget: checker::linearizability::DEFAULT_BUDGET,
            net: NetConfig::hostile(),
            disk: DiskConfig::hostile(),
            faults: FaultConfig {
                window: (500 * MILLIS, duration),
                ..FaultConfig::default()
            },
            raft: RaftConfig::default(),
            workload: WorkloadConfig::default(),
            trace_level: Level::Off,
        }
    }
}

impl SimConfig {
    pub fn with_seed(seed: u64) -> SimConfig {
        SimConfig {
            seed,
            ..SimConfig::default()
        }
    }

    /// A perfect world: no faults, no loss, no disk lies. If a seed fails here,
    /// the bug has nothing to do with fault handling.
    pub fn benign(seed: u64) -> SimConfig {
        let duration = 5 * SECONDS;
        SimConfig {
            seed,
            duration,
            settle: 2 * SECONDS,
            drain: 2 * SECONDS,
            net: NetConfig::reliable(),
            disk: DiskConfig::reliable(),
            faults: FaultConfig::none(),
            ..SimConfig::default()
        }
    }

    /// Keep the fault window aligned with the run length after either changes.
    pub fn normalise(&mut self) {
        let start = self.faults.window.0.min(self.duration);
        self.faults.window = (start, self.duration);
    }

    pub fn total_time(&self) -> Nanos {
        self.duration + self.settle + self.drain
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RunStats {
    pub events: u64,
    pub sim_time: Nanos,
    pub ops_started: u64,
    pub ops_completed: u64,
    pub ops_abandoned: u64,
    pub retries: u64,
    pub max_committed: u64,
    pub elections: u64,
    pub leaders_elected: u64,
    pub truncations: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub io_failures: u64,
    pub partitions: u64,
    pub messages_sent: u64,
    pub messages_delivered: u64,
    pub messages_dropped: u64,
    pub bad_messages: u64,
    pub linearizability_steps: u64,
    /// Reads answered through ReadIndex rather than through the log.
    pub index_reads: u64,
    /// Membership changes proposed, and ones the leader refused (another in
    /// progress, or no entry of its term committed yet).
    pub reconfigs: u64,
    pub reconfigs_refused: u64,
    /// Membership changes requested of a leader moments after it was elected.
    pub fresh_leader_reconfigs: u64,
}

/// The result of one run.
#[derive(Debug)]
pub struct RunOutcome {
    pub seed: u64,
    pub config: SimConfig,
    pub fingerprint: Fingerprint,
    pub report: Report,
    pub verdict: Verdict,
    pub stats: RunStats,
    /// True if the run hit its event budget before finishing.
    pub incomplete: bool,
    pub history: History,
    pub trace: Option<String>,
}

impl RunOutcome {
    pub fn failed(&self) -> bool {
        !self.report.is_empty()
    }

    /// A stable name for *how* this run failed. Shrinking uses it to insist
    /// that a smaller configuration reproduces the same bug rather than some
    /// other one it happened to stumble into.
    pub fn signature(&self) -> &'static str {
        self.report
            .signature()
            .unwrap_or(if self.incomplete { "incomplete" } else { "ok" })
    }

    pub fn summary(&self) -> String {
        let s = &self.stats;
        let membership = if s.reconfigs + s.reconfigs_refused > 0 {
            format!(
                " | {} reconfigs ({} refused, {} asked of new leaders)",
                s.reconfigs, s.reconfigs_refused, s.fresh_leader_reconfigs
            )
        } else {
            String::new()
        };
        format!(
            "seed {:>6} | {:>5} ops ({} abandoned, {} index reads) | {} committed | \
             {} elections, {} leaders | {} crashes, {} partitions{} | {} events | fp {} | {}",
            self.seed,
            s.ops_completed,
            s.ops_abandoned,
            s.index_reads,
            s.max_committed,
            s.elections,
            s.leaders_elected,
            s.crashes,
            s.partitions,
            membership,
            s.events,
            self.fingerprint,
            if self.failed() {
                self.signature()
            } else if self.incomplete {
                "incomplete"
            } else {
                "ok"
            }
        )
    }

    pub fn detail(&self) -> String {
        let mut s = String::new();
        s.push_str(&self.summary());
        s.push('\n');
        if !self.report.is_empty() {
            s.push_str("\nviolations:\n");
            s.push_str(&self.report.render());
        }
        if let Verdict::Unknown { key, reason } = &self.verdict {
            s.push_str(&format!(
                "\nlinearizability undecided for key {key}: {reason}\n"
            ));
        }
        s
    }
}

/// Run one simulation.
pub fn run(cfg: SimConfig) -> RunOutcome {
    Cluster::new(cfg).run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_benign_run_is_correct_and_makes_progress() {
        let out = run(SimConfig::benign(1));
        assert!(
            !out.failed(),
            "a fault-free run must be clean:\n{}",
            out.detail()
        );
        assert_eq!(out.verdict, Verdict::Linearizable);
        assert!(
            out.stats.ops_completed > 50,
            "expected real progress, got {}",
            out.stats.ops_completed
        );
        assert!(out.stats.leaders_elected >= 1, "someone has to lead");
    }

    #[test]
    fn a_follower_keeps_its_durability_claims_across_a_matching_snapshot() {
        // Regression: installing a snapshot that matched the follower's log
        // threw away its parked durability claims for the entries it kept. Its
        // durable index stuck, it acknowledged the same point forever, and the
        // leader resent forever -- 580,000 events where 24,000 was normal. No
        // safety property broke, so only the efficiency check can see it.
        let out = run(SimConfig::benign(200));
        assert!(!out.failed(), "{}", out.detail());
        assert!(
            out.stats.events < 60_000,
            "expected a normal event count, got {}",
            out.stats.events
        );
    }

    #[test]
    fn reads_bypass_the_log_by_default() {
        // ReadIndex is the default: reads are answered from memory after a
        // leadership check, so the log carries writes only.
        let index = run(SimConfig::benign(3));
        assert!(!index.failed(), "{}", index.detail());
        assert!(
            index.stats.index_reads > 20,
            "expected reads served through ReadIndex, got {}",
            index.stats.index_reads
        );

        let mut log_cfg = SimConfig::benign(3);
        log_cfg.raft.read_mode = kvstore::raft::ReadMode::Log;
        let log = run(log_cfg);
        assert!(!log.failed(), "{}", log.detail());
        assert_eq!(log.stats.index_reads, 0, "log mode must not use ReadIndex");
        assert!(
            log.stats.max_committed > index.stats.max_committed,
            "putting reads in the log should make it longer ({} vs {})",
            log.stats.max_committed,
            index.stats.max_committed
        );
    }

    #[test]
    fn read_index_stays_linearizable_under_faults() {
        // The same hostile world as every other sweep, with reads served from
        // memory: partitions strand leaders, and none of them may answer.
        for seed in 1..=6 {
            let mut cfg = SimConfig::with_seed(seed);
            cfg.duration = 6 * SECONDS;
            cfg.settle = 10 * SECONDS;
            cfg.drain = 3 * SECONDS;
            cfg.normalise();
            let out = run(cfg);
            assert!(!out.failed(), "{}", out.detail());
            assert!(
                out.stats.index_reads > 0,
                "seed {seed} served no index reads"
            );
        }
    }

    #[test]
    fn a_single_node_cluster_works() {
        // Degenerate but legal: quorum of one, elects itself immediately.
        let mut cfg = SimConfig::benign(7);
        cfg.servers = 1;
        cfg.clients = 2;
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
        assert!(out.stats.ops_completed > 20);
    }

    /// Sweep seeds in parallel and report how the first failure looked.
    ///
    /// Parallel rather than a sequential loop because the budget has to be
    /// realistic: the rarest of these defects shows up in roughly one run in
    /// three hundred, and a threshold set just above the observed rate is a
    /// flaky test waiting to happen.
    fn find_failure(bug: InjectedBug, seeds: u64) -> Option<(u64, &'static str)> {
        let mut base = SimConfig::with_seed(1).with_bug(bug);
        if bug == InjectedBug::ConfigBeforeTermCommit {
            base = base.with_reconfig(2);
        }
        base.duration = 6 * SECONDS;
        base.settle = 10 * SECONDS;
        base.drain = 3 * SECONDS;
        base.normalise();
        let result = sweep::sweep(sweep::SweepConfig {
            base,
            start_seed: 1,
            count: seeds,
            threads: sweep::default_threads(),
            stop_after: 1,
            verbose: false,
            quiet: true,
        });
        result.failures.first().map(|f| (f.seed, f.signature))
    }

    #[test]
    fn every_injected_bug_is_caught() {
        // The test that keeps the rest of the suite honest. A harness that has
        // never failed is indistinguishable from one that cannot fail, so each
        // deliberate defect has to be detected -- otherwise the oracles are
        // decoration.
        //
        // One defect is a known gap rather than a passing case, and it is
        // reported as such instead of being quietly dropped from the list.
        for bug in InjectedBug::ALL {
            let found = find_failure(bug, 400);
            match (found, bug.detection_gap()) {
                (Some((seed, signature)), _) => {
                    assert_ne!(signature, "ok");
                    // Every defect here breaks a safety property, so it has to
                    // be caught by one. A stalled cluster is not a catch: for a
                    // long time vote-before-sync "passed" this test as
                    // no_progress_after_recovery, because the send guard was
                    // dropping its early votes rather than letting them out,
                    // and the defect it claims to model was never exercised.
                    assert_ne!(
                        signature,
                        "no_progress_after_recovery",
                        "{} was caught only as a stall at seed {seed}",
                        bug.name()
                    );
                    println!("{} caught at seed {seed} as [{signature}]", bug.name());
                }
                (None, Some(why)) => {
                    println!("{} NOT caught (known gap): {why}", bug.name());
                }
                #[allow(unreachable_patterns)]
                (None, None) => panic!(
                    "the checkers did not notice a store that {} within 400 seeds",
                    bug.describe()
                ),
            }
        }
    }

    #[test]
    fn an_unmodified_store_survives_the_same_seeds() {
        // The control: the seeds that expose the injected defects must not
        // fail without them, or the test above proves nothing.
        assert_eq!(find_failure(InjectedBug::None, 400), None);
    }

    /// A seed pinned to the fault model it was found under.
    ///
    /// Every fault or protocol change added later reshuffles every seed's
    /// schedule, and a regression seed that no longer reaches its bug's state
    /// passes whether the fix is there or not. Anything added after these
    /// seeds were found is switched off here, so each one keeps reproducing its
    /// own bug.
    fn regression_seed(seed: u64) -> SimConfig {
        let mut cfg = SimConfig::with_seed(seed);
        cfg.faults.peer_isolate_weight = 0;
        cfg.faults.fresh_leader_reconfig_ppm = 0;
        cfg.raft.pre_vote = false;
        cfg
    }

    #[test]
    fn a_follower_only_acknowledges_what_it_has_verified() {
        // Regression for the matchIndex bug. With two-entry batches, a follower
        // used to acknowledge its whole durable log -- including an older-term
        // suffix the leader had never compared -- and the leader committed
        // entries that follower did not hold. Seed 388 overwrote committed data.
        let mut cfg = regression_seed(388);
        cfg.raft.max_batch = 2;
        cfg.duration = 8 * SECONDS;
        cfg.settle = 12 * SECONDS;
        cfg.drain = 3 * SECONDS;
        cfg.normalise();
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
    }

    fn snapshot_regression(seed: u64, threshold: u64, servers: usize, max_batch: usize) {
        let mut cfg = regression_seed(seed);
        cfg.servers = servers;
        cfg.raft.snapshot_threshold = threshold;
        cfg.raft.max_batch = max_batch;
        cfg.duration = 8 * SECONDS;
        cfg.settle = 12 * SECONDS;
        cfg.drain = 3 * SECONDS;
        cfg.normalise();
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
    }

    #[test]
    fn concurrent_snapshot_installs_do_not_share_an_image() {
        // Two installs in flight shared one data slot; the first completed with
        // the second's image missing, advanced last_applied over a state
        // machine that had not moved, and served a stale read.
        snapshot_regression(26, 3, 3, 64);
    }

    #[test]
    fn a_snapshot_write_never_targets_the_only_durable_copy() {
        // An install and a local snapshot in flight together: the second went
        // into the file holding the only durable snapshot and truncated it, and
        // a crash lost both -- along with every entry they had absorbed.
        snapshot_regression(287, 10, 5, 2);
    }

    #[test]
    fn a_term_learned_from_a_snapshot_reaches_disk() {
        // A follower restarted at term 15 and first heard of term 17 through
        // InstallSnapshot. That path adopted the term in memory but never wrote
        // the hard state, so the send guard withheld every reply it owed the
        // leader -- whose retries kept its election timer quiet. The cluster
        // stalled for good with a live leader and a live quorum.
        let mut cfg = regression_seed(392).with_reconfig(2);
        cfg.raft.snapshot_threshold = 5;
        cfg.raft.max_batch = 2;
        cfg.normalise();
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
    }

    #[test]
    fn a_repeated_vote_request_waits_for_the_vote_to_be_durable() {
        // A duplicated RequestVote arrived while the first grant's hard-state
        // write was in flight. The term had been durable for a while, so the
        // term-only send guard let the repeat grant out before the vote itself
        // reached disk -- a crash there would forget a vote already cast.
        let mut cfg = regression_seed(480).with_reconfig(2);
        cfg.normalise();
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
    }

    #[test]
    fn a_pre_vote_from_a_server_behind_on_terms_still_counts() {
        // A newly added server, still at term 0, granted a pre-vote for term 2
        // and answered in its own term. The candidate matched answers on the
        // voter's term, discarded the yes as stale, and stayed one pre-vote
        // short of a majority for the rest of the run: 33 operations instead
        // of 1094. Found under the current fault model, so not pinned.
        let mut cfg = SimConfig::with_seed(33976).with_reconfig(2);
        cfg.normalise();
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
    }

    #[test]
    fn config_normalisation_keeps_the_window_inside_the_run() {
        let mut cfg = SimConfig {
            duration: 3 * SECONDS,
            ..SimConfig::default()
        };
        cfg.normalise();
        assert_eq!(cfg.faults.window.1, 3 * SECONDS);
        assert!(cfg.faults.window.0 <= cfg.faults.window.1);
    }
}
