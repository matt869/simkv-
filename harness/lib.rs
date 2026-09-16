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
use kvstore::raft::RaftConfig;
use simcore::disk::DiskConfig;
use simcore::faults::FaultConfig;
use simcore::net::NetConfig;
use simcore::trace::{Fingerprint, Level};
use simcore::{Nanos, MILLIS, SECONDS};

/// Everything that determines a run. Same config plus same seed, same run.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub servers: usize,
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
    pub check_liveness: bool,
    pub linearizability_budget: u64,
    pub net: NetConfig,
    pub disk: DiskConfig,
    pub faults: FaultConfig,
    pub raft: RaftConfig,
    pub workload: WorkloadConfig,
    pub trace_level: Level,
}

impl Default for SimConfig {
    fn default() -> Self {
        let duration = 20 * SECONDS;
        SimConfig {
            seed: 0,
            servers: 3,
            clients: 4,
            duration,
            settle: 15 * SECONDS,
            drain: 5 * SECONDS,
            max_events: 4_000_000,
            check_every: 1,
            check_liveness: true,
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
        self.report.signature().unwrap_or(if self.incomplete {
            "incomplete"
        } else {
            "ok"
        })
    }

    pub fn summary(&self) -> String {
        let s = &self.stats;
        format!(
            "seed {:>6} | {:>5} ops ({} abandoned) | {} committed | {} elections, {} leaders | \
             {} crashes, {} partitions | {} events | fp {} | {}",
            self.seed,
            s.ops_completed,
            s.ops_abandoned,
            s.max_committed,
            s.elections,
            s.leaders_elected,
            s.crashes,
            s.partitions,
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
            s.push_str(&format!("\nlinearizability undecided for key {key}: {reason}\n"));
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
    fn a_single_node_cluster_works() {
        // Degenerate but legal: quorum of one, elects itself immediately.
        let mut cfg = SimConfig::benign(7);
        cfg.servers = 1;
        cfg.clients = 2;
        let out = run(cfg);
        assert!(!out.failed(), "{}", out.detail());
        assert!(out.stats.ops_completed > 20);
    }

    #[test]
    fn config_normalisation_keeps_the_window_inside_the_run() {
        let mut cfg = SimConfig::default();
        cfg.duration = 3 * SECONDS;
        cfg.normalise();
        assert_eq!(cfg.faults.window.1, 3 * SECONDS);
        assert!(cfg.faults.window.0 <= cfg.faults.window.1);
    }
}
