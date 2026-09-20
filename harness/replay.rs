//! Replaying a seed, and proving the replay is faithful.
//!
//! Reproducing a failure is only useful if the reproduction is the *same* run.
//! So replay does two things at once: it re-runs the seed with tracing turned
//! up, and it runs it several times over to confirm every run produces an
//! identical fingerprint.
//!
//! That second part is not ceremony. The fingerprint is fed only by
//! [`simcore::trace::Trace::observe`], which is deliberately independent of the
//! log level -- so a silent run and a fully traced run must agree. If they ever
//! disagree, tracing is perturbing the system, every seed recorded so far is
//! suspect, and the harness has to be fixed before any bug report from it can
//! be believed.

use crate::{run, RunOutcome, SimConfig};
use simcore::determinism::{self, Divergence};
use simcore::trace::{Fingerprint, Level};

pub struct ReplayResult {
    pub outcome: RunOutcome,
    pub fingerprint: Fingerprint,
    /// One entry per verification run, in order.
    pub fingerprints: Vec<Fingerprint>,
    pub divergence: Option<Divergence>,
    pub trace: String,
}

impl ReplayResult {
    pub fn deterministic(&self) -> bool {
        self.divergence.is_none()
    }

    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::new();
        let _ = writeln!(s, "{}", self.outcome.detail());
        match &self.divergence {
            None => {
                let _ = writeln!(
                    s,
                    "determinism: {} runs agreed on fingerprint {}",
                    self.fingerprints.len(),
                    self.fingerprint
                );
            }
            Some(d) => {
                let _ = writeln!(s, "DETERMINISM FAILURE: {d}");
                let _ = writeln!(
                    s,
                    "the simulator is not reproducible; fix this before trusting any seed"
                );
            }
        }
        s
    }
}

/// Re-run `cfg` with tracing at `level`, and verify it is reproducible.
///
/// `verify_runs` extra runs are made at the *silent* level; agreeing with the
/// traced run is what proves tracing is free of side effects.
pub fn replay(cfg: SimConfig, level: Level, verify_runs: usize) -> ReplayResult {
    let mut traced = cfg.clone();
    traced.trace_level = level;
    traced.normalise();
    let outcome = run(traced);
    let fingerprint = outcome.fingerprint;
    let trace = outcome.trace.clone().unwrap_or_default();

    let mut fingerprints = vec![fingerprint];
    let mut divergence = None;
    if verify_runs > 0 {
        let mut silent = cfg.clone();
        silent.trace_level = Level::Off;
        silent.normalise();
        let result = determinism::verify(verify_runs + 1, |i| {
            if i == 0 {
                fingerprint
            } else {
                let f = run(silent.clone()).fingerprint;
                fingerprints.push(f);
                f
            }
        });
        if let Err(d) = result {
            divergence = Some(d);
        }
    }

    ReplayResult {
        outcome,
        fingerprint,
        fingerprints,
        divergence,
        trace,
    }
}

/// Write a replay's trace to a file.
pub fn write_trace(path: &str, result: &ReplayResult) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "# seed {}", result.outcome.seed)?;
    writeln!(f, "# fingerprint {}", result.fingerprint)?;
    writeln!(f, "# {}", result.outcome.summary())?;
    if !result.outcome.report.is_empty() {
        writeln!(f, "#\n# violations:")?;
        for v in result.outcome.report.violations() {
            writeln!(f, "#   {v}")?;
        }
    }
    writeln!(f)?;
    f.write_all(result.trace.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_replays_identically() {
        let r = replay(SimConfig::benign(11), Level::Off, 2);
        assert!(r.deterministic(), "{}", r.render());
        assert_eq!(r.fingerprints.len(), 3);
        assert!(r.fingerprints.iter().all(|f| *f == r.fingerprint));
    }

    #[test]
    fn tracing_does_not_change_the_run() {
        // The property the whole debugging story rests on: turning the log up
        // must not alter a single decision.
        let cfg = SimConfig::benign(12);
        let quiet = run({
            let mut c = cfg.clone();
            c.trace_level = Level::Off;
            c
        });
        let loud = replay(cfg, Level::Debug, 0);
        assert_eq!(
            quiet.fingerprint, loud.fingerprint,
            "tracing perturbed the simulation"
        );
        assert!(
            !loud.trace.is_empty(),
            "debug replay should produce a trace"
        );
        assert_eq!(quiet.stats.ops_completed, loud.outcome.stats.ops_completed);
    }

    #[test]
    fn different_seeds_produce_different_fingerprints() {
        let a = replay(SimConfig::benign(21), Level::Off, 0);
        let b = replay(SimConfig::benign(22), Level::Off, 0);
        assert_ne!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn a_faulty_run_is_also_reproducible() {
        let mut cfg = SimConfig::with_seed(4242);
        cfg.duration = 3 * simcore::SECONDS;
        cfg.settle = 6 * simcore::SECONDS;
        cfg.drain = 2 * simcore::SECONDS;
        cfg.normalise();
        let r = replay(cfg, Level::Off, 2);
        assert!(r.deterministic(), "{}", r.render());
    }
}
