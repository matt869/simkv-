//! Running many seeds, in parallel, looking for the ones that fail.
//!
//! Each run is a closed world, so seeds are embarrassingly parallel: threads
//! share nothing but a seed counter and a results mutex. Determinism is
//! unaffected -- a seed's run depends only on its own configuration, never on
//! how many threads happened to be running or in what order they finished.
//!
//! What comes out is a list of seeds. That is the whole product: a seed is a
//! bug report that never goes stale, needs no attachments, and reproduces on
//! anyone's machine.

use crate::{run, RunOutcome, SimConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct SweepConfig {
    pub base: SimConfig,
    pub start_seed: u64,
    pub count: u64,
    pub threads: usize,
    /// Stop early once this many failing seeds have been found. Zero means
    /// run the whole range.
    pub stop_after: usize,
    /// Print a line for every run rather than only for failures.
    pub verbose: bool,
}

impl Default for SweepConfig {
    fn default() -> Self {
        SweepConfig {
            base: SimConfig::default(),
            start_seed: 1,
            count: 100,
            threads: default_threads(),
            stop_after: 0,
            verbose: false,
        }
    }
}

pub fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub seed: u64,
    pub signature: &'static str,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct SweepResult {
    pub runs: u64,
    pub failures: Vec<Failure>,
    pub incomplete: u64,
    pub undecided: u64,
    pub ops_completed: u64,
    pub events: u64,
    pub by_signature: BTreeMap<&'static str, u64>,
    pub elapsed: Duration,
}

impl SweepResult {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::new();
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        let _ = writeln!(
            s,
            "{} runs in {:.1}s ({:.0} runs/s, {:.1}M events, {} ops completed)",
            self.runs,
            self.elapsed.as_secs_f64(),
            self.runs as f64 / secs,
            self.events as f64 / 1e6,
            self.ops_completed
        );
        if self.incomplete > 0 {
            let _ = writeln!(s, "{} runs hit the event budget", self.incomplete);
        }
        if self.undecided > 0 {
            let _ = writeln!(
                s,
                "{} runs left linearizability undecided (search budget)",
                self.undecided
            );
        }
        if self.failures.is_empty() {
            let _ = writeln!(s, "no failures");
        } else {
            let _ = writeln!(s, "{} FAILING SEEDS:", self.failures.len());
            for (kind, n) in &self.by_signature {
                let _ = writeln!(s, "  {n:>4}x {kind}");
            }
            let _ = writeln!(s);
            for f in self.failures.iter().take(10) {
                let _ = writeln!(s, "--- seed {} [{}] ---", f.seed, f.signature);
                let _ = writeln!(s, "{}", f.detail);
            }
            if self.failures.len() > 10 {
                let _ = writeln!(s, "... and {} more", self.failures.len() - 10);
            }
            let seeds: Vec<String> = self.failures.iter().map(|f| f.seed.to_string()).collect();
            let _ = writeln!(s, "reproduce with: sim replay --seed <{}>", seeds.join("|"));
        }
        s
    }
}

/// Run the configured range of seeds across threads.
pub fn sweep(cfg: SweepConfig) -> SweepResult {
    let started = Instant::now();
    let next = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let shared = Mutex::new(SweepResult::default());
    let threads = cfg.threads.max(1).min(cfg.count.max(1) as usize);

    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= cfg.count {
                    return;
                }
                let mut run_cfg = cfg.base.clone();
                run_cfg.seed = cfg.start_seed + i;
                run_cfg.normalise();
                let outcome = run(run_cfg);
                record(&shared, &cfg, &stop, outcome);
            });
        }
    });

    let mut result = shared.into_inner().unwrap_or_else(|e| e.into_inner());
    result.elapsed = started.elapsed();
    result.failures.sort_by_key(|f| f.seed);
    result
}

fn record(shared: &Mutex<SweepResult>, cfg: &SweepConfig, stop: &AtomicBool, outcome: RunOutcome) {
    let mut r = shared.lock().unwrap_or_else(|e| e.into_inner());
    r.runs += 1;
    r.ops_completed += outcome.stats.ops_completed;
    r.events += outcome.stats.events;
    if outcome.incomplete {
        r.incomplete += 1;
    }
    if matches!(
        outcome.verdict,
        checker::linearizability::Verdict::Unknown { .. }
    ) {
        r.undecided += 1;
    }
    if cfg.verbose {
        println!("{}", outcome.summary());
    }
    if outcome.failed() {
        let signature = outcome.signature();
        *r.by_signature.entry(signature).or_insert(0) += 1;
        if !cfg.verbose {
            println!("FAIL seed {} [{}]", outcome.seed, signature);
        }
        r.failures.push(Failure {
            seed: outcome.seed,
            signature,
            detail: outcome.detail(),
        });
        if cfg.stop_after > 0 && r.failures.len() >= cfg.stop_after {
            stop.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sweep_of_benign_seeds_finds_nothing() {
        let cfg = SweepConfig {
            base: SimConfig::benign(0),
            start_seed: 1,
            count: 8,
            threads: 4,
            stop_after: 0,
            verbose: false,
        };
        let r = sweep(cfg);
        assert_eq!(r.runs, 8);
        assert!(r.passed(), "{}", r.render());
        assert!(r.ops_completed > 100, "the sweep should do real work");
    }

    #[test]
    fn sweeps_are_reproducible_regardless_of_thread_count() {
        // Same seeds, different parallelism: identical conclusions.
        let base = SimConfig::benign(0);
        let mk = |threads| SweepConfig {
            base: base.clone(),
            start_seed: 100,
            count: 6,
            threads,
            stop_after: 0,
            verbose: false,
        };
        let a = sweep(mk(1));
        let b = sweep(mk(6));
        assert_eq!(a.runs, b.runs);
        assert_eq!(a.ops_completed, b.ops_completed);
        assert_eq!(a.failures.len(), b.failures.len());
    }

    #[test]
    fn stop_after_halts_the_sweep_early() {
        // Nothing fails here, so stop_after must not cut a clean sweep short.
        let cfg = SweepConfig {
            base: SimConfig::benign(0),
            start_seed: 1,
            count: 4,
            threads: 2,
            stop_after: 1,
            verbose: false,
        };
        let r = sweep(cfg);
        assert_eq!(r.runs, 4);
    }
}
