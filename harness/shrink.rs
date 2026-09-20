//! Cutting a failing configuration down to the smallest one that still fails.
//!
//! A raw failure is a twenty-second run with five nodes, eight clients,
//! partitions, crashes, torn writes and clock skew all at once. Almost none of
//! that is load-bearing. Shrinking repeatedly proposes a simpler configuration
//! -- fewer nodes, less time, one fault type switched off -- keeps it if the run
//! *still fails the same way*, and stops when nothing more can be removed.
//!
//! What comes out is a much better bug report: "three nodes, four seconds, no
//! network faults at all, but torn writes on" says where to look in a way that
//! the original never did.
//!
//! A caveat worth being honest about. The seed is held fixed, but changing the
//! configuration changes how the random stream is consumed, so a shrunk run is
//! not the original run with pieces removed -- it is a different run that fails
//! the same way. That is what makes shrinking here a search rather than a
//! deduction, and it is why every candidate is verified by actually running it.

use crate::{run, RunOutcome, SimConfig};
use simcore::{Nanos, MILLIS};

#[derive(Debug)]
pub struct ShrinkResult {
    pub original: SimConfig,
    pub minimal: SimConfig,
    /// The failure kind that had to be preserved.
    pub signature: &'static str,
    /// Configurations tried.
    pub attempts: usize,
    /// Configurations that reproduced the failure and were adopted.
    pub steps: usize,
    /// Human-readable account of what was removed.
    pub removed: Vec<String>,
    pub outcome: RunOutcome,
}

impl ShrinkResult {
    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::new();
        let _ = writeln!(
            s,
            "shrank seed {} [{}] in {} steps ({} candidates tried)",
            self.original.seed, self.signature, self.steps, self.attempts
        );
        let _ = writeln!(s, "\nminimal reproduction:");
        let _ = writeln!(s, "{}", describe(&self.minimal));
        if !self.removed.is_empty() {
            let _ = writeln!(s, "removed along the way:");
            for r in &self.removed {
                let _ = writeln!(s, "  - {r}");
            }
        }
        let _ = writeln!(s, "\nfailure:\n{}", self.outcome.detail());
        s
    }
}

/// A one-line description of a configuration, in the form of the flags that
/// would rebuild it.
pub fn describe(cfg: &SimConfig) -> String {
    let f = &cfg.faults;
    let mut faults = Vec::new();
    if f.enable_crashes {
        faults.push("crashes");
    }
    if f.enable_partitions {
        faults.push("partitions");
    }
    if f.enable_clock_skew {
        faults.push("clock-skew");
    }
    if f.enable_slow_links {
        faults.push("slow-links");
    }
    let mut net = Vec::new();
    if cfg.net.drop_ppm > 0 {
        net.push(format!("drop={}ppm", cfg.net.drop_ppm));
    }
    if cfg.net.duplicate_ppm > 0 {
        net.push(format!("dup={}ppm", cfg.net.duplicate_ppm));
    }
    if cfg.net.corrupt_ppm > 0 {
        net.push(format!("corrupt={}ppm", cfg.net.corrupt_ppm));
    }
    let mut disk = Vec::new();
    if cfg.disk.io_error_ppm > 0 {
        disk.push(format!("io-error={}ppm", cfg.disk.io_error_ppm));
    }
    if cfg.disk.torn_write_ppm > 0 {
        disk.push(format!("torn={}ppm", cfg.disk.torn_write_ppm));
    }
    if cfg.disk.lost_write_ppm > 0 {
        disk.push(format!("lost-write={}ppm", cfg.disk.lost_write_ppm));
    }
    if cfg.disk.reorder_unsynced {
        disk.push("reorder-unsynced".into());
    }
    let w = &cfg.workload;
    let mix: Vec<String> = [
        ("read", w.read_weight),
        ("write", w.write_weight),
        ("cas", w.cas_weight),
        ("delete", w.delete_weight),
    ]
    .iter()
    .filter(|(_, weight)| *weight > 0)
    .map(|(name, weight)| format!("{name}:{weight}"))
    .collect();
    format!(
        "  seed={} servers={} clients={} keys={} duration={}ms settle={}ms\n  \
         faults=[{}]\n  net=[{}]\n  disk=[{}]\n  workload=[{}]",
        cfg.seed,
        cfg.servers,
        cfg.clients,
        cfg.workload.keys,
        cfg.duration / MILLIS,
        cfg.settle / MILLIS,
        faults.join(", "),
        net.join(", "),
        disk.join(", "),
        mix.join(", "),
    )
}

type Simplify = Box<dyn Fn(&SimConfig) -> Option<SimConfig>>;

/// One proposed simplification.
struct Candidate {
    label: &'static str,
    apply: Simplify,
}

fn candidates() -> Vec<Candidate> {
    fn c(label: &'static str, f: impl Fn(&SimConfig) -> Option<SimConfig> + 'static) -> Candidate {
        Candidate {
            label,
            apply: Box::new(f),
        }
    }

    // Ordered roughly by how much they simplify a report: switching a whole
    // fault class off says more than shaving a second off the run.
    vec![
        c("crashes disabled", |cfg| {
            cfg.faults.enable_crashes.then(|| {
                let mut n = cfg.clone();
                n.faults.enable_crashes = false;
                n
            })
        }),
        c("partitions disabled", |cfg| {
            cfg.faults.enable_partitions.then(|| {
                let mut n = cfg.clone();
                n.faults.enable_partitions = false;
                n
            })
        }),
        c("clock skew disabled", |cfg| {
            cfg.faults.enable_clock_skew.then(|| {
                let mut n = cfg.clone();
                n.faults.enable_clock_skew = false;
                n
            })
        }),
        c("slow links disabled", |cfg| {
            cfg.faults.enable_slow_links.then(|| {
                let mut n = cfg.clone();
                n.faults.enable_slow_links = false;
                n
            })
        }),
        c("message loss disabled", |cfg| {
            (cfg.net.drop_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.net.drop_ppm = 0;
                n
            })
        }),
        c("message duplication disabled", |cfg| {
            (cfg.net.duplicate_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.net.duplicate_ppm = 0;
                n
            })
        }),
        c("message corruption disabled", |cfg| {
            (cfg.net.corrupt_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.net.corrupt_ppm = 0;
                n
            })
        }),
        c("disk io errors disabled", |cfg| {
            (cfg.disk.io_error_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.disk.io_error_ppm = 0;
                n
            })
        }),
        c("torn writes disabled", |cfg| {
            (cfg.disk.torn_write_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.disk.torn_write_ppm = 0;
                n
            })
        }),
        c("lost writes disabled", |cfg| {
            (cfg.disk.lost_write_ppm > 0).then(|| {
                let mut n = cfg.clone();
                n.disk.lost_write_ppm = 0;
                n
            })
        }),
        c("unsynced write reordering disabled", |cfg| {
            cfg.disk.reorder_unsynced.then(|| {
                let mut n = cfg.clone();
                n.disk.reorder_unsynced = false;
                n
            })
        }),
        c("fewer servers", |cfg| {
            (cfg.servers > 1).then(|| {
                let mut n = cfg.clone();
                n.servers = if cfg.servers > 3 { 3 } else { 1 };
                n
            })
        }),
        c("fewer clients", |cfg| {
            (cfg.clients > 1).then(|| {
                let mut n = cfg.clone();
                n.clients = (cfg.clients / 2).max(1);
                n
            })
        }),
        c("fewer keys", |cfg| {
            (cfg.workload.keys > 1).then(|| {
                let mut n = cfg.clone();
                n.workload.keys = (cfg.workload.keys / 2).max(1);
                n
            })
        }),
        c("shorter run", |cfg| {
            (cfg.duration > 500 * MILLIS).then(|| {
                let mut n = cfg.clone();
                n.duration = (cfg.duration / 2).max(500 * MILLIS);
                n.normalise();
                n
            })
        }),
        c("shorter settle", |cfg| {
            (cfg.settle > 5 * simcore::SECONDS).then(|| {
                let mut n = cfg.clone();
                n.settle = (cfg.settle / 2).max(5 * simcore::SECONDS);
                n
            })
        }),
        c("no compare-and-set", |cfg| {
            (cfg.workload.cas_weight > 0).then(|| {
                let mut n = cfg.clone();
                n.workload.cas_weight = 0;
                n
            })
        }),
        c("no deletes", |cfg| {
            (cfg.workload.delete_weight > 0).then(|| {
                let mut n = cfg.clone();
                n.workload.delete_weight = 0;
                n
            })
        }),
        c("no reads", |cfg| {
            (cfg.workload.read_weight > 0 && cfg.workload.write_weight > 0).then(|| {
                let mut n = cfg.clone();
                n.workload.read_weight = 0;
                n
            })
        }),
    ]
}

/// Shrink `cfg`, which must already fail.
///
/// `budget` bounds the number of candidate runs; shrinking is otherwise run to
/// a fixed point.
pub fn shrink(cfg: SimConfig, budget: usize, mut log: impl FnMut(&str)) -> Option<ShrinkResult> {
    let mut current = cfg.clone();
    current.normalise();
    let first = run(current.clone());
    if !first.failed() {
        log(&format!(
            "seed {} does not fail with this configuration; nothing to shrink",
            cfg.seed
        ));
        return None;
    }
    let signature = first.signature();
    log(&format!(
        "shrinking seed {} failing as [{signature}]",
        cfg.seed
    ));

    let mut best = first;
    let mut attempts = 0usize;
    let mut steps = 0usize;
    let mut removed = Vec::new();
    let cs = candidates();

    // Keep sweeping the candidate list until a whole pass changes nothing.
    loop {
        let mut progressed = false;
        for cand in &cs {
            if attempts >= budget {
                log("shrink budget exhausted");
                return Some(ShrinkResult {
                    original: cfg,
                    minimal: current,
                    signature,
                    attempts,
                    steps,
                    removed,
                    outcome: best,
                });
            }
            let Some(next) = (cand.apply)(&current) else {
                continue;
            };
            attempts += 1;
            let outcome = run(next.clone());
            if outcome.failed() && outcome.signature() == signature {
                log(&format!("  kept: {}", cand.label));
                current = next;
                best = outcome;
                steps += 1;
                progressed = true;
                removed.push(cand.label.to_string());
            }
        }
        if !progressed {
            break;
        }
    }

    Some(ShrinkResult {
        original: cfg,
        minimal: current,
        signature,
        attempts,
        steps,
        removed,
        outcome: best,
    })
}

/// Shorten a duration to the nearest multiple of 100ms, for tidier reports.
pub fn round_duration(d: Nanos) -> Nanos {
    let unit = 100 * MILLIS;
    d.div_ceil(unit) * unit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shrinking_a_passing_config_returns_nothing() {
        let mut logged = String::new();
        let r = shrink(SimConfig::benign(1), 10, |s| logged.push_str(s));
        assert!(r.is_none());
        assert!(logged.contains("does not fail"));
    }

    #[test]
    fn every_candidate_strictly_simplifies() {
        // A candidate that returns an unchanged config would loop forever.
        let cfg = SimConfig::default();
        for c in candidates() {
            if let Some(next) = (c.apply)(&cfg) {
                assert_ne!(
                    describe(&next),
                    describe(&cfg),
                    "candidate {:?} did not change anything visible",
                    c.label
                );
            }
        }
    }

    #[test]
    fn candidates_are_idempotent_at_their_floor() {
        // Applying a candidate to a config it has already been applied to must
        // eventually return None, or shrinking would not terminate.
        let mut cfg = SimConfig {
            servers: 1,
            clients: 1,
            duration: 500 * MILLIS,
            settle: 5 * simcore::SECONDS,
            ..SimConfig::default()
        };
        cfg.workload.keys = 1;
        cfg.faults = simcore::faults::FaultConfig::none();
        cfg.net = simcore::net::NetConfig::reliable();
        cfg.disk = simcore::disk::DiskConfig::reliable();
        cfg.workload.cas_weight = 0;
        cfg.workload.delete_weight = 0;
        cfg.workload.read_weight = 0;
        for c in candidates() {
            assert!(
                (c.apply)(&cfg).is_none(),
                "candidate {:?} still wants to shrink a minimal config",
                c.label
            );
        }
    }

    #[test]
    fn describe_names_the_active_faults() {
        let cfg = SimConfig::default();
        let d = describe(&cfg);
        assert!(d.contains("crashes"));
        assert!(d.contains("partitions"));
        assert!(d.contains("torn="));
        assert!(
            d.contains("cas:"),
            "the op mix belongs in a repro description"
        );
    }

    #[test]
    fn round_duration_rounds_up() {
        assert_eq!(round_duration(1), 100 * MILLIS);
        assert_eq!(round_duration(100 * MILLIS), 100 * MILLIS);
        assert_eq!(round_duration(101 * MILLIS), 200 * MILLIS);
    }
}
