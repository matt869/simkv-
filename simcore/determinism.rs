//! Verifying that the simulator is actually deterministic.
//!
//! Everything else in this project is built on one assumption: a seed names a
//! run. If that assumption quietly breaks -- a `HashMap` iteration order leaks
//! into control flow, a pointer address gets hashed, a `SystemTime::now()`
//! sneaks in -- then shrinking chases ghosts and every recorded repro is
//! worthless. So the assumption is tested, not trusted.
//!
//! ## The rules a deterministic simulation has to follow
//!
//! * All randomness comes from [`crate::rng::Rng`], seeded from the run seed.
//! * All time comes from [`crate::scheduler::Scheduler`], never from the OS.
//! * No iteration over `HashMap`/`HashSet` may affect behaviour. This codebase
//!   uses `BTreeMap`/`BTreeSet` in simulated paths so the question cannot arise.
//! * No addresses, no thread ids, no environment.

use crate::trace::{Fingerprint, Record};

/// Two runs of the same seed disagreed.
#[derive(Clone, Debug)]
pub struct Divergence {
    pub run: usize,
    pub expected: Fingerprint,
    pub actual: Fingerprint,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "non-determinism: run {} produced fingerprint {} but run 0 produced {}",
            self.run, self.actual, self.expected
        )
    }
}

impl std::error::Error for Divergence {}

/// Run the same scenario `runs` times and require identical fingerprints.
///
/// The closure receives the run index purely so it can log; it must not use it
/// to change behaviour.
pub fn verify<F>(runs: usize, mut run: F) -> Result<Fingerprint, Divergence>
where
    F: FnMut(usize) -> Fingerprint,
{
    assert!(runs >= 1, "need at least one run");
    let first = run(0);
    for i in 1..runs {
        let f = run(i);
        if f != first {
            return Err(Divergence {
                run: i,
                expected: first,
                actual: f,
            });
        }
    }
    Ok(first)
}

/// Index of the first trace record where two runs diverge, for pinpointing
/// *where* determinism broke rather than merely that it did.
pub fn first_divergence(a: &[Record], b: &[Record]) -> Option<usize> {
    let n = a.len().min(b.len());
    for i in 0..n {
        if a[i].render() != b[i].render() {
            return Some(i);
        }
    }
    if a.len() == b.len() {
        None
    } else {
        Some(n)
    }
}

/// A side-by-side excerpt around a divergence point.
pub fn divergence_report(a: &[Record], b: &[Record], context: usize) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    match first_divergence(a, b) {
        None => out.push_str("traces are identical\n"),
        Some(i) => {
            let start = i.saturating_sub(context);
            let _ = writeln!(out, "first divergence at record {i}:");
            for j in start..=i {
                let _ = writeln!(
                    out,
                    "  A[{j}] {}",
                    a.get(j).map_or("<end>".into(), |r| r.render())
                );
                let _ = writeln!(
                    out,
                    "  B[{j}] {}",
                    b.get(j).map_or("<end>".into(), |r| r.render())
                );
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;
    use crate::trace::{Level, Trace};
    use crate::NodeId;

    #[test]
    fn deterministic_runs_agree() {
        let f = verify(5, |_| {
            let mut t = Trace::new(Level::Off);
            let mut r = Rng::new(99);
            for i in 0..100 {
                t.observe(i, Some(NodeId(0)), "x", &[r.next_u64()]);
            }
            t.fingerprint()
        })
        .expect("should be deterministic");
        assert_ne!(f, Fingerprint::new());
    }

    #[test]
    fn hidden_state_is_caught() {
        let mut counter = 0u64;
        let err = verify(3, |_| {
            counter += 1;
            let mut t = Trace::new(Level::Off);
            t.observe(0, None, "x", &[counter]);
            t.fingerprint()
        })
        .expect_err("must detect the smuggled counter");
        assert_eq!(err.run, 1);
    }

    #[test]
    fn divergence_points_at_the_first_difference() {
        let mk = |vals: &[u64]| {
            let mut t = Trace::new(Level::Debug);
            for v in vals {
                t.log(Level::Info, *v, None, "c", format!("v={v}"));
            }
            t
        };
        let a = mk(&[1, 2, 3, 4]);
        let b = mk(&[1, 2, 9, 4]);
        assert_eq!(first_divergence(a.records(), b.records()), Some(2));
        assert_eq!(first_divergence(a.records(), a.records()), None);
        // A prefix diverges at the point it runs out.
        let c = mk(&[1, 2]);
        assert_eq!(first_divergence(a.records(), c.records()), Some(2));
        assert!(divergence_report(a.records(), b.records(), 1).contains("record 2"));
    }
}
