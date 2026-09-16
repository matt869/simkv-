//! `checker` -- the oracles that decide whether a run was correct.
//!
//! Three of them, at different levels:
//!
//! * [`invariants`] watches the cluster's internal state continuously and
//!   enforces the Raft safety properties -- one leader per term, log matching,
//!   committed entries never lost or rewritten. These catch bugs at the moment
//!   they happen, with the state still on screen.
//! * [`durability`] checks the story the disks tell: that everything the
//!   cluster considers committed really is on stable storage on a majority, and
//!   that a restarted node recovers a prefix of what it had.
//! * [`linearizability`] checks the only thing a user can actually observe --
//!   the sequence of requests and replies -- against a single-threaded model.
//!
//! The internal checks are the ones that make failures debuggable; the external
//! check is the one that makes them meaningful. A system can pass every
//! invariant and still hand a client a stale read.

pub mod durability;
pub mod invariants;
pub mod linearizability;

use simcore::{Nanos, NodeId};

/// Something that must never happen, having happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Violation {
    /// Short stable name, used to tell failures apart when shrinking.
    pub kind: &'static str,
    pub time: Nanos,
    pub node: Option<NodeId>,
    pub detail: String,
}

impl Violation {
    pub fn new(kind: &'static str, time: Nanos, node: Option<NodeId>, detail: String) -> Violation {
        Violation {
            kind,
            time,
            node,
            detail,
        }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ms = self.time as f64 / simcore::MILLIS as f64;
        match self.node {
            Some(n) => write!(f, "[{ms:.3}ms {n}] {}: {}", self.kind, self.detail),
            None => write!(f, "[{ms:.3}ms] {}: {}", self.kind, self.detail),
        }
    }
}

/// Everything wrong with one run, in the order it was noticed.
#[derive(Clone, Debug, Default)]
pub struct Report {
    violations: Vec<Violation>,
}

impl Report {
    pub fn new() -> Report {
        Report::default()
    }

    pub fn add(&mut self, v: Violation) {
        // One broken invariant usually trips several checks on every
        // subsequent event; keeping the first of each kind keeps the report
        // readable without hiding a genuinely different second failure.
        if self.violations.iter().filter(|e| e.kind == v.kind).count() < 3 {
            self.violations.push(v);
        }
    }

    pub fn extend(&mut self, vs: impl IntoIterator<Item = Violation>) {
        for v in vs {
            self.add(v);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }

    /// The name of the first failure, used as the identity of a bug when
    /// shrinking: a smaller input only counts if it still fails this way.
    pub fn signature(&self) -> Option<&'static str> {
        self.violations.first().map(|v| v.kind)
    }

    pub fn render(&self) -> String {
        let mut s = String::new();
        for v in &self.violations {
            s.push_str(&v.to_string());
            s.push('\n');
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_keeps_the_first_failures_of_each_kind() {
        let mut r = Report::new();
        for i in 0..10 {
            r.add(Violation::new("election_safety", i, None, format!("v{i}")));
        }
        r.add(Violation::new("log_matching", 100, None, "x".into()));
        assert_eq!(r.violations().len(), 4);
        assert_eq!(r.signature(), Some("election_safety"));
        assert!(r.render().contains("log_matching"));
    }

    #[test]
    fn an_empty_report_has_no_signature() {
        assert!(Report::new().is_empty());
        assert_eq!(Report::new().signature(), None);
    }
}
