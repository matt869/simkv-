//! Linearizability checking.
//!
//! A history is linearizable if you can pick one instant inside each
//! operation's invoke..return window, order the operations by those instants,
//! and get a sequence that a single-threaded model would produce. Deciding this
//! is NP-complete in general, so this is the standard search -- Wing & Gong's
//! algorithm with the refinements that make it usable in practice:
//!
//! * **Partition by key.** Operations on different keys cannot influence each
//!   other under this model, and a history is linearizable exactly when each
//!   per-key sub-history is (Herlihy & Wing's locality property). Ten keys turn
//!   one intractable search into ten small ones.
//! * **Memoise on (set of linearized ops, model state).** Two different orders
//!   that consume the same operations and reach the same state are
//!   interchangeable; without this the search re-explores permutations forever.
//! * **Incremental lift/unlift.** Linearizing an operation removes its call and
//!   return from a doubly linked list; backtracking puts them back, which is
//!   O(1) rather than rebuilding the history.
//!
//! Operations that never received a definite answer are the subtle part. A
//! client that times out does not know whether its write happened, and neither
//! does the checker: such an operation is given a return at the end of time and
//! a result that matches anything, so the search may place it anywhere after its
//! invocation -- including last, which is equivalent to it never happening.

use kvstore::log::Op;
use kvstore::server::Outcome;
use simcore::Nanos;
use std::collections::{BTreeMap, HashSet};

/// One client operation as observed from outside.
#[derive(Clone, Debug)]
pub struct Operation {
    pub id: u64,
    pub client: u32,
    pub op: Op,
    /// When the client sent the first attempt.
    pub invoked: Nanos,
    /// When the client learned the answer. `None` if it never did.
    pub completed: Option<Nanos>,
    /// The definite answer, if there was one.
    pub outcome: Option<Outcome>,
}

impl Operation {
    pub fn is_pending(&self) -> bool {
        self.outcome.is_none()
    }

    pub fn key(&self) -> Option<&str> {
        self.op.key()
    }

    fn render(&self) -> String {
        let op = match &self.op {
            Op::Noop => "noop".to_string(),
            Op::Get { key } => format!("get({key})"),
            Op::Put { key, value } => format!("put({key}, {value})"),
            Op::Delete { key } => format!("del({key})"),
            Op::Cas { key, expect, value } => {
                format!("cas({key}, {expect:?} -> {value})")
            }
        };
        let ret = match (&self.outcome, self.completed) {
            (Some(o), Some(t)) => format!("=> {o:?} at {t}"),
            _ => "=> <no answer>".to_string(),
        };
        format!(
            "c{} #{} {op} [{}..] {ret}",
            self.client, self.id, self.invoked
        )
    }
}

/// The full observed history.
#[derive(Clone, Debug, Default)]
pub struct History {
    ops: Vec<Operation>,
    /// operation id -> position, so completing an operation stays O(1) rather
    /// than rescanning a history that grows to tens of thousands of entries.
    index: BTreeMap<u64, usize>,
}

impl History {
    pub fn new() -> History {
        History::default()
    }

    pub fn push(&mut self, op: Operation) {
        self.index.insert(op.id, self.ops.len());
        self.ops.push(op);
    }

    /// Record the definite answer to an operation. Returns false if the id is
    /// unknown, which would mean the recorder lost track of its own request.
    pub fn complete(&mut self, id: u64, time: Nanos, outcome: Outcome) -> bool {
        let Some(i) = self.index.get(&id) else {
            return false;
        };
        let op = &mut self.ops[*i];
        op.completed = Some(time);
        op.outcome = Some(outcome);
        true
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn operations(&self) -> &[Operation] {
        &self.ops
    }

    pub fn completed(&self) -> usize {
        self.ops.iter().filter(|o| !o.is_pending()).count()
    }

    pub fn pending(&self) -> usize {
        self.ops.iter().filter(|o| o.is_pending()).count()
    }

    /// Sanity checks on the recording itself. A history that fails these says
    /// the harness is broken, not the store.
    pub fn well_formed(&self) -> Result<(), String> {
        let mut seen = HashSet::new();
        for o in &self.ops {
            if !seen.insert(o.id) {
                return Err(format!("duplicate operation id {}", o.id));
            }
            if let Some(t) = o.completed {
                if t < o.invoked {
                    return Err(format!("operation {} completed before it started", o.id));
                }
            }
            if o.completed.is_some() != o.outcome.is_some() {
                return Err(format!(
                    "operation {} has a completion time and outcome that disagree",
                    o.id
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    Violation {
        key: String,
        detail: String,
    },
    /// The search ran out of budget. Not a pass and not a failure.
    Unknown {
        key: String,
        reason: String,
    },
}

impl Verdict {
    pub fn is_violation(&self) -> bool {
        matches!(self, Verdict::Violation { .. })
    }
}

/// The value of one key: the whole model, since keys are independent.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Reg {
    Absent,
    Value(String),
}

impl Reg {
    fn as_opt(&self) -> Option<&String> {
        match self {
            Reg::Absent => None,
            Reg::Value(v) => Some(v),
        }
    }
}

/// Apply one operation to the model.
///
/// Returns the resulting state and whether the observed outcome is consistent
/// with taking this step here. A pending operation accepts any result.
fn step(state: &Reg, op: &Operation) -> (bool, Reg) {
    let wildcard = op.outcome.is_none();
    match &op.op {
        Op::Noop => (true, state.clone()),
        Op::Get { .. } => {
            let expected = Outcome::Value(state.as_opt().cloned());
            let ok = wildcard || op.outcome.as_ref() == Some(&expected);
            (ok, state.clone())
        }
        Op::Put { value, .. } => {
            let ok = wildcard || op.outcome.as_ref() == Some(&Outcome::Written);
            (ok, Reg::Value(value.clone()))
        }
        Op::Delete { .. } => {
            let ok = wildcard || op.outcome.as_ref() == Some(&Outcome::Written);
            (ok, Reg::Absent)
        }
        Op::Cas { expect, value, .. } => {
            let matched = state.as_opt() == expect.as_ref();
            let ok = wildcard || op.outcome.as_ref() == Some(&Outcome::Cas(matched));
            let next = if matched {
                Reg::Value(value.clone())
            } else {
                state.clone()
            };
            (ok, next)
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Node {
    is_call: bool,
    op: usize,
    matching: usize,
    prev: usize,
    next: usize,
}

/// How much work the search may do per key before giving up.
pub const DEFAULT_BUDGET: u64 = 5_000_000;

#[derive(Clone, Copy, Debug, Default)]
pub struct CheckStats {
    pub partitions: usize,
    pub operations: usize,
    pub steps: u64,
    pub max_partition: usize,
}

pub struct Checker {
    pub budget: u64,
    pub stats: CheckStats,
}

impl Default for Checker {
    fn default() -> Self {
        Checker {
            budget: DEFAULT_BUDGET,
            stats: CheckStats::default(),
        }
    }
}

impl Checker {
    pub fn new() -> Checker {
        Checker::default()
    }

    /// Check a whole history, one key at a time.
    pub fn check(&mut self, history: &History) -> Verdict {
        let mut by_key: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
        for op in &history.ops {
            if let Some(k) = op.key() {
                by_key.entry(k).or_default().push(op);
            }
        }
        self.stats.partitions = by_key.len();
        self.stats.operations = history.len();
        self.stats.max_partition = by_key.values().map(|v| v.len()).max().unwrap_or(0);

        for (key, ops) in by_key {
            match self.check_partition(key, &ops) {
                Verdict::Linearizable => {}
                other => return other,
            }
        }
        Verdict::Linearizable
    }

    fn check_partition(&mut self, key: &str, ops: &[&Operation]) -> Verdict {
        let n = ops.len();
        if n == 0 {
            return Verdict::Linearizable;
        }

        // Build the entry list: a call at the invoke time and a return at the
        // completion time. Pending operations return at the end of time.
        // Returns sort before calls at equal timestamps, which is what makes
        // the real-time constraint bite: an operation that returned at t must
        // be linearized before one invoked at t.
        let mut order: Vec<(Nanos, u8, u64, usize, bool)> = Vec::with_capacity(2 * n);
        for (i, op) in ops.iter().enumerate() {
            order.push((op.invoked, 1, op.id, i, true));
            order.push((op.completed.unwrap_or(Nanos::MAX), 0, op.id, i, false));
        }
        order.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
                .then(a.4.cmp(&b.4))
        });

        let sentinel = 2 * n;
        let mut nodes = vec![
            Node {
                is_call: false,
                op: usize::MAX,
                matching: usize::MAX,
                prev: sentinel,
                next: sentinel,
            };
            2 * n + 1
        ];
        let mut call_of = vec![usize::MAX; n];
        let mut ret_of = vec![usize::MAX; n];
        for (pos, (_, _, _, op, is_call)) in order.iter().enumerate() {
            nodes[pos].is_call = *is_call;
            nodes[pos].op = *op;
            if *is_call {
                call_of[*op] = pos;
            } else {
                ret_of[*op] = pos;
            }
        }
        for i in 0..n {
            nodes[call_of[i]].matching = ret_of[i];
            nodes[ret_of[i]].matching = call_of[i];
        }
        // Thread the list, with `sentinel` as both head and tail.
        let mut prev = sentinel;
        for pos in 0..2 * n {
            nodes[prev].next = pos;
            nodes[pos].prev = prev;
            prev = pos;
        }
        nodes[prev].next = sentinel;
        nodes[sentinel].prev = prev;

        let words = n.div_ceil(64);
        let mut linearized = vec![0u64; words];
        let mut cache: HashSet<(Vec<u64>, Reg)> = HashSet::new();
        let mut calls: Vec<(usize, Reg)> = Vec::new();
        let mut state = Reg::Absent;
        let mut entry = nodes[sentinel].next;
        let mut steps = 0u64;

        loop {
            if entry == sentinel {
                self.stats.steps += steps;
                return Verdict::Linearizable;
            }
            steps += 1;
            if steps > self.budget {
                self.stats.steps += steps;
                return Verdict::Unknown {
                    key: key.to_string(),
                    reason: format!(
                        "search budget of {} steps exhausted over {n} operations",
                        self.budget
                    ),
                };
            }

            if nodes[entry].is_call {
                let i = nodes[entry].op;
                let (ok, next_state) = step(&state, ops[i]);
                let mut advanced = false;
                if ok {
                    let mut bits = linearized.clone();
                    bits[i / 64] |= 1 << (i % 64);
                    if cache.insert((bits.clone(), next_state.clone())) {
                        linearized = bits;
                        calls.push((entry, state));
                        state = next_state;
                        let ret = nodes[entry].matching;
                        unlink(&mut nodes, entry);
                        unlink(&mut nodes, ret);
                        entry = nodes[sentinel].next;
                        advanced = true;
                    }
                }
                if !advanced {
                    entry = nodes[entry].next;
                }
            } else {
                // A return reached before its call was linearized: this prefix
                // cannot be extended, so undo the most recent choice.
                let Some((call, prior)) = calls.pop() else {
                    self.stats.steps += steps;
                    return Verdict::Violation {
                        key: key.to_string(),
                        detail: describe(key, ops, nodes[entry].op),
                    };
                };
                let i = nodes[call].op;
                linearized[i / 64] &= !(1 << (i % 64));
                state = prior;
                let ret = nodes[call].matching;
                relink(&mut nodes, ret);
                relink(&mut nodes, call);
                entry = nodes[call].next;
            }
        }
    }
}

fn unlink(nodes: &mut [Node], i: usize) {
    let (p, n) = (nodes[i].prev, nodes[i].next);
    nodes[p].next = n;
    nodes[n].prev = p;
}

/// Undo an [`unlink`]. Valid because removals and restores are perfectly
/// nested: the node's own prev/next were left untouched.
fn relink(nodes: &mut [Node], i: usize) {
    let (p, n) = (nodes[i].prev, nodes[i].next);
    nodes[p].next = i;
    nodes[n].prev = i;
}

fn describe(key: &str, ops: &[&Operation], stuck: usize) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    let _ = writeln!(
        s,
        "no ordering of the {} operations on key {key:?} satisfies the model",
        ops.len()
    );
    let _ = writeln!(s, "the search got stuck at: {}", ops[stuck].render());
    let _ = writeln!(s, "history for this key, in invocation order:");
    let mut sorted: Vec<&&Operation> = ops.iter().collect();
    sorted.sort_by_key(|o| (o.invoked, o.id));
    for o in sorted {
        let _ = writeln!(s, "  {}", o.render());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(id: u64, client: u32, invoked: Nanos, completed: Nanos, o: Op, out: Outcome) -> Operation {
        Operation {
            id,
            client,
            op: o,
            invoked,
            completed: Some(completed),
            outcome: Some(out),
        }
    }

    fn pending(id: u64, client: u32, invoked: Nanos, o: Op) -> Operation {
        Operation {
            id,
            client,
            op: o,
            invoked,
            completed: None,
            outcome: None,
        }
    }

    fn put(k: &str, v: &str) -> Op {
        Op::Put {
            key: k.into(),
            value: v.into(),
        }
    }

    fn get(k: &str) -> Op {
        Op::Get { key: k.into() }
    }

    fn val(v: Option<&str>) -> Outcome {
        Outcome::Value(v.map(str::to_string))
    }

    fn check(ops: Vec<Operation>) -> Verdict {
        let mut h = History::new();
        for o in ops {
            h.push(o);
        }
        h.well_formed().expect("test history should be well formed");
        Checker::new().check(&h)
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert_eq!(check(vec![]), Verdict::Linearizable);
    }

    #[test]
    fn a_sequential_history_is_linearizable() {
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(2, 0, 20, 30, get("k"), val(Some("a"))),
            op(3, 0, 40, 50, put("k", "b"), Outcome::Written),
            op(4, 0, 60, 70, get("k"), val(Some("b"))),
        ];
        assert_eq!(check(h), Verdict::Linearizable);
    }

    #[test]
    fn a_stale_read_is_caught() {
        // The read starts after the write returned, so it cannot see the old
        // value under any ordering.
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(2, 1, 20, 30, get("k"), val(None)),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn a_read_concurrent_with_a_write_may_see_either_value() {
        for observed in [None, Some("a")] {
            let h = vec![
                op(1, 0, 0, 100, put("k", "a"), Outcome::Written),
                op(2, 1, 10, 90, get("k"), val(observed)),
            ];
            assert_eq!(
                check(h),
                Verdict::Linearizable,
                "a concurrent read seeing {observed:?} is legal"
            );
        }
    }

    #[test]
    fn once_a_value_is_read_it_cannot_unread() {
        // Two reads after a concurrent write: seeing "a" then None is illegal,
        // because the write can only be linearized once.
        let h = vec![
            op(1, 0, 0, 100, put("k", "a"), Outcome::Written),
            op(2, 1, 10, 20, get("k"), val(Some("a"))),
            op(3, 1, 30, 40, get("k"), val(None)),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn keys_are_checked_independently() {
        // Illegal on k2 only; the checker must still find it.
        let h = vec![
            op(1, 0, 0, 10, put("k1", "a"), Outcome::Written),
            op(2, 0, 20, 30, get("k1"), val(Some("a"))),
            op(3, 0, 0, 10, put("k2", "b"), Outcome::Written),
            op(4, 0, 20, 30, get("k2"), val(None)),
        ];
        match check(h) {
            Verdict::Violation { key, .. } => assert_eq!(key, "k2"),
            other => panic!("expected a violation on k2, got {other:?}"),
        }
    }

    #[test]
    fn a_lost_update_through_cas_is_caught() {
        // Two clients each swap a -> their own value, both reporting success.
        // Only one can succeed under any linearization.
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(
                2,
                0,
                20,
                40,
                Op::Cas {
                    key: "k".into(),
                    expect: Some("a".into()),
                    value: "b".into(),
                },
                Outcome::Cas(true),
            ),
            op(
                3,
                1,
                20,
                40,
                Op::Cas {
                    key: "k".into(),
                    expect: Some("a".into()),
                    value: "c".into(),
                },
                Outcome::Cas(true),
            ),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn a_cas_that_loses_the_race_reports_failure() {
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(
                2,
                0,
                20,
                40,
                Op::Cas {
                    key: "k".into(),
                    expect: Some("a".into()),
                    value: "b".into(),
                },
                Outcome::Cas(true),
            ),
            op(
                3,
                1,
                20,
                40,
                Op::Cas {
                    key: "k".into(),
                    expect: Some("a".into()),
                    value: "c".into(),
                },
                Outcome::Cas(false),
            ),
            op(4, 0, 50, 60, get("k"), val(Some("b"))),
        ];
        assert_eq!(check(h), Verdict::Linearizable);
    }

    #[test]
    fn a_pending_write_may_have_happened() {
        // The client never learned the outcome; a later read sees the value.
        let h = vec![
            pending(1, 0, 0, put("k", "a")),
            op(2, 1, 50, 60, get("k"), val(Some("a"))),
        ];
        assert_eq!(check(h), Verdict::Linearizable);
    }

    #[test]
    fn a_pending_write_may_also_never_have_happened() {
        let h = vec![
            pending(1, 0, 0, put("k", "a")),
            op(2, 1, 50, 60, get("k"), val(None)),
        ];
        assert_eq!(check(h), Verdict::Linearizable);
    }

    #[test]
    fn a_pending_write_cannot_rescue_an_impossible_read() {
        // The read claims a value nobody ever wrote.
        let h = vec![
            pending(1, 0, 0, put("k", "a")),
            op(2, 1, 50, 60, get("k"), val(Some("z"))),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn a_pending_write_cannot_be_linearized_before_it_was_invoked() {
        // The read completes before the write is even sent.
        let h = vec![
            op(1, 1, 0, 10, get("k"), val(Some("a"))),
            pending(2, 0, 20, put("k", "a")),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn deletes_are_modelled() {
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(
                2,
                0,
                20,
                30,
                Op::Delete { key: "k".into() },
                Outcome::Written,
            ),
            op(3, 0, 40, 50, get("k"), val(None)),
        ];
        assert_eq!(check(h), Verdict::Linearizable);
    }

    #[test]
    fn a_wide_concurrent_history_still_finishes() {
        // Everything overlaps everything: the worst case for the search, and
        // the reason the memo table exists.
        let mut ops = vec![op(0, 0, 0, 1000, put("k", "v0"), Outcome::Written)];
        for i in 1..12u64 {
            ops.push(op(
                i,
                i as u32,
                1,
                999,
                put("k", &format!("v{i}")),
                Outcome::Written,
            ));
        }
        for i in 12..20u64 {
            ops.push(op(i, i as u32, 1, 999, get("k"), val(Some("v0"))));
        }
        let mut c = Checker::new();
        let mut h = History::new();
        for o in ops {
            h.push(o);
        }
        assert_eq!(c.check(&h), Verdict::Linearizable);
        assert!(c.stats.steps > 0);
    }

    #[test]
    fn real_time_order_is_enforced_across_clients() {
        // c0 writes and returns; c1 then writes and returns; a third client
        // reads the first value afterwards. Illegal.
        let h = vec![
            op(1, 0, 0, 10, put("k", "a"), Outcome::Written),
            op(2, 1, 20, 30, put("k", "b"), Outcome::Written),
            op(3, 2, 40, 50, get("k"), val(Some("a"))),
        ];
        assert!(check(h).is_violation());
    }

    #[test]
    fn budget_exhaustion_reports_unknown_not_failure() {
        let mut ops = vec![];
        for i in 0..24u64 {
            ops.push(op(
                i,
                i as u32,
                0,
                10_000,
                put("k", &format!("v{i}")),
                Outcome::Written,
            ));
            ops.push(op(100 + i, i as u32, 1, 9_999, get("k"), val(None)));
        }
        let mut h = History::new();
        for o in ops {
            h.push(o);
        }
        let mut c = Checker {
            budget: 500,
            ..Checker::new()
        };
        match c.check(&h) {
            Verdict::Unknown { .. } => {}
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn well_formed_catches_recording_mistakes() {
        let mut h = History::new();
        h.push(op(1, 0, 100, 50, get("k"), val(None)));
        assert!(h.well_formed().is_err(), "return before invoke");

        let mut h = History::new();
        h.push(op(1, 0, 0, 10, get("k"), val(None)));
        h.push(op(1, 0, 20, 30, get("k"), val(None)));
        assert!(h.well_formed().is_err(), "duplicate id");
    }
}
