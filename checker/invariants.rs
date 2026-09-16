//! Raft's safety properties, checked continuously against the real cluster
//! state.
//!
//! These are the five claims from the paper, plus the ones a running system
//! also has to honour:
//!
//! | property | what it forbids |
//! |---|---|
//! | election safety | two leaders in one term |
//! | leader append-only | a leader deleting or overwriting its own entries |
//! | log matching | the same (index, term) holding different commands |
//! | leader completeness | a new leader missing a committed entry |
//! | state machine safety | two nodes applying different commands at one index |
//! | term durability | a restarted node coming back with a lower term than it acted on |
//! | commit monotonicity | a running node's commit index going backwards |
//!
//! A checker that walked every log on every event would cost more than the
//! simulation, so each log is verified incrementally from a per-node watermark.
//! The watermark is reset whenever that node reports a truncation, which is the
//! only way already-verified entries can change.

use crate::Violation;
use kvstore::log::{Command, RaftLog};
use kvstore::raft::Role;
use simcore::{Nanos, NodeId};
use std::collections::BTreeMap;

/// A read-only look at one node, as the driver sees it.
pub struct NodeView<'a> {
    pub id: NodeId,
    pub up: bool,
    pub role: Role,
    pub term: u64,
    pub log: &'a RaftLog,
    pub commit_index: u64,
    pub last_applied: u64,
    pub durable_index: u64,
    /// Bumped on every restart, so volatile state resetting is not mistaken
    /// for state going backwards.
    pub incarnation: u64,
    /// Count of log truncations this node has performed.
    pub truncations: u64,
}

#[derive(Clone, Debug, Default)]
struct NodeTrack {
    checked_upto: u64,
    truncations: u64,
    incarnation: u64,
    commit_index: u64,
    acted_term: u64,
}

#[derive(Default)]
pub struct Invariants {
    /// term -> the node that was seen leading it.
    leader_by_term: BTreeMap<u64, NodeId>,
    /// (index, term) -> the command that (index, term) must always name.
    entries: BTreeMap<(u64, u64), Command>,
    /// index -> the entry every node must eventually agree on.
    committed: BTreeMap<u64, (u64, Command)>,
    /// Highest index anyone has reported committed.
    max_committed: u64,
    /// index -> what was applied to a state machine there.
    applied: BTreeMap<u64, (u64, Command)>,
    nodes: BTreeMap<NodeId, NodeTrack>,
    /// (node, term) -> longest log seen while it led that term.
    leader_log_len: BTreeMap<(NodeId, u64), u64>,
    checks: u64,
}

impl Invariants {
    pub fn new() -> Invariants {
        Invariants::default()
    }

    pub fn checks_run(&self) -> u64 {
        self.checks
    }

    /// Record that `from` sent a message carrying `term`.
    ///
    /// Every message a node sends follows an `fsync` of at least that term, so
    /// the node must never afterwards be seen at a lower term -- not even after
    /// a crash. This is the check that would catch a lost hard-state write.
    pub fn note_sent(&mut self, from: NodeId, term: u64) {
        let t = self.nodes.entry(from).or_default();
        t.acted_term = t.acted_term.max(term);
    }

    /// Check everything against the current state of the cluster.
    pub fn observe(&mut self, time: Nanos, views: &[NodeView]) -> Vec<Violation> {
        self.checks += 1;
        let mut out = Vec::new();
        for v in views {
            self.check_node(time, v, &mut out);
        }
        out
    }

    fn check_node(&mut self, time: Nanos, v: &NodeView, out: &mut Vec<Violation>) {
        let track = self.nodes.entry(v.id).or_default();
        let restarted = track.incarnation != v.incarnation;
        let truncated = track.truncations != v.truncations;
        track.incarnation = v.incarnation;
        if truncated {
            track.truncations = v.truncations;
            // Entries at or after the truncation point may have been replaced,
            // so everything has to be looked at again.
            track.checked_upto = 0;
        }
        let acted_term = track.acted_term;
        let prev_commit = track.commit_index;
        let checked_upto = track.checked_upto;

        if !v.up {
            // A down node's volatile state is meaningless; its disk is checked
            // by the durability checker instead.
            return;
        }

        // --- term durability -------------------------------------------
        if v.term < acted_term {
            out.push(Violation::new(
                "term_regression",
                time,
                Some(v.id),
                format!(
                    "node is at term {} but has already sent messages in term {acted_term}",
                    v.term
                ),
            ));
        }

        // --- structural sanity -----------------------------------------
        if v.commit_index > v.log.last_index() {
            out.push(Violation::new(
                "commit_beyond_log",
                time,
                Some(v.id),
                format!(
                    "commit index {} exceeds last log index {}",
                    v.commit_index,
                    v.log.last_index()
                ),
            ));
        }
        if v.last_applied > v.commit_index {
            out.push(Violation::new(
                "applied_beyond_commit",
                time,
                Some(v.id),
                format!(
                    "applied {} but only committed {}",
                    v.last_applied, v.commit_index
                ),
            ));
        }
        if v.durable_index > v.log.last_index() {
            out.push(Violation::new(
                "durable_beyond_log",
                time,
                Some(v.id),
                format!(
                    "claims {} durable with a log of {}",
                    v.durable_index,
                    v.log.last_index()
                ),
            ));
        }

        // --- commit monotonicity ---------------------------------------
        if !restarted && v.commit_index < prev_commit {
            out.push(Violation::new(
                "commit_regression",
                time,
                Some(v.id),
                format!("commit index went from {prev_commit} to {}", v.commit_index),
            ));
        }

        // --- election safety -------------------------------------------
        if v.role == Role::Leader {
            match self.leader_by_term.get(&v.term) {
                Some(other) if *other != v.id => out.push(Violation::new(
                    "election_safety",
                    time,
                    Some(v.id),
                    format!("term {} already had leader {other}", v.term),
                )),
                _ => {
                    self.leader_by_term.insert(v.term, v.id);
                }
            }

            // --- leader append-only ------------------------------------
            let key = (v.id, v.term);
            let seen = self.leader_log_len.entry(key).or_insert(0);
            if v.log.last_index() < *seen {
                out.push(Violation::new(
                    "leader_append_only",
                    time,
                    Some(v.id),
                    format!(
                        "leader log shrank from {seen} to {} in term {}",
                        v.log.last_index(),
                        v.term
                    ),
                ));
            }
            *seen = (*seen).max(v.log.last_index());

            // --- leader completeness -----------------------------------
            // A leader must hold every entry that was already committed when it
            // was elected. Checking against everything known committed is
            // stricter than needed only for entries committed in this instant,
            // and those are its own.
            for (index, (term, cmd)) in self.committed.range(..=self.max_committed) {
                if *index > v.log.last_index() {
                    out.push(Violation::new(
                        "leader_completeness",
                        time,
                        Some(v.id),
                        format!(
                            "leader of term {} is missing committed index {index} (log ends at {})",
                            v.term,
                            v.log.last_index()
                        ),
                    ));
                    break;
                }
                let entry = v.log.get(*index).expect("index is within the log");
                if entry.term != *term || entry.cmd != *cmd {
                    out.push(Violation::new(
                        "leader_completeness",
                        time,
                        Some(v.id),
                        format!(
                            "leader of term {} has a different entry at committed index {index}: \
                             term {} vs {term}",
                            v.term, entry.term
                        ),
                    ));
                    break;
                }
            }
        }

        // --- log matching ----------------------------------------------
        // Every (index, term) pair must name exactly one command, everywhere.
        let mut newly_checked = checked_upto;
        for entry in v.log.entries().iter().filter(|e| e.index > checked_upto) {
            match self.entries.get(&(entry.index, entry.term)) {
                Some(known) if *known != entry.cmd => {
                    out.push(Violation::new(
                        "log_matching",
                        time,
                        Some(v.id),
                        format!(
                            "index {} term {} holds two different commands: {:?} vs {:?}",
                            entry.index, entry.term, known, entry.cmd
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    self.entries
                        .insert((entry.index, entry.term), entry.cmd.clone());
                }
            }
            newly_checked = entry.index;
        }

        // --- committed entries are immutable ---------------------------
        for index in 1..=v.commit_index {
            let Some(entry) = v.log.get(index) else {
                break;
            };
            match self.committed.get(&index) {
                Some((term, cmd)) if *term != entry.term || *cmd != entry.cmd => {
                    out.push(Violation::new(
                        "committed_entry_changed",
                        time,
                        Some(v.id),
                        format!(
                            "committed index {index} was term {term} {cmd:?}, now term {} {:?}",
                            entry.term, entry.cmd
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    self.committed
                        .insert(index, (entry.term, entry.cmd.clone()));
                    self.max_committed = self.max_committed.max(index);
                }
            }
        }

        // --- state machine safety --------------------------------------
        for index in 1..=v.last_applied {
            let Some(entry) = v.log.get(index) else {
                break;
            };
            match self.applied.get(&index) {
                Some((term, cmd)) if *term != entry.term || *cmd != entry.cmd => {
                    out.push(Violation::new(
                        "state_machine_safety",
                        time,
                        Some(v.id),
                        format!(
                            "index {index} applied as term {term} {cmd:?} elsewhere, \
                             but as term {} {:?} here",
                            entry.term, entry.cmd
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    self.applied.insert(index, (entry.term, entry.cmd.clone()));
                }
            }
        }

        let track = self.nodes.entry(v.id).or_default();
        track.checked_upto = newly_checked;
        track.commit_index = v.commit_index;
    }

    /// Highest index any node has reported committed.
    pub fn max_committed(&self) -> u64 {
        self.max_committed
    }

    /// The entry that must live at `index` on every node, once committed.
    pub fn committed_entry(&self, index: u64) -> Option<&(u64, Command)> {
        self.committed.get(&index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvstore::log::{Entry, Op};

    fn cmd(client: u32, seq: u64) -> Command {
        Command {
            client,
            seq,
            op: Op::Put {
                key: "k".into(),
                value: format!("{client}:{seq}"),
            },
        }
    }

    fn log_of(entries: &[(u64, u64, u32)]) -> RaftLog {
        RaftLog::from_entries(
            entries
                .iter()
                .map(|(index, term, client)| Entry {
                    term: *term,
                    index: *index,
                    cmd: cmd(*client, *index),
                })
                .collect(),
        )
    }

    fn view<'a>(id: u32, role: Role, term: u64, log: &'a RaftLog, commit: u64) -> NodeView<'a> {
        NodeView {
            id: NodeId(id),
            up: true,
            role,
            term,
            log,
            commit_index: commit,
            last_applied: commit,
            durable_index: log.last_index(),
            incarnation: 0,
            truncations: 0,
        }
    }

    #[test]
    fn a_healthy_cluster_reports_nothing() {
        let log = log_of(&[(1, 1, 0), (2, 1, 0), (3, 2, 0)]);
        let mut inv = Invariants::new();
        for t in 0..10 {
            let views = vec![
                view(0, Role::Leader, 2, &log, 3),
                view(1, Role::Follower, 2, &log, 3),
                view(2, Role::Follower, 2, &log, 2),
            ];
            assert!(inv.observe(t, &views).is_empty());
        }
    }

    #[test]
    fn two_leaders_in_one_term_are_caught() {
        let log = log_of(&[(1, 5, 0)]);
        let mut inv = Invariants::new();
        let views = vec![view(0, Role::Leader, 5, &log, 0)];
        assert!(inv.observe(0, &views).is_empty());
        let views = vec![view(1, Role::Leader, 5, &log, 0)];
        let v = inv.observe(1, &views);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "election_safety");
    }

    #[test]
    fn leaders_in_different_terms_are_fine() {
        let log = log_of(&[(1, 5, 0)]);
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Leader, 5, &log, 0)]).is_empty());
        assert!(inv.observe(1, &[view(1, Role::Leader, 6, &log, 0)]).is_empty());
    }

    #[test]
    fn the_same_index_and_term_cannot_hold_two_commands() {
        let a = log_of(&[(1, 1, 0)]);
        let b = log_of(&[(1, 1, 9)]); // same index and term, different command
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Follower, 1, &a, 0)]).is_empty());
        let v = inv.observe(1, &[view(1, Role::Follower, 1, &b, 0)]);
        assert_eq!(v[0].kind, "log_matching");
    }

    #[test]
    fn rewriting_a_committed_entry_is_caught() {
        let a = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let b = log_of(&[(1, 1, 0), (2, 2, 0)]); // index 2 replaced
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Follower, 1, &a, 2)]).is_empty());
        let v = inv.observe(1, &[view(1, Role::Follower, 2, &b, 2)]);
        assert!(v.iter().any(|x| x.kind == "committed_entry_changed"));
    }

    #[test]
    fn a_leader_missing_a_committed_entry_is_caught() {
        let full = log_of(&[(1, 1, 0), (2, 1, 0), (3, 1, 0)]);
        let short = log_of(&[(1, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &full, 3)])
            .is_empty());
        let v = inv.observe(1, &[view(1, Role::Leader, 2, &short, 1)]);
        assert!(
            v.iter().any(|x| x.kind == "leader_completeness"),
            "got {v:?}"
        );
    }

    #[test]
    fn a_leader_log_may_not_shrink() {
        let long = log_of(&[(1, 3, 0), (2, 3, 0), (3, 3, 0)]);
        let short = log_of(&[(1, 3, 0)]);
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Leader, 3, &long, 0)]).is_empty());
        let v = inv.observe(1, &[view(0, Role::Leader, 3, &short, 0)]);
        assert!(v.iter().any(|x| x.kind == "leader_append_only"), "got {v:?}");
    }

    #[test]
    fn divergent_applied_entries_are_caught() {
        let a = log_of(&[(1, 1, 0)]);
        let b = log_of(&[(1, 2, 5)]);
        let mut inv = Invariants::new();
        let mut va = view(0, Role::Follower, 1, &a, 1);
        va.last_applied = 1;
        assert!(inv.observe(0, &[va]).is_empty());
        let mut vb = view(1, Role::Follower, 2, &b, 1);
        vb.last_applied = 1;
        let v = inv.observe(1, &[vb]);
        assert!(v.iter().any(|x| x.kind == "state_machine_safety"), "got {v:?}");
    }

    #[test]
    fn a_term_may_not_go_below_what_was_acted_on() {
        let log = log_of(&[(1, 4, 0)]);
        let mut inv = Invariants::new();
        inv.note_sent(NodeId(0), 7);
        let v = inv.observe(0, &[view(0, Role::Follower, 4, &log, 0)]);
        assert!(v.iter().any(|x| x.kind == "term_regression"), "got {v:?}");
        // Coming back at or above the acted term is fine.
        assert!(inv
            .observe(1, &[view(0, Role::Follower, 7, &log, 0)])
            .iter()
            .all(|x| x.kind != "term_regression"));
    }

    #[test]
    fn commit_may_not_go_backwards_within_an_incarnation() {
        let log = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Follower, 1, &log, 2)]).is_empty());
        let v = inv.observe(1, &[view(0, Role::Follower, 1, &log, 1)]);
        assert!(v.iter().any(|x| x.kind == "commit_regression"), "got {v:?}");
    }

    #[test]
    fn restarting_resets_volatile_state_without_complaint() {
        let log = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Follower, 1, &log, 2)]).is_empty());
        let mut v2 = view(0, Role::Follower, 1, &log, 0);
        v2.incarnation = 1;
        assert!(inv.observe(1, &[v2]).is_empty());
    }

    #[test]
    fn structural_impossibilities_are_caught() {
        let log = log_of(&[(1, 1, 0)]);
        let mut inv = Invariants::new();
        let mut v = view(0, Role::Follower, 1, &log, 5);
        v.last_applied = 9;
        v.durable_index = 4;
        let found = inv.observe(0, &[v]);
        let kinds: Vec<&str> = found.iter().map(|x| x.kind).collect();
        assert!(kinds.contains(&"commit_beyond_log"));
        assert!(kinds.contains(&"applied_beyond_commit"));
        assert!(kinds.contains(&"durable_beyond_log"));
    }

    #[test]
    fn a_truncation_forces_a_recheck() {
        let a = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv.observe(0, &[view(0, Role::Follower, 1, &a, 0)]).is_empty());
        // The node truncates and replaces index 2 with a conflicting command
        // at the same term -- only a recheck can see it.
        let b = log_of(&[(1, 1, 0), (2, 1, 7)]);
        let mut v = view(0, Role::Follower, 1, &b, 0);
        v.truncations = 1;
        let found = inv.observe(1, &[v]);
        assert!(found.iter().any(|x| x.kind == "log_matching"), "got {found:?}");
    }

    #[test]
    fn down_nodes_are_not_judged_on_volatile_state() {
        let log = log_of(&[(1, 1, 0)]);
        let mut inv = Invariants::new();
        inv.note_sent(NodeId(0), 9);
        let mut v = view(0, Role::Follower, 1, &log, 0);
        v.up = false;
        assert!(inv.observe(0, &[v]).is_empty());
    }
}
