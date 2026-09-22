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
    committed_checked: u64,
    applied_checked: u64,
    truncations: u64,
    incarnation: u64,
    commit_index: u64,
    acted_term: u64,
}

/// A committed entry, with the evidence for when it was committed.
#[derive(Clone, Debug)]
pub struct CommittedRec {
    pub term: u64,
    pub cmd: Command,
    /// The term of the first node observed reporting this index committed.
    ///
    /// Leader completeness only binds leaders of *later* terms: an entry
    /// committed in term 5 must appear in every leader from term 6 onwards. A
    /// node still calling itself leader of term 1 because it has been
    /// partitioned away has no obligation to hold it, and reporting one would
    /// be a false alarm on entirely correct behaviour.
    ///
    /// Recorded once and never lowered. Since the checker runs after every
    /// event, the first reporter is the committing leader or one of its
    /// followers; if it were ever later than the real committing term, the
    /// effect is a weaker check, never a spurious failure.
    pub committed_in_term: u64,
}

#[derive(Default)]
pub struct Invariants {
    /// term -> the node that was seen leading it.
    leader_by_term: BTreeMap<u64, NodeId>,
    /// (index, term) -> the command that (index, term) must always name.
    entries: BTreeMap<(u64, u64), Command>,
    /// index -> the entry every node must eventually agree on.
    committed: BTreeMap<u64, CommittedRec>,
    /// (node, term) -> how far leader completeness has been verified for that
    /// leadership, so the check stays incremental instead of rescanning every
    /// committed entry on every event.
    leader_verified: BTreeMap<(NodeId, u64), u64>,
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
        if truncated || restarted {
            track.truncations = v.truncations;
            // Entries at or after the truncation point may have been replaced,
            // and a restart may have lost an unsynced tail, so everything has
            // to be looked at again.
            track.checked_upto = 0;
            track.committed_checked = 0;
            track.applied_checked = 0;
        }
        let acted_term = track.acted_term;
        let prev_commit = track.commit_index;
        let checked_upto = track.checked_upto;
        let committed_from = track.committed_checked + 1;
        let applied_from = track.applied_checked + 1;

        if !v.up {
            // A down node's volatile state is meaningless; its disk is checked
            // by the durability checker instead.
            //
            // Forget the volatile state it had, because a crash genuinely
            // destroys it: commit index and applied index come back at zero,
            // and the log may have lost its unsynced tail, so everything has to
            // be looked at again once it returns. Remembering the pre-crash
            // values here would report every single restart as state going
            // backwards.
            track.commit_index = 0;
            track.checked_upto = 0;
            track.committed_checked = 0;
            track.applied_checked = 0;
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

        // --- the commit rule -------------------------------------------
        // Raft Figure 8: a leader may only advance its commit index onto an
        // entry from its *own* term. Counting replicas of an older entry can
        // commit something a later leader is still entitled to overwrite.
        // Entries below that point are committed implicitly and are fine; it is
        // the index the leader actually lands on that must be current-term.
        if v.role == Role::Leader && v.commit_index > prev_commit && !restarted {
            if let Some(t) = v.log.term_at(v.commit_index) {
                if t != v.term {
                    out.push(Violation::new(
                        "commit_of_foreign_term",
                        time,
                        Some(v.id),
                        format!(
                            "leader of term {} advanced commit to index {}, whose entry is from                              term {t}",
                            v.term, v.commit_index
                        ),
                    ));
                }
            }
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
            // An entry committed under a leader of term T must be present in
            // every leader from term T+1 onwards. Entries committed in this
            // node's own term or later are not its obligation: either it
            // committed them itself, or a later leader did and this one is
            // simply out of date and unable to commit anything anyway.
            let verified = self.leader_verified.entry((v.id, v.term)).or_insert(0);
            let from = *verified + 1;
            let mut checked_to = *verified;
            for (index, rec) in self.committed.range(from..) {
                if rec.committed_in_term >= v.term {
                    checked_to = *index;
                    continue;
                }
                if *index > v.log.last_index() {
                    out.push(Violation::new(
                        "leader_completeness",
                        time,
                        Some(v.id),
                        format!(
                            "leader of term {} is missing index {index}, committed in term {} \
                             (log ends at {})",
                            v.term,
                            rec.committed_in_term,
                            v.log.last_index()
                        ),
                    ));
                    break;
                }
                let entry = v.log.get(*index).expect("index is within the log");
                if entry.term != rec.term || entry.cmd != rec.cmd {
                    out.push(Violation::new(
                        "leader_completeness",
                        time,
                        Some(v.id),
                        format!(
                            "leader of term {} has a different entry at index {index}, \
                             committed in term {}: term {} vs {}",
                            v.term, rec.committed_in_term, entry.term, rec.term
                        ),
                    ));
                    break;
                }
                checked_to = *index;
            }
            self.leader_verified.insert((v.id, v.term), checked_to);
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
        // Resumed from a watermark: a full rescan on every event would make
        // checking cost more than simulating. The watermark is reset whenever
        // the node truncates or restarts, which are the only ways an entry
        // already looked at can change.
        let mut committed_checked = committed_from.saturating_sub(1);
        for index in committed_from..=v.commit_index {
            let Some(entry) = v.log.get(index) else {
                break;
            };
            match self.committed.get(&index) {
                Some(rec) if rec.term != entry.term || rec.cmd != entry.cmd => {
                    out.push(Violation::new(
                        "committed_entry_changed",
                        time,
                        Some(v.id),
                        format!(
                            "committed index {index} was term {} {:?}, now term {} {:?}",
                            rec.term, rec.cmd, entry.term, entry.cmd
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    self.committed.insert(
                        index,
                        CommittedRec {
                            term: entry.term,
                            cmd: entry.cmd.clone(),
                            committed_in_term: v.term,
                        },
                    );
                    self.max_committed = self.max_committed.max(index);
                }
            }
            committed_checked = index;
        }

        // --- state machine safety --------------------------------------
        let mut applied_checked = applied_from.saturating_sub(1);
        for index in applied_from..=v.last_applied {
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
            applied_checked = index;
        }

        let track = self.nodes.entry(v.id).or_default();
        track.checked_upto = newly_checked;
        track.committed_checked = committed_checked;
        track.applied_checked = applied_checked;
        track.commit_index = v.commit_index;
    }

    /// Highest index any node has reported committed.
    pub fn max_committed(&self) -> u64 {
        self.max_committed
    }

    /// The entry that must live at `index` on every node, once committed.
    pub fn committed_entry(&self, index: u64) -> Option<&CommittedRec> {
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
        assert!(inv
            .observe(0, &[view(0, Role::Leader, 5, &log, 0)])
            .is_empty());
        assert!(inv
            .observe(1, &[view(1, Role::Leader, 6, &log, 0)])
            .is_empty());
    }

    #[test]
    fn the_same_index_and_term_cannot_hold_two_commands() {
        let a = log_of(&[(1, 1, 0)]);
        let b = log_of(&[(1, 1, 9)]); // same index and term, different command
        let mut inv = Invariants::new();
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &a, 0)])
            .is_empty());
        let v = inv.observe(1, &[view(1, Role::Follower, 1, &b, 0)]);
        assert_eq!(v[0].kind, "log_matching");
    }

    #[test]
    fn rewriting_a_committed_entry_is_caught() {
        let a = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let b = log_of(&[(1, 1, 0), (2, 2, 0)]); // index 2 replaced
        let mut inv = Invariants::new();
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &a, 2)])
            .is_empty());
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
        assert!(inv
            .observe(0, &[view(0, Role::Leader, 3, &long, 0)])
            .is_empty());
        let v = inv.observe(1, &[view(0, Role::Leader, 3, &short, 0)]);
        assert!(
            v.iter().any(|x| x.kind == "leader_append_only"),
            "got {v:?}"
        );
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
        assert!(
            v.iter().any(|x| x.kind == "state_machine_safety"),
            "got {v:?}"
        );
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
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &log, 2)])
            .is_empty());
        let v = inv.observe(1, &[view(0, Role::Follower, 1, &log, 1)]);
        assert!(v.iter().any(|x| x.kind == "commit_regression"), "got {v:?}");
    }

    #[test]
    fn a_node_seen_down_may_come_back_at_zero() {
        // The common case: the checker observes the node while it is down, so
        // the incarnation bump is consumed before it returns. Coming back with
        // an empty commit index is a crash doing its job, not a regression.
        let log = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &log, 2)])
            .is_empty());

        let mut down = view(0, Role::Follower, 1, &log, 0);
        down.up = false;
        down.incarnation = 1;
        assert!(inv.observe(1, &[down]).is_empty());

        let mut back = view(0, Role::Follower, 1, &log, 0);
        back.incarnation = 1;
        let found = inv.observe(2, &[back]);
        assert!(
            found.iter().all(|x| x.kind != "commit_regression"),
            "a restart is not a commit regression: {found:?}"
        );
    }

    #[test]
    fn restarting_resets_volatile_state_without_complaint() {
        let log = log_of(&[(1, 1, 0), (2, 1, 0)]);
        let mut inv = Invariants::new();
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &log, 2)])
            .is_empty());
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
        assert!(inv
            .observe(0, &[view(0, Role::Follower, 1, &a, 0)])
            .is_empty());
        // The node truncates and replaces index 2 with a conflicting command
        // at the same term -- only a recheck can see it.
        let b = log_of(&[(1, 1, 0), (2, 1, 7)]);
        let mut v = view(0, Role::Follower, 1, &b, 0);
        v.truncations = 1;
        let found = inv.observe(1, &[v]);
        assert!(
            found.iter().any(|x| x.kind == "log_matching"),
            "got {found:?}"
        );
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
