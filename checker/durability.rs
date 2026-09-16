//! What the disks actually say.
//!
//! Every other check in this crate reads the cluster's *opinion* of itself: a
//! node's in-memory log, its commit index, what it says it made durable. This
//! one reads the bytes that would survive a power cut, decodes them with the
//! same recovery rules a restarting node uses, and asks whether the cluster's
//! claims are backed by them.
//!
//! The central claim is this: **a committed entry is on stable storage on a
//! majority of nodes.** If that ever fails to hold, the cluster has told a
//! client that something is durable while a simultaneous power cut across a
//! majority would lose it -- which is precisely the failure mode consensus is
//! supposed to eliminate. An implementation that acknowledged before `fsync`
//! would sail past every in-memory invariant and fail here.

use crate::Violation;
use kvstore::log::{recover_hard_state, recover_log, Command, Entry};
use kvstore::raft::LOG_REGION;
use simcore::{Nanos, NodeId};

/// The durable image of one node's write-ahead file.
pub struct DiskView {
    pub id: NodeId,
    pub bytes: Vec<u8>,
}

/// What a node would recover if it restarted right now.
#[derive(Clone, Debug, Default)]
pub struct Recovered {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    pub entries: Vec<Entry>,
}

impl Recovered {
    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }

    pub fn get(&self, index: u64) -> Option<&Entry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }
}

/// Decode a durable image exactly as [`kvstore::raft::Raft::recover`] would,
/// including dropping entries whose term was never made durable.
pub fn recover(bytes: &[u8]) -> Recovered {
    let hs = recover_hard_state(bytes);
    let log_bytes = if bytes.len() > LOG_REGION {
        &bytes[LOG_REGION..]
    } else {
        &[][..]
    };
    let mut entries = recover_log(log_bytes).entries;
    entries.retain(|e| e.term <= hs.term);
    Recovered {
        term: hs.term,
        voted_for: hs.voted_for,
        entries,
    }
}

/// One committed entry, as the cluster believes it.
pub struct CommittedEntry {
    pub index: u64,
    pub term: u64,
    pub cmd: Command,
}

/// Check that everything the cluster considers committed is durable on a
/// majority, and that no node's durable log contradicts a committed entry.
pub fn check_committed_durable(
    time: Nanos,
    committed: &[CommittedEntry],
    disks: &[DiskView],
    cluster_size: usize,
) -> Vec<Violation> {
    let mut out = Vec::new();
    let quorum = cluster_size / 2 + 1;
    let recovered: Vec<(NodeId, Recovered)> =
        disks.iter().map(|d| (d.id, recover(&d.bytes))).collect();

    for c in committed {
        // Only an exact match counts. A node whose disk holds something *else*
        // at this index is not a holder -- but it is not a violation on its own
        // either: a lost truncation can leave a stale uncommitted entry behind,
        // and the leader will overwrite it again on recovery. What matters is
        // whether enough nodes hold the real thing, which is what is checked
        // below.
        let holders = recovered
            .iter()
            .filter(|(_, r)| r.get(c.index).is_some_and(|e| e.term == c.term && e.cmd == c.cmd))
            .count();
        if holders < quorum {
            out.push(Violation::new(
                "committed_not_durable",
                time,
                None,
                format!(
                    "committed index {} (term {}) is on stable storage on only {holders} of {} \
                     nodes; a quorum is {quorum}",
                    c.index, c.term, cluster_size
                ),
            ));
            // One report is enough; the rest of the log will say the same.
            break;
        }
    }
    out
}

/// Check that a restarted node still has everything it had made durable.
///
/// The right yardstick is the node's own `durable_index` at the moment it
/// crashed: every entry up to there was covered by a completed `fsync`, so it
/// must come back unchanged. That is a real guarantee, and losing one of those
/// entries is a genuine data-loss bug.
///
/// Beyond that point, anything is legal. The unsynced tail may be lost; a
/// truncation whose `set_len` never reached the platter may resurrect entries
/// the node had already discarded, leaving it with a *longer* log than it had
/// in memory. That looks alarming and is harmless: those entries were never
/// committed (a truncation only ever removes uncommitted entries), their terms
/// are older than whatever replaced them, so the election restriction stops
/// them winning anything, and the current leader will overwrite them again.
pub fn check_recovery(
    time: Nanos,
    node: NodeId,
    before: &[Entry],
    durable_index: u64,
    after: &[Entry],
) -> Vec<Violation> {
    let mut out = Vec::new();
    let promised = durable_index.min(before.len() as u64);
    if (after.len() as u64) < promised {
        out.push(Violation::new(
            "durable_entry_lost",
            time,
            Some(node),
            format!(
                "recovered only {} entries, but {promised} had been synced before the crash",
                after.len()
            ),
        ));
        return out;
    }
    for i in 0..promised as usize {
        if after[i] != before[i] {
            out.push(Violation::new(
                "durable_entry_changed",
                time,
                Some(node),
                format!(
                    "index {} was synced as term {} {:?} but recovered as term {} {:?}",
                    before[i].index, before[i].term, before[i].cmd, after[i].term, after[i].cmd
                ),
            ));
            break;
        }
    }
    out
}

/// Check a live node's durability claim against its actual disk.
///
/// `durable_index` is the node's own belief about how much of its log is on
/// stable storage, and it is the number it reports to its leader -- so commits
/// are built on it. This decodes the bytes that would survive a power cut right
/// now and checks that the belief is backed by them.
///
/// This is the check that catches a persistence discipline that is subtly out
/// of order: acknowledging before the `fsync` completes, letting a later write
/// overwrite a synced region, or raising the claim after a truncation has
/// invalidated it.
pub fn check_durable_claim(
    time: Nanos,
    node: NodeId,
    log: &[Entry],
    durable_index: u64,
    disk: &DiskView,
) -> Vec<Violation> {
    if durable_index == 0 {
        return Vec::new();
    }
    let r = recover(&disk.bytes);
    if r.last_index() < durable_index {
        return vec![Violation::new(
            "durable_claim_unbacked",
            time,
            Some(node),
            format!(
                "claims {durable_index} entries are durable, but the disk would recover only {}",
                r.last_index()
            ),
        )];
    }
    for i in 0..(durable_index as usize).min(log.len()) {
        let on_disk = &r.entries[i];
        if *on_disk != log[i] {
            return vec![Violation::new(
                "durable_claim_unbacked",
                time,
                Some(node),
                format!(
                    "index {} is term {} {:?} in memory but term {} {:?} on disk, \
                     inside the range claimed durable ({durable_index})",
                    log[i].index, log[i].term, log[i].cmd, on_disk.term, on_disk.cmd
                ),
            )];
        }
    }
    Vec::new()
}

/// Check that a node's durable term never goes backwards.
pub fn check_term_durable(
    time: Nanos,
    node: NodeId,
    acted_term: u64,
    disk: &DiskView,
) -> Vec<Violation> {
    let r = recover(&disk.bytes);
    if r.term < acted_term {
        return vec![Violation::new(
            "durable_term_lost",
            time,
            Some(node),
            format!(
                "disk holds term {} but this node has already sent messages in term {acted_term}",
                r.term
            ),
        )];
    }
    Vec::new()
}

/// After the cluster has been left alone to settle, every node that has applied
/// the same number of entries must hold the same data.
pub fn check_convergence(
    time: Nanos,
    states: &[(NodeId, u64, &std::collections::BTreeMap<String, String>)],
) -> Vec<Violation> {
    let mut out = Vec::new();
    let Some((first_id, first_applied, first_state)) = states.first() else {
        return out;
    };
    for (id, applied, state) in states.iter().skip(1) {
        if applied != first_applied {
            continue; // still catching up; not a disagreement
        }
        if state != first_state {
            let differing: Vec<String> = first_state
                .iter()
                .filter(|(k, v)| state.get(*k) != Some(*v))
                .map(|(k, v)| format!("{k}={v:?} vs {:?}", state.get(k)))
                .take(4)
                .collect();
            out.push(Violation::new(
                "replicas_diverged",
                time,
                Some(*id),
                format!(
                    "after applying {applied} entries, {id} disagrees with {first_id}: {}",
                    differing.join(", ")
                ),
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvstore::log::{HardState, Op, SLOT_SIZE};
    use std::collections::BTreeMap;

    fn cmd(seq: u64) -> Command {
        Command {
            client: 1,
            seq,
            op: Op::Put {
                key: "k".into(),
                value: format!("v{seq}"),
            },
        }
    }

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            term,
            index,
            cmd: cmd(index),
        }
    }

    /// Build a durable image the way a node would write one.
    fn image(term: u64, entries: &[Entry]) -> Vec<u8> {
        let hs = HardState {
            term,
            voted_for: None,
            seq: 1,
        };
        let mut bytes = vec![0u8; LOG_REGION];
        let off = HardState::slot_offset(hs.seq);
        bytes[off..off + SLOT_SIZE].copy_from_slice(&hs.to_slot());
        for e in entries {
            bytes.extend_from_slice(&e.to_record());
        }
        bytes
    }

    fn disk(id: u32, term: u64, entries: &[Entry]) -> DiskView {
        DiskView {
            id: NodeId(id),
            bytes: image(term, entries),
        }
    }

    fn committed(entries: &[Entry]) -> Vec<CommittedEntry> {
        entries
            .iter()
            .map(|e| CommittedEntry {
                index: e.index,
                term: e.term,
                cmd: e.cmd.clone(),
            })
            .collect()
    }

    #[test]
    fn recovery_round_trips_a_clean_image() {
        let entries = vec![entry(1, 1), entry(2, 1), entry(3, 2)];
        let r = recover(&image(2, &entries));
        assert_eq!(r.term, 2);
        assert_eq!(r.entries, entries);
        assert_eq!(r.last_index(), 3);
    }

    #[test]
    fn recovery_drops_entries_ahead_of_the_durable_term() {
        // Term 3 entries survived on disk but the term update did not: they
        // were never acknowledged, so they must be discarded.
        let entries = vec![entry(1, 1), entry(2, 3)];
        let r = recover(&image(1, &entries));
        assert_eq!(r.entries, vec![entry(1, 1)]);
    }

    #[test]
    fn an_empty_disk_recovers_to_nothing() {
        let r = recover(&[]);
        assert_eq!(r.term, 0);
        assert!(r.entries.is_empty());
        assert_eq!(r.get(1), None);
    }

    #[test]
    fn a_quorum_holding_the_entry_passes() {
        let entries = vec![entry(1, 1), entry(2, 1)];
        let disks = vec![
            disk(0, 1, &entries),
            disk(1, 1, &entries),
            disk(2, 1, &entries[..1]),
        ];
        assert!(check_committed_durable(0, &committed(&entries), &disks, 3).is_empty());
    }

    #[test]
    fn a_committed_entry_on_a_minority_is_a_violation() {
        let entries = vec![entry(1, 1), entry(2, 1)];
        let disks = vec![
            disk(0, 1, &entries),
            disk(1, 1, &entries[..1]),
            disk(2, 1, &entries[..1]),
        ];
        let v = check_committed_durable(0, &committed(&entries), &disks, 3);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, "committed_not_durable");
        assert!(v[0].detail.contains("only 1 of 3"));
    }

    #[test]
    fn a_disagreeing_disk_does_not_count_towards_the_quorum() {
        let good = vec![entry(1, 1), entry(2, 1)];
        let mut bad = good.clone();
        bad[1] = Entry {
            term: 2,
            index: 2,
            cmd: cmd(99),
        };
        // One node disagrees at index 2, but the other two hold it: safe.
        let disks = vec![disk(0, 2, &good), disk(1, 2, &good), disk(2, 2, &bad)];
        assert!(check_committed_durable(0, &committed(&good), &disks, 3).is_empty());

        // Two disagree, so the committed entry is no longer on a majority.
        let disks = vec![disk(0, 2, &good), disk(1, 2, &bad), disk(2, 2, &bad)];
        let v = check_committed_durable(0, &committed(&good), &disks, 3);
        assert_eq!(v[0].kind, "committed_not_durable");
    }

    #[test]
    fn losing_the_unsynced_tail_is_legal_recovery() {
        let before = vec![entry(1, 1), entry(2, 1), entry(3, 1)];
        // Only the first entry was synced, so only it has to come back.
        assert!(check_recovery(0, NodeId(0), &before, 1, &before[..1]).is_empty());
    }

    #[test]
    fn losing_a_synced_entry_is_caught() {
        let before = vec![entry(1, 1), entry(2, 1), entry(3, 1)];
        let v = check_recovery(0, NodeId(0), &before, 3, &before[..2]);
        assert_eq!(v[0].kind, "durable_entry_lost");
    }

    #[test]
    fn changing_a_synced_entry_is_caught() {
        let before = vec![entry(1, 1), entry(2, 1)];
        let after = vec![entry(1, 1), Entry { term: 5, index: 2, cmd: cmd(2) }];
        let v = check_recovery(0, NodeId(0), &before, 2, &after);
        assert_eq!(v[0].kind, "durable_entry_changed");
    }

    #[test]
    fn a_lost_truncation_may_resurrect_uncommitted_entries() {
        // The node truncated to one entry in memory, but the set_len never
        // reached the platter, so the old tail came back. Unsynced, uncommitted
        // and harmless: not a violation.
        let before = vec![entry(1, 1)];
        let after = vec![entry(1, 1), entry(2, 1), entry(3, 1)];
        assert!(check_recovery(0, NodeId(0), &before, 1, &after).is_empty());
    }

    #[test]
    fn a_lost_durable_term_is_caught() {
        let d = disk(0, 3, &[]);
        assert!(check_term_durable(0, NodeId(0), 3, &d).is_empty());
        let v = check_term_durable(0, NodeId(0), 4, &d);
        assert_eq!(v[0].kind, "durable_term_lost");
    }

    #[test]
    fn convergence_compares_only_equally_advanced_replicas() {
        let a: BTreeMap<String, String> = [("k".to_string(), "1".to_string())].into();
        let b: BTreeMap<String, String> = [("k".to_string(), "2".to_string())].into();
        // Different applied counts: one is simply behind.
        assert!(check_convergence(0, &[(NodeId(0), 5, &a), (NodeId(1), 3, &b)]).is_empty());
        // Same applied count, different data: a real divergence.
        let v = check_convergence(0, &[(NodeId(0), 5, &a), (NodeId(1), 5, &b)]);
        assert_eq!(v[0].kind, "replicas_diverged");
        // Agreement passes.
        assert!(check_convergence(0, &[(NodeId(0), 5, &a), (NodeId(1), 5, &a)]).is_empty());
    }
}
