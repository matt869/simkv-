//! Durable storage, as seen by a node.
//!
//! The whole interface exists to make one distinction impossible to ignore:
//!
//! * a write *completing* means the bytes are in the page cache;
//! * a `sync` *completing* means the bytes will survive a power cut.
//!
//! Both are asynchronous. A node that replies to a peer before the relevant
//! sync has completed has broken Raft's persistence rule, and the crash model
//! in `simcore::disk` will eventually make it pay for that.

use simcore::disk::{FileId, IoResult, OpId};

/// The write-ahead file. Hard state and the log share one file on purpose:
/// a single `fsync` then orders the term that authorises an entry ahead of the
/// entry itself. Two files would need two syncs, and a crash between them can
/// leave a durable log entry whose term was never durable -- which is enough to
/// elect two leaders in one term.
pub const FILE_WAL: FileId = 0;

/// Snapshots alternate between two files.
///
/// A snapshot is written whole, so writing a new one over the old one leaves a
/// window where a crash has destroyed the only copy -- and by then the log
/// prefix it replaced may already be gone. Alternating means the previous
/// snapshot is still intact until the new one is safely down.
pub const FILE_SNAPSHOT_A: FileId = 1;
pub const FILE_SNAPSHOT_B: FileId = 2;

/// Which file the snapshot with this sequence number belongs in.
pub fn snapshot_file(seq: u64) -> FileId {
    if seq.is_multiple_of(2) {
        FILE_SNAPSHOT_A
    } else {
        FILE_SNAPSHOT_B
    }
}

pub trait Storage {
    /// Append to the end of the file. Returns the id of the operation whose
    /// completion event will report success or failure.
    fn append(&mut self, file: FileId, bytes: &[u8]) -> OpId;

    fn write_at(&mut self, file: FileId, offset: usize, bytes: &[u8]) -> OpId;

    /// Set the file length, discarding anything beyond it.
    fn set_len(&mut self, file: FileId, len: usize) -> OpId;

    /// Make everything written before this call durable, once the returned
    /// operation completes.
    fn sync(&mut self, file: FileId) -> OpId;

    /// Read the whole file. Synchronous, and only legitimate during recovery:
    /// a restarting node has nothing else to do while it reads its log.
    fn read_all(&self, file: FileId) -> Vec<u8>;

    fn size(&self, file: FileId) -> usize;
}

/// Tracks operations whose completion the caller is still waiting on, and what
/// to do when they finish.
///
/// A node that simply remembers "an fsync is in flight" cannot tell *which*
/// one completed when several overlap; this keeps the association explicit.
#[derive(Debug)]
pub struct PendingOps<T> {
    ops: Vec<(OpId, T)>,
}

impl<T> Default for PendingOps<T> {
    fn default() -> Self {
        PendingOps { ops: Vec::new() }
    }
}

impl<T> PendingOps<T> {
    pub fn new() -> Self {
        PendingOps::default()
    }

    pub fn insert(&mut self, op: OpId, value: T) {
        self.ops.push((op, value));
    }

    /// Take the action associated with a completed operation, if any.
    pub fn take(&mut self, op: OpId) -> Option<T> {
        let i = self.ops.iter().position(|(o, _)| *o == op)?;
        Some(self.ops.remove(i).1)
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Drop everything: used when a node restarts, since in-flight operations
    /// from the previous incarnation will never complete.
    pub fn clear(&mut self) {
        self.ops.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = &(OpId, T)> {
        self.ops.iter()
    }
}

/// Outcome of a storage completion, as handed to the application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Completion {
    pub op: OpId,
    pub result: IoResult,
}

impl Completion {
    pub fn ok(&self) -> bool {
        self.result.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_ops_match_by_id() {
        let mut p: PendingOps<&str> = PendingOps::new();
        p.insert(1, "vote");
        p.insert(2, "append");
        p.insert(3, "commit");
        assert_eq!(p.take(2), Some("append"));
        assert_eq!(p.take(2), None, "an op completes at most once");
        assert_eq!(p.len(), 2);
        assert_eq!(p.take(1), Some("vote"));
        assert_eq!(p.take(3), Some("commit"));
        assert!(p.is_empty());
    }

    #[test]
    fn overlapping_syncs_stay_distinguishable() {
        // Two syncs in flight at once must not be confused for each other.
        let mut p: PendingOps<u64> = PendingOps::new();
        p.insert(10, 100);
        p.insert(11, 200);
        assert_eq!(p.take(11), Some(200));
        assert_eq!(p.take(10), Some(100));
    }

    #[test]
    fn clear_drops_everything() {
        let mut p: PendingOps<u8> = PendingOps::new();
        p.insert(1, 1);
        p.clear();
        assert!(p.is_empty());
        assert_eq!(p.take(1), None);
    }
}
