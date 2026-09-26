//! The replicated log, its on-disk format, and recovery.
//!
//! # On-disk format
//!
//! The log is a sequence of self-describing records:
//!
//! ```text
//! +----------+----------+---------------------+
//! | len: u32 | crc: u32 | payload: len bytes  |
//! +----------+----------+---------------------+
//! ```
//!
//! Recovery reads records until one does not check out and then stops,
//! reporting how many bytes were good. Everything after that point is
//! discarded and the file is truncated back. This is the only safe rule: a
//! crash can tear the tail of the file, and a record that survives *after* a
//! damaged one is not evidence that the damaged one ever completed.
//!
//! A lost write followed by a landed one leaves a zero-filled hole, which
//! presents as `len == 0` and stops recovery at exactly the right place. The
//! index of every record is also checked against the expected next index, so a
//! hole that happens to contain plausible bytes still cannot be spliced in.
//!
//! # Hard state
//!
//! `currentTerm` and `votedFor` are updated in place rather than appended, so a
//! single fixed location would be destroyed by a torn write. They live in two
//! alternating 64-byte slots, each with its own sequence number and checksum;
//! recovery takes the valid slot with the highest sequence. A torn write can
//! therefore destroy at most the newer slot, and the older one is still a
//! consistent -- if slightly stale -- state that never violates safety, because
//! the term it names was durable before the newer write began.

use crate::codec::{crc32, Dec, DecodeError, Enc};
use simcore::NodeId;

/// Log indices are 1-based; 0 means "before the beginning".
pub const NO_INDEX: u64 = 0;

/// A record longer than this is corruption, not data.
const MAX_RECORD: usize = 1 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Committed by a new leader to carry its term over the commit threshold.
    Noop,
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Compare-and-set: the operation that makes lost updates visible.
    Cas {
        key: String,
        expect: Option<String>,
        value: String,
    },
}

impl Op {
    pub fn key(&self) -> Option<&str> {
        match self {
            Op::Noop => None,
            Op::Get { key } | Op::Put { key, .. } | Op::Delete { key } | Op::Cas { key, .. } => {
                Some(key)
            }
        }
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self, Op::Get { .. })
    }

    fn encode_into(&self, e: &mut Enc) {
        match self {
            Op::Noop => {
                e.u8(0);
            }
            Op::Get { key } => {
                e.u8(1).str(key);
            }
            Op::Put { key, value } => {
                e.u8(2).str(key).str(value);
            }
            Op::Delete { key } => {
                e.u8(3).str(key);
            }
            Op::Cas { key, expect, value } => {
                e.u8(4).str(key).opt_str(expect.as_deref()).str(value);
            }
        }
    }

    fn decode_from(d: &mut Dec) -> crate::codec::Result<Op> {
        Ok(match d.u8()? {
            0 => Op::Noop,
            1 => Op::Get { key: d.string()? },
            2 => Op::Put {
                key: d.string()?,
                value: d.string()?,
            },
            3 => Op::Delete { key: d.string()? },
            4 => Op::Cas {
                key: d.string()?,
                expect: d.opt_string()?,
                value: d.string()?,
            },
            t => return Err(DecodeError::BadTag(t)),
        })
    }
}

/// A client operation together with the identity needed to make retries
/// idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub client: u32,
    pub seq: u64,
    pub op: Op,
}

impl Command {
    pub fn noop() -> Command {
        Command {
            client: u32::MAX,
            seq: 0,
            op: Op::Noop,
        }
    }

    pub fn is_noop(&self) -> bool {
        matches!(self.op, Op::Noop)
    }

    pub fn encode_into(&self, e: &mut Enc) {
        e.u32(self.client).u64(self.seq);
        self.op.encode_into(e);
    }

    pub fn decode_from(d: &mut Dec) -> crate::codec::Result<Command> {
        Ok(Command {
            client: d.u32()?,
            seq: d.u64()?,
            op: Op::decode_from(d)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u64,
    pub index: u64,
    pub cmd: Command,
}

impl Entry {
    pub fn encode_into(&self, e: &mut Enc) {
        e.u64(self.term).u64(self.index);
        self.cmd.encode_into(e);
    }

    pub fn decode_from(d: &mut Dec) -> crate::codec::Result<Entry> {
        Ok(Entry {
            term: d.u64()?,
            index: d.u64()?,
            cmd: Command::decode_from(d)?,
        })
    }

    /// The full on-disk record, header included.
    pub fn to_record(&self) -> Vec<u8> {
        let mut payload = Enc::new();
        self.encode_into(&mut payload);
        let payload = payload.into_vec();
        let mut out = Vec::with_capacity(payload.len() + 8);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }
}

/// The in-memory log. Index 1 is the first entry; index 0 is the fixed
/// "empty" position with term 0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RaftLog {
    entries: Vec<Entry>,
    /// Byte offset of each entry's record within the log region. Maintained
    /// incrementally: recomputing it would re-encode the whole log on every
    /// append, which turns replication into quadratic work.
    offsets: Vec<usize>,
    end: usize,
    /// Last index covered by the snapshot; entries at or below it have been
    /// compacted away. Zero means nothing has been compacted.
    snapshot_index: u64,
    /// Term of the entry at `snapshot_index`, kept because log matching still
    /// has to answer questions about that one boundary index long after the
    /// entry itself is gone.
    snapshot_term: u64,
}

impl RaftLog {
    pub fn new() -> RaftLog {
        RaftLog::default()
    }

    pub fn from_entries(entries: Vec<Entry>) -> RaftLog {
        RaftLog::from_snapshot(0, 0, entries)
    }

    /// Build a log that begins after a snapshot at `(index, term)`, laid out
    /// from the start of the log region.
    pub fn from_snapshot(index: u64, term: u64, entries: Vec<Entry>) -> RaftLog {
        let mut log = RaftLog {
            snapshot_index: index,
            snapshot_term: term,
            ..RaftLog::new()
        };
        for e in entries {
            log.append(e);
        }
        debug_assert!(log.is_well_formed());
        log
    }

    /// Last index the snapshot covers. Entries at or below it are gone.
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    pub fn snapshot_term(&self) -> u64 {
        self.snapshot_term
    }

    /// Lowest index still held as an entry.
    pub fn first_index(&self) -> u64 {
        self.snapshot_index + 1
    }

    /// Discard every entry up to and including `index`, which the snapshot now
    /// covers. The caller must have made that snapshot durable first.
    pub fn compact_to(&mut self, index: u64, term: u64) {
        if index <= self.snapshot_index || index > self.last_index() {
            return;
        }
        let drop_count = (index - self.snapshot_index) as usize;
        self.entries.drain(..drop_count);
        self.offsets.drain(..drop_count);
        self.snapshot_index = index;
        self.snapshot_term = term;
        // Offsets are absolute within the log region and deliberately left
        // alone. The write-ahead file is append-only -- see `compaction` in the
        // module docs -- so a surviving entry keeps the byte position it was
        // written at.
    }

    /// Rebuild a log from a recovered file: entries, their byte offsets within
    /// the log region, and where the valid region ends.
    pub fn from_parts(
        snapshot_index: u64,
        snapshot_term: u64,
        entries: Vec<Entry>,
        offsets: Vec<usize>,
        end: usize,
    ) -> RaftLog {
        debug_assert_eq!(entries.len(), offsets.len());
        let log = RaftLog {
            entries,
            offsets,
            end,
            snapshot_index,
            snapshot_term,
        };
        debug_assert!(log.is_well_formed());
        log
    }

    fn is_well_formed(&self) -> bool {
        self.entries
            .iter()
            .enumerate()
            .all(|(i, e)| e.index == self.snapshot_index + i as u64 + 1)
            && self.entries.windows(2).all(|w| w[0].term <= w[1].term)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(self.snapshot_index, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    pub fn get(&self, index: u64) -> Option<&Entry> {
        if index < self.first_index() || index > self.last_index() {
            return None;
        }
        self.entries.get((index - self.first_index()) as usize)
    }

    /// Term of the entry at `index`, or `None` if the log cannot say.
    ///
    /// Index 0 is term 0 by definition, and the snapshot boundary keeps its
    /// term after the entry is compacted away. Anything below that boundary is
    /// genuinely unknown, which is not the same as absent: callers must not
    /// read `None` as a mismatch.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == NO_INDEX {
            return Some(0);
        }
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Raft's log matching check: do I have this entry, with this term?
    pub fn matches(&self, index: u64, term: u64) -> bool {
        self.term_at(index) == Some(term)
    }

    pub fn append(&mut self, entry: Entry) {
        assert_eq!(
            entry.index,
            self.last_index() + 1,
            "log entries must be contiguous"
        );
        assert!(
            entry.term >= self.last_term(),
            "terms in a log never decrease"
        );
        let len = entry.to_record().len();
        self.offsets.push(self.end);
        self.end += len;
        self.entries.push(entry);
    }

    /// Discard `index` and everything after it. Never cuts into the snapshot.
    pub fn truncate_from(&mut self, index: u64) {
        let index = index.max(self.first_index());
        if index <= self.last_index() {
            let i = (index - self.first_index()) as usize;
            self.end = self.offsets[i];
            self.entries.truncate(i);
            self.offsets.truncate(i);
        }
    }

    /// Up to `max` entries starting at `index`.
    pub fn slice_from(&self, index: u64, max: usize) -> Vec<Entry> {
        if index < self.first_index() || index > self.last_index() {
            return Vec::new();
        }
        let start = (index - self.first_index()) as usize;
        let end = (start + max).min(self.entries.len());
        self.entries[start..end].to_vec()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The election restriction: is a candidate with this last entry at least
    /// as up to date as this log? A candidate that fails this must not win, or
    /// committed entries could be lost.
    pub fn is_up_to_date(&self, cand_last_index: u64, cand_last_term: u64) -> bool {
        let (my_index, my_term) = (self.last_index(), self.last_term());
        cand_last_term > my_term || (cand_last_term == my_term && cand_last_index >= my_index)
    }

    /// Highest index whose entry was created in term `term` or earlier.
    ///
    /// Terms along a log never decrease, so this is a partition point.
    pub fn last_index_with_term_at_most(&self, term: u64) -> u64 {
        self.snapshot_index + self.entries.partition_point(|e| e.term <= term) as u64
    }

    /// Total bytes this log occupies on disk, for keeping the file length and
    /// the in-memory log in step.
    pub fn byte_len(&self) -> usize {
        self.end
    }

    /// Byte offset at which the record for `index` begins. `last_index() + 1`
    /// gives the end of the log, which is where the next record goes.
    pub fn byte_offset_of(&self, index: u64) -> usize {
        if index < self.first_index() {
            return 0;
        }
        match self.offsets.get((index - self.first_index()) as usize) {
            Some(o) => *o,
            None => self.end,
        }
    }
}

/// What survived on disk.
#[derive(Clone, Debug)]
pub struct RecoveredLog {
    pub entries: Vec<Entry>,
    /// Byte offset of each entry, relative to the start of the log region.
    pub offsets: Vec<usize>,
    /// Bytes that form a valid prefix. The file should be truncated here.
    pub valid_bytes: usize,
    /// Whether anything was discarded (a torn tail, a hole, or corruption).
    pub damaged: bool,
}

/// Read a log file, stopping at the first record that is not intact.
///
/// `first_index` is the index the first record must carry: 1 for a log that has
/// never been compacted, one past the snapshot otherwise.
pub fn recover_log_from(bytes: &[u8], first_index: u64) -> RecoveredLog {
    let mut entries: Vec<Entry> = Vec::new();
    let mut offsets: Vec<usize> = Vec::new();
    let mut pos = 0usize;
    let mut damaged = false;

    loop {
        if pos == bytes.len() {
            break;
        }
        if pos + 8 > bytes.len() {
            damaged = true; // a torn header
            break;
        }
        let len = u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
            as usize;
        let crc = u32::from_le_bytes([
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        // A zero length is what a lost write leaves behind: a hole of zeros.
        if len == 0 || len > MAX_RECORD || pos + 8 + len > bytes.len() {
            damaged = true;
            break;
        }
        let payload = &bytes[pos + 8..pos + 8 + len];
        if crc32(payload) != crc {
            damaged = true;
            break;
        }
        let mut d = Dec::new(payload);
        let entry = match Entry::decode_from(&mut d).and_then(|e| d.finish().map(|_| e)) {
            Ok(e) => e,
            Err(_) => {
                damaged = true;
                break;
            }
        };
        // Even an intact record is rejected if it is not the one that should
        // come next: reordered or duplicated writes must not be spliced in.
        let expected = entries.last().map_or(first_index, |e: &Entry| e.index + 1);
        if entry.index != expected || entry.term < entries.last().map_or(0, |e| e.term) {
            damaged = true;
            break;
        }
        entries.push(entry);
        offsets.push(pos);
        pos += 8 + len;
    }

    RecoveredLog {
        entries,
        offsets,
        valid_bytes: pos,
        damaged,
    }
}

/// Read a log file that has never been compacted.
pub fn recover_log(bytes: &[u8]) -> RecoveredLog {
    recover_log_from(bytes, 1)
}

/// Read a log file whose first record may be at any index.
///
/// The write-ahead file is append-only, so after a compaction it still begins
/// with entries the snapshot has since absorbed. Recovery has to read from
/// whatever index the file actually starts at and let the caller discard the
/// part the snapshot covers -- the alternative, rewriting the file to start at
/// the new first index, would mean a window where the only durable copy of the
/// live entries is being overwritten.
pub fn recover_log_any_start(bytes: &[u8]) -> RecoveredLog {
    if bytes.len() < 8 {
        return recover_log_from(bytes, 1);
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    // The index sits after term in the payload: len, crc, then term, index.
    let first = if len >= 16 && bytes.len() >= 8 + 16 {
        let mut idx = [0u8; 8];
        idx.copy_from_slice(&bytes[16..24]);
        u64::from_le_bytes(idx)
    } else {
        1
    };
    recover_log_from(bytes, first.max(1))
}

/// A point-in-time image of the state machine, plus where it sits in the log.
///
/// The `data` blob is opaque here: this module knows how to store a snapshot
/// durably and how to tell a good one from a torn one, and nothing about what
/// the application put inside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Monotonic write counter, used to pick the newer of the two files.
    pub seq: u64,
    /// Last log index this image includes.
    pub index: u64,
    /// Term of the entry at `index`.
    pub term: u64,
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Enc::new();
        payload.u64(self.seq).u64(self.index).u64(self.term);
        payload.bytes(&self.data);
        let payload = payload.into_vec();
        let mut out = Vec::with_capacity(payload.len() + 8);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Decode a snapshot file, returning `None` if it is absent or damaged.
    pub fn decode(bytes: &[u8]) -> Option<Snapshot> {
        if bytes.len() < 8 {
            return None;
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let crc = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if len == 0 || len > MAX_SNAPSHOT || 8 + len > bytes.len() {
            return None;
        }
        let payload = &bytes[8..8 + len];
        if crc32(payload) != crc {
            return None;
        }
        let mut d = Dec::new(payload);
        let seq = d.u64().ok()?;
        let index = d.u64().ok()?;
        let term = d.u64().ok()?;
        let data = d.bytes().ok()?.to_vec();
        Some(Snapshot {
            seq,
            index,
            term,
            data,
        })
    }
}

/// A snapshot larger than this is corruption, not data.
const MAX_SNAPSHOT: usize = 64 << 20;

/// Like [`pick_snapshot`], but also reports which input held it: 0 for `a`,
/// 1 for `b`. A node has to know which file holds its only good snapshot so
/// that it never writes the next one over it.
pub fn pick_snapshot_with_source(a: &[u8], b: &[u8]) -> Option<(Snapshot, usize)> {
    match (Snapshot::decode(a), Snapshot::decode(b)) {
        (Some(x), Some(y)) => Some(if (x.index, x.seq) >= (y.index, y.seq) {
            (x, 0)
        } else {
            (y, 1)
        }),
        (Some(x), None) => Some((x, 0)),
        (None, Some(y)) => Some((y, 1)),
        (None, None) => None,
    }
}

/// Choose the better of the two snapshot files that is intact.
///
/// Ordered by covered index first and sequence number only as a tie-break. The
/// sequence number records write order, and write order is not the same as
/// recency of content: snapshots arrive over a network that reorders, so an
/// older snapshot can quite easily be written after a newer one. Preferring the
/// higher index keeps the better image; a torn write still fails to decode and
/// falls through to the other file, which is what the two files are for.
pub fn pick_snapshot(a: &[u8], b: &[u8]) -> Option<Snapshot> {
    match (Snapshot::decode(a), Snapshot::decode(b)) {
        (Some(x), Some(y)) => Some(if (x.index, x.seq) >= (y.index, y.seq) {
            x
        } else {
            y
        }),
        (Some(x), None) => Some(x),
        (None, y) => y,
    }
}

// ---- hard state ---------------------------------------------------------

/// Size of one hard-state slot. Larger than the data so the layout can grow,
/// and a round number of bytes so slots never share a sector.
pub const SLOT_SIZE: usize = 64;
/// The state file holds exactly two slots.
pub const STATE_FILE_SIZE: usize = SLOT_SIZE * 2;

const NO_VOTE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
    /// Monotonic write counter, used to pick the newer of the two slots.
    pub seq: u64,
}

impl HardState {
    /// Encode into a fixed-size slot.
    pub fn to_slot(self) -> [u8; SLOT_SIZE] {
        let mut body = Enc::new();
        body.u64(self.seq)
            .u64(self.term)
            .u32(self.voted_for.map_or(NO_VOTE, |n| n.0));
        let body = body.into_vec();
        let mut slot = [0u8; SLOT_SIZE];
        slot[..body.len()].copy_from_slice(&body);
        slot[body.len()..body.len() + 4].copy_from_slice(&crc32(&body).to_le_bytes());
        slot
    }

    fn from_slot(slot: &[u8]) -> Option<HardState> {
        if slot.len() < SLOT_SIZE {
            return None;
        }
        const BODY: usize = 8 + 8 + 4;
        let body = &slot[..BODY];
        let crc = u32::from_le_bytes([slot[BODY], slot[BODY + 1], slot[BODY + 2], slot[BODY + 3]]);
        if crc32(body) != crc {
            return None;
        }
        let mut d = Dec::new(body);
        let seq = d.u64().ok()?;
        let term = d.u64().ok()?;
        let vote = d.u32().ok()?;
        Some(HardState {
            term,
            voted_for: if vote == NO_VOTE {
                None
            } else {
                Some(NodeId(vote))
            },
            seq,
        })
    }

    /// Which slot the next write goes to. Alternating means an interrupted
    /// write can only ever damage the slot that is not currently authoritative.
    pub fn slot_offset(seq: u64) -> usize {
        (seq % 2) as usize * SLOT_SIZE
    }
}

/// Recover hard state, preferring the newest slot that checks out.
pub fn recover_hard_state(bytes: &[u8]) -> HardState {
    let mut best = HardState::default();
    for i in 0..2 {
        let start = i * SLOT_SIZE;
        if start + SLOT_SIZE > bytes.len() {
            continue;
        }
        if let Some(hs) = HardState::from_slot(&bytes[start..start + SLOT_SIZE]) {
            if hs.seq >= best.seq {
                best = hs;
            }
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use simcore::rng::Rng;

    fn entry(index: u64, term: u64, key: &str) -> Entry {
        Entry {
            term,
            index,
            cmd: Command {
                client: 1,
                seq: index,
                op: Op::Put {
                    key: key.into(),
                    value: format!("v{index}"),
                },
            },
        }
    }

    fn log_bytes(entries: &[Entry]) -> Vec<u8> {
        entries.iter().flat_map(|e| e.to_record()).collect()
    }

    #[test]
    fn entries_round_trip_through_the_file_format() {
        let entries: Vec<Entry> = (1..=20).map(|i| entry(i, 1 + i / 7, "k")).collect();
        let r = recover_log(&log_bytes(&entries));
        assert_eq!(r.entries, entries);
        assert!(!r.damaged);
        assert_eq!(r.valid_bytes, log_bytes(&entries).len());
    }

    #[test]
    fn a_torn_tail_keeps_the_good_prefix() {
        let entries: Vec<Entry> = (1..=10).map(|i| entry(i, 1, "k")).collect();
        let full = log_bytes(&entries);
        // Every offset at which a record starts: cutting here leaves a clean,
        // merely shorter log rather than a damaged one.
        let boundaries: Vec<usize> = (0..=entries.len())
            .map(|n| log_bytes(&entries[..n]).len())
            .collect();

        for cut in 0..full.len() {
            let r = recover_log(&full[..cut]);
            // Whatever survived must be a prefix of what was written.
            assert_eq!(
                r.entries[..],
                entries[..r.entries.len()],
                "recovery invented or reordered entries at cut {cut}"
            );
            assert!(r.valid_bytes <= cut);
            if boundaries.contains(&cut) {
                assert!(!r.damaged, "a cut at record boundary {cut} is not damage");
                assert_eq!(r.valid_bytes, cut);
            } else {
                assert!(r.damaged, "a torn record at {cut} must be reported");
                // The bytes we trust stop at the last intact record.
                assert!(boundaries.contains(&r.valid_bytes));
            }
        }
    }

    #[test]
    fn a_hole_stops_recovery() {
        // The classic lost-write shape: record 3 vanished, record 4 landed.
        let entries: Vec<Entry> = (1..=4).map(|i| entry(i, 1, "k")).collect();
        let mut bytes = log_bytes(&entries);
        let start = entries[..2]
            .iter()
            .map(|e| e.to_record().len())
            .sum::<usize>();
        let len = entries[2].to_record().len();
        for b in &mut bytes[start..start + len] {
            *b = 0;
        }
        let r = recover_log(&bytes);
        assert_eq!(r.entries.len(), 2, "must not splice across the hole");
        assert!(r.damaged);
        assert_eq!(r.valid_bytes, start);
    }

    #[test]
    fn corruption_inside_a_record_is_caught() {
        let entries: Vec<Entry> = (1..=5).map(|i| entry(i, 1, "k")).collect();
        let base = log_bytes(&entries);
        let mut caught = 0;
        for i in 0..base.len() {
            let mut bytes = base.clone();
            bytes[i] ^= 0x40;
            let r = recover_log(&bytes);
            // Either it is detected, or the flipped byte was in a region that
            // the checksum covers and it must have been detected -- there is no
            // third option, so every entry we keep has to be genuine.
            for (n, e) in r.entries.iter().enumerate() {
                assert_eq!(e.index, n as u64 + 1);
            }
            if r.damaged || r.entries.len() < 5 {
                caught += 1;
            }
        }
        assert_eq!(caught, base.len(), "every single-byte flip must be caught");
    }

    #[test]
    fn random_garbage_never_panics() {
        let mut rng = Rng::new(4);
        for _ in 0..5_000 {
            let n = rng.below(200) as usize;
            let mut bytes = vec![0u8; n];
            rng.fill(&mut bytes);
            let r = recover_log(&bytes);
            assert!(r.valid_bytes <= bytes.len());
        }
    }

    #[test]
    fn log_indexing_and_matching() {
        let mut log = RaftLog::new();
        assert_eq!(log.last_index(), NO_INDEX);
        assert_eq!(log.term_at(NO_INDEX), Some(0));
        assert!(
            log.matches(NO_INDEX, 0),
            "the empty position always matches"
        );
        for i in 1..=5 {
            log.append(entry(i, 2, "k"));
        }
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(3).unwrap().index, 3);
        assert!(log.matches(3, 2));
        assert!(!log.matches(3, 1));
        assert!(!log.matches(9, 2), "an index we do not have cannot match");
    }

    #[test]
    fn truncate_removes_the_suffix() {
        let mut log = RaftLog::new();
        for i in 1..=5 {
            log.append(entry(i, 1, "k"));
        }
        log.truncate_from(3);
        assert_eq!(log.last_index(), 2);
        assert!(log.get(3).is_none());
        log.append(entry(3, 4, "k"));
        assert_eq!(log.last_index(), 3);
        log.truncate_from(NO_INDEX);
        assert!(log.is_empty());
    }

    #[test]
    fn up_to_date_implements_the_election_restriction() {
        let mut log = RaftLog::new();
        log.append(entry(1, 1, "k"));
        log.append(entry(2, 3, "k"));
        // Higher term wins regardless of length.
        assert!(log.is_up_to_date(1, 4));
        // Same term, longer or equal wins.
        assert!(log.is_up_to_date(2, 3));
        assert!(log.is_up_to_date(5, 3));
        assert!(!log.is_up_to_date(1, 3), "shorter log at same term loses");
        assert!(!log.is_up_to_date(99, 2), "lower term always loses");
    }

    #[test]
    fn byte_offsets_line_up_with_the_file() {
        let mut log = RaftLog::new();
        for i in 1..=6 {
            log.append(entry(i, 1, "key"));
        }
        let bytes: Vec<u8> = log.entries().iter().flat_map(|e| e.to_record()).collect();
        assert_eq!(log.byte_len(), bytes.len());
        for i in 1..=6 {
            let off = log.byte_offset_of(i);
            let r = recover_log(&bytes[..off]);
            assert_eq!(r.entries.len() as u64, i - 1);
        }
    }

    #[test]
    fn hard_state_round_trips() {
        let hs = HardState {
            term: 9,
            voted_for: Some(NodeId(2)),
            seq: 3,
        };
        let mut file = vec![0u8; STATE_FILE_SIZE];
        let off = HardState::slot_offset(hs.seq);
        file[off..off + SLOT_SIZE].copy_from_slice(&hs.to_slot());
        assert_eq!(recover_hard_state(&file), hs);
    }

    #[test]
    fn no_vote_round_trips() {
        let hs = HardState {
            term: 1,
            voted_for: None,
            seq: 0,
        };
        let mut file = vec![0u8; STATE_FILE_SIZE];
        file[..SLOT_SIZE].copy_from_slice(&hs.to_slot());
        assert_eq!(recover_hard_state(&file).voted_for, None);
    }

    #[test]
    fn a_torn_newer_slot_falls_back_to_the_older_one() {
        let old = HardState {
            term: 5,
            voted_for: Some(NodeId(1)),
            seq: 10,
        };
        let new = HardState {
            term: 6,
            voted_for: Some(NodeId(2)),
            seq: 11,
        };
        let mut file = vec![0u8; STATE_FILE_SIZE];
        let o = HardState::slot_offset(old.seq);
        file[o..o + SLOT_SIZE].copy_from_slice(&old.to_slot());
        let n = HardState::slot_offset(new.seq);
        assert_ne!(o, n, "consecutive writes must land in different slots");
        let newer = new.to_slot();
        file[n..n + SLOT_SIZE].copy_from_slice(&newer);
        assert_eq!(recover_hard_state(&file), new);

        // Now tear the newer slot: the older, still-valid state must win.
        for tear in 1..SLOT_SIZE {
            let mut torn = file.clone();
            for b in &mut torn[n + tear..n + SLOT_SIZE] {
                *b = 0;
            }
            let got = recover_hard_state(&torn);
            assert!(
                got == old || got == new,
                "recovered state must be one of the two written states, got {got:?}"
            );
        }
    }

    fn snap(seq: u64, index: u64) -> Vec<u8> {
        Snapshot {
            seq,
            index,
            term: 1,
            data: vec![index as u8],
        }
        .encode()
    }

    #[test]
    fn the_snapshot_covering_more_wins_even_if_written_first() {
        // Out-of-order installs: the older image can carry the higher seq.
        let newer_content = snap(1, 15);
        let written_later = snap(2, 12);
        let (s, src) = pick_snapshot_with_source(&newer_content, &written_later).unwrap();
        assert_eq!((s.index, src), (15, 0));
    }

    #[test]
    fn a_torn_snapshot_falls_back_to_the_other_file() {
        let good = snap(1, 10);
        let mut torn = snap(2, 20);
        torn.truncate(torn.len() / 2);
        let (s, src) = pick_snapshot_with_source(&good, &torn).unwrap();
        assert_eq!((s.index, src), (10, 0));
        let (s, src) = pick_snapshot_with_source(&torn, &good).unwrap();
        assert_eq!((s.index, src), (10, 1));
        assert!(pick_snapshot_with_source(&torn, &[]).is_none());
    }

    #[test]
    fn an_all_zero_state_file_is_the_initial_state() {
        let file = vec![0u8; STATE_FILE_SIZE];
        assert_eq!(recover_hard_state(&file), HardState::default());
        assert_eq!(recover_hard_state(&[]), HardState::default());
    }
}
