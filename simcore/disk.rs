//! A simulated disk with an honest crash model.
//!
//! The single most important thing this file does is refuse to pretend that a
//! completed write is a durable write. Every file has two images:
//!
//! * `durable` -- what survives a crash: the content as of the last completed
//!   `fsync`, plus whatever the crash model decides to keep of the rest.
//! * `cache`   -- what a reader sees right now, i.e. the page cache.
//!
//! A write lands in `cache` and is remembered as *pending* until an `fsync`
//! that was issued after it completes. If the node crashes first, each pending
//! write is independently lost, torn (applied as a partial prefix), or applied
//! in full. Writes may also be reapplied out of order, which leaves zero-filled
//! holes -- exactly the shape of corruption a naive log reader mishandles.

use crate::rng::Rng;
use crate::Nanos;
use std::collections::BTreeMap;

pub type OpId = u64;

/// Files are identified by a small integer chosen by the application layer.
pub type FileId = u32;

/// The sector size the torn-write model rounds to. Real drives tear at sector
/// granularity most of the time; the model also tears mid-sector sometimes,
/// because assuming otherwise is how people write log readers that break.
pub const SECTOR: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoResult {
    Ok,
    /// The device rejected the operation. The data was not written.
    Error,
}

impl IoResult {
    pub fn is_ok(self) -> bool {
        self == IoResult::Ok
    }
}

#[derive(Clone, Debug)]
pub struct DiskConfig {
    pub write_latency: (Nanos, Nanos),
    pub sync_latency: (Nanos, Nanos),
    /// Probability a given operation fails outright.
    pub io_error_ppm: u32,
    /// Probability an unsynced write is torn rather than applied whole.
    pub torn_write_ppm: u32,
    /// Probability an unsynced write is lost entirely on crash.
    pub lost_write_ppm: u32,
    /// Whether unsynced writes may reach the platter out of order.
    pub reorder_unsynced: bool,
    /// Probability an operation hits a latency spike.
    pub slow_ppm: u32,
    pub slow_factor: u64,
}

impl Default for DiskConfig {
    fn default() -> Self {
        DiskConfig {
            write_latency: (20 * crate::MICROS, 500 * crate::MICROS),
            sync_latency: (200 * crate::MICROS, 8 * crate::MILLIS),
            io_error_ppm: 0,
            torn_write_ppm: 0,
            lost_write_ppm: 0,
            reorder_unsynced: false,
            slow_ppm: 0,
            slow_factor: 20,
        }
    }
}

impl DiskConfig {
    /// A disk that never lies and never fails: used to prove that a failing
    /// seed really does depend on storage faults.
    pub fn reliable() -> DiskConfig {
        DiskConfig::default()
    }

    pub fn hostile() -> DiskConfig {
        DiskConfig {
            io_error_ppm: 2_000,
            torn_write_ppm: 150_000,
            lost_write_ppm: 300_000,
            reorder_unsynced: true,
            slow_ppm: 10_000,
            ..DiskConfig::default()
        }
    }
}

#[derive(Clone, Debug)]
enum WriteKind {
    /// Overwrite (and zero-extend to reach) `offset..offset+len`.
    Data { offset: usize, bytes: Vec<u8> },
    /// Set the file length, zero-extending if it grows.
    SetLen(usize),
}

#[derive(Clone, Debug, Default)]
struct SimFile {
    durable: Vec<u8>,
    cache: Vec<u8>,
    /// Writes that have landed in the cache but are not yet covered by a
    /// completed sync, in the order they were issued.
    pending: Vec<WriteKind>,
}

fn apply(buf: &mut Vec<u8>, kind: &WriteKind) {
    match kind {
        WriteKind::Data { offset, bytes } => {
            let end = offset + bytes.len();
            if buf.len() < end {
                // A write past EOF leaves a zero-filled hole if an earlier
                // write was lost. That is the real behaviour, and the log
                // reader has to cope with it.
                buf.resize(end, 0);
            }
            buf[*offset..end].copy_from_slice(bytes);
        }
        WriteKind::SetLen(len) => buf.resize(*len, 0),
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DiskStats {
    pub writes: u64,
    pub syncs: u64,
    pub errors: u64,
    pub crashes: u64,
    pub bytes_written: u64,
    pub writes_lost: u64,
    pub writes_torn: u64,
}

/// The outcome of issuing an operation: it completes at `now + delay`, and
/// `result` is what the completion will carry.
#[derive(Clone, Copy, Debug)]
pub struct Issued {
    pub op: OpId,
    pub delay: Nanos,
    pub result: IoResult,
}

#[derive(Clone, Debug)]
pub struct Disk {
    cfg: DiskConfig,
    files: BTreeMap<FileId, SimFile>,
    next_op: OpId,
    stats: DiskStats,
}

impl Disk {
    pub fn new(cfg: DiskConfig) -> Disk {
        Disk {
            cfg,
            files: BTreeMap::new(),
            next_op: 1,
            stats: DiskStats::default(),
        }
    }

    pub fn config(&self) -> &DiskConfig {
        &self.cfg
    }

    pub fn stats(&self) -> DiskStats {
        self.stats
    }

    fn file(&mut self, id: FileId) -> &mut SimFile {
        self.files.entry(id).or_default()
    }

    fn latency(&self, rng: &mut Rng, base: (Nanos, Nanos)) -> Nanos {
        let d = rng.skewed(base.0, base.1);
        if rng.chance_ppm(self.cfg.slow_ppm) {
            d.saturating_mul(self.cfg.slow_factor)
        } else {
            d
        }
    }

    fn issue(&mut self, rng: &mut Rng, kind: WriteKind, file: FileId) -> Issued {
        let op = self.next_op;
        self.next_op += 1;
        let delay = self.latency(rng, self.cfg.write_latency);
        // Decide failure at issue time so the data never reaches the cache.
        if rng.chance_ppm(self.cfg.io_error_ppm) {
            self.stats.errors += 1;
            return Issued {
                op,
                delay,
                result: IoResult::Error,
            };
        }
        self.stats.writes += 1;
        if let WriteKind::Data { bytes, .. } = &kind {
            self.stats.bytes_written += bytes.len() as u64;
        }
        let f = self.file(file);
        apply(&mut f.cache, &kind);
        f.pending.push(kind);
        Issued {
            op,
            delay,
            result: IoResult::Ok,
        }
    }

    /// Overwrite bytes at `offset`, extending the file if needed.
    pub fn write_at(&mut self, rng: &mut Rng, file: FileId, offset: usize, bytes: &[u8]) -> Issued {
        let kind = WriteKind::Data {
            offset,
            bytes: bytes.to_vec(),
        };
        self.issue(rng, kind, file)
    }

    /// Append to the end of the file as it currently appears in cache.
    pub fn append(&mut self, rng: &mut Rng, file: FileId, bytes: &[u8]) -> Issued {
        let offset = self.file(file).cache.len();
        self.write_at(rng, file, offset, bytes)
    }

    /// Set the file length (used to discard a conflicting log suffix).
    pub fn set_len(&mut self, rng: &mut Rng, file: FileId, len: usize) -> Issued {
        self.issue(rng, WriteKind::SetLen(len), file)
    }

    /// Make everything written *before this call* durable once the returned
    /// operation completes. Writes issued afterwards are not covered, which is
    /// the guarantee a real `fsync` gives.
    pub fn sync(&mut self, rng: &mut Rng, file: FileId) -> Issued {
        let op = self.next_op;
        self.next_op += 1;
        let delay = self.latency(rng, self.cfg.sync_latency);
        if rng.chance_ppm(self.cfg.io_error_ppm) {
            self.stats.errors += 1;
            return Issued {
                op,
                delay,
                result: IoResult::Error,
            };
        }
        self.stats.syncs += 1;
        let f = self.file(file);
        // Snapshot which writes this sync covers. It takes effect immediately
        // in the model rather than at completion time: the ordering guarantee
        // is what matters, and the caller must not act until completion
        // regardless, because a crash before completion may still lose data.
        let covered: Vec<WriteKind> = std::mem::take(&mut f.pending);
        for w in &covered {
            apply(&mut f.durable, w);
        }
        Issued {
            op,
            delay,
            result: IoResult::Ok,
        }
    }

    /// Read the whole file as the running node sees it (page cache view).
    pub fn read_all(&self, file: FileId) -> Vec<u8> {
        self.files
            .get(&file)
            .map(|f| f.cache.clone())
            .unwrap_or_default()
    }

    pub fn len(&self, file: FileId) -> usize {
        self.files.get(&file).map_or(0, |f| f.cache.len())
    }

    /// What is guaranteed to survive a crash right now. For the durability
    /// checker only -- no simulated node may look at this.
    pub fn durable_image(&self, file: FileId) -> Vec<u8> {
        self.files
            .get(&file)
            .map(|f| f.durable.clone())
            .unwrap_or_default()
    }

    pub fn has_unsynced(&self, file: FileId) -> bool {
        self.files.get(&file).is_some_and(|f| !f.pending.is_empty())
    }

    /// Power loss. Pending writes are resolved by the fault model, the page
    /// cache evaporates, and the file is left in whatever state that produced.
    pub fn crash(&mut self, rng: &mut Rng) {
        self.stats.crashes += 1;
        let (torn_ppm, lost_ppm, reorder) = (
            self.cfg.torn_write_ppm,
            self.cfg.lost_write_ppm,
            self.cfg.reorder_unsynced,
        );
        for f in self.files.values_mut() {
            let mut pending = std::mem::take(&mut f.pending);
            if reorder {
                rng.shuffle(&mut pending);
            }
            for w in &pending {
                if rng.chance_ppm(lost_ppm) {
                    self.stats.writes_lost += 1;
                    continue;
                }
                match w {
                    WriteKind::Data { offset, bytes } if rng.chance_ppm(torn_ppm) => {
                        let keep = tear_length(rng, bytes.len());
                        if keep > 0 {
                            self.stats.writes_torn += 1;
                            apply(
                                &mut f.durable,
                                &WriteKind::Data {
                                    offset: *offset,
                                    bytes: bytes[..keep].to_vec(),
                                },
                            );
                        } else {
                            self.stats.writes_lost += 1;
                        }
                    }
                    kind => apply(&mut f.durable, kind),
                }
            }
            f.cache = f.durable.clone();
        }
    }
}

/// How much of a torn write actually landed. Usually a sector boundary,
/// sometimes an arbitrary byte offset.
fn tear_length(rng: &mut Rng, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    if len > SECTOR && rng.chance_ppm(800_000) {
        let sectors = len / SECTOR;
        rng.below(sectors as u64 + 1) as usize * SECTOR
    } else {
        rng.below(len as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(cfg: DiskConfig) -> (Disk, Rng) {
        (Disk::new(cfg), Rng::new(1))
    }

    #[test]
    fn synced_data_survives_a_crash() {
        let (mut d, mut r) = disk(DiskConfig::hostile());
        d.append(&mut r, 0, b"durable");
        d.sync(&mut r, 0);
        d.crash(&mut r);
        assert_eq!(d.read_all(0), b"durable");
    }

    #[test]
    fn unsynced_data_is_visible_but_not_durable() {
        let cfg = DiskConfig {
            lost_write_ppm: 1_000_000,
            ..DiskConfig::default()
        };
        let (mut d, mut r) = disk(cfg);
        d.append(&mut r, 0, b"ephemeral");
        assert_eq!(d.read_all(0), b"ephemeral", "page cache should show it");
        assert!(d.durable_image(0).is_empty(), "nothing is durable yet");
        d.crash(&mut r);
        assert!(d.read_all(0).is_empty(), "unsynced write must be lost");
    }

    #[test]
    fn sync_covers_only_earlier_writes() {
        let cfg = DiskConfig {
            lost_write_ppm: 1_000_000,
            ..DiskConfig::default()
        };
        let (mut d, mut r) = disk(cfg);
        d.append(&mut r, 0, b"before");
        d.sync(&mut r, 0);
        d.append(&mut r, 0, b"after");
        d.crash(&mut r);
        assert_eq!(d.read_all(0), b"before");
    }

    #[test]
    fn torn_write_leaves_a_prefix() {
        let cfg = DiskConfig {
            torn_write_ppm: 1_000_000,
            lost_write_ppm: 0,
            ..DiskConfig::default()
        };
        let (mut d, mut r) = disk(cfg);
        let payload = vec![7u8; 4096];
        d.append(&mut r, 0, &payload);
        d.crash(&mut r);
        let after = d.read_all(0);
        assert!(after.len() < payload.len(), "write should have been torn");
        assert!(after.iter().all(|b| *b == 7), "prefix must be intact");
    }

    #[test]
    fn lost_write_before_a_later_one_leaves_a_hole() {
        // Two appends, the first lost: the second still lands at its offset,
        // so the file has a zero-filled gap.
        let cfg = DiskConfig {
            lost_write_ppm: 500_000,
            reorder_unsynced: true,
            ..DiskConfig::default()
        };
        let mut found_hole = false;
        for seed in 0..200 {
            let mut d = Disk::new(cfg.clone());
            let mut r = Rng::new(seed);
            d.append(&mut r, 0, b"AAAA");
            d.append(&mut r, 0, b"BBBB");
            d.crash(&mut r);
            let img = d.read_all(0);
            if img.len() == 8 && &img[0..4] == b"\0\0\0\0" {
                found_hole = true;
                break;
            }
        }
        assert!(found_hole, "expected a lost-write hole in 200 attempts");
    }

    #[test]
    fn io_errors_do_not_modify_the_file() {
        let cfg = DiskConfig {
            io_error_ppm: 1_000_000,
            ..DiskConfig::default()
        };
        let (mut d, mut r) = disk(cfg);
        let issued = d.append(&mut r, 0, b"nope");
        assert_eq!(issued.result, IoResult::Error);
        assert!(d.read_all(0).is_empty());
    }

    #[test]
    fn set_len_truncates_and_zero_extends() {
        let (mut d, mut r) = disk(DiskConfig::default());
        d.append(&mut r, 0, b"0123456789");
        d.set_len(&mut r, 0, 4);
        assert_eq!(d.read_all(0), b"0123");
        d.set_len(&mut r, 0, 6);
        assert_eq!(d.read_all(0), b"0123\0\0");
    }

    #[test]
    fn crash_is_deterministic_for_a_seed() {
        let outcome = |seed| {
            let mut d = Disk::new(DiskConfig::hostile());
            let mut r = Rng::new(seed);
            for i in 0..50u8 {
                d.append(&mut r, 0, &[i; 300]);
                if i % 10 == 0 {
                    d.sync(&mut r, 0);
                }
            }
            d.crash(&mut r);
            d.read_all(0)
        };
        assert_eq!(outcome(99), outcome(99));
    }
}
