//! `simcore` -- the deterministic world a distributed system runs inside.
//!
//! The simulator is a single thread, a virtual clock, and one seeded random
//! source. Nodes never touch the OS: they send messages through [`net`], write
//! to [`disk`], and set timers on the [`scheduler`]. Faults come from [`faults`].
//! Given a seed, the entire run -- every message order, every latency, every
//! torn write -- is reproducible on any machine, which is what turns a rare
//! concurrency bug into a unit test.
//!
//! This crate knows nothing about the application under test. It moves bytes,
//! loses bytes, and lies about durability; interpreting those bytes is
//! somebody else's job.

pub mod determinism;
pub mod disk;
pub mod faults;
pub mod net;
pub mod rng;
pub mod scheduler;
pub mod trace;

use disk::{Disk, DiskConfig, DiskStats, FileId, IoResult, OpId};
use faults::{ClockSkew, FaultConfig, FaultInjector};
use net::{NetConfig, NetStats, Network};
use rng::Rng;
use scheduler::{Event, EventId, Scheduler};
use trace::{Fingerprint, Level, Trace};

/// Virtual time, in nanoseconds since the start of the run.
pub type Nanos = u64;

pub const MICROS: Nanos = 1_000;
pub const MILLIS: Nanos = 1_000_000;
pub const SECONDS: Nanos = 1_000_000_000;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeId(pub u32);

impl NodeId {
    #[inline]
    pub fn idx(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Everything needed to construct a run.
#[derive(Clone, Debug)]
pub struct WorldConfig {
    pub nodes: usize,
    pub seed: u64,
    pub net: NetConfig,
    pub disk: DiskConfig,
    pub faults: FaultConfig,
    pub trace_level: Level,
}

impl Default for WorldConfig {
    fn default() -> Self {
        WorldConfig {
            nodes: 3,
            seed: 0,
            net: NetConfig::default(),
            disk: DiskConfig::default(),
            faults: FaultConfig::default(),
            trace_level: Level::Off,
        }
    }
}

/// The simulated world: time, randomness, network, disks, faults, trace.
///
/// Nodes are deliberately *not* stored here. The driver owns them, so a node
/// handler can borrow itself mutably and the world mutably at the same time.
pub struct World {
    pub sched: Scheduler,
    pub rng: Rng,
    pub net: Network,
    pub trace: Trace,
    pub faults: FaultInjector,
    disks: Vec<Disk>,
    skews: Vec<ClockSkew>,
    up: Vec<bool>,
    /// Incremented every time a node restarts. Late completions addressed to
    /// an older incarnation are discarded.
    incarnation: Vec<u64>,
    seed: u64,
}

impl World {
    pub fn new(cfg: WorldConfig) -> World {
        let mut rng = Rng::new(cfg.seed);
        let n = cfg.nodes;
        let skews = (0..n)
            .map(|_| {
                if cfg.faults.enable_clock_skew {
                    ClockSkew::random(&mut rng, cfg.faults.max_clock_skew)
                } else {
                    ClockSkew::default()
                }
            })
            .collect();
        World {
            sched: Scheduler::new(),
            net: Network::new(n, cfg.net),
            trace: Trace::new(cfg.trace_level),
            faults: FaultInjector::new(cfg.faults),
            disks: (0..n).map(|_| Disk::new(cfg.disk.clone())).collect(),
            skews,
            up: vec![true; n],
            incarnation: vec![0; n],
            rng,
            seed: cfg.seed,
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn nodes(&self) -> usize {
        self.up.len()
    }

    pub fn all_nodes(&self) -> impl Iterator<Item = NodeId> {
        (0..self.up.len() as u32).map(NodeId)
    }

    /// True simulation time.
    #[inline]
    pub fn now(&self) -> Nanos {
        self.sched.now()
    }

    /// Time as `node` believes it to be, including its skew and drift. This is
    /// the only clock a node is ever allowed to read.
    pub fn node_now(&self, node: NodeId) -> Nanos {
        self.skews[node.idx()].apply(self.sched.now())
    }

    pub fn is_up(&self, node: NodeId) -> bool {
        self.up[node.idx()]
    }

    pub fn up_count(&self) -> usize {
        self.up.iter().filter(|u| **u).count()
    }

    pub fn up_flags(&self) -> &[bool] {
        &self.up
    }

    pub fn incarnation(&self, node: NodeId) -> u64 {
        self.incarnation[node.idx()]
    }

    pub fn observe(&mut self, node: Option<NodeId>, tag: &'static str, args: &[u64]) {
        let now = self.sched.now();
        self.trace.observe(now, node, tag, args);
    }

    pub fn log(&mut self, level: Level, node: Option<NodeId>, cat: &'static str, msg: impl Into<String>) {
        let now = self.sched.now();
        self.trace.log(level, now, node, cat, msg);
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.trace.fingerprint()
    }

    // ---- network ----------------------------------------------------------

    /// Send bytes. Silently does nothing if the sender is down -- a crashed
    /// node cannot transmit.
    pub fn send(&mut self, from: NodeId, to: NodeId, payload: &[u8]) -> usize {
        if !self.up[from.idx()] {
            return 0;
        }
        self.net
            .send(&mut self.rng, &mut self.sched, from, to, payload)
    }

    /// Whether a message that has arrived should be handed to the receiver.
    pub fn accept_delivery(&mut self, from: NodeId, to: NodeId) -> bool {
        if !self.up[to.idx()] {
            return false;
        }
        self.net.accept_delivery(from, to)
    }

    // ---- timers -----------------------------------------------------------

    pub fn set_timer(&mut self, node: NodeId, delay: Nanos, timer: u64) -> EventId {
        self.sched.after(delay, Event::Timer { node, timer })
    }

    pub fn cancel(&mut self, id: EventId) {
        self.sched.cancel(id);
    }

    // ---- storage ----------------------------------------------------------

    /// Append bytes to a node's file. The completion event fires later; the
    /// data is *not* durable until a subsequent `sync` completes.
    pub fn disk_append(&mut self, node: NodeId, file: FileId, bytes: &[u8]) -> OpId {
        let issued = self.disks[node.idx()].append(&mut self.rng, file, bytes);
        self.complete_storage(node, issued.op, issued.delay, issued.result);
        issued.op
    }

    pub fn disk_write_at(&mut self, node: NodeId, file: FileId, offset: usize, bytes: &[u8]) -> OpId {
        let issued = self.disks[node.idx()].write_at(&mut self.rng, file, offset, bytes);
        self.complete_storage(node, issued.op, issued.delay, issued.result);
        issued.op
    }

    pub fn disk_set_len(&mut self, node: NodeId, file: FileId, len: usize) -> OpId {
        let issued = self.disks[node.idx()].set_len(&mut self.rng, file, len);
        self.complete_storage(node, issued.op, issued.delay, issued.result);
        issued.op
    }

    pub fn disk_sync(&mut self, node: NodeId, file: FileId) -> OpId {
        let issued = self.disks[node.idx()].sync(&mut self.rng, file);
        self.complete_storage(node, issued.op, issued.delay, issued.result);
        issued.op
    }

    fn complete_storage(&mut self, node: NodeId, op: OpId, delay: Nanos, result: IoResult) {
        self.sched
            .after(delay, Event::Storage { node, op, result });
    }

    /// Read a file as the running node sees it. Used at startup for recovery.
    pub fn disk_read(&self, node: NodeId, file: FileId) -> Vec<u8> {
        self.disks[node.idx()].read_all(file)
    }

    pub fn disk_len(&self, node: NodeId, file: FileId) -> usize {
        self.disks[node.idx()].len(file)
    }

    /// The crash-survivable image. Checkers only -- never a simulated node.
    pub fn durable_image(&self, node: NodeId, file: FileId) -> Vec<u8> {
        self.disks[node.idx()].durable_image(file)
    }

    pub fn disk_stats(&self, node: NodeId) -> DiskStats {
        self.disks[node.idx()].stats()
    }

    // ---- lifecycle --------------------------------------------------------

    /// Power loss on one node: unsynced writes are resolved by the disk model,
    /// and every event still in flight for that node is discarded. Messages
    /// already on the wire are *not* discarded -- they arrive and are dropped
    /// on delivery, which is what happens on a real network.
    pub fn crash_node(&mut self, node: NodeId) {
        if !self.up[node.idx()] {
            return;
        }
        self.disks[node.idx()].crash(&mut self.rng);
        self.up[node.idx()] = false;
        self.incarnation[node.idx()] += 1;
        self.sched.retain(|e| match e {
            Event::Timer { node: n, .. } | Event::Storage { node: n, .. } => *n != node,
            _ => true,
        });
        self.observe(Some(node), "crash", &[]);
    }

    pub fn restart_node(&mut self, node: NodeId) {
        if self.up[node.idx()] {
            return;
        }
        self.up[node.idx()] = true;
        self.observe(Some(node), "restart", &[]);
    }

    pub fn clock_jump(&mut self, node: NodeId, delta: i64) {
        self.skews[node.idx()].offset += delta;
        self.observe(Some(node), "clockjump", &[delta as u64]);
    }

    pub fn clock_skew(&self, node: NodeId) -> ClockSkew {
        self.skews[node.idx()]
    }

    pub fn net_stats(&self) -> NetStats {
        self.net.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn world(seed: u64) -> World {
        World::new(WorldConfig {
            nodes: 3,
            seed,
            faults: FaultConfig::none(),
            ..WorldConfig::default()
        })
    }

    #[test]
    fn crashed_nodes_cannot_send_or_receive() {
        let mut w = world(1);
        w.crash_node(NodeId(0));
        assert_eq!(w.send(NodeId(0), NodeId(1), b"x"), 0);
        assert!(!w.accept_delivery(NodeId(1), NodeId(0)));
        w.restart_node(NodeId(0));
        assert_eq!(w.send(NodeId(0), NodeId(1), b"x"), 1);
    }

    #[test]
    fn crash_discards_pending_timers_and_io() {
        let mut w = world(2);
        w.set_timer(NodeId(0), 100, 7);
        w.set_timer(NodeId(1), 100, 7);
        w.disk_append(NodeId(0), 0, b"data");
        assert_eq!(w.sched.pending(), 3);
        w.crash_node(NodeId(0));
        assert_eq!(w.sched.pending(), 1, "only n1's timer should remain");
    }

    #[test]
    fn unsynced_writes_do_not_survive_a_crash() {
        let mut w = World::new(WorldConfig {
            nodes: 1,
            seed: 3,
            disk: DiskConfig {
                lost_write_ppm: 1_000_000,
                ..DiskConfig::default()
            },
            faults: FaultConfig::none(),
            ..WorldConfig::default()
        });
        w.disk_append(NodeId(0), 0, b"hello");
        assert_eq!(w.disk_read(NodeId(0), 0), b"hello");
        w.crash_node(NodeId(0));
        assert!(w.disk_read(NodeId(0), 0).is_empty());
    }

    #[test]
    fn incarnation_advances_on_crash() {
        let mut w = world(4);
        assert_eq!(w.incarnation(NodeId(0)), 0);
        w.crash_node(NodeId(0));
        w.restart_node(NodeId(0));
        assert_eq!(w.incarnation(NodeId(0)), 1);
    }

    #[test]
    fn node_clocks_disagree_but_are_stable() {
        let w = World::new(WorldConfig {
            nodes: 5,
            seed: 12345,
            ..WorldConfig::default()
        });
        let times: Vec<Nanos> = w.all_nodes().map(|n| w.node_now(n)).collect();
        // With skew enabled at t=0 the offsets differ, but each node's view is
        // a pure function of true time, so reading twice agrees.
        let again: Vec<Nanos> = w.all_nodes().map(|n| w.node_now(n)).collect();
        assert_eq!(times, again);
        assert!(times.iter().any(|t| *t != times[0]) || w.clock_skew(NodeId(0)).offset == 0);
    }

    #[test]
    fn the_same_seed_produces_the_same_world() {
        let script = |seed: u64| {
            let mut w = world(seed);
            for i in 0..200u64 {
                let from = NodeId((i % 3) as u32);
                let to = NodeId(((i + 1) % 3) as u32);
                w.send(from, to, &i.to_le_bytes());
                w.disk_append(to, 0, &i.to_le_bytes());
                if i % 17 == 0 {
                    w.disk_sync(to, 0);
                }
                if let Some(f) = w.sched.next() {
                    w.trace.observe(f.time, f.event.target(), f.event.tag(), &[f.id]);
                }
            }
            w.fingerprint()
        };
        assert_eq!(script(777), script(777));
        assert_ne!(script(777), script(778));
    }
}
