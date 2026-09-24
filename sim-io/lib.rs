//! `sim-io` -- the boundary between an application and the world it runs in.
//!
//! A node under test never touches [`simcore::World`] directly. It is handed an
//! [`Io`], through which it can read its own clock, set timers, send bytes, and
//! write to its own disk -- and nothing else. It cannot see the true time, the
//! other nodes' state, whether a peer is up, or whether the network is
//! partitioned.
//!
//! That restriction is the reason the tests mean anything: a node that can only
//! observe what a real node could observe cannot accidentally cheat, and code
//! written against this trait is the same code that would run against a real
//! kernel.

pub mod clock;
pub mod network;
pub mod storage;

pub use clock::{Clock, Deadline, TimerHandle, TimerTag};
pub use network::{peers, quorum, Net};
pub use storage::{
    snapshot_file, Completion, PendingOps, Storage, FILE_SNAPSHOT_A, FILE_SNAPSHOT_B, FILE_WAL,
};

use simcore::disk::{FileId, OpId};
use simcore::trace::Level;
use simcore::{NodeId, World};

/// Everything a node is allowed to do to the outside world.
///
/// Object-safe on purpose: nodes take `&mut dyn Io`, so the same code can be
/// driven by the simulator here or by a real event loop elsewhere.
pub trait Io: Clock + Net + Storage {
    fn trace_enabled(&self, level: Level) -> bool;

    /// Human-readable diagnostics. Filtered by level; costs nothing when off.
    fn trace(&mut self, level: Level, category: &'static str, message: String);

    /// Record a behaviour-defining event into the determinism fingerprint.
    /// Never filtered, so it must only be called for real state changes.
    fn observe(&mut self, tag: &'static str, args: &[u64]);

    /// Deterministic randomness for protocol jitter (election timeouts).
    fn rand_range(&mut self, lo: u64, hi: u64) -> u64;
}

/// Log through an [`Io`], skipping the formatting when the level is disabled.
#[macro_export]
macro_rules! io_trace {
    ($io:expr, $level:expr, $cat:expr, $($arg:tt)*) => {{
        let io = &mut *$io;
        if io.trace_enabled($level) {
            io.trace($level, $cat, format!($($arg)*));
        }
    }};
}

/// The simulator's implementation of [`Io`], bound to one node for the duration
/// of one event.
pub struct SimIo<'a> {
    world: &'a mut World,
    node: NodeId,
}

impl<'a> SimIo<'a> {
    pub fn new(world: &'a mut World, node: NodeId) -> SimIo<'a> {
        SimIo { world, node }
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    /// True simulation time. Not exposed through [`Io`]: nodes must not see it.
    pub fn true_now(&self) -> simcore::Nanos {
        self.world.now()
    }
}

impl Clock for SimIo<'_> {
    fn now(&self) -> simcore::Nanos {
        self.world.node_now(self.node)
    }

    fn set_timer(&mut self, delay: simcore::Nanos, tag: TimerTag) -> TimerHandle {
        // The delay is measured on the node's own (drifting) clock; the drift
        // is parts-per-million, so scheduling it against true time is within
        // the noise of the latency model and keeps the queue in one time base.
        TimerHandle(self.world.set_timer(self.node, delay, tag))
    }

    fn cancel_timer(&mut self, handle: TimerHandle) {
        self.world.cancel(handle.0);
    }
}

impl Net for SimIo<'_> {
    fn me(&self) -> NodeId {
        self.node
    }

    fn cluster_size(&self) -> usize {
        self.world.nodes()
    }

    fn send(&mut self, to: NodeId, payload: &[u8]) {
        if to == self.node {
            return;
        }
        self.world.send(self.node, to, payload);
    }
}

impl Storage for SimIo<'_> {
    fn append(&mut self, file: FileId, bytes: &[u8]) -> OpId {
        self.world.disk_append(self.node, file, bytes)
    }

    fn write_at(&mut self, file: FileId, offset: usize, bytes: &[u8]) -> OpId {
        self.world.disk_write_at(self.node, file, offset, bytes)
    }

    fn set_len(&mut self, file: FileId, len: usize) -> OpId {
        self.world.disk_set_len(self.node, file, len)
    }

    fn sync(&mut self, file: FileId) -> OpId {
        self.world.disk_sync(self.node, file)
    }

    fn read_all(&self, file: FileId) -> Vec<u8> {
        self.world.disk_read(self.node, file)
    }

    fn size(&self, file: FileId) -> usize {
        self.world.disk_len(self.node, file)
    }
}

impl Io for SimIo<'_> {
    fn trace_enabled(&self, level: Level) -> bool {
        self.world.trace.enabled(level)
    }

    fn trace(&mut self, level: Level, category: &'static str, message: String) {
        self.world.log(level, Some(self.node), category, message);
    }

    fn observe(&mut self, tag: &'static str, args: &[u64]) {
        self.world.observe(Some(self.node), tag, args);
    }

    fn rand_range(&mut self, lo: u64, hi: u64) -> u64 {
        self.world.rng.range(lo, hi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simcore::faults::FaultConfig;
    use simcore::scheduler::Event;
    use simcore::{WorldConfig, MILLIS};

    fn world() -> World {
        World::new(WorldConfig {
            nodes: 3,
            seed: 5,
            faults: FaultConfig::none(),
            ..WorldConfig::default()
        })
    }

    #[test]
    fn io_is_object_safe() {
        let mut w = world();
        let mut io = SimIo::new(&mut w, NodeId(0));
        let dynamic: &mut dyn Io = &mut io;
        dynamic.send(NodeId(1), b"hello");
        assert_eq!(dynamic.me(), NodeId(0));
    }

    #[test]
    fn timers_fire_with_their_tag() {
        let mut w = world();
        {
            let mut io = SimIo::new(&mut w, NodeId(1));
            io.set_timer(10 * MILLIS, 0xbeef);
        }
        match w.sched.next().unwrap().event {
            Event::Timer { node, timer } => {
                assert_eq!(node, NodeId(1));
                assert_eq!(timer, 0xbeef);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn cancelled_timers_never_arrive() {
        let mut w = world();
        {
            let mut io = SimIo::new(&mut w, NodeId(1));
            let h = io.set_timer(10 * MILLIS, 1);
            io.cancel_timer(h);
        }
        assert!(w.sched.next().is_none());
    }

    #[test]
    fn a_node_sees_its_own_writes_before_they_are_durable() {
        let mut w = world();
        let mut io = SimIo::new(&mut w, NodeId(0));
        io.append(FILE_WAL, b"entry");
        assert_eq!(io.read_all(FILE_WAL), b"entry");
        assert_eq!(io.size(FILE_WAL), 5);
    }

    #[test]
    fn nodes_see_skewed_clocks() {
        let mut w = World::new(WorldConfig {
            nodes: 3,
            seed: 42,
            ..WorldConfig::default()
        });
        w.clock_jump(NodeId(0), 1_000_000);
        let a = SimIo::new(&mut w, NodeId(0)).now();
        let b = SimIo::new(&mut w, NodeId(1)).now();
        assert_ne!(a, b, "a jumped clock must be visible to that node only");
    }

    #[test]
    fn self_sends_are_dropped() {
        let mut w = world();
        let mut io = SimIo::new(&mut w, NodeId(0));
        io.send(NodeId(0), b"loopback");
        assert!(w.sched.next().is_none());
    }
}
