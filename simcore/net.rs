//! The network model: latency, loss, duplication, reordering, partitions.
//!
//! Links are directed. A partition that blocks A->B but leaves B->A open is
//! not an exotic case -- asymmetric reachability is common in practice and it
//! breaks consensus implementations that assume "I can hear you" implies "you
//! can hear me".
//!
//! Reordering is not simulated with a special flag: every message draws its own
//! latency, so overtaking happens naturally, at a rate that depends on the
//! spread of the latency distribution.

use crate::rng::Rng;
use crate::scheduler::{Event, Scheduler};
use crate::{Nanos, NodeId};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub struct NetConfig {
    pub latency: (Nanos, Nanos),
    pub drop_ppm: u32,
    pub duplicate_ppm: u32,
    /// Probability a delivered message arrives with a flipped byte. The
    /// receiver is expected to reject it, not to panic.
    pub corrupt_ppm: u32,
    pub slow_ppm: u32,
    pub slow_factor: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        NetConfig {
            latency: (200 * crate::MICROS, 3 * crate::MILLIS),
            drop_ppm: 0,
            duplicate_ppm: 0,
            corrupt_ppm: 0,
            slow_ppm: 0,
            slow_factor: 30,
        }
    }
}

impl NetConfig {
    pub fn reliable() -> NetConfig {
        NetConfig::default()
    }

    pub fn hostile() -> NetConfig {
        NetConfig {
            drop_ppm: 50_000,
            duplicate_ppm: 20_000,
            corrupt_ppm: 1_000,
            slow_ppm: 20_000,
            ..NetConfig::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NetStats {
    pub sent: u64,
    pub dropped_loss: u64,
    pub dropped_partition: u64,
    pub duplicated: u64,
    pub corrupted: u64,
    pub delivered: u64,
}

#[derive(Clone, Debug)]
pub struct Network {
    cfg: NetConfig,
    nodes: usize,
    /// Directed links that are currently cut.
    blocked: BTreeSet<(u32, u32)>,
    /// Directed links that are up but degraded.
    slow: BTreeSet<(u32, u32)>,
    next_msg: u64,
    stats: NetStats,
}

impl Network {
    pub fn new(nodes: usize, cfg: NetConfig) -> Network {
        Network {
            cfg,
            nodes,
            blocked: BTreeSet::new(),
            slow: BTreeSet::new(),
            next_msg: 1,
            stats: NetStats::default(),
        }
    }

    pub fn config(&self) -> &NetConfig {
        &self.cfg
    }

    pub fn stats(&self) -> NetStats {
        self.stats
    }

    pub fn nodes(&self) -> usize {
        self.nodes
    }

    pub fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from.0, to.0))
    }

    /// Cut one direction only.
    pub fn cut(&mut self, from: NodeId, to: NodeId) {
        self.blocked.insert((from.0, to.0));
    }

    pub fn heal(&mut self, from: NodeId, to: NodeId) {
        self.blocked.remove(&(from.0, to.0));
    }

    pub fn heal_all(&mut self) {
        self.blocked.clear();
        self.slow.clear();
    }

    pub fn slow_link(&mut self, from: NodeId, to: NodeId) {
        self.slow.insert((from.0, to.0));
    }

    /// Cut every link in both directions between `node` and the rest.
    pub fn isolate(&mut self, node: NodeId) {
        for other in 0..self.nodes as u32 {
            if other != node.0 {
                self.blocked.insert((node.0, other));
                self.blocked.insert((other, node.0));
            }
        }
    }

    /// Split the cluster into disjoint groups that cannot talk across the
    /// boundary. Nodes not listed in any group keep talking to everyone.
    pub fn partition(&mut self, groups: &[Vec<NodeId>]) {
        let mut group_of = vec![usize::MAX; self.nodes];
        for (gi, g) in groups.iter().enumerate() {
            for n in g {
                if (n.0 as usize) < self.nodes {
                    group_of[n.0 as usize] = gi;
                }
            }
        }
        for a in 0..self.nodes {
            for b in 0..self.nodes {
                if a == b {
                    continue;
                }
                let (ga, gb) = (group_of[a], group_of[b]);
                if ga != usize::MAX && gb != usize::MAX && ga != gb {
                    self.blocked.insert((a as u32, b as u32));
                }
            }
        }
    }

    pub fn blocked_links(&self) -> usize {
        self.blocked.len()
    }

    /// Send `payload` from `from` to `to`, scheduling delivery events.
    ///
    /// Returns the number of copies that will be delivered (0 if dropped).
    pub fn send(
        &mut self,
        rng: &mut Rng,
        sched: &mut Scheduler,
        from: NodeId,
        to: NodeId,
        payload: &[u8],
    ) -> usize {
        self.stats.sent += 1;
        if self.is_blocked(from, to) {
            self.stats.dropped_partition += 1;
            return 0;
        }
        if rng.chance_ppm(self.cfg.drop_ppm) {
            self.stats.dropped_loss += 1;
            return 0;
        }

        // A duplicate is a second, independently delayed copy -- so the
        // duplicate may well arrive before the original.
        let copies = if rng.chance_ppm(self.cfg.duplicate_ppm) {
            self.stats.duplicated += 1;
            2
        } else {
            1
        };

        for _ in 0..copies {
            let msg = self.next_msg;
            self.next_msg += 1;
            let mut delay = rng.skewed(self.cfg.latency.0, self.cfg.latency.1);
            if self.slow.contains(&(from.0, to.0)) || rng.chance_ppm(self.cfg.slow_ppm) {
                delay = delay.saturating_mul(self.cfg.slow_factor);
            }
            let mut bytes = payload.to_vec();
            if rng.chance_ppm(self.cfg.corrupt_ppm) && !bytes.is_empty() {
                self.stats.corrupted += 1;
                let i = rng.below(bytes.len() as u64) as usize;
                bytes[i] ^= 1 << rng.below(8);
            }
            sched.after(
                delay,
                Event::Deliver {
                    from,
                    to,
                    payload: bytes,
                    msg,
                },
            );
        }
        copies
    }

    /// Called at delivery time. A partition that appears while a message is in
    /// flight still eats it, which is what real networks do.
    pub fn accept_delivery(&mut self, from: NodeId, to: NodeId) -> bool {
        if self.is_blocked(from, to) {
            self.stats.dropped_partition += 1;
            false
        } else {
            self.stats.delivered += 1;
            true
        }
    }

    pub fn latency_bounds(&self) -> (Nanos, Nanos) {
        self.cfg.latency
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(i: u32) -> NodeId {
        NodeId(i)
    }

    #[test]
    fn messages_are_delivered_with_latency() {
        let mut net = Network::new(3, NetConfig::default());
        let mut rng = Rng::new(1);
        let mut sched = Scheduler::new();
        assert_eq!(net.send(&mut rng, &mut sched, n(0), n(1), b"hi"), 1);
        let fired = sched.next().expect("a delivery should be scheduled");
        assert!(fired.time > 0, "delivery should not be instantaneous");
        match fired.event {
            Event::Deliver { from, to, payload, .. } => {
                assert_eq!((from.0, to.0), (0, 1));
                assert_eq!(payload, b"hi");
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn partitions_are_directional() {
        let mut net = Network::new(2, NetConfig::default());
        let mut rng = Rng::new(1);
        let mut sched = Scheduler::new();
        net.cut(n(0), n(1));
        assert_eq!(net.send(&mut rng, &mut sched, n(0), n(1), b"x"), 0);
        assert_eq!(net.send(&mut rng, &mut sched, n(1), n(0), b"x"), 1);
    }

    #[test]
    fn isolate_cuts_both_directions() {
        let mut net = Network::new(3, NetConfig::default());
        net.isolate(n(1));
        assert!(net.is_blocked(n(1), n(0)));
        assert!(net.is_blocked(n(0), n(1)));
        assert!(!net.is_blocked(n(0), n(2)));
    }

    #[test]
    fn partition_splits_groups() {
        let mut net = Network::new(5, NetConfig::default());
        net.partition(&[vec![n(0), n(1)], vec![n(2), n(3), n(4)]]);
        assert!(net.is_blocked(n(0), n(2)));
        assert!(net.is_blocked(n(2), n(0)));
        assert!(!net.is_blocked(n(0), n(1)));
        assert!(!net.is_blocked(n(3), n(4)));
        net.heal_all();
        assert!(!net.is_blocked(n(0), n(2)));
    }

    #[test]
    fn in_flight_messages_die_in_a_new_partition() {
        let mut net = Network::new(2, NetConfig::default());
        let mut rng = Rng::new(1);
        let mut sched = Scheduler::new();
        net.send(&mut rng, &mut sched, n(0), n(1), b"x");
        net.cut(n(0), n(1));
        assert!(!net.accept_delivery(n(0), n(1)));
    }

    #[test]
    fn loss_and_duplication_are_observable() {
        let cfg = NetConfig {
            drop_ppm: 300_000,
            duplicate_ppm: 300_000,
            ..NetConfig::default()
        };
        let mut net = Network::new(2, cfg);
        let mut rng = Rng::new(4);
        let mut sched = Scheduler::new();
        let mut counts = [0usize; 3];
        for _ in 0..1000 {
            counts[net.send(&mut rng, &mut sched, n(0), n(1), b"x")] += 1;
        }
        assert!(counts[0] > 200, "expected drops, got {counts:?}");
        assert!(counts[2] > 100, "expected duplicates, got {counts:?}");
    }

    #[test]
    fn duplicates_can_overtake_the_original() {
        let cfg = NetConfig {
            duplicate_ppm: 1_000_000,
            ..NetConfig::default()
        };
        let mut net = Network::new(2, cfg);
        let mut rng = Rng::new(8);
        let mut reordered = false;
        for _ in 0..200 {
            let mut sched = Scheduler::new();
            net.send(&mut rng, &mut sched, n(0), n(1), b"x");
            let first = sched.next().unwrap();
            let second = sched.next().unwrap();
            // Copies are numbered in send order; a later id arriving first is
            // a reordering.
            if let (Event::Deliver { msg: a, .. }, Event::Deliver { msg: b, .. }) =
                (&first.event, &second.event)
            {
                if a > b {
                    reordered = true;
                    break;
                }
            }
        }
        assert!(reordered, "duplicates should sometimes arrive out of order");
    }

    #[test]
    fn corruption_flips_exactly_one_bit() {
        let cfg = NetConfig {
            corrupt_ppm: 1_000_000,
            ..NetConfig::default()
        };
        let mut net = Network::new(2, cfg);
        let mut rng = Rng::new(2);
        let mut sched = Scheduler::new();
        let original = b"abcdefgh";
        net.send(&mut rng, &mut sched, n(0), n(1), original);
        match sched.next().unwrap().event {
            Event::Deliver { payload, .. } => {
                let diff: u32 = payload
                    .iter()
                    .zip(original)
                    .map(|(a, b)| (a ^ b).count_ones())
                    .sum();
                assert_eq!(diff, 1);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
