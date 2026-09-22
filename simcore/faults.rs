//! Fault injection policy: what goes wrong, how often, and within what bounds.
//!
//! The injector decides *what* to do; the layer driving the simulation carries
//! it out, because only that layer knows how to rebuild a node from its disk.
//!
//! Two rules keep runs useful rather than merely brutal:
//!
//! * Faults are confined to a window. After it closes the world heals and every
//!   node comes back, so the run has a quiescent tail in which a correct
//!   cluster must converge. Without that tail, "nothing was answered" would be
//!   indistinguishable from "the data was lost".
//! * By default no more than a minority is unavailable at once. A cluster that
//!   has lost quorum is allowed to stall, so those runs test little. Set
//!   [`FaultConfig::allow_quorum_loss`] to test safety without liveness.

use crate::rng::Rng;
use crate::{Nanos, NodeId};

/// A concrete thing to do to the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultAction {
    /// Periodic decision point for the injector.
    Tick,
    /// Power loss: volatile state is gone, the disk keeps whatever the crash
    /// model left behind.
    Crash(NodeId),
    /// Bring a crashed node back, recovering its state from disk.
    Restart(NodeId),
    /// Cut the network into groups that cannot talk across the boundary.
    Partition(Vec<Vec<NodeId>>),
    /// Isolate a single node in both directions.
    Isolate(NodeId),
    /// Degrade one direction of one link.
    SlowLink(NodeId, NodeId),
    /// Repair every link.
    HealNetwork,
    /// Step a node's clock. Nothing in a correct consensus implementation may
    /// depend on clocks agreeing.
    ClockJump(NodeId, i64),
    /// End of the fault window: heal everything and restart every node.
    Recover,
}

#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub enable_crashes: bool,
    pub enable_partitions: bool,
    pub enable_clock_skew: bool,
    pub enable_slow_links: bool,
    /// Mean gap between fault decisions.
    pub tick_interval: (Nanos, Nanos),
    /// Weights for choosing among enabled fault kinds.
    pub crash_weight: u32,
    pub restart_weight: u32,
    pub partition_weight: u32,
    pub isolate_weight: u32,
    pub slow_link_weight: u32,
    pub clock_jump_weight: u32,
    pub heal_weight: u32,
    /// How long a partition lasts before it is healed automatically.
    pub partition_duration: (Nanos, Nanos),
    /// How long a crashed node stays down before it may restart.
    pub restart_delay: (Nanos, Nanos),
    /// Permit taking down a majority. Safety must still hold; liveness may not.
    pub allow_quorum_loss: bool,
    /// Faults only happen in `[start, end)`; after `end` the world heals.
    pub window: (Nanos, Nanos),
    /// Maximum absolute clock offset handed to a node at startup.
    pub max_clock_skew: Nanos,
    pub max_clock_jump: Nanos,
    /// How often a crash or isolation targets the *leader* rather than a node
    /// picked uniformly at random.
    ///
    /// Uniform faults are good at finding common bugs and bad at finding rare
    /// interleavings. The interesting windows in a consensus protocol are the
    /// ones around a leadership change -- between an election and the new
    /// leader replicating its first entry, say -- and a uniform injector
    /// reaches them only by luck. Aiming at the leader walks the cluster
    /// through those windows deliberately.
    pub leader_bias_ppm: u32,
    /// How often a crash targets a node that is holding unsynced writes.
    ///
    /// A durability bug only bites if the node dies while data it has acted on
    /// is still in the page cache. That window is short, so crashing uniformly
    /// misses it almost every time; aiming at it turns a rare coincidence into
    /// a routine event.
    pub unsynced_bias_ppm: u32,
}

impl Default for FaultConfig {
    fn default() -> Self {
        FaultConfig {
            enable_crashes: true,
            enable_partitions: true,
            enable_clock_skew: true,
            enable_slow_links: true,
            tick_interval: (20 * crate::MILLIS, 400 * crate::MILLIS),
            crash_weight: 30,
            restart_weight: 40,
            partition_weight: 20,
            isolate_weight: 15,
            slow_link_weight: 10,
            clock_jump_weight: 5,
            heal_weight: 25,
            partition_duration: (100 * crate::MILLIS, 3 * crate::SECONDS),
            restart_delay: (50 * crate::MILLIS, 2 * crate::SECONDS),
            allow_quorum_loss: false,
            window: (500 * crate::MILLIS, 20 * crate::SECONDS),
            max_clock_skew: 50 * crate::MILLIS,
            max_clock_jump: 500 * crate::MILLIS,
            leader_bias_ppm: 300_000,
            unsynced_bias_ppm: 500_000,
        }
    }
}

impl FaultConfig {
    /// No faults at all. A failing seed that still fails under this config is
    /// a plain logic bug, not a fault-handling bug.
    pub fn none() -> FaultConfig {
        FaultConfig {
            enable_crashes: false,
            enable_partitions: false,
            enable_clock_skew: false,
            enable_slow_links: false,
            window: (Nanos::MAX, Nanos::MAX),
            ..FaultConfig::default()
        }
    }

    pub fn any_enabled(&self) -> bool {
        self.enable_crashes
            || self.enable_partitions
            || self.enable_clock_skew
            || self.enable_slow_links
    }
}

/// A node's private view of time: a fixed offset plus a linear drift.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClockSkew {
    pub offset: i64,
    pub drift_ppm: i64,
}

impl ClockSkew {
    /// Translate true simulation time into what this node believes it is.
    pub fn apply(&self, true_time: Nanos) -> Nanos {
        let drift = (true_time as i128 * self.drift_ppm as i128) / 1_000_000;
        let t = true_time as i128 + self.offset as i128 + drift;
        t.max(0) as Nanos
    }

    pub fn random(rng: &mut Rng, max_offset: Nanos) -> ClockSkew {
        let mag = rng.below(max_offset + 1) as i64;
        let sign = if rng.chance_ppm(500_000) { -1 } else { 1 };
        ClockSkew {
            offset: mag * sign,
            // +/- 200 ppm is roughly a bad-but-real crystal.
            drift_ppm: rng.range(0, 400) as i64 - 200,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FaultStats {
    pub ticks: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub partitions: u64,
    pub isolations: u64,
    pub heals: u64,
    pub clock_jumps: u64,
    pub slow_links: u64,
    /// Faults aimed at the leader rather than a random node.
    pub leader_targeted: u64,
    /// Crashes aimed at a node holding unsynced writes.
    pub unsynced_targeted: u64,
}

/// What the driver knows about the cluster, for aiming faults.
///
/// The injector lives in `simcore` and deliberately knows nothing about
/// consensus; this is the narrow channel through which the layer that *does*
/// know can say where a fault would be most revealing.
#[derive(Default)]
pub struct FaultHint<'a> {
    /// Whoever currently believes they are leading, if anyone.
    pub leader: Option<NodeId>,
    /// Per node: is it holding writes it has not synced?
    pub unsynced: &'a [bool],
}

pub struct FaultInjector {
    cfg: FaultConfig,
    stats: FaultStats,
    recovered: bool,
}

impl FaultInjector {
    pub fn new(cfg: FaultConfig) -> FaultInjector {
        FaultInjector {
            cfg,
            stats: FaultStats::default(),
            recovered: false,
        }
    }

    pub fn config(&self) -> &FaultConfig {
        &self.cfg
    }

    pub fn stats(&self) -> FaultStats {
        self.stats
    }

    pub fn recovered(&self) -> bool {
        self.recovered
    }

    /// When the next decision point should fire.
    pub fn next_tick_delay(&self, rng: &mut Rng) -> Nanos {
        rng.range(self.cfg.tick_interval.0, self.cfg.tick_interval.1)
    }

    pub fn window_start(&self) -> Nanos {
        self.cfg.window.0
    }

    pub fn window_end(&self) -> Nanos {
        self.cfg.window.1
    }

    /// How many nodes may be down simultaneously.
    fn max_down(&self, n: usize) -> usize {
        if self.cfg.allow_quorum_loss {
            n.saturating_sub(1)
        } else {
            (n.saturating_sub(1)) / 2
        }
    }

    /// Choose the next fault, given who is currently up.
    ///
    /// `None` means "do nothing this tick", which is itself important: a
    /// cluster that is never left alone never gets to demonstrate progress.
    /// Choose the next fault.
    ///
    /// `hint` is what the driver knows about the cluster right now. It is used
    /// to aim faults rather than scatter them: see [`FaultHint`].
    pub fn decide(
        &mut self,
        rng: &mut Rng,
        now: Nanos,
        up: &[bool],
        hint: &FaultHint,
    ) -> Option<FaultAction> {
        let leader = hint.leader;
        if now >= self.cfg.window.1 {
            if !self.recovered {
                self.recovered = true;
                return Some(FaultAction::Recover);
            }
            return None;
        }
        if now < self.cfg.window.0 || !self.cfg.any_enabled() {
            return None;
        }
        self.stats.ticks += 1;

        let n = up.len();
        let down: Vec<NodeId> = (0..n)
            .filter(|i| !up[*i])
            .map(|i| NodeId(i as u32))
            .collect();
        let alive: Vec<NodeId> = (0..n)
            .filter(|i| up[*i])
            .map(|i| NodeId(i as u32))
            .collect();

        // Build the menu of currently legal actions with their weights.
        let mut menu: Vec<(u32, FaultChoice)> = Vec::new();
        let c = &self.cfg;
        if c.enable_crashes {
            if down.len() < self.max_down(n) && !alive.is_empty() {
                menu.push((c.crash_weight, FaultChoice::Crash));
            }
            if !down.is_empty() {
                menu.push((c.restart_weight, FaultChoice::Restart));
            }
        }
        if c.enable_partitions && n >= 2 {
            menu.push((c.partition_weight, FaultChoice::Partition));
            menu.push((c.isolate_weight, FaultChoice::Isolate));
            menu.push((c.heal_weight, FaultChoice::Heal));
        }
        if c.enable_slow_links && n >= 2 {
            menu.push((c.slow_link_weight, FaultChoice::SlowLink));
        }
        if c.enable_clock_skew {
            menu.push((c.clock_jump_weight, FaultChoice::ClockJump));
        }
        let choice = weighted_pick(rng, &menu)?;

        let action = match choice {
            FaultChoice::Crash => {
                self.stats.crashes += 1;
                // Aim, in order of how much a crash there is likely to reveal:
                // the leader, then whoever is holding unsynced writes, then
                // anyone at all.
                let dirty: Vec<NodeId> = alive
                    .iter()
                    .copied()
                    .filter(|n| hint.unsynced.get(n.idx()).copied().unwrap_or(false))
                    .collect();
                let target = match leader {
                    Some(l) if alive.contains(&l) && rng.chance_ppm(c.leader_bias_ppm) => {
                        self.stats.leader_targeted += 1;
                        l
                    }
                    _ if !dirty.is_empty() && rng.chance_ppm(c.unsynced_bias_ppm) => {
                        self.stats.unsynced_targeted += 1;
                        *rng.choose(&dirty)?
                    }
                    _ => *rng.choose(&alive)?,
                };
                FaultAction::Crash(target)
            }
            FaultChoice::Restart => {
                self.stats.restarts += 1;
                FaultAction::Restart(*rng.choose(&down)?)
            }
            FaultChoice::Partition => {
                self.stats.partitions += 1;
                let mut ids: Vec<NodeId> = (0..n as u32).map(NodeId).collect();
                rng.shuffle(&mut ids);
                // A split anywhere in 1..n, so both majority/minority splits
                // and even splits all occur.
                let cut = 1 + rng.below(n as u64 - 1) as usize;
                let (a, b) = ids.split_at(cut);
                FaultAction::Partition(vec![a.to_vec(), b.to_vec()])
            }
            FaultChoice::Isolate => {
                self.stats.isolations += 1;
                let target = match leader {
                    Some(l) if rng.chance_ppm(c.leader_bias_ppm) => {
                        self.stats.leader_targeted += 1;
                        l
                    }
                    _ => NodeId(rng.below(n as u64) as u32),
                };
                FaultAction::Isolate(target)
            }
            FaultChoice::Heal => {
                self.stats.heals += 1;
                FaultAction::HealNetwork
            }
            FaultChoice::SlowLink => {
                self.stats.slow_links += 1;
                let a = rng.below(n as u64) as u32;
                let mut b = rng.below(n as u64) as u32;
                if a == b {
                    b = (b + 1) % n as u32;
                }
                FaultAction::SlowLink(NodeId(a), NodeId(b))
            }
            FaultChoice::ClockJump => {
                self.stats.clock_jumps += 1;
                let mag = rng.below(c.max_clock_jump + 1) as i64;
                let sign = if rng.chance_ppm(500_000) { -1 } else { 1 };
                FaultAction::ClockJump(NodeId(rng.below(n as u64) as u32), mag * sign)
            }
        };
        Some(action)
    }

    pub fn restart_delay(&self, rng: &mut Rng) -> Nanos {
        rng.range(self.cfg.restart_delay.0, self.cfg.restart_delay.1)
    }

    pub fn partition_duration(&self, rng: &mut Rng) -> Nanos {
        rng.range(self.cfg.partition_duration.0, self.cfg.partition_duration.1)
    }
}

#[derive(Clone, Copy, Debug)]
enum FaultChoice {
    Crash,
    Restart,
    Partition,
    Isolate,
    Heal,
    SlowLink,
    ClockJump,
}

fn weighted_pick(rng: &mut Rng, menu: &[(u32, FaultChoice)]) -> Option<FaultChoice> {
    let total: u64 = menu.iter().map(|(w, _)| *w as u64).sum();
    if total == 0 {
        return None;
    }
    let mut pick = rng.below(total);
    for (w, choice) in menu {
        if pick < *w as u64 {
            return Some(*choice);
        }
        pick -= *w as u64;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_all_on() -> FaultConfig {
        FaultConfig {
            window: (0, Nanos::MAX / 2),
            ..FaultConfig::default()
        }
    }

    #[test]
    fn no_faults_when_disabled() {
        let mut inj = FaultInjector::new(FaultConfig::none());
        let mut rng = Rng::new(1);
        for t in 0..1000 {
            assert_eq!(
                inj.decide(&mut rng, t, &[true; 3], &FaultHint::default()),
                None
            );
        }
    }

    #[test]
    fn faults_respect_the_window() {
        let cfg = FaultConfig {
            window: (100, 200),
            ..FaultConfig::default()
        };
        let mut inj = FaultInjector::new(cfg);
        let mut rng = Rng::new(1);
        assert_eq!(
            inj.decide(&mut rng, 50, &[true; 5], &FaultHint::default()),
            None
        );
        let during: Vec<_> = (100..200)
            .filter_map(|t| inj.decide(&mut rng, t, &[true; 5], &FaultHint::default()))
            .collect();
        assert!(!during.is_empty(), "expected faults inside the window");
        assert_eq!(
            inj.decide(&mut rng, 250, &[true; 5], &FaultHint::default()),
            Some(FaultAction::Recover)
        );
        assert_eq!(
            inj.decide(&mut rng, 260, &[true; 5], &FaultHint::default()),
            None
        );
    }

    #[test]
    fn never_crashes_past_a_minority() {
        let mut inj = FaultInjector::new(cfg_all_on());
        let mut rng = Rng::new(7);
        let mut up = [true; 5];
        for t in 0..20_000 {
            match inj.decide(&mut rng, t, &up, &FaultHint::default()) {
                Some(FaultAction::Crash(n)) => up[n.idx()] = false,
                Some(FaultAction::Restart(n)) => up[n.idx()] = true,
                _ => {}
            }
            let down = up.iter().filter(|u| !**u).count();
            assert!(down <= 2, "quorum lost: {down} of 5 down");
        }
    }

    #[test]
    fn quorum_loss_is_reachable_when_allowed() {
        let cfg = FaultConfig {
            allow_quorum_loss: true,
            ..cfg_all_on()
        };
        let mut inj = FaultInjector::new(cfg);
        let mut rng = Rng::new(7);
        let mut up = [true; 3];
        let mut saw_majority_down = false;
        for t in 0..20_000 {
            match inj.decide(&mut rng, t, &up, &FaultHint::default()) {
                Some(FaultAction::Crash(n)) => up[n.idx()] = false,
                Some(FaultAction::Restart(n)) => up[n.idx()] = true,
                _ => {}
            }
            if up.iter().filter(|u| !**u).count() >= 2 {
                saw_majority_down = true;
            }
            assert!(up.iter().any(|u| *u), "at least one node stays up");
        }
        assert!(saw_majority_down);
    }

    #[test]
    fn crashes_only_target_live_nodes() {
        let mut inj = FaultInjector::new(cfg_all_on());
        let mut rng = Rng::new(3);
        let up = [true, false, true, true, false];
        for t in 0..5_000 {
            match inj.decide(&mut rng, t, &up, &FaultHint::default()) {
                Some(FaultAction::Crash(n)) => assert!(up[n.idx()]),
                Some(FaultAction::Restart(n)) => assert!(!up[n.idx()]),
                _ => {}
            }
        }
    }

    #[test]
    fn partitions_are_a_proper_split() {
        let mut inj = FaultInjector::new(cfg_all_on());
        let mut rng = Rng::new(5);
        let mut seen = 0;
        for t in 0..5_000 {
            if let Some(FaultAction::Partition(groups)) =
                inj.decide(&mut rng, t, &[true; 5], &FaultHint::default())
            {
                seen += 1;
                assert_eq!(groups.len(), 2);
                assert!(!groups[0].is_empty() && !groups[1].is_empty());
                let total: usize = groups.iter().map(|g| g.len()).sum();
                assert_eq!(total, 5, "every node lands in exactly one group");
                let mut all: Vec<u32> = groups.iter().flatten().map(|n| n.0).collect();
                all.sort();
                assert_eq!(all, vec![0, 1, 2, 3, 4]);
            }
        }
        assert!(seen > 10, "expected partitions, saw {seen}");
    }

    #[test]
    fn clock_skew_applies_offset_and_drift() {
        let s = ClockSkew {
            offset: -1000,
            drift_ppm: 1_000_000,
        };
        // Doubling drift: 5000 true -> 5000 + 5000 - 1000.
        assert_eq!(s.apply(5000), 9000);
        // Never negative.
        let s2 = ClockSkew {
            offset: -10_000,
            drift_ppm: 0,
        };
        assert_eq!(s2.apply(100), 0);
    }

    #[test]
    fn decisions_are_deterministic() {
        let run = || {
            let mut inj = FaultInjector::new(cfg_all_on());
            let mut rng = Rng::new(1234);
            (0..2000)
                .filter_map(|t| {
                    inj.decide(
                        &mut rng,
                        t,
                        &[true, true, false, true, true],
                        &FaultHint::default(),
                    )
                })
                .map(|a| format!("{a:?}"))
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
