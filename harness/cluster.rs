//! The driver: one simulated run from bootstrap to verdict.
//!
//! This is the only place that sees everything -- the world, the servers, the
//! clients and the checkers -- and it is careful about what it lets through.
//! Servers are handed a [`SimIo`] bound to their own node and nothing else, so
//! they cannot consult the oracle they are being judged by.
//!
//! A run has three phases:
//!
//! 1. **Chaos.** Faults are injected inside the configured window: crashes,
//!    partitions, clock jumps, slow links, disk errors.
//! 2. **Quiesce.** Everything heals, every node comes back, and the clients keep
//!    working. A correct cluster must elect a leader and start answering again.
//!    Without this phase, "the cluster is wedged" and "the cluster is fine but
//!    busy" would look identical.
//! 3. **Drain.** Clients stop, in-flight work finishes, and the final checks
//!    run against a settled system.

use crate::workload::{Client, TIMER_THINK};
use crate::{RunOutcome, RunStats, SimConfig};
use checker::durability::{self, CommittedEntry, DiskView};
use checker::invariants::{Invariants, NodeView};
use checker::linearizability::{Checker, History, Verdict};
use checker::{Report, Violation};
use kvstore::log::{Entry, RaftLog};
use kvstore::raft::Role;
use kvstore::{KvServer, Message};
use sim_io::{SimIo, FILE_WAL};
use simcore::faults::FaultAction;
use simcore::scheduler::{Event, Fired};
use simcore::trace::Level;
use simcore::{Nanos, NodeId, World, WorldConfig};
use std::collections::BTreeMap;

const APP_FAULT_TICK: u32 = 1;
const APP_HEAL: u32 = 2;
const APP_RESTART: u32 = 3;
const APP_STOP_CLIENTS: u32 = 4;
const APP_STOP: u32 = 5;

pub struct Cluster {
    world: World,
    servers: Vec<Option<KvServer>>,
    clients: Vec<Client>,
    members: Vec<NodeId>,
    cfg: SimConfig,

    history: History,
    invariants: Invariants,
    report: Report,

    /// Each node's log immediately before it crashed, so recovery can be
    /// checked for having produced a prefix of it.
    pre_crash_log: BTreeMap<NodeId, (Vec<Entry>, u64)>,
    /// Highest term each node has been seen to send a message in.
    acted_term: BTreeMap<NodeId, u64>,
    /// Stand-in log for a node that is currently down.
    empty_log: RaftLog,
    /// Per-node count of committed-entry truncations already reported.
    truncated_committed_seen: Vec<u64>,

    events: u64,
    incomplete: bool,
    finished: bool,
    recovered_at: Option<Nanos>,
    completed_at_recovery: usize,
    stats: RunStats,
}

impl Cluster {
    pub fn new(cfg: SimConfig) -> Cluster {
        let members: Vec<NodeId> = (0..cfg.servers as u32).map(NodeId).collect();
        let mut world = World::new(WorldConfig {
            nodes: cfg.servers + cfg.clients,
            seed: cfg.seed,
            net: cfg.net.clone(),
            disk: cfg.disk.clone(),
            faults: cfg.faults.clone(),
            trace_level: cfg.trace_level,
        });

        let mut servers = Vec::with_capacity(cfg.servers);
        for i in 0..cfg.servers {
            let node = NodeId(i as u32);
            let seed = node_seed(cfg.seed, node, 0);
            let mut io = SimIo::new(&mut world, node);
            servers.push(Some(KvServer::recover(
                &mut io,
                node,
                members.clone(),
                cfg.raft.clone(),
                seed,
            )));
        }

        let clients = (0..cfg.clients)
            .map(|i| {
                Client::new(
                    i as u32,
                    NodeId((cfg.servers + i) as u32),
                    members.clone(),
                    cfg.seed ^ 0xC11E_0000 ^ i as u64,
                    cfg.workload.clone(),
                )
            })
            .collect();

        let truncated_committed_seen = vec![0; servers.len()];
        Cluster {
            world,
            servers,
            clients,
            members,
            cfg,
            history: History::new(),
            invariants: Invariants::new(),
            report: Report::new(),
            pre_crash_log: BTreeMap::new(),
            acted_term: BTreeMap::new(),
            empty_log: RaftLog::new(),
            truncated_committed_seen,
            events: 0,
            incomplete: false,
            finished: false,
            recovered_at: None,
            completed_at_recovery: 0,
            stats: RunStats::default(),
        }
    }

    /// Run to completion and judge the result.
    pub fn run(mut self) -> RunOutcome {
        self.bootstrap();
        while !self.finished {
            let Some(fired) = self.world.sched.next() else {
                break;
            };
            self.events += 1;
            if self.events > self.cfg.max_events {
                self.incomplete = true;
                break;
            }
            self.dispatch(fired);
            self.reap_failed();
            if self.events.is_multiple_of(self.cfg.check_every) {
                self.check_invariants();
            }
            if self.events.is_multiple_of(self.cfg.check_durability_every) {
                self.check_durable_claims();
            }
        }
        self.check_invariants();
        self.finalize()
    }

    fn bootstrap(&mut self) {
        let stop_clients = self.cfg.duration + self.cfg.settle;
        let stop = stop_clients + self.cfg.drain;
        self.world.sched.at(
            self.cfg.faults.window.0,
            Event::App {
                tag: APP_FAULT_TICK,
                arg: 0,
            },
        );
        self.world.sched.at(
            stop_clients,
            Event::App {
                tag: APP_STOP_CLIENTS,
                arg: 0,
            },
        );
        self.world.sched.at(
            stop,
            Event::App {
                tag: APP_STOP,
                arg: 0,
            },
        );

        // Stagger the clients so they do not all hit the same node at t=0.
        for i in 0..self.clients.len() {
            let node = self.clients[i].node();
            let delay = self.world.rng.range(0, 20 * simcore::MILLIS);
            self.world.set_timer(node, delay, TIMER_THINK);
        }
    }

    fn dispatch(&mut self, fired: Fired) {
        let now = fired.time;
        match fired.event {
            Event::Timer { node, timer } => self.on_timer(node, timer, now),
            Event::Deliver {
                from,
                to,
                payload,
                msg: _,
            } => self.on_deliver(from, to, &payload, now),
            Event::Storage { node, op, result } => {
                if !self.world.is_up(node) {
                    return;
                }
                let i = node.idx();
                if let Some(server) = self.servers[i].as_mut() {
                    let mut io = SimIo::new(&mut self.world, node);
                    server.on_storage(&mut io, op, result.is_ok());
                }
            }
            Event::Fault(action) => self.apply_fault(action, now),
            Event::App { tag, arg } => self.on_app(tag, arg, now),
        }
    }

    fn on_timer(&mut self, node: NodeId, timer: u64, now: Nanos) {
        if self.is_client(node) {
            let i = node.idx() - self.cfg.servers;
            let mut io = SimIo::new(&mut self.world, node);
            self.clients[i].on_timer(&mut io, &mut self.history, now, timer);
            return;
        }
        if !self.world.is_up(node) {
            return;
        }
        let i = node.idx();
        if let Some(server) = self.servers[i].as_mut() {
            let mut io = SimIo::new(&mut self.world, node);
            server.on_timer(&mut io, timer);
        }
    }

    fn on_deliver(&mut self, from: NodeId, to: NodeId, payload: &[u8], now: Nanos) {
        // A message that was sent is evidence about its sender regardless of
        // whether it is allowed to arrive.
        if !self.is_client(from) {
            if let Ok(Message::Raft(m)) = Message::decode(payload) {
                let term = m.term();
                self.invariants.note_sent(from, term);
                let e = self.acted_term.entry(from).or_insert(0);
                *e = (*e).max(term);
            }
        }
        if !self.world.accept_delivery(from, to) {
            return;
        }
        self.stats.messages_delivered += 1;
        if self.is_client(to) {
            let i = to.idx() - self.cfg.servers;
            let mut io = SimIo::new(&mut self.world, to);
            self.clients[i].on_bytes(&mut io, &mut self.history, now, payload);
        } else if let Some(server) = self.servers[to.idx()].as_mut() {
            let mut io = SimIo::new(&mut self.world, to);
            server.on_bytes(&mut io, from, payload);
        }
    }

    fn on_app(&mut self, tag: u32, arg: u64, now: Nanos) {
        match tag {
            APP_FAULT_TICK => {
                let up: Vec<bool> = self.world.up_flags()[..self.cfg.servers].to_vec();
                let leader = self.current_leader();
                if let Some(action) =
                    self.world
                        .faults
                        .decide(&mut self.world.rng, now, &up, leader)
                {
                    self.apply_fault(action, now);
                }
                // Keep ticking until the injector has emitted its recovery.
                if !self.world.faults.recovered() {
                    let delay = self.world.faults.next_tick_delay(&mut self.world.rng);
                    self.world.sched.after(
                        delay,
                        Event::App {
                            tag: APP_FAULT_TICK,
                            arg: 0,
                        },
                    );
                }
            }
            APP_HEAL => {
                self.world.net.heal_all();
                self.world.observe(None, "heal", &[]);
            }
            APP_RESTART => {
                let node = NodeId(arg as u32);
                self.restart(node, now);
            }
            APP_STOP_CLIENTS => {
                for c in &mut self.clients {
                    c.stop();
                }
            }
            APP_STOP => self.finished = true,
            _ => {}
        }
    }

    fn apply_fault(&mut self, action: FaultAction, now: Nanos) {
        match action {
            FaultAction::Tick => {}
            FaultAction::Crash(node) => {
                self.crash(node);
                self.stats.crashes += 1;
            }
            FaultAction::Restart(node) => self.restart(node, now),
            FaultAction::Partition(groups) => {
                // Clients are placed on one side or the other, so a client can
                // find itself talking to a minority that cannot commit.
                let mut groups = groups;
                for c in &self.clients {
                    let g = self.world.rng.below(groups.len() as u64) as usize;
                    groups[g].push(c.node());
                }
                self.world.net.heal_all();
                self.world.net.partition(&groups);
                self.stats.partitions += 1;
                self.world
                    .observe(None, "partition", &[groups.len() as u64]);
                self.schedule_heal();
            }
            FaultAction::Isolate(node) => {
                self.world.net.isolate(node);
                self.stats.partitions += 1;
                self.world.observe(Some(node), "isolate", &[]);
                self.schedule_heal();
            }
            FaultAction::SlowLink(a, b) => self.world.net.slow_link(a, b),
            FaultAction::HealNetwork => self.world.net.heal_all(),
            FaultAction::ClockJump(node, delta) => self.world.clock_jump(node, delta),
            FaultAction::Recover => {
                self.world.net.heal_all();
                let down: Vec<NodeId> = self
                    .members
                    .iter()
                    .copied()
                    .filter(|n| !self.world.is_up(*n))
                    .collect();
                for n in down {
                    self.restart(n, now);
                }
                self.recovered_at = Some(now);
                self.completed_at_recovery = self.history.completed();
                self.world.log(
                    Level::Info,
                    None,
                    "harness",
                    "fault window closed; cluster healed".to_string(),
                );
            }
        }
    }

    fn schedule_heal(&mut self) {
        let d = self.world.faults.partition_duration(&mut self.world.rng);
        self.world.sched.after(
            d,
            Event::App {
                tag: APP_HEAL,
                arg: 0,
            },
        );
    }

    fn crash(&mut self, node: NodeId) {
        if !self.world.is_up(node) {
            return;
        }
        if let Some(server) = &self.servers[node.idx()] {
            // Remember both the log and how much of it the node believed was
            // synced: that is the part recovery is obliged to bring back.
            self.pre_crash_log.insert(
                node,
                (
                    server.raft.log().entries().to_vec(),
                    server.raft.durable_index(),
                ),
            );
        }
        self.servers[node.idx()] = None;
        self.world.crash_node(node);
    }

    fn restart(&mut self, node: NodeId, now: Nanos) {
        if self.world.is_up(node) {
            return;
        }
        self.world.restart_node(node);
        let incarnation = self.world.incarnation(node);
        let seed = node_seed(self.cfg.seed, node, incarnation);
        let members = self.members.clone();
        let raft_cfg = self.cfg.raft.clone();
        let mut io = SimIo::new(&mut self.world, node);
        let server = KvServer::recover(&mut io, node, members, raft_cfg, seed);

        // Everything this node had synced must have come back unchanged.
        if let Some((before, durable)) = self.pre_crash_log.get(&node) {
            let after = server.raft.log().entries();
            self.report.extend(durability::check_recovery(
                now, node, before, *durable, after,
            ));
        }
        // And the disk must not have forgotten a term this node acted on.
        if let Some(acted) = self.acted_term.get(&node).copied() {
            let disk = DiskView {
                id: node,
                bytes: self.world.durable_image(node, FILE_WAL),
            };
            self.report
                .extend(durability::check_term_durable(now, node, acted, &disk));
        }
        self.servers[node.idx()] = Some(server);
        self.stats.restarts += 1;
    }

    /// A node that hit a storage error takes itself down; bring it back after a
    /// pause, the way a supervisor would restart a crashed process.
    fn reap_failed(&mut self) {
        for i in 0..self.cfg.servers {
            let node = NodeId(i as u32);
            let failed = self.servers[i].as_ref().is_some_and(|s| s.failed());
            if failed {
                self.crash(node);
                self.stats.io_failures += 1;
                let delay = self.world.faults.restart_delay(&mut self.world.rng);
                self.world.sched.after(
                    delay,
                    Event::App {
                        tag: APP_RESTART,
                        arg: node.0 as u64,
                    },
                );
            }
        }
    }

    /// Whoever currently believes they are leading, at the highest term seen.
    ///
    /// A best-effort view for aiming faults, not an oracle: during an election
    /// there may be nobody, and a partitioned-away old leader still counts
    /// itself as one. Both are fine -- hitting a stale leader is a useful fault
    /// too.
    fn current_leader(&self) -> Option<NodeId> {
        let mut best: Option<(u64, NodeId)> = None;
        for i in 0..self.cfg.servers {
            let id = NodeId(i as u32);
            if !self.world.is_up(id) {
                continue;
            }
            if let Some(s) = &self.servers[i] {
                if s.raft.is_leader() && best.is_none_or(|(t, _)| s.raft.term() > t) {
                    best = Some((s.raft.term(), id));
                }
            }
        }
        best.map(|(_, id)| id)
    }

    fn is_client(&self, node: NodeId) -> bool {
        node.idx() >= self.cfg.servers
    }

    /// Verify every live node's durability claim against the bytes that would
    /// actually survive a power cut.
    fn check_durable_claims(&mut self) {
        let now = self.world.now();
        for i in 0..self.cfg.servers {
            let node = NodeId(i as u32);
            if !self.world.is_up(node) {
                continue;
            }
            let Some(server) = &self.servers[i] else {
                continue;
            };
            let disk = DiskView {
                id: node,
                bytes: self.world.durable_image(node, FILE_WAL),
            };
            let found = durability::check_durable_claim(
                now,
                node,
                server.raft.log().entries(),
                server.raft.durable_index(),
                &disk,
            );
            self.report.extend(found);
        }
    }

    fn check_invariants(&mut self) {
        let now = self.world.now();
        for i in 0..self.cfg.servers {
            let Some(s) = &self.servers[i] else { continue };
            let n = s.raft.stats().truncated_committed;
            if n > self.truncated_committed_seen[i] {
                self.truncated_committed_seen[i] = n;
                self.report.add(Violation::new(
                    "truncated_committed",
                    now,
                    Some(NodeId(i as u32)),
                    format!(
                        "node truncated its log at or below its own commit index                          (commit {})",
                        s.raft.commit_index()
                    ),
                ));
            }
        }
        let views: Vec<NodeView> = (0..self.cfg.servers)
            .map(|i| {
                let id = NodeId(i as u32);
                match &self.servers[i] {
                    Some(s) => NodeView {
                        id,
                        up: self.world.is_up(id),
                        role: s.raft.role(),
                        term: s.raft.term(),
                        log: s.raft.log(),
                        commit_index: s.raft.commit_index(),
                        last_applied: s.raft.last_applied(),
                        durable_index: s.raft.durable_index(),
                        incarnation: self.world.incarnation(id),
                        truncations: s.raft.stats().truncations,
                    },
                    None => NodeView {
                        id,
                        up: false,
                        role: Role::Follower,
                        term: 0,
                        log: &self.empty_log,
                        commit_index: 0,
                        last_applied: 0,
                        durable_index: 0,
                        incarnation: self.world.incarnation(id),
                        truncations: 0,
                    },
                }
            })
            .collect();
        let found = self.invariants.observe(now, &views);
        self.report.extend(found);
    }

    fn finalize(mut self) -> RunOutcome {
        let now = self.world.now();

        // The recording itself has to be sane before anything based on it is.
        if let Err(e) = self.history.well_formed() {
            self.report.add(Violation::new(
                "history_malformed",
                now,
                None,
                format!("the harness recorded an impossible history: {e}"),
            ));
        }

        // Replicas that have applied the same number of entries must agree.
        let states: Vec<(NodeId, u64, &std::collections::BTreeMap<String, String>)> =
            (0..self.cfg.servers)
                .filter_map(|i| {
                    let id = NodeId(i as u32);
                    let s = self.servers[i].as_ref()?;
                    self.world
                        .is_up(id)
                        .then(|| (id, s.raft.last_applied(), s.state()))
                })
                .collect();
        self.report
            .extend(durability::check_convergence(now, &states));

        // Everything committed must be on stable storage on a majority.
        let committed: Vec<CommittedEntry> = (1..=self.invariants.max_committed())
            .filter_map(|index| {
                self.invariants
                    .committed_entry(index)
                    .map(|rec| CommittedEntry {
                        index,
                        term: rec.term,
                        cmd: rec.cmd.clone(),
                    })
            })
            .collect();
        let disks: Vec<DiskView> = self
            .members
            .iter()
            .map(|n| DiskView {
                id: *n,
                bytes: self.world.durable_image(*n, FILE_WAL),
            })
            .collect();
        self.report.extend(durability::check_committed_durable(
            now,
            &committed,
            &disks,
            self.cfg.servers,
        ));

        // Liveness: after everything healed, the cluster had to get back to
        // work. A wedged cluster is a bug even if it never says anything wrong.
        if self.cfg.check_liveness && !self.incomplete && !self.clients.is_empty() {
            if let Some(at) = self.recovered_at {
                let after = self.history.completed() - self.completed_at_recovery;
                if after == 0 {
                    self.report.add(Violation::new(
                        "no_progress_after_recovery",
                        now,
                        None,
                        format!(
                            "no operation completed in the {:.1}s after the cluster healed at {:.1}s",
                            (now.saturating_sub(at)) as f64 / simcore::SECONDS as f64,
                            at as f64 / simcore::SECONDS as f64
                        ),
                    ));
                }
            }
        }

        // And finally the only thing a user can see.
        let mut lin = Checker {
            budget: self.cfg.linearizability_budget,
            ..Checker::new()
        };
        let verdict = lin.check(&self.history);
        if let Verdict::Violation { key, detail } = &verdict {
            self.report.add(Violation::new(
                "linearizability",
                now,
                None,
                format!("key {key}: {detail}"),
            ));
        }

        self.stats.events = self.events;
        self.stats.sim_time = now;
        self.stats.ops_started = self.clients.iter().map(|c| c.stats().started).sum();
        self.stats.ops_completed = self.history.completed() as u64;
        self.stats.ops_abandoned = self.clients.iter().map(|c| c.stats().abandoned).sum();
        self.stats.retries = self.clients.iter().map(|c| c.stats().retries).sum();
        self.stats.max_committed = self.invariants.max_committed();
        self.stats.elections = self
            .servers
            .iter()
            .flatten()
            .map(|s| s.raft.stats().elections_started)
            .sum();
        self.stats.leaders_elected = self
            .servers
            .iter()
            .flatten()
            .map(|s| s.raft.stats().elections_won)
            .sum();
        self.stats.truncations = self
            .servers
            .iter()
            .flatten()
            .map(|s| s.raft.stats().truncations)
            .sum();
        self.stats.bad_messages = self
            .servers
            .iter()
            .flatten()
            .map(|s| s.stats().bad_messages)
            .sum();
        let net = self.world.net_stats();
        self.stats.messages_sent = net.sent;
        self.stats.messages_dropped = net.dropped_loss + net.dropped_partition;
        self.stats.linearizability_steps = lin.stats.steps;

        let trace = if self.cfg.trace_level > Level::Off {
            Some(self.world.trace.render())
        } else {
            None
        };

        RunOutcome {
            seed: self.cfg.seed,
            config: self.cfg,
            fingerprint: self.world.fingerprint(),
            report: self.report,
            verdict,
            stats: self.stats,
            incomplete: self.incomplete,
            history: self.history,
            trace,
        }
    }
}

/// A node's private random stream. Mixing in the incarnation stops a restarted
/// node from replaying the same election timeouts it used before it crashed,
/// which would make two nodes livelock in lockstep.
fn node_seed(seed: u64, node: NodeId, incarnation: u64) -> u64 {
    seed ^ (node.0 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ incarnation.wrapping_mul(0xD6E8_FEB8_6659_FD93)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_seeds_are_distinct_per_node_and_incarnation() {
        let a = node_seed(1, NodeId(0), 0);
        let b = node_seed(1, NodeId(1), 0);
        let c = node_seed(1, NodeId(0), 1);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
        // Deterministic, though.
        assert_eq!(a, node_seed(1, NodeId(0), 0));
    }
}
