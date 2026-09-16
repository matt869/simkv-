//! Simulated clients, and the history they record.
//!
//! Clients live on the network like anything else: their requests are delayed,
//! dropped, duplicated and misdelivered, and they only learn what the replies
//! tell them.
//!
//! The subtle part is what counts as *one operation*. A client that times out
//! and retries has not performed two operations -- it has performed one, whose
//! outcome it does not yet know. Every attempt carries the same `seq`, the
//! store deduplicates on it, and the history records a single operation whose
//! invocation is the first attempt and whose return is the definite answer.
//! Recording each attempt separately would report writes the store never made,
//! and the linearizability checker would reject a correct system.
//!
//! An operation the client gives up on stays in the history with no return at
//! all. That is the honest record: it may have happened, it may not, and the
//! checker is expected to consider both.

use checker::linearizability::{History, Operation};
use kvstore::log::Op;
use kvstore::server::{ClientMsg, Outcome};
use kvstore::Message;
use sim_io::{Io, TimerHandle, TimerTag};
use simcore::rng::Rng;
use simcore::trace::Level;
use simcore::{Nanos, NodeId, MILLIS};
use std::collections::BTreeMap;

pub const TIMER_REQUEST: TimerTag = 1;
pub const TIMER_THINK: TimerTag = 2;

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub read_weight: u32,
    pub write_weight: u32,
    pub cas_weight: u32,
    pub delete_weight: u32,
    /// Number of distinct keys. Fewer keys means more contention, which is
    /// where the interesting histories are.
    pub keys: usize,
    /// Pause between finishing one operation and starting the next.
    pub think_time: (Nanos, Nanos),
    /// How long to wait for a reply before trying another server.
    pub request_timeout: Nanos,
    /// Attempts before the client gives up and records an unknown outcome.
    pub max_attempts: u32,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            read_weight: 40,
            write_weight: 40,
            cas_weight: 15,
            delete_weight: 5,
            keys: 6,
            think_time: (MILLIS, 20 * MILLIS),
            request_timeout: 500 * MILLIS,
            max_attempts: 12,
        }
    }
}

#[derive(Clone, Debug)]
struct InFlight {
    /// Identifies this operation in the history.
    history_id: u64,
    /// Deduplication token: every retry reuses it.
    seq: u64,
    /// Distinguishes replies to this attempt from replies to earlier ones.
    req_id: u64,
    op: Op,
    attempts: u32,
    timer: Option<TimerHandle>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClientStats {
    pub started: u64,
    pub completed: u64,
    pub abandoned: u64,
    pub retries: u64,
    pub redirects: u64,
    pub stale_replies: u64,
}

pub struct Client {
    id: u32,
    node: NodeId,
    servers: Vec<NodeId>,
    /// Who this client currently believes is the leader.
    target: usize,
    seq: u64,
    next_req_id: u64,
    next_history_id: u64,
    inflight: Option<InFlight>,
    /// The last value this client believes each key holds, used to make `cas`
    /// attempts that sometimes succeed.
    known: BTreeMap<String, Option<String>>,
    rng: Rng,
    cfg: WorkloadConfig,
    stopped: bool,
    stats: ClientStats,
}

impl Client {
    pub fn new(
        id: u32,
        node: NodeId,
        servers: Vec<NodeId>,
        seed: u64,
        cfg: WorkloadConfig,
    ) -> Client {
        let mut rng = Rng::new(seed);
        let target = rng.below(servers.len() as u64) as usize;
        Client {
            id,
            node,
            servers,
            target,
            seq: 0,
            next_req_id: 1,
            next_history_id: 1,
            inflight: None,
            known: BTreeMap::new(),
            rng,
            cfg,
            stopped: false,
            stats: ClientStats::default(),
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn node(&self) -> NodeId {
        self.node
    }

    pub fn stats(&self) -> ClientStats {
        self.stats
    }

    pub fn is_idle(&self) -> bool {
        self.inflight.is_none()
    }

    /// Stop starting new operations. Anything in flight is still allowed to
    /// finish, so the history ends cleanly.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    pub fn resume(&mut self) {
        self.stopped = false;
    }

    /// Begin the next operation, unless one is already running.
    pub fn begin(&mut self, io: &mut dyn Io, history: &mut History, now: Nanos) {
        if self.stopped || self.inflight.is_some() {
            return;
        }
        let op = self.pick_op();
        self.seq += 1;
        let history_id = self.next_history_id;
        self.next_history_id += 1;
        self.stats.started += 1;
        history.push(Operation {
            id: (self.id as u64) << 32 | history_id,
            client: self.id,
            op: op.clone(),
            invoked: now,
            completed: None,
            outcome: None,
        });
        self.inflight = Some(InFlight {
            history_id,
            seq: self.seq,
            req_id: 0,
            op,
            attempts: 0,
            timer: None,
        });
        self.attempt(io);
    }

    /// Send (or resend) the current operation to the current target.
    fn attempt(&mut self, io: &mut dyn Io) {
        let Some(f) = self.inflight.as_mut() else {
            return;
        };
        f.attempts += 1;
        self.next_req_id += 1;
        f.req_id = self.next_req_id;
        let to = self.servers[self.target];
        let msg = Message::Client(ClientMsg::Request {
            req_id: f.req_id,
            client: self.id,
            seq: f.seq,
            op: f.op.clone(),
        });
        let timeout = self.cfg.request_timeout;
        let handle = io.set_timer(timeout, TIMER_REQUEST);
        f.timer = Some(handle);
        io.trace(
            Level::Debug,
            "client",
            format!(
                "c{} attempt {} of seq {} -> {to}: {:?}",
                self.id, f.attempts, f.seq, f.op
            ),
        );
        io.send(to, &msg.encode());
    }

    pub fn on_bytes(
        &mut self,
        io: &mut dyn Io,
        history: &mut History,
        now: Nanos,
        bytes: &[u8],
    ) {
        let Ok(Message::Client(ClientMsg::Reply { req_id, outcome })) = Message::decode(bytes)
        else {
            // Corrupted, or something a client has no business receiving.
            self.stats.stale_replies += 1;
            return;
        };
        let Some(f) = self.inflight.as_ref() else {
            self.stats.stale_replies += 1;
            return;
        };
        if f.req_id != req_id {
            // A reply to an attempt we already gave up on. Harmless, but it
            // must not be mistaken for an answer to the current attempt.
            self.stats.stale_replies += 1;
            return;
        }

        if outcome.is_definite() {
            self.finish(io, history, now, outcome);
            return;
        }

        // Not an answer: find another server and try again with the same seq.
        match outcome {
            Outcome::NotLeader(hint) => {
                self.stats.redirects += 1;
                match hint {
                    Some(n) => {
                        if let Some(i) = self.servers.iter().position(|s| *s == n) {
                            self.target = i;
                        }
                    }
                    None => self.rotate_target(),
                }
            }
            _ => self.rotate_target(),
        }
        self.retry(io, history, now);
    }

    pub fn on_timer(&mut self, io: &mut dyn Io, history: &mut History, now: Nanos, tag: TimerTag) {
        match tag {
            TIMER_REQUEST => {
                if self.inflight.is_some() {
                    // The server may be down, partitioned away, or simply slow.
                    self.rotate_target();
                    self.retry(io, history, now);
                }
            }
            TIMER_THINK => self.begin(io, history, now),
            _ => {}
        }
    }

    fn retry(&mut self, io: &mut dyn Io, history: &mut History, now: Nanos) {
        let Some(f) = self.inflight.as_mut() else {
            return;
        };
        if let Some(h) = f.timer.take() {
            io.cancel_timer(h);
        }
        if f.attempts >= self.cfg.max_attempts {
            // Give up. The operation stays in the history without a return:
            // it may have committed, and the checker has to allow for that.
            self.stats.abandoned += 1;
            io.trace(
                Level::Info,
                "client",
                format!("c{} abandoning seq {} after {} attempts", self.id, f.seq, f.attempts),
            );
            self.inflight = None;
            self.think(io, history, now);
            return;
        }
        self.stats.retries += 1;
        self.attempt(io);
    }

    fn finish(&mut self, io: &mut dyn Io, history: &mut History, now: Nanos, outcome: Outcome) {
        let Some(f) = self.inflight.take() else {
            return;
        };
        if let Some(h) = f.timer {
            io.cancel_timer(h);
        }
        self.stats.completed += 1;
        self.remember(&f.op, &outcome);
        let id = (self.id as u64) << 32 | f.history_id;
        debug_assert!(
            history.complete(id, now, outcome.clone()),
            "reply for an operation the history never recorded"
        );
        io.trace(
            Level::Debug,
            "client",
            format!("c{} seq {} => {outcome:?}", self.id, f.seq),
        );
        io.observe("client_done", &[self.id as u64, f.seq]);
        self.think(io, history, now);
    }

    fn think(&mut self, io: &mut dyn Io, history: &mut History, now: Nanos) {
        if self.stopped {
            return;
        }
        let delay = self.rng.range(self.cfg.think_time.0, self.cfg.think_time.1);
        if delay == 0 {
            self.begin(io, history, now);
        } else {
            io.set_timer(delay, TIMER_THINK);
        }
    }

    fn rotate_target(&mut self) {
        // Move to a different server, chosen at random rather than round-robin
        // so a whole fleet of clients does not stampede the same node.
        if self.servers.len() > 1 {
            let mut next = self.rng.below(self.servers.len() as u64) as usize;
            if next == self.target {
                next = (next + 1) % self.servers.len();
            }
            self.target = next;
        }
    }

    fn remember(&mut self, op: &Op, outcome: &Outcome) {
        match (op, outcome) {
            (Op::Get { key }, Outcome::Value(v)) => {
                self.known.insert(key.clone(), v.clone());
            }
            (Op::Put { key, value }, Outcome::Written) => {
                self.known.insert(key.clone(), Some(value.clone()));
            }
            (Op::Delete { key }, Outcome::Written) => {
                self.known.insert(key.clone(), None);
            }
            (Op::Cas { key, value, .. }, Outcome::Cas(true)) => {
                self.known.insert(key.clone(), Some(value.clone()));
            }
            (Op::Cas { key, .. }, Outcome::Cas(false)) => {
                // Our idea of the value was wrong; forget it and let a later
                // read refresh it.
                self.known.remove(key);
            }
            _ => {}
        }
    }

    fn pick_op(&mut self) -> Op {
        let key = format!("k{}", self.rng.below(self.cfg.keys as u64));
        let value = format!("c{}v{}", self.id, self.seq + 1);
        let c = &self.cfg;
        let total = c.read_weight + c.write_weight + c.cas_weight + c.delete_weight;
        let mut pick = self.rng.below(total.max(1) as u64) as u32;
        if pick < c.read_weight {
            return Op::Get { key };
        }
        pick -= c.read_weight;
        if pick < c.write_weight {
            return Op::Put { key, value };
        }
        pick -= c.write_weight;
        if pick < c.cas_weight {
            let expect = self.known.get(&key).cloned().unwrap_or(None);
            return Op::Cas { key, expect, value };
        }
        Op::Delete { key }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> WorkloadConfig {
        WorkloadConfig::default()
    }

    fn client(seed: u64) -> Client {
        Client::new(
            0,
            NodeId(3),
            vec![NodeId(0), NodeId(1), NodeId(2)],
            seed,
            cfg(),
        )
    }

    #[test]
    fn operations_cover_the_configured_mix() {
        let mut c = client(1);
        let mut reads = 0;
        let mut writes = 0;
        let mut cas = 0;
        let mut dels = 0;
        for _ in 0..4000 {
            match c.pick_op() {
                Op::Get { .. } => reads += 1,
                Op::Put { .. } => writes += 1,
                Op::Cas { .. } => cas += 1,
                Op::Delete { .. } => dels += 1,
                Op::Noop => panic!("clients never issue noops"),
            }
        }
        assert!(reads > 1200 && reads < 2000, "reads = {reads}");
        assert!(writes > 1200 && writes < 2000, "writes = {writes}");
        assert!(cas > 400 && cas < 800, "cas = {cas}");
        assert!(dels > 100 && dels < 350, "deletes = {dels}");
    }

    #[test]
    fn keys_stay_within_the_configured_space() {
        let mut c = client(2);
        for _ in 0..1000 {
            let op = c.pick_op();
            let key = op.key().unwrap();
            let n: usize = key.trim_start_matches('k').parse().unwrap();
            assert!(n < cfg().keys);
        }
    }

    #[test]
    fn cas_uses_the_last_known_value() {
        let mut c = client(3);
        c.remember(
            &Op::Put {
                key: "k1".into(),
                value: "v".into(),
            },
            &Outcome::Written,
        );
        assert_eq!(c.known.get("k1"), Some(&Some("v".to_string())));
        // A failed cas forgets, so the next attempt does not repeat a guess we
        // already know to be wrong.
        c.remember(
            &Op::Cas {
                key: "k1".into(),
                expect: Some("v".into()),
                value: "w".into(),
            },
            &Outcome::Cas(false),
        );
        assert_eq!(c.known.get("k1"), None);
    }

    #[test]
    fn a_successful_cas_updates_the_known_value() {
        let mut c = client(4);
        c.remember(
            &Op::Cas {
                key: "k".into(),
                expect: None,
                value: "new".into(),
            },
            &Outcome::Cas(true),
        );
        assert_eq!(c.known.get("k"), Some(&Some("new".to_string())));
    }

    #[test]
    fn a_delete_is_remembered_as_absent() {
        let mut c = client(5);
        c.remember(&Op::Delete { key: "k".into() }, &Outcome::Written);
        assert_eq!(c.known.get("k"), Some(&None));
    }

    #[test]
    fn rotate_target_always_moves() {
        let mut c = client(6);
        for _ in 0..200 {
            let before = c.target;
            c.rotate_target();
            assert_ne!(c.target, before);
            assert!(c.target < 3);
        }
    }

    #[test]
    fn a_single_server_target_cannot_rotate() {
        let mut c = Client::new(0, NodeId(1), vec![NodeId(0)], 1, cfg());
        c.rotate_target();
        assert_eq!(c.target, 0);
    }
}
