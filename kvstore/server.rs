//! The replicated key-value store: a state machine on top of [`crate::raft`],
//! plus the client protocol.
//!
//! Two decisions here do most of the work of being linearizable:
//!
//! * **Reads go through the log.** A `Get` is proposed like any write and
//!   answered when it commits. That is slower than reading local state, and it
//!   is also the only version that is obviously correct: a leader that has been
//!   deposed but does not know it yet cannot serve a stale read, because its
//!   read never commits.
//! * **Every command carries `(client, seq)`, and the state machine keeps the
//!   last result per client.** A retry of the same `seq` is answered from that
//!   cache instead of being applied twice. Without it, a client that retries
//!   after a lost reply turns one logical `Put` into two applied `Put`s -- and
//!   with `Cas` in the mix, that is a lost update.
//!
//! An answer of [`Outcome::NotLeader`] or [`Outcome::Dropped`] is *not* a
//! failure -- it means "unknown, ask again". The request may still be sitting in
//! a log somewhere and may yet commit. Clients must retry such requests with the
//! same `seq`, which is what makes the dedup table meaningful.

use crate::codec::{Dec, DecodeError, Enc};
use crate::log::{Command, Op};
use crate::raft::{InjectedBug, Raft, RaftConfig, RaftMsg, Role};
use crate::Message;
use sim_io::{Io, TimerTag};
use simcore::disk::OpId;
use simcore::trace::Level;
use simcore::NodeId;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The value a `Get` observed (`None` = absent).
    Value(Option<String>),
    /// A `Put` or `Delete` was applied.
    Written,
    /// A `Cas` was applied; the flag says whether it matched.
    Cas(bool),
    /// Not the leader. Try `hint`, if there is one.
    NotLeader(Option<NodeId>),
    /// Accepted but never resolved here: leadership changed underneath it.
    /// The command may or may not eventually commit.
    Dropped,
}

impl Outcome {
    /// Whether this answers the question. Anything else means "retry".
    pub fn is_definite(&self) -> bool {
        matches!(
            self,
            Outcome::Value(_) | Outcome::Written | Outcome::Cas(_)
        )
    }

    fn encode_into(&self, e: &mut Enc) {
        match self {
            Outcome::Value(v) => {
                e.u8(0).opt_str(v.as_deref());
            }
            Outcome::Written => {
                e.u8(1);
            }
            Outcome::Cas(ok) => {
                e.u8(2).u8(*ok as u8);
            }
            Outcome::NotLeader(hint) => {
                e.u8(3).u32(hint.map_or(u32::MAX, |n| n.0));
            }
            Outcome::Dropped => {
                e.u8(4);
            }
        }
    }

    fn decode_from(d: &mut Dec) -> crate::codec::Result<Outcome> {
        Ok(match d.u8()? {
            0 => Outcome::Value(d.opt_string()?),
            1 => Outcome::Written,
            2 => Outcome::Cas(d.u8()? != 0),
            3 => {
                let n = d.u32()?;
                Outcome::NotLeader(if n == u32::MAX { None } else { Some(NodeId(n)) })
            }
            4 => Outcome::Dropped,
            t => return Err(DecodeError::BadTag(t)),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientMsg {
    Request {
        req_id: u64,
        client: u32,
        seq: u64,
        op: Op,
    },
    Reply {
        req_id: u64,
        outcome: Outcome,
    },
}

impl ClientMsg {
    pub fn encode_into(&self, e: &mut Enc) {
        match self {
            ClientMsg::Request {
                req_id,
                client,
                seq,
                op,
            } => {
                e.u8(1).u64(*req_id).u32(*client).u64(*seq);
                let cmd = Command {
                    client: *client,
                    seq: *seq,
                    op: op.clone(),
                };
                cmd.encode_into(e);
            }
            ClientMsg::Reply { req_id, outcome } => {
                e.u8(2).u64(*req_id);
                outcome.encode_into(e);
            }
        }
    }

    pub fn decode_from(d: &mut Dec) -> crate::codec::Result<ClientMsg> {
        Ok(match d.u8()? {
            1 => {
                let req_id = d.u64()?;
                let client = d.u32()?;
                let seq = d.u64()?;
                let cmd = Command::decode_from(d)?;
                ClientMsg::Request {
                    req_id,
                    client,
                    seq,
                    op: cmd.op,
                }
            }
            2 => ClientMsg::Reply {
                req_id: d.u64()?,
                outcome: Outcome::decode_from(d)?,
            },
            t => return Err(DecodeError::BadTag(t)),
        })
    }
}

/// The last result handed to a client, so a retry can be answered without
/// applying the command twice.
#[derive(Clone, Debug)]
struct Session {
    seq: u64,
    outcome: Outcome,
}

/// The deterministic state machine: a key-value map plus one remembered result
/// per client.
///
/// Kept separate from [`KvServer`] so it can be tested on its own -- and because
/// the linearizability checker needs a reference implementation of exactly
/// these semantics.
#[derive(Debug, Default)]
pub struct StateMachine {
    state: BTreeMap<String, String>,
    sessions: BTreeMap<u32, Session>,
    bug: InjectedBug,
}

impl StateMachine {
    pub fn new() -> StateMachine {
        StateMachine::default()
    }

    pub fn with_bug(bug: InjectedBug) -> StateMachine {
        StateMachine {
            bug,
            ..StateMachine::default()
        }
    }

    pub fn state(&self) -> &BTreeMap<String, String> {
        &self.state
    }

    /// The remembered result for `(client, seq)`, if this exact request has
    /// already been applied.
    pub fn cached(&self, client: u32, seq: u64) -> Option<Outcome> {
        if self.bug == InjectedBug::NoDedup {
            return None;
        }
        self.sessions
            .get(&client)
            .filter(|s| s.seq == seq)
            .map(|s| s.outcome.clone())
    }

    /// Run one committed command. Applying the same `(client, seq)` twice is
    /// a no-op that returns the original result.
    pub fn apply(&mut self, cmd: &Command) -> Outcome {
        if cmd.is_noop() {
            return Outcome::Written;
        }
        if let Some(s) = self.sessions.get(&cmd.client) {
            // The defect: without this, a retry that reaches the log twice is
            // applied twice, which turns one logical operation into two.
            if s.seq >= cmd.seq && self.bug != InjectedBug::NoDedup {
                // Already applied: a duplicate reached the log anyway (two
                // leaders, or a retry that raced its own original).
                return s.outcome.clone();
            }
        }
        let outcome = match &cmd.op {
            Op::Noop => Outcome::Written,
            Op::Get { key } => Outcome::Value(self.state.get(key).cloned()),
            Op::Put { key, value } => {
                self.state.insert(key.clone(), value.clone());
                Outcome::Written
            }
            Op::Delete { key } => {
                self.state.remove(key);
                Outcome::Written
            }
            Op::Cas { key, expect, value } => {
                if self.state.get(key) == expect.as_ref() {
                    self.state.insert(key.clone(), value.clone());
                    Outcome::Cas(true)
                } else {
                    Outcome::Cas(false)
                }
            }
        };
        self.sessions.insert(
            cmd.client,
            Session {
                seq: cmd.seq,
                outcome: outcome.clone(),
            },
        );
        outcome
    }
}

/// A client waiting on a log index.
#[derive(Clone, Debug)]
struct Waiting {
    to: NodeId,
    req_id: u64,
    client: u32,
    seq: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ServerStats {
    pub requests: u64,
    pub deduped: u64,
    pub redirected: u64,
    pub dropped: u64,
    pub applied: u64,
    pub bad_messages: u64,
}

pub struct KvServer {
    pub raft: Raft,
    sm: StateMachine,
    waiting: BTreeMap<u64, Waiting>,
    stats: ServerStats,
}

impl KvServer {
    pub fn recover(
        io: &mut dyn Io,
        id: NodeId,
        members: Vec<NodeId>,
        cfg: RaftConfig,
        seed: u64,
    ) -> KvServer {
        KvServer {
            raft: Raft::recover(io, id, members, cfg.clone(), seed),
            // The state machine is volatile and rebuilt by replaying the log as
            // entries are committed, exactly as Raft intends.
            sm: StateMachine::with_bug(cfg.bug),
            waiting: BTreeMap::new(),
            stats: ServerStats::default(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.raft.id()
    }

    pub fn state(&self) -> &BTreeMap<String, String> {
        self.sm.state()
    }

    pub fn stats(&self) -> ServerStats {
        self.stats
    }

    pub fn failed(&self) -> bool {
        self.raft.failed()
    }

    /// Deliver bytes from the network. Malformed input is counted and dropped:
    /// the network corrupts messages, and a node that panics on a bad byte is
    /// a node that can be killed by a cosmic ray.
    pub fn on_bytes(&mut self, io: &mut dyn Io, from: NodeId, bytes: &[u8]) {
        match Message::decode(bytes) {
            Ok(Message::Raft(m)) => self.on_raft(io, from, m),
            Ok(Message::Client(ClientMsg::Request {
                req_id,
                client,
                seq,
                op,
            })) => self.on_request(io, from, req_id, client, seq, op),
            Ok(Message::Client(ClientMsg::Reply { .. })) => {
                // Servers do not receive replies; ignore rather than trust.
                self.stats.bad_messages += 1;
            }
            Err(e) => {
                self.stats.bad_messages += 1;
                io.trace(Level::Warn, "wire", format!("dropping bad message from {from}: {e}"));
            }
        }
    }

    fn on_raft(&mut self, io: &mut dyn Io, from: NodeId, msg: RaftMsg) {
        self.raft.on_message(io, from, msg);
        self.drain(io);
    }

    pub fn on_timer(&mut self, io: &mut dyn Io, tag: TimerTag) {
        self.raft.on_timer(io, tag);
        self.drain(io);
    }

    pub fn on_storage(&mut self, io: &mut dyn Io, op: OpId, ok: bool) {
        self.raft.on_storage(io, op, ok);
        self.drain(io);
    }

    fn on_request(
        &mut self,
        io: &mut dyn Io,
        from: NodeId,
        req_id: u64,
        client: u32,
        seq: u64,
        op: Op,
    ) {
        self.stats.requests += 1;
        if !self.raft.is_leader() {
            self.stats.redirected += 1;
            self.reply(io, from, req_id, Outcome::NotLeader(self.raft.leader_hint()));
            return;
        }
        // A retry whose original already committed is answered from the session
        // cache. Re-proposing it would apply the command a second time.
        if let Some(outcome) = self.sm.cached(client, seq) {
            self.stats.deduped += 1;
            self.reply(io, from, req_id, outcome);
            return;
        }
        // Also deduplicate against a copy that is already in flight, so a
        // duplicated network message does not occupy two log slots.
        if let Some((_, w)) = self
            .waiting
            .iter_mut()
            .find(|(_, w)| w.client == client && w.seq == seq)
        {
            self.stats.deduped += 1;
            // Answer the newest copy of the request.
            w.to = from;
            w.req_id = req_id;
            return;
        }

        let cmd = Command { client, seq, op };
        match self.raft.propose(io, cmd) {
            Some(index) => {
                self.waiting.insert(
                    index,
                    Waiting {
                        to: from,
                        req_id,
                        client,
                        seq,
                    },
                );
            }
            None => {
                self.stats.redirected += 1;
                self.reply(io, from, req_id, Outcome::NotLeader(self.raft.leader_hint()));
            }
        }
        self.drain(io);
    }

    /// Apply everything Raft has committed, answer whoever was waiting, and
    /// release requests this node can no longer resolve.
    fn drain(&mut self, io: &mut dyn Io) {
        for entry in self.raft.take_applied() {
            let index = entry.index;
            let outcome = self.sm.apply(&entry.cmd);
            self.stats.applied += 1;
            if let Some(w) = self.waiting.remove(&index) {
                // The slot may have been filled by a different command from a
                // later leader. Only answer if this is genuinely our request.
                if w.client == entry.cmd.client && w.seq == entry.cmd.seq {
                    self.reply(io, w.to, w.req_id, outcome);
                } else {
                    self.stats.dropped += 1;
                    self.reply(io, w.to, w.req_id, Outcome::Dropped);
                }
            }
        }

        // Anything still waiting on an index at or below the applied point can
        // never be answered by this node.
        let applied = self.raft.last_applied();
        let stale: Vec<u64> = self
            .waiting
            .range(..=applied)
            .map(|(i, _)| *i)
            .collect();
        for i in stale {
            if let Some(w) = self.waiting.remove(&i) {
                self.stats.dropped += 1;
                self.reply(io, w.to, w.req_id, Outcome::Dropped);
            }
        }

        // Losing leadership means the pending proposals are no longer this
        // node's to resolve. Telling the client "unknown" promptly is better
        // than leaving it to time out, and it is safe: the retry carries the
        // same seq, so it commits at most once.
        if self.raft.role() != Role::Leader && !self.waiting.is_empty() {
            let pending: Vec<u64> = self.waiting.keys().copied().collect();
            for i in pending {
                if let Some(w) = self.waiting.remove(&i) {
                    self.stats.dropped += 1;
                    self.reply(io, w.to, w.req_id, Outcome::Dropped);
                }
            }
        }
    }

    fn reply(&mut self, io: &mut dyn Io, to: NodeId, req_id: u64, outcome: Outcome) {
        let bytes = Message::Client(ClientMsg::Reply { req_id, outcome }).encode();
        io.send(to, &bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(client: u32, seq: u64, op: Op) -> Command {
        Command { client, seq, op }
    }

    fn put(k: &str, v: &str) -> Op {
        Op::Put {
            key: k.into(),
            value: v.into(),
        }
    }

    fn get(k: &str) -> Op {
        Op::Get { key: k.into() }
    }

    fn cas(k: &str, expect: Option<&str>, v: &str) -> Op {
        Op::Cas {
            key: k.into(),
            expect: expect.map(str::to_string),
            value: v.into(),
        }
    }

    #[test]
    fn outcomes_round_trip() {
        let outcomes = vec![
            Outcome::Value(None),
            Outcome::Value(Some("v".into())),
            Outcome::Written,
            Outcome::Cas(true),
            Outcome::Cas(false),
            Outcome::NotLeader(None),
            Outcome::NotLeader(Some(NodeId(2))),
            Outcome::Dropped,
        ];
        for o in outcomes {
            let m = Message::Client(ClientMsg::Reply {
                req_id: 1,
                outcome: o.clone(),
            });
            assert_eq!(Message::decode(&m.encode()).unwrap(), m);
        }
    }

    #[test]
    fn requests_round_trip() {
        let m = Message::Client(ClientMsg::Request {
            req_id: 9,
            client: 4,
            seq: 77,
            op: cas("k", Some("old"), "new"),
        });
        assert_eq!(Message::decode(&m.encode()).unwrap(), m);
    }

    #[test]
    fn definite_outcomes_are_the_answers() {
        assert!(Outcome::Written.is_definite());
        assert!(Outcome::Value(None).is_definite());
        assert!(Outcome::Cas(false).is_definite());
        assert!(!Outcome::NotLeader(None).is_definite());
        assert!(!Outcome::Dropped.is_definite());
    }

    #[test]
    fn state_machine_applies_every_operation() {
        let mut m = StateMachine::new();
        assert_eq!(m.apply(&cmd(1, 1, get("k"))), Outcome::Value(None));
        assert_eq!(m.apply(&cmd(1, 2, put("k", "v1"))), Outcome::Written);
        assert_eq!(
            m.apply(&cmd(1, 3, get("k"))),
            Outcome::Value(Some("v1".into()))
        );
        assert_eq!(
            m.apply(&cmd(1, 4, cas("k", Some("wrong"), "v2"))),
            Outcome::Cas(false)
        );
        assert_eq!(m.state().get("k").map(String::as_str), Some("v1"));
        assert_eq!(
            m.apply(&cmd(1, 5, cas("k", Some("v1"), "v2"))),
            Outcome::Cas(true)
        );
        assert_eq!(
            m.apply(&cmd(1, 6, Op::Delete { key: "k".into() })),
            Outcome::Written
        );
        assert_eq!(m.apply(&cmd(1, 7, get("k"))), Outcome::Value(None));
        assert_eq!(
            m.apply(&cmd(1, 8, cas("k", None, "fresh"))),
            Outcome::Cas(true),
            "cas against absent must match None"
        );
    }

    #[test]
    fn a_duplicated_command_is_applied_exactly_once() {
        let mut m = StateMachine::new();
        m.apply(&cmd(1, 1, put("k", "a")));
        assert_eq!(
            m.apply(&cmd(1, 2, cas("k", Some("a"), "b"))),
            Outcome::Cas(true)
        );
        // Someone else resets the key.
        m.apply(&cmd(2, 1, put("k", "a")));
        // The same (client 1, seq 2) command reaches the log a second time.
        assert_eq!(
            m.apply(&cmd(1, 2, cas("k", Some("a"), "b"))),
            Outcome::Cas(true),
            "the remembered result is replayed"
        );
        assert_eq!(
            m.state().get("k").map(String::as_str),
            Some("a"),
            "the duplicate must not have been applied again"
        );
    }

    #[test]
    fn cached_results_answer_retries() {
        let mut m = StateMachine::new();
        assert_eq!(m.cached(1, 5), None);
        m.apply(&cmd(1, 5, put("k", "v")));
        assert_eq!(m.cached(1, 5), Some(Outcome::Written));
        assert_eq!(m.cached(1, 6), None, "only the latest seq is remembered");
        assert_eq!(m.cached(2, 5), None, "sessions are per client");
    }

    #[test]
    fn clients_do_not_share_dedup_state() {
        let mut m = StateMachine::new();
        m.apply(&cmd(1, 1, put("k", "one")));
        m.apply(&cmd(2, 1, put("k", "two")));
        assert_eq!(m.state().get("k").map(String::as_str), Some("two"));
    }

    #[test]
    fn noops_do_not_touch_sessions() {
        let mut m = StateMachine::new();
        assert_eq!(m.apply(&Command::noop()), Outcome::Written);
        assert_eq!(m.cached(u32::MAX, 0), None);
        assert!(m.state().is_empty());
    }
}
