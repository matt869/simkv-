//! Raft, implemented against [`sim_io`] so it can be run inside the simulator.
//!
//! The interesting part of this file is not the state machine -- that follows
//! the paper -- but the *persistence discipline*, which is where real
//! implementations quietly go wrong:
//!
//! * A node never acts on a write until the `fsync` covering it has completed.
//!   Every reply that depends on durable state is deferred behind a batch of
//!   `write -> confirm -> sync -> confirm` completions.
//! * `currentTerm`, `votedFor` and the log all live in one file, so a single
//!   sync orders them relative to each other. Two files with two syncs would
//!   allow a crash to keep the log entry and lose the term that authorised it,
//!   which is enough to elect two leaders in one term.
//! * A write error is fatal. A node that cannot persist cannot participate,
//!   so it stops rather than answering from memory it may lose.
//!
//! Not implemented, deliberately: snapshots, log compaction, membership
//! changes, and lease-based reads. See the crate docs.

use crate::codec::{Dec, DecodeError, Enc};
use crate::log::{
    recover_hard_state, recover_log, Command, Entry, HardState, RaftLog, NO_INDEX, SLOT_SIZE,
    STATE_FILE_SIZE,
};
use crate::Message;
use sim_io::storage::FILE_WAL;
use sim_io::{Deadline, Io, PendingOps, TimerTag};
use simcore::disk::OpId;
use simcore::rng::Rng;
use simcore::trace::Level;
use simcore::{Nanos, NodeId, MILLIS};
use std::collections::{BTreeMap, BTreeSet};

/// The log begins after the two hard-state slots.
pub const LOG_REGION: usize = STATE_FILE_SIZE;

pub const TIMER_ELECTION: TimerTag = 1;
pub const TIMER_HEARTBEAT: TimerTag = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Election timeouts are drawn uniformly from this range. The spread has to
    /// be wide enough that two nodes rarely time out together, or elections
    /// livelock.
    pub election_timeout: (Nanos, Nanos),
    pub heartbeat_interval: Nanos,
    /// Maximum entries per AppendEntries.
    pub max_batch: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout: (300 * MILLIS, 600 * MILLIS),
            heartbeat_interval: 50 * MILLIS,
            max_batch: 64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftMsg {
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_index: u64,
        last_term: u64,
    },
    RequestVoteResp {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        /// Highest log index this node has made *durable*.
        match_index: u64,
        /// Where the leader should back up to on failure.
        conflict_index: u64,
    },
}

impl RaftMsg {
    pub fn term(&self) -> u64 {
        match self {
            RaftMsg::RequestVote { term, .. }
            | RaftMsg::RequestVoteResp { term, .. }
            | RaftMsg::AppendEntries { term, .. }
            | RaftMsg::AppendEntriesResp { term, .. } => *term,
        }
    }

    pub fn encode_into(&self, e: &mut Enc) {
        match self {
            RaftMsg::RequestVote {
                term,
                candidate,
                last_index,
                last_term,
            } => {
                e.u8(1).u64(*term).u32(candidate.0).u64(*last_index).u64(*last_term);
            }
            RaftMsg::RequestVoteResp { term, granted } => {
                e.u8(2).u64(*term).u8(*granted as u8);
            }
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
                e.u8(3)
                    .u64(*term)
                    .u32(leader.0)
                    .u64(*prev_index)
                    .u64(*prev_term)
                    .u64(*leader_commit)
                    .u32(entries.len() as u32);
                for entry in entries {
                    entry.encode_into(e);
                }
            }
            RaftMsg::AppendEntriesResp {
                term,
                success,
                match_index,
                conflict_index,
            } => {
                e.u8(4)
                    .u64(*term)
                    .u8(*success as u8)
                    .u64(*match_index)
                    .u64(*conflict_index);
            }
        }
    }

    pub fn decode_from(d: &mut Dec) -> crate::codec::Result<RaftMsg> {
        Ok(match d.u8()? {
            1 => RaftMsg::RequestVote {
                term: d.u64()?,
                candidate: NodeId(d.u32()?),
                last_index: d.u64()?,
                last_term: d.u64()?,
            },
            2 => RaftMsg::RequestVoteResp {
                term: d.u64()?,
                granted: d.u8()? != 0,
            },
            3 => {
                let term = d.u64()?;
                let leader = NodeId(d.u32()?);
                let prev_index = d.u64()?;
                let prev_term = d.u64()?;
                let leader_commit = d.u64()?;
                let count = d.u32()? as usize;
                // A corrupted count must not make us allocate wildly; entries
                // are bounded by what could plausibly be in one message.
                if count > 4096 {
                    return Err(DecodeError::TooLong);
                }
                let mut entries = Vec::with_capacity(count.min(64));
                for _ in 0..count {
                    entries.push(Entry::decode_from(d)?);
                }
                RaftMsg::AppendEntries {
                    term,
                    leader,
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit,
                }
            }
            4 => RaftMsg::AppendEntriesResp {
                term: d.u64()?,
                success: d.u8()? != 0,
                match_index: d.u64()?,
                conflict_index: d.u64()?,
            },
            t => return Err(DecodeError::BadTag(t)),
        })
    }
}

/// What to do once a batch of writes has been made durable.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Persist {
    /// Hard state is durable: this node may now campaign for `term`.
    Campaign { term: u64 },
    /// Hard state is durable: the vote decision may now be revealed.
    VoteReply { to: NodeId, term: u64, granted: bool },
    /// The log is durable through `index`.
    LogDurable {
        index: u64,
        reply_to: Option<NodeId>,
        reply_term: u64,
    },
    /// Hard state is durable; nothing to say about it.
    Quiet,
}

impl Persist {
    /// After a truncation, an in-flight batch can no longer vouch for indices
    /// beyond the truncation point: those bytes describe entries that are no
    /// longer in the log.
    fn clamp(&mut self, cap: u64) {
        if let Persist::LogDurable { index, .. } = self {
            *index = (*index).min(cap);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpKind {
    Write,
    Sync,
}

#[derive(Clone, Debug)]
struct Batch {
    id: u64,
    writes_outstanding: usize,
    sync_issued: bool,
    action: Persist,
}

/// Sequences `write -> confirm -> fsync -> confirm` for a group of writes that
/// must become durable together.
///
/// Issuing the `fsync` only after every write has been confirmed is not
/// pedantry: a write that fails after the sync was issued would otherwise be
/// reported as durable, and the node would answer for data it never wrote.
#[derive(Debug, Default)]
struct Persister {
    next_id: u64,
    batches: Vec<Batch>,
    ops: PendingOps<(u64, OpKind)>,
}

#[derive(Debug, PartialEq, Eq)]
enum BatchEvent {
    Nothing,
    NeedSync(u64),
    Done(Persist),
    Failed,
}

impl Persister {
    fn start(&mut self, action: Persist) -> u64 {
        self.next_id += 1;
        self.batches.push(Batch {
            id: self.next_id,
            writes_outstanding: 0,
            sync_issued: false,
            action,
        });
        self.next_id
    }

    fn batch(&mut self, id: u64) -> Option<&mut Batch> {
        self.batches.iter_mut().find(|b| b.id == id)
    }

    fn note_write(&mut self, id: u64, op: OpId) {
        if let Some(b) = self.batch(id) {
            b.writes_outstanding += 1;
        }
        self.ops.insert(op, (id, OpKind::Write));
    }

    fn note_sync(&mut self, id: u64, op: OpId) {
        if let Some(b) = self.batch(id) {
            b.sync_issued = true;
        }
        self.ops.insert(op, (id, OpKind::Sync));
    }

    fn on_complete(&mut self, op: OpId, ok: bool) -> BatchEvent {
        let Some((id, kind)) = self.ops.take(op) else {
            return BatchEvent::Nothing;
        };
        if !ok {
            self.batches.retain(|b| b.id != id);
            return BatchEvent::Failed;
        }
        match kind {
            OpKind::Write => {
                let Some(b) = self.batch(id) else {
                    return BatchEvent::Nothing;
                };
                b.writes_outstanding -= 1;
                if b.writes_outstanding == 0 && !b.sync_issued {
                    BatchEvent::NeedSync(id)
                } else {
                    BatchEvent::Nothing
                }
            }
            OpKind::Sync => {
                let Some(i) = self.batches.iter().position(|b| b.id == id) else {
                    return BatchEvent::Nothing;
                };
                BatchEvent::Done(self.batches.remove(i).action)
            }
        }
    }

    fn clamp_all(&mut self, cap: u64) {
        for b in &mut self.batches {
            b.action.clamp(cap);
        }
    }

    fn in_flight(&self) -> usize {
        self.batches.len()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RaftStats {
    pub elections_started: u64,
    pub elections_won: u64,
    pub appends_sent: u64,
    pub entries_replicated: u64,
    pub truncations: u64,
    pub step_downs: u64,
    pub bad_messages: u64,
}

pub struct Raft {
    id: NodeId,
    members: Vec<NodeId>,
    cfg: RaftConfig,

    role: Role,
    term: u64,
    voted_for: Option<NodeId>,
    log: RaftLog,
    commit_index: u64,
    last_applied: u64,
    leader: Option<NodeId>,

    votes: BTreeSet<NodeId>,
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,

    /// Highest log index this node knows to be on stable storage.
    durable_index: u64,
    /// Sequence number for the next hard-state slot write.
    state_seq: u64,

    persister: Persister,
    election_timer: Deadline,
    heartbeat_timer: Deadline,
    rng: Rng,
    failed: bool,
    stats: RaftStats,
}

impl Raft {
    /// Start (or restart) a node, recovering whatever is on its disk.
    pub fn recover(io: &mut dyn Io, id: NodeId, members: Vec<NodeId>, cfg: RaftConfig, seed: u64) -> Raft {
        assert!(members.contains(&id), "a node must be a member of its cluster");
        let bytes = io.read_all(FILE_WAL);
        let hs = recover_hard_state(&bytes);
        let log_bytes = if bytes.len() > LOG_REGION {
            &bytes[LOG_REGION..]
        } else {
            &[][..]
        };
        let recovered = recover_log(log_bytes);
        let mut entries = recovered.entries;

        // Entries whose term exceeds the durable term were never acknowledged:
        // acknowledging one would have required the sync that also made the
        // term durable. Dropping them keeps "durable log implies durable term",
        // which is what stops two leaders sharing a term after a crash.
        let dropped_ahead_of_term = entries.iter().filter(|e| e.term > hs.term).count();
        entries.retain(|e| e.term <= hs.term);

        let log = RaftLog::from_entries(entries);
        let damaged = recovered.damaged || dropped_ahead_of_term > 0;
        if damaged {
            // Cut the file back to the prefix we trust, so later appends do not
            // land after garbage.
            io.set_len(FILE_WAL, LOG_REGION + log.byte_len());
            io.sync(FILE_WAL);
        }

        let durable_index = log.last_index();
        let mut raft = Raft {
            id,
            members,
            cfg,
            role: Role::Follower,
            term: hs.term,
            voted_for: hs.voted_for,
            log,
            commit_index: NO_INDEX,
            last_applied: NO_INDEX,
            leader: None,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            durable_index,
            state_seq: hs.seq + 1,
            persister: Persister::default(),
            election_timer: Deadline::new(),
            heartbeat_timer: Deadline::new(),
            rng: Rng::new(seed),
            failed: false,
            stats: RaftStats::default(),
        };
        raft.observe_state(io, "recover");
        io.trace(
            Level::Info,
            "raft",
            format!(
                "recovered term={} vote={:?} log={} durable={} damaged={}",
                raft.term,
                raft.voted_for.map(|n| n.0),
                raft.log.last_index(),
                durable_index,
                damaged
            ),
        );
        raft.reset_election_timer(io);
        raft
    }

    // ---- accessors --------------------------------------------------------

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn log(&self) -> &RaftLog {
        &self.log
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }
    pub fn durable_index(&self) -> u64 {
        self.durable_index
    }
    pub fn leader_hint(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// A node that hit an unrecoverable storage error. The driver is expected
    /// to take it down.
    pub fn failed(&self) -> bool {
        self.failed
    }
    pub fn stats(&self) -> RaftStats {
        self.stats
    }
    pub fn persists_in_flight(&self) -> usize {
        self.persister.in_flight()
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }

    fn peers(&self) -> Vec<NodeId> {
        self.members.iter().copied().filter(|n| *n != self.id).collect()
    }

    // ---- event entry points ----------------------------------------------

    pub fn on_message(&mut self, io: &mut dyn Io, from: NodeId, msg: RaftMsg) {
        if self.failed {
            return;
        }
        match msg {
            RaftMsg::RequestVote {
                term,
                candidate,
                last_index,
                last_term,
            } => self.on_request_vote(io, from, term, candidate, last_index, last_term),
            RaftMsg::RequestVoteResp { term, granted } => self.on_vote_resp(io, from, term, granted),
            RaftMsg::AppendEntries {
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                io,
                from,
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            ),
            RaftMsg::AppendEntriesResp {
                term,
                success,
                match_index,
                conflict_index,
            } => self.on_append_resp(io, from, term, success, match_index, conflict_index),
        }
    }

    pub fn on_timer(&mut self, io: &mut dyn Io, tag: TimerTag) {
        if self.failed {
            return;
        }
        match tag {
            TIMER_ELECTION => {
                if !self.election_timer.fired() {
                    return; // a cancelled timer that arrived anyway
                }
                if self.role == Role::Leader {
                    return;
                }
                self.become_candidate(io);
            }
            TIMER_HEARTBEAT => {
                if !self.heartbeat_timer.fired() {
                    return;
                }
                if self.role != Role::Leader {
                    return;
                }
                self.broadcast_append(io);
                self.reset_heartbeat_timer(io);
            }
            _ => {}
        }
    }

    pub fn on_storage(&mut self, io: &mut dyn Io, op: OpId, ok: bool) {
        if self.failed {
            return;
        }
        match self.persister.on_complete(op, ok) {
            BatchEvent::Nothing => {}
            BatchEvent::Failed => {
                self.failed = true;
                io.trace(Level::Error, "raft", "storage failure; node is going down".into());
                io.observe("io_failed", &[]);
            }
            BatchEvent::NeedSync(id) => {
                let sync_op = io.sync(FILE_WAL);
                self.persister.note_sync(id, sync_op);
            }
            BatchEvent::Done(action) => self.on_durable(io, action),
        }
    }

    /// Append a client command. Returns the log index it was assigned, or
    /// `None` if this node is not the leader.
    pub fn propose(&mut self, io: &mut dyn Io, cmd: Command) -> Option<u64> {
        if self.role != Role::Leader || self.failed {
            return None;
        }
        let index = self.log.last_index() + 1;
        self.log.append(Entry {
            term: self.term,
            index,
            cmd,
        });
        self.persist_log_from(io, index, false, None, 0);
        self.broadcast_append(io);
        Some(index)
    }

    /// Entries that have been committed and not yet handed to the state
    /// machine.
    pub fn take_applied(&mut self) -> Vec<Entry> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            match self.log.get(next) {
                Some(e) => {
                    out.push(e.clone());
                    self.last_applied = next;
                }
                None => break,
            }
        }
        out
    }

    // ---- elections --------------------------------------------------------

    fn become_candidate(&mut self, io: &mut dyn Io) {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.stats.elections_started += 1;
        io.observe("campaign", &[self.term]);
        io.trace(
            Level::Info,
            "raft",
            format!("standing for election in term {}", self.term),
        );
        // The vote for itself has to be on disk before it is cast, or a crash
        // could let this node vote twice in one term.
        let batch = self.persister.start(Persist::Campaign { term: self.term });
        self.write_hard_state(io, batch);
        self.reset_election_timer(io);
    }

    fn on_request_vote(
        &mut self,
        io: &mut dyn Io,
        from: NodeId,
        term: u64,
        candidate: NodeId,
        last_index: u64,
        last_term: u64,
    ) {
        if term < self.term {
            self.send(
                io,
                from,
                RaftMsg::RequestVoteResp {
                    term: self.term,
                    granted: false,
                },
            );
            return;
        }
        let mut dirty = false;
        if term > self.term {
            self.step_down(io, term);
            dirty = true;
        }
        let free_to_vote = self.voted_for.is_none() || self.voted_for == Some(candidate);
        let up_to_date = self.log.is_up_to_date(last_index, last_term);
        let granted = free_to_vote && up_to_date;
        if granted && self.voted_for != Some(candidate) {
            self.voted_for = Some(candidate);
            dirty = true;
        }
        if granted {
            // Only a granted vote earns the candidate a stay of execution.
            self.reset_election_timer(io);
        }
        io.trace(
            Level::Debug,
            "raft",
            format!(
                "vote request from {candidate} term={term} granted={granted} (up_to_date={up_to_date}, free={free_to_vote})"
            ),
        );

        if dirty {
            let batch = self.persister.start(Persist::VoteReply {
                to: from,
                term: self.term,
                granted,
            });
            self.write_hard_state(io, batch);
        } else {
            self.send(
                io,
                from,
                RaftMsg::RequestVoteResp {
                    term: self.term,
                    granted,
                },
            );
        }
    }

    fn on_vote_resp(&mut self, io: &mut dyn Io, from: NodeId, term: u64, granted: bool) {
        if term > self.term {
            self.step_down_and_persist(io, term);
            return;
        }
        if self.role != Role::Candidate || term != self.term {
            return;
        }
        if granted {
            self.votes.insert(from);
            if self.votes.len() >= self.quorum() {
                self.become_leader(io);
            }
        }
    }

    fn become_leader(&mut self, io: &mut dyn Io) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.stats.elections_won += 1;
        let next = self.log.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for p in self.peers() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, NO_INDEX);
        }
        io.observe("leader", &[self.term, self.log.last_index()]);
        io.trace(
            Level::Info,
            "raft",
            format!("elected leader for term {} with {} votes", self.term, self.votes.len()),
        );

        // A no-op from the new term. Without it, entries carried over from a
        // previous term could never reach the commit threshold, because Raft
        // only commits by counting replicas of a *current-term* entry.
        let index = self.log.last_index() + 1;
        self.log.append(Entry {
            term: self.term,
            index,
            cmd: Command::noop(),
        });
        self.persist_log_from(io, index, false, None, 0);

        self.election_timer.disarm(io);
        self.broadcast_append(io);
        self.reset_heartbeat_timer(io);
    }

    fn step_down(&mut self, io: &mut dyn Io, term: u64) {
        debug_assert!(term > self.term);
        if self.role != Role::Follower {
            self.stats.step_downs += 1;
            io.trace(
                Level::Info,
                "raft",
                format!("stepping down: saw term {term} while {}", self.role.name()),
            );
        }
        self.term = term;
        self.voted_for = None;
        self.role = Role::Follower;
        self.leader = None;
        self.votes.clear();
        self.heartbeat_timer.disarm(io);
        self.reset_election_timer(io);
    }

    fn step_down_and_persist(&mut self, io: &mut dyn Io, term: u64) {
        self.step_down(io, term);
        let batch = self.persister.start(Persist::Quiet);
        self.write_hard_state(io, batch);
    }

    // ---- replication ------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        io: &mut dyn Io,
        from: NodeId,
        term: u64,
        leader: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
    ) {
        if term < self.term {
            self.send(
                io,
                from,
                RaftMsg::AppendEntriesResp {
                    term: self.term,
                    success: false,
                    match_index: NO_INDEX,
                    conflict_index: NO_INDEX,
                },
            );
            return;
        }
        let mut dirty = false;
        if term > self.term {
            self.step_down(io, term);
            dirty = true;
        }
        if self.role != Role::Follower {
            // A candidate that hears from a leader of its own term concedes.
            self.role = Role::Follower;
            self.votes.clear();
            self.heartbeat_timer.disarm(io);
        }
        self.leader = Some(leader);
        self.reset_election_timer(io);

        if !self.log.matches(prev_index, prev_term) {
            let conflict = self.conflict_hint(prev_index);
            let reply = RaftMsg::AppendEntriesResp {
                term: self.term,
                success: false,
                match_index: self.durable_index,
                conflict_index: conflict,
            };
            io.trace(
                Level::Debug,
                "raft",
                format!(
                    "rejecting append prev=({prev_index},{prev_term}); my last=({},{}) hint={conflict}",
                    self.log.last_index(),
                    self.log.last_term()
                ),
            );
            if dirty {
                let batch = self.persister.start(Persist::Quiet);
                self.write_hard_state(io, batch);
            }
            self.send(io, from, reply);
            return;
        }

        // Find the first entry that is genuinely new or conflicting. Entries we
        // already have are skipped rather than rewritten: a delayed duplicate
        // must never truncate a log that has moved on.
        let mut first_new: Option<usize> = None;
        let mut conflict_at: Option<u64> = None;
        for (i, e) in entries.iter().enumerate() {
            let idx = prev_index + 1 + i as u64;
            match self.log.term_at(idx) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    conflict_at = Some(idx);
                    first_new = Some(i);
                    break;
                }
                None => {
                    first_new = Some(i);
                    break;
                }
            }
        }

        let last_new_index = prev_index + entries.len() as u64;
        if let Some(i) = first_new {
            let start = prev_index + 1 + i as u64;
            if let Some(at) = conflict_at {
                self.truncate(io, at);
            }
            for e in &entries[i..] {
                self.log.append(e.clone());
            }
            self.stats.entries_replicated += (entries.len() - i) as u64;
            if dirty {
                // Same batch as the entries: one sync orders the term update
                // ahead of the entries it authorises.
                let batch = self.persister.start(Persist::LogDurable {
                    index: self.log.last_index(),
                    reply_to: Some(from),
                    reply_term: self.term,
                });
                self.write_hard_state(io, batch);
                self.write_log_from(io, batch, start, conflict_at.is_some());
            } else {
                self.persist_log_from(io, start, conflict_at.is_some(), Some(from), self.term);
            }
        } else if dirty {
            let batch = self.persister.start(Persist::LogDurable {
                index: self.durable_index,
                reply_to: Some(from),
                reply_term: self.term,
            });
            self.write_hard_state(io, batch);
        } else {
            // Nothing new to persist: answer with what is already durable.
            self.send(
                io,
                from,
                RaftMsg::AppendEntriesResp {
                    term: self.term,
                    success: true,
                    match_index: self.durable_index,
                    conflict_index: NO_INDEX,
                },
            );
        }

        if leader_commit > self.commit_index {
            let new_commit = leader_commit.min(last_new_index).min(self.log.last_index());
            if new_commit > self.commit_index {
                self.commit_index = new_commit;
                io.observe("commit", &[self.commit_index]);
            }
        }
    }

    /// Where the leader should back up to, given that `prev_index` did not
    /// match. Skipping the whole conflicting term at once turns a linear walk
    /// into one round trip per term.
    fn conflict_hint(&self, prev_index: u64) -> u64 {
        if self.log.last_index() < prev_index {
            return self.log.last_index() + 1;
        }
        match self.log.term_at(prev_index) {
            None => self.log.last_index() + 1,
            Some(t) => {
                let mut i = prev_index;
                while i > 1 && self.log.term_at(i - 1) == Some(t) {
                    i -= 1;
                }
                i
            }
        }
    }

    fn truncate(&mut self, io: &mut dyn Io, at: u64) {
        debug_assert!(
            at > self.commit_index,
            "committed entries must never be truncated: at={at} commit={}",
            self.commit_index
        );
        self.stats.truncations += 1;
        io.trace(
            Level::Info,
            "raft",
            format!("truncating log from index {at} (had {})", self.log.last_index()),
        );
        io.observe("truncate", &[at]);
        self.log.truncate_from(at);
        let cap = at - 1;
        self.durable_index = self.durable_index.min(cap);
        self.persister.clamp_all(cap);
    }

    fn on_append_resp(
        &mut self,
        io: &mut dyn Io,
        from: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
    ) {
        if term > self.term {
            self.step_down_and_persist(io, term);
            return;
        }
        if self.role != Role::Leader || term != self.term {
            return;
        }
        if success {
            let m = self.match_index.entry(from).or_insert(NO_INDEX);
            // Responses can arrive out of order; match_index only ever grows.
            *m = (*m).max(match_index);
            let m = *m;
            self.next_index.insert(from, m + 1);
            self.maybe_commit(io);
            // If the follower is still behind, keep the pipeline moving rather
            // than waiting for the next heartbeat.
            if m < self.log.last_index() {
                self.send_append_to(io, from);
            }
        } else {
            let cur = self.next_index.get(&from).copied().unwrap_or(1);
            let hint = conflict_index.max(1);
            let next = hint.min(cur.saturating_sub(1)).max(1);
            self.next_index.insert(from, next);
            self.send_append_to(io, from);
        }
    }

    fn maybe_commit(&mut self, io: &mut dyn Io) {
        let mut indices: Vec<u64> = self
            .members
            .iter()
            .map(|m| {
                if *m == self.id {
                    self.durable_index
                } else {
                    self.match_index.get(m).copied().unwrap_or(NO_INDEX)
                }
            })
            .collect();
        indices.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = indices[self.quorum() - 1];
        // Raft's commit rule: a leader may only commit by counting replicas of
        // an entry from its *own* term. Counting an older entry's replicas can
        // commit something a later leader will overwrite.
        if candidate > self.commit_index && self.log.term_at(candidate) == Some(self.term) {
            self.commit_index = candidate;
            io.observe("commit", &[self.commit_index]);
            io.trace(
                Level::Debug,
                "raft",
                format!("commit index advanced to {}", self.commit_index),
            );
        }
    }

    fn broadcast_append(&mut self, io: &mut dyn Io) {
        for p in self.peers() {
            self.send_append_to(io, p);
        }
    }

    fn send_append_to(&mut self, io: &mut dyn Io, to: NodeId) {
        let next = self.next_index.get(&to).copied().unwrap_or(1).max(1);
        let prev_index = next - 1;
        let Some(prev_term) = self.log.term_at(prev_index) else {
            // Only reachable with log compaction, which this build does not do.
            return;
        };
        let entries = self.log.slice_from(next, self.cfg.max_batch);
        self.stats.appends_sent += 1;
        let msg = RaftMsg::AppendEntries {
            term: self.term,
            leader: self.id,
            prev_index,
            prev_term,
            entries,
            leader_commit: self.commit_index,
        };
        self.send(io, to, msg);
    }

    // ---- persistence ------------------------------------------------------

    fn write_hard_state(&mut self, io: &mut dyn Io, batch: u64) {
        let hs = HardState {
            term: self.term,
            voted_for: self.voted_for,
            seq: self.state_seq,
        };
        let offset = HardState::slot_offset(hs.seq);
        self.state_seq += 1;
        let op = io.write_at(FILE_WAL, offset, &hs.to_slot());
        self.persister.note_write(batch, op);
    }

    fn persist_log_from(
        &mut self,
        io: &mut dyn Io,
        from: u64,
        truncated: bool,
        reply_to: Option<NodeId>,
        reply_term: u64,
    ) {
        let batch = self.persister.start(Persist::LogDurable {
            index: self.log.last_index(),
            reply_to,
            reply_term,
        });
        self.write_log_from(io, batch, from, truncated);
    }

    fn write_log_from(&mut self, io: &mut dyn Io, batch: u64, from: u64, truncated: bool) {
        let offset = LOG_REGION + self.log.byte_offset_of(from);
        if truncated {
            // Drop the stale suffix first so a crash cannot leave a longer,
            // half-overwritten log behind.
            let op = io.set_len(FILE_WAL, offset);
            self.persister.note_write(batch, op);
        }
        let mut bytes = Vec::new();
        for e in self.log.entries().iter().filter(|e| e.index >= from) {
            bytes.extend_from_slice(&e.to_record());
        }
        if bytes.is_empty() {
            // Nothing to write, but the batch still needs one operation so its
            // completion path fires; a zero-length set_len at the end of the
            // file is a harmless no-op that keeps the sequencing uniform.
            let op = io.set_len(FILE_WAL, offset);
            self.persister.note_write(batch, op);
            return;
        }
        let op = io.write_at(FILE_WAL, offset, &bytes);
        self.persister.note_write(batch, op);
    }

    fn on_durable(&mut self, io: &mut dyn Io, action: Persist) {
        match action {
            Persist::Campaign { term } => {
                if self.term == term && self.role == Role::Candidate {
                    let msg = RaftMsg::RequestVote {
                        term,
                        candidate: self.id,
                        last_index: self.log.last_index(),
                        last_term: self.log.last_term(),
                    };
                    for p in self.peers() {
                        self.send(io, p, msg.clone());
                    }
                    // A single-node cluster elects itself here.
                    if self.votes.len() >= self.quorum() {
                        self.become_leader(io);
                    }
                }
            }
            Persist::VoteReply { to, term, granted } => {
                self.send(io, to, RaftMsg::RequestVoteResp { term, granted });
            }
            Persist::LogDurable {
                index,
                reply_to,
                reply_term,
            } => {
                let index = index.min(self.log.last_index());
                if index > self.durable_index {
                    self.durable_index = index;
                    io.observe("durable", &[index]);
                }
                if let Some(to) = reply_to {
                    // Only claim what is durable *and* still ours: a truncation
                    // may have overtaken this batch.
                    self.send(
                        io,
                        to,
                        RaftMsg::AppendEntriesResp {
                            term: reply_term,
                            success: true,
                            match_index: self.durable_index,
                            conflict_index: NO_INDEX,
                        },
                    );
                }
                if self.role == Role::Leader {
                    self.maybe_commit(io);
                }
            }
            Persist::Quiet => {}
        }
    }

    // ---- plumbing ---------------------------------------------------------

    fn send(&mut self, io: &mut dyn Io, to: NodeId, msg: RaftMsg) {
        let bytes = Message::Raft(msg).encode();
        io.send(to, &bytes);
    }

    fn reset_election_timer(&mut self, io: &mut dyn Io) {
        let d = self
            .rng
            .range(self.cfg.election_timeout.0, self.cfg.election_timeout.1);
        self.election_timer.reset(io, d, TIMER_ELECTION);
    }

    fn reset_heartbeat_timer(&mut self, io: &mut dyn Io) {
        self.heartbeat_timer
            .reset(io, self.cfg.heartbeat_interval, TIMER_HEARTBEAT);
    }

    fn observe_state(&mut self, io: &mut dyn Io, tag: &'static str) {
        let args = [self.term, self.log.last_index(), self.durable_index];
        io.observe(tag, &args);
    }

    /// Bytes of the state region as they should appear on disk, for tests.
    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            voted_for: self.voted_for,
            seq: self.state_seq.saturating_sub(1),
        }
    }
}

/// Size of one hard-state slot, re-exported for the durability checker.
pub const HARD_STATE_SLOT: usize = SLOT_SIZE;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Op;

    #[test]
    fn messages_round_trip() {
        let msgs = vec![
            RaftMsg::RequestVote {
                term: 7,
                candidate: NodeId(3),
                last_index: 12,
                last_term: 5,
            },
            RaftMsg::RequestVoteResp {
                term: 7,
                granted: true,
            },
            RaftMsg::AppendEntries {
                term: 9,
                leader: NodeId(1),
                prev_index: 4,
                prev_term: 8,
                entries: vec![Entry {
                    term: 9,
                    index: 5,
                    cmd: Command {
                        client: 2,
                        seq: 1,
                        op: Op::Put {
                            key: "a".into(),
                            value: "b".into(),
                        },
                    },
                }],
                leader_commit: 3,
            },
            RaftMsg::AppendEntriesResp {
                term: 9,
                success: false,
                match_index: 4,
                conflict_index: 2,
            },
        ];
        for m in msgs {
            let wire = Message::Raft(m.clone()).encode();
            assert_eq!(Message::decode(&wire).unwrap(), Message::Raft(m));
        }
    }

    #[test]
    fn absurd_entry_counts_are_rejected() {
        let mut e = Enc::new();
        e.u8(3).u64(1).u32(0).u64(0).u64(0).u64(0).u32(u32::MAX);
        let payload = e.into_vec();
        let mut d = Dec::new(&payload);
        assert_eq!(RaftMsg::decode_from(&mut d), Err(DecodeError::TooLong));
    }

    #[test]
    fn persister_sequences_write_then_sync() {
        let mut p = Persister::default();
        let b = p.start(Persist::Quiet);
        p.note_write(b, 1);
        p.note_write(b, 2);
        assert_eq!(p.on_complete(1, true), BatchEvent::Nothing);
        assert_eq!(
            p.on_complete(2, true),
            BatchEvent::NeedSync(b),
            "the sync waits for every write to be confirmed"
        );
        p.note_sync(b, 3);
        assert_eq!(p.on_complete(3, true), BatchEvent::Done(Persist::Quiet));
        assert_eq!(p.in_flight(), 0);
    }

    #[test]
    fn a_failed_write_kills_the_batch() {
        let mut p = Persister::default();
        let b = p.start(Persist::Quiet);
        p.note_write(b, 1);
        assert_eq!(p.on_complete(1, false), BatchEvent::Failed);
        assert_eq!(p.in_flight(), 0);
    }

    #[test]
    fn batches_do_not_interfere() {
        let mut p = Persister::default();
        let a = p.start(Persist::Campaign { term: 1 });
        let b = p.start(Persist::Quiet);
        p.note_write(a, 10);
        p.note_write(b, 20);
        assert_eq!(p.on_complete(20, true), BatchEvent::NeedSync(b));
        assert_eq!(p.on_complete(10, true), BatchEvent::NeedSync(a));
        p.note_sync(b, 21);
        p.note_sync(a, 11);
        assert_eq!(p.on_complete(11, true), BatchEvent::Done(Persist::Campaign { term: 1 }));
        assert_eq!(p.on_complete(21, true), BatchEvent::Done(Persist::Quiet));
    }

    #[test]
    fn truncation_clamps_in_flight_durability_claims() {
        let mut p = Persister::default();
        let b = p.start(Persist::LogDurable {
            index: 10,
            reply_to: None,
            reply_term: 0,
        });
        p.note_write(b, 1);
        p.clamp_all(6);
        p.on_complete(1, true);
        p.note_sync(b, 2);
        assert_eq!(
            p.on_complete(2, true),
            BatchEvent::Done(Persist::LogDurable {
                index: 6,
                reply_to: None,
                reply_term: 0
            })
        );
    }

    #[test]
    fn unknown_ops_are_ignored() {
        let mut p = Persister::default();
        assert_eq!(p.on_complete(999, true), BatchEvent::Nothing);
    }
}
