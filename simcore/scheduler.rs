//! The discrete-event scheduler: the simulator's only source of time.
//!
//! There are no threads and no wall clock. Everything that will ever happen is
//! an [`Event`] sitting in a priority queue keyed by `(time, insertion
//! sequence)`. Ties break by insertion order, never by heap shape, so two runs
//! with the same seed pop events in exactly the same order.
//!
//! Virtual time also means a simulated hour costs microseconds of real time,
//! which is what makes wide seed sweeps affordable.

use crate::disk::{IoResult, OpId};
use crate::faults::FaultAction;
use crate::{Nanos, NodeId};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};

pub type EventId = u64;

/// Everything that can happen in the world.
#[derive(Clone, Debug)]
pub enum Event {
    /// A timer set by a node has expired.
    Timer { node: NodeId, timer: u64 },
    /// A network message arrives at its destination.
    Deliver {
        from: NodeId,
        to: NodeId,
        payload: Vec<u8>,
        msg: u64,
    },
    /// A storage operation has completed (and only now may be acted upon).
    Storage {
        node: NodeId,
        op: OpId,
        result: IoResult,
    },
    /// The fault injector wants to do something to the cluster.
    Fault(FaultAction),
    /// Reserved for the layer driving the simulation (clients, workload ticks).
    App { tag: u32, arg: u64 },
}

impl Event {
    /// Short label used in traces and fingerprints.
    pub fn tag(&self) -> &'static str {
        match self {
            Event::Timer { .. } => "timer",
            Event::Deliver { .. } => "deliver",
            Event::Storage { .. } => "storage",
            Event::Fault(_) => "fault",
            Event::App { .. } => "app",
        }
    }

    /// The node this event is directed at, if any.
    pub fn target(&self) -> Option<NodeId> {
        match self {
            Event::Timer { node, .. } => Some(*node),
            Event::Deliver { to, .. } => Some(*to),
            Event::Storage { node, .. } => Some(*node),
            Event::Fault(_) | Event::App { .. } => None,
        }
    }
}

struct Entry {
    time: Nanos,
    id: EventId,
    event: Event,
}

// Ordering is deliberately only over (time, id). Two events at the same
// instant run in the order they were scheduled -- a total order that does not
// depend on the payload, the heap's internal layout, or any hash.
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time).then(self.id.cmp(&other.id))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.id == other.id
    }
}
impl Eq for Entry {}

/// A popped event, with the time it fired at.
#[derive(Clone, Debug)]
pub struct Fired {
    pub time: Nanos,
    pub id: EventId,
    pub event: Event,
}

#[derive(Default)]
pub struct Scheduler {
    now: Nanos,
    next_id: EventId,
    queue: BinaryHeap<Reverse<Entry>>,
    cancelled: BTreeSet<EventId>,
    fired: u64,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler::default()
    }

    #[inline]
    pub fn now(&self) -> Nanos {
        self.now
    }

    /// Number of events fired so far. Used as a fuel limit so a livelocked
    /// cluster fails the run instead of spinning forever.
    pub fn fired_count(&self) -> u64 {
        self.fired
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Schedule at an absolute time. Scheduling into the past is a bug in the
    /// caller, not a recoverable condition: it would make the run depend on
    /// pop order rather than time.
    pub fn at(&mut self, time: Nanos, event: Event) -> EventId {
        assert!(
            time >= self.now,
            "cannot schedule into the past: {time} < {}",
            self.now
        );
        let id = self.next_id;
        self.next_id += 1;
        self.queue.push(Reverse(Entry { time, id, event }));
        id
    }

    /// Schedule `delay` nanoseconds from now.
    pub fn after(&mut self, delay: Nanos, event: Event) -> EventId {
        self.at(self.now.saturating_add(delay), event)
    }

    /// Cancel a scheduled event. Cheap: the entry is tombstoned and skipped
    /// when it reaches the head of the queue.
    pub fn cancel(&mut self, id: EventId) {
        self.cancelled.insert(id);
    }

    pub fn is_cancelled(&self, id: EventId) -> bool {
        self.cancelled.contains(&id)
    }

    /// Pop the next live event, advancing virtual time to it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Fired> {
        while let Some(Reverse(entry)) = self.queue.pop() {
            if self.cancelled.remove(&entry.id) {
                continue;
            }
            debug_assert!(entry.time >= self.now, "time went backwards");
            self.now = entry.time;
            self.fired += 1;
            return Some(Fired {
                time: entry.time,
                id: entry.id,
                event: entry.event,
            });
        }
        None
    }

    /// The time of the next live event, without popping it.
    pub fn peek_time(&self) -> Option<Nanos> {
        // Tombstoned entries may sit at the head, so this is a lower bound on
        // the next fire time -- good enough for "is anything left to do".
        self.queue.peek().map(|Reverse(e)| e.time)
    }

    /// Drop every queued event. Used when a node crashes: its in-flight
    /// timers and disk completions die with it.
    pub fn retain<F: FnMut(&Event) -> bool>(&mut self, mut keep: F) {
        let kept: Vec<_> = self
            .queue
            .drain()
            .filter(|Reverse(e)| keep(&e.event))
            .collect();
        self.queue.extend(kept);
    }

    /// Advance time with nothing scheduled, e.g. to let a quiescent period pass.
    pub fn advance_to(&mut self, time: Nanos) {
        if time > self.now {
            self.now = time;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(tag: u32, arg: u64) -> Event {
        Event::App { tag, arg }
    }

    fn arg_of(e: &Event) -> u64 {
        match e {
            Event::App { arg, .. } => *arg,
            _ => panic!("not an app event"),
        }
    }

    #[test]
    fn fires_in_time_order() {
        let mut s = Scheduler::new();
        s.at(30, app(0, 3));
        s.at(10, app(0, 1));
        s.at(20, app(0, 2));
        let mut got = vec![];
        while let Some(f) = s.next() {
            got.push((f.time, arg_of(&f.event)));
        }
        assert_eq!(got, vec![(10, 1), (20, 2), (30, 3)]);
    }

    #[test]
    fn ties_break_by_insertion_order() {
        let mut s = Scheduler::new();
        for i in 0..64 {
            s.at(100, app(0, i));
        }
        let mut got = vec![];
        while let Some(f) = s.next() {
            got.push(arg_of(&f.event));
        }
        assert_eq!(got, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn time_advances_to_event() {
        let mut s = Scheduler::new();
        assert_eq!(s.now(), 0);
        s.at(500, app(0, 0));
        s.next().unwrap();
        assert_eq!(s.now(), 500);
        // `after` is relative to the new now.
        s.after(50, app(0, 1));
        assert_eq!(s.next().unwrap().time, 550);
    }

    #[test]
    fn cancelled_events_do_not_fire() {
        let mut s = Scheduler::new();
        s.at(10, app(0, 1));
        let doomed = s.at(20, app(0, 2));
        s.at(30, app(0, 3));
        s.cancel(doomed);
        let mut got = vec![];
        while let Some(f) = s.next() {
            got.push(arg_of(&f.event));
        }
        assert_eq!(got, vec![1, 3]);
    }

    #[test]
    #[should_panic(expected = "into the past")]
    fn scheduling_into_the_past_panics() {
        let mut s = Scheduler::new();
        s.at(100, app(0, 0));
        s.next().unwrap();
        s.at(50, app(0, 1));
    }

    #[test]
    fn retain_drops_matching_events() {
        let mut s = Scheduler::new();
        s.at(10, app(1, 0));
        s.at(20, app(2, 0));
        s.at(30, app(1, 1));
        s.retain(|e| matches!(e, Event::App { tag: 2, .. }));
        assert_eq!(s.pending(), 1);
        assert_eq!(s.next().unwrap().time, 20);
        assert!(s.next().is_none());
    }

    #[test]
    fn identical_programs_produce_identical_orders() {
        // The property the whole simulator rests on.
        let run = || {
            let mut s = Scheduler::new();
            let mut order = vec![];
            for i in 0..100u64 {
                s.at(i % 7, app(0, i));
            }
            while let Some(f) = s.next() {
                order.push((f.time, arg_of(&f.event)));
            }
            order
        };
        assert_eq!(run(), run());
    }
}
