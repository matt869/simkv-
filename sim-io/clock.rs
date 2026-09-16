//! Time and timers, as seen by a node.
//!
//! A node reads *its own* clock, which is skewed and drifting relative to true
//! simulation time and to every other node's clock. Nothing in a correct
//! consensus implementation may compare timestamps across nodes; the skew is
//! here to make violations of that rule fail loudly.

use simcore::Nanos;

/// An application-chosen label for a timer, carried back when it fires.
pub type TimerTag = u64;

/// Handle used to cancel a timer that has not fired yet.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct TimerHandle(pub u64);

pub trait Clock {
    /// This node's view of the current time. Monotonic within a node except
    /// when the fault injector steps the clock.
    fn now(&self) -> Nanos;

    /// Fire a `Timer` event carrying `tag` after `delay` of this node's time.
    fn set_timer(&mut self, delay: Nanos, tag: TimerTag) -> TimerHandle;

    /// Cancel a pending timer. Cancelling an already-fired timer is harmless.
    fn cancel_timer(&mut self, handle: TimerHandle);
}

/// A timer that is set, reset, and cancelled as a unit.
///
/// Election and heartbeat logic resets timers constantly; forgetting to cancel
/// the previous one leaves duplicate timers firing, which is a classic source
/// of spurious elections. Keeping the handle inside the timer makes the
/// cancel-then-set sequence the only way to use it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Deadline {
    handle: Option<TimerHandle>,
    fires_at: Nanos,
}

impl Deadline {
    pub fn new() -> Deadline {
        Deadline {
            handle: None,
            fires_at: 0,
        }
    }

    pub fn is_armed(&self) -> bool {
        self.handle.is_some()
    }

    pub fn fires_at(&self) -> Nanos {
        self.fires_at
    }

    /// Arm (or re-arm) the timer, cancelling any previous one.
    pub fn reset(&mut self, clock: &mut dyn Clock, delay: Nanos, tag: TimerTag) {
        self.disarm(clock);
        self.fires_at = clock.now().saturating_add(delay);
        self.handle = Some(clock.set_timer(delay, tag));
    }

    pub fn disarm(&mut self, clock: &mut dyn Clock) {
        if let Some(h) = self.handle.take() {
            clock.cancel_timer(h);
        }
    }

    /// Called when a timer event arrives: reports whether it is the one this
    /// deadline is waiting for. A late copy of a cancelled timer returns false.
    pub fn fired(&mut self) -> bool {
        self.handle.take().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeClock {
        now: Nanos,
        next: u64,
        set: Vec<(Nanos, TimerTag, TimerHandle)>,
        cancelled: Vec<TimerHandle>,
    }

    impl Clock for FakeClock {
        fn now(&self) -> Nanos {
            self.now
        }
        fn set_timer(&mut self, delay: Nanos, tag: TimerTag) -> TimerHandle {
            self.next += 1;
            let h = TimerHandle(self.next);
            self.set.push((self.now + delay, tag, h));
            h
        }
        fn cancel_timer(&mut self, handle: TimerHandle) {
            self.cancelled.push(handle);
        }
    }

    #[test]
    fn reset_cancels_the_previous_timer() {
        let mut c = FakeClock::default();
        let mut d = Deadline::new();
        d.reset(&mut c, 100, 1);
        let first = c.set[0].2;
        d.reset(&mut c, 200, 1);
        assert_eq!(c.cancelled, vec![first], "the old timer must be cancelled");
        assert_eq!(d.fires_at(), 200);
        assert!(d.is_armed());
    }

    #[test]
    fn disarm_is_idempotent() {
        let mut c = FakeClock::default();
        let mut d = Deadline::new();
        d.reset(&mut c, 100, 1);
        d.disarm(&mut c);
        d.disarm(&mut c);
        assert_eq!(c.cancelled.len(), 1);
        assert!(!d.is_armed());
    }

    #[test]
    fn fired_is_true_once_then_false() {
        let mut c = FakeClock::default();
        let mut d = Deadline::new();
        d.reset(&mut c, 100, 1);
        assert!(d.fired(), "the armed timer fired");
        assert!(!d.fired(), "a duplicate delivery must not fire twice");
    }

    #[test]
    fn deadline_tracks_node_time_not_true_time() {
        let mut c = FakeClock {
            now: 5_000,
            ..Default::default()
        };
        let mut d = Deadline::new();
        d.reset(&mut c, 250, 7);
        assert_eq!(d.fires_at(), 5_250);
        assert_eq!(c.set[0].1, 7);
    }
}
