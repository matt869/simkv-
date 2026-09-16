//! Event tracing and the determinism fingerprint.
//!
//! Two separate streams live here, and keeping them separate is the point:
//!
//! * [`Trace::log`] produces human-readable lines and is filtered by level.
//!   Turning it up costs time and memory, so sweeps run with it off.
//! * [`Trace::observe`] folds a *significant* event into a 64-bit rolling
//!   fingerprint. It is never filtered, so a silent sweep run and a verbose
//!   replay of the same seed produce the identical fingerprint. If they ever
//!   differ, the simulator itself is non-deterministic and every bug report it
//!   produced is suspect.

use crate::{Nanos, NodeId};
use std::fmt::Write as _;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum Level {
    #[default]
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl Level {
    pub fn parse(s: &str) -> Option<Level> {
        match s.to_ascii_lowercase().as_str() {
            "off" => Some(Level::Off),
            "error" => Some(Level::Error),
            "warn" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            _ => None,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Level::Off => "OFF",
            Level::Error => "ERR",
            Level::Warn => "WRN",
            Level::Info => "INF",
            Level::Debug => "DBG",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub time: Nanos,
    pub node: Option<NodeId>,
    pub level: Level,
    pub category: &'static str,
    pub message: String,
}

impl Record {
    pub fn render(&self) -> String {
        let mut s = String::with_capacity(64 + self.message.len());
        let ms = self.time / crate::MILLIS;
        let us = (self.time % crate::MILLIS) / crate::MICROS;
        let _ = write!(s, "[{ms:>7}.{us:03}ms]");
        match self.node {
            Some(n) => {
                let _ = write!(s, " n{:<2}", n.0);
            }
            None => s.push_str("  -  "),
        }
        let _ = write!(s, " {} {:<9} {}", self.level.tag(), self.category, self.message);
        s
    }
}

/// FNV-1a. Chosen for being trivially portable and order-sensitive; this is a
/// fingerprint for divergence detection, not a security primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fingerprint(pub u64);

impl Fingerprint {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    pub fn new() -> Fingerprint {
        Fingerprint(Self::OFFSET)
    }

    pub fn feed_bytes(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= *b as u64;
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }

    pub fn feed_u64(&mut self, v: u64) {
        self.feed_bytes(&v.to_le_bytes());
    }
}

impl Default for Fingerprint {
    fn default() -> Self {
        Fingerprint::new()
    }
}

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Trace {
    level: Level,
    records: Vec<Record>,
    max_records: usize,
    dropped: u64,
    fingerprint: Fingerprint,
    observed: u64,
}

impl Default for Trace {
    fn default() -> Self {
        Trace::new(Level::Off)
    }
}

impl Trace {
    pub fn new(level: Level) -> Trace {
        Trace {
            level,
            records: Vec::new(),
            // A runaway simulation must not exhaust memory before the checker
            // gets to run; keep the newest window and count what was dropped.
            max_records: 200_000,
            dropped: 0,
            fingerprint: Fingerprint::new(),
            observed: 0,
        }
    }

    pub fn level(&self) -> Level {
        self.level
    }

    pub fn set_level(&mut self, level: Level) {
        self.level = level;
    }

    pub fn enabled(&self, level: Level) -> bool {
        level <= self.level && self.level > Level::Off
    }

    pub fn log(
        &mut self,
        level: Level,
        time: Nanos,
        node: Option<NodeId>,
        category: &'static str,
        message: impl Into<String>,
    ) {
        if !self.enabled(level) {
            return;
        }
        if self.records.len() >= self.max_records {
            // Drop the oldest half rather than one at a time, so this stays
            // amortised O(1) instead of O(n) per record.
            let half = self.records.len() / 2;
            self.records.drain(..half);
            self.dropped += half as u64;
        }
        self.records.push(Record {
            time,
            node,
            level,
            category,
            message: message.into(),
        });
    }

    /// Fold a significant, behaviour-defining event into the fingerprint.
    /// Never filtered: the fingerprint must not depend on the log level.
    pub fn observe(&mut self, time: Nanos, node: Option<NodeId>, tag: &'static str, args: &[u64]) {
        self.observed += 1;
        self.fingerprint.feed_u64(time);
        self.fingerprint.feed_u64(node.map_or(u64::MAX, |n| n.0 as u64));
        self.fingerprint.feed_bytes(tag.as_bytes());
        for a in args {
            self.fingerprint.feed_u64(*a);
        }
    }

    pub fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    pub fn observed_count(&self) -> u64 {
        self.observed
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        if self.dropped > 0 {
            let _ = writeln!(out, "... {} earlier records dropped ...", self.dropped);
        }
        for r in &self.records {
            out.push_str(&r.render());
            out.push('\n');
        }
        out
    }

    /// The last `n` records, for printing context around a failure.
    pub fn tail(&self, n: usize) -> String {
        let start = self.records.len().saturating_sub(n);
        let mut out = String::new();
        for r in &self.records[start..] {
            out.push_str(&r.render());
            out.push('\n');
        }
        out
    }
}

/// Log a formatted line, skipping the `format!` entirely when the level is off.
#[macro_export]
macro_rules! trace {
    ($trace:expr, $level:expr, $time:expr, $node:expr, $cat:expr, $($arg:tt)*) => {{
        let t = &mut $trace;
        if t.enabled($level) {
            t.log($level, $time, $node, $cat, format!($($arg)*));
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_order_sensitive() {
        let mut a = Trace::new(Level::Off);
        let mut b = Trace::new(Level::Off);
        a.observe(1, Some(NodeId(0)), "x", &[1]);
        a.observe(2, Some(NodeId(1)), "y", &[2]);
        b.observe(2, Some(NodeId(1)), "y", &[2]);
        b.observe(1, Some(NodeId(0)), "x", &[1]);
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn fingerprint_ignores_log_level() {
        let mut quiet = Trace::new(Level::Off);
        let mut loud = Trace::new(Level::Debug);
        for i in 0..100 {
            quiet.observe(i, Some(NodeId(0)), "step", &[i]);
            loud.observe(i, Some(NodeId(0)), "step", &[i]);
            quiet.log(Level::Debug, i, None, "noise", "hello");
            loud.log(Level::Debug, i, None, "noise", "hello");
        }
        assert_eq!(quiet.fingerprint(), loud.fingerprint());
        assert!(quiet.records().is_empty());
        assert_eq!(loud.records().len(), 100);
    }

    #[test]
    fn filtering_respects_level() {
        let mut t = Trace::new(Level::Warn);
        t.log(Level::Debug, 0, None, "c", "no");
        t.log(Level::Info, 0, None, "c", "no");
        t.log(Level::Warn, 0, None, "c", "yes");
        t.log(Level::Error, 0, None, "c", "yes");
        assert_eq!(t.records().len(), 2);
    }

    #[test]
    fn record_window_is_bounded() {
        let mut t = Trace::new(Level::Debug);
        t.max_records = 100;
        for i in 0..1000 {
            t.log(Level::Info, i, None, "c", "x");
        }
        assert!(t.records().len() <= 100);
        assert!(t.dropped() > 0);
        // The newest record must survive eviction.
        assert_eq!(t.records().last().unwrap().time, 999);
    }

    #[test]
    fn level_parses() {
        assert_eq!(Level::parse("DEBUG"), Some(Level::Debug));
        assert_eq!(Level::parse("off"), Some(Level::Off));
        assert_eq!(Level::parse("bogus"), None);
    }
}
