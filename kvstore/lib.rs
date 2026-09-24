//! `kvstore` -- the system under test: a replicated key-value store built on
//! Raft, written against [`sim_io`] and therefore runnable inside the
//! simulator.
//!
//! Nothing in this crate knows it is being simulated. It sets timers, sends
//! bytes, and waits for `fsync` completions exactly as a real implementation
//! would, which is the point: the bugs the harness finds are bugs the real
//! thing would have.
//!
//! Scope, stated honestly:
//!
//! * Fixed membership. Joint-consensus reconfiguration is not implemented.
//! * Log compaction is implemented, with snapshots transferred to followers
//!   that have fallen behind. Disk space is not reclaimed: the write-ahead file
//!   is append-only, because compacting it in place would need an atomic
//!   rename, and the storage model deliberately does not offer one.
//! * Reads go through the log, which is the simple way to be linearizable.
//!   Lease-based or read-index reads would be faster and much easier to get
//!   subtly wrong.

pub mod log;
pub mod raft;
pub mod server;

pub use log::{Command, Entry, Op, RaftLog};
pub use raft::{Raft, RaftMsg, Role};
pub use server::{ClientMsg, KvServer, Outcome};

/// A hand-rolled wire codec.
///
/// It exists because the network model corrupts bytes: every decode path is
/// bounds-checked and checksum-guarded, and a malformed frame must produce an
/// error rather than a panic or a wild allocation.
pub mod codec {
    /// Refuse to allocate for an implausible length field. A corrupted u32
    /// would otherwise ask for gigabytes.
    pub const MAX_FIELD: usize = 1 << 20;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum DecodeError {
        Truncated,
        BadTag(u8),
        BadUtf8,
        BadChecksum,
        TooLong,
        TrailingBytes,
    }

    impl std::fmt::Display for DecodeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                DecodeError::Truncated => write!(f, "truncated"),
                DecodeError::BadTag(t) => write!(f, "unknown tag {t}"),
                DecodeError::BadUtf8 => write!(f, "invalid utf-8"),
                DecodeError::BadChecksum => write!(f, "checksum mismatch"),
                DecodeError::TooLong => write!(f, "length field out of range"),
                DecodeError::TrailingBytes => write!(f, "trailing bytes"),
            }
        }
    }

    pub type Result<T> = std::result::Result<T, DecodeError>;

    #[derive(Default)]
    pub struct Enc {
        buf: Vec<u8>,
    }

    impl Enc {
        pub fn new() -> Enc {
            Enc { buf: Vec::new() }
        }

        pub fn u8(&mut self, v: u8) -> &mut Self {
            self.buf.push(v);
            self
        }

        pub fn u32(&mut self, v: u32) -> &mut Self {
            self.buf.extend_from_slice(&v.to_le_bytes());
            self
        }

        pub fn u64(&mut self, v: u64) -> &mut Self {
            self.buf.extend_from_slice(&v.to_le_bytes());
            self
        }

        pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
            self.u32(b.len() as u32);
            self.buf.extend_from_slice(b);
            self
        }

        pub fn str(&mut self, s: &str) -> &mut Self {
            self.bytes(s.as_bytes())
        }

        pub fn opt_str(&mut self, s: Option<&str>) -> &mut Self {
            match s {
                None => self.u8(0),
                Some(v) => {
                    self.u8(1);
                    self.str(v)
                }
            }
        }

        pub fn into_vec(self) -> Vec<u8> {
            self.buf
        }

        pub fn as_slice(&self) -> &[u8] {
            &self.buf
        }
    }

    pub struct Dec<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl<'a> Dec<'a> {
        pub fn new(buf: &'a [u8]) -> Dec<'a> {
            Dec { buf, pos: 0 }
        }

        fn take(&mut self, n: usize) -> Result<&'a [u8]> {
            if self.pos + n > self.buf.len() {
                return Err(DecodeError::Truncated);
            }
            let s = &self.buf[self.pos..self.pos + n];
            self.pos += n;
            Ok(s)
        }

        pub fn u8(&mut self) -> Result<u8> {
            Ok(self.take(1)?[0])
        }

        pub fn u32(&mut self) -> Result<u32> {
            let b = self.take(4)?;
            Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }

        pub fn u64(&mut self) -> Result<u64> {
            let b = self.take(8)?;
            let mut a = [0u8; 8];
            a.copy_from_slice(b);
            Ok(u64::from_le_bytes(a))
        }

        pub fn bytes(&mut self) -> Result<&'a [u8]> {
            let n = self.u32()? as usize;
            if n > MAX_FIELD {
                return Err(DecodeError::TooLong);
            }
            self.take(n)
        }

        pub fn string(&mut self) -> Result<String> {
            let b = self.bytes()?;
            std::str::from_utf8(b)
                .map(|s| s.to_string())
                .map_err(|_| DecodeError::BadUtf8)
        }

        pub fn opt_string(&mut self) -> Result<Option<String>> {
            match self.u8()? {
                0 => Ok(None),
                1 => Ok(Some(self.string()?)),
                t => Err(DecodeError::BadTag(t)),
            }
        }

        pub fn remaining(&self) -> usize {
            self.buf.len() - self.pos
        }

        pub fn finish(self) -> Result<()> {
            if self.remaining() == 0 {
                Ok(())
            } else {
                Err(DecodeError::TrailingBytes)
            }
        }
    }

    /// CRC-32 (IEEE), computed bitwise. Small and obviously correct; the
    /// volumes here do not justify a table.
    pub fn crc32(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFFu32;
        for byte in data {
            crc ^= *byte as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
        !crc
    }

    /// Wrap a payload in a checksummed frame, so a corrupted message is
    /// detected at the edge instead of being interpreted.
    pub fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(payload.len() + 4);
        out.extend_from_slice(&crc32(payload).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    pub fn unframe(bytes: &[u8]) -> Result<&[u8]> {
        if bytes.len() < 4 {
            return Err(DecodeError::Truncated);
        }
        let expect = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let payload = &bytes[4..];
        if crc32(payload) != expect {
            return Err(DecodeError::BadChecksum);
        }
        Ok(payload)
    }
}

use codec::{Dec, DecodeError, Enc};

/// Everything that travels on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Raft(RaftMsg),
    Client(ClientMsg),
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        match self {
            Message::Raft(m) => {
                e.u8(1);
                m.encode_into(&mut e);
            }
            Message::Client(m) => {
                e.u8(2);
                m.encode_into(&mut e);
            }
        }
        codec::frame(e.as_slice())
    }

    pub fn decode(bytes: &[u8]) -> codec::Result<Message> {
        let payload = codec::unframe(bytes)?;
        let mut d = Dec::new(payload);
        let msg = match d.u8()? {
            1 => Message::Raft(RaftMsg::decode_from(&mut d)?),
            2 => Message::Client(ClientMsg::decode_from(&mut d)?),
            t => return Err(DecodeError::BadTag(t)),
        };
        d.finish()?;
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::codec::*;
    use super::*;
    use simcore::rng::Rng;
    use simcore::NodeId;

    #[test]
    fn primitives_round_trip() {
        let mut e = Enc::new();
        e.u8(7).u32(0xdead_beef).u64(u64::MAX).str("hello");
        e.opt_str(None).opt_str(Some("x"));
        let v = e.into_vec();
        let mut d = Dec::new(&v);
        assert_eq!(d.u8().unwrap(), 7);
        assert_eq!(d.u32().unwrap(), 0xdead_beef);
        assert_eq!(d.u64().unwrap(), u64::MAX);
        assert_eq!(d.string().unwrap(), "hello");
        assert_eq!(d.opt_string().unwrap(), None);
        assert_eq!(d.opt_string().unwrap(), Some("x".into()));
        d.finish().unwrap();
    }

    #[test]
    fn truncation_is_an_error_not_a_panic() {
        let mut e = Enc::new();
        e.u64(1).str("abcdef");
        let full = e.into_vec();
        for cut in 0..full.len() {
            let mut d = Dec::new(&full[..cut]);
            // Whatever it does, it must not panic and must not succeed fully.
            let ok = d.u64().is_ok() && d.string().is_ok();
            assert!(!ok || cut == full.len());
        }
    }

    #[test]
    fn absurd_length_fields_are_rejected() {
        let mut e = Enc::new();
        e.u32(u32::MAX); // length prefix for a string that cannot exist
        let v = e.into_vec();
        assert_eq!(Dec::new(&v).string(), Err(DecodeError::TooLong));
    }

    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn framing_detects_corruption() {
        let framed = frame(b"important");
        assert_eq!(unframe(&framed).unwrap(), b"important");
        for i in 0..framed.len() {
            for bit in 0..8 {
                let mut bad = framed.clone();
                bad[i] ^= 1 << bit;
                assert_eq!(
                    unframe(&bad).err(),
                    Some(DecodeError::BadChecksum),
                    "single-bit flip at byte {i} bit {bit} slipped through"
                );
            }
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        // The network flips bits; decode is the first thing that sees them.
        let mut rng = Rng::new(1);
        let template = Message::Raft(RaftMsg::RequestVote {
            term: 3,
            candidate: NodeId(1),
            last_index: 9,
            last_term: 2,
        })
        .encode();
        for _ in 0..20_000 {
            let mut bytes = template.clone();
            let flips = rng.range(1, 4);
            for _ in 0..flips {
                let i = rng.below(bytes.len() as u64) as usize;
                bytes[i] ^= 1 << rng.below(8);
            }
            let _ = Message::decode(&bytes);
        }
        for len in 0..40 {
            let mut bytes = vec![0u8; len];
            rng.fill(&mut bytes);
            let _ = Message::decode(&bytes);
        }
    }

    #[test]
    fn message_round_trips() {
        let msgs = vec![
            Message::Raft(RaftMsg::RequestVote {
                term: 4,
                candidate: NodeId(2),
                last_index: 7,
                last_term: 3,
            }),
            Message::Client(ClientMsg::Request {
                req_id: 88,
                client: 3,
                seq: 12,
                op: Op::Put {
                    key: "k".into(),
                    value: "v".into(),
                },
            }),
        ];
        for m in msgs {
            assert_eq!(Message::decode(&m.encode()).unwrap(), m);
        }
    }
}
