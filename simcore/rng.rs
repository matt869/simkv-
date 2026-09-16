//! Deterministic pseudo-random number generation.
//!
//! Every non-deterministic choice in the simulator flows through this type.
//! The algorithm is xoshiro256++ seeded through SplitMix64; it is stable
//! across platforms and Rust versions, which is what makes a seed a durable
//! bug report. Nothing here touches the OS, the clock, or thread-local state.

/// SplitMix64: used only to expand a user seed into the 256-bit state.
#[derive(Clone, Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// A deterministic random source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rng {
    s: [u64; 4],
}

impl Rng {
    /// Create a generator from a seed. The same seed always yields the same
    /// stream, on every machine.
    pub fn new(seed: u64) -> Rng {
        let mut sm = SplitMix64(seed);
        Rng {
            s: [sm.next(), sm.next(), sm.next(), sm.next()],
        }
    }

    /// Derive an independent child generator. Used to give each node its own
    /// stream so that adding a node does not perturb the others' draws.
    pub fn fork(&mut self, label: u64) -> Rng {
        let mixed = self.next_u64() ^ label.wrapping_mul(0xD6E8_FEB8_6659_FD93);
        Rng::new(mixed)
    }

    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[0]
            .wrapping_add(self.s[3])
            .rotate_left(23)
            .wrapping_add(self.s[0]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    #[inline]
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Uniform in `[0, n)`. Unbiased (Lemire's method with rejection).
    /// Returns 0 when `n == 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        if n <= 1 {
            return 0;
        }
        // Rejection sampling on the 128-bit product keeps the distribution
        // exactly uniform; biased modulo would skew rare-fault probabilities.
        let threshold = n.wrapping_neg() % n;
        loop {
            let x = self.next_u64();
            let m = (x as u128) * (n as u128);
            let low = m as u64;
            if low >= threshold {
                return (m >> 64) as u64;
            }
        }
    }

    /// Uniform in `[lo, hi]`, inclusive. `lo > hi` yields `lo`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        lo + self.below(hi - lo + 1)
    }

    /// True with probability `ppm / 1_000_000`.
    ///
    /// Probabilities are parts-per-million integers rather than floats so the
    /// decision is bit-exact everywhere.
    pub fn chance_ppm(&mut self, ppm: u32) -> bool {
        if ppm == 0 {
            return false;
        }
        if ppm >= 1_000_000 {
            return true;
        }
        self.below(1_000_000) < ppm as u64
    }

    /// Uniform index into a slice of `len` items, or `None` if empty.
    pub fn index(&mut self, len: usize) -> Option<usize> {
        if len == 0 {
            None
        } else {
            Some(self.below(len as u64) as usize)
        }
    }

    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        self.index(items.len()).map(|i| &items[i])
    }

    /// In-place Fisher-Yates.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        let mut chunks = buf.chunks_exact_mut(8);
        for c in &mut chunks {
            c.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let rest = chunks.into_remainder();
        if !rest.is_empty() {
            let bytes = self.next_u64().to_le_bytes();
            rest.copy_from_slice(&bytes[..rest.len()]);
        }
    }

    /// Draw from a rough exponential-ish distribution over `[lo, hi]` biased
    /// toward `lo`. Real network latency has a long tail; a flat distribution
    /// hides the timing-sensitive bugs that only a tail exposes.
    pub fn skewed(&mut self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        let span = hi - lo;
        // Minimum of two draws: a cheap, allocation-free way to bias low while
        // still reaching the tail often enough to matter.
        let a = self.below(span + 1);
        let b = self.below(span + 1);
        lo + a.min(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let diff = (0..100).filter(|_| a.next_u64() != b.next_u64()).count();
        assert!(diff > 95, "streams should differ, only {diff}/100 differed");
    }

    #[test]
    fn below_is_in_range_and_covers() {
        let mut r = Rng::new(7);
        let mut seen = [0u32; 5];
        for _ in 0..10_000 {
            let v = r.below(5);
            assert!(v < 5);
            seen[v as usize] += 1;
        }
        // Uniform enough: every bucket within 20% of the 2000 expectation.
        for (i, c) in seen.iter().enumerate() {
            assert!(*c > 1600 && *c < 2400, "bucket {i} = {c}");
        }
    }

    #[test]
    fn below_edge_cases() {
        let mut r = Rng::new(9);
        assert_eq!(r.below(0), 0);
        assert_eq!(r.below(1), 0);
        assert_eq!(r.range(5, 5), 5);
        assert_eq!(r.range(9, 3), 9);
    }

    #[test]
    fn chance_ppm_bounds() {
        let mut r = Rng::new(11);
        assert!(!r.chance_ppm(0));
        assert!(r.chance_ppm(1_000_000));
        let hits = (0..100_000).filter(|_| r.chance_ppm(100_000)).count();
        assert!((8_000..12_000).contains(&hits), "hits = {hits}");
    }

    #[test]
    fn shuffle_is_a_permutation() {
        let mut r = Rng::new(3);
        let mut v: Vec<u32> = (0..64).collect();
        r.shuffle(&mut v);
        let mut sorted = v.clone();
        sorted.sort();
        assert_eq!(sorted, (0..64).collect::<Vec<_>>());
        assert_ne!(v, sorted, "shuffle of 64 items should not be identity");
    }

    #[test]
    fn fork_is_deterministic_but_independent() {
        let mut parent = Rng::new(5);
        let mut a = parent.fork(0);
        let mut b = parent.fork(1);
        let mut parent2 = Rng::new(5);
        let mut a2 = parent2.fork(0);
        assert_eq!(a.next_u64(), a2.next_u64());
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn skewed_stays_in_bounds_and_biases_low() {
        let mut r = Rng::new(13);
        let mut low = 0;
        for _ in 0..10_000 {
            let v = r.skewed(10, 110);
            assert!((10..=110).contains(&v));
            if v < 60 {
                low += 1;
            }
        }
        assert!(low > 6_500, "expected a low bias, got {low}/10000");
    }
}
