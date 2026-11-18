//! Binary Fuse 8 membership filters over the 8-byte key prefix (FIX-2 design, Decision 2).
//!
//! A [`KeyFilter`] answers "might this prefix be in the range?" with no false negatives and a
//! false positive rate of about 2^-8, for 9.0 bits per key at a million keys and up (a size
//! factor of 1.125 bytes per key). The next layer holds one filter for the block hashes and
//! one for the request keys of every range in memory, so a lookup reads only a range whose
//! filters say "maybe".
//!
//! The construction is the binary fuse graph of Graf and Lemire (arXiv:2201.01174, 3-wise,
//! `FastFilter` reference layout) written out here instead of taken from the `xorf` crate:
//! `xorf` keeps the seed and segment sizes private, so a filter built by it cannot be saved in
//! our own layout. Keys are sorted and deduplicated first, the seed sequence is fixed, and every
//! hash is a bijection of the key, so two builds of the same key set are byte-identical.
//!
//! File layout (see `docs/format.md`, "`kf.fil`"), integers little-endian:
//!
//! ```text
//! file:    "KDXF"  u8 version = 1  filter(bh)  filter(rk)
//! filter:  u64 seed  u32 segment_length  u32 segment_count_length  u64 n_keys
//!          u64 fp_len  [fp_len] fingerprints
//! ```
//!
//! [`KeyFilter::to_bytes`] and [`KeyFilter::from_bytes`] handle one `filter` section;
//! [`encode_pair`] and [`decode_pair`] handle the whole file. Decoding is fail-closed: a
//! truncated, oversized, inconsistent or wrongly tagged input is an error, never a filter that
//! could answer "no" for a key it should know.

use anyhow::{bail, Result};
use kdx_types::Hash32;

use crate::phf::IndexKey;

/// File magic of `kf.fil`.
pub const MAGIC: &[u8; 4] = b"KDXF";
/// File format version of `kf.fil`.
pub const VERSION: u8 = 1;

/// Bytes of the fixed part of one filter section (everything before the fingerprints).
const SECTION_HEADER: usize = 8 + 4 + 4 + 8 + 8;
/// Largest segment: 2^18 slots, as in the reference implementation.
const MAX_SEGMENT_LENGTH: u32 = 1 << 18;
/// Seeds tried before construction gives up (the reference allows 1000; failure is a
/// probability far below 2^-100 for any real key set).
const MAX_ATTEMPTS: u32 = 1000;
/// Fuse arity: every key owns one slot in each of three consecutive segments.
const ARITY: u32 = 3;

/// A Binary Fuse 8 filter over u64 keys.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyFilter {
    seed: u64,
    segment_length: u32,
    segment_count_length: u32,
    n_keys: u64,
    fingerprints: Box<[u8]>,
}

impl std::fmt::Debug for KeyFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyFilter")
            .field("n_keys", &self.n_keys)
            .field("bytes", &self.fingerprints.len())
            .finish()
    }
}

/// Murmur3 64-bit finaliser: a bijection of u64.
#[inline]
fn mix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// The seed sequence: splitmix64 from a fixed state, so the n-th attempt is the same everywhere.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[inline]
fn fingerprint(hash: u64) -> u8 {
    (hash ^ (hash >> 32)) as u8
}

/// The three slots of a hash.
#[inline]
fn slots(hash: u64, segment_length: u32, segment_count_length: u32) -> [usize; 3] {
    let mask = segment_length - 1;
    let h0 = ((u128::from(hash) * u128::from(segment_count_length)) >> 64) as u32;
    let h1 = (h0 + segment_length) ^ ((hash >> 18) as u32 & mask);
    let h2 = (h0 + 2 * segment_length) ^ (hash as u32 & mask);
    [h0 as usize, h1 as usize, h2 as usize]
}

/// `(segment_length, segment_count_length, fingerprint array length)` for `n` distinct keys.
fn params(n: usize) -> (u32, u32, usize) {
    // n < 2^32 is enforced by the caller (slots are u32).
    let n32 = n as u32;
    let segment_length: u32 = if n == 0 {
        4
    } else {
        (1u32 << ((f64::from(n32).ln() / 3.33_f64.ln() + 2.25).floor() as u32))
            .min(MAX_SEGMENT_LENGTH)
    };
    let size_factor = f64::max(
        1.125,
        0.875 + 0.25 * 1_000_000_f64.ln() / f64::from(n32).ln(),
    );
    let capacity: u32 = if n > 1 {
        (f64::from(n32) * size_factor).round() as u32
    } else {
        0
    };
    let init_segment_count = capacity.div_ceil(segment_length);
    let proposed = (init_segment_count * segment_length).div_ceil(segment_length);
    let segment_count = if proposed < ARITY {
        1
    } else {
        proposed - (ARITY - 1)
    };
    let array_len = (segment_count + ARITY - 1) * segment_length;
    (
        segment_length,
        segment_count * segment_length,
        array_len as usize,
    )
}

/// One construction attempt with `seed`. `None` means the fuse graph was not peelable (or a
/// counter overflowed); the caller tries the next seed.
fn try_seed(
    keys: &[u64],
    seed: u64,
    segment_length: u32,
    segment_count_length: u32,
    array_len: usize,
) -> Option<Box<[u8]>> {
    // Sorted hashes visit the slots segment by segment, which keeps the passes cache-friendly.
    let mut hashes: Vec<u64> = keys.iter().map(|&k| mix64(k.wrapping_add(seed))).collect();
    hashes.sort_unstable();

    // count << 2 | xor of the positions (0..3) of the keys in the slot.
    let mut t2count = vec![0u8; array_len];
    let mut t2hash = vec![0u64; array_len];
    for &h in &hashes {
        for (pos, i) in slots(h, segment_length, segment_count_length)
            .into_iter()
            .enumerate()
        {
            t2count[i] = t2count[i].checked_add(4)? ^ pos as u8;
            t2hash[i] ^= h;
        }
    }

    let mut alone: Vec<u32> = (0..array_len)
        .filter(|&i| t2count[i] >> 2 == 1)
        .map(|i| i as u32)
        .collect();
    // The sorted hashes are not needed any more (`t2hash` holds them): their buffer becomes
    // the peeling order, so the peak stays at about 20 bytes per key.
    let n = hashes.len();
    let mut order = hashes;
    order.clear();
    let mut found_at: Vec<u8> = Vec::with_capacity(n);
    while let Some(i) = alone.pop() {
        let i = i as usize;
        if t2count[i] >> 2 != 1 {
            continue;
        }
        let h = t2hash[i];
        let found = t2count[i] & 3;
        order.push(h);
        found_at.push(found);
        let s = slots(h, segment_length, segment_count_length);
        for off in 1..ARITY as u8 {
            let pos = (found + off) % 3;
            let other = s[pos as usize];
            t2count[other] = (t2count[other] - 4) ^ pos;
            t2hash[other] ^= h;
            if t2count[other] >> 2 == 1 {
                alone.push(other as u32);
            }
        }
    }
    if order.len() != n {
        return None;
    }
    drop((t2count, t2hash, alone));

    let mut fp = vec![0u8; array_len].into_boxed_slice();
    for (&h, &found) in order.iter().zip(&found_at).rev() {
        let s = slots(h, segment_length, segment_count_length);
        let f = found as usize;
        fp[s[f]] = fingerprint(h) ^ fp[s[(f + 1) % 3]] ^ fp[s[(f + 2) % 3]];
    }
    Some(fp)
}

impl KeyFilter {
    /// Builds a filter over `keys`. The vector is sorted and deduplicated in place (equal
    /// prefixes are normal: four ranges use the sorted index form because prefixes repeat), so
    /// the result depends only on the key set. Every input key is checked against the finished
    /// filter and a miss is an error: a filter with a false negative is never returned.
    pub fn build(keys: &mut Vec<u64>) -> Result<Self> {
        keys.sort_unstable();
        keys.dedup();
        let n = keys.len();
        if n >= u32::MAX as usize / 2 {
            bail!("key filter: {n} keys exceed the supported size");
        }
        let n_keys = n as u64;
        if n == 0 {
            return Ok(Self {
                seed: 0,
                segment_length: 4,
                segment_count_length: 4,
                n_keys,
                fingerprints: Box::default(),
            });
        }
        let (segment_length, segment_count_length, array_len) = params(n);
        let mut state = 1u64;
        for _ in 0..MAX_ATTEMPTS {
            let seed = splitmix64(&mut state);
            let Some(fingerprints) =
                try_seed(keys, seed, segment_length, segment_count_length, array_len)
            else {
                continue;
            };
            let f = Self {
                seed,
                segment_length,
                segment_count_length,
                n_keys,
                fingerprints,
            };
            if let Some(k) = keys.iter().find(|&&k| !f.contains(k)) {
                bail!("key filter self-check failed: key {k:#018x} is missing from the filter");
            }
            return Ok(f);
        }
        bail!("key filter: construction failed for {n} keys after {MAX_ATTEMPTS} seeds")
    }

    /// True when `key` may be in the set; false means it is certainly not. A false positive
    /// has probability about 2^-8.
    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        if self.fingerprints.is_empty() {
            return false;
        }
        let h = mix64(key.wrapping_add(self.seed));
        let [a, b, c] = slots(h, self.segment_length, self.segment_count_length);
        fingerprint(h) ^ self.fingerprints[a] ^ self.fingerprints[b] ^ self.fingerprints[c] == 0
    }

    /// Builds the filter of a range's index keys (rk or bh): the key of each is its 8-byte
    /// prefix, the same value [`key_of`] gives for the full hash.
    pub fn from_index_keys(keys: &[IndexKey]) -> Result<Self> {
        let mut v: Vec<u64> = keys.iter().map(|k| k.prefix).collect();
        Self::build(&mut v)
    }

    /// [`KeyFilter::contains`] for a full 32-byte hash (request key or block hash).
    #[inline]
    pub fn contains_hash(&self, h: &Hash32) -> bool {
        self.contains(key_of(h))
    }

    /// Distinct keys the filter was built from.
    pub fn len(&self) -> u64 {
        self.n_keys
    }

    /// True for the filter of an empty key set (it contains nothing).
    pub fn is_empty(&self) -> bool {
        self.n_keys == 0
    }

    /// Bytes of fingerprints held in memory: the RAM cost of the filter.
    pub fn bytes(&self) -> usize {
        self.fingerprints.len()
    }

    /// Length of [`KeyFilter::to_bytes`].
    pub fn encoded_len(&self) -> usize {
        SECTION_HEADER + self.fingerprints.len()
    }

    /// Serialises one filter section (no magic; see the module docs).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.write_section(&mut out);
        out
    }

    fn write_section(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.segment_length.to_le_bytes());
        out.extend_from_slice(&self.segment_count_length.to_le_bytes());
        out.extend_from_slice(&self.n_keys.to_le_bytes());
        out.extend_from_slice(&(self.fingerprints.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.fingerprints);
    }

    /// Parses one filter section that must fill `bytes` exactly.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (f, used) = Self::read_section(bytes)?;
        if used != bytes.len() {
            bail!("key filter: {} trailing bytes", bytes.len() - used);
        }
        Ok(f)
    }

    /// Parses a filter section from the front of `bytes`; returns it and the bytes consumed.
    fn read_section(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.len() < SECTION_HEADER {
            bail!("key filter: truncated header ({} bytes)", bytes.len());
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let seed = u64_at(0);
        let segment_length = u32_at(8);
        let segment_count_length = u32_at(12);
        let n_keys = u64_at(16);
        let fp_len = u64_at(24);
        let body = bytes.len() - SECTION_HEADER;
        if fp_len > body as u64 {
            bail!("key filter: {fp_len} fingerprint bytes declared, {body} present");
        }
        let fp_len = fp_len as usize;
        if fp_len == 0 {
            if n_keys != 0 {
                bail!("key filter: {n_keys} keys but no fingerprints");
            }
        } else {
            // Every slot index `contains` can compute must be inside the array: h0 lies in
            // [0, segment_count_length) and h1, h2 stay within the two segments after h0's.
            if !segment_length.is_power_of_two() || segment_length > MAX_SEGMENT_LENGTH {
                bail!("key filter: bad segment length {segment_length}");
            }
            if segment_count_length == 0 || segment_count_length % segment_length != 0 {
                bail!("key filter: segment count length {segment_count_length} is not a multiple of {segment_length}");
            }
            if fp_len as u64 != u64::from(segment_count_length) + 2 * u64::from(segment_length) {
                bail!("key filter: {fp_len} fingerprint bytes do not match the segment sizes");
            }
            // Slot indices are u32 (`slots`): the whole array must be addressable by one.
            if fp_len as u64 > u64::from(u32::MAX) {
                bail!("key filter: {fp_len} fingerprint bytes exceed the u32 slot space");
            }
            if n_keys == 0 || n_keys > fp_len as u64 {
                bail!("key filter: {n_keys} keys cannot fit {fp_len} fingerprint bytes");
            }
        }
        let start = SECTION_HEADER;
        let fingerprints: Box<[u8]> = bytes[start..start + fp_len].into();
        let f = Self {
            seed,
            segment_length,
            segment_count_length,
            n_keys,
            fingerprints,
        };
        Ok((f, start + fp_len))
    }
}

/// The filter key of a 32-byte hash: its 8-byte big-endian prefix, the value the rk/bh
/// index keys carry ([`IndexKey::new`]). Writer and lookup both go through this.
#[inline]
pub fn key_of(h: &Hash32) -> u64 {
    crate::phf::prefix(h)
}

/// The two filters of one range (`kf.fil`), held in owned memory by its reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyFilters {
    pub bh: KeyFilter,
    pub rk: KeyFilter,
}

impl KeyFilters {
    /// Parses a whole `kf.fil` ([`decode_pair`]).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let (bh, rk) = decode_pair(bytes)?;
        Ok(Self { bh, rk })
    }

    /// Serialises a whole `kf.fil` ([`encode_pair`]).
    pub fn to_bytes(&self) -> Vec<u8> {
        encode_pair(&self.bh, &self.rk)
    }

    /// Fingerprint bytes held in memory.
    pub fn bytes(&self) -> usize {
        self.bh.bytes() + self.rk.bytes()
    }
}

/// Serialises a whole `kf.fil`: the block-hash filter, then the request-key filter.
pub fn encode_pair(bh: &KeyFilter, rk: &KeyFilter) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + bh.encoded_len() + rk.encoded_len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    bh.write_section(&mut out);
    rk.write_section(&mut out);
    out
}

/// Parses a whole `kf.fil` into `(bh, rk)`; anything but exactly the two sections is an error.
pub fn decode_pair(bytes: &[u8]) -> Result<(KeyFilter, KeyFilter)> {
    let Some(rest) = bytes.strip_prefix(MAGIC) else {
        bail!("key filter file: bad magic");
    };
    let Some((&version, rest)) = rest.split_first() else {
        bail!("key filter file: truncated");
    };
    if version != VERSION {
        bail!("key filter file: unsupported version {version}");
    }
    let (bh, used) = KeyFilter::read_section(rest)?;
    let rk = KeyFilter::from_bytes(&rest[used..])?;
    Ok((bh, rk))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic test key stream (splitmix64), independent of the filter's own seeds.
    fn stream(seed: u64) -> impl Iterator<Item = u64> {
        let mut s = seed;
        std::iter::from_fn(move || Some(splitmix64(&mut s)))
    }

    fn build(keys: &[u64]) -> KeyFilter {
        KeyFilter::build(&mut keys.to_vec()).unwrap()
    }

    #[test]
    fn no_false_negatives_on_a_million_random_keys() {
        let mut keys: Vec<u64> = stream(7).take(1_000_000).collect();
        let f = KeyFilter::build(&mut keys).unwrap();
        assert_eq!(f.len(), keys.len() as u64);
        assert!(keys.iter().all(|&k| f.contains(k)));
    }

    #[test]
    fn false_positive_rate_and_size_at_a_million_keys() {
        let mut keys: Vec<u64> = stream(11).take(1_000_000).collect();
        let f = KeyFilter::build(&mut keys).unwrap();
        // keys is sorted now; random probes are absent unless they collide, which a binary
        // search excludes.
        let mut fp = 0u64;
        let mut absent = 0u64;
        for k in stream(0xabcd).take(10_000_000) {
            if keys.binary_search(&k).is_ok() {
                continue;
            }
            absent += 1;
            fp += u64::from(f.contains(k));
        }
        let rate = fp as f64 / absent as f64;
        eprintln!("false positive rate {rate:.5} over {absent} absent keys");
        assert!(
            (0.003..=0.005).contains(&rate),
            "false positive rate {rate}"
        );
        let per_key = f.bytes() as f64 / keys.len() as f64;
        assert!(per_key <= 1.15, "{per_key} bytes per key");
    }

    /// The largest real range (14.3 M keys): size and construction time. Run with
    /// `cargo test -p kdx-segment --release -- --ignored large_range --nocapture`.
    #[test]
    #[ignore = "14 M keys; slow in debug builds"]
    fn large_range() {
        let mut keys: Vec<u64> = stream(21).take(14_300_000).collect();
        let t = std::time::Instant::now();
        let f = KeyFilter::build(&mut keys).unwrap();
        let per_key = f.bytes() as f64 / keys.len() as f64;
        eprintln!(
            "built {} keys in {:?}, {per_key:.4} bytes/key",
            keys.len(),
            t.elapsed()
        );
        assert!(per_key <= 1.15);
        assert!(keys.iter().all(|&k| f.contains(k)));
    }

    #[test]
    fn small_and_degenerate_sets() {
        // Empty: nothing is contained.
        let f = build(&[]);
        assert!(f.is_empty());
        assert!(!f.contains(0) && !f.contains(1) && !f.contains(u64::MAX));
        // 1 and 2 keys, including 0 and u64::MAX.
        for set in [
            vec![0],
            vec![u64::MAX],
            vec![1],
            vec![0, 1],
            vec![0, u64::MAX],
            vec![5, 5],
        ] {
            let f = build(&set);
            assert!(set.iter().all(|&k| f.contains(k)), "{set:?}");
        }
        // Every size up to a few hundred builds and keeps all its keys.
        for n in 1..400u64 {
            let keys: Vec<u64> = stream(n).take(n as usize).collect();
            let f = build(&keys);
            assert!(keys.iter().all(|&k| f.contains(k)), "n = {n}");
        }
    }

    #[test]
    fn duplicates_are_removed_before_building() {
        let mut keys: Vec<u64> = stream(3).take(5_000).collect();
        let distinct = keys.clone();
        for _ in 0..3 {
            keys.extend_from_slice(&distinct);
        }
        keys.extend([0, 0, 0, u64::MAX, u64::MAX]);
        let f = KeyFilter::build(&mut keys).unwrap();
        assert_eq!(f.len(), 5_002);
        assert_eq!(
            keys.len(),
            5_002,
            "the input is left sorted and deduplicated"
        );
        assert!(keys.iter().all(|&k| f.contains(k)));
    }

    #[test]
    fn adversarial_key_shapes() {
        let shapes: Vec<(&str, Vec<u64>)> = vec![
            ("consecutive", (0..200_000).collect()),
            (
                "consecutive high",
                (u64::MAX - 200_000..=u64::MAX).collect(),
            ),
            (
                "equal high 32 bits",
                (0..100_000u64).map(|i| 0xdead_beef_0000_0000 | i).collect(),
            ),
            (
                "equal low 32 bits",
                (0..100_000u64).map(|i| (i << 32) | 0x1234_5678).collect(),
            ),
            ("powers of two", (0..64).map(|i| 1u64 << i).collect()),
            (
                "stride",
                (0..100_000u64)
                    .map(|i| i.wrapping_mul(0x1_0000_0001))
                    .collect(),
            ),
            ("single byte varies", (0..256u64).map(|i| i << 56).collect()),
        ];
        for (name, keys) in shapes {
            let f = build(&keys);
            assert!(keys.iter().all(|&k| f.contains(k)), "{name}");
        }
    }

    /// Pins the bytes built for a fixed key set. Filters already on disk were built with the
    /// current hash, slot math, sizing and seed sequence; a change to any of them must fail
    /// here instead of silently missing keys in those files.
    #[test]
    fn golden_bytes_for_a_fixed_key_set() {
        let mut keys: Vec<u64> = (0..10_000u64)
            .map(|i| i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (i << 7))
            .collect();
        let f = KeyFilter::build(&mut keys).unwrap();
        let bytes = encode_pair(&f, &KeyFilter::build(&mut vec![1, 2, 3]).unwrap());
        assert_eq!(bytes.len(), GOLDEN_LEN);
        assert_eq!(blake3::hash(&bytes).to_hex().as_str(), GOLDEN_BLAKE3);
        assert_eq!(f.seed, GOLDEN_SEED);
        assert_eq!((f.segment_length, f.segment_count_length), GOLDEN_SEGMENTS);
        // A few answers: members, and non-members that the pinned filter rejects or admits.
        assert!(keys.iter().all(|&k| f.contains(k)));
        let probes: Vec<bool> = (0..2048u64).map(|i| f.contains(i << 40 | 0xabc)).collect();
        let admitted: Vec<usize> = (0..2048).filter(|&i| probes[i]).collect();
        assert_eq!(admitted, GOLDEN_ADMITTED.to_vec());
        // The 32-byte wrapper uses the same key.
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&keys[17].to_be_bytes());
        assert!(f.contains_hash(&Hash32(h)));
    }
    const GOLDEN_LEN: usize = 12_893;
    const GOLDEN_BLAKE3: &str = "21dcc589d772f47689843772a32ba2182e58fe2f9f8474d03553b7a52cb15ee2";
    const GOLDEN_SEED: u64 = 10_451_216_379_200_822_465;
    const GOLDEN_SEGMENTS: (u32, u32) = (512, 11_776);
    const GOLDEN_ADMITTED: [usize; 10] = [259, 867, 979, 1150, 1158, 1209, 1290, 1500, 1730, 1853];

    #[test]
    fn same_key_set_gives_identical_bytes() {
        let keys: Vec<u64> = stream(99).take(300_000).collect();
        let mut reversed = keys.clone();
        reversed.reverse();
        let mut shuffled_dups = keys.clone();
        shuffled_dups.extend(keys.iter().step_by(7));
        shuffled_dups.sort_by_key(|k| k.rotate_left(17));
        let a = build(&keys).to_bytes();
        assert_eq!(a, build(&keys).to_bytes());
        assert_eq!(a, KeyFilter::build(&mut reversed).unwrap().to_bytes());
        assert_eq!(a, KeyFilter::build(&mut shuffled_dups).unwrap().to_bytes());
    }

    #[test]
    fn section_round_trip() {
        for n in [0usize, 1, 2, 3, 10, 1_000, 123_456] {
            let keys: Vec<u64> = stream(n as u64).take(n).collect();
            let f = build(&keys);
            let bytes = f.to_bytes();
            assert_eq!(bytes.len(), f.encoded_len());
            let g = KeyFilter::from_bytes(&bytes).unwrap();
            assert_eq!(f, g);
            assert!(keys.iter().all(|&k| g.contains(k)));
            let absent: Vec<u64> = stream(!n as u64).take(1_000).collect();
            assert!(absent.iter().all(|&k| f.contains(k) == g.contains(k)));
        }
    }

    #[test]
    fn section_refuses_truncation_and_trailing_bytes() {
        let keys: Vec<u64> = stream(5).take(1_000).collect();
        let bytes = build(&keys).to_bytes();
        for cut in [
            0,
            1,
            7,
            SECTION_HEADER - 1,
            SECTION_HEADER,
            bytes.len() / 2,
            bytes.len() - 1,
        ] {
            assert!(
                KeyFilter::from_bytes(&bytes[..cut]).is_err(),
                "cut at {cut}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(KeyFilter::from_bytes(&longer).is_err());
    }

    #[test]
    fn section_refuses_inconsistent_headers() {
        let keys: Vec<u64> = stream(6).take(1_000).collect();
        let good = build(&keys).to_bytes();
        let patch = |at: usize, v: &[u8]| {
            let mut b = good.clone();
            b[at..at + v.len()].copy_from_slice(v);
            b
        };
        // segment_length: zero, not a power of two, too large.
        for v in [0u32, 3, 6, MAX_SEGMENT_LENGTH << 1] {
            assert!(
                KeyFilter::from_bytes(&patch(8, &v.to_le_bytes())).is_err(),
                "len {v}"
            );
        }
        // segment_count_length: zero, not a multiple, mismatched with fp_len.
        for v in [0u32, 1, u32::MAX] {
            assert!(
                KeyFilter::from_bytes(&patch(12, &v.to_le_bytes())).is_err(),
                "scl {v}"
            );
        }
        // n_keys: zero with fingerprints, more than slots.
        for v in [0u64, u64::MAX] {
            assert!(
                KeyFilter::from_bytes(&patch(16, &v.to_le_bytes())).is_err(),
                "n {v}"
            );
        }
        // fp_len: smaller, larger, absurd.
        for v in [0u64, 12, (good.len() - SECTION_HEADER) as u64 + 1, u64::MAX] {
            assert!(
                KeyFilter::from_bytes(&patch(24, &v.to_le_bytes())).is_err(),
                "fp {v}"
            );
        }
        // Keys declared, no fingerprints.
        let mut empty = build(&[]).to_bytes();
        empty[16..24].copy_from_slice(&1u64.to_le_bytes());
        assert!(KeyFilter::from_bytes(&empty).is_err());
    }

    #[test]
    fn pair_round_trip_and_refusals() {
        let bh = build(&stream(1).take(2_000).collect::<Vec<_>>());
        let rk = build(&stream(2).take(5_000).collect::<Vec<_>>());
        let bytes = encode_pair(&bh, &rk);
        assert_eq!(&bytes[..4], MAGIC);
        let (bh2, rk2) = decode_pair(&bytes).unwrap();
        assert_eq!((&bh, &rk), (&bh2, &rk2));

        // Every strict prefix is refused.
        for cut in 0..bytes.len() {
            assert!(decode_pair(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        // Bad magic, bad version, trailing byte.
        let mut b = bytes.clone();
        b[0] ^= 1;
        assert!(decode_pair(&b).is_err());
        let mut b = bytes.clone();
        b[4] = VERSION + 1;
        assert!(decode_pair(&b).is_err());
        let mut b = bytes.clone();
        b.push(0);
        assert!(decode_pair(&b).is_err());
        // Both filters may be empty.
        let e = build(&[]);
        let (a, b) = decode_pair(&encode_pair(&e, &e)).unwrap();
        assert!(a.is_empty() && b.is_empty());
    }

    #[test]
    fn decoded_garbage_never_panics() {
        // Valid-looking headers over arbitrary fingerprints must answer without panicking.
        let keys: Vec<u64> = stream(8).take(500).collect();
        let good = build(&keys).to_bytes();
        let mut s = 42u64;
        for _ in 0..200 {
            let mut b = good.clone();
            for _ in 0..4 {
                let r = splitmix64(&mut s);
                let at = (r as usize) % b.len();
                b[at] ^= (r >> 40) as u8 | 1;
            }
            if let Ok(f) = KeyFilter::from_bytes(&b) {
                for k in stream(s).take(200) {
                    let _ = f.contains(k);
                }
                let _ = f.contains(0);
                let _ = f.contains(u64::MAX);
            }
        }
    }
}
