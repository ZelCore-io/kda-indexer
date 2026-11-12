//! Checked reads of a stored sucds `EliasFano` (sec M-6, R-P5P9-25).
//!
//! `EliasFano::deserialize_from` trusts every stored length: a damaged file panics inside
//! sucds, or aborts the process on a huge `Vec::with_capacity`. Nothing here hands stored
//! bytes to sucds. [`values`] walks the serialized layout with every length checked against
//! the bytes left, checks the shape `EliasFanoBuilder` gives (`low_len`, the bit lengths) and
//! decodes the values from the raw bits. [`load`] then rebuilds the sequence with the builder
//! and requires its serialization to equal the stored bytes, so the select index sucds walks
//! is one it built itself.
//!
//! Layout (sucds 0.8, 64-bit `usize`, as `EliasFanoBuilder::build` writes it):
//! high bits (`DArray`: bit vector, select1 index, `Option` select0 = none, `Option` rank9 =
//! none), low bits (bit vector), `low_len`, `universe`. A bit vector is a `Vec<usize>` of
//! words (u64 count, then the words) followed by its bit length.

use anyhow::{bail, ensure, Context, Result};
use sucds::mii_sequences::{EliasFano, EliasFanoBuilder};
use sucds::Serializable;

const WORD: usize = 64;

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&e| e <= self.b.len())
            .context("Elias-Fano data is truncated")?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn usize(&mut self) -> Result<usize> {
        let v = u64::from_le_bytes(self.take(8)?.try_into()?);
        Ok(usize::try_from(v)?)
    }

    fn flag(&mut self) -> Result<bool> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            b => bail!("Elias-Fano flag byte {b}"),
        }
    }

    /// A sucds `Vec<T>` of `size`-byte items; the stored count is checked against the bytes
    /// left before anything is read or allocated.
    fn vec(&mut self, size: usize) -> Result<&'a [u8]> {
        let n = self.usize()?;
        let bytes = n
            .checked_mul(size)
            .context("Elias-Fano vector length overflows")?;
        self.take(bytes)
    }

    /// A sucds `BitVector`: its words must cover the bit length exactly.
    fn bits(&mut self) -> Result<Bits<'a>> {
        let words = self.vec(8)?;
        let len = self.usize()?;
        ensure!(
            words.len() / 8 == len.div_ceil(WORD),
            "Elias-Fano bit vector: {} words for {len} bits",
            words.len() / 8
        );
        Ok(Bits { words, len })
    }
}

struct Bits<'a> {
    words: &'a [u8],
    len: usize,
}

impl Bits<'_> {
    fn word(&self, i: usize) -> u64 {
        u64::from_le_bytes(self.words[8 * i..8 * i + 8].try_into().expect("8 bytes"))
    }

    /// `len < 64` bits at `pos`; the caller keeps `pos + len <= self.len`.
    fn get(&self, pos: usize, len: usize) -> u64 {
        if len == 0 {
            return 0;
        }
        let (w, s) = (pos / WORD, pos % WORD);
        let mut v = self.word(w) >> s;
        if s + len > WORD {
            v |= self.word(w + 1) << (WORD - s);
        }
        v & ((1u64 << len) - 1)
    }
}

/// A decoded sequence: its values (ascending), its universe and the bytes it took.
pub struct Decoded {
    pub values: Vec<u64>,
    pub universe: usize,
    pub used: usize,
}

/// Decode the Elias-Fano sequence serialized at the start of `b` without sucds. Every stored
/// length is checked against `b`; a damaged sequence is an `Err`, never a panic or an
/// allocation larger than a small multiple of `b`.
pub fn values(b: &[u8]) -> Result<Decoded> {
    let mut c = Cursor { b, at: 0 };
    let high = c.bits()?;
    // The select1 index (block and subblock inventories, overflow positions). `load` checks
    // its content by rebuilding it; here only its lengths matter.
    c.vec(8)?;
    c.vec(2)?;
    c.vec(8)?;
    let ones = c.usize()?;
    c.flag()?;
    ensure!(
        !c.flag()? && !c.flag()?,
        "Elias-Fano high bits carry an index the writer never builds"
    );
    let low = c.bits()?;
    let low_len = c.usize()?;
    let universe = c.usize()?;
    ensure!(ones > 0, "empty Elias-Fano sequence");
    // The shape `EliasFanoBuilder::new(universe, ones)` gives.
    let want_low = (universe / ones).checked_ilog2().unwrap_or(0) as usize;
    ensure!(
        low_len == want_low,
        "Elias-Fano low width {low_len}, want {want_low}"
    );
    let want_high = ones
        .checked_add(2)
        .and_then(|x| x.checked_add(universe >> low_len))
        .context("Elias-Fano universe overflows")?;
    ensure!(
        high.len == want_high,
        "Elias-Fano high bits: {} bits, want {want_high}",
        high.len
    );
    ensure!(
        ones.checked_mul(low_len) == Some(low.len),
        "Elias-Fano low bits: {} bits for {ones} values of {low_len}",
        low.len
    );
    // `ones < high.len`, and `high.len` is backed by stored words: the capacity is bounded.
    let mut values = Vec::with_capacity(ones);
    let mut prev = 0u64;
    let words = high.words.len() / 8;
    for w in 0..words {
        let mut x = high.word(w);
        let tail = high.len - w * WORD;
        if tail < WORD {
            x &= (1u64 << tail) - 1;
        }
        while x != 0 {
            let p = w * WORD + x.trailing_zeros() as usize;
            x &= x - 1;
            let i = values.len();
            ensure!(
                i < ones,
                "Elias-Fano high bits hold more than {ones} values"
            );
            // Set bits are distinct and ascending, so `p >= i`.
            let hi = (p - i) as u64;
            let v = (hi << low_len) | low.get(i * low_len, low_len);
            ensure!(
                v >= prev && v < universe as u64,
                "Elias-Fano value {v} out of order or past the universe {universe}"
            );
            prev = v;
            values.push(v);
        }
    }
    ensure!(
        values.len() == ones,
        "Elias-Fano high bits hold {} values, the index says {ones}",
        values.len()
    );
    Ok(Decoded {
        values,
        universe,
        used: c.at,
    })
}

/// A sucds `EliasFano` for the sequence serialized at the start of `b`, and the bytes it took.
/// The sequence is rebuilt from its decoded values; the stored bytes must equal the rebuilt
/// serialization, so a damaged select index is an `Err` too.
pub fn load(b: &[u8]) -> Result<(EliasFano, usize)> {
    let d = values(b)?;
    let mut eb = EliasFanoBuilder::new(d.universe, d.values.len())?;
    for &v in &d.values {
        eb.push(usize::try_from(v)?)?;
    }
    let ef = eb.build();
    let mut again = Vec::with_capacity(d.used);
    ef.serialize_into(&mut again)?;
    ensure!(
        again[..] == b[..d.used],
        "Elias-Fano index does not match its values"
    );
    Ok((ef, d.used))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(universe: usize, vals: &[usize]) -> Vec<u8> {
        let mut b = EliasFanoBuilder::new(universe, vals.len()).unwrap();
        for &v in vals {
            b.push(v).unwrap();
        }
        let mut out = Vec::new();
        b.build().serialize_into(&mut out).unwrap();
        out
    }

    #[test]
    fn decodes_what_sucds_wrote() {
        for (u, vals) in [
            (1usize, vec![0usize]),
            (10, vec![0, 0, 3, 9]),
            (1 << 20, (0..5000).map(|i| i * 200).collect()),
            (70, (0..70).collect()),
        ] {
            let b = stored(u, &vals);
            let d = values(&b).unwrap();
            assert_eq!(d.used, b.len());
            assert_eq!(d.universe, u);
            let got: Vec<usize> = d.values.iter().map(|&v| v as usize).collect();
            assert_eq!(got, vals);
            let (ef, used) = load(&b).unwrap();
            assert_eq!(used, b.len());
            assert_eq!(ef.iter(0).collect::<Vec<_>>(), vals);
        }
    }

    #[test]
    fn every_damaged_byte_is_an_error_or_a_valid_sequence() {
        // Flip each byte of a stored sequence: `load` either refuses it or gives a sequence
        // whose own serialization is the damaged bytes (so sucds built it, not the file).
        let good = stored(1000, &[1, 5, 5, 80, 400, 999]);
        for at in 0..good.len() {
            for x in [0x01u8, 0x80, 0xff] {
                let mut b = good.clone();
                b[at] ^= x;
                if let Ok((ef, used)) = load(&b) {
                    let mut again = Vec::new();
                    ef.serialize_into(&mut again).unwrap();
                    assert_eq!(again[..], b[..used]);
                    let _ = ef.iter(0).count();
                }
            }
        }
    }

    #[test]
    fn huge_stored_lengths_are_refused_without_allocating() {
        let good = stored(1000, &[1, 5, 80, 999]);
        for at in 0..good.len().saturating_sub(8) {
            let mut b = good.clone();
            b[at..at + 8].copy_from_slice(&(1u64 << 60).to_le_bytes());
            // A length field is refused; damage inside the value bits can still spell another
            // valid sequence (the range's blake3 catches that), which sucds then built itself.
            if let Ok((ef, used)) = load(&b) {
                let mut again = Vec::new();
                ef.serialize_into(&mut again).unwrap();
                assert_eq!(again[..], b[..used], "byte {at}");
            }
        }
        assert!(values(&[]).is_err());
        assert!(values(&[0xff; 64]).is_err());
    }
}
