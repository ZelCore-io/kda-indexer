//! LEB128 unsigned varints and length-prefixed byte strings.

use crate::{Error, Result};

pub fn put_u64(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get_u64(buf: &mut &[u8]) -> Result<u64> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    loop {
        let (&b, rest) = buf.split_first().ok_or(Error::Eof)?;
        *buf = rest;
        if shift == 63 && b > 1 {
            return Err(Error::VarintOverflow);
        }
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            // Canonical form only: a zero final byte after the first is a padded encoding.
            if b == 0 && shift > 0 {
                return Err(Error::Codec("non-canonical varint".into()));
            }
            return Ok(v);
        }
        shift += 7;
        if shift > 63 {
            return Err(Error::VarintOverflow);
        }
    }
}

pub fn get_u32(buf: &mut &[u8]) -> Result<u32> {
    let v = get_u64(buf)?;
    u32::try_from(v).map_err(|_| Error::Codec(format!("varint {v} exceeds u32")))
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u64(out, b.len() as u64);
    out.extend_from_slice(b);
}

pub fn get_bytes<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    let n = get_u64(buf)? as usize;
    take(buf, n)
}

/// Take exactly `n` raw bytes.
pub fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if buf.len() < n {
        return Err(Error::Eof);
    }
    let (a, rest) = buf.split_at(n);
    *buf = rest;
    Ok(a)
}

pub fn take_array<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N]> {
    let s = take(buf, N)?;
    let mut a = [0u8; N];
    a.copy_from_slice(s);
    Ok(a)
}

pub fn get_u8(buf: &mut &[u8]) -> Result<u8> {
    Ok(take(buf, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_canonical_only() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut b = Vec::new();
            put_u64(&mut b, v);
            assert_eq!(get_u64(&mut b.as_slice()), Ok(v));
        }
        assert!(get_u64(&mut [0x80u8, 0x00].as_slice()).is_err());
        assert!(get_u64(&mut [0xffu8, 0x80, 0x00].as_slice()).is_err());
    }
}
