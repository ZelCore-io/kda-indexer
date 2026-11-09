//! 32-byte hashes, unpadded base64url on the wire (spec §2.2).

use std::fmt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use blake2::{Blake2b256, Digest};

use crate::{Error, Result};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Hash32(pub [u8; 32]);

impl Hash32 {
    /// Strict: unpadded base64url, exactly 43 chars, canonical trailing bits.
    pub fn from_b64url(s: &str) -> Result<Hash32> {
        if s.len() != 43 {
            return Err(Error::Length { expected: 43, got: s.len() });
        }
        let v = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| Error::Base64(e.to_string()))?;
        Hash32::from_slice(&v)
    }
    pub fn from_slice(b: &[u8]) -> Result<Hash32> {
        let a: [u8; 32] = b
            .try_into()
            .map_err(|_| Error::Length { expected: 32, got: b.len() })?;
        Ok(Hash32(a))
    }
    pub fn to_b64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash32({})", self.to_b64url())
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_b64url())
    }
}

/// blake2b-256 of `data` (the request key is this over the `cmd` string bytes).
pub fn blake2b_256(data: &[u8]) -> Hash32 {
    let d = Blake2b256::digest(data);
    let mut a = [0u8; 32];
    a.copy_from_slice(&d);
    Hash32(a)
}

/// Unpadded base64url decode (strict), for fields that are not 32-byte hashes.
pub fn b64url_decode(s: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(s)
        .map_err(|e| Error::Base64(e.to_string()))
}

pub fn b64url_encode(b: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(b)
}

