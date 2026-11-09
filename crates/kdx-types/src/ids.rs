//! Transaction key (spec §3.2): `height (32) | chain (8) | position (16) | reserved (8)`.

use std::fmt;

/// Sorts by height, then chain, then position. Position 0 is the coinbase,
/// user txs are 1..n in payload order. The low 8 bits are reserved and zero.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TxKey(pub u64);

impl TxKey {
    pub fn new(height: u32, chain: u8, pos: u16) -> TxKey {
        TxKey((height as u64) << 32 | (chain as u64) << 24 | (pos as u64) << 8)
    }
    pub fn height(self) -> u32 {
        (self.0 >> 32) as u32
    }
    pub fn chain(self) -> u8 {
        (self.0 >> 24) as u8
    }
    pub fn pos(self) -> u16 {
        (self.0 >> 8) as u16
    }
    pub fn is_coinbase(self) -> bool {
        self.pos() == 0
    }
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub fn from_be_bytes(b: [u8; 8]) -> TxKey {
        TxKey(u64::from_be_bytes(b))
    }
}

impl fmt::Debug for TxKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TxKey({}/{}/{})",
            self.height(),
            self.chain(),
            self.pos()
        )
    }
}
