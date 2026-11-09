//! kdx-types: identifiers, hashes, dictionaries, varints and the lossless
//! value codec shared by every kda-indexer crate (spec §3.2, §2.2, §3.4).

pub mod dict;
pub mod hash;
pub mod ids;
pub mod value;
pub mod varint;

pub use dict::{Dict, DictKind, DictMut, MemDict};
pub use hash::{blake2b_256, Hash32};
pub use ids::TxKey;

/// Errors from the codecs in this crate.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("unexpected end of input")]
    Eof,
    #[error("varint overflows u64")]
    VarintOverflow,
    #[error("invalid base64url: {0}")]
    Base64(String),
    #[error("wrong length: expected {expected}, got {got}")]
    Length { expected: usize, got: usize },
    #[error("codec: {0}")]
    Codec(String),
    #[error("unknown dictionary id {id} for {kind:?}")]
    UnknownDictId { kind: DictKind, id: u32 },
}

pub type Result<T> = std::result::Result<T, Error>;
