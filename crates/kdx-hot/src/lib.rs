//! kdx-hot: the MDBX hot store (spec §3.3): dictionaries, tail blocks and txs, tail postings,
//! defpacts, undo records, stage checkpoints and counters.
//!
//! Values are the v1 record codec bytes (`encode_tx`/`encode_block`, plain form) or small
//! big-endian layouts documented in `tables.rs`. Keys are big-endian so MDBX order is numeric
//! order. No `rkyv` in P2.

pub mod checkpoint;
pub mod dict;
pub mod env;
pub mod tables;
pub mod tail;
pub mod undo;

pub use checkpoint::{checkpoint, set_checkpoint, Stage};
pub use dict::HotDict;
pub use env::{Geometry, HotStore, RevScan, RoTxn, RwTxn, Txn, FORMAT_VERSION};
pub use tables::{Table, Tables};
pub use undo::UndoOp;
