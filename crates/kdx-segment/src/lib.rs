//! Frozen range/step segment files, indexes and reader (spec §3.4-§3.7). The on-disk format
//! is documented in `docs/format.md`.

pub mod adj;
pub mod blockfile;
pub mod check;
pub mod content;
pub mod ef;
pub mod etl;
pub mod files;
pub mod keys;
pub mod manifest;
pub mod phf;
pub mod postings;
pub mod reader;
pub mod swap;
pub mod writer;

pub use check::{check, CheckReport};
pub use manifest::{Manifest, RangeEntry, RangeKind};
pub use phf::{Index, IndexKind, KeyIndex, PtrHashIndex, SortedIndex};
pub use postings::{ListMemo, PostingIter, RoaringView};
pub use reader::{RangeReader, Segments};
pub use writer::{RangeInfo, RangeWriter, WriteOpts, SLOT_WIDTH};
