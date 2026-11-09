//! Canonical header walk, fetch, decode/index pipeline, backfill, tail, reorgs, freezer (spec §4).

pub mod backfill;
pub mod canon;
pub mod ctx;
pub mod disk;
pub mod etl;
pub mod fetch;
pub mod pipeline;

pub use canon::{HeaderMode, Walker};
pub use ctx::{Ctx, IngestOpts};
