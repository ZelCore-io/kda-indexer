//! Canonical header walk, fetch, decode/index pipeline, backfill, tail, reorgs, freezer (spec §4).

pub mod apply;
pub mod backfill;
pub mod canon;
pub mod ctx;
pub mod disk;
pub mod etl;
pub mod fetch;
pub mod freezer;
pub mod heartbeat;
pub mod nft_details;
pub mod nft_fill;
pub mod pipeline;
pub mod reconcile;
pub mod reorg;
pub mod search_stage;
pub mod seed;
pub mod tail;

pub use canon::{HeaderMode, Walker};
pub use ctx::{Ctx, IngestOpts};
