//! Canonical header walk, fetch, decode/index pipeline, backfill, tail, reorgs, freezer (spec §4).

pub mod add_index;
pub mod apply;
pub mod backfill;
pub mod canon;
pub mod content;
pub mod ctx;
pub mod disk;
pub mod etl;
pub mod fetch;
pub mod freezer;
pub mod heartbeat;
pub mod keylist;
pub mod nft_details;
pub mod nft_fill;
pub mod pipeline;
pub mod rebuild;
pub mod reconcile;
pub mod reorg;
pub mod search_stage;
pub mod seed;
pub mod seed_receivers;
pub mod tail;

pub use canon::{HeaderMode, Walker};
pub use ctx::{Ctx, IngestOpts};
