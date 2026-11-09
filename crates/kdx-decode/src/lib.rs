//! kdx-decode: payload decoding, ingest verification (spec §2.2), building
//! v1 records and re-rendering them as node JSON.

pub mod build;
pub mod payload;
pub mod render;
pub mod verify;

pub use build::build_records;
pub use payload::{decode_block, DecodeError, DecodedBlock, DecodedTx, Header};
pub use render::{
    compare_json, render_cmd, render_header, render_miner, render_output, render_tx, RenderError,
};
pub use verify::{verify_block, verify_block_strict, verify_linkage, VerifyError, VerifyReport};
