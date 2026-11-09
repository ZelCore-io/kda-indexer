//! kdx-decode: payload decoding, ingest verification (spec §2.2), building
//! v1 records and re-rendering them as node JSON.

pub mod build;
pub mod index;
pub mod json;
pub mod payload;
pub mod render;
pub mod verify;

pub use build::{build_records, BuildError};
pub use index::{extract, index_block, Family, IndexedBlock, TxIndex};
pub use payload::{decode_block, DecodeError, DecodedBlock, DecodedTx, Header, TxFlags};
pub use render::{
    compare_json, render_cmd, render_header, render_miner, render_miner_bytes, render_output,
    render_output_bytes, render_output_typed, render_tx, render_tx_bytes, render_tx_typed,
    RenderError,
};
pub use verify::{verify_block, verify_block_strict, verify_linkage, VerifyError, VerifyReport};
