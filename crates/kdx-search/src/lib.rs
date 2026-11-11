//! Code search over distinct `exec` code bodies (spec §3.8, B5, plan Task 8).
//!
//! One tantivy document per distinct code body (CodeHash): the code as 3-grams for the
//! prefilter and in the zstd doc store for verification (S-2), the full hash, and the heights
//! `first_h`/`last_h` that bound where the body's txs are (the caller expands a hash to txs
//! through the `code` postings). The index lives in the data dir's `search/` next to
//! `search/manifest.json`; `search rebuild` writes `search.new/` and swaps it in by rename.
//! Layout and rules: `docs/format.md` "Search index".

use std::path::{Path, PathBuf};

use kdx_types::record::TxRecord;

pub mod build;
pub mod census;
pub mod manifest;
pub mod query;
pub mod schema;
pub mod snapshot;

pub use build::{dry_run, estimate, progress, rebuild, BuildOpts, Estimate, RebuildReport};
pub use census::{distinct_code_keys, Census};
pub use manifest::SearchManifest;
pub use query::{grams, Candidates, SearchError, Searcher, Window};
pub use schema::{Doc, Fields, SearchWriter};

/// tantivy version the index is written with (pinned exactly in the workspace).
pub const TANTIVY_VERSION: &str = "0.27.0";

/// `search/`: the live index and its manifest.
pub fn search_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("search")
}

/// `search.new/`: a rebuild in progress (removed at the start of the next rebuild).
pub fn new_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("search.new")
}

/// `exec.code` of a tx record: None for coinbase, `cont` txs and a `cmd` that does not parse
/// (the same txs the `code` posting family skips).
pub fn exec_code(r: &TxRecord) -> Option<String> {
    if r.cmd_unparsed {
        return None;
    }
    let v: serde_json::Value = serde_json::from_slice(r.cmd.as_deref()?).ok()?;
    v.pointer("/payload/exec/code")?.as_str().map(str::to_owned)
}

/// A module or interface deployment (`module` field = 1; always indexed in fallback mode).
pub fn is_module(code: &str) -> bool {
    code.contains("(module ") || code.contains("(interface ")
}

/// Which bodies the index holds (S-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Every distinct body.
    Full,
    /// Bodies of at least `min_len` bytes, plus every module or interface deployment. The
    /// others are served by the bounded scan.
    Fallback { min_len: u32 },
}

impl Mode {
    pub fn admits(self, code: &str) -> bool {
        match self {
            Mode::Full => true,
            Mode::Fallback { min_len } => code.len() >= min_len as usize || is_module(code),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Mode::Full => "full",
            Mode::Fallback { .. } => "fallback",
        }
    }
    pub fn min_len(self) -> u32 {
        match self {
            Mode::Full => 0,
            Mode::Fallback { min_len } => min_len,
        }
    }
}

/// `[search] mode`: `auto` decides by the S-1 projection at rebuild time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeSel {
    Auto,
    Full,
    /// `fallback_min_len` 0 = derive the threshold from the projection.
    Fallback(u32),
}

impl ModeSel {
    pub fn parse(s: &str, fallback_min_len: u32) -> anyhow::Result<ModeSel> {
        Ok(match s {
            "auto" => ModeSel::Auto,
            "full" => ModeSel::Full,
            "fallback" => ModeSel::Fallback(fallback_min_len),
            o => anyhow::bail!("search mode {o:?}: expected auto, full or fallback"),
        })
    }
}
