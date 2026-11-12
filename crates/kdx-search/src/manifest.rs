//! `search/manifest.json` (plan Task 8): what the index holds and how far it is built.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::Mode;

/// Version of the `search/` layout (manifest fields, schema, tokenizer).
pub const FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchManifest {
    pub format: u32,
    /// tantivy version that wrote the index.
    pub tantivy: String,
    /// `full` or `fallback`.
    pub mode: String,
    /// Fallback threshold in bytes of code (0 in full mode).
    pub fallback_min_len: u32,
    /// Every exec tx at or below this height (on every chain) has its body in the index.
    /// Bodies of later txs may be in it too (the tail stage adds them; docs are never
    /// deleted). The tail stage moves it forward after each commit.
    pub built_through_height: u32,
    /// `built_through_height` as the last rebuild wrote it; the tail stage never moves it.
    /// Docs are never updated, so a body's `last_h` is only exact up to this height: readers
    /// expand a hit through its `code` postings within `first_h..=last_h` plus every height
    /// above this one (absent in a manifest from before this field: expand from `first_h`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebuilt_through_height: Option<u32>,
    /// Lowest height whose content a `rebuild-range` swap changed after this index was built
    /// (M-7): docs from there on may name bodies that are gone and miss new ones. Readers
    /// treat the index as lagging (native `partial: true`, compat `/txs/search` scans) and a
    /// periodic rebuild is due. A rebuild writes a manifest without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_from_height: Option<u32>,
    pub docs: u64,
    /// Index bytes on disk (at the last rebuild or tail commit).
    pub bytes: u64,
    /// Unix seconds of the rebuild that created this index (a reader that sees it change
    /// reopens: the directory was swapped).
    pub built_at: u64,
}

impl SearchManifest {
    pub fn load(dir: &Path) -> Result<Option<SearchManifest>> {
        let p = dir.join("manifest.json");
        let b = match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", p.display())),
        };
        let m: SearchManifest =
            serde_json::from_slice(&b).with_context(|| format!("parse {}", p.display()))?;
        if m.format != FORMAT {
            bail!(
                "{}: search format {} (this binary reads {FORMAT}); run `search rebuild`",
                p.display(),
                m.format
            );
        }
        Ok(Some(m))
    }

    /// Atomic replace (write, fsync, rename, fsync dir).
    pub fn commit(&self, dir: &Path) -> Result<()> {
        let b = serde_json::to_vec_pretty(self)?;
        kdx_segment::files::replace_file(&dir.join("manifest.json"), &b)
    }

    pub fn mode(&self) -> Mode {
        match self.mode.as_str() {
            "fallback" => Mode::Fallback {
                min_len: self.fallback_min_len,
            },
            _ => Mode::Full,
        }
    }
}

/// Record in `dir/manifest.json` that content from `height` on changed under the index
/// (M-7); keeps the lowest such height. `Ok(false)` when no index is built.
pub fn mark_stale(dir: &Path, height: u32) -> Result<bool> {
    let Some(mut m) = SearchManifest::load(dir)? else {
        return Ok(false);
    };
    m.stale_from_height = Some(m.stale_from_height.map_or(height, |h| h.min(height)));
    m.commit(dir)?;
    Ok(true)
}

/// Bytes of the regular files directly in `dir`.
pub fn dir_bytes(dir: &Path) -> Result<u64> {
    let mut n = 0;
    for e in std::fs::read_dir(dir)? {
        let m = e?.metadata()?;
        if m.is_file() {
            n += m.len();
        }
    }
    Ok(n)
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
