//! The file set of a consistent copy of `search/` (`snapshot`, plan Task 8): our
//! `manifest.json`, then tantivy's `meta.json`, each read once; the files are those of the
//! segments that `meta.json` lists. Segment files are immutable, so hard links are safe; a
//! tantivy merge or commit can delete one before it is linked (the caller re-reads).
//! The manifest is read first, so its `built_through_height` never runs ahead of the docs.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};

pub struct SearchFiles {
    pub manifest: Vec<u8>,
    pub meta: Vec<u8>,
    /// File names (in `search/`) of the live segments, sorted.
    pub files: Vec<String>,
}

/// None when `dir` holds no built index.
pub fn live_files(dir: &Path) -> Result<Option<SearchFiles>> {
    let manifest = match std::fs::read(dir.join("manifest.json")) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("read search/manifest.json"),
    };
    let meta = std::fs::read(dir.join("meta.json")).context("read search/meta.json")?;
    let v: serde_json::Value = serde_json::from_slice(&meta).context("parse search/meta.json")?;
    let ids: HashSet<String> = v
        .get("segments")
        .and_then(|s| s.as_array())
        .context("search/meta.json has no segments")?
        .iter()
        .filter_map(|s| s.get("segment_id")?.as_str())
        .map(|s| s.replace('-', "").to_ascii_lowercase())
        .collect();
    let mut files = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name().to_string_lossy().into_owned();
        let Some((stem, ext)) = name.split_once('.') else {
            continue;
        };
        // `.store.temp` exists only while a merge writes it.
        if ids.contains(stem) && !ext.ends_with("temp") {
            files.push(name);
        }
    }
    files.sort();
    Ok(Some(SearchFiles {
        manifest,
        meta,
        files,
    }))
}
