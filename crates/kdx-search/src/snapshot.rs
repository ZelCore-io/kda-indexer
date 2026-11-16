//! The file set of a consistent copy of `search/` (`snapshot`, plan Task 8): our
//! `manifest.json`, then tantivy's `meta.json`, each read once; the files are those of the
//! segments that `meta.json` lists. Segment files are immutable, so hard links are safe; a
//! tantivy merge or commit can delete one before it is linked (the caller re-reads).
//! The manifest is read first, so its `built_through_height` never runs ahead of the docs,
//! and again after `meta.json`: a rebuild swapped in between gives [`Live::Changed`] (the
//! caller re-reads), never an old manifest over a new index; so does a segment of `meta.json`
//! without files. The caller also holds the swap lock (`build::swap_lock`) while it reads and
//! links, when it can open it.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};

pub struct SearchFiles {
    pub manifest: Vec<u8>,
    pub meta: Vec<u8>,
    /// File names (in `search/`) of the live segments, sorted.
    pub files: Vec<String>,
}

pub enum Live {
    /// No built index.
    None,
    /// `search/` changed between the reads: read again.
    Changed,
    Files(SearchFiles),
}

fn read_manifest(dir: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(dir.join("manifest.json")) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("read search/manifest.json"),
    }
}

/// The live file set of `dir` (see the module doc).
pub fn live(dir: &Path) -> Result<Live> {
    let Some(manifest) = read_manifest(dir)? else {
        return Ok(Live::None);
    };
    let meta = match std::fs::read(dir.join("meta.json")) {
        Ok(b) => b,
        // Mid-swap.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Live::Changed),
        Err(e) => return Err(e).context("read search/meta.json"),
    };
    let built_at = |b: &[u8]| {
        serde_json::from_slice::<serde_json::Value>(b)
            .ok()
            .and_then(|v| v.get("built_at")?.as_u64())
    };
    if read_manifest(dir)?.as_deref().map(built_at) != Some(built_at(&manifest)) {
        return Ok(Live::Changed);
    }
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
    let mut seen = HashSet::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name().to_string_lossy().into_owned();
        let Some((stem, ext)) = name.split_once('.') else {
            continue;
        };
        // `.store.temp` exists only while a merge writes it.
        if ids.contains(stem) && !ext.ends_with("temp") {
            seen.insert(stem.to_owned());
            files.push(name);
        }
    }
    // A segment of `meta.json` without files: the directory changed after the read.
    if seen.len() != ids.len() {
        return Ok(Live::Changed);
    }
    files.sort();
    Ok(Live::Files(SearchFiles {
        manifest,
        meta,
        files,
    }))
}

/// [`live`] for callers that only need a stable directory (None: no index, or it changed).
pub fn live_files(dir: &Path) -> Result<Option<SearchFiles>> {
    Ok(match live(dir)? {
        Live::Files(f) => Some(f),
        Live::None | Live::Changed => None,
    })
}

#[cfg(test)]
mod tests {
    use super::{live, Live};
    use crate::manifest::{SearchManifest, FORMAT};
    use crate::schema::{create, to_doc, writer, Doc};

    fn index(dir: &std::path::Path, code: &str, built_at: u64) {
        let (i, f) = create(dir).unwrap();
        let mut w = writer(&i, 1, 16).unwrap();
        let d = Doc {
            hash: kdx_types::blake2b_128(code.as_bytes()),
            code: code.into(),
            first_h: 1,
            last_h: 1,
        };
        w.add_document(to_doc(&f, &d)).unwrap();
        w.commit().unwrap();
        SearchManifest {
            format: FORMAT,
            tantivy: crate::TANTIVY_VERSION.into(),
            mode: "full".into(),
            fallback_min_len: 0,
            built_through_height: 1,
            rebuilt_through_height: Some(1),
            stale_from_height: None,
            docs: 1,
            bytes: 0,
            built_at,
        }
        .commit(dir)
        .unwrap();
    }

    /// M-10: an old manifest never pairs with another index's `meta.json`.
    #[test]
    fn a_meta_from_another_index_reads_as_changed() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        index(a.path(), "(a.one)", 1);
        index(b.path(), "(b.one)", 2);
        assert!(matches!(live(a.path()).unwrap(), Live::Files(f) if !f.files.is_empty()));
        std::fs::copy(b.path().join("meta.json"), a.path().join("meta.json")).unwrap();
        assert!(matches!(live(a.path()).unwrap(), Live::Changed));
        std::fs::remove_file(a.path().join("meta.json")).unwrap();
        assert!(matches!(live(a.path()).unwrap(), Live::Changed));
    }
}
