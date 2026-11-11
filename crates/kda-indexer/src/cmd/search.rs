//! `search rebuild [--dry-run]` and `search query` (plan Task 8, S-1). Both are read-only on
//! the hot store and the segments, so they can run beside a live `run`; `rebuild` writes only
//! `search.new/` and swaps it in, `--dry-run` writes nothing durable (its trial index lives
//! in `search.dryrun/` and is removed).

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use kdx_search::{BuildOpts, ModeSel, SearchError, Searcher};
use kdx_types::like::Like;

use crate::config::Config;

pub struct RebuildArgs {
    pub data_dir: PathBuf,
    pub dry_run: bool,
    /// Overrides `[search] mode` (`auto`, `full`, `fallback`).
    pub mode: Option<String>,
    pub trial_docs: u64,
}

pub fn rebuild(cfg: &Config, a: RebuildArgs) -> anyhow::Result<()> {
    let s = &cfg.search;
    let mode = match &a.mode {
        Some(m) => ModeSel::parse(m, s.fallback_min_len)?,
        None => s.mode_sel()?,
    };
    let o = BuildOpts {
        data_dir: a.data_dir.clone(),
        mode,
        threads: s.build_threads as usize,
        heap_mb: s.heap_mb as usize,
        trial_docs: a.trial_docs,
        stop: Arc::new(AtomicBool::new(false)),
    };
    let hot = kdx_hot::HotStore::open_ro(&super::hot::hot_dir(&a.data_dir))?;
    if a.dry_run {
        let e = kdx_search::dry_run(&o, &hot)?;
        println!(
            "search dry run: {} distinct code bodies over {} ranges (+hot); sample {} bodies, \
             avg code {:.0} B, module share {:.4}; trial {} docs {} B ({:.0} B/doc, {:.3} B per \
             code byte); projected full {:.2} GB -> {} (fallback_min_len {}, projected {:.2} GB); \
             {:.1} s",
            e.distinct,
            e.ranges,
            e.sample_bodies,
            e.sample_avg_code_len,
            e.sample_module_share,
            e.trial_docs,
            e.trial_bytes,
            e.bytes_per_doc,
            e.bytes_per_code_byte,
            e.projected_full_bytes as f64 / 1e9,
            e.decision,
            e.fallback_min_len,
            e.projected_bytes as f64 / 1e9,
            e.secs
        );
        println!("{}", serde_json::to_string(&e)?);
        return Ok(());
    }
    let r = kdx_search::rebuild(&o, &hot)?;
    println!(
        "search rebuild: {} docs, {} B, mode {} (fallback_min_len {}), built through height {}, \
         {} census keys, {} bodies left to the scan by mode, {:.1} s",
        r.manifest.docs,
        r.manifest.bytes,
        r.manifest.mode,
        r.manifest.fallback_min_len,
        r.manifest.built_through_height,
        r.census_keys,
        r.skipped_by_mode,
        r.secs
    );
    println!("{}", serde_json::to_string(&r)?);
    Ok(())
}

/// Dev aid: the candidate bodies of a LIKE pattern, one JSON line each.
pub fn query(cfg: &Config, data_dir: PathBuf, pattern: &str, show: usize) -> anyhow::Result<()> {
    let s = &cfg.search;
    let dir = kdx_search::search_dir(&data_dir);
    let se = Searcher::open(&dir, s.max_candidates as usize)?
        .with_context(|| format!("{} has no index: run `search rebuild`", dir.display()))?;
    let like = Like::new(pattern);
    let t0 = Instant::now();
    match se.candidates(&like, t0 + Duration::from_millis(s.timeout_ms)) {
        Ok(c) => {
            for (h, f, l) in c.hashes.iter().take(show) {
                println!(
                    "{}",
                    serde_json::json!({"hash": hex::encode(h), "first_h": f, "last_h": l})
                );
            }
            println!(
                "{}",
                serde_json::json!({
                    "matches": c.hashes.len(), "examined": c.examined, "truncated": c.truncated,
                    "docs": se.num_docs(), "ms": t0.elapsed().as_millis() as u64,
                })
            );
            Ok(())
        }
        Err(SearchError::NoIndexPath) => {
            println!("{}", serde_json::json!({"error": "no_index_path"}));
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
