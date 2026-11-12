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
    // The segment swap lock, shared, from the census to the manifest commit: `rebuild-range`
    // takes it exclusively before it marks the index stale and swaps a range, so a rebuild
    // either reads the swapped content or is marked stale after it commits (M-7). Waits
    // while a swap is in progress; snapshots share it.
    let _swap = if a.dry_run {
        None
    } else {
        Some(kdx_segment::swap::swap_lock(
            &a.data_dir.join("segments"),
            false,
        )?)
    };
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
    match se.candidates(
        &like,
        kdx_search::Window::ALL,
        t0 + Duration::from_millis(s.timeout_ms),
    ) {
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    /// M-7 follow-up: `search rebuild` waits while `rebuild-range` holds the segment swap
    /// lock (it marks the index stale and swaps under it), and goes on once it is released.
    #[test]
    fn search_rebuild_waits_for_a_segment_swap_in_progress() {
        let d = tempfile::tempdir().unwrap();
        let seg = d.path().join("segments");
        std::fs::create_dir_all(&seg).unwrap();
        let swap = kdx_segment::swap::swap_lock(&seg, true).unwrap();
        let (tx, rx) = mpsc::channel();
        let dir = d.path().to_path_buf();
        let cfg: Config = toml::from_str(
            "network = \"mainnet01\"\ndata_dir = \"unused\"\n[node]\nurl = \"http://127.0.0.1:1\"\n",
        )
        .unwrap();
        let t = std::thread::spawn(move || {
            let r = rebuild(
                &cfg,
                RebuildArgs {
                    data_dir: dir,
                    dry_run: false,
                    mode: None,
                    trial_docs: 0,
                },
            );
            tx.send(r.is_ok()).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "search rebuild went on during a swap"
        );
        drop(swap);
        // No hot store here: past the lock the rebuild fails, which is all this needs.
        rx.recv_timeout(Duration::from_secs(30))
            .expect("search rebuild still waiting after the swap");
        t.join().unwrap();
    }
}
