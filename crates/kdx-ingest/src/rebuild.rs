//! `rebuild-range` (T-4): refetch one range or step from the node, build it again with the
//! backfill writer into `segments/rebuild.<start>/`, compare it with the stored one (record
//! counts, content digest, blake3 of every file) and swap it in crash-safely
//! (`kdx_segment::swap`). Used for repair (a damaged file) and deep-reorg recovery.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use kdx_segment::content::{self, ContentSummary};
use kdx_segment::files::{blake3_file, range_file_names};
use kdx_segment::{swap, Manifest, RangeEntry, RangeKind, RangeReader, RangeWriter, WriteOpts};
use serde::Serialize;

use crate::backfill::{fetch_unit, Unit};
use crate::canon::{HeaderMode, Walker};
use crate::ctx::Ctx;
use crate::freezer::crash_at;
use crate::pipeline::{self, parse_spool_key};

/// What `rebuild-range` did (printed as JSON; also the swap journal).
#[derive(Debug, Clone, Serialize)]
pub struct RebuildReport {
    pub kind: &'static str,
    pub start: u32,
    pub end: u32,
    pub dry_run: bool,
    pub tx_count: u64,
    pub block_count: u64,
    /// `None` when the stored files could not be read (repair).
    pub old_content: Option<ContentSummary>,
    pub old_error: Option<String>,
    pub new_content: ContentSummary,
    pub content_equal: Option<bool>,
    /// blake3 of every file: stored and rebuilt (`-` for a missing stored file).
    pub old_blake3: BTreeMap<String, String>,
    pub new_blake3: BTreeMap<String, String>,
    pub bytes_equal: bool,
    pub swapped: bool,
}

fn file_hashes(dir: &std::path::Path, net: &str, start: u32, end: u32) -> BTreeMap<String, String> {
    range_file_names(net, start, end)
        .into_iter()
        .map(|n| {
            let h = blake3_file(&dir.join(&n)).unwrap_or_else(|_| "-".into());
            (n, h)
        })
        .collect()
}

/// Rebuild the manifest entry of `kind` starting at `start`. The caller holds the data dir
/// lock. `--dry-run` builds and compares, then removes the rebuild dir.
pub async fn run(
    ctx: Arc<Ctx>,
    kind: RangeKind,
    start: u32,
    mode: HeaderMode,
    dry_run: bool,
) -> Result<RebuildReport> {
    let net = ctx.opts.network.clone();
    for l in swap::recover(&ctx.segdir)? {
        tracing::warn!("rebuild: {l}");
    }
    let manifest = Manifest::load(&ctx.segdir)?.context("no manifest.json")?;
    let entry = manifest
        .entries
        .iter()
        .find(|e| e.start == start && e.kind == kind)
        .cloned()
        .with_context(|| format!("no {} starting at {start} in the manifest", kind.name()))?;

    // The stored side: content digest when readable, blake3 of every file regardless.
    let (old_content, old_error) = {
        let dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        match RangeReader::open(&ctx.segdir, &entry, &net).and_then(|r| content::range(&r, &*dict))
        {
            Ok(c) => (Some(c), None),
            Err(e) => (None, Some(format!("{e:#}"))),
        }
    };
    let old_blake3 = file_hashes(&ctx.segdir, &net, entry.start, entry.end);

    let rdir = swap::rebuild_dir(&ctx.segdir, start);
    swap::remove(&ctx.segdir, start)?;
    std::fs::create_dir_all(&rdir)?;
    let built = build(ctx.clone(), &entry, mode).await;
    let new_entry = match built {
        Ok(e) => e,
        Err(e) => {
            let _ = swap::remove(&ctx.segdir, start);
            return Err(e);
        }
    };
    crash_at("rebuild-after-build");
    let cleanup = |e: anyhow::Error| -> anyhow::Error {
        let _ = swap::remove(&ctx.segdir, start);
        e
    };
    if new_entry != entry {
        return Err(cleanup(anyhow::anyhow!(
            "rebuilt {}..{} holds {} txs / {} blocks from TxNum {}, the manifest {} / {} from {}: not swapped",
            start,
            entry.end,
            new_entry.tx_count,
            new_entry.block_count,
            new_entry.first_txnum,
            entry.tx_count,
            entry.block_count,
            entry.first_txnum
        )));
    }
    let new_content = {
        let dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        RangeReader::open(&rdir, &new_entry, &net)
            .and_then(|r| content::range(&r, &*dict))
            .map_err(cleanup)?
    };
    let new_blake3 = file_hashes(&rdir, &net, entry.start, entry.end);
    let content_equal = old_content.as_ref().map(|c| *c == new_content);
    let bytes_equal = old_blake3 == new_blake3;
    for (n, b) in &new_blake3 {
        let a = &old_blake3[n];
        tracing::info!(file = %n, old = %a, new = %b, same = a == b, "rebuild: blake3");
    }
    match content_equal {
        Some(true) => tracing::info!(
            start,
            content = %new_content.blake3,
            bytes_equal,
            "rebuild: content equal"
        ),
        Some(false) => tracing::warn!(
            start,
            old = %old_content.as_ref().map(|c| c.blake3.as_str()).unwrap_or("-"),
            new = %new_content.blake3,
            "rebuild: content differs from the stored range (the node's blocks win; rebuild the search index and re-mark balances if this was not a repair)"
        ),
        None => {
            tracing::warn!(start, error = ?old_error, "rebuild: stored range unreadable, repairing")
        }
    }
    let mut rep = RebuildReport {
        kind: kind.name(),
        start,
        end: entry.end,
        dry_run,
        tx_count: entry.tx_count,
        block_count: entry.block_count,
        old_content,
        old_error,
        new_content,
        content_equal,
        old_blake3,
        new_blake3,
        bytes_equal,
        swapped: false,
    };
    if dry_run {
        swap::remove(&ctx.segdir, start)?;
        return Ok(rep);
    }

    // Commit point: the journal. Readers take the rebuilt files from here on.
    crash_at("rebuild-before-rename");
    rep.swapped = true;
    swap::commit_journal(&ctx.segdir, start, &serde_json::to_vec_pretty(&rep)?)?;
    crash_at("rebuild-after-rename");
    let mut names = range_file_names(&net, entry.start, entry.end);
    let toml = names
        .iter()
        .position(|n| n.ends_with(".toml"))
        .context("no range.toml name")?;
    let t = names.remove(toml);
    names.push(t);
    swap::finish(&ctx.segdir, start, &names, &|| crash_at("rebuild-mid-swap"))?;
    manifest.commit(&ctx.segdir)?;
    crash_at("rebuild-before-cleanup");
    swap::remove(&ctx.segdir, start)?;
    Ok(rep)
}

/// Fetch and build `entry`'s heights into the rebuild dir; returns the written entry.
async fn build(ctx: Arc<Ctx>, entry: &RangeEntry, mode: HeaderMode) -> Result<RangeEntry> {
    let cut = ctx.node.cut().await?;
    let walker = Arc::new(Walker::new(mode, cut));
    let u = Unit {
        kind: entry.kind,
        start: entry.start,
        end: entry.end,
    };
    let fetched = fetch_unit(ctx.clone(), walker, u, 64 << 20).await?;
    let entry = entry.clone();
    let c = ctx.clone();
    tokio::task::spawn_blocking(move || {
        let r = write(&c, &entry, fetched.coll);
        if r.is_err() {
            let mut d = c.dict.lock().unwrap_or_else(|e| e.into_inner());
            c.reload_dict(&mut d);
        }
        r
    })
    .await?
}

fn write(ctx: &Ctx, entry: &RangeEntry, coll: crate::etl::Collector) -> Result<RangeEntry> {
    let o = &ctx.opts;
    let expected: u64 = (entry.start..entry.end)
        .map(|h| ctx.graph.chains_at(u64::from(h)).len() as u64)
        .sum();
    if coll.len() != expected {
        bail!(
            "{}..{}: fetched {} blocks, expected {expected}",
            entry.start,
            entry.end,
            coll.len()
        );
    }
    let level = match entry.kind {
        RangeKind::Range => o.range_level,
        _ => o.step_level,
    };
    let mut w = RangeWriter::create(
        &swap::rebuild_dir(&ctx.segdir, entry.start),
        &ctx.etl,
        &o.network,
        entry.kind,
        entry.start,
        entry.end,
        entry.first_txnum,
        WriteOpts::new(level, o.dict_samples),
    )?;
    let mut dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
    let mut prev: BTreeMap<u8, (u32, kdx_types::Hash32)> = BTreeMap::new();
    let mut it = coll.into_sorted()?;
    let mut batch = Vec::with_capacity(1024);
    loop {
        batch.clear();
        for r in it.by_ref() {
            let (k, v) = r?;
            let (h, c) = parse_spool_key(&k)?;
            batch.push((h, c, v));
            if batch.len() == 1024 {
                break;
            }
        }
        if batch.is_empty() {
            break;
        }
        let mut prepared = pipeline::prepare_batch(&batch)?;
        prepared.iter_mut().for_each(pipeline::debug_corrupt_merkle);
        for p in &prepared {
            let hd = &p.0.header;
            let (c, h) = (hd.chain_id, hd.height as u32);
            // Legacy-era mismatches are stored unverified by backfill too (R-5); any
            // other mismatch aborts the rebuild.
            let m = pipeline::merkle_of(p);
            if !matches!(m.label(), "verified" | "mismatch_legacy") {
                bail!("chain {c} height {h}: payload Merkle {}", m.label());
            }
            if let Some(&(ph, phash)) = prev.get(&c) {
                if ph + 1 != h || hd.parent != phash {
                    bail!("linkage chain {c} height {h}: parent does not follow {ph}");
                }
            }
            let ib = pipeline::index(p, &mut *dict)?;
            w.push(&ib)?;
            prev.insert(c, (h, hd.hash));
        }
    }
    drop(it);
    let _ = std::fs::remove_dir_all(ctx.etl.join(format!("raw-{:09}", entry.start)));
    let new = w.finish()?;
    // Names first seen in the refetched blocks (deep reorg) must be durable before the swap.
    let mut txn = ctx.hot.rw()?;
    dict.persist(&mut txn)?;
    txn.commit()?;
    Ok(new)
}
