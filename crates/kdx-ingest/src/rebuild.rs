//! `rebuild-range` (T-4): refetch one range or step from the node, build it again with the
//! backfill writer into `segments/rebuild.<start>/`, compare it with the stored one (record
//! counts, content digest, blake3 of every file) and swap it in crash-safely
//! (`kdx_segment::swap`). Used for repair (a damaged file) and deep-reorg recovery.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use kdx_hot::{Table, Txn};
use kdx_segment::content::{self, ContentSummary};
use kdx_segment::files::{blake3_file, range_file_names};
use kdx_segment::{swap, Manifest, RangeEntry, RangeKind, RangeReader, RangeWriter, WriteOpts};
use kdx_types::record::decode_block;
use kdx_types::Hash32;
use serde::Serialize;

use crate::backfill::{fetch_unit, Prev, Unit};
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
    /// Balance rows marked dirty for the swap (M-7): every xfer/acct posting of the old and
    /// the new content, as the tail marks them. 0 when the content is equal or on a dry run.
    #[serde(default)]
    pub dirty_marks: u64,
    /// The search index was marked stale from `start` (M-7).
    #[serde(default)]
    pub search_marked_stale: bool,
}

/// (family, entity, chain) -> highest height of every xfer and acct posting in `r`: the
/// balance rows the range's txs can change (`balances::mark_posting`).
fn posting_marks(r: &RangeReader) -> Result<BTreeMap<(u8, u64, u8), u32>> {
    use kdx_decode::Family;
    let mut out = BTreeMap::new();
    for f in [Family::Xfer, Family::Acct] {
        let p = &r.postings[f as usize];
        for i in 0..p.entities() {
            let e = p.entity_at(i);
            for rel in p.list_at(i)? {
                let slot = r
                    .keys
                    .slot_of(u64::from(rel))
                    .context("posting past the range's txs")?;
                let (chain, height) = r.slot_pos(slot);
                let h = out.entry((f as u8, e, chain)).or_insert(height);
                *h = (*h).max(height);
            }
        }
    }
    Ok(out)
}

/// Linkage anchors around `entry` (m7), as backfill checks them: the last block hash per chain
/// of the entry that ends where it starts, and the parent hash per chain of the first height
/// after it (the next entry, else the hot store). An unreadable neighbour is not checked (a
/// WARN): two damaged neighbours must stay repairable.
fn anchors(ctx: &Ctx, m: &Manifest, entry: &RangeEntry) -> Result<(Prev, BTreeMap<u8, Hash32>)> {
    let net = &ctx.opts.network;
    let mut prev = Prev::new();
    if let Some(pe) = m.entries.iter().find(|e| e.end == entry.start) {
        let h = entry.start - 1;
        match RangeReader::open(&ctx.segdir, pe, net) {
            Ok(r) => {
                for &c in ctx.graph.chains_at(u64::from(h)) {
                    let slot = r.slot(c, h).context("previous entry: last height slot")?;
                    let hash = r
                        .block_hash(slot)?
                        .with_context(|| format!("segments miss block chain {c} height {h}"))?;
                    prev.insert(c, (h, hash));
                }
            }
            Err(e) => {
                tracing::warn!(start = pe.start, error = %format!("{e:#}"), "rebuild: previous entry unreadable, its linkage is not checked")
            }
        }
    }
    let mut next = BTreeMap::new();
    let h = entry.end;
    if let Some(ne) = m.entries.iter().find(|e| e.start == entry.end) {
        match RangeReader::open(&ctx.segdir, ne, net) {
            Ok(r) => {
                for &c in ctx.graph.chains_at(u64::from(h)) {
                    if let Some(b) = r.slot(c, h).map(|s| r.block_at(s)).transpose()?.flatten() {
                        next.insert(c, b.parent);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(start = ne.start, error = %format!("{e:#}"), "rebuild: next entry unreadable, its linkage is not checked")
            }
        }
    } else {
        let txn = ctx.hot.ro()?;
        for &c in ctx.graph.chains_at(u64::from(h)) {
            if let Some(v) = txn.get(Table::Headers, &kdx_hot::tables::block_key(c, h))? {
                next.insert(c, decode_block(&v)?.parent);
            }
        }
    }
    Ok((prev, next))
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
/// lock. `--dry-run` builds and compares, then removes the rebuild dir; it writes nothing to
/// the hot store (names first seen in the refetched blocks are dropped again, m6), but like
/// every run it first recovers other ranges' committed swaps (`swap::recover`).
///
/// When the content differs (or the stored range is unreadable), the balance rows of every
/// xfer/acct posting of the old and the new content are marked dirty and the search index is
/// marked stale from `start`, both before the swap commits (M-7).
pub async fn run(
    ctx: Arc<Ctx>,
    kind: RangeKind,
    start: u32,
    mode: HeaderMode,
    dry_run: bool,
) -> Result<RebuildReport> {
    let net = ctx.opts.network.clone();
    {
        // Snapshots hold this shared while they place segment files (m14).
        let _swap = swap::swap_lock(&ctx.segdir, true)?;
        for l in swap::recover(&ctx.segdir)? {
            tracing::warn!("rebuild: {l}");
        }
    }
    let manifest = Manifest::load(&ctx.segdir)?.context("no manifest.json")?;
    let entry = manifest
        .entries
        .iter()
        .find(|e| e.start == start && e.kind == kind)
        .cloned()
        .with_context(|| format!("no {} starting at {start} in the manifest", kind.name()))?;

    // The stored side: content digest and balance rows when readable, blake3 of every file
    // regardless.
    let (old_content, old_error, old_marks) = {
        let dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        match RangeReader::open(&ctx.segdir, &entry, &net)
            .and_then(|r| Ok((content::range(&r, &*dict)?, posting_marks(&r)?)))
        {
            Ok((c, m)) => (Some(c), None, m),
            Err(e) => (None, Some(format!("{e:#}")), BTreeMap::new()),
        }
    };
    let link = anchors(&ctx, &manifest, &entry)?;
    let old_blake3 = file_hashes(&ctx.segdir, &net, entry.start, entry.end);

    let rdir = swap::rebuild_dir(&ctx.segdir, start);
    swap::remove(&ctx.segdir, start)?;
    std::fs::create_dir_all(&rdir)?;
    let built = build(ctx.clone(), &entry, mode, link, dry_run).await;
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
    let (new_content, new_marks) = {
        let dict = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        RangeReader::open(&rdir, &new_entry, &net)
            .and_then(|r| Ok((content::range(&r, &*dict)?, posting_marks(&r)?)))
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
            "rebuild: content differs from the stored range (the node's blocks win); balances of both versions are marked dirty and the search index stale; run `search rebuild` unless the periodic rebuild is on"
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
        dirty_marks: 0,
        search_marked_stale: false,
    };
    if dry_run {
        swap::remove(&ctx.segdir, start)?;
        return Ok(rep);
    }

    // Held from here to the end: snapshots hold it shared while they place segment files
    // (m14), and `search rebuild` holds it shared from its census to its manifest commit, so
    // the stale mark below lands after any rebuild that read the old content (M-7).
    let _swap = swap::swap_lock(&ctx.segdir, true)?;

    // M-7: before the commit point, so a crash after it never loses them (marks for a swap
    // that then does not happen only cost a re-read).
    if content_equal != Some(true) {
        let mut txn = ctx.hot.rw()?;
        if kdx_hot::balances::marking() {
            let coin = kdx_hot::balances::coin_module(&txn)?;
            for (marks, note) in [(&old_marks, false), (&new_marks, true)] {
                for (&(f, e, c), &h) in marks {
                    let key = kdx_types::TxKey::new(h, c, 0);
                    kdx_hot::balances::mark_posting(&mut txn, f, e, key, coin, note)?;
                    rep.dirty_marks += 1;
                }
            }
        }
        txn.commit()?;
        let sdir = kdx_search::search_dir(ctx.segdir.parent().unwrap_or(&ctx.segdir));
        rep.search_marked_stale = kdx_search::manifest::mark_stale(&sdir, start)?;
        tracing::warn!(
            start,
            dirty_marks = rep.dirty_marks,
            search_marked_stale = rep.search_marked_stale,
            "rebuild: balances re-marked and search index marked stale for the changed range"
        );
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
async fn build(
    ctx: Arc<Ctx>,
    entry: &RangeEntry,
    mode: HeaderMode,
    link: (Prev, BTreeMap<u8, Hash32>),
    dry_run: bool,
) -> Result<RangeEntry> {
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
        let r = write(&c, &entry, fetched.coll, link, dry_run);
        if r.is_err() || dry_run {
            let mut d = c.dict.lock().unwrap_or_else(|e| e.into_inner());
            c.reload_dict(&mut d);
        }
        r
    })
    .await?
}

fn write(
    ctx: &Ctx,
    entry: &RangeEntry,
    coll: crate::etl::Collector,
    (mut prev, next): (Prev, BTreeMap<u8, Hash32>),
    dry_run: bool,
) -> Result<RangeEntry> {
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
            match prev.get(&c) {
                Some(&(ph, phash)) => {
                    if ph + 1 != h || hd.parent != phash {
                        bail!("linkage chain {c} height {h}: parent {} does not follow stored {ph} {phash}", hd.parent);
                    }
                }
                None => {
                    // m7: the first block of a chain must link to the entry before, as in
                    // backfill (an unreadable neighbour gave no anchor and was logged).
                    if h == entry.start
                        && h > 0
                        && ctx.graph.chains_at(u64::from(h) - 1).contains(&c)
                        && !prev.is_empty()
                    {
                        bail!("linkage chain {c} height {h}: no stored parent");
                    }
                }
            }
            let ib = pipeline::index(p, &mut *dict)?;
            w.push(&ib)?;
            prev.insert(c, (h, hd.hash));
        }
    }
    drop(it);
    let _ = std::fs::remove_dir_all(ctx.etl.join(format!("raw-{:09}", entry.start)));
    // m7: the last block of each chain must be the parent the next stored block names.
    for (c, parent) in &next {
        match prev.get(c) {
            Some((_, hash)) if hash == parent => {}
            Some((h, hash)) => bail!(
                "linkage chain {c} height {}: the stored block names parent {parent}, the rebuilt height {h} is {hash}: not swapped (rebuild the following range too)",
                entry.end
            ),
            // A chain that starts at `entry.end` has nothing to link to here.
            None if !ctx.graph.chains_at(u64::from(entry.end) - 1).contains(c) => {}
            None => bail!("linkage chain {c}: the rebuilt range holds no block below {}", entry.end),
        }
    }
    let new = w.finish()?;
    // Names first seen in the refetched blocks (deep reorg) must be durable before the swap;
    // a dry run drops them again (the caller reloads the dictionary, m6).
    if !dry_run {
        let mut txn = ctx.hot.rw()?;
        dict.persist(&mut txn)?;
        txn.commit()?;
    }
    Ok(new)
}
