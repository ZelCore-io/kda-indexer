//! Freezer (spec §4.4): moves hot tail heights into step files once every chain's stored tip
//! is `F` above them, and merges ten aligned steps into one range.
//!
//! Commit order of a step: write the step (`RangeWriter`, temp dir + renames) → commit the
//! manifest → **then** one MDBX txn deleting the step's hot rows (Headers, HeaderByHash,
//! TailTxs, TailRk, TailPostings, Undo) and advancing `frozen_frontier`, `next_txnum`, the
//! `Headers` checkpoints and `dict_lens@{end}`. A crash between the manifest and the MDBX txn
//! is repaired by [`reconcile`] on the next start (the hot rows at or below the manifest
//! frontier are deleted). A merge rewrites the steps' records at the range level, commits the
//! manifest replacing the ten entries, then deletes the step files; TxNums do not change.

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context, Result};
use kdx_decode::{Family, IndexedBlock, TxIndex};
use kdx_hot::checkpoint::set_checkpoint;
use kdx_hot::tables::{block_key, parse_txkey};
use kdx_hot::{dict, Stage, Table, Txn};
use kdx_segment::files::{fsync_dir, range_file_names};
use kdx_segment::{Manifest, RangeEntry, RangeKind, RangeReader, RangeWriter, WriteOpts};
use kdx_types::record::{decode_block, decode_tx};
use kdx_types::TxKey;

use crate::ctx::Ctx;
use crate::{backfill, disk, etl};

/// Meta key: the first height of a store started with `run --start-height` (dev).
pub const META_PARTIAL_FROM: &str = "partial_from";

fn crash_at(point: &str) {
    if std::env::var("KDX_CRASH_AT").ok().as_deref() == Some(point) {
        eprintln!("KDX_CRASH_AT={point}: aborting");
        std::process::abort();
    }
}

fn load_manifest(ctx: &Ctx) -> Result<Manifest> {
    Ok(Manifest::load(&ctx.segdir)?.unwrap_or_else(|| Manifest::new(&ctx.opts.network)))
}

/// Lowest stored tail tip over the chains present at `h` (None if a chain has none).
fn min_tail_tip(txn: &impl Txn, ctx: &Ctx, h: u32) -> Result<Option<u32>> {
    let mut m: Option<u32> = None;
    for &c in ctx.graph.chains_at(u64::from(h)) {
        match kdx_hot::checkpoint(txn, Stage::Tail, c)? {
            Some((t, _)) => m = Some(m.map_or(t, |x| x.min(t))),
            None => return Ok(None),
        }
    }
    Ok(m)
}

/// Delete the hot rows of every block below `end` and move the frontier there, in `txn`.
/// Returns the hashes at `end − 1` per chain (the new `Headers` checkpoints).
fn drop_hot_below(txn: &mut kdx_hot::RwTxn<'_>, end: u32) -> Result<u64> {
    let mut n = 0u64;
    // Txs (and the request keys they own) by TxKey order: height-major.
    let end_key = TxKey::new(end, 0, 0).to_be_bytes();
    let txs = txn.scan(Table::TailTxs, &[], |k| k < &end_key[..])?;
    for (k, v) in &txs {
        let r = decode_tx(v).context("hot tx")?;
        if let Some(rk) = &r.request_key {
            if txn.get(Table::TailRk, &rk.0)?.as_deref() == Some(&k[..]) {
                txn.del(Table::TailRk, &rk.0)?;
            }
        }
        txn.del(Table::TailTxs, k)?;
    }
    let posts = txn.scan(Table::TailPostings, &[], |_| true)?;
    for (k, _) in &posts {
        let key = parse_txkey(&k[9..]).context("posting key")?;
        if key.height() < end {
            txn.del(Table::TailPostings, k)?;
        }
    }
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        let rows = txn.scan(Table::Headers, &block_key(c, 0), |k| {
            k.first() == Some(&c) && k[1..5] < end.to_be_bytes()[..]
        })?;
        for (k, v) in &rows {
            let b = decode_block(v).context("hot header")?;
            if b.height + 1 == u64::from(end) {
                set_checkpoint(txn, Stage::Headers, c, end - 1, b.hash)?;
            }
            txn.del(Table::HeaderByHash, &b.hash.0)?;
            txn.del(Table::Headers, k)?;
            txn.del(Table::Undo, k)?;
            n += 1;
        }
    }
    txn.set_frozen_frontier(end)?;
    Ok(n)
}

/// On start: clear `etl/`, delete segment files the manifest does not list, and finish a
/// freeze that reached the manifest but not the hot store.
pub fn reconcile(ctx: &Ctx) -> Result<()> {
    etl::clear_dir(&ctx.etl)?;
    let m = load_manifest(ctx)?;
    let removed = backfill::remove_unlisted(ctx, &m)?;
    if !removed.is_empty() {
        tracing::warn!(files = ?removed, "freezer: removed files the manifest does not list");
    }
    if m.entries.is_empty() {
        return Ok(());
    }
    let hot_f = ctx.hot.ro()?.frozen_frontier()?;
    if m.frontier > hot_f {
        let _d = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        let mut txn = ctx.hot.rw()?;
        let n = drop_hot_below(&mut txn, m.frontier)?;
        txn.set_next_txnum(m.next_txnum)?;
        txn.set_meta(&dict::lens_key(m.frontier), &dict::encode_lens(&_d))?;
        txn.commit()?;
        tracing::warn!(from = hot_f, to = m.frontier, blocks = n, "freezer: finished an interrupted freeze (hot rows below the manifest frontier deleted)");
    } else if m.frontier < hot_f {
        bail!(
            "hot frozen_frontier {hot_f} is ahead of the manifest frontier {} with tail data present",
            m.frontier
        );
    }
    Ok(())
}

/// The step's blocks from the hot store, in (height, chain) order.
fn hot_step_blocks(ctx: &Ctx, start: u32, end: u32) -> Result<Vec<IndexedBlock>> {
    let txn = ctx.hot.ro()?;
    let lo = TxKey::new(start, 0, 0).to_be_bytes();
    let hi = TxKey::new(end, 0, 0).to_be_bytes();
    let mut posts: BTreeMap<TxKey, Vec<(Family, u64)>> = BTreeMap::new();
    for (k, _) in txn.scan(Table::TailPostings, &[], |_| true)? {
        let key = parse_txkey(&k[9..]).context("posting key")?;
        if key.height() >= start && key.height() < end {
            let f = Family::from_u8(k[0]).context("posting family")?;
            let e = u64::from_be_bytes(k[1..9].try_into()?);
            posts.entry(key).or_default().push((f, e));
        }
    }
    let mut txs: BTreeMap<(u32, u8), Vec<kdx_types::record::TxRecord>> = BTreeMap::new();
    for (_, v) in txn.scan(Table::TailTxs, &lo, |k| k < &hi[..])? {
        let r = decode_tx(&v).context("hot tx")?;
        txs.entry((r.key.height(), r.key.chain()))
            .or_default()
            .push(r);
    }
    let mut out = Vec::new();
    for h in start..end {
        for &c in ctx.graph.chains_at(u64::from(h)) {
            let v = txn
                .get(Table::Headers, &block_key(c, h))?
                .with_context(|| format!("freezer: hot block chain {c} height {h} missing"))?;
            let block = decode_block(&v)?;
            let t = txs.remove(&(h, c)).unwrap_or_default();
            let idx = t
                .iter()
                .map(|r| {
                    let mut p = posts.remove(&r.key).unwrap_or_default();
                    p.sort_unstable_by_key(|&(f, e)| (f as u8, e));
                    TxIndex {
                        key: r.key,
                        postings: p,
                        pact_id: None,
                        code_hash: None,
                    }
                })
                .collect();
            let cmd_reencoded = t.iter().filter(|r| r.cmd_reencoded).count() as u32;
            out.push(IndexedBlock {
                block,
                txs: t,
                idx,
                cmd_reencoded,
                unindexed_accounts: 0,
            });
        }
    }
    ensure!(
        txs.is_empty(),
        "freezer: hot txs without a block in {start}..{end}"
    );
    Ok(out)
}

/// One freezer step: freeze the next step if it is final, else merge if ten aligned steps
/// exist. Returns the new manifest entry, `None` when there was nothing to do.
pub fn tick(ctx: &Ctx) -> Result<Option<RangeEntry>> {
    if let Some(e) = freeze_step(ctx)? {
        return Ok(Some(e));
    }
    merge(ctx)
}

fn freeze_step(ctx: &Ctx) -> Result<Option<RangeEntry>> {
    let o = &ctx.opts;
    let step = u32::try_from(o.step_heights)?;
    let f = u32::try_from(o.finality_depth)?;
    let (start, first_txnum) = {
        let txn = ctx.hot.ro()?;
        let start = txn.frozen_frontier()?;
        let Some(min_tip) = min_tail_tip(&txn, ctx, start + step - 1)? else {
            return Ok(None);
        };
        if u64::from(start) + u64::from(step) > u64::from(min_tip.saturating_sub(f)) {
            return Ok(None);
        }
        (start, txn.next_txnum()?)
    };
    let end = start + step;
    let mut m = load_manifest(ctx)?;
    ensure!(
        m.entries.is_empty() || (m.frontier == start && m.next_txnum == first_txnum),
        "freezer: manifest {}/{} differs from hot frontier {start}/{first_txnum}",
        m.frontier,
        m.next_txnum
    );
    disk::ensure_free(&ctx.segdir, o.min_free_gb)?;
    let t0 = std::time::Instant::now();
    let blocks = hot_step_blocks(ctx, start, end)?;
    let mut w = RangeWriter::create(
        &ctx.segdir,
        &ctx.etl,
        &o.network,
        RangeKind::Step,
        start,
        end,
        first_txnum,
        WriteOpts::new(o.step_level, o.dict_samples),
    )?;
    for b in &blocks {
        w.push(b)?;
    }
    let entry = w.finish()?;
    m.push(entry.clone())?;
    m.commit(&ctx.segdir)?;
    crash_at("freeze-after-manifest");
    {
        let d = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
        let mut txn = ctx.hot.rw()?;
        let n = drop_hot_below(&mut txn, end)?;
        ensure!(
            n == blocks.len() as u64,
            "freezer: deleted {n} hot blocks, froze {}",
            blocks.len()
        );
        txn.set_next_txnum(entry.end_txnum())?;
        txn.set_meta(&dict::lens_key(end), &dict::encode_lens(&d))?;
        txn.commit()?;
    }
    tracing::info!(
        start,
        end,
        blocks = entry.block_count,
        txs = entry.tx_count,
        secs = t0.elapsed().as_secs_f64(),
        "freezer: step frozen"
    );
    Ok(Some(entry))
}

/// Merge the first ten (range/step) consecutive aligned steps into one range.
pub fn merge(ctx: &Ctx) -> Result<Option<RangeEntry>> {
    let o = &ctx.opts;
    let range = u32::try_from(o.range_heights)?;
    let mut m = load_manifest(ctx)?;
    let Some(i) = m
        .entries
        .iter()
        .position(|e| e.kind == RangeKind::Step && e.start % range == 0)
    else {
        return Ok(None);
    };
    let start = m.entries[i].start;
    let end = start + range;
    let Some(j) = m.entries.iter().position(|e| e.end == end) else {
        return Ok(None);
    };
    if m.entries[i..=j].iter().any(|e| e.kind != RangeKind::Step) {
        return Ok(None);
    }
    disk::ensure_free(&ctx.segdir, o.min_free_gb)?;
    let t0 = std::time::Instant::now();
    let olds: Vec<RangeEntry> = m.entries[i..=j].to_vec();
    let mut w = RangeWriter::create(
        &ctx.segdir,
        &ctx.etl,
        &o.network,
        RangeKind::Range,
        start,
        end,
        olds[0].first_txnum,
        WriteOpts::new(o.range_level, o.dict_samples),
    )?;
    for e in &olds {
        let r = RangeReader::open(&ctx.segdir, e, &o.network)?;
        // Postings per relative TxNum, from the step's posting files.
        let mut posts: Vec<Vec<(Family, u64)>> = vec![Vec::new(); e.tx_count as usize];
        for f in Family::ALL {
            let p = &r.postings[f as usize];
            for k in 0..p.entities() {
                let ent = p.entity_at(k);
                for rel in p.list_at(k)? {
                    posts
                        .get_mut(rel as usize)
                        .context("posting beyond the step's txs")?
                        .push((f, ent));
                }
            }
        }
        for slot in 0..r.slots() {
            let Some(block) = r.block_at(slot)? else {
                continue;
            };
            let first = r.keys.first(slot).context("keys first")?;
            let n = r.keys.count(slot);
            let mut txs = Vec::with_capacity(n as usize);
            let mut idx = Vec::with_capacity(n as usize);
            for rel in first..first + n {
                let t = r.tx_rel(rel)?;
                let mut p = std::mem::take(&mut posts[rel as usize]);
                p.sort_unstable_by_key(|&(f, e)| (f as u8, e));
                idx.push(TxIndex {
                    key: t.key,
                    postings: p,
                    pact_id: None,
                    code_hash: None,
                });
                txs.push(t);
            }
            let cmd_reencoded = txs.iter().filter(|r| r.cmd_reencoded).count() as u32;
            w.push(&IndexedBlock {
                block,
                txs,
                idx,
                cmd_reencoded,
                unindexed_accounts: 0,
            })?;
        }
    }
    let entry = w.finish()?;
    let replaced = m.replace_span(entry.clone())?;
    m.commit(&ctx.segdir)?;
    crash_at("merge-after-manifest");
    for e in &replaced {
        for name in range_file_names(&o.network, e.start, e.end) {
            let p = ctx.segdir.join(&name);
            if p.exists() {
                std::fs::remove_file(&p).with_context(|| format!("remove {}", p.display()))?;
            }
        }
    }
    fsync_dir(&ctx.segdir)?;
    tracing::info!(
        start,
        end,
        steps = replaced.len(),
        txs = entry.tx_count,
        secs = t0.elapsed().as_secs_f64(),
        "freezer: steps merged into a range"
    );
    Ok(Some(entry))
}
