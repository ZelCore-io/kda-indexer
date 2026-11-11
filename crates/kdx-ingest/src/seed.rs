//! The balance seed (ruling B-2, plan Task 9): mark every known `(account, module, chain)`
//! dirty once, so the reconciler reads it from the node.
//!
//! - Rows: every `xfer` entity `(account, module)` on each chain its postings touch, and every
//!   `acct` entity as `(account, coin)` on each chain its postings touch (pre-event senders,
//!   miners, zero-balance creates). A posting's chain comes from its TxKey (`keys.ef` for
//!   segments). The walk of one entity stops at [`MAX_CHAINS`] chains.
//! - `Tokens.first_height` from the oldest `xfer` posting per module.
//! - Order: the hot `TailPostings` first, then the segments (opened after the hot pass, so a
//!   block the freezer moves meanwhile is seen in one of the two). Blocks the tail applies
//!   later are marked by the tail itself (B-3).
//! - Marks are inserted with height 0 and never overwrite a tail mark.
//! - Batches of [`BATCH`] rows per write txn, each with the resume cursor in `Meta
//!   balances_seed_cursor` (range start, range end, family, entity index). A rerun redoes the
//!   hot pass (idempotent) and resumes the segments at the cursor; a range merged meanwhile is
//!   redone from its first entity. The end sets `Meta balances_seeded` (the highest tail
//!   height) and removes the cursor.
//! - Reads: a fresh hot read txn per chunk; segment readers are mmaps (no txn).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use kdx_decode::Family;
use kdx_hot::balances::{self, bal_key, meta};
use kdx_hot::tables::parse_txkey;
use kdx_hot::{checkpoint, HotStore, Stage, Table, Txn};
use kdx_segment::Segments;
use kdx_types::MemDict;
use serde::Serialize;

/// Rows per write txn.
pub const BATCH: usize = 100_000;
/// Chains per entity after which its walk stops (every chain of the mainnet graph).
pub const MAX_CHAINS: u32 = 20;
/// Hot `TailPostings` rows per read txn.
const HOT_CHUNK: usize = 100_000;

#[derive(Debug, Default, Serialize)]
pub struct SeedReport {
    pub hot_postings: u64,
    pub ranges: u64,
    pub ranges_skipped: u64,
    pub entities: u64,
    /// Dirty rows inserted (rows already dirty are not counted).
    pub dirty_inserted: u64,
    pub tokens_noted: u64,
    pub seeded_height: Option<u32>,
    pub stopped: bool,
    pub secs: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    start: u32,
    end: u32,
    family: u8,
    idx: u32,
}

impl Cursor {
    fn encode(&self) -> [u8; 13] {
        let mut v = [0u8; 13];
        v[..4].copy_from_slice(&self.start.to_be_bytes());
        v[4..8].copy_from_slice(&self.end.to_be_bytes());
        v[8] = self.family;
        v[9..].copy_from_slice(&self.idx.to_be_bytes());
        v
    }
    fn decode(v: &[u8]) -> Option<Cursor> {
        if v.len() != 13 {
            return None;
        }
        Some(Cursor {
            start: u32::from_be_bytes(v[..4].try_into().ok()?),
            end: u32::from_be_bytes(v[4..8].try_into().ok()?),
            family: v[8],
            idx: u32::from_be_bytes(v[9..].try_into().ok()?),
        })
    }
}

/// Pending writes of one batch.
#[derive(Default)]
struct Pending {
    rows: Vec<[u8; 9]>,
    tokens: HashMap<u32, u32>,
}

impl Pending {
    fn token(&mut self, module: u32, h: u32) {
        let e = self.tokens.entry(module).or_insert(h);
        *e = (*e).min(h);
    }
    fn flush(
        &mut self,
        hot: &HotStore,
        cursor: Option<Cursor>,
        rep: &mut SeedReport,
    ) -> Result<()> {
        let mut txn = hot.rw()?;
        let mark = balances::Mark {
            height: 0,
            seq: txn.txn_id()?,
        }
        .encode();
        for k in self.rows.drain(..) {
            if txn.insert(Table::BalanceDirty, &k, &mark)? {
                rep.dirty_inserted += 1;
            }
        }
        for (m, h) in self.tokens.drain() {
            balances::note_token(&mut txn, m, h)?;
            rep.tokens_noted += 1;
        }
        if let Some(c) = cursor {
            txn.set_meta(meta::BALANCES_SEED_CURSOR, &c.encode())?;
        }
        txn.commit()
    }
}

/// Whether the seed has finished on this store.
pub fn seeded(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(meta::BALANCES_SEEDED)?.is_some())
}

/// Rows of one posting: `(account, module, chain)` keys for its chain set.
fn push_rows(p: &mut Pending, family: u8, entity: u64, coin: Option<u32>, chains: u32) {
    let (account, module) = if family == Family::Xfer as u8 {
        ((entity >> 32) as u32, entity as u32)
    } else {
        match (coin, u32::try_from(entity)) {
            (Some(c), Ok(a)) => (a, c),
            _ => return,
        }
    };
    for c in 0..32u8 {
        if chains & (1 << c) != 0 {
            p.rows.push(bal_key(account, module, c));
        }
    }
}

/// Run (or resume) the seed. `stop` is polled between entities; a stopped seed commits its
/// batch and cursor and returns with `stopped`.
pub fn run(hot: &HotStore, segdir: &Path, stop: &dyn Fn() -> bool) -> Result<SeedReport> {
    let t0 = Instant::now();
    let mut rep = SeedReport::default();
    let (coin, cursor) = {
        let txn = hot.ro()?;
        (
            balances::coin_module(&txn)?,
            txn.meta(meta::BALANCES_SEED_CURSOR)?
                .as_deref()
                .and_then(Cursor::decode),
        )
    };
    let mut p = Pending::default();

    // Hot pass: TailPostings of `xfer` (0) and `acct` (1).
    for fam in [Family::Xfer as u8, Family::Acct as u8] {
        let mut after: Option<Vec<u8>> = Some(vec![fam]);
        let mut first = true;
        loop {
            let rows = {
                let txn = hot.ro()?;
                if first {
                    txn.scan_limit(Table::TailPostings, &[fam], HOT_CHUNK, |k| k[0] == fam)?
                } else {
                    txn.scan_chunk(Table::TailPostings, after.as_deref(), HOT_CHUNK, |k| {
                        k[0] == fam
                    })?
                }
            };
            first = false;
            let Some((last, _)) = rows.last() else { break };
            after = Some(last.clone());
            for (k, _) in &rows {
                let entity = u64::from_be_bytes(k[1..9].try_into().context("posting key")?);
                let key = parse_txkey(&k[9..17]).context("posting key")?;
                push_rows(&mut p, fam, entity, coin, 1 << key.chain());
                if fam == Family::Xfer as u8 {
                    p.token(entity as u32, key.height());
                }
                rep.hot_postings += 1;
            }
            if p.rows.len() >= BATCH {
                p.flush(hot, None, &mut rep)?;
            }
            if rows.len() < HOT_CHUNK || stop() {
                break;
            }
        }
    }
    p.flush(hot, None, &mut rep)?;
    if stop() {
        rep.stopped = true;
        rep.secs = t0.elapsed().as_secs_f64();
        return Ok(rep);
    }

    // Segment pass, in manifest order, resuming at the cursor.
    let segs = Segments::open(segdir, Arc::new(MemDict::new()))?;
    for (r, e) in segs.ranges.iter().zip(&segs.manifest.entries) {
        let (fam0, idx0) = match cursor {
            Some(c) if e.end <= c.start => {
                rep.ranges_skipped += 1;
                continue;
            }
            Some(c) if (e.start, e.end) == (c.start, c.end) => (c.family, c.idx),
            _ => (Family::Xfer as u8, 0),
        };
        rep.ranges += 1;
        for fam in [Family::Xfer, Family::Acct] {
            let f = fam as u8;
            if f < fam0 {
                continue;
            }
            let post = &r.postings[fam as usize];
            let from = if f == fam0 { idx0 as usize } else { 0 };
            for i in from..post.entities() {
                if stop() {
                    let c = Cursor {
                        start: e.start,
                        end: e.end,
                        family: f,
                        idx: i as u32,
                    };
                    p.flush(hot, Some(c), &mut rep)?;
                    rep.stopped = true;
                    rep.secs = t0.elapsed().as_secs_f64();
                    return Ok(rep);
                }
                let entity = post.entity_at(i);
                let list = post.list_at(i)?;
                let mut chains = 0u32;
                for &rel in &list {
                    if let Some(k) = r.txkey_rel(u64::from(rel)) {
                        chains |= 1 << k.chain();
                        if chains.count_ones() >= MAX_CHAINS {
                            break;
                        }
                    }
                }
                if fam == Family::Xfer {
                    if let Some(k) = list.first().and_then(|&rel| r.txkey_rel(u64::from(rel))) {
                        p.token(entity as u32, k.height());
                    }
                }
                push_rows(&mut p, f, entity, coin, chains);
                rep.entities += 1;
                if p.rows.len() >= BATCH {
                    let c = Cursor {
                        start: e.start,
                        end: e.end,
                        family: f,
                        idx: (i + 1) as u32,
                    };
                    p.flush(hot, Some(c), &mut rep)?;
                }
            }
        }
        // Range done: the next one starts from its first entity.
        let c = Cursor {
            start: e.end,
            end: e.end,
            family: Family::Xfer as u8,
            idx: 0,
        };
        p.flush(hot, Some(c), &mut rep)?;
    }

    // Done: the highest tail height marks the seed point.
    let mut txn = hot.rw()?;
    let mut top = txn.frozen_frontier()?;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(&txn, Stage::Tail, c)? {
            top = top.max(h);
        }
    }
    txn.set_meta(meta::BALANCES_SEEDED, &top.to_be_bytes())?;
    txn.del(Table::Meta, meta::BALANCES_SEED_CURSOR.as_bytes())?;
    txn.commit()?;
    rep.seeded_height = Some(top);
    rep.secs = t0.elapsed().as_secs_f64();
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_codec_and_rows() {
        let c = Cursor {
            start: 1,
            end: 2,
            family: 1,
            idx: 77,
        };
        assert_eq!(Cursor::decode(&c.encode()), Some(c));
        let mut p = Pending::default();
        push_rows(&mut p, 0, (5u64 << 32) | 9, None, 0b101);
        push_rows(&mut p, 1, 5, Some(3), 0b10);
        push_rows(&mut p, 1, 6, None, 0b10);
        assert_eq!(
            p.rows,
            vec![bal_key(5, 9, 0), bal_key(5, 9, 2), bal_key(5, 3, 1)]
        );
    }
}
