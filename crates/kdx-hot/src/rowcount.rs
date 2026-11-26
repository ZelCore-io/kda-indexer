//! Stored per-key row counts (`RowCounts`, PR #48 part 2): the exact `totalCount` of the
//! posting-keyed GraphQL lists without a walk.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `RowCounts` | kind u8 (`kdx_decode::rowcount::CountKind`), entity u64, chain u8 | user rows u64, coinbase rows u64 |
//!
//! - The table is global, frozen and hot alike: `tail::apply` adds each tx's terms
//!   ([`kdx_decode::rowcount::tx_counts`]) in the block's write txn, and `tail::unwind_to`
//!   subtracts the same terms, recomputed from the record before it is deleted (the undo
//!   format does not change). The freezer moves records and leaves the counts alone.
//! - `Meta row_counts_ready` holds the TxNum frontier the counts were built at by
//!   `kda-indexer reconcile-counts` ([`set_ready`]). Readers use the table only while it is
//!   set ([`ready`]); every path that writes frozen ranges outside the freezer clears it
//!   ([`clear_ready`]), and so does a tx whose output does not render. Without the marker the
//!   table still follows the tail, it is just not trusted.
//! - Rollback guard (PR #48 review I-1): `tail::apply` and `tail::unwind_to_with` write
//!   `Meta row_counts_at@<chain>` = that chain's new `Tail` checkpoint (height and hash) in the
//!   txn that changes the counts, and `reconcile-counts` writes it for every chain when it sets
//!   the marker. A build before #48 moves the tips without it, so [`ready`] (and
//!   [`guard`], run at `run` and tail startup) sees the mismatch and the counts are not used.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use kdx_decode::rowcount::{tx_counts, CountKind};
use kdx_types::record::TxRecord;
use kdx_types::Dict;

use crate::checkpoint::Stage;
use crate::env::{RwTxn, Txn};
use crate::tables::Table;

/// Meta key of the readiness marker.
pub const READY: &str = "row_counts_ready";
/// Meta key prefix of the per-chain tip the counts follow (`row_counts_at@<chain>`).
pub const AT_PREFIX: &str = "row_counts_at@";

pub fn at_key(chain: u8) -> String {
    format!("{AT_PREFIX}{chain}")
}

/// Record that the counts include `chain` up to its current `Tail` checkpoint (absent when
/// the chain has none). Called in the write txn that changed the counts.
pub fn stamp(txn: &mut RwTxn<'_>, chain: u8) -> Result<()> {
    match txn.get(Table::StageCheckpoint, &[Stage::Tail as u8, chain])? {
        Some(v) => txn.set_meta(&at_key(chain), &v),
        None => txn.del(Table::Meta, at_key(chain).as_bytes()).map(drop),
    }
}

/// [`stamp`] every chain: the stamps become exactly the current tips (`reconcile-counts`).
pub fn stamp_all(txn: &mut RwTxn<'_>) -> Result<()> {
    for c in stamps(txn)?.into_keys() {
        txn.del(Table::Meta, at_key(c).as_bytes())?;
    }
    for (c, v) in tips(txn)? {
        txn.set_meta(&at_key(c), &v)?;
    }
    Ok(())
}

fn tips(txn: &impl Txn) -> Result<BTreeMap<u8, Vec<u8>>> {
    let p = Stage::Tail as u8;
    Ok(txn
        .scan(Table::StageCheckpoint, &[p], |k| k.first() == Some(&p))?
        .into_iter()
        .filter_map(|(k, v)| Some((*k.get(1)?, v)))
        .collect())
}

fn stamps(txn: &impl Txn) -> Result<BTreeMap<u8, Vec<u8>>> {
    let p = AT_PREFIX.as_bytes();
    let mut out = BTreeMap::new();
    for (k, v) in txn.scan(Table::Meta, p, |k| k.starts_with(p))? {
        let c = std::str::from_utf8(&k[p.len()..])
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .with_context(|| format!("bad Meta key {}", String::from_utf8_lossy(&k)))?;
        out.insert(c, v);
    }
    Ok(out)
}

/// Why the stored counts do not follow the hot tips (None: every chain's stamp equals its
/// `Tail` checkpoint, and no chain without a tip has a stamp).
pub fn tip_mismatch(txn: &impl Txn) -> Result<Option<String>> {
    let (tips, stamps) = (tips(txn)?, stamps(txn)?);
    if tips == stamps {
        return Ok(None);
    }
    let show = |v: Option<&Vec<u8>>| match v {
        None => "none".to_string(),
        Some(v) if v.len() >= 4 => {
            format!(
                "height {}",
                u32::from_be_bytes(v[..4].try_into().unwrap_or_default())
            )
        }
        Some(_) => "unreadable".to_string(),
    };
    let c = tips
        .keys()
        .chain(stamps.keys())
        .find(|c| tips.get(c) != stamps.get(c))
        .copied()
        .unwrap_or_default();
    Ok(Some(format!(
        "chain {c}: counts follow {} but the tail tip is {} (blocks applied or unwound by a build without stored counts?)",
        show(stamps.get(&c)),
        show(tips.get(&c))
    )))
}

pub fn key(kind: CountKind, entity: u64, chain: u8) -> [u8; 10] {
    let mut k = [0u8; 10];
    k[0] = kind as u8;
    k[1..9].copy_from_slice(&entity.to_be_bytes());
    k[9] = chain;
    k
}

pub fn parse_key(k: &[u8]) -> Option<(CountKind, u64, u8)> {
    if k.len() != 10 {
        return None;
    }
    Some((
        CountKind::from_u8(k[0])?,
        u64::from_be_bytes(k[1..9].try_into().ok()?),
        k[9],
    ))
}

/// `(user, coinbase)` rows of one value.
pub fn decode_val(v: &[u8]) -> (u64, u64) {
    let g = |r: std::ops::Range<usize>| {
        v.get(r)
            .and_then(|b| b.try_into().ok())
            .map_or(0, u64::from_be_bytes)
    };
    (g(0..8), g(8..16))
}

pub fn encode_val((u, c): (u64, u64)) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[..8].copy_from_slice(&u.to_be_bytes());
    v[8..].copy_from_slice(&c.to_be_bytes());
    v
}

/// Whether the readiness marker is set (whatever the tips; see [`ready`]).
pub fn marker_set(txn: &impl Txn) -> Result<bool> {
    Ok(txn.meta(READY)?.is_some())
}

/// Whether the stored counts may answer: the table exists, the marker is set and every
/// chain's `row_counts_at` equals its hot tip, read in `txn`'s snapshot.
pub fn ready(txn: &impl Txn) -> Result<bool> {
    Ok(txn.has_table(Table::RowCounts) && marker_set(txn)? && tip_mismatch(txn)?.is_none())
}

/// Startup check (`run`, the tail): clear a set marker whose counts do not follow the tips,
/// logging why. Returns the reason when it cleared it.
pub fn guard(txn: &mut RwTxn<'_>) -> Result<Option<String>> {
    if !marker_set(txn)? {
        return Ok(None);
    }
    let Some(why) = tip_mismatch(txn)? else {
        return Ok(None);
    };
    tracing::warn!(reason = %why, "row counts: the counts do not follow the hot tips");
    clear_ready(txn)?;
    Ok(Some(why))
}

/// Set the marker at TxNum frontier `next_txnum` (the end of what `reconcile-counts` read).
pub fn set_ready(txn: &mut RwTxn<'_>, next_txnum: u64) -> Result<()> {
    txn.set_meta_u64(READY, next_txnum)
}

/// Clear the marker: the counts no longer describe the store; readers walk until the next
/// `reconcile-counts`. Returns whether it was set.
pub fn clear_ready(txn: &mut RwTxn<'_>) -> Result<bool> {
    let was = txn.meta(READY)?.is_some();
    if was {
        txn.del(Table::Meta, READY.as_bytes())?;
        tracing::warn!("row counts: readiness marker cleared; totalCount walks until `kda-indexer reconcile-counts`");
    }
    Ok(was)
}

/// `(user, coinbase)` rows stored for `(kind, entity)`, on `chain` or summed over all chains.
pub fn sum(txn: &impl Txn, kind: CountKind, entity: u64, chain: Option<u8>) -> Result<(u64, u64)> {
    if let Some(c) = chain {
        return Ok(txn
            .get(Table::RowCounts, &key(kind, entity, c))?
            .map_or((0, 0), |v| decode_val(&v)));
    }
    let from = key(kind, entity, 0);
    let rows = txn.scan(Table::RowCounts, &from, |k| k.get(..9) == Some(&from[..9]))?;
    Ok(rows.iter().fold((0, 0), |(u, c), (_, v)| {
        let (a, b) = decode_val(v);
        (u + a, c + b)
    }))
}

/// Add `sign` × the terms of `r` (1 at apply, -1 at unwind). A record whose output does not
/// render clears the marker instead (the walk fails on it too, so no count is trusted).
pub fn add_tx(txn: &mut RwTxn<'_>, r: &TxRecord, dict: &dyn Dict, sign: i64) -> Result<()> {
    let terms = match tx_counts(r, dict) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(chain = r.key.chain(), height = r.key.height(), pos = r.key.pos(),
                error = %e, "row counts: output does not render");
            clear_ready(txn)?;
            return Ok(());
        }
    };
    let cb = r.key.is_coinbase();
    for t in terms {
        let k = key(t.kind, t.entity, r.key.chain());
        let (mut u, mut c) = txn
            .get(Table::RowCounts, &k)?
            .map_or((0, 0), |v| decode_val(&v));
        let slot = if cb { &mut c } else { &mut u };
        *slot = if sign >= 0 {
            slot.saturating_add(t.rows)
        } else {
            match slot.checked_sub(t.rows) {
                Some(v) => v,
                None => {
                    // Only possible when the table was not built for this record (the marker
                    // is not set then, or the store is damaged): never trusted again as is.
                    clear_ready(txn)?;
                    0
                }
            }
        };
        if (u, c) == (0, 0) {
            txn.del(Table::RowCounts, &k)?;
        } else {
            txn.put(Table::RowCounts, &k, &encode_val((u, c)))?;
        }
    }
    Ok(())
}

/// Rows of one kind: `(entity, chain) -> (user, coinbase)`.
pub type KindMap = HashMap<(u64, u8), (u64, u64)>;
/// One kind's rows sorted by key (the table's order within the kind).
pub type SortedRows = Vec<SortedRow>;
/// `((entity, chain), (user, coinbase))`.
pub type SortedRow = ((u64, u8), (u64, u64));

/// Counts being built in memory (`reconcile-counts`), keyed like the table: one map per
/// [`CountKind`], so the write and the `--check` compare turn one kind at a time into a
/// sorted run ([`Tally::take_sorted`]) and the whole map never coexists with a whole copy.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tally([KindMap; CountKind::ALL.len()]);

impl Tally {
    pub fn len(&self) -> usize {
        self.0.iter().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = ((CountKind, u64, u8), (u64, u64))> + '_ {
        CountKind::ALL
            .iter()
            .zip(&self.0)
            .flat_map(|(&k, m)| m.iter().map(move |(&(e, c), &v)| ((k, e, c), v)))
    }

    pub fn get(&self, (kind, e, c): (CountKind, u64, u8)) -> Option<(u64, u64)> {
        self.0[kind as usize].get(&(e, c)).copied()
    }

    pub fn insert(&mut self, (kind, e, c): (CountKind, u64, u8), v: (u64, u64)) {
        self.0[kind as usize].insert((e, c), v);
    }

    /// Add the terms of `r`; `false` when its output does not render.
    pub fn add(&mut self, r: &TxRecord, dict: &dyn Dict) -> bool {
        let Ok(terms) = tx_counts(r, dict) else {
            return false;
        };
        let cb = r.key.is_coinbase();
        for t in terms {
            let e = self.0[t.kind as usize]
                .entry((t.entity, r.key.chain()))
                .or_insert((0, 0));
            if cb {
                e.1 += t.rows;
            } else {
                e.0 += t.rows;
            }
        }
        true
    }

    pub fn merge(&mut self, o: Tally) {
        for (mine, mut theirs) in self.0.iter_mut().zip(o.0) {
            if mine.len() < theirs.len() {
                std::mem::swap(mine, &mut theirs);
            }
            for (k, (u, c)) in theirs {
                let e = mine.entry(k).or_insert((0, 0));
                e.0 += u;
                e.1 += c;
            }
        }
    }

    /// Remove `kind`'s rows and return them sorted. Only that kind's map and its run are
    /// alive together (the map is freed when this returns).
    pub fn take_sorted(&mut self, kind: CountKind) -> SortedRows {
        let m = std::mem::take(&mut self.0[kind as usize]);
        let mut v: SortedRows = m.into_iter().collect();
        v.sort_unstable_by_key(|r| r.0);
        v
    }
}

/// Every `RowCounts` row as a [`Tally`] (tests).
pub fn read_all(txn: &impl Txn) -> Result<Tally> {
    let mut t = Tally::default();
    if !txn.has_table(Table::RowCounts) {
        return Ok(t);
    }
    for (k, v) in txn.scan(Table::RowCounts, &[], |_| true)? {
        let k = parse_key(&k).context("bad RowCounts key")?;
        t.insert(k, decode_val(&v));
    }
    Ok(t)
}

/// Stored rows read per cursor step of [`differing`].
const CMP_CHUNK: usize = 100_000;

/// Rows of the stored table that differ from `want`, the sorted counts of `kind` (a missing,
/// an extra or a different row each count 1): a merge against the table read in chunks of
/// [`CMP_CHUNK`] rows in `txn`'s snapshot, so the extra memory is one chunk.
pub fn differing(txn: &impl Txn, kind: CountKind, want: &[SortedRow]) -> Result<u64> {
    let mut diff = 0u64;
    let mut w = want.iter().peekable();
    let kb = kind as u8;
    let mut after: Option<Vec<u8>> = None;
    let first = [kb];
    loop {
        let rows = match &after {
            None => txn.scan_limit(Table::RowCounts, &first, CMP_CHUNK, |k| {
                k.first() == Some(&kb)
            })?,
            Some(a) => txn.scan_chunk(Table::RowCounts, Some(a), CMP_CHUNK, |k| {
                k.first() == Some(&kb)
            })?,
        };
        let Some((last, _)) = rows.last() else {
            break;
        };
        after = Some(last.clone());
        for (k, v) in &rows {
            let got = parse_key(k).map(|(_, e, c)| ((e, c), decode_val(v)));
            let Some((gk, gv)) = got else {
                diff += 1;
                continue;
            };
            // Wanted rows below this stored key are missing from the table.
            while w.next_if(|r| r.0 < gk).is_some() {
                diff += 1;
            }
            match w.next_if(|r| r.0 == gk) {
                Some(r) if r.1 == gv => {}
                _ => diff += 1,
            }
        }
    }
    Ok(diff + w.count() as u64)
}

/// Stored rows of no known kind (a damaged table): each one differs.
pub fn unknown_kind_rows(txn: &impl Txn) -> Result<u64> {
    let from = [CountKind::ALL.len() as u8];
    Ok(txn.scan(Table::RowCounts, &from, |_| true)?.len() as u64)
}

/// Delete every `RowCounts` row: one MDBX clear of the table, in `txn` (atomic with whatever
/// else it does, such as clearing the marker).
pub fn clear_table(txn: &mut RwTxn<'_>) -> Result<()> {
    txn.clear(Table::RowCounts)
}
