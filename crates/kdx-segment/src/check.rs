//! `segments check [--deep]`: manifest consistency, blake3 of every file, TxNum density,
//! orphan files; `deep` also decodes every record and resolves every rk, bh and posting list,
//! checks `sndr ⊆ acct` per entity and runs every stored rk/bh key through the range's key
//! filter (a false negative there would be a 404 for a stored key).

use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use kdx_decode::Family;
use kdx_types::record::{decode_block_with, decode_tx};
use kdx_types::Hash32;
use serde::Serialize;

use crate::blockfile::{row_len, row_record};
use crate::files::{blake3_file, file_name, range_file_names, FileKind};
use crate::manifest::Manifest;
use crate::phf::KeyIndex;
use crate::reader::RangeReader;
use crate::writer::RangeInfo;

#[derive(Debug, Default, Clone, Serialize)]
pub struct CheckReport {
    pub ranges: usize,
    pub files_ok: usize,
    pub blake3_bad: Vec<String>,
    /// `(expected first TxNum, found)` where entries do not continue each other.
    pub txnum_gaps: Vec<(u64, u64)>,
    pub deep_decode_errors: u64,
    pub rk_misses: u64,
    pub bh_misses: u64,
    /// Stored request keys / block hashes a range's `kf.fil` rules out (deep).
    pub filter_misses: u64,
    /// `(sndr entity, tx)` postings with no `acct` posting of the same entity (deep).
    pub sndr_not_in_acct: u64,
    /// Files of ranges not in the manifest, and leftover temp dirs.
    pub orphan_files: Vec<String>,
    /// Other failures (missing files, unreadable headers, count mismatches), one line each.
    pub errors: Vec<String>,
    pub deep: bool,
    pub txs_checked: u64,
    pub blocks_checked: u64,
    pub posting_lists_checked: u64,
}

impl CheckReport {
    pub fn ok(&self) -> bool {
        self.blake3_bad.is_empty()
            && self.txnum_gaps.is_empty()
            && self.deep_decode_errors == 0
            && self.rk_misses == 0
            && self.bh_misses == 0
            && self.filter_misses == 0
            && self.sndr_not_in_acct == 0
            && self.orphan_files.is_empty()
            && self.errors.is_empty()
    }
}

pub fn check(dir: &Path, deep: bool) -> Result<CheckReport> {
    let mut rep = CheckReport {
        deep,
        ..Default::default()
    };
    let m = match Manifest::load(dir) {
        Ok(Some(m)) => m,
        Ok(None) => {
            rep.errors.push("no manifest.json".into());
            return Ok(rep);
        }
        Err(e) => {
            rep.errors.push(format!("manifest: {e:#}"));
            return Ok(rep);
        }
    };
    rep.ranges = m.entries.len();
    let mut expect = m.entries.first().map(|e| e.first_txnum).unwrap_or(0);
    let mut known: HashSet<String> = HashSet::from(["manifest.json".to_string()]);
    for e in &m.entries {
        if e.first_txnum != expect {
            rep.txnum_gaps.push((expect, e.first_txnum));
        }
        expect = e.end_txnum();
        known.extend(range_file_names(&m.network, e.start, e.end));
        let rt = crate::swap::resolve(dir, &file_name(&m.network, FileKind::Range, e.start, e.end));
        let info = match RangeInfo::load(&rt) {
            Ok(i) => i,
            Err(err) => {
                rep.errors.push(format!("{}..{}: {err:#}", e.start, e.end));
                continue;
            }
        };
        rep.files_ok += 1;
        // The optional files it lists belong to the range too.
        known.extend(info.files.keys().cloned());
        for (name, st) in &info.files {
            let p = crate::swap::resolve(dir, name);
            match std::fs::metadata(&p) {
                Err(_) => rep.errors.push(format!("missing {name}")),
                Ok(md) if md.len() != st.bytes => rep.blake3_bad.push(format!("{name} (size)")),
                Ok(_) => match blake3_file(&p) {
                    Ok(h) if h == st.blake3 => rep.files_ok += 1,
                    Ok(_) => rep.blake3_bad.push(name.clone()),
                    Err(err) => rep.errors.push(format!("{name}: {err:#}")),
                },
            }
        }
        if deep {
            match RangeReader::open(dir, e, &m.network) {
                Ok(r) => deep_range(&r, &mut rep),
                Err(err) => rep.errors.push(format!("{}..{}: {err:#}", e.start, e.end)),
            }
        }
    }
    if expect != m.next_txnum && !m.entries.is_empty() {
        rep.txnum_gaps.push((expect, m.next_txnum));
    }
    for de in std::fs::read_dir(dir)? {
        let n = de?.file_name().to_string_lossy().into_owned();
        if n.starts_with(".tmp-")
            || (!known.contains(&n) && n.starts_with(&format!("{}-", m.network)))
        {
            rep.orphan_files.push(n);
        }
    }
    rep.orphan_files.sort();
    Ok(rep)
}

fn deep_range(r: &RangeReader, rep: &mut CheckReport) {
    let tag = format!("{}..{}", r.info.start, r.info.end);
    let mut cand = Vec::new();
    // A listed optional file the reader did not take (missing, or a key filter it does not
    // trust) is a finding: the range works, but slower or without the sender view.
    for (k, loaded) in [
        (FileKind::Sndr, r.sndr.is_some()),
        (FileKind::KeyFilter, r.filter.is_some()),
    ] {
        let n = file_name(&r.info.network, k, r.info.start, r.info.end);
        if r.info.files.contains_key(&n) && !loaded {
            rep.errors.push(format!(
                "{tag}: {n} is listed but was not loaded (see the warning)"
            ));
        }
    }
    // Txs: row by row; TxKey must match keys.ef; every request key must resolve.
    let mut rel = 0u64;
    for row_i in 0..r.txs.rows() {
        let row = match r.txs.row(row_i) {
            Ok(x) => x,
            Err(e) => {
                rep.deep_decode_errors += 1;
                rep.errors.push(format!("{tag} txs row {row_i}: {e:#}"));
                rel += r.info.block_records as u64;
                continue;
            }
        };
        let n = row_len(&row).unwrap_or(0);
        for i in 0..n {
            let ok = row_record(&row, i)
                .map_err(|e| e.to_string())
                .and_then(|b| decode_tx(b).map_err(|e| e.to_string()));
            match ok {
                Ok(tx) => {
                    if Some(tx.key) != r.txkey_rel(rel) {
                        rep.deep_decode_errors += 1;
                        if rep.errors.len() < 50 {
                            rep.errors
                                .push(format!("{tag} tx {rel}: key {:?} != keys.ef", tx.key));
                        }
                    }
                    if let Some(rk) = &tx.request_key {
                        r.rk.candidates(rk, &mut cand);
                        if !cand.contains(&(rel as u32)) {
                            rep.rk_misses += 1;
                        }
                        if !r.may_have_rk(rk) {
                            rep.filter_misses += 1;
                        }
                    }
                }
                Err(e) => {
                    rep.deep_decode_errors += 1;
                    if rep.errors.len() < 50 {
                        rep.errors.push(format!("{tag} tx {rel}: {e}"));
                    }
                }
            }
            rel += 1;
            rep.txs_checked += 1;
        }
    }
    if rel != r.info.tx_count {
        rep.errors.push(format!(
            "{tag}: {rel} tx records, range.toml says {}",
            r.info.tx_count
        ));
    }
    // Blocks: every present slot decodes (adjacent refs resolved), matches its slot, has txs,
    // and its hash resolves through bh.idx. Absent slots have no txs.
    for s in 0..r.slots() {
        let (c, h) = r.slot_pos(s);
        if !r.present(s) {
            if r.keys.count(s) != 0 {
                rep.errors
                    .push(format!("{tag}: absent block ({c}, {h}) has txs"));
            }
            continue;
        }
        rep.blocks_checked += 1;
        let raw = match r.block_raw(s) {
            Ok(Some(x)) => x,
            _ => {
                rep.deep_decode_errors += 1;
                continue;
            }
        };
        let resolve = |rc: u8, rh: u64| -> Option<Hash32> {
            let sl = r.slot(rc, u32::try_from(rh).ok()?)?;
            r.block_hash(sl).ok().flatten()
        };
        match decode_block_with(&raw, &resolve) {
            Ok(b) if b.chain == c && b.height == h as u64 && r.keys.count(s) > 0 => {
                r.bh.candidates(&b.hash, &mut cand);
                if !cand.contains(&(s as u32)) {
                    rep.bh_misses += 1;
                }
                if !r.may_have_bh(&b.hash) {
                    rep.filter_misses += 1;
                }
            }
            Ok(_) => {
                rep.deep_decode_errors += 1;
                rep.errors.push(format!(
                    "{tag}: block in slot ({c}, {h}) mismatched or without txs"
                ));
            }
            Err(e) => {
                rep.deep_decode_errors += 1;
                if rep.errors.len() < 50 {
                    rep.errors.push(format!("{tag}: block ({c}, {h}): {e}"));
                }
            }
        }
    }
    // Posting lists: decode, ascending, inside the range; a Roaring list's lazy walk (the
    // read path) equals its full decode from three bounds.
    for f in Family::ALL {
        let Some(p) = r.family(f) else {
            continue;
        };
        for i in 0..p.entities() {
            rep.posting_lists_checked += 1;
            match p.list_at(i) {
                Ok(v)
                    if v.windows(2).all(|w| w[0] < w[1])
                        && v.last().is_none_or(|&x| (x as u64) < r.info.tx_count)
                        && !v.is_empty() =>
                {
                    let seed = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ f as u64;
                    let why = match p.lazy_walk_matches(i, &v, seed) {
                        Ok(true) => None,
                        Ok(false) => Some("lazy Roaring walk differs from the full decode".into()),
                        Err(e) => Some(format!("lazy Roaring walk: {e:#}")),
                    };
                    if let Some(why) = why {
                        rep.deep_decode_errors += 1;
                        rep.errors
                            .push(format!("{tag} {}: list {i}: {why}", f.name()));
                    }
                }
                Ok(_) => {
                    rep.deep_decode_errors += 1;
                    rep.errors.push(format!(
                        "{tag} {}: list {i} out of order or range",
                        f.name()
                    ));
                }
                Err(e) => {
                    rep.deep_decode_errors += 1;
                    rep.errors
                        .push(format!("{tag} {}: list {i}: {e:#}", f.name()));
                }
            }
        }
    }
    sndr_within_acct(r, &tag, rep);
}

/// Every `sndr` list is a subset of the `acct` list of the same entity (the sender of a tx
/// always gets an `acct` posting too).
fn sndr_within_acct(r: &RangeReader, tag: &str, rep: &mut CheckReport) {
    let Some(sndr) = &r.sndr else {
        return;
    };
    let acct = &r.postings[Family::Acct as usize];
    for i in 0..sndr.entities() {
        let ent = sndr.entity_at(i);
        let Ok(list) = sndr.list_at(i) else {
            continue; // counted above
        };
        let have: HashSet<u64> = acct.list(ent).map(|it| it.collect()).unwrap_or_default();
        let first = r.info.first_txnum;
        let out = list
            .iter()
            .filter(|&&rel| !have.contains(&(first + u64::from(rel))))
            .count() as u64;
        if out > 0 {
            rep.sndr_not_in_acct += out;
            if rep.errors.len() < 50 {
                rep.errors.push(format!(
                    "{tag} sndr: entity {ent}: {out} txs without an acct posting"
                ));
            }
        }
    }
}
