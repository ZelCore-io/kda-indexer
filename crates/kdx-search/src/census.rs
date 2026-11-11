//! B5 census: the exact set of distinct CodeHash prefixes (the `code` posting family's
//! entities) with the first and last range that holds each, by a k-way merge of every range's
//! `code.pst` entity directory plus the hot store's `TailPostings` family 4.
//!
//! Struct of arrays: 8 + 2 + 2 = 12 bytes per key in RAM.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use anyhow::{bail, Result};
use kdx_decode::Family;
use kdx_hot::{checkpoint, HotStore, Stage, Table, Txn};
use kdx_segment::Segments;

/// Hot rows read per read txn.
const HOT_CHUNK: usize = 100_000;

#[derive(Debug, Default)]
pub struct Census {
    /// Distinct prefixes, ascending.
    pub keys: Vec<u64>,
    /// Range index of the first range holding each key (`hot_idx` = the hot store).
    pub first: Vec<u16>,
    pub last: Vec<u16>,
    /// Last height (inclusive) of each range index; the hot store's is its highest tail
    /// checkpoint.
    pub range_last_h: Vec<u32>,
    /// Range index of the hot store (= number of segment ranges).
    pub hot_idx: u16,
    /// Manifest frontier of the segments the census read.
    pub frontier: u32,
}

impl Census {
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    /// `(first, last)` range index of a prefix.
    pub fn get(&self, prefix: u64) -> Option<(u16, u16)> {
        let i = self.keys.binary_search(&prefix).ok()?;
        Some((self.first[i], self.last[i]))
    }
    /// Last height of range index `r`.
    pub fn last_h(&self, r: u16) -> u32 {
        self.range_last_h
            .get(r as usize)
            .copied()
            .unwrap_or(u32::MAX)
    }
    /// Keys whose first range is `r`.
    pub fn count_first(&self, r: u16) -> usize {
        self.first.iter().filter(|&&f| f == r).count()
    }
}

/// Sorted, distinct `code` entities of the hot store (one read txn per chunk).
fn hot_code_keys(hot: &HotStore) -> Result<Vec<u64>> {
    let fam = Family::Code as u8;
    let mut out: Vec<u64> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let txn = hot.ro()?;
        let from = match &after {
            None => vec![fam],
            Some(a) => {
                let mut f = a.clone();
                f.push(0);
                f
            }
        };
        let mut n = 0usize;
        let mut last = None;
        txn.walk_keys(Table::TailPostings, &from, |k| {
            if k.first() != Some(&fam) || k.len() != 17 || n >= HOT_CHUNK {
                return false;
            }
            n += 1;
            let e = u64::from_be_bytes(k[1..9].try_into().expect("8"));
            if out.last() != Some(&e) {
                out.push(e);
            }
            last = Some(k.to_vec());
            true
        })?;
        match last {
            Some(l) if n >= HOT_CHUNK => after = Some(l),
            _ => break,
        }
    }
    out.dedup();
    Ok(out)
}

/// Highest tail checkpoint over all chains (None: no tail data).
pub fn hot_tip(hot: &HotStore) -> Result<Option<u32>> {
    let txn = hot.ro()?;
    let mut tip = None;
    for c in 0..kdx_segment::SLOT_WIDTH as u8 {
        if let Some((h, _)) = checkpoint(&txn, Stage::Tail, c)? {
            tip = tip.max(Some(h));
        }
    }
    Ok(tip)
}

/// The census over `seg` (a snapshot of the manifest) and the hot store.
pub fn distinct_code_keys(seg: &Segments, hot: &HotStore) -> Result<Census> {
    let n = seg.ranges.len();
    if n >= u16::MAX as usize {
        bail!("{n} ranges: the census indexes ranges by u16");
    }
    let hot_idx = n as u16;
    let hot_keys = hot_code_keys(hot)?;
    let fam = Family::Code as usize;
    let len = |r: usize| -> usize {
        if r == n {
            hot_keys.len()
        } else {
            seg.ranges[r].postings[fam].entities()
        }
    };
    let at = |r: usize, i: usize| -> u64 {
        if r == n {
            hot_keys[i]
        } else {
            seg.ranges[r].postings[fam].entity_at(i)
        }
    };
    let mut heap = BinaryHeap::with_capacity(n + 1);
    let mut total = 0usize;
    for r in 0..=n {
        total += len(r);
        if len(r) > 0 {
            heap.push(Reverse((at(r, 0), r as u16, 0usize)));
        }
    }
    let mut c = Census {
        hot_idx,
        frontier: seg.frontier(),
        ..Default::default()
    };
    // Distinct keys are at most `total`; the oldest-range share is usually most of them.
    c.keys.reserve(total / 2);
    while let Some(Reverse((e, r, i))) = heap.pop() {
        match c.keys.last() {
            Some(&k) if k == e => {
                let l = c.last.last_mut().expect("parallel arrays");
                *l = (*l).max(r);
                let f = c.first.last_mut().expect("parallel arrays");
                *f = (*f).min(r);
            }
            Some(&k) if k > e => bail!("range {r}: code.pst entities out of order"),
            _ => {
                c.keys.push(e);
                c.first.push(r);
                c.last.push(r);
            }
        }
        if i + 1 < len(r as usize) {
            heap.push(Reverse((at(r as usize, i + 1), r, i + 1)));
        }
    }
    c.range_last_h = seg
        .manifest
        .entries
        .iter()
        .map(|e| e.end.saturating_sub(1))
        .collect();
    c.range_last_h
        .push(hot_tip(hot)?.unwrap_or_else(|| seg.frontier().saturating_sub(1)));
    Ok(c)
}
