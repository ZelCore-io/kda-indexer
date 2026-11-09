//! `HotDict`: the persistent interner (spec §3.2). All names are loaded into a `MemDict` at
//! open; new names are queued and written by `persist` in the same write txn as the data that
//! uses them. Backfill persists only when a range commits; the tail per batch.

use std::borrow::Cow;
use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};
use kdx_types::{Dict, DictKind, DictMut, MemDict};

use crate::env::{be_u32, RwTxn, Txn};
use crate::tables::Table;

#[derive(Debug, Default)]
pub struct HotDict {
    mem: MemDict,
    /// Names interned since the last `persist`, in id order per kind.
    pending: Vec<(DictKind, u32)>,
}

impl HotDict {
    /// Load every name; ids must be dense from 0 per kind.
    pub fn load(txn: &impl Txn) -> Result<HotDict> {
        let mut m: BTreeMap<DictKind, Vec<String>> = BTreeMap::new();
        for k in DictKind::ALL {
            let rows = txn.scan(Table::DictRev(k), &[], |_| true)?;
            let mut names = Vec::with_capacity(rows.len());
            for (i, (key, v)) in rows.into_iter().enumerate() {
                let id = be_u32(&key).context("DictRev key is not a u32")?;
                ensure!(
                    id as usize == i,
                    "{k:?} dictionary ids are not dense at {i}"
                );
                names.push(String::from_utf8(v).context("dictionary name is not utf-8")?);
            }
            m.insert(k, names);
        }
        Ok(HotDict {
            mem: MemDict::from(m),
            pending: Vec::new(),
        })
    }

    pub fn len(&self, k: DictKind) -> usize {
        self.mem.len(k)
    }

    pub fn is_empty(&self) -> bool {
        self.mem.is_empty()
    }

    /// Names interned but not yet persisted.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    pub fn mem(&self) -> &MemDict {
        &self.mem
    }

    /// Write the queued names (forward and reverse rows) in `txn`.
    pub fn persist(&mut self, txn: &mut RwTxn<'_>) -> Result<()> {
        for &(k, id) in &self.pending {
            let name = self.mem.name(k, id).expect("pending id was interned");
            let idb = id.to_be_bytes();
            txn.put(Table::DictFwd(k), &fwd_key(name.as_bytes()), &idb)?;
            txn.put(Table::DictRev(k), &idb, name.as_bytes())?;
        }
        self.pending.clear();
        Ok(())
    }
}

/// Names longer than this are keyed in `DictFwd` by their hash, so no key reaches the MDBX
/// key-size limit (about 2 KB at a 4 KB page).
pub const FWD_KEY_MAX: usize = 480;

/// `DictFwd` key of `name`: the name bytes, or `0xFF ‖ blake2b-256(name)` when the name is
/// longer than `FWD_KEY_MAX` (0xFF never starts UTF-8, so the two forms cannot collide).
pub fn fwd_key(name: &[u8]) -> Cow<'_, [u8]> {
    if name.len() <= FWD_KEY_MAX {
        Cow::Borrowed(name)
    } else {
        let mut k = Vec::with_capacity(33);
        k.push(0xFF);
        k.extend_from_slice(&kdx_types::blake2b_256(name).0);
        Cow::Owned(k)
    }
}

/// Meta key holding the dictionary sizes at a frozen frontier (backfill range commits).
pub fn lens_key(frontier: u32) -> String {
    format!("dict_lens@{frontier}")
}

/// Sizes per kind (`DictKind::ALL` order) as 5 × u32 BE.
pub fn encode_lens(d: &HotDict) -> Vec<u8> {
    DictKind::ALL
        .iter()
        .flat_map(|&k| (d.len(k) as u32).to_be_bytes())
        .collect()
}

pub fn decode_lens(v: &[u8]) -> Result<Vec<u32>> {
    ensure!(v.len() == 4 * DictKind::ALL.len(), "bad dict_lens value");
    Ok(v.chunks(4).map(|c| be_u32(c).unwrap_or(0)).collect())
}

/// Delete every name whose id is at or above `lens` (per kind, `DictKind::ALL` order).
/// Only for names no committed data can reference: backfill resume, when a range reached
/// MDBX but not the manifest and there is no tail. Returns the names removed.
pub fn truncate(txn: &mut RwTxn<'_>, lens: &[u32]) -> Result<u64> {
    let mut n = 0;
    for (i, &k) in DictKind::ALL.iter().enumerate() {
        let from = lens[i].to_be_bytes();
        for (id, name) in txn.scan(Table::DictRev(k), &from, |_| true)? {
            txn.del(Table::DictFwd(k), &fwd_key(&name))?;
            txn.del(Table::DictRev(k), &id)?;
            n += 1;
        }
    }
    Ok(n)
}

impl Dict for HotDict {
    fn find(&self, k: DictKind, s: &str) -> Option<u32> {
        self.mem.find(k, s)
    }
    fn name(&self, k: DictKind, id: u32) -> Option<&str> {
        self.mem.name(k, id)
    }
}

impl DictMut for HotDict {
    fn intern(&mut self, k: DictKind, s: &str) -> u32 {
        let before = self.mem.len(k);
        let id = self.mem.intern(k, s);
        if self.mem.len(k) != before {
            self.pending.push((k, id));
        }
        id
    }
}
