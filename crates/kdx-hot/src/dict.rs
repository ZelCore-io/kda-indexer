//! `HotDict`: the persistent interner (spec §3.2). All names are loaded into a `MemDict` at
//! open; new names are queued and written by `persist` in the same write txn as the data that
//! uses them. Backfill persists only when a range commits; the tail per batch.

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
            txn.put(Table::DictFwd(k), name.as_bytes(), &idb)?;
            txn.put(Table::DictRev(k), &idb, name.as_bytes())?;
        }
        self.pending.clear();
        Ok(())
    }
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
