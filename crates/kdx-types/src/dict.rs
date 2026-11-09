//! Interning dictionaries (spec §3.2). `MemDict` is the in-memory version used
//! by P0; P2 replaces it with the MDBX interner behind the same traits.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DictKind {
    Account,
    Module,
    EventName,
    PubKey,
    ModuleHash,
}

impl DictKind {
    pub const ALL: [DictKind; 5] = [
        DictKind::Account,
        DictKind::Module,
        DictKind::EventName,
        DictKind::PubKey,
        DictKind::ModuleHash,
    ];
    fn idx(self) -> usize {
        self as usize
    }
}

pub trait Dict {
    fn find(&self, k: DictKind, s: &str) -> Option<u32>;
    fn name(&self, k: DictKind, id: u32) -> Option<&str>;
}

pub trait DictMut: Dict {
    /// Returns the existing id or assigns the next dense id (first-seen order).
    fn intern(&mut self, k: DictKind, s: &str) -> u32;
}

/// One table per kind: ids dense from 0 in first-seen order. Serializes as
/// `{"Account": [names in id order], ...}`; deserializing rebuilds the reverse maps.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(from = "BTreeMap<DictKind, Vec<String>>", into = "BTreeMap<DictKind, Vec<String>>")]
pub struct MemDict {
    /// `names[kind as usize][id]`.
    names: [Vec<String>; 5],
    ids: [HashMap<String, u32>; 5],
}

impl From<BTreeMap<DictKind, Vec<String>>> for MemDict {
    fn from(m: BTreeMap<DictKind, Vec<String>>) -> MemDict {
        let mut d = MemDict::default();
        for (k, v) in m {
            d.names[k.idx()] = v;
        }
        d.reindex();
        d
    }
}

impl From<MemDict> for BTreeMap<DictKind, Vec<String>> {
    fn from(d: MemDict) -> Self {
        DictKind::ALL
            .iter()
            .zip(d.names)
            .map(|(k, v)| (*k, v))
            .collect()
    }
}

impl MemDict {
    pub fn new() -> MemDict {
        MemDict::default()
    }
    pub fn len(&self, k: DictKind) -> usize {
        self.names[k.idx()].len()
    }
    pub fn is_empty(&self) -> bool {
        self.names.iter().all(Vec::is_empty)
    }
    pub fn names(&self, k: DictKind) -> &[String] {
        &self.names[k.idx()]
    }
    /// Rebuild the reverse maps from `names`.
    fn reindex(&mut self) {
        for (i, names) in self.names.iter().enumerate() {
            self.ids[i] = names
                .iter()
                .enumerate()
                .map(|(id, s)| (s.clone(), id as u32))
                .collect();
        }
    }
}

impl Dict for MemDict {
    fn find(&self, k: DictKind, s: &str) -> Option<u32> {
        self.ids[k.idx()].get(s).copied()
    }
    fn name(&self, k: DictKind, id: u32) -> Option<&str> {
        self.names[k.idx()].get(id as usize).map(String::as_str)
    }
}

impl DictMut for MemDict {
    fn intern(&mut self, k: DictKind, s: &str) -> u32 {
        if let Some(id) = self.ids[k.idx()].get(s) {
            return *id;
        }
        let id = self.names[k.idx()].len() as u32;
        self.names[k.idx()].push(s.to_owned());
        self.ids[k.idx()].insert(s.to_owned(), id);
        id
    }
}
