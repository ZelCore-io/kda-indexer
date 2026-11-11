//! Table names and key/value layouts (spec §3.3). All integers big-endian.
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | `Meta` | name (utf-8) | bytes (see `env.rs` for the keys) |
//! | `StageCheckpoint` | stage u8, chain u8 | height u32, hash [32] |
//! | `DictFwd{kind}` | name bytes, or 0xFF ‖ blake2b-256(name) over 480 bytes | id u32 |
//! | `DictRev{kind}` | id u32 | name bytes |
//! | `Headers` | chain u8, height u32 | `encode_block` bytes (plain form) |
//! | `HeaderByHash` | hash [32] | chain u8, height u32 |
//! | `TailTxs` | TxKey u64 | `encode_tx` bytes |
//! | `TailRk` | request key [32] | TxKey u64 |
//! | `TailPostings` | family u8 (0..=5, `kdx_decode::Family`), entity u64, TxKey u64 | empty |
//! | `Defpacts` | pactId [32] | TxKey u64 × n, ascending (tail only; superseded by the `pact` postings, no reader) |
//! | `Undo` | chain u8, height u32 | encoded `Vec<UndoOp>` (`undo.rs`) |
//! | `Counters` | name (utf-8) | u64 |
//!
//! `TailPostings` uses a composite key with an empty value instead of a dup-sorted table:
//! same order and prefix scans, one row per posting, exact deletes by key.
//! `Balances`, `Guards`, `KeyAccounts`, `NftBalances` are P6 and not created.

use kdx_types::{DictKind, Hash32, TxKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    Meta,
    StageCheckpoint,
    DictFwd(DictKind),
    DictRev(DictKind),
    Headers,
    HeaderByHash,
    TailTxs,
    TailRk,
    TailPostings,
    Defpacts,
    Undo,
    Counters,
}

fn kind_name(k: DictKind) -> &'static str {
    match k {
        DictKind::Account => "Account",
        DictKind::Module => "Module",
        DictKind::EventName => "EventName",
        DictKind::PubKey => "PubKey",
        DictKind::ModuleHash => "ModuleHash",
    }
}

impl Table {
    pub fn all() -> Vec<Table> {
        let mut v = vec![Table::Meta, Table::StageCheckpoint];
        for k in DictKind::ALL {
            v.push(Table::DictFwd(k));
            v.push(Table::DictRev(k));
        }
        v.extend([
            Table::Headers,
            Table::HeaderByHash,
            Table::TailTxs,
            Table::TailRk,
            Table::TailPostings,
            Table::Defpacts,
            Table::Undo,
            Table::Counters,
        ]);
        v
    }

    pub fn name(self) -> String {
        match self {
            Table::Meta => "Meta".into(),
            Table::StageCheckpoint => "StageCheckpoint".into(),
            Table::DictFwd(k) => format!("DictFwd{}", kind_name(k)),
            Table::DictRev(k) => format!("DictRev{}", kind_name(k)),
            Table::Headers => "Headers".into(),
            Table::HeaderByHash => "HeaderByHash".into(),
            Table::TailTxs => "TailTxs".into(),
            Table::TailRk => "TailRk".into(),
            Table::TailPostings => "TailPostings".into(),
            Table::Defpacts => "Defpacts".into(),
            Table::Undo => "Undo".into(),
            Table::Counters => "Counters".into(),
        }
    }
}

/// Opened database handles, one per table.
#[derive(Debug, Clone)]
pub struct Tables {
    dbis: Vec<(Table, u32)>,
}

impl Tables {
    pub(crate) fn new(dbis: Vec<(Table, u32)>) -> Tables {
        Tables { dbis }
    }
    pub fn dbi(&self, t: Table) -> u32 {
        self.dbis
            .iter()
            .find(|(x, _)| *x == t)
            .map(|(_, d)| *d)
            .expect("every table is opened at HotStore::open")
    }
    pub fn iter(&self) -> impl Iterator<Item = (Table, u32)> + '_ {
        self.dbis.iter().copied()
    }
}

/// `(chain u8, height u32)` key of `Headers` and `Undo`.
pub fn block_key(chain: u8, height: u32) -> [u8; 5] {
    let mut k = [0u8; 5];
    k[0] = chain;
    k[1..].copy_from_slice(&height.to_be_bytes());
    k
}

pub fn parse_block_key(k: &[u8]) -> Option<(u8, u32)> {
    if k.len() != 5 {
        return None;
    }
    Some((k[0], u32::from_be_bytes(k[1..5].try_into().ok()?)))
}

/// `(family u8, entity u64, TxKey u64)` key of `TailPostings`.
pub fn posting_key(family: u8, entity: u64, key: TxKey) -> [u8; 17] {
    let mut k = [0u8; 17];
    k[0] = family;
    k[1..9].copy_from_slice(&entity.to_be_bytes());
    k[9..].copy_from_slice(&key.to_be_bytes());
    k
}

pub fn parse_txkey(v: &[u8]) -> Option<TxKey> {
    Some(TxKey::from_be_bytes(v.try_into().ok()?))
}

pub fn parse_hash(v: &[u8]) -> Option<Hash32> {
    Some(Hash32(v.try_into().ok()?))
}
