//! MDBX environment, transactions and `Meta`.
//!
//! `Meta` keys: `format_version` (u64), `network` (utf-8), `chain_graph` (JSON of the node's
//! `ChainGraph`), `frozen_frontier` (u32: every height below it is in segments), `next_txnum`
//! (u64), `geometry` (JSON of `Geometry` plus the page size, B9).

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use anyhow::{bail, Context, Result};
use reth_libmdbx::{
    DatabaseFlags, Environment, PageSize, Transaction, TransactionKind, WriteFlags, RO, RW,
};

use crate::tables::{Table, Tables};

/// Hot store format version (Meta `format_version`).
pub const FORMAT_VERSION: u64 = 1;

/// MDBX page size (B9: 4 KB pages on Ceph RBD, matching the block device and ext4).
pub const PAGE_SIZE: usize = 4096;

const GIB: usize = 1 << 30;
const MIB: usize = 1 << 20;

/// Map geometry from `[storage]` (B9). Growth steps of 256 MB keep remaps rare on Ceph; the
/// upper bound only reserves address space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub max_gb: u64,
    pub growth_mb: u64,
}

impl Default for Geometry {
    fn default() -> Self {
        Geometry {
            max_gb: 64,
            growth_mb: 256,
        }
    }
}

pub mod meta {
    pub const FORMAT_VERSION: &str = "format_version";
    pub const NETWORK: &str = "network";
    pub const CHAIN_GRAPH: &str = "chain_graph";
    pub const FROZEN_FRONTIER: &str = "frozen_frontier";
    pub const NEXT_TXNUM: &str = "next_txnum";
    pub const GEOMETRY: &str = "geometry";
}

pub struct HotStore {
    env: Environment,
    tables: Tables,
    writer: Mutex<()>,
    geometry: Geometry,
}

impl HotStore {
    /// Open (or create) the hot store in `dir`. A new store gets `format_version` 1,
    /// `frozen_frontier` 0, `next_txnum` 0 and its geometry in `Meta`; an existing store with
    /// another format version is refused.
    pub fn open(dir: &Path, g: Geometry) -> Result<HotStore> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let max = usize::try_from(g.max_gb)
            .ok()
            .and_then(|x| x.checked_mul(GIB))
            .context("storage.mdbx_max_gb too large")?;
        let growth = usize::try_from(g.growth_mb)
            .ok()
            .and_then(|x| x.checked_mul(MIB))
            .context("storage.mdbx_growth_mb too large")?;
        let env = Environment::builder()
            .set_max_dbs(32)
            .set_geometry(reth_libmdbx::Geometry {
                size: Some(0..max),
                growth_step: Some(growth as isize),
                shrink_threshold: None,
                page_size: Some(PageSize::Set(PAGE_SIZE)),
            })
            .open(dir)
            .with_context(|| format!("opening MDBX env in {}", dir.display()))?;
        let txn = env.begin_rw_txn()?;
        let mut dbis = Vec::new();
        for t in Table::all() {
            let db = txn.create_db(Some(&t.name()), DatabaseFlags::CREATE)?;
            dbis.push((t, db.dbi()));
        }
        let tables = Tables::new(dbis);
        let mdbi = tables.dbi(Table::Meta);
        match txn.get::<Vec<u8>>(mdbi, meta::FORMAT_VERSION.as_bytes())? {
            None => {
                let put = |k: &str, v: &[u8]| txn.put(mdbi, k, v, WriteFlags::empty());
                put(meta::FORMAT_VERSION, &FORMAT_VERSION.to_be_bytes())?;
                put(meta::FROZEN_FRONTIER, &0u32.to_be_bytes())?;
                put(meta::NEXT_TXNUM, &0u64.to_be_bytes())?;
            }
            Some(v) => {
                let v = be_u64(&v).context("Meta format_version")?;
                if v != FORMAT_VERSION {
                    bail!("hot store format version {v}, this build reads {FORMAT_VERSION}");
                }
            }
        }
        // The geometry in effect is recorded on every open (it can be changed in the config).
        let geo = format!(
            "{{\"page_size\":{PAGE_SIZE},\"growth_mb\":{},\"max_gb\":{}}}",
            g.growth_mb, g.max_gb
        );
        txn.put(mdbi, meta::GEOMETRY, geo.as_bytes(), WriteFlags::empty())?;
        txn.commit()?;
        Ok(HotStore {
            env,
            tables,
            writer: Mutex::new(()),
            geometry: g,
        })
    }

    pub fn geometry(&self) -> Geometry {
        self.geometry
    }

    pub fn env(&self) -> &Environment {
        &self.env
    }

    pub fn tables(&self) -> &Tables {
        &self.tables
    }

    /// A read-only snapshot.
    pub fn ro(&self) -> Result<RoTxn> {
        Ok(RoTxn {
            txn: self.env.begin_ro_txn()?,
            tables: self.tables.clone(),
        })
    }

    /// The write transaction. One writer at a time: blocks while another `RwTxn` is alive.
    pub fn rw(&self) -> Result<RwTxn<'_>> {
        let guard = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        Ok(RwTxn {
            txn: self.env.begin_rw_txn()?,
            tables: self.tables.clone(),
            _guard: guard,
        })
    }
}

pub(crate) fn be_u64(v: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(v.try_into().ok()?))
}

pub(crate) fn be_u32(v: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(v.try_into().ok()?))
}

/// Reads shared by `RoTxn` and `RwTxn`.
pub trait Txn {
    type Kind: TransactionKind;
    fn raw(&self) -> &Transaction<Self::Kind>;
    fn tables(&self) -> &Tables;

    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.raw().get::<Vec<u8>>(self.tables().dbi(t), key)?)
    }

    /// Rows with `from <= key`, in key order, while `keep(key)` holds (`from = &[]` = all).
    fn scan(
        &self,
        t: Table,
        from: &[u8],
        mut keep: impl FnMut(&[u8]) -> bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut c = self.raw().cursor(self.tables().dbi(t))?;
        let mut out = Vec::new();
        let first = if from.is_empty() {
            c.first::<Vec<u8>, Vec<u8>>()?
        } else {
            c.set_range::<Vec<u8>, Vec<u8>>(from)?
        };
        let mut cur = first;
        while let Some((k, v)) = cur {
            if !keep(&k) {
                break;
            }
            out.push((k, v));
            cur = c.next::<Vec<u8>, Vec<u8>>()?;
        }
        Ok(out)
    }

    /// Number of rows in a table.
    fn count(&self, t: Table) -> Result<usize> {
        Ok(self.raw().db_stat_with_dbi(self.tables().dbi(t))?.entries())
    }

    fn meta(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.get(Table::Meta, name.as_bytes())
    }
    fn meta_u64(&self, name: &str) -> Result<Option<u64>> {
        self.meta(name)?
            .map(|v| be_u64(&v).with_context(|| format!("Meta {name} is not a u64")))
            .transpose()
    }
    fn frozen_frontier(&self) -> Result<u32> {
        let v = self.meta(meta::FROZEN_FRONTIER)?.unwrap_or_default();
        be_u32(&v).context("Meta frozen_frontier is not a u32")
    }
    fn next_txnum(&self) -> Result<u64> {
        Ok(self.meta_u64(meta::NEXT_TXNUM)?.unwrap_or(0))
    }
    fn counter(&self, name: &str) -> Result<u64> {
        Ok(self
            .get(Table::Counters, name.as_bytes())?
            .and_then(|v| be_u64(&v))
            .unwrap_or(0))
    }
}

pub struct RoTxn {
    txn: Transaction<RO>,
    tables: Tables,
}

impl Txn for RoTxn {
    type Kind = RO;
    fn raw(&self) -> &Transaction<RO> {
        &self.txn
    }
    fn tables(&self) -> &Tables {
        &self.tables
    }
}

pub struct RwTxn<'a> {
    txn: Transaction<RW>,
    tables: Tables,
    _guard: MutexGuard<'a, ()>,
}

impl Txn for RwTxn<'_> {
    type Kind = RW;
    fn raw(&self) -> &Transaction<RW> {
        &self.txn
    }
    fn tables(&self) -> &Tables {
        &self.tables
    }
}

impl RwTxn<'_> {
    pub fn put(&mut self, t: Table, key: &[u8], val: &[u8]) -> Result<()> {
        self.txn
            .put(self.tables.dbi(t), key, val, WriteFlags::empty())?;
        Ok(())
    }

    /// Insert only if the key is absent; returns whether it was inserted.
    pub fn insert(&mut self, t: Table, key: &[u8], val: &[u8]) -> Result<bool> {
        match self
            .txn
            .put(self.tables.dbi(t), key, val, WriteFlags::NO_OVERWRITE)
        {
            Ok(()) => Ok(true),
            Err(reth_libmdbx::Error::KeyExist) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a row; returns whether it existed.
    pub fn del(&mut self, t: Table, key: &[u8]) -> Result<bool> {
        Ok(self.txn.del(self.tables.dbi(t), key, None)?)
    }

    pub fn set_meta(&mut self, name: &str, val: &[u8]) -> Result<()> {
        self.put(Table::Meta, name.as_bytes(), val)
    }
    pub fn set_meta_u64(&mut self, name: &str, v: u64) -> Result<()> {
        self.set_meta(name, &v.to_be_bytes())
    }
    pub fn set_frozen_frontier(&mut self, h: u32) -> Result<()> {
        self.set_meta(meta::FROZEN_FRONTIER, &h.to_be_bytes())
    }
    pub fn set_next_txnum(&mut self, n: u64) -> Result<()> {
        self.set_meta_u64(meta::NEXT_TXNUM, n)
    }

    /// Add `delta` to a counter (saturating at 0 and u64::MAX).
    pub fn add_counter(&mut self, name: &str, delta: i64) -> Result<u64> {
        let v = self.counter(name)?;
        let n = if delta >= 0 {
            v.saturating_add(delta as u64)
        } else {
            v.saturating_sub(delta.unsigned_abs())
        };
        self.put(Table::Counters, name.as_bytes(), &n.to_be_bytes())?;
        Ok(n)
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()?;
        Ok(())
    }
}
