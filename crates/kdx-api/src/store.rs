//! `Store` and `View`: consistent reads over the hot store and the segments while ingest
//! writes (spec §4.4–§4.5; `docs/format.md` "Readers").
//!
//! A `View` is one MDBX read txn plus one `Segments` snapshot, opened **txn first, then the
//! manifest stat** (refresh when it changed). Split rule: heights `< seg_frontier` come from
//! segments, heights `>= max(hot_frontier, seg_frontier)` from hot rows; hot rows below
//! `seg_frontier` are skipped. Safe because the freezer commits the manifest before it
//! deletes the hot rows: a txn that no longer sees the rows was opened after the manifest
//! changed, and the stat that follows sees the new manifest. Backfill commits MDBX (names,
//! `frozen_frontier`) before the manifest and writes no hot rows, so a just-built range is
//! briefly invisible, never doubled. Deleted step files stay readable through the mmaps of
//! the readers a `View` holds.
//!
//! A `View` lives for one request at most: it pins MDBX pages while the tail writes.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use arc_swap::ArcSwap;
use kdx_decode::index::pact_entity;
use kdx_decode::Family;
use kdx_hot::tables::{block_key, parse_block_key, parse_txkey, posting_key};
use kdx_hot::{dict as hdict, HotStore, RoTxn, Stage, Table, Txn};
use kdx_segment::manifest::MANIFEST;
pub use kdx_segment::ListMemo;
use kdx_segment::Segments;
use kdx_types::record::{decode_block, decode_tx, BlockRecord, TxRecord};
use kdx_types::{DictKind, Hash32, MemDict, TxKey};

use crate::dict::ViewDict;
use crate::{ApiError, ApiResult};

/// Identity of `manifest.json` (it is replaced by rename, so the inode changes on commit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestId {
    dev: u64,
    ino: u64,
    mtime_ns: i128,
    len: u64,
}

fn manifest_id(dir: &Path) -> anyhow::Result<Option<ManifestId>> {
    match std::fs::metadata(dir.join(MANIFEST)) {
        Ok(m) => Ok(Some(ManifestId {
            dev: m.dev(),
            ino: m.ino(),
            mtime_ns: m.mtime() as i128 * 1_000_000_000 + m.mtime_nsec() as i128,
            len: m.size(),
        })),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("stat manifest.json"),
    }
}

/// The dictionary snapshot reloads when MDBX holds this many more names than it.
pub const DICT_RELOAD_NAMES: u64 = 10_000;
/// ... or at this age when MDBX holds any name it lacks.
pub const DICT_RELOAD_AGE: Duration = Duration::from_secs(60);
/// Rows per short read txn when loading dictionary names.
const DICT_CHUNK: usize = 4096;

struct DictSnap {
    mem: Arc<MemDict>,
    at: Instant,
}

/// Shared read side of one data dir. Cheap to share (`Arc<Store>`); every request takes a
/// [`View`].
pub struct Store {
    pub hot: Arc<HotStore>,
    pub segdir: PathBuf,
    seg: ArcSwap<Segments>,
    seg_id: Mutex<Option<ManifestId>>,
    dict: ArcSwap<DictSnap>,
    dict_reloading: AtomicBool,
    /// Network and genesis heights from `Meta` (empty until ingest writes the chain graph).
    genesis: ArcSwap<Genesis>,
}

/// Network id and the genesis height of every chain, from the hot store's `network` and
/// `chain_graph` meta (R9: no mainnet constants in the API).
#[derive(Debug, Clone, Default)]
pub struct Genesis {
    pub network: String,
    /// Indexed by chain id.
    heights: Vec<Option<u32>>,
    /// Request keys that occur in more than one genesis block, with every location (F4: the
    /// request-key index keeps one location per key, but genesis txs repeat their request
    /// key across chains). Built on first use once every genesis block is stored.
    shared_rks: OnceLock<HashMap<Hash32, Vec<TxKey>>>,
}

impl Genesis {
    /// From the meta values; `graph` is the JSON `[[height, [chain, ...]], ...]` ingest stores.
    pub fn from_meta(network: Option<&[u8]>, graph: Option<&[u8]>) -> Genesis {
        let network = network
            .map(|n| String::from_utf8_lossy(n).into_owned())
            .unwrap_or_default();
        let mut hist: Vec<(u64, Vec<u8>)> = graph
            .and_then(|g| serde_json::from_slice(g).ok())
            .unwrap_or_default();
        hist.sort_by_key(|(h, _)| *h);
        let mut heights: Vec<Option<u32>> = Vec::new();
        for (h, chains) in hist {
            for c in chains {
                let c = usize::from(c);
                if heights.len() <= c {
                    heights.resize(c + 1, None);
                }
                if heights[c].is_none() {
                    heights[c] = u32::try_from(h).ok();
                }
            }
        }
        Genesis {
            network,
            heights,
            shared_rks: OnceLock::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.heights.is_empty()
    }

    /// True for a tx of a chain's genesis block.
    pub fn is_genesis(&self, k: TxKey) -> bool {
        self.heights.get(usize::from(k.chain())).copied().flatten() == Some(k.height())
    }

    /// `(chain, genesis height)` of every known chain.
    pub fn chains(&self) -> impl Iterator<Item = (u8, u32)> + '_ {
        self.heights
            .iter()
            .enumerate()
            .filter_map(|(c, h)| Some((u8::try_from(c).ok()?, (*h)?)))
    }
}

fn load_genesis(txn: &RoTxn) -> anyhow::Result<Genesis> {
    let n = txn.meta(kdx_hot::env::meta::NETWORK)?;
    let g = txn.meta(kdx_hot::env::meta::CHAIN_GRAPH)?;
    Ok(Genesis::from_meta(n.as_deref(), g.as_deref()))
}

impl Store {
    /// Open the read side over an opened hot store (`open_ro` in `serve`, the shared writer
    /// env in `run`) and `<data_dir>/segments`.
    pub fn open(hot: Arc<HotStore>, data_dir: &Path) -> anyhow::Result<Store> {
        let segdir = data_dir.join("segments");
        let mem = Arc::new(load_names(&hot, None)?);
        let id = manifest_id(&segdir)?;
        let seg = Segments::open(&segdir, mem.clone())?;
        let genesis = load_genesis(&hot.ro()?)?;
        Ok(Store {
            hot,
            segdir,
            seg: ArcSwap::from_pointee(seg),
            seg_id: Mutex::new(id),
            dict: ArcSwap::from_pointee(DictSnap {
                mem,
                at: Instant::now(),
            }),
            dict_reloading: AtomicBool::new(false),
            genesis: ArcSwap::from_pointee(genesis),
        })
    }

    /// The hot store (tests write fixtures through it).
    #[cfg(test)]
    pub fn hot_for_tests(&self) -> &HotStore {
        &self.hot
    }

    /// Names in the current dictionary snapshot (all kinds).
    pub fn dict_names(&self) -> usize {
        let snap = self.dict.load();
        DictKind::ALL.iter().map(|&k| snap.mem.len(k)).sum()
    }

    /// Current segments snapshot (may lag the manifest until the next `view`).
    pub fn segments(&self) -> Arc<Segments> {
        self.seg.load_full()
    }

    /// Reload the segments when `manifest.json` changed since the last look. Retries once:
    /// a merge can delete step files between our manifest load and their open.
    fn refresh_segments(&self) -> anyhow::Result<()> {
        let id = manifest_id(&self.segdir)?;
        let mut cur = self.seg_id.lock().unwrap_or_else(|e| e.into_inner());
        if *cur == id {
            return Ok(());
        }
        let mut last = None;
        for _ in 0..3 {
            match self.seg.load().refresh() {
                Ok(Some(s)) => {
                    self.seg.store(Arc::new(s));
                    *cur = id;
                    return Ok(());
                }
                Ok(None) => {
                    *cur = id;
                    return Ok(());
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last
            .expect("three attempts")
            .context("reloading the segments"))
    }

    /// One consistent snapshot for one request. See the module docs for the order.
    pub fn view(self: &Arc<Self>, deadline: Instant) -> ApiResult<View> {
        let opened = Instant::now();
        let mut attempt = 0;
        loop {
            let txn = self.hot.ro().map_err(ApiError::Internal)?;
            self.refresh_segments().map_err(ApiError::Internal)?;
            let seg = self.seg.load_full();
            let hot_frontier = txn.frozen_frontier().map_err(ApiError::Internal)?;
            let seg_frontier = seg.frontier();
            // Backfill commits the range's names to MDBX before the manifest. A txn opened
            // before that commit with a stat after the manifest commit would hold segments
            // whose names it cannot see: `dict_lens@F` is written in that same MDBX commit,
            // so its absence means "maybe"; reopen the txn (cheap). After a freeze the key is
            // also absent until the hot delete, which is harmless (names came with the tail).
            if seg_frontier > hot_frontier
                && attempt < 2
                && txn
                    .meta(&hdict::lens_key(seg_frontier))
                    .map_err(ApiError::Internal)?
                    .is_none()
            {
                attempt += 1;
                continue;
            }
            self.maybe_reload_dict(&txn);
            if self.genesis.load().is_empty() {
                // A fresh store: ingest writes the chain graph after the API opened.
                let g = load_genesis(&txn).map_err(ApiError::Internal)?;
                if !g.is_empty() {
                    self.genesis.store(Arc::new(g));
                }
            }
            kdx_metrics::get().read_view("open", opened.elapsed().as_secs_f64());
            return Ok(View {
                genesis: self.genesis.load_full(),
                txn,
                seg,
                hot_frontier,
                seg_frontier,
                deadline,
                dict: self.dict.load().mem.clone(),
                born: Instant::now(),
                memo: ListMemo::new(),
            });
        }
    }

    /// Start a background reload of the dictionary snapshot when it is far behind MDBX, old,
    /// or ahead of it (a backfill resume truncated names). Loads in short read txns.
    fn maybe_reload_dict(self: &Arc<Self>, txn: &RoTxn) {
        let snap = self.dict.load();
        let mut behind = 0u64;
        let mut ahead = false;
        for k in DictKind::ALL {
            let n = txn.count(Table::DictRev(k)).unwrap_or(0) as u64;
            let m = snap.mem.len(k) as u64;
            behind += n.saturating_sub(m);
            ahead |= n < m;
        }
        let due = ahead
            || behind >= DICT_RELOAD_NAMES
            || (behind > 0 && snap.at.elapsed() >= DICT_RELOAD_AGE);
        if !due || self.dict_reloading.swap(true, Ordering::AcqRel) {
            return;
        }
        let me = self.clone();
        let base = (!ahead).then(|| snap.mem.clone());
        std::thread::spawn(move || {
            let t = Instant::now();
            match load_names(&me.hot, base.as_deref()) {
                Ok(m) => {
                    tracing::debug!(
                        ms = t.elapsed().as_millis() as u64,
                        "api: dictionary snapshot reloaded"
                    );
                    me.dict.store(Arc::new(DictSnap {
                        mem: Arc::new(m),
                        at: Instant::now(),
                    }));
                }
                Err(e) => tracing::warn!(error = %e, "api: dictionary reload failed"),
            }
            me.dict_reloading.store(false, Ordering::Release);
        });
    }
}

/// Every name in `DictRev`, read in chunks of short txns, appended to a copy of `base`
/// (names are append-only, ids dense from 0).
fn load_names(hot: &HotStore, base: Option<&MemDict>) -> anyhow::Result<MemDict> {
    use kdx_types::{Dict, DictMut};
    let mut d = base.cloned().unwrap_or_default();
    for k in DictKind::ALL {
        loop {
            let next = d.len(k) as u32;
            let rows = {
                let txn = hot.ro()?;
                txn.scan_limit(Table::DictRev(k), &next.to_be_bytes(), DICT_CHUNK, |_| true)?
            };
            for (key, v) in &rows {
                let id = u32::from_be_bytes(key[..].try_into().context("DictRev key")?);
                let want = d.len(k) as u32;
                anyhow::ensure!(id == want, "{k:?} dictionary ids are not dense at {want}");
                let name = std::str::from_utf8(v).context("dictionary name is not utf-8")?;
                // `intern` of an existing name would not add an id: names are unique per kind.
                anyhow::ensure!(d.find(k, name).is_none(), "{k:?} name {id} repeats");
                d.intern(k, name);
            }
            if rows.len() < DICT_CHUNK {
                break;
            }
        }
    }
    Ok(d)
}

/// Filters of a posting walk. Heights are inclusive.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostingQuery {
    pub min_height: Option<u32>,
    pub max_height: Option<u32>,
    pub chain: Option<u8>,
    /// Only keys strictly below this one (a page cursor).
    pub before: Option<TxKey>,
}

/// Deadline check interval of long walks.
const CHECK_EVERY: u32 = 64;

/// One consistent read snapshot (see the module docs). Never keep it past the request.
pub struct View {
    pub txn: RoTxn,
    pub seg: Arc<Segments>,
    /// `frozen_frontier` as this txn sees it.
    pub hot_frontier: u32,
    /// The manifest frontier of `seg`.
    pub seg_frontier: u32,
    pub deadline: Instant,
    /// Network and genesis heights.
    pub genesis: Arc<Genesis>,
    dict: Arc<MemDict>,
    born: Instant,
    /// Per-request memo of parsed Roaring directories: walks that re-seek a list within this
    /// view (k-way merges of event-name lists, the token then coin pass of `/v1/txs`, a
    /// resume inside a page) parse its header once. Capped at 64 MB.
    memo: ListMemo,
}

impl Drop for View {
    fn drop(&mut self) {
        kdx_metrics::get().read_view("held", self.born.elapsed().as_secs_f64());
    }
}

impl View {
    /// Heights served from segments.
    pub fn frozen(&self, h: u32) -> bool {
        h < self.seg_frontier
    }
    /// Lowest height served from hot rows.
    pub fn hot_floor(&self) -> u32 {
        self.hot_frontier.max(self.seg_frontier)
    }
    fn hot(&self, h: u32) -> bool {
        h >= self.hot_floor()
    }

    pub fn check_deadline(&self) -> ApiResult<()> {
        if Instant::now() >= self.deadline {
            Err(ApiError::Deadline)
        } else {
            Ok(())
        }
    }

    /// Names for rendering this view's records.
    pub fn dict(&self) -> ViewDict<'_> {
        ViewDict::new(&self.txn, self.dict.clone())
    }

    pub fn tx(&self, k: TxKey) -> ApiResult<Option<TxRecord>> {
        let h = k.height();
        if self.frozen(h) {
            return match self.seg.txnum_of(k) {
                Some(t) => Ok(self.seg.tx(t)?),
                None => Ok(None),
            };
        }
        if !self.hot(h) {
            return Ok(None);
        }
        match self.txn.get(Table::TailTxs, &k.to_be_bytes())? {
            Some(v) => Ok(Some(decode_tx(&v)?)),
            None => Ok(None),
        }
    }

    /// Every stored location of a request key, in key order. The request-key index holds one
    /// location per key; only genesis txs share a request key across chains (F4: chainweb-data
    /// lists each copy on `/txs/txs`), so their other copies come from the genesis blocks.
    pub fn txs_by_rk(&self, rk: &Hash32) -> ApiResult<Vec<(TxKey, TxRecord)>> {
        let mut out: Vec<(TxKey, TxRecord)> = self.tx_by_rk(rk)?.into_iter().collect();
        for k in self.genesis_copies(rk)? {
            if out.iter().any(|(x, _)| *x == k) {
                continue;
            }
            if let Some(r) = self.tx(k)? {
                out.push((k, r));
            }
        }
        out.sort_by_key(|(k, _)| *k);
        Ok(out)
    }

    /// Genesis-block locations of `rk` when it occurs in more than one genesis block.
    fn genesis_copies(&self, rk: &Hash32) -> ApiResult<Vec<TxKey>> {
        if let Some(m) = self.genesis.shared_rks.get() {
            return Ok(m.get(rk).cloned().unwrap_or_default());
        }
        let mut all: HashMap<Hash32, Vec<TxKey>> = HashMap::new();
        let mut complete = true;
        for (c, h) in self.genesis.chains() {
            if self.block_hash(c, h)?.is_none() {
                complete = false;
                continue;
            }
            for r in self.block_txs(c, h)? {
                if let Some(x) = r.request_key {
                    all.entry(x).or_default().push(r.key);
                }
            }
        }
        all.retain(|_, ks| ks.len() > 1);
        let found = all.get(rk).cloned().unwrap_or_default();
        // A store still ingesting its genesis blocks is scanned again on the next lookup.
        if complete && !self.genesis.is_empty() {
            let _ = self.genesis.shared_rks.set(all);
        }
        Ok(found)
    }

    /// Tx by request key: the hot store first (newest), then the segments.
    pub fn tx_by_rk(&self, rk: &Hash32) -> ApiResult<Option<(TxKey, TxRecord)>> {
        if let Some(v) = self.txn.get(Table::TailRk, &rk.0)? {
            let k = parse_txkey(&v).context("TailRk value")?;
            if self.hot(k.height()) {
                if let Some(r) = self.tx(k)? {
                    return Ok(Some((k, r)));
                }
            }
        }
        if let Some(t) = self.seg.by_rk(rk)? {
            let k = self.seg.txkey(t).context("segment TxNum without a key")?;
            if self.frozen(k.height()) {
                if let Some(r) = self.seg.tx(t)? {
                    return Ok(Some((k, r)));
                }
            }
        }
        Ok(None)
    }

    pub fn block(&self, chain: u8, height: u32) -> ApiResult<Option<BlockRecord>> {
        if self.frozen(height) {
            return Ok(self.seg.block(chain, height)?);
        }
        if !self.hot(height) {
            return Ok(None);
        }
        match self.txn.get(Table::Headers, &block_key(chain, height))? {
            Some(v) => Ok(Some(decode_block(&v)?)),
            None => Ok(None),
        }
    }

    /// Hash of block (chain, height) without decoding the frozen record's adjacents.
    pub fn block_hash(&self, chain: u8, height: u32) -> ApiResult<Option<Hash32>> {
        if self.frozen(height) {
            return Ok(self.seg.block_hash(chain, height)?);
        }
        Ok(self.block(chain, height)?.map(|b| b.hash))
    }

    /// Block by hash: (chain, height, record).
    pub fn block_by_hash(&self, h: &Hash32) -> ApiResult<Option<(u8, u32, BlockRecord)>> {
        if let Some(v) = self.txn.get(Table::HeaderByHash, &h.0)? {
            let (c, ht) = parse_block_key(&v).context("HeaderByHash value")?;
            if self.hot(ht) {
                if let Some(b) = self.block(c, ht)? {
                    return Ok(Some((c, ht, b)));
                }
            }
        }
        if let Some((c, ht)) = self.seg.by_block_hash(h)? {
            if self.frozen(ht) {
                if let Some(b) = self.seg.block(c, ht)? {
                    return Ok(Some((c, ht, b)));
                }
            }
        }
        Ok(None)
    }

    /// Txs of block (chain, height), coinbase first.
    pub fn block_txs(&self, chain: u8, height: u32) -> ApiResult<Vec<TxRecord>> {
        if self.frozen(height) {
            let Some(r) = self.seg.block_txnums(chain, height) else {
                return Ok(Vec::new());
            };
            let mut out = Vec::with_capacity(r.end.saturating_sub(r.start) as usize);
            for t in r {
                out.push(self.seg.tx(t)?.context("segment tx missing")?);
            }
            return Ok(out);
        }
        if !self.hot(height) {
            return Ok(Vec::new());
        }
        let lo = TxKey::new(height, chain, 0).to_be_bytes();
        let hi = match chain.checked_add(1) {
            Some(c) => TxKey::new(height, c, 0).to_be_bytes(),
            None => TxKey::new(height.saturating_add(1), 0, 0).to_be_bytes(),
        };
        let rows = self.txn.scan(Table::TailTxs, &lo, |k| k < &hi[..])?;
        rows.iter()
            .map(|(_, v)| Ok(decode_tx(v)?))
            .collect::<ApiResult<_>>()
    }

    /// Per chain: (chain, height, hash) of the stored tip (`Tail` checkpoint, else the
    /// `Headers` checkpoint a backfill leaves).
    pub fn chain_tips(&self) -> ApiResult<Vec<(u8, u32, Hash32)>> {
        let mut out = Vec::new();
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            let cp = match kdx_hot::checkpoint(&self.txn, Stage::Tail, c)? {
                Some(x) => Some(x),
                None => kdx_hot::checkpoint(&self.txn, Stage::Headers, c)?,
            };
            if let Some((h, hash)) = cp {
                out.push((c, h, hash));
            }
        }
        Ok(out)
    }

    /// Highest stored height over all chains (None: empty store).
    pub fn max_indexed_height(&self) -> ApiResult<Option<u32>> {
        let tips = self.chain_tips()?.into_iter().map(|(_, h, _)| h).max();
        Ok(tips.or_else(|| self.seg_frontier.checked_sub(1)))
    }

    /// Highest height whose txs are committed: the tail checkpoints, else the segment
    /// frontier. During backfill the `Headers` checkpoint runs far above the data, so walks
    /// that start from `max_indexed_height` spend their budget on empty heights (R13).
    pub fn max_data_height(&self) -> ApiResult<Option<u32>> {
        let mut tail: Option<u32> = None;
        for c in 0..kdx_segment::SLOT_WIDTH as u8 {
            if let Some((h, _)) = kdx_hot::checkpoint(&self.txn, Stage::Tail, c)? {
                tail = Some(tail.map_or(h, |t| t.max(h)));
            }
        }
        let seg = self.seg_frontier.checked_sub(1);
        Ok(match (tail, seg) {
            (Some(t), Some(s)) => Some(t.max(s)),
            (t, s) => t.or(s),
        })
    }

    /// Stored txs of the defpact `pact`, ascending by TxKey (hot and frozen): the exec that
    /// started it (`cont` false) and each `cont` step (`cont` true). Candidates come from the
    /// `pact` postings (an 8-byte entity); each record is checked to hold this exact pactId,
    /// as the cmd's `payload.cont.pactId` or the output's `continuation.pactId`. At most `max`
    /// txs, the newest ones; the flag is true when the walk stopped there (R-P5P9-3).
    pub fn pact_txs(&self, pact: &Hash32, max: usize) -> ApiResult<(Vec<(TxKey, bool)>, bool)> {
        let mut out = Vec::new();
        let mut capped = false;
        for k in self.postings(Family::Pact, pact_entity(pact), PostingQuery::default())? {
            let k = k?;
            if let Some(r) = self.tx(k)? {
                let cont = cmd_pact_id(&r) == Some(*pact);
                if cont || record_pact_id(&r, &self.dict())? == Some(*pact) {
                    if out.len() == max {
                        capped = true;
                        break;
                    }
                    out.push((k, cont));
                }
            }
        }
        out.reverse();
        Ok((out, capped))
    }

    /// TxKeys of the exec txs whose code is one of `hits` (code search, plan Task 10), newest
    /// first, deduplicated, with the query's filters. Per hit, only the `code` postings in
    /// `first_h..=last_h`, plus every height above `rebuilt_through` (the last rebuild's
    /// `built_through_height`: docs are never updated, so `last_h` is exact only up to there;
    /// `None` expands from `first_h`), plus the hot part. Each record's blake2b-128 of its
    /// exec code must equal a hit's full hash (the posting entity is an 8-byte prefix). The
    /// deadline is checked every 64 items.
    pub fn code_txs(
        &self,
        hits: &[CodeHit],
        rebuilt_through: Option<u32>,
        q: PostingQuery,
    ) -> ApiResult<Box<dyn Iterator<Item = ApiResult<TxKey>> + '_>> {
        // Hits sharing an entity walk its postings once.
        let mut by_entity: std::collections::BTreeMap<u64, (u32, u32, Vec<[u8; 16]>)> =
            std::collections::BTreeMap::new();
        for h in hits {
            let e = by_entity
                .entry(kdx_decode::index::code_entity(&h.hash))
                .or_insert((h.first_h, h.last_h, Vec::new()));
            e.0 = e.0.min(h.first_h);
            e.1 = e.1.max(h.last_h);
            e.2.push(h.hash);
        }
        let upper_from = rebuilt_through
            .map_or(0, |b| b.saturating_add(1))
            .min(self.hot_floor());
        let q_min = q.min_height.unwrap_or(0);
        let q_max = q.max_height.unwrap_or(u32::MAX);
        let mut its: Vec<Box<dyn Iterator<Item = ApiResult<TxKey>> + '_>> = Vec::new();
        for (entity, (first_h, last_h, hashes)) in by_entity {
            let mut windows = Vec::with_capacity(2);
            if first_h >= upper_from {
                windows.push((first_h.max(q_min), q_max));
            } else {
                windows.push((upper_from.max(q_min), q_max));
                windows.push((first_h.max(q_min), last_h.min(upper_from - 1).min(q_max)));
            }
            let mut walks: Vec<Box<dyn Iterator<Item = ApiResult<TxKey>> + '_>> = Vec::new();
            for (lo, hi) in windows {
                if lo > hi {
                    continue;
                }
                let wq = PostingQuery {
                    min_height: Some(lo),
                    max_height: Some(hi),
                    ..q
                };
                walks.push(self.postings(Family::Code, entity, wq)?);
            }
            // Windows are disjoint and the first is the higher one: chained, still newest first.
            let walk = walks.into_iter().flatten();
            its.push(Box::new(walk.filter_map(move |r| {
                match r {
                    Err(e) => Some(Err(e)),
                    Ok(k) => match self.tx(k) {
                        Err(e) => Some(Err(e)),
                        Ok(None) => None,
                        Ok(Some(rec)) => kdx_search::exec_code(&rec)
                            .map(|c| kdx_types::blake2b_128(c.as_bytes()))
                            .filter(|h| hashes.contains(h))
                            .map(|_| Ok(k)),
                    },
                }
            })));
        }
        Ok(Box::new(MergeDesc::new(its, self.deadline)))
    }

    /// TxKeys of `(f, entity)`, newest first: the hot part, then the frozen part, with the
    /// query's filters. The deadline is checked every 64 items.
    pub fn postings(
        &self,
        f: Family,
        entity: u64,
        q: PostingQuery,
    ) -> ApiResult<Box<dyn Iterator<Item = ApiResult<TxKey>> + '_>> {
        // Exclusive upper key from `before` and `max_height`.
        let mut upper: Option<TxKey> = q.before;
        if let Some(mh) = q.max_height {
            if let Some(h1) = mh.checked_add(1) {
                let k = TxKey::new(h1, 0, 0);
                upper = Some(upper.map_or(k, |u| u.min(k)));
            }
        }
        let min_h = q.min_height.unwrap_or(0);
        let chain = q.chain;
        let keep = move |k: &TxKey| chain.is_none_or(|c| k.chain() == c);

        // Hot part: keys >= hot_floor.
        let floor = self.hot_floor();
        let hot: Box<dyn Iterator<Item = ApiResult<TxKey>> + '_> = if upper
            .is_some_and(|u| u.height() < floor)
        {
            Box::new(std::iter::empty())
        } else {
            let prefix = posting_key(f as u8, entity, TxKey::new(0, 0, 0));
            let up = upper
                .map(|u| posting_key(f as u8, entity, u).to_vec())
                .unwrap_or_default();
            let scan = self
                .txn
                .scan_rev_prefix(Table::TailPostings, &prefix[..9], &up)?;
            Box::new(
                scan.map(|r| -> ApiResult<TxKey> {
                    let (k, _) = r?;
                    Ok(parse_txkey(&k[9..]).context("posting key")?)
                })
                .take_while(move |r| r.as_ref().map_or(true, |k| k.height() >= floor.max(min_h)))
                .filter(move |r| r.as_ref().map_or(true, keep)),
            )
        };

        // Frozen part: TxNums below the bound, mapped to keys.
        let seg = &self.seg;
        let memo = &self.memo;
        let frozen: Box<dyn Iterator<Item = ApiResult<TxKey>> + '_> =
            if self.seg_frontier == 0 || self.seg_frontier <= min_h {
                Box::new(std::iter::empty())
            } else {
                let max_txnum = match upper {
                    Some(u) if u.height() < self.seg_frontier => match seg.txnum_of(u) {
                        Some(t) => t.checked_sub(1),
                        // Not a stored key: bound by its height, the key filter below is exact.
                        None => seg.last_txnum_below_height(u.height().saturating_add(1)),
                    },
                    _ => seg.last_txnum_below_height(self.seg_frontier),
                };
                match max_txnum {
                    None => Box::new(std::iter::empty()),
                    Some(m) => Box::new(
                        seg.postings_from_memo(f, entity, Some(m), Some(memo))
                            .map(move |r| -> ApiResult<TxKey> {
                                let t = r?;
                                Ok(seg.txkey(t).context("posting TxNum without a key")?)
                            })
                            .take_while(move |r| r.as_ref().map_or(true, |k| k.height() >= min_h))
                            .filter(move |r| {
                                r.as_ref()
                                    .map_or(true, |k| keep(k) && upper.is_none_or(|u| *k < u))
                            }),
                    ),
                }
            };

        let mut n = 0u32;
        let deadline = self.deadline;
        let mut failed = false;
        Ok(Box::new(hot.chain(frozen).map_while(move |r| {
            if failed {
                return None;
            }
            n += 1;
            if n.is_multiple_of(CHECK_EVERY) && Instant::now() >= deadline {
                failed = true;
                return Some(Err(ApiError::Deadline));
            }
            if r.is_err() {
                failed = true;
            }
            Some(r)
        })))
    }
}

/// A verified code-search body: its CodeHash and the heights its txs are known to lie in
/// (`first_h`/`last_h` of the search doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeHit {
    pub hash: [u8; 16],
    pub first_h: u32,
    pub last_h: u32,
}

/// Newest-first merge of newest-first TxKey streams, duplicates dropped (a binary heap, so
/// thousands of streams stay cheap). The first error ends it; the deadline is checked every
/// 64 items.
pub struct MergeDesc<'v> {
    its: Vec<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>>,
    heap: std::collections::BinaryHeap<(TxKey, usize)>,
    started: bool,
    last: Option<TxKey>,
    deadline: Instant,
    n: u32,
    done: bool,
}

impl<'v> MergeDesc<'v> {
    pub fn new(
        its: Vec<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>>,
        deadline: Instant,
    ) -> Self {
        MergeDesc {
            heap: std::collections::BinaryHeap::with_capacity(its.len()),
            its,
            started: false,
            last: None,
            deadline,
            n: 0,
            done: false,
        }
    }

    /// Pull the next key of stream `i` into the heap.
    fn refill(&mut self, i: usize) -> ApiResult<()> {
        match self.its[i].next() {
            Some(Ok(k)) => {
                self.heap.push((k, i));
                Ok(())
            }
            Some(Err(e)) => Err(e),
            None => Ok(()),
        }
    }
}

impl Iterator for MergeDesc<'_> {
    type Item = ApiResult<TxKey>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if !self.started {
            self.started = true;
            for i in 0..self.its.len() {
                if i % 64 == 63 && Instant::now() >= self.deadline {
                    self.done = true;
                    return Some(Err(ApiError::Deadline));
                }
                if let Err(e) = self.refill(i) {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
        loop {
            self.n += 1;
            if self.n.is_multiple_of(CHECK_EVERY) && Instant::now() >= self.deadline {
                self.done = true;
                return Some(Err(ApiError::Deadline));
            }
            let (k, i) = self.heap.pop()?;
            if let Err(e) = self.refill(i) {
                self.done = true;
                return Some(Err(e));
            }
            if self.last == Some(k) {
                continue;
            }
            self.last = Some(k);
            return Some(Ok(k));
        }
    }
}

/// `payload.cont.pactId` of a user tx's cmd.
fn cmd_pact_id(r: &TxRecord) -> Option<Hash32> {
    if r.cmd_unparsed {
        return None;
    }
    let cmd = kdx_decode::json::parse_untrusted(r.cmd.as_deref()?).ok()?;
    cmd.pointer("/payload/cont/pactId")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Hash32::from_b64url(s).ok())
}

/// `continuation.pactId` of a tx's output.
fn record_pact_id(r: &TxRecord, dict: &dyn kdx_types::Dict) -> ApiResult<Option<Hash32>> {
    if r.raw_output.is_none() && r.continuation.is_none() {
        return Ok(None);
    }
    let out = kdx_decode::render_output(r, dict).map_err(|e| ApiError::Internal(e.into()))?;
    Ok(out
        .pointer("/continuation/pactId")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Hash32::from_b64url(s).ok()))
}

#[cfg(test)]
mod data_height_tests {
    use std::time::{Duration, Instant};

    use kdx_hot::Stage;
    use kdx_types::Hash32;

    /// R13: a Headers checkpoint far above the data does not move the data tip.
    #[test]
    fn headers_checkpoint_is_not_data() {
        let (_d, store) = crate::testutil::store();
        let mut w = store.hot_for_tests().rw().unwrap();
        kdx_hot::set_checkpoint(&mut w, Stage::Headers, 0, 5_000, Hash32([1; 32])).unwrap();
        w.commit().unwrap();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        assert_eq!(view.max_indexed_height().unwrap(), Some(5_000));
        assert_eq!(view.max_data_height().unwrap(), None);
        drop(view);
        let mut w = store.hot_for_tests().rw().unwrap();
        kdx_hot::set_checkpoint(&mut w, Stage::Tail, 1, 4_000, Hash32([2; 32])).unwrap();
        w.commit().unwrap();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        assert_eq!(view.max_data_height().unwrap(), Some(4_000));
    }
}

#[cfg(test)]
mod genesis_rk_tests {
    use std::time::{Duration, Instant};

    use kdx_decode::{IndexedBlock, TxIndex};
    use kdx_hot::HotDict;
    use kdx_types::record::{BlockRecord, TxRecord};
    use kdx_types::{Hash32, TxKey};

    fn block(chain: u8, height: u32, rks: &[u8]) -> IndexedBlock {
        let txs: Vec<TxRecord> = rks
            .iter()
            .enumerate()
            .map(|(i, &x)| TxRecord {
                key: TxKey::new(height, chain, i as u16 + 1),
                request_key: Some(Hash32([x; 32])),
                ..TxRecord::default()
            })
            .collect();
        let idx = txs
            .iter()
            .map(|r| TxIndex {
                key: r.key,
                postings: Vec::new(),
                pact_id: None,
                code_hash: None,
            })
            .collect();
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([100 + chain; 32]),
                chain,
                height: u64::from(height),
                ..BlockRecord::default()
            },
            txs,
            idx,
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// F4: a genesis request key stored on several chains yields every location; the
    /// request-key index alone holds one.
    #[test]
    fn genesis_request_key_on_several_chains() {
        let (_d, store) = crate::testutil::store();
        let hot = store.hot_for_tests();
        let mut w = hot.rw().unwrap();
        w.set_meta(kdx_hot::env::meta::NETWORK, b"testnet").unwrap();
        w.set_meta(kdx_hot::env::meta::CHAIN_GRAPH, b"[[0,[0,1]],[5,[0,1,2]]]")
            .unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        // Key 1 in every genesis block (chains 0 and 1 at 0, chain 2 at 5); key 2 only on
        // chain 1; key 3 in a later, non-genesis block.
        for b in [
            block(0, 0, &[1]),
            block(1, 0, &[2, 1]),
            block(2, 5, &[1]),
            block(0, 6, &[3]),
        ] {
            kdx_hot::tail::apply(&mut w, &b, &mut dict).unwrap();
        }
        w.commit().unwrap();
        let view = store
            .view(Instant::now() + Duration::from_secs(60))
            .unwrap();
        // The index keeps one location for key 1 (the first block stored).
        let (k, _) = view.tx_by_rk(&Hash32([1; 32])).unwrap().unwrap();
        assert_eq!(k, TxKey::new(0, 0, 1));
        let keys = |x: u8| -> Vec<TxKey> {
            view.txs_by_rk(&Hash32([x; 32]))
                .unwrap()
                .into_iter()
                .map(|(k, r)| {
                    assert_eq!(r.key, k);
                    k
                })
                .collect()
        };
        assert_eq!(
            keys(1),
            vec![
                TxKey::new(0, 0, 1),
                TxKey::new(0, 1, 2),
                TxKey::new(5, 2, 1)
            ]
        );
        assert_eq!(keys(2), vec![TxKey::new(0, 1, 1)]);
        assert_eq!(keys(3), vec![TxKey::new(6, 0, 1)]);
        assert!(keys(4).is_empty());
        // Cached after the first complete scan.
        assert!(view.genesis.shared_rks.get().is_some());
    }
}
