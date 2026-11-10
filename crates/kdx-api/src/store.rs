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

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use arc_swap::ArcSwap;
use kdx_decode::Family;
use kdx_hot::tables::{block_key, parse_block_key, parse_txkey, posting_key};
use kdx_hot::{dict as hdict, HotStore, RoTxn, Stage, Table, Txn};
use kdx_segment::manifest::MANIFEST;
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
        Genesis { network, heights }
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
        Err(last.expect("three attempts").context("reloading the segments"))
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
                    tracing::debug!(ms = t.elapsed().as_millis() as u64, "api: dictionary snapshot reloaded");
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
        let hot: Box<dyn Iterator<Item = ApiResult<TxKey>> + '_> =
            if upper.is_some_and(|u| u.height() < floor) {
                Box::new(std::iter::empty())
            } else {
                let prefix = posting_key(f as u8, entity, TxKey::new(0, 0, 0));
                let up = upper.map(|u| posting_key(f as u8, entity, u).to_vec()).unwrap_or_default();
                let scan = self.txn.scan_rev_prefix(Table::TailPostings, &prefix[..9], &up)?;
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
        let frozen: Box<dyn Iterator<Item = ApiResult<TxKey>> + '_> = if self.seg_frontier == 0
            || self.seg_frontier <= min_h
        {
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
                    seg.postings_from(f, entity, Some(m))
                        .map(move |r| -> ApiResult<TxKey> {
                            let t = r?;
                            Ok(seg.txkey(t).context("posting TxNum without a key")?)
                        })
                        .take_while(move |r| r.as_ref().map_or(true, |k| k.height() >= min_h))
                        .filter(move |r| {
                            r.as_ref().map_or(true, |k| {
                                keep(k) && upper.is_none_or(|u| *k < u)
                            })
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
