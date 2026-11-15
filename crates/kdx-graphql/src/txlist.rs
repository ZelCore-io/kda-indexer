//! Tx-keyed lists (Task 4): transactions, events and transfers.
//!
//! - A list is a [`ListSpec`]: a key source (one posting list, a request key, a block, or the
//!   heights walked down from the tip), key-level filters (chain, heights, `minimumDepth`)
//!   and a row rule that turns each tx into zero or more items.
//! - List order is TxKey descending (height, then chain, then position in the block) and,
//!   inside one tx, descending by event position (the oracle's order, R-P5P9-19: order
//!   index 1 before 0). Cursors are `kdx_api::Cursor` (TxKey +
//!   position + block fingerprint), so they are portable across instances and refused after
//!   a reorg of their block.
//! - A cursor names a row: its tx and the row's position in the tx (`sub`: the event
//!   position of an event or transfer, 0 for a tx row). The next page resumes at the row after
//!   it, inside the same tx when the tx has more rows (R-P5P9-62, `through_tx`). The cursor
//!   format (version 1) did not change with that fix, only the resume: a cursor issued before
//!   it decodes as before and now resumes at its row (before, a frozen tx's remaining rows were
//!   skipped); a damaged cursor or a block-list cursor is refused with `invalid cursor`, one
//!   whose block was reorged away with `cursor invalidated by reorg`.
//! - One page is one store read (GQ-5): the records, their blocks, outputs and, when the
//!   operation selects it, the other half of each cross-chain transfer are loaded in that
//!   read; nested fields then answer from owned data. No `View` crosses an `.await`. A list
//!   is a heavy read (`ApiState::read_heavy`, H-1), except a list of one block
//!   ([`block_list`], R-P5P9-60 D1): nested in the block or a root list with `blockHash`,
//!   which reads that block by key on a plain read slot.
//! - `first`/`after` walks newest first. `last`/`before` first collects the keys between the
//!   cursor and the newest end (keys only, at most [`BACK_KEYS`]), then loads from the cursor
//!   outward. Both check the request deadline every 64 keys.
//! - `totalCount` (GQ-10) is a count walk of at most [`COUNT_CAP`] items within half of the
//!   remaining deadline (`COUNT_TIMEOUT` past it), run only when the field is selected.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_graphql::Context;
use kdx_api::compat::shapes::{self, Ev};
use kdx_api::store::PactStep;
use kdx_api::{ApiError, ApiResult, Cursor, PostingQuery, View};
use kdx_decode::index::xfer_entity;
use kdx_decode::Family;
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Dict, DictKind, Hash32, TxKey};
use serde_json::Value;

use crate::connection::{finish, Dir, Page, PageArgs, PageCursor, PageInfo, Total};
use crate::error::{bad_input, err, from_api, Code};
use crate::types::Block;
use crate::{deadline, gql};

/// Keys a backward page may collect between its cursor and the newest end.
pub const BACK_KEYS: usize = 200_000;
/// `totalCount` cap (GQ-10).
pub const COUNT_CAP: u64 = 10_000;
/// Heights a list without a selective argument walks down from the tip (as `/txs/recent`).
pub const MAX_HEIGHTS: u32 = 20_000;
/// Longest name (account, module, event, public key) a list argument may carry.
pub const MAX_NAME: usize = 512;

const CHECK_EVERY: usize = 64;
/// How long one page read may scan (H-1): a sparse or empty match stops here and releases
/// its heavy slot instead of holding it to the request deadline.
pub const SCAN_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

// ---------------------------------------------------------------------------------------
// One loaded tx

/// A tx with everything its GraphQL fields read: the record, its block, the rendered output,
/// the parsed command, events and transfers (chainweb-data rules, as compat).
#[derive(Debug)]
pub struct TxData {
    pub rec: TxRecord,
    pub block: Arc<BlockRecord>,
    pub out: Value,
    pub cmd: Option<Value>,
    pub evs: Vec<Ev>,
    pub trs: Vec<shapes::Transfer>,
    /// NFT transfers (ZelCore extension `transfers(isNFT: true)`): the four-parameter
    /// `TRANSFER (id sender receiver amount)` events of NFT ledgers, with the token id.
    pub nfts: Vec<NftTransfer>,
}

/// One NFT transfer of a tx: the transfer (amount from the fourth parameter) and its token.
#[derive(Debug, Clone)]
pub struct NftTransfer {
    pub tr: shapes::Transfer,
    pub token_id: String,
}

/// The NFT transfers of a tx's events (ledgers per `kdx_hot::nft::is_ledger`).
pub fn nft_transfers(evs: &[Ev]) -> Vec<NftTransfer> {
    evs.iter()
        .filter_map(|e| {
            if !kdx_hot::nft::is_ledger(&e.module)
                || e.qual_name != format!("{}.TRANSFER", e.module)
            {
                return None;
            }
            let p = e.params.as_array().filter(|p| p.len() == 4)?;
            let amount = match &p[3] {
                Value::Number(n) => n.to_string(),
                Value::Object(o) => match o.get("decimal").or_else(|| o.get("int"))? {
                    Value::String(s) => s.clone(),
                    v => v.to_string(),
                },
                _ => return None,
            };
            Some(NftTransfer {
                tr: shapes::Transfer {
                    pos: e.pos,
                    token: e.module.clone(),
                    from: p[1].as_str()?.to_owned(),
                    to: p[2].as_str()?.to_owned(),
                    amount,
                },
                token_id: p[0].as_str()?.to_owned(),
            })
        })
        .collect()
}

impl TxData {
    pub fn key(&self) -> TxKey {
        self.rec.key
    }
    /// Request key text. A coinbase has the node's coinbase `reqKey` from its stored output,
    /// as the oracle sends it (R-P5P9-57 F4); compat's `<coinbase>` only when the output has
    /// none.
    pub fn rk(&self) -> String {
        if self.is_coinbase() {
            if let Some(rk) = self.out.get("reqKey").and_then(Value::as_str) {
                return rk.to_owned();
            }
        }
        shapes::rk_text(&self.rec)
    }
    /// The command's `meta.creationTime` (`None` for a coinbase or a command without one).
    pub fn meta_creation_time(&self) -> Option<crate::scalars::DateTime> {
        creation_time(self.cmd.as_ref()?)
    }
    pub fn block_hash(&self) -> String {
        self.block.hash.to_b64url()
    }
    pub fn is_coinbase(&self) -> bool {
        self.rec.key.is_coinbase()
    }
    /// The page cursor of the row at `sub` inside this tx.
    pub fn cursor(&self, sub: u16) -> Cursor {
        let h = &self.block.hash.0;
        Cursor {
            key: self.rec.key,
            sub,
            fp: u32::from_be_bytes([h[0], h[1], h[2], h[3]]),
        }
    }
    pub fn sender(&self) -> Option<&str> {
        self.cmd.as_ref()?.pointer("/meta/sender")?.as_str()
    }
}

/// Block records of one read.
#[derive(Default)]
pub struct Blocks(HashMap<(u8, u32), Option<Arc<BlockRecord>>>);

impl Blocks {
    pub fn get(&mut self, view: &View, c: u8, h: u32) -> ApiResult<Option<Arc<BlockRecord>>> {
        if self.0.len() > 512 {
            self.0.clear();
        }
        if let Some(b) = self.0.get(&(c, h)) {
            return Ok(b.clone());
        }
        let b = view.block(c, h)?.map(Arc::new);
        self.0.insert((c, h), b.clone());
        Ok(b)
    }
}

/// A command's `meta.creationTime` in whole seconds, as the oracle sends it: a Pact
/// `ParsedInteger` (a number, a numeric string or `{"int": n}`), a fraction rounded as Pact
/// rounds it. One parser for `Transfer.creationTime` and `TransactionMeta.creationTime`
/// (R-P5P9-57 F3), so the two never disagree on a tx.
pub fn creation_time(cmd: &Value) -> Option<crate::scalars::DateTime> {
    let secs = shapes::parsed_integer(cmd.pointer("/meta/creationTime")?)?;
    Some(crate::scalars::DateTime(secs.checked_mul(1_000_000)?))
}

/// Load the tx at `k` (or the record already read).
pub fn load(
    view: &View,
    k: TxKey,
    rec: Option<TxRecord>,
    blocks: &mut Blocks,
) -> ApiResult<Option<Arc<TxData>>> {
    let rec = match rec {
        Some(r) => r,
        None => match view.tx(k)? {
            Some(r) => r,
            None => return Ok(None),
        },
    };
    let Some(block) = blocks.get(view, k.chain(), k.height())? else {
        return Ok(None);
    };
    let out = shapes::output(view, &rec)?;
    let cmd = if k.is_coinbase() {
        None
    } else {
        shapes::cmd_value(&rec)
    };
    let evs = shapes::events(&out);
    let trs = shapes::transfers(&evs);
    let nfts = nft_transfers(&evs);
    Ok(Some(Arc::new(TxData {
        rec,
        block,
        out,
        cmd,
        evs,
        trs,
        nfts,
    })))
}

/// The `Block` node of a stored block. `types/block.rs` (Task 3) gives `Block` its fields;
/// this is the one place tx, event and transfer nodes build it.
pub fn block_node(b: &Arc<BlockRecord>) -> Block {
    Block::new(b.clone())
}

// ---------------------------------------------------------------------------------------
// List specs

/// Where the candidate keys come from (names are resolved inside the read).
#[derive(Debug, Clone)]
pub enum SourceSpec {
    /// Txs touching an account (`acct` postings).
    Acct(String),
    /// Txs with a transfer of `module` on the account's side (`xfer` postings).
    Xfer { account: String, module: String },
    /// Txs emitting a qualified event (`evt` postings of the event name).
    EvtName(String),
    /// Txs signed by a public key (`pk` postings).
    Pk(String),
    /// `acct` postings of the account when the dictionary has it, else the `evt` postings
    /// of the event name (an `events.parametersFilter` whose first value is a string).
    AcctOrEvt { account: String, qual: String },
    /// Every stored copy of a request key.
    Rk(Hash32),
    /// The txs of one block.
    Block(Hash32),
    /// Every user tx, heights walked down from the tip (GQ-20 exception).
    Heights,
}

/// Key-level filters (no record decode).
#[derive(Debug, Clone, Copy, Default)]
pub struct KeyFilter {
    pub chain: Option<u8>,
    pub min_h: Option<u32>,
    pub max_h: Option<u32>,
    /// Only keys at least this deep below their chain's tip.
    pub depth: Option<u32>,
    /// Drop coinbase txs (transaction lists).
    pub user_only: bool,
}

/// `events.parametersFilter` (GQ-11): only `{"array_starts_with": <value or array>}`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamFilter {
    pub prefix: Vec<Value>,
}

impl ParamFilter {
    pub fn parse(s: &str) -> async_graphql::Result<ParamFilter> {
        let unsupported = || {
            err(
                Code::UnsupportedFilter,
                "parametersFilter: only {\"array_starts_with\": <value or array>} is served",
            )
        };
        if s.len() > 8 * 1024 {
            return Err(bad_input("parametersFilter is longer than 8 KB"));
        }
        let v: Value = serde_json::from_str(s).map_err(|_| unsupported())?;
        let o = v.as_object().ok_or_else(unsupported)?;
        if o.len() != 1 {
            return Err(unsupported());
        }
        let prefix = match o.get("array_starts_with") {
            Some(Value::Array(a)) if !a.is_empty() && a.len() <= 16 => a.clone(),
            Some(Value::Array(_)) | None => return Err(unsupported()),
            Some(x) => vec![x.clone()],
        };
        Ok(ParamFilter { prefix })
    }

    pub fn matches(&self, params: &Value) -> bool {
        params.as_array().is_some_and(|p| {
            p.len() >= self.prefix.len() && self.prefix.iter().zip(p).all(|(a, b)| a == b)
        })
    }

    /// The first parameter when it is a string (an account reference candidate).
    pub fn first_str(&self) -> Option<&str> {
        self.prefix.first().and_then(Value::as_str)
    }
}

/// How one tx turns into list items.
#[derive(Debug, Clone)]
pub enum Rows {
    /// The tx itself; `sender` keeps only txs whose `meta.sender` is it; `nft` keeps only txs
    /// with an event of an NFT ledger.
    Txs { sender: Option<String>, nft: bool },
    /// Its events (named `qual` when given: `Block.events` lists them all; of `module` when
    /// given, the ZelCore `events(moduleName)` extension), with the optional `orderIndex` and
    /// parameter filters.
    Events {
        qual: Option<String>,
        module: Option<String>,
        order_index: Option<i64>,
        params: Option<ParamFilter>,
    },
    /// Its transfers, of `module` and with `account` on a side when given; its NFT
    /// transfers instead with `nft` (ZelCore `transfers(isNFT: true)`).
    Transfers {
        module: Option<String>,
        account: Option<String>,
        nft: bool,
    },
}

/// One list item: a tx, or the event or transfer at index `i` of it.
#[derive(Debug, Clone)]
pub enum Item {
    Tx(Arc<TxData>),
    Event(Arc<TxData>, usize),
    /// The transfer and, when preloaded, the other half of a cross-chain transfer.
    Transfer(Arc<TxData>, usize, Option<XHalf>),
    /// The NFT transfer at index `i` of `nfts`.
    Nft(Arc<TxData>, usize),
}

/// A preloaded `crossChainTransfer`: `None` when there is no other half (yet).
pub type XHalf = Option<(Arc<TxData>, usize)>;

fn sub_of(pos: usize) -> u16 {
    u16::try_from(pos).unwrap_or(u16::MAX)
}

impl Rows {
    /// Whether a count must decode records (one key is not one item).
    fn needs_record(&self) -> bool {
        !matches!(
            self,
            Rows::Txs {
                sender: None,
                nft: false
            }
        )
    }

    pub fn rows(&self, d: &Arc<TxData>) -> Vec<(u16, Item)> {
        match self {
            Rows::Txs { sender, nft } => {
                if d.is_coinbase() {
                    return vec![];
                }
                if sender.as_deref().is_some_and(|s| d.sender() != Some(s)) {
                    return vec![];
                }
                if *nft && !d.evs.iter().any(|e| kdx_hot::nft::is_ledger(&e.module)) {
                    return vec![];
                }
                vec![(0, Item::Tx(d.clone()))]
            }
            Rows::Events {
                qual,
                module,
                order_index,
                params,
            } => d
                .evs
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    qual.as_deref().is_none_or(|q| e.qual_name == q)
                        && module.as_deref().is_none_or(|m| e.module == m)
                        && order_index.is_none_or(|o| shapes::ev_idx(d.key(), e.pos) == o)
                        && params.as_ref().is_none_or(|p| p.matches(&e.params))
                })
                .map(|(i, e)| (sub_of(e.pos), Item::Event(d.clone(), i)))
                .collect(),
            Rows::Transfers {
                module,
                account,
                nft: true,
            } => d
                .nfts
                .iter()
                .enumerate()
                .filter(|(_, n)| {
                    let t = &n.tr;
                    module.as_deref().is_none_or(|m| t.token == m)
                        && account.as_deref().is_none_or(|a| t.from == a || t.to == a)
                })
                .map(|(i, n)| (sub_of(n.tr.pos), Item::Nft(d.clone(), i)))
                .collect(),
            Rows::Transfers {
                module,
                account,
                nft: false,
            } => d
                .trs
                .iter()
                .enumerate()
                .filter(|(_, t)| {
                    module.as_deref().is_none_or(|m| t.token == m)
                        && account.as_deref().is_none_or(|a| t.from == a || t.to == a)
                })
                .map(|(i, t)| (sub_of(t.pos), Item::Transfer(d.clone(), i, None)))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListSpec {
    pub source: SourceSpec,
    pub filter: KeyFilter,
    pub rows: Rows,
}

// ---------------------------------------------------------------------------------------
// Key sources

enum Src {
    Post(Family, u64),
    Rk(Hash32),
    Block(u8, u32),
    Heights,
    Empty,
}

fn resolve(view: &View, s: &SourceSpec) -> ApiResult<Src> {
    let d = view.dict();
    Ok(match s {
        SourceSpec::Acct(a) => match d.find(DictKind::Account, a) {
            Some(id) => Src::Post(Family::Acct, u64::from(id)),
            None => Src::Empty,
        },
        SourceSpec::Xfer { account, module } => match (
            d.find(DictKind::Account, account),
            d.find(DictKind::Module, module),
        ) {
            (Some(a), Some(m)) => Src::Post(Family::Xfer, xfer_entity(a, m)),
            _ => Src::Empty,
        },
        SourceSpec::EvtName(q) => match d.find(DictKind::EventName, q) {
            Some(id) => Src::Post(Family::Evt, u64::from(id)),
            None => Src::Empty,
        },
        SourceSpec::Pk(pk) => match d.find(DictKind::PubKey, pk) {
            Some(id) => Src::Post(Family::Pk, u64::from(id)),
            None => Src::Empty,
        },
        SourceSpec::AcctOrEvt { account, qual } => match d.find(DictKind::Account, account) {
            Some(id) => Src::Post(Family::Acct, u64::from(id)),
            None => match d.find(DictKind::EventName, qual) {
                Some(id) => Src::Post(Family::Evt, u64::from(id)),
                None => Src::Empty,
            },
        },
        SourceSpec::Rk(rk) => Src::Rk(*rk),
        SourceSpec::Block(h) => match view.block_by_hash(h)? {
            Some((c, height, _)) => Src::Block(c, height),
            None => Src::Empty,
        },
        SourceSpec::Heights => Src::Heights,
    })
}

/// Highest height allowed per chain by `minimumDepth` (chains without a tip allow none).
struct Caps(Option<HashMap<u8, Option<u32>>>);

impl Caps {
    fn of(view: &View, f: &KeyFilter) -> ApiResult<Caps> {
        let Some(depth) = f.depth else {
            return Ok(Caps(None));
        };
        let mut m = HashMap::new();
        for (c, h, _) in view.chain_tips()? {
            m.insert(c, h.checked_sub(depth));
        }
        Ok(Caps(Some(m)))
    }
    fn allows(&self, k: TxKey) -> bool {
        match &self.0 {
            None => true,
            Some(m) => m
                .get(&k.chain())
                .copied()
                .flatten()
                .is_some_and(|max| k.height() <= max),
        }
    }
    fn max(&self) -> Option<u32> {
        self.0
            .as_ref()
            .map(|m| m.values().filter_map(|x| *x).max().unwrap_or(0))
    }
}

/// Bounds of one walk: keys at or below `upto`, heights at or above `min_h`.
#[derive(Debug, Clone, Copy, Default)]
struct Bounds {
    upto: Option<TxKey>,
    min_h: Option<u32>,
}

type KeySource<'v> = Box<dyn Iterator<Item = ApiResult<(TxKey, Option<TxRecord>)>> + 'v>;

fn opt_max(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, y) => x.or(y),
    }
}

fn opt_min(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    }
}

/// The exclusive posting bound of a walk that lists keys up to and including `upto`: the next
/// tx position after it (a TxKey keeps the position in bits 8..24), as the native walk bounds
/// a cursor (`kdx_api::native`). A page that ended inside a tx resumes at that tx's next row,
/// so the tx itself must stay in the walk (R-P5P9-62). The bound was `upto + 1`, which is not a
/// tx key: the frozen half of `View::postings` looked it up by its (height, chain, position),
/// found the cursor's own tx and stopped below it, so the rest of a frozen tx was dropped and
/// the page resumed at the next tx. `View::postings` now keeps that tx for such a bound too.
fn through_tx(upto: TxKey) -> TxKey {
    TxKey(upto.0.saturating_add(1 << 8))
}

/// Candidate keys, newest first, with every key-level filter applied.
fn keys<'v>(
    view: &'v View,
    src: &Src,
    f: KeyFilter,
    caps: &'v Caps,
    b: Bounds,
) -> ApiResult<KeySource<'v>> {
    let min_h = opt_max(f.min_h, b.min_h);
    let max_h = opt_min(f.max_h, caps.max());
    let keep = move |k: TxKey| {
        (!f.user_only || !k.is_coinbase())
            && f.chain.is_none_or(|c| k.chain() == c)
            && min_h.is_none_or(|m| k.height() >= m)
            && max_h.is_none_or(|m| k.height() <= m)
            && b.upto.is_none_or(|u| k <= u)
            && caps.allows(k)
    };
    let listed = |mut v: Vec<(TxKey, TxRecord)>| -> KeySource<'v> {
        v.retain(|(k, _)| keep(*k));
        v.sort_by_key(|(k, _)| std::cmp::Reverse(*k));
        Box::new(v.into_iter().map(|(k, r)| Ok((k, Some(r)))))
    };
    Ok(match src {
        Src::Empty => Box::new(std::iter::empty()),
        Src::Post(fam, entity) => {
            let q = PostingQuery {
                min_height: min_h,
                max_height: max_h,
                chain: f.chain,
                before: b.upto.map(through_tx),
            };
            Box::new(
                view.postings(*fam, *entity, q)?
                    .filter(move |r| r.as_ref().map_or(true, |k| keep(*k)))
                    .map(|r| r.map(|k| (k, None))),
            )
        }
        Src::Rk(rk) => listed(view.txs_by_rk(rk)?),
        Src::Block(c, h) => listed(
            view.block_txs(*c, *h)?
                .into_iter()
                .map(|r| (r.key, r))
                .collect(),
        ),
        Src::Heights => {
            let Some(top) = view.max_data_height()? else {
                return Ok(Box::new(std::iter::empty()));
            };
            let top = opt_min(Some(top), opt_min(max_h, b.upto.map(|u| u.height())));
            let top = top.unwrap_or(0);
            let floor = kdx_api::compat::store_floor(view).max(min_h.unwrap_or(0));
            let chains: Vec<u8> = match f.chain {
                Some(c) => vec![c],
                None => view.chain_tips()?.into_iter().map(|(c, _, _)| c).collect(),
            };
            let mut h = Some(top);
            let mut steps = 0u32;
            let mut buf: std::vec::IntoIter<(TxKey, TxRecord)> = Vec::new().into_iter();
            Box::new(std::iter::from_fn(move || loop {
                if let Some((k, r)) = buf.next() {
                    return Some(Ok((k, Some(r))));
                }
                let cur = h.filter(|x| *x >= floor && steps < MAX_HEIGHTS)?;
                h = cur.checked_sub(1);
                steps += 1;
                if let Err(e) = view.check_deadline() {
                    h = None;
                    return Some(Err(e));
                }
                let mut v = Vec::new();
                for &c in &chains {
                    match view.block_txs(c, cur) {
                        Ok(txs) => v.extend(txs.into_iter().map(|r| (r.key, r))),
                        Err(e) => {
                            h = None;
                            return Some(Err(e));
                        }
                    }
                }
                v.retain(|(k, _)| keep(*k));
                v.sort_by_key(|(k, _)| std::cmp::Reverse(*k));
                buf = v.into_iter();
            }))
        }
    })
}

// ---------------------------------------------------------------------------------------
// Pages and counts

fn tx_cursor(view: &View, page: &Page<PageCursor>) -> ApiResult<Option<Cursor>> {
    match &page.cursor {
        None => Ok(None),
        Some(PageCursor::Tx(c)) => {
            c.check(view)?;
            Ok(Some(*c))
        }
        Some(PageCursor::Block(_)) => Err(ApiError::BadRequest("invalid cursor".into())),
    }
}

fn past(deadline: Instant, i: usize) -> ApiResult<()> {
    if i % CHECK_EVERY == CHECK_EVERY - 1 && Instant::now() >= deadline {
        return Err(ApiError::Deadline);
    }
    Ok(())
}

/// One page of `spec`, as `(cursor, item)` edges in list order.
pub fn run_page(
    view: &View,
    spec: &ListSpec,
    page: &Page<PageCursor>,
    deadline: Instant,
) -> ApiResult<(Vec<(String, Item)>, PageInfo)> {
    let cur = tx_cursor(view, page)?;
    let src = resolve(view, &spec.source)?;
    let caps = Caps::of(view, &spec.filter)?;
    let mut blocks = Blocks::default();
    let mut walked: Vec<(String, Item)> = Vec::new();
    match page.dir {
        Dir::Forward => {
            let b = Bounds {
                upto: cur.map(|c| c.key),
                min_h: None,
            };
            'k: for (i, kr) in keys(view, &src, spec.filter, &caps, b)?.enumerate() {
                past(deadline, i)?;
                let (k, rec) = kr?;
                let Some(d) = load(view, k, rec, &mut blocks)? else {
                    continue;
                };
                let mut rs = spec.rows.rows(&d);
                rs.reverse();
                for (sub, it) in rs {
                    if cur.is_some_and(|c| c.key == k && sub >= c.sub) {
                        continue;
                    }
                    walked.push((d.cursor(sub).encode(), it));
                    if walked.len() > page.n {
                        break 'k;
                    }
                }
            }
        }
        Dir::Backward => {
            let b = Bounds {
                upto: None,
                min_h: cur.map(|c| c.key.height()),
            };
            let mut ks = Vec::new();
            for (i, kr) in keys(view, &src, spec.filter, &caps, b)?.enumerate() {
                past(deadline, i)?;
                let (k, _) = kr?;
                if cur.is_some_and(|c| k < c.key) {
                    break;
                }
                ks.push(k);
                if ks.len() > BACK_KEYS {
                    return Err(ApiError::Unavailable(format!(
                        "last/before reaches more than {BACK_KEYS} txs: page with first/after"
                    )));
                }
            }
            'b: for (i, k) in ks.into_iter().rev().enumerate() {
                past(deadline, i)?;
                let Some(d) = load(view, k, None, &mut blocks)? else {
                    continue;
                };
                for (sub, it) in spec.rows.rows(&d) {
                    if cur.is_some_and(|c| c.key == k && sub <= c.sub) {
                        continue;
                    }
                    walked.push((d.cursor(sub).encode(), it));
                    if walked.len() > page.n {
                        break 'b;
                    }
                }
            }
        }
    }
    let (edges, info) = finish(walked, page, |e| e.0.clone());
    Ok((edges.into_iter().map(|(_, e)| e).collect(), info))
}

/// Items of `spec`, up to [`COUNT_CAP`], and whether the count is exact: when `until`
/// passes first, the items counted so far (a lower bound) and `false`.
pub fn count(view: &View, spec: &ListSpec, until: Instant) -> ApiResult<(u64, bool)> {
    let src = resolve(view, &spec.source)?;
    let caps = Caps::of(view, &spec.filter)?;
    let mut blocks = Blocks::default();
    let decode = spec.rows.needs_record();
    let mut n = 0u64;
    for (i, kr) in keys(view, &src, spec.filter, &caps, Bounds::default())?.enumerate() {
        if i % CHECK_EVERY == CHECK_EVERY - 1 && Instant::now() >= until {
            return Ok((n, false));
        }
        let (k, rec) = kr?;
        if decode {
            if let Some(d) = load(view, k, rec, &mut blocks)? {
                n += spec.rows.rows(&d).len() as u64;
            }
        } else {
            n += 1;
        }
        if n >= COUNT_CAP {
            return Ok((COUNT_CAP, true));
        }
    }
    Ok((n, true))
}

/// Steps of a pact a `crossChainTransfer` lookup reads first, and at most when the pact has
/// more (failed retries of a continuation can push the step that succeeded out of the
/// newest 16; R-P5P9-57, node-checked: pact `XSU4iQjQ…` has 34 steps). The larger bound is
/// the native defpact cap.
const XCHAIN_STEPS: usize = 16;
const XCHAIN_STEPS_MAX: usize = 1_000;
/// Pact-step reads one request may spend on `crossChainTransfer` lookups, shared by every
/// edge and field of the request (H-1: a page of transfers whose pacts carry many retries
/// cannot multiply the per-item bound). Past it a lookup answers "no other half", as one
/// that found none.
pub const XCHAIN_BUDGET: usize = 2_000;

/// The request's budget of pact-step reads for `crossChainTransfer` ([`XCHAIN_BUDGET`]).
#[derive(Debug)]
pub struct XchainBudget(AtomicUsize);

impl Default for XchainBudget {
    fn default() -> Self {
        XchainBudget(AtomicUsize::new(XCHAIN_BUDGET))
    }
}

impl XchainBudget {
    /// Take up to `want` reads; the number granted (0 when spent).
    pub fn take(&self, want: usize) -> usize {
        let mut got = 0;
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                got = left.min(want);
                Some(left - got)
            });
        got
    }
    /// Give back reads granted but not used.
    pub fn refund(&self, n: usize) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn left(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// The budget of the request being resolved (one per request, installed with the response
/// flags); a fresh one outside a request.
pub fn xchain_budget(ctx: &Context<'_>) -> Arc<XchainBudget> {
    ctx.data_opt::<Arc<XchainBudget>>()
        .cloned()
        .unwrap_or_default()
}

/// The first succeeded continuation step of a pact (oldest first), other than `me`, that
/// `matches`. `steps_of(cap)` lists up to `cap` steps (the newest ones, ascending) and
/// whether there are more. Reads [`XCHAIN_STEPS`] first and, only when the pact has more,
/// again up to [`XCHAIN_STEPS_MAX`]; every listed step and every full decode is paid from
/// `budget`, and `check` (the request deadline) runs before each decode. Start and failed
/// steps are skipped on their stored flags, with no decode. A spent budget answers `None`.
fn find_step<T>(
    mut steps_of: impl FnMut(usize) -> ApiResult<(Vec<PactStep>, bool)>,
    me: TxKey,
    budget: &XchainBudget,
    mut check: impl FnMut() -> ApiResult<()>,
    mut matches: impl FnMut(TxKey) -> ApiResult<Option<T>>,
) -> ApiResult<Option<T>> {
    for want in [XCHAIN_STEPS, XCHAIN_STEPS_MAX] {
        let cap = budget.take(want);
        if cap == 0 {
            return Ok(None);
        }
        let (steps, capped) = steps_of(cap)?;
        budget.refund(cap.saturating_sub(steps.len()));
        for st in steps {
            if !st.cont || !st.ok || st.key == me {
                continue;
            }
            check()?;
            if budget.take(1) == 0 {
                return Ok(None);
            }
            if let Some(t) = matches(st.key)? {
                return Ok(Some(t));
            }
        }
        if !capped || cap < want {
            break;
        }
    }
    Ok(None)
}

/// The other half of the cross-chain transfer at `trs[i]` of `d`: for an outgoing half
/// (empty receiver) the `cont` step of the pact this tx started, for an incoming half (empty
/// sender, a `cont` tx) the tx that started the pact. `None` when it is not (yet) indexed.
pub fn xchain_of(
    view: &View,
    d: &TxData,
    i: usize,
    blocks: &mut Blocks,
    budget: &XchainBudget,
) -> ApiResult<Option<(Arc<TxData>, usize)>> {
    let Some(t) = d.trs.get(i) else {
        return Ok(None);
    };
    if d.is_coinbase() {
        return Ok(None);
    }
    if t.to.is_empty() && !t.from.is_empty() {
        let Some(rk) = d.rec.request_key else {
            return Ok(None);
        };
        return find_step(
            |cap| view.pact_steps(&rk, cap),
            d.key(),
            budget,
            || view.check_deadline(),
            |k| {
                let Some(o) = load(view, k, None, blocks)? else {
                    return Ok(None);
                };
                Ok(o.trs
                    .iter()
                    .position(|x| x.from.is_empty() && !x.to.is_empty() && x.token == t.token)
                    .map(|j| (o.clone(), j)))
            },
        );
    }
    if t.from.is_empty() && !t.to.is_empty() {
        let Some(pact) = d.cmd.as_ref().and_then(shapes::pact_id) else {
            return Ok(None);
        };
        let Some((k, r)) = view.tx_by_rk(&pact)? else {
            return Ok(None);
        };
        if let Some(o) = load(view, k, Some(r), blocks)? {
            if let Some(j) = o
                .trs
                .iter()
                .position(|x| x.to.is_empty() && !x.from.is_empty() && x.token == t.token)
            {
                return Ok(Some((o, j)));
            }
        }
    }
    Ok(None)
}

/// Whether the field being resolved selects `name` anywhere below it.
pub fn selects(ctx: &Context<'_>, name: &str) -> bool {
    fn walk(f: async_graphql::SelectionField<'_>, name: &str, depth: usize) -> bool {
        depth < 16
            && f.selection_set()
                .any(|s| s.name() == name || walk(s, name, depth + 1))
    }
    walk(ctx.field(), name, 0)
}

/// Whether the field being resolved selects one of `names` directly (aliases resolved).
pub fn selects_direct(ctx: &Context<'_>, names: &[&str]) -> bool {
    ctx.field()
        .selection_set()
        .any(|s| names.contains(&s.name()))
}

/// A page of a tx-keyed list and its lazy `totalCount`.
pub struct ListPage {
    pub edges: Vec<(String, Item)>,
    pub info: PageInfo,
    pub total: Total,
}

/// Run one list page for the field being resolved, as a heavy read (H-1: per-client share).
pub async fn list(
    ctx: &Context<'_>,
    spec: ListSpec,
    args: PageArgs,
) -> async_graphql::Result<ListPage> {
    list_as(ctx, spec, args, false).await
}

/// `list` for a list of one block: a connection nested in it (`Block.transactions`,
/// `Block.events`; R-P5P9-60 D1) or a root list with `blockHash` and no `requestKey`
/// (`transactions`, `events`, `transfers`; R-P5P9-60 round 2). A plain read, not a per-client
/// heavy one, so one request over many blocks gets every block's page. Such a page reads only
/// its own block, by key: the block's location by hash, the block's txs (`block_txs` at that
/// chain and height; `chainId`, `minHeight` and `maxHeight` only filter those keys in memory),
/// the cursor's block fingerprint, and each tx's block record and output; its `totalCount`
/// walks the same txs. Anything that could read past the block stays a heavy read: another
/// source (an account, a posting, a height walk, a request key, which wins over `blockHash`),
/// a `minimumDepth` filter (it reads the chain tips), and, for transfers, a selected
/// `crossChainTransfer` (it reads the pact's other chain).
pub async fn block_list(
    ctx: &Context<'_>,
    spec: ListSpec,
    args: PageArgs,
) -> async_graphql::Result<ListPage> {
    let one_block = matches!(spec.source, SourceSpec::Block(_)) && spec.filter.depth.is_none();
    list_as(ctx, spec, args, one_block).await
}

/// One store read of `state`: plain when `one_block` ([`block_list`]), else heavy.
async fn read_list<T, F>(state: &Arc<kdx_api::ApiState>, one_block: bool, f: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(&View) -> ApiResult<T> + Send + 'static,
{
    if one_block {
        state.read(f).await
    } else {
        state.read_heavy(f).await
    }
}

async fn list_as(
    ctx: &Context<'_>,
    spec: ListSpec,
    args: PageArgs,
    one_block: bool,
) -> async_graphql::Result<ListPage> {
    let g = gql(ctx);
    let state = g.state()?.clone();
    let page = args.parse(&g.settings)?;
    let dl = deadline(ctx);
    let xchain = matches!(spec.rows, Rows::Transfers { nft: false, .. })
        && selects(ctx, "crossChainTransfer");
    let budget = xchain_budget(ctx);
    // A `crossChainTransfer` lookup reads the pact's other chain: past one block.
    let one_block = one_block && !xchain;
    // The page is read only when its items or page info are selected.
    let reads = selects_direct(ctx, &["edges", "pageInfo"]);
    // A block's or a request key's rows are few: counted with the page, exactly.
    let bounded = matches!(spec.source, SourceSpec::Block(_) | SourceSpec::Rk(_))
        && selects_direct(ctx, &["totalCount"]);
    let sp = spec.clone();
    let (edges, info, exact) = read_list(&state, one_block, move |view| {
        let dl = dl.min(Instant::now() + SCAN_WINDOW);
        let exact = if bounded {
            Some(count(view, &sp, dl)?.0)
        } else {
            None
        };
        if !reads {
            return Ok((Vec::new(), PageInfo::default(), exact));
        }
        let (mut edges, info) = run_page(view, &sp, &page, dl)?;
        if xchain {
            let mut blocks = Blocks::default();
            for (_, it) in edges.iter_mut() {
                if let Item::Transfer(d, i, x) = it {
                    view.check_deadline()?;
                    *x = Some(xchain_of(view, d, *i, &mut blocks, &budget)?);
                }
            }
        }
        Ok((edges, info, exact))
    })
    .await
    .map_err(from_api)?;
    if let Some(n) = exact {
        return Ok(ListPage {
            edges,
            info,
            total: Total::Exact(n),
        });
    }
    let total = Total::Deferred(Arc::new(move || {
        let state = state.clone();
        let spec = spec.clone();
        Box::pin(async move {
            let now = Instant::now();
            let until = now + dl.saturating_duration_since(now) / 2;
            read_list(&state, one_block, move |view| count(view, &spec, until))
                .await
                .map_err(from_api)
        })
    }));
    Ok(ListPage { edges, info, total })
}

/// The rows of one tx a page walks, up to `take`: `rows` come ascending by position; the list
/// is descending (the oracle's order), so a forward page starts at the highest position
/// below the cursor and a backward one at the lowest above it.
fn in_tx_walk<T>(rows: Vec<(u16, T)>, dir: Dir, cur: Option<u16>, take: usize) -> Vec<(u16, T)> {
    match dir {
        Dir::Forward => rows
            .into_iter()
            .rev()
            .filter(|(s, _)| cur.is_none_or(|c| *s < c))
            .take(take)
            .collect(),
        Dir::Backward => rows
            .into_iter()
            .filter(|(s, _)| cur.is_none_or(|c| *s > c))
            .take(take)
            .collect(),
    }
}

/// Page over the rows of one tx (`TransactionResult.events`/`transfers`): `subs` are the
/// rows' positions (the rows come ascending; the page lists them descending).
pub fn page_in_tx<T: Clone>(
    d: &TxData,
    rows: Vec<(u16, T)>,
    page: &Page<PageCursor>,
) -> async_graphql::Result<(Vec<(String, T)>, PageInfo)> {
    let cur = match &page.cursor {
        None => None,
        Some(PageCursor::Tx(c)) if c.key == d.key() => Some(c.sub),
        Some(_) => return Err(bad_input("invalid cursor")),
    };
    let walked: Vec<(String, T)> = in_tx_walk(rows, page.dir, cur, page.n + 1)
        .into_iter()
        .map(|(s, t)| (d.cursor(s).encode(), t))
        .collect();
    let (edges, info) = finish(walked, page, |e| e.0.clone());
    Ok((edges.into_iter().map(|(_, e)| e).collect(), info))
}

// ---------------------------------------------------------------------------------------
// Argument parsing

/// A `chainId` argument (`"0"`..`"99"`).
pub fn chain_arg(s: &str) -> async_graphql::Result<u8> {
    match s.parse::<u8>() {
        Ok(c) if s.len() <= 2 && c < 100 => Ok(c),
        _ => Err(bad_input("chainId must be a chain number such as \"0\"")),
    }
}

/// A non-negative height or depth argument.
pub fn height_arg(name: &str, v: Option<i32>) -> async_graphql::Result<Option<u32>> {
    match v {
        None => Ok(None),
        Some(x) if x >= 0 => Ok(Some(x as u32)),
        Some(_) => Err(bad_input(format!("{name} must not be negative"))),
    }
}

/// A request key or block hash argument (43-char base64url).
pub fn hash_arg(name: &str, s: &str) -> async_graphql::Result<Hash32> {
    if s.len() > 64 {
        return Err(bad_input(format!("{name} is not a hash")));
    }
    Hash32::from_b64url(s).map_err(|_| bad_input(format!("{name} is not a hash")))
}

/// A name argument (account, module, event name, public key).
pub fn name_arg<'a>(name: &str, s: &'a str) -> async_graphql::Result<&'a str> {
    if s.is_empty() || s.len() > MAX_NAME {
        return Err(bad_input(format!("{name} must be 1..={MAX_NAME} bytes")));
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn params_filter_forms() {
        let f = ParamFilter::parse(r#"{"array_starts_with":"k:abc"}"#).unwrap();
        assert_eq!(f.first_str(), Some("k:abc"));
        assert!(f.matches(&json!(["k:abc", "k:def", 1.5])));
        assert!(!f.matches(&json!(["k:def", "k:abc"])));
        assert!(!f.matches(&json!([])));
        let f = ParamFilter::parse(r#"{"array_starts_with":["k:a","k:b"]}"#).unwrap();
        assert!(f.matches(&json!(["k:a", "k:b", {"decimal": "1.0"}])));
        assert!(!f.matches(&json!(["k:a"])));
        for bad in [
            r#"{"path":["0"],"equals":"x"}"#,
            r#"{"array_starts_with":"a","x":1}"#,
            r#"{"array_starts_with":[]}"#,
            "[1]",
            "not json",
        ] {
            let e = ParamFilter::parse(bad).unwrap_err();
            assert_eq!(
                e.extensions
                    .as_ref()
                    .and_then(|x| x.get("code"))
                    .map(|v| v.to_string()),
                Some("\"UNSUPPORTED_FILTER\"".into()),
                "{bad}"
            );
        }
    }

    #[test]
    fn rows_inside_one_tx_list_descending() {
        let rows = || vec![(0u16, "a"), (1, "b"), (2, "c")];
        // Forward: 2, 1, 0; after the cursor at 1 only 0 is left.
        let f = in_tx_walk(rows(), Dir::Forward, None, 10);
        assert_eq!(f.iter().map(|r| r.0).collect::<Vec<_>>(), [2, 1, 0]);
        let f = in_tx_walk(rows(), Dir::Forward, Some(1), 10);
        assert_eq!(f.iter().map(|r| r.0).collect::<Vec<_>>(), [0]);
        // Backward (`last`/`before`): walks away from the cursor towards the newest end.
        let b = in_tx_walk(rows(), Dir::Backward, Some(0), 10);
        assert_eq!(b.iter().map(|r| r.0).collect::<Vec<_>>(), [1, 2]);
    }

    #[test]
    fn argument_bounds() {
        assert_eq!(chain_arg("19").unwrap(), 19);
        assert!(chain_arg("x").is_err() && chain_arg("100").is_err() && chain_arg("-1").is_err());
        assert!(height_arg("minHeight", Some(-1)).is_err());
        assert_eq!(height_arg("minHeight", Some(5)).unwrap(), Some(5));
        assert!(name_arg("accountName", "").is_err());
        assert!(name_arg("accountName", &"a".repeat(513)).is_err());
        assert!(hash_arg("requestKey", "short").is_err());
    }
}

#[cfg(test)]
mod nft_tests {
    use super::*;
    use serde_json::json;

    fn ev(pos: usize, module: &str, name: &str, params: Value) -> Ev {
        Ev {
            pos,
            qual_name: format!("{module}.{name}"),
            module: module.to_owned(),
            module_hash: String::new(),
            params,
        }
    }

    #[test]
    fn nft_transfers_are_four_parameter_ledger_transfers() {
        let evs = vec![
            ev(0, "coin", "TRANSFER", json!(["a", "b", 1.5])),
            ev(
                1,
                "marmalade-v2.ledger",
                "TRANSFER",
                json!(["t:x", "a", "b", 1.0]),
            ),
            ev(
                2,
                "marmalade-v2.ledger",
                "TRANSFER",
                json!(["t:y", "a", "b", {"decimal": "2.5"}]),
            ),
            ev(3, "free.ledger", "TRANSFER", json!(["t:z", "a", "b", 1.0])),
            ev(
                4,
                "marmalade-v2.ledger",
                "RECONCILE",
                json!(["t:x", 1.0, {}, {}]),
            ),
            ev(
                5,
                "marmalade-v2.ledger",
                "TRANSFER",
                json!(["t:w", "a", "b"]),
            ),
        ];
        let n = nft_transfers(&evs);
        let got: Vec<_> = n
            .iter()
            .map(|t| {
                (
                    t.token_id.as_str(),
                    t.tr.pos,
                    t.tr.from.as_str(),
                    t.tr.amount.as_str(),
                )
            })
            .collect();
        assert_eq!(got, vec![("t:x", 1, "a", "1.0"), ("t:y", 2, "a", "2.5")]);
        // The fungible rule is unchanged: only the coin transfer.
        assert_eq!(shapes::transfers(&evs).len(), 1);
    }
}

#[cfg(test)]
mod xchain_tests {
    use std::cell::Cell;

    use super::*;

    fn step(key: TxKey, cont: bool, ok: bool) -> PactStep {
        PactStep { key, cont, ok }
    }

    /// The newest `cap` of `all` (ascending) and whether there are more, as `pact_steps`.
    fn newest(all: &[PactStep], cap: usize) -> (Vec<PactStep>, bool) {
        let from = all.len().saturating_sub(cap);
        (all[from..].to_vec(), from > 0)
    }

    /// R-P5P9-57 (F10 crossChainTransfer): the step that succeeded is found even when more
    /// than 16 later retries of the continuation follow it; failed retries are never decoded.
    #[test]
    fn successful_step_behind_many_retries_is_found() {
        let me = TxKey::new(3_115_490, 2, 1);
        let ok = TxKey::new(3_115_507, 1, 1);
        let mut all = vec![step(me, false, true), step(ok, true, true)];
        all.extend((0..32u32).map(|i| step(TxKey::new(3_115_509 + i, 1, 2), true, false)));
        let budget = XchainBudget::default();
        let mut caps = Vec::new();
        let decoded = Cell::new(0);
        let got = find_step(
            |cap| {
                caps.push(cap);
                Ok(newest(&all, cap))
            },
            me,
            &budget,
            || Ok(()),
            |k| {
                decoded.set(decoded.get() + 1);
                Ok((k == ok).then_some(k))
            },
        )
        .unwrap();
        assert_eq!(got, Some(ok));
        assert_eq!(caps, [XCHAIN_STEPS, XCHAIN_STEPS_MAX]);
        assert_eq!(decoded.get(), 1, "only the succeeded step is decoded");
        // Charged: the 16 listed, the 34 listed, one decode.
        assert_eq!(XCHAIN_BUDGET - budget.left(), 16 + 34 + 1);
    }

    /// The honest case: a short pact is read once; the start itself never matches.
    #[test]
    fn short_pact_is_read_once() {
        let me = TxKey::new(10, 0, 1);
        let ok = TxKey::new(12, 1, 1);
        let all = vec![step(me, false, true), step(ok, true, true)];
        let budget = XchainBudget::default();
        let mut calls = 0;
        let got = find_step(
            |cap| {
                calls += 1;
                Ok(newest(&all, cap))
            },
            me,
            &budget,
            || Ok(()),
            |k| Ok(Some(k)),
        )
        .unwrap();
        assert_eq!((got, calls), (Some(ok), 1));
        assert_eq!(XCHAIN_BUDGET - budget.left(), 2 + 1);
        let none = find_step(
            |_| Ok((vec![step(me, true, true)], false)),
            me,
            &budget,
            || Ok(()),
            |k| Ok(Some(k)),
        )
        .unwrap();
        assert_eq!(none, None);
    }

    /// The security finding on e7da3d0: a page of N edges whose pacts each carry more than
    /// 16 failed retries spends at most the request budget in total (not N times the
    /// per-item bound); past it every lookup answers `None`, not an error.
    #[test]
    fn a_page_of_retry_heavy_pacts_stays_within_the_request_budget() {
        const N: usize = 100;
        let budget = XchainBudget::default();
        let listed = Cell::new(0usize);
        let decoded = Cell::new(0usize);
        let mut answers = Vec::new();
        for e in 0..N as u32 {
            let me = TxKey::new(1_000 + e, 2, 1);
            let mut all = vec![step(me, false, true)];
            all.extend(
                (0..900u32).map(|i| step(TxKey::new(2_000 + i, 1, 1 + e as u16), true, false)),
            );
            let got = find_step(
                |cap| {
                    let r = newest(&all, cap);
                    listed.set(listed.get() + r.0.len());
                    Ok(r)
                },
                me,
                &budget,
                || Ok(()),
                |k| {
                    decoded.set(decoded.get() + 1);
                    Ok(Some(k))
                },
            );
            answers.push(got.expect("a spent budget is not an error"));
        }
        assert!(answers.iter().all(Option::is_none));
        assert_eq!(decoded.get(), 0, "failed retries are never decoded");
        assert!(listed.get() <= XCHAIN_BUDGET, "listed {}", listed.get());
        assert_eq!(budget.left(), 0);
        // An honest pact after the budget is spent answers "no other half".
        let me = TxKey::new(5, 0, 1);
        let ok = TxKey::new(6, 1, 1);
        let all = [step(me, false, true), step(ok, true, true)];
        let got = find_step(
            |cap| Ok(newest(&all, cap)),
            me,
            &budget,
            || Ok(()),
            |k| Ok(Some(k)),
        );
        assert_eq!(got.unwrap(), None);
    }

    /// The request deadline stops the walk before a decode.
    #[test]
    fn the_deadline_stops_the_walk() {
        let me = TxKey::new(10, 0, 1);
        let all = [
            step(me, false, true),
            step(TxKey::new(12, 1, 1), true, true),
        ];
        let budget = XchainBudget::default();
        let decoded = Cell::new(0);
        let got = find_step(
            |cap| Ok(newest(&all, cap)),
            me,
            &budget,
            || Err(ApiError::Deadline),
            |k| {
                decoded.set(decoded.get() + 1);
                Ok(Some(k))
            },
        );
        assert!(matches!(got, Err(ApiError::Deadline)));
        assert_eq!(decoded.get(), 0);
    }
}
