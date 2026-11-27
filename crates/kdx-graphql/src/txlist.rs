//! Tx-keyed lists (Task 4): transactions, events and transfers.
//!
//! - A list is a [`ListSpec`]: a key source (one posting list, a request key, a block, or the
//!   heights walked down from the tip), key-level filters (chain, heights, `minimumDepth`)
//!   and a row rule that turns each tx into zero or more items.
//! - List order is TxKey descending (height, then chain, then position in the block) and,
//!   inside one tx, descending by event position (the oracle's order, R-P5P9-19: order
//!   index 1 before 0). `transactions(isCoinbase: true)` puts each block's coinbase before
//!   the block's user txs, as stock orders it ([`coinbase_first`]). Cursors are
//!   `kdx_api::Cursor` (TxKey +
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
//! - `first`/`after` walks newest first. `last`/`before` on a plain posting list (an account,
//!   an account's transfers of one module, an event name, a public key, a sender) reads the
//!   postings oldest first from the cursor (or the oldest row) and stops at the page, so
//!   `last: N` costs N rows plus the rows the filters drop. Other sources (a sender
//!   intersection, `isCoinbase: true`, the height walk) first collect the keys between the
//!   cursor and the newest end (keys only, at most [`BACK_KEYS`], else `LIMIT`), then load
//!   from the cursor outward. Both check the request deadline every 64 keys.
//! - `totalCount` (GQ-10) is exact, as stock's: a count walk of every item, run only when the
//!   field is selected, bounded only by the request deadline. A walk the deadline cuts answers
//!   a `COUNT_TIMEOUT` error at `totalCount`, never a partial number ([`count`]).

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

/// Keys a backward page of a source with no oldest-first walk ([`oldest_keys`]) may collect
/// between its cursor and the newest end (`GraphqlSettings::back_keys`).
pub const BACK_KEYS: usize = 200_000;
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
#[derive(Debug, Clone)]
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
    /// Txs the account sent (`meta.sender`): its `sndr` postings when the view is
    /// sender-ready (FIX-2), else its `acct` postings (the rows keep the sender filter).
    Sndr(String),
    /// Txs the account sent with a transfer of `module` on its side: the intersection of its
    /// `sndr` and `xfer` postings when the view is sender-ready, else the `xfer` postings.
    SndrXfer { account: String, module: String },
    /// Txs emitting a qualified event (`evt` postings of the event name).
    EvtName(String),
    /// Txs signed by a public key (`pk` postings).
    Pk(String),
    /// `acct` postings of the account when the dictionary has it, else the `evt` postings
    /// of the event name (an `events.parametersFilter` whose first value is a string).
    AcctOrEvt { account: String, qual: String },
    /// Every stored copy of a request key; with `block`, only the copy in that block (stock
    /// ANDs `requestKey` and `blockHash`; audit F-30). `rk` is `None` for a key that is no
    /// hash: no copies ([`rk_arg`]).
    Rk {
        rk: Option<Hash32>,
        block: Option<Hash32>,
    },
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

/// `events.parametersFilter` (GQ-11). Stock documents a Prisma JSON filter here and then
/// never reads the argument: every value, malformed included, answers the unfiltered events.
/// We serve the Prisma JSON filter forms on the params array (each a row predicate, so the
/// list costs what the unfiltered list costs), ANDed when several keys are given:
///
/// - `array_starts_with`, `array_ends_with`: a value, or an array of values in order;
/// - `array_contains`: a value, or an array whose every value is a member;
/// - `equals`, `not`: the whole (or `path`ed) value equal / not equal;
/// - `string_contains`, `string_starts_with`, `string_ends_with`: a string target;
/// - `path`: an array of keys or indexes (`["0"]` is the first parameter) the other keys
///   apply to.
///
/// Any other value (unknown key, wrong shape, not JSON, over 8 KB) is ignored as stock
/// ignores it: the unfiltered events, with `extensions.parametersFilterIgnored: true`.
#[derive(Debug, Clone, PartialEq)]
pub struct ParamFilter {
    path: Vec<String>,
    preds: Vec<ParamPred>,
}

#[derive(Debug, Clone, PartialEq)]
enum ParamPred {
    StartsWith(Vec<Value>),
    EndsWith(Vec<Value>),
    Contains(Vec<Value>),
    Equals(Value),
    Not(Value),
    StrContains(String),
    StrStarts(String),
    StrEnds(String),
}

/// The values of an `array_*` operand: an array as its values (at most 16, at least one),
/// any other value as itself.
fn operand(v: &Value) -> Option<Vec<Value>> {
    match v {
        Value::Array(a) if !a.is_empty() && a.len() <= 16 => Some(a.clone()),
        Value::Array(_) => None,
        x => Some(vec![x.clone()]),
    }
}

impl ParamFilter {
    /// The filter `s` serves, or `None` when stock's answer (no filter) applies.
    pub fn parse(s: &str) -> Option<ParamFilter> {
        if s.len() > 8 * 1024 {
            return None;
        }
        let v: Value = serde_json::from_str(s).ok()?;
        let o = v.as_object()?;
        let mut f = ParamFilter {
            path: Vec::new(),
            preds: Vec::new(),
        };
        for (k, x) in o {
            let text = || x.as_str().map(str::to_owned);
            let p = match k.as_str() {
                "path" => {
                    let a = x.as_array().filter(|a| a.len() <= 16)?;
                    f.path = a
                        .iter()
                        .map(|e| match e {
                            Value::String(s) => Some(s.clone()),
                            Value::Number(n) => n.as_u64().map(|n| n.to_string()),
                            _ => None,
                        })
                        .collect::<Option<_>>()?;
                    continue;
                }
                "array_starts_with" => ParamPred::StartsWith(operand(x)?),
                "array_ends_with" => ParamPred::EndsWith(operand(x)?),
                "array_contains" => ParamPred::Contains(operand(x)?),
                "equals" => ParamPred::Equals(x.clone()),
                "not" => ParamPred::Not(x.clone()),
                "string_contains" => ParamPred::StrContains(text()?),
                "string_starts_with" => ParamPred::StrStarts(text()?),
                "string_ends_with" => ParamPred::StrEnds(text()?),
                _ => return None,
            };
            f.preds.push(p);
        }
        (!f.preds.is_empty()).then_some(f)
    }

    /// [`ParamFilter::parse`] of an optional argument, marking the request when the argument
    /// is ignored (`parametersFilterIgnored`).
    pub fn arg(ctx: &async_graphql::Context<'_>, s: Option<&str>) -> Option<ParamFilter> {
        let s = s?;
        let f = ParamFilter::parse(s);
        if f.is_none() {
            crate::flags::mark(ctx, crate::flags::Flag::ParametersFilterIgnored);
        }
        f
    }

    pub fn matches(&self, params: &Value) -> bool {
        let mut t = params;
        for k in &self.path {
            let next = match t {
                Value::Array(a) => k.parse::<usize>().ok().and_then(|i| a.get(i)),
                Value::Object(o) => o.get(k),
                _ => None,
            };
            match next {
                Some(n) => t = n,
                None => return false,
            }
        }
        self.preds.iter().all(|p| match p {
            ParamPred::StartsWith(x) => t
                .as_array()
                .is_some_and(|a| a.len() >= x.len() && x.iter().zip(a).all(|(x, y)| x == y)),
            ParamPred::EndsWith(x) => t.as_array().is_some_and(|a| {
                a.len() >= x.len() && x.iter().rev().zip(a.iter().rev()).all(|(x, y)| x == y)
            }),
            ParamPred::Contains(x) => t
                .as_array()
                .is_some_and(|a| x.iter().all(|x| a.contains(x))),
            ParamPred::Equals(x) => t == x,
            ParamPred::Not(x) => t != x,
            ParamPred::StrContains(x) => t.as_str().is_some_and(|s| s.contains(x.as_str())),
            ParamPred::StrStarts(x) => t.as_str().is_some_and(|s| s.starts_with(x.as_str())),
            ParamPred::StrEnds(x) => t.as_str().is_some_and(|s| s.ends_with(x.as_str())),
        })
    }

    /// The first parameter every match has when it is a string (an account reference
    /// candidate): `array_starts_with` at the root, or `equals` on `path: ["0"]`.
    pub fn first_str(&self) -> Option<&str> {
        self.preds
            .iter()
            .find_map(|p| match (p, self.path.as_slice()) {
                (ParamPred::StartsWith(x), []) => x.first().and_then(Value::as_str),
                (ParamPred::Equals(x), [z]) if z == "0" => x.as_str(),
                _ => None,
            })
    }
}

/// How one tx turns into list items.
#[derive(Debug, Clone)]
pub enum Rows {
    /// The tx itself; `sender` keeps only txs whose `meta.sender` is it; `nft` keeps only txs
    /// with an event of an NFT ledger; `module` keeps only txs with an event of that module
    /// (stock's `fungibleName` rule beside `requestKey` or `blockHash`; audit F-30).
    Txs {
        sender: Option<String>,
        nft: bool,
        module: Option<String>,
        /// Keep coinbase txs too (`transactions(isCoinbase: true)`, as stock: its
        /// `t.sender != 'coinbase'` condition is dropped, nothing else changes).
        coinbase: bool,
    },
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
                nft: false,
                module: None,
                coinbase: _,
            }
        )
    }

    pub fn rows(&self, d: &Arc<TxData>) -> Vec<(u16, Item)> {
        match self {
            Rows::Txs {
                sender,
                nft,
                module,
                coinbase,
            } => {
                if d.is_coinbase() && !coinbase {
                    return vec![];
                }
                if sender.as_deref().is_some_and(|s| d.sender() != Some(s)) {
                    return vec![];
                }
                if *nft && !d.evs.iter().any(|e| kdx_hot::nft::is_ledger(&e.module)) {
                    return vec![];
                }
                if module
                    .as_deref()
                    .is_some_and(|m| !d.evs.iter().any(|e| e.module == m))
                {
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
    /// Keys in both posting lists (a merge-intersection of two newest-first walks).
    And((Family, u64), (Family, u64)),
    /// The copies of a request key, only the one at `(chain, height)` when given.
    Rk(Hash32, Option<(u8, u32)>),
    Block(u8, u32),
    Heights,
    Empty,
}

/// The key source and row rule of `spec` in `view`. A sender source on a sender-ready view
/// reads the `sndr` postings, which hold exactly the txs a sender filter keeps, so its rows
/// drop that filter (a count is then a key-only walk). Otherwise it is the old path: the
/// account's `acct` (or `xfer`) postings with the rows' sender filter.
fn plan(view: &View, spec: &ListSpec) -> ApiResult<(Src, Rows)> {
    let sender = matches!(
        spec.source,
        SourceSpec::Sndr(_) | SourceSpec::SndrXfer { .. }
    );
    if sender && view.sender_ready()? {
        let rows = match &spec.rows {
            Rows::Txs {
                sender: _,
                nft,
                module,
                coinbase,
            } => Rows::Txs {
                sender: None,
                nft: *nft,
                module: module.clone(),
                coinbase: *coinbase,
            },
            r => r.clone(),
        };
        return Ok((resolve_sndr(view, &spec.source), rows));
    }
    Ok((resolve(view, &spec.source)?, spec.rows.clone()))
}

/// The `sndr` source of a sender spec (the view is sender-ready).
fn resolve_sndr(view: &View, s: &SourceSpec) -> Src {
    let d = view.dict();
    match s {
        SourceSpec::Sndr(a) => match d.find(DictKind::Account, a) {
            Some(id) => Src::Post(Family::Sndr, u64::from(id)),
            None => Src::Empty,
        },
        SourceSpec::SndrXfer { account, module } => match (
            d.find(DictKind::Account, account),
            d.find(DictKind::Module, module),
        ) {
            (Some(a), Some(m)) => Src::And(
                (Family::Sndr, u64::from(a)),
                (Family::Xfer, xfer_entity(a, m)),
            ),
            _ => Src::Empty,
        },
        _ => Src::Empty,
    }
}

fn resolve(view: &View, s: &SourceSpec) -> ApiResult<Src> {
    let d = view.dict();
    Ok(match s {
        SourceSpec::Acct(a) | SourceSpec::Sndr(a) => match d.find(DictKind::Account, a) {
            Some(id) => Src::Post(Family::Acct, u64::from(id)),
            None => Src::Empty,
        },
        SourceSpec::Xfer { account, module } | SourceSpec::SndrXfer { account, module } => {
            match (
                d.find(DictKind::Account, account),
                d.find(DictKind::Module, module),
            ) {
                (Some(a), Some(m)) => Src::Post(Family::Xfer, xfer_entity(a, m)),
                _ => Src::Empty,
            }
        }
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
        SourceSpec::Rk { rk: None, .. } => Src::Empty,
        SourceSpec::Rk {
            rk: Some(rk),
            block: None,
        } => Src::Rk(*rk, None),
        // The store keeps the canonical chain only: a hash names one (chain, height).
        SourceSpec::Rk {
            rk: Some(rk),
            block: Some(h),
        } => match view.block_by_hash(h)? {
            Some((c, height, _)) => Src::Rk(*rk, Some((c, height))),
            None => Src::Empty,
        },
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
    /// When a walk that skips keys internally (the `sndr` ∩ `xfer` intersection) must give
    /// up with [`ApiError::Deadline`]: the page's scan window or the count's budget.
    until: Option<Instant>,
    /// The coinbase is the first row of its block ([`coinbase_first`]): keys compare by
    /// [`rank`] and the walk yields each block's coinbase before its user txs.
    cb_first: bool,
}

/// Whether `spec`'s newest-first walk puts each block's coinbase before its user txs:
/// `transactions(isCoinbase: true)`, as stock orders by creation time and the coinbase has the
/// block's (controller ruling on PR #30). A list of one block is already in node order, the
/// coinbase first ([`run_page`]), and lists without the flag have no coinbase rows, so both keep
/// the TxKey order and their cursors.
fn coinbase_first(spec: &ListSpec) -> bool {
    matches!(spec.rows, Rows::Txs { coinbase: true, .. })
        && !matches!(spec.source, SourceSpec::Block(_))
}

/// Position a coinbase row takes in a [`coinbase_first`] walk: above every user tx of its block
/// (no block holds `u16::MAX` txs). Its page cursor carries this key, so `after` and `before`
/// compare it in the walk's order; the cursor's block fingerprint reads only height and chain.
/// User txs keep their own key, so their cursors are the ones the list without the flag gives.
fn rank(k: TxKey, cb_first: bool) -> TxKey {
    if cb_first && k.is_coinbase() {
        TxKey::new(k.height(), k.chain(), u16::MAX)
    } else {
        k
    }
}

/// A newest-first key walk with each block's coinbase moved before the block's user txs. The
/// source yields a block's keys together and its coinbase (position 0) last, so this holds back
/// at most one block's user keys. An error passes through after the keys held back.
struct CoinbaseFirst<'v> {
    inner: KeySource<'v>,
    held: Vec<(TxKey, Option<TxRecord>)>,
    out: std::collections::VecDeque<ApiResult<(TxKey, Option<TxRecord>)>>,
}

impl Iterator for CoinbaseFirst<'_> {
    type Item = ApiResult<(TxKey, Option<TxRecord>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = |k: TxKey| (k.height(), k.chain());
        while self.out.is_empty() {
            match self.inner.next() {
                None => {
                    self.out.extend(self.held.drain(..).map(Ok));
                    break;
                }
                Some(Err(e)) => {
                    self.out.extend(self.held.drain(..).map(Ok));
                    self.out.push_back(Err(e));
                }
                Some(Ok((k, r))) => {
                    if self
                        .held
                        .first()
                        .is_some_and(|(h, _)| block(*h) != block(k))
                    {
                        self.out.extend(self.held.drain(..).map(Ok));
                    }
                    if k.is_coinbase() {
                        self.out.push_back(Ok((k, r)));
                        self.out.extend(self.held.drain(..).map(Ok));
                    } else {
                        self.held.push((k, r));
                    }
                }
            }
        }
        self.out.pop_front()
    }
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

/// The height bounds of a walk (`f`'s, the walk's and the depth caps') and the key-level
/// filter every walk applies: [`keys`] newest first and [`oldest_keys`] oldest first.
fn key_rule<'v>(
    f: KeyFilter,
    caps: &'v Caps,
    b: Bounds,
) -> (Option<u32>, Option<u32>, impl Fn(TxKey) -> bool + Copy + 'v) {
    let min_h = opt_max(f.min_h, b.min_h);
    let max_h = opt_min(f.max_h, caps.max());
    let keep = move |k: TxKey| {
        (!f.user_only || !k.is_coinbase())
            && f.chain.is_none_or(|c| k.chain() == c)
            && min_h.is_none_or(|m| k.height() >= m)
            && max_h.is_none_or(|m| k.height() <= m)
            && b.upto.is_none_or(|u| rank(k, b.cb_first) <= u)
            && caps.allows(k)
    };
    (min_h, max_h, keep)
}

/// Candidate keys of a plain posting source, **oldest first**, from `from` (inclusive) on,
/// with the same key-level filters as [`keys`]: the keys [`keys`] lists at or above `from`,
/// in reverse. `None` for a source with no oldest-first walk: a sender intersection, a
/// [`coinbase_first`] walk (its order is not TxKey order) and the listed or height sources,
/// which keep the newest-first collection.
fn oldest_keys<'v>(
    view: &'v View,
    src: &Src,
    f: KeyFilter,
    caps: &'v Caps,
    from: Option<TxKey>,
    cb_first: bool,
) -> ApiResult<Option<Box<dyn Iterator<Item = ApiResult<TxKey>> + 'v>>> {
    let Src::Post(fam, entity) = *src else {
        return Ok(None);
    };
    if cb_first {
        return Ok(None);
    }
    let b = Bounds {
        min_h: from.map(|k| k.height()),
        ..Bounds::default()
    };
    let (min_h, max_h, keep) = key_rule(f, caps, b);
    let walk = view
        .postings_oldest(fam, entity, min_h.unwrap_or(0))?
        // Ascending: past `max_h` no key can be kept.
        .take_while(move |r| {
            r.as_ref()
                .map_or(true, |k| max_h.is_none_or(|m| k.height() <= m))
        })
        .filter(move |r| {
            r.as_ref()
                .map_or(true, |k| from.is_none_or(|c| *k >= c) && keep(*k))
        });
    Ok(Some(Box::new(walk)))
}

/// Candidate keys, newest first, with every key-level filter applied.
fn keys<'v>(
    view: &'v View,
    src: &Src,
    f: KeyFilter,
    caps: &'v Caps,
    b: Bounds,
) -> ApiResult<KeySource<'v>> {
    let (min_h, max_h, keep) = key_rule(f, caps, b);
    let listed = |mut v: Vec<(TxKey, TxRecord)>| -> KeySource<'v> {
        v.retain(|(k, _)| keep(*k));
        v.sort_by_key(|(k, _)| std::cmp::Reverse(*k));
        Box::new(v.into_iter().map(|(k, r)| Ok((k, Some(r)))))
    };
    let walk: KeySource<'v> = match src {
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
        Src::And((fa, ea), (fb, eb)) => {
            let q = PostingQuery {
                min_height: min_h,
                max_height: max_h,
                chain: f.chain,
                before: b.upto.map(through_tx),
            };
            Box::new(
                IntersectDesc::with_until(
                    view.postings(*fa, *ea, q)?,
                    view.postings(*fb, *eb, q)?,
                    b.until,
                )
                .filter(move |r| r.as_ref().map_or(true, |k| keep(*k)))
                .map(|r| r.map(|k| (k, None))),
            )
        }
        Src::Rk(rk, at) => listed(
            view.txs_by_rk(rk)?
                .into_iter()
                .filter(|(k, _)| at.is_none_or(|(c, h)| k.chain() == c && k.height() == h))
                .collect(),
        ),
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
    };
    Ok(if b.cb_first {
        Box::new(CoinbaseFirst {
            inner: walk,
            held: Vec::new(),
            out: Default::default(),
        })
    } else {
        walk
    })
}

/// Input keys the intersection walk reads between two looks at the clock.
const INTERSECT_CHECK: usize = 1024;

/// The keys two newest-first (strictly descending) key streams share, newest first. The
/// first error ends it. With `until`, the walk reads the clock every [`INTERSECT_CHECK`]
/// input keys and ends with [`ApiError::Deadline`] once it has passed: a long `xfer` list
/// with few sends is skipped inside one `next()`, so the caller's per-output-key checks
/// cannot bound it.
struct IntersectDesc<A, B> {
    a: A,
    b: B,
    done: bool,
    until: Option<Instant>,
    steps: usize,
}

impl<A, B> IntersectDesc<A, B> {
    #[cfg(test)]
    fn new(a: A, b: B) -> Self {
        Self::with_until(a, b, None)
    }
    fn with_until(a: A, b: B, until: Option<Instant>) -> Self {
        IntersectDesc {
            a,
            b,
            done: false,
            until,
            steps: 0,
        }
    }
}

impl<A, B> Iterator for IntersectDesc<A, B>
where
    A: Iterator<Item = ApiResult<TxKey>>,
    B: Iterator<Item = ApiResult<TxKey>>,
{
    type Item = ApiResult<TxKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let r = intersect_next(&mut self.a, &mut self.b, self.until, &mut self.steps);
        if !matches!(r, Some(Ok(_))) {
            self.done = true;
        }
        r
    }
}

/// The next key both streams hold (descending), an error, or `None` when one is exhausted.
fn intersect_next(
    a: &mut impl Iterator<Item = ApiResult<TxKey>>,
    b: &mut impl Iterator<Item = ApiResult<TxKey>>,
    until: Option<Instant>,
    steps: &mut usize,
) -> Option<ApiResult<TxKey>> {
    let mut tick = || {
        *steps += 1;
        (*steps).is_multiple_of(INTERSECT_CHECK) && until.is_some_and(|u| Instant::now() >= u)
    };
    if tick() {
        return Some(Err(ApiError::Deadline));
    }
    let mut x = match a.next()? {
        Ok(k) => k,
        Err(e) => return Some(Err(e)),
    };
    let mut y = match b.next()? {
        Ok(k) => k,
        Err(e) => return Some(Err(e)),
    };
    loop {
        // Descending: the larger key cannot be in the other stream any more.
        let (it, cur): (&mut dyn Iterator<Item = ApiResult<TxKey>>, &mut TxKey) = match x.cmp(&y) {
            std::cmp::Ordering::Equal => return Some(Ok(x)),
            std::cmp::Ordering::Greater => (a, &mut x),
            std::cmp::Ordering::Less => (b, &mut y),
        };
        if tick() {
            return Some(Err(ApiError::Deadline));
        }
        match it.next()? {
            Ok(k) => *cur = k,
            Err(e) => return Some(Err(e)),
        }
    }
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
///
/// A list of one block (`Block.events`, `Block.transactions`, a root list with `blockHash`)
/// is in the node's order (FIX-4 item 6): the coinbase first (the first leaf of the
/// payload's `outputsHash`, executed before the user txs), then the user txs in payload
/// order, each tx's rows by ascending position. That is the newest-first walk read from
/// the other end: a forward page is the backward page of the walk, reversed, with
/// `hasNextPage` and `hasPreviousPage` swapped (and the other way round). Cursors are the
/// same tokens. Every other list stays newest first ([`coinbase_first`] for the coinbase).
pub fn run_page(
    view: &View,
    spec: &ListSpec,
    page: &Page<PageCursor>,
    deadline: Instant,
    back_keys: usize,
) -> ApiResult<(Vec<(String, Item)>, PageInfo)> {
    if !matches!(spec.source, SourceSpec::Block(_)) {
        return run_page_desc(view, spec, page, deadline, back_keys);
    }
    let flipped = Page {
        dir: match page.dir {
            Dir::Forward => Dir::Backward,
            Dir::Backward => Dir::Forward,
        },
        ..page.clone()
    };
    let (mut edges, info) = run_page_desc(view, spec, &flipped, deadline, back_keys)?;
    edges.reverse();
    Ok((
        edges,
        PageInfo {
            start_cursor: info.end_cursor,
            end_cursor: info.start_cursor,
            has_next_page: info.has_previous_page,
            has_previous_page: info.has_next_page,
        },
    ))
}

/// One page of `spec` in the newest-first walk.
fn run_page_desc(
    view: &View,
    spec: &ListSpec,
    page: &Page<PageCursor>,
    deadline: Instant,
    back_keys: usize,
) -> ApiResult<(Vec<(String, Item)>, PageInfo)> {
    let cur = tx_cursor(view, page)?;
    let cb_first = coinbase_first(spec);
    let cursor = |d: &TxData, sub: u16| {
        let mut c = d.cursor(sub);
        c.key = rank(c.key, cb_first);
        c.encode()
    };
    let (src, rows) = plan(view, spec)?;
    let caps = Caps::of(view, &spec.filter)?;
    let mut blocks = Blocks::default();
    let mut walked: Vec<(String, Item)> = Vec::new();
    match page.dir {
        Dir::Forward => {
            let b = Bounds {
                upto: cur.map(|c| c.key),
                min_h: None,
                until: Some(deadline),
                cb_first,
            };
            'k: for (i, kr) in keys(view, &src, spec.filter, &caps, b)?.enumerate() {
                past(deadline, i)?;
                let (k, rec) = kr?;
                let Some(d) = load(view, k, rec, &mut blocks)? else {
                    continue;
                };
                let mut rs = rows.rows(&d);
                rs.reverse();
                for (sub, it) in rs {
                    if cur.is_some_and(|c| c.key == rank(k, cb_first) && sub >= c.sub) {
                        continue;
                    }
                    walked.push((cursor(&d, sub), it));
                    if walked.len() > page.n {
                        break 'k;
                    }
                }
            }
        }
        Dir::Backward => {
            // A plain posting list reads from the cursor (or its oldest row) towards the
            // newest end and stops at the page: `last: N` costs N rows plus the rows the
            // filters drop, however far the newest row is (gql.transfers:1000551).
            if let Some(asc) =
                oldest_keys(view, &src, spec.filter, &caps, cur.map(|c| c.key), cb_first)?
            {
                'a: for (i, kr) in asc.enumerate() {
                    past(deadline, i)?;
                    let k = kr?;
                    let Some(d) = load(view, k, None, &mut blocks)? else {
                        continue;
                    };
                    for (sub, it) in rows.rows(&d) {
                        if cur.is_some_and(|c| c.key == k && sub <= c.sub) {
                            continue;
                        }
                        walked.push((cursor(&d, sub), it));
                        if walked.len() > page.n {
                            break 'a;
                        }
                    }
                }
                let (edges, info) = finish(walked, page, |e| e.0.clone());
                return Ok((edges.into_iter().map(|(_, e)| e).collect(), info));
            }
            // Other sources collect the keys between the cursor and the newest end first.
            let b = Bounds {
                upto: None,
                min_h: cur.map(|c| c.key.height()),
                until: Some(deadline),
                cb_first,
            };
            let mut ks = Vec::new();
            for (i, kr) in keys(view, &src, spec.filter, &caps, b)?.enumerate() {
                past(deadline, i)?;
                let (k, _) = kr?;
                if cur.is_some_and(|c| rank(k, cb_first) < c.key) {
                    break;
                }
                ks.push(k);
                if ks.len() > back_keys {
                    return Err(ApiError::Unavailable(format!(
                        "last/before reaches more than {back_keys} txs: page with first/after"
                    )));
                }
            }
            'b: for (i, k) in ks.into_iter().rev().enumerate() {
                past(deadline, i)?;
                let Some(d) = load(view, k, None, &mut blocks)? else {
                    continue;
                };
                for (sub, it) in rows.rows(&d) {
                    if cur.is_some_and(|c| c.key == rank(k, cb_first) && sub <= c.sub) {
                        continue;
                    }
                    walked.push((cursor(&d, sub), it));
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

/// Items of `spec`, exactly: every item the full walk lists. A walk still running at `until`
/// (the request deadline) ends with [`ApiError::Deadline`], never with a partial count.
pub fn count(view: &View, spec: &ListSpec, until: Instant) -> ApiResult<u64> {
    let (src, rows) = plan(view, spec)?;
    if let Some(n) = stored_count(view, spec, &src, &rows)? {
        return Ok(n);
    }
    let caps = Caps::of(view, &spec.filter)?;
    let mut blocks = Blocks::default();
    let decode = rows.needs_record();
    let mut n = 0u64;
    // The key walk reads the clock every `CHECK_EVERY` keys; a decoding walk also reads it
    // before each record it decodes.
    let every = CHECK_EVERY;
    let b = Bounds {
        until: Some(until),
        ..Bounds::default()
    };
    // A decoding walk reads its frozen records ahead in batches of `COUNT_PREFETCH` keys:
    // the cold record reads overlap instead of faulting in one by one.
    let mut batch: Vec<(TxKey, Option<TxRecord>)> = Vec::new();
    let flush =
        |batch: &mut Vec<(TxKey, Option<TxRecord>)>, blocks: &mut Blocks| -> ApiResult<u64> {
            view.prefetch(batch.iter().filter(|(_, r)| r.is_none()).map(|(k, _)| *k))?;
            let mut m = 0u64;
            for (k, rec) in batch.drain(..) {
                if Instant::now() >= until {
                    return Err(ApiError::Deadline);
                }
                if let Some(d) = load(view, k, rec, blocks)? {
                    m += rows.rows(&d).len() as u64;
                }
            }
            Ok(m)
        };
    for (i, kr) in keys(view, &src, spec.filter, &caps, b)?.enumerate() {
        if i % every == every - 1 && Instant::now() >= until {
            return Err(ApiError::Deadline);
        }
        let (k, rec) = kr?;
        if decode {
            batch.push((k, rec));
            if batch.len() == COUNT_PREFETCH {
                n += flush(&mut batch, &mut blocks)?;
            }
        } else {
            n += 1;
        }
    }
    n += flush(&mut batch, &mut blocks)?;
    Ok(n)
}

/// The count of `spec` from the stored per-key row counts (PR #48 part 2), or `None` when
/// they cannot answer it: the counts are not ready, or the list has a filter they do not
/// hold. They hold rows per (list kind, key, chain) split into user and coinbase rows, so
/// they answer the plain lists below with no filter, a `chainId`, or `isCoinbase`. Heights,
/// `minimumDepth`, a sender filter on a view that is not sender-ready, `fungibleName` on an
/// account's transactions, NFT transfers and event `parametersFilter` / `orderIndex` /
/// `moduleName` keep the walk.
fn stored_count(view: &View, spec: &ListSpec, src: &Src, rows: &Rows) -> ApiResult<Option<u64>> {
    use kdx_decode::rowcount::CountKind as K;
    let f = &spec.filter;
    if f.min_h.is_some() || f.max_h.is_some() || f.depth.is_some() {
        return Ok(None);
    }
    let &Src::Post(fam, e) = src else {
        return Ok(None);
    };
    let d = view.dict();
    let evt = |q: &str| d.find(DictKind::EventName, q).map(u64::from);
    let acct = |a: &str| d.find(DictKind::Account, a);
    let plain_txs = |r: &Rows| {
        matches!(
            r,
            Rows::Txs {
                sender: None,
                nft: false,
                module: None,
                ..
            }
        )
    };
    let kind = match (fam, rows) {
        (Family::Sndr, r) if plain_txs(r) => K::Sndr,
        (Family::Pk, r) if plain_txs(r) => K::Pk,
        (Family::Evt, r) if plain_txs(r) && e < 1 << 32 => K::EvtTxs,
        (
            Family::Acct,
            Rows::Transfers {
                module: None,
                account: Some(a),
                nft: false,
            },
        ) if acct(a).map(u64::from) == Some(e) => K::AcctXfer,
        (
            Family::Xfer,
            Rows::Transfers {
                module: Some(m),
                account: Some(a),
                nft: false,
            },
        ) if acct(a)
            .zip(d.find(DictKind::Module, m))
            .map(|(a, m)| xfer_entity(a, m))
            == Some(e) =>
        {
            K::Xfer
        }
        (
            Family::Evt,
            Rows::Transfers {
                module: Some(m),
                account: None,
                nft: false,
            },
        ) if evt(&format!("{m}.TRANSFER")) == Some(e) => K::TokenXfer,
        (
            Family::Evt,
            Rows::Events {
                qual: Some(q),
                module: None,
                order_index: None,
                params: None,
            },
        ) if evt(q) == Some(e) => K::EvtRows,
        _ => return Ok(None),
    };
    let Some((user, cb)) = view.row_count(kind, e, f.chain)? else {
        return Ok(None);
    };
    // Coinbase rows are listed unless the keys drop them (`user_only`) or a tx list keeps
    // only user txs (`isCoinbase` absent).
    let with_cb = !f.user_only
        && match rows {
            Rows::Txs { coinbase, .. } => *coinbase,
            _ => true,
        };
    Ok(Some(user.saturating_add(if with_cb { cb } else { 0 })))
}

/// Keys a decoding count walk reads ahead at once ([`count`]).
const COUNT_PREFETCH: usize = 256;

/// The error of a count the deadline cut (`COUNT_TIMEOUT`): `totalCount` is exact or absent.
pub fn count_error(e: ApiError) -> async_graphql::Error {
    match e {
        ApiError::Deadline => err(
            Code::CountTimeout,
            "totalCount could not be counted exactly before the request deadline",
        ),
        e => from_api(e),
    }
}

/// Steps of a pact one outgoing `crossChainTransfer` lookup lists at most, oldest first
/// (the native defpact cap). The walk stops at the step that succeeded, which a pact holds at
/// most once per step and which comes before its failed retries ("Already used"), so this
/// binds only on a pact with that many failed steps before the success (O2: the newest-first
/// walk it replaces missed the success behind more than 1,000 later retries).
const XCHAIN_STEPS_MAX: usize = 1_000;
/// Heights below the start tx an outgoing lookup still reads. A continuation's SPV proof is
/// checked on a later block than the start (the target chain must have the source block in
/// its past), so none should be needed; the margin only costs opening a few more posting
/// lists, never a record read (the pact's postings hold only its own txs).
const XCHAIN_HEIGHT_SLACK: u32 = 1_000;
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

/// The first succeeded continuation step of a pact, other than `me`, that `matches`.
/// `steps` lists the pact's steps oldest first, lazily. Every listed step is paid from
/// `budget` before it is read (at most [`XCHAIN_STEPS_MAX`] per lookup), and so is every
/// full decode; `check` (the request deadline) runs before each decode. Start and failed
/// steps are skipped on their stored flags, with no decode. A spent budget answers `None`.
fn find_step<T>(
    mut steps: impl Iterator<Item = ApiResult<PactStep>>,
    me: TxKey,
    budget: &XchainBudget,
    mut check: impl FnMut() -> ApiResult<()>,
    mut matches: impl FnMut(TxKey) -> ApiResult<Option<T>>,
) -> ApiResult<Option<T>> {
    for _ in 0..XCHAIN_STEPS_MAX {
        if budget.take(1) == 0 {
            return Ok(None);
        }
        let Some(st) = steps.next() else {
            budget.refund(1);
            return Ok(None);
        };
        let st = st?;
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
    Ok(None)
}

/// The continuation step of the pact `pact` (started by `me`) that `matches`, for the
/// outgoing half of a cross-chain transfer: the pact's steps from `me`'s height on, oldest
/// first, stopping at the first succeeded `cont` that matches. On a normal pact that is the
/// start and its one continuation: two listed steps and one decode.
fn outgoing_cont<T>(
    view: &View,
    pact: &Hash32,
    me: TxKey,
    budget: &XchainBudget,
    check: impl FnMut() -> ApiResult<()>,
    matches: impl FnMut(TxKey) -> ApiResult<Option<T>>,
) -> ApiResult<Option<T>> {
    let steps = view.pact_steps_oldest(pact, me.height().saturating_sub(XCHAIN_HEIGHT_SLACK))?;
    find_step(steps, me, budget, check, matches)
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
        return outgoing_cont(
            view,
            &rk,
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
/// source (an account, a posting, a height walk, a request key, which a `blockHash` beside
/// it only filters), a `minimumDepth` filter (it reads the chain tips), and, for transfers,
/// a selected `crossChainTransfer` (it reads the pact's other chain).
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
    let back_keys = g.settings.back_keys;
    let dl = deadline(ctx);
    let xchain = matches!(spec.rows, Rows::Transfers { nft: false, .. })
        && selects(ctx, "crossChainTransfer");
    let budget = xchain_budget(ctx);
    // A `crossChainTransfer` lookup reads the pact's other chain: past one block.
    let one_block = one_block && !xchain;
    // The page is read only when its items or page info are selected.
    let reads = selects_direct(ctx, &["edges", "pageInfo"]);
    // A block's or a request key's rows are few: counted with the page, exactly.
    let bounded = matches!(spec.source, SourceSpec::Block(_) | SourceSpec::Rk { .. })
        && selects_direct(ctx, &["totalCount"]);
    let sp = spec.clone();
    let (edges, info, exact) = read_list(&state, one_block, move |view| {
        // The count runs to the request deadline; a cut one is an error, not a number (audit
        // F-50, GQ-10). It is carried out of the read so the page is still served.
        let exact = bounded.then(|| count(view, &sp, dl));
        if !reads {
            return Ok((Vec::new(), PageInfo::default(), exact));
        }
        let dl = dl.min(Instant::now() + SCAN_WINDOW);
        let (mut edges, info) = run_page(view, &sp, &page, dl, back_keys)?;
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
        let total = match n {
            Ok(n) => Total::Exact(n),
            Err(e) => Total::Failed(count_error(e)),
        };
        return Ok(ListPage { edges, info, total });
    }
    let total = Total::Deferred(Arc::new(move || {
        let state = state.clone();
        let spec = spec.clone();
        Box::pin(async move {
            read_list(&state, one_block, move |view| count(view, &spec, dl))
                .await
                .map_err(count_error)
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

/// A request key argument used as a lookup: `None` when it is no hash, which finds nothing.
/// Stock compares the text with its stored keys, so such a key (a base64-wrapped coinbase key
/// from its own lists, any other text) answers `null` or no rows, never an error (G4 class 2).
pub fn rk_arg(s: &str) -> Option<Hash32> {
    hash_arg("requestKey", s).ok()
}

/// A request key or block hash argument (43-char base64url).
pub fn hash_arg(name: &str, s: &str) -> async_graphql::Result<Hash32> {
    if s.len() > 64 {
        return Err(bad_input(format!("{name} is not a hash")));
    }
    Hash32::from_b64url(s).map_err(|_| bad_input(format!("{name} is not a hash")))
}

/// A `fungibleName` argument: trimmed first, as stock reads it (`"coin "` is `coin`, FIX-4
/// part B); a name of spaces alone is refused as an empty one.
pub fn fungible_arg(s: &str) -> async_graphql::Result<&str> {
    name_arg("fungibleName", s.trim())
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
        let f = ParamFilter::parse(r#"{"array_ends_with":1.5}"#).unwrap();
        assert_eq!(f.first_str(), None);
        assert!(f.matches(&json!(["k:a", "k:b", 1.5])));
        assert!(!f.matches(&json!([1.5, "k:a"])));
        let f = ParamFilter::parse(r#"{"array_contains":["k:b","k:a"]}"#).unwrap();
        assert!(f.matches(&json!(["k:a", "k:b", 1.5])));
        assert!(!f.matches(&json!(["k:a", 1.5])));
        let f = ParamFilter::parse(r#"{"path":["0"],"equals":"k:a"}"#).unwrap();
        assert_eq!(f.first_str(), Some("k:a"));
        assert!(f.matches(&json!(["k:a", "k:b"])));
        assert!(!f.matches(&json!(["k:b", "k:a"])));
        let f = ParamFilter::parse(r#"{"path":[1],"string_starts_with":"k:"}"#).unwrap();
        assert!(f.matches(&json!(["", "k:b", 1])));
        assert!(!f.matches(&json!(["k:a", "w:b", 1])));
        assert!(!f.matches(&json!(["k:a"])));
        let f = ParamFilter::parse(r#"{"array_starts_with":"k:a","array_ends_with":2}"#).unwrap();
        assert!(f.matches(&json!(["k:a", "k:b", 2])));
        assert!(!f.matches(&json!(["k:a", "k:b", 1])));
        let f = ParamFilter::parse(r#"{"not":[]}"#).unwrap();
        assert!(f.matches(&json!(["x"])) && !f.matches(&json!([])));
        // Stock ignores the argument: what we cannot serve is ignored too (no filter).
        for ignored in [
            r#"{"array_starts_with":"a","x":1}"#,
            r#"{"array_starts_with":[]}"#,
            r#"{"path":["0"]}"#,
            r#"{"string_contains":1}"#,
            r#"{}"#,
            "[1]",
            "not json",
        ] {
            assert_eq!(ParamFilter::parse(ignored), None, "{ignored}");
        }
        assert_eq!(
            ParamFilter::parse(&format!("\"{}\"", "a".repeat(9000))),
            None
        );
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
mod intersect_tests {
    use super::*;

    fn keys(v: &[u64]) -> Vec<ApiResult<TxKey>> {
        v.iter().map(|&k| Ok(TxKey(k))).collect()
    }

    fn run(a: Vec<ApiResult<TxKey>>, b: Vec<ApiResult<TxKey>>) -> Vec<Result<u64, String>> {
        IntersectDesc::new(a.into_iter(), b.into_iter())
            .map(|r| r.map(|k| k.0).map_err(|e| e.to_string()))
            .collect()
    }

    /// FIX-2: the `sndr` ∩ `xfer` walk keeps the shared keys, newest first, and stops at the
    /// end of either stream or at the first error.
    #[test]
    fn intersection_of_descending_streams() {
        let ok = |v: &[u64]| v.iter().map(|&k| Ok(k)).collect::<Vec<_>>();
        assert_eq!(run(keys(&[9, 7, 5, 3]), keys(&[8, 7, 3, 1])), ok(&[7, 3]));
        assert_eq!(run(keys(&[9, 7]), keys(&[6, 5])), ok(&[]));
        assert_eq!(run(keys(&[]), keys(&[6, 5])), ok(&[]));
        assert_eq!(run(keys(&[4, 3, 2]), keys(&[4, 3, 2])), ok(&[4, 3, 2]));
        let mut b = keys(&[9]);
        b.push(Err(ApiError::Deadline));
        b.extend(keys(&[5]));
        let r = run(keys(&[9, 5]), b);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], Ok(9));
        assert!(r[1].is_err());
    }

    /// FIX-2 fix 1: an account with a huge `xfer` list and few sends (a pool's account) must
    /// not walk the whole list inside one `next()`: the walk reads the clock every
    /// [`INTERSECT_CHECK`] input keys and ends with `Deadline` once `until` has passed.
    #[test]
    fn intersection_walk_stops_at_the_deadline() {
        use std::cell::Cell;
        use std::rc::Rc;
        let pulled = Rc::new(Cell::new(0u64));
        let p = pulled.clone();
        // 50 M transfer keys, none of them a send; one old send below them all.
        let xfer = (0..50_000_000u64).rev().map(move |k| {
            p.set(p.get() + 1);
            Ok(TxKey(k + 10))
        });
        let sndr = keys(&[5]).into_iter();
        let t0 = Instant::now();
        let mut it = IntersectDesc::with_until(sndr, xfer, Some(Instant::now()));
        assert!(matches!(it.next(), Some(Err(ApiError::Deadline))));
        assert!(it.next().is_none());
        assert!(
            pulled.get() <= 2 * INTERSECT_CHECK as u64,
            "pulled {}",
            pulled.get()
        );
        assert!(t0.elapsed() < std::time::Duration::from_secs(1));
        // With no deadline the same walk is exact.
        let a = (0..1_000u64).rev().map(|k| Ok(TxKey(k + 10)));
        let got: Vec<_> = IntersectDesc::with_until(keys(&[500, 5]).into_iter(), a, None)
            .map(|r| r.map(|k| k.0).map_err(|e| e.to_string()))
            .collect();
        assert_eq!(got, vec![Ok(500)]);
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

    /// `all` (oldest first) as `pact_steps_oldest` streams it, counting the steps read.
    fn stream<'a>(
        all: &'a [PactStep],
        read: &'a Cell<usize>,
    ) -> impl Iterator<Item = ApiResult<PactStep>> + 'a {
        all.iter().map(move |s| {
            read.set(read.get() + 1);
            Ok(*s)
        })
    }

    /// R-P5P9-57 and O2: the step that succeeded is found however many later retries of the
    /// continuation follow it, and the walk stops there; failed retries are never decoded.
    #[test]
    fn successful_step_before_many_retries_is_found() {
        let me = TxKey::new(3_115_490, 2, 1);
        let ok = TxKey::new(3_115_507, 1, 1);
        let mut all = vec![step(me, false, true), step(ok, true, true)];
        all.extend((0..1_200u32).map(|i| step(TxKey::new(3_115_509 + i, 1, 2), true, false)));
        let budget = XchainBudget::default();
        let read = Cell::new(0);
        let decoded = Cell::new(0);
        let got = find_step(
            stream(&all, &read),
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
        assert_eq!((read.get(), decoded.get()), (2, 1));
        // Charged: the 2 listed, one decode.
        assert_eq!(XCHAIN_BUDGET - budget.left(), 2 + 1);
    }

    /// The honest case: a short pact costs its steps and one decode; the start itself never
    /// matches, and an exhausted stream refunds its last grant.
    #[test]
    fn short_pact_costs_its_steps() {
        let me = TxKey::new(10, 0, 1);
        let ok = TxKey::new(12, 1, 1);
        let all = vec![step(me, false, true), step(ok, true, true)];
        let budget = XchainBudget::default();
        let read = Cell::new(0);
        let got = find_step(stream(&all, &read), me, &budget, || Ok(()), |k| Ok(Some(k)));
        assert_eq!(got.unwrap(), Some(ok));
        assert_eq!(XCHAIN_BUDGET - budget.left(), 2 + 1);
        let budget = XchainBudget::default();
        let only_me = [step(me, true, true)];
        let none = find_step(
            stream(&only_me, &read),
            me,
            &budget,
            || Ok(()),
            |k| Ok(Some(k)),
        );
        assert_eq!(none.unwrap(), None);
        assert_eq!(XCHAIN_BUDGET - budget.left(), 1);
    }

    /// A non-matching success does not stop the walk (a later step of a longer pact may
    /// carry the transfer); one lookup lists at most `XCHAIN_STEPS_MAX` steps.
    #[test]
    fn the_walk_is_bounded_per_lookup() {
        let me = TxKey::new(10, 0, 1);
        let all: Vec<PactStep> = (0..1_500u32)
            .map(|i| step(TxKey::new(11 + i, 1, 1), true, i == 3 || i == 1_200))
            .collect();
        let budget = XchainBudget::default();
        let read = Cell::new(0);
        let decoded = Cell::new(0);
        let got = find_step(
            stream(&all, &read),
            me,
            &budget,
            || Ok(()),
            |k| {
                decoded.set(decoded.get() + 1);
                Ok((k == all[1_200].key).then_some(k))
            },
        );
        assert_eq!(
            got.unwrap(),
            None,
            "the match lies past the per-lookup bound"
        );
        assert_eq!((read.get(), decoded.get()), (XCHAIN_STEPS_MAX, 1));
        assert_eq!(XCHAIN_BUDGET - budget.left(), XCHAIN_STEPS_MAX + 1);
    }

    /// The security finding on e7da3d0: a page of N edges whose pacts each carry many failed
    /// retries (and no success) spends at most the request budget in total, and reads no
    /// step it has not paid for; past it every lookup answers `None`, not an error.
    #[test]
    fn a_page_of_retry_heavy_pacts_stays_within_the_request_budget() {
        const N: usize = 100;
        let budget = XchainBudget::default();
        let read = Cell::new(0usize);
        let decoded = Cell::new(0usize);
        let mut answers = Vec::new();
        for e in 0..N as u32 {
            let me = TxKey::new(1_000 + e, 2, 1);
            let mut all = vec![step(me, false, true)];
            all.extend(
                (0..900u32).map(|i| step(TxKey::new(2_000 + i, 1, 1 + e as u16), true, false)),
            );
            let got = find_step(
                stream(&all, &read),
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
        assert_eq!(read.get(), XCHAIN_BUDGET, "read {}", read.get());
        assert_eq!(budget.left(), 0);
        // An honest pact after the budget is spent answers "no other half", reading nothing.
        let me = TxKey::new(5, 0, 1);
        let ok = TxKey::new(6, 1, 1);
        let all = [step(me, false, true), step(ok, true, true)];
        let got = find_step(stream(&all, &read), me, &budget, || Ok(()), |k| Ok(Some(k)));
        assert_eq!(got.unwrap(), None);
        assert_eq!(read.get(), XCHAIN_BUDGET);
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
        let read = Cell::new(0);
        let got = find_step(
            stream(&all, &read),
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

/// FIX-3 (O2): the outgoing half of a cross-chain transfer, read from a real store.
#[cfg(test)]
mod xchain_store_tests {
    use std::cell::Cell;

    use kdx_decode::index::pact_entity;
    use kdx_decode::{IndexedBlock, TxIndex};
    use kdx_types::record::BlockRecord;

    use super::*;
    use crate::testutil::{self, Fixture};

    /// The start tx of every pact here (chain 2); it is not stored, the walk skips it.
    fn start(n: u32) -> TxKey {
        TxKey::new(100 + n, 2, 1)
    }

    /// A synthetic pact id (never a real request key).
    fn pact(n: u8) -> Hash32 {
        Hash32([0xA0 + n; 32])
    }

    /// Block (chain 0, `height`): its coinbase, then one `cont` step of `p` per flag.
    fn block(height: u32, p: &Hash32, oks: &[bool]) -> IndexedBlock {
        let cmd = serde_json::json!({"payload": {"cont": {"pactId": p.to_b64url(), "step": 1}}});
        let coinbase = TxRecord {
            key: TxKey::new(height, 0, 0),
            status_ok: true,
            ..TxRecord::default()
        };
        let txs: Vec<TxRecord> = std::iter::once(coinbase)
            .chain(oks.iter().enumerate().map(|(i, &ok)| {
                let mut rk = [0u8; 32];
                rk[..4].copy_from_slice(&height.to_be_bytes());
                rk[4..6].copy_from_slice(&(i as u16).to_be_bytes());
                rk[6] = p.0[0];
                TxRecord {
                    key: TxKey::new(height, 0, i as u16 + 1),
                    request_key: Some(Hash32(rk)),
                    cmd: Some(serde_json::to_vec(&cmd).unwrap()),
                    status_ok: ok,
                    ..TxRecord::default()
                }
            }))
            .collect();
        let idx = txs
            .iter()
            .map(|r| TxIndex {
                key: r.key,
                postings: if r.key.is_coinbase() {
                    Vec::new()
                } else {
                    vec![(Family::Pact, pact_entity(p))]
                },
                pact_id: None,
                code_hash: None,
                d3: Default::default(),
                holders: Vec::new(),
            })
            .collect();
        let mut h = [0u8; 32];
        h[..4].copy_from_slice(&height.to_be_bytes());
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32(h),
                chain: 0,
                height: u64::from(height),
                ..BlockRecord::default()
            },
            txs,
            idx,
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    }

    /// Store `blocks` of pact `p` from `height` on (one block per entry), returning the next
    /// free height.
    fn put(f: &Fixture, mut height: u32, p: &Hash32, blocks: &[Vec<bool>]) -> u32 {
        for oks in blocks {
            f.apply_block(&block(height, p, oks));
            height += 1;
        }
        height
    }

    fn view(f: &Fixture) -> View {
        f.st.store
            .view(Instant::now() + std::time::Duration::from_secs(60))
            .unwrap()
    }

    /// The lookup with a matcher that accepts every step it is asked about, counting decodes.
    fn lookup(
        v: &View,
        n: u8,
        budget: &XchainBudget,
        decoded: &Cell<usize>,
    ) -> ApiResult<Option<TxKey>> {
        outgoing_cont(
            v,
            &pact(n),
            start(u32::from(n)),
            budget,
            || v.check_deadline(),
            |k| {
                decoded.set(decoded.get() + 1);
                Ok(Some(k))
            },
        )
    }

    /// O2: one successful continuation, then 1,200 failed retries (more than the old
    /// 1,000-step newest-first cap): the link is found, and cheaply.
    #[test]
    fn success_then_1200_failed_retries_is_found() {
        let f = testutil::store();
        let p = pact(1);
        let mut h = put(&f, 200, &p, &[vec![true]]);
        h = put(&f, h, &p, &vec![vec![false; 100]; 12]);
        assert_eq!(h, 213);
        let v = view(&f);
        let budget = XchainBudget::default();
        let decoded = Cell::new(0);
        let got = lookup(&v, 1, &budget, &decoded).unwrap();
        assert_eq!(got, Some(TxKey::new(200, 0, 1)));
        assert_eq!(decoded.get(), 1);
        let spent = XCHAIN_BUDGET - budget.left();
        assert!(spent < 20, "spent {spent} reads");
    }

    /// The success is not the first step: failed retries before and after it.
    #[test]
    fn success_between_failed_retries_is_found() {
        let f = testutil::store();
        let p = pact(2);
        let mut h = put(&f, 300, &p, &[vec![false, false, false]]);
        h = put(&f, h, &p, &[vec![false, true, false]]);
        put(&f, h, &p, &vec![vec![false; 100]; 11]);
        let v = view(&f);
        let budget = XchainBudget::default();
        let decoded = Cell::new(0);
        let got = lookup(&v, 2, &budget, &decoded).unwrap();
        assert_eq!(got, Some(TxKey::new(301, 0, 2)));
        assert_eq!(decoded.get(), 1, "failed steps are never decoded");
        assert!(XCHAIN_BUDGET - budget.left() < 20);
    }

    /// No step succeeded: no other half, and no error.
    #[test]
    fn no_success_is_none() {
        let f = testutil::store();
        let p = pact(3);
        put(&f, 400, &p, &[vec![false; 5], vec![false; 3]]);
        let v = view(&f);
        let budget = XchainBudget::default();
        let decoded = Cell::new(0);
        assert_eq!(lookup(&v, 3, &budget, &decoded).unwrap(), None);
        assert_eq!(decoded.get(), 0);
        // A pact with no stored step at all.
        assert_eq!(lookup(&v, 9, &budget, &decoded).unwrap(), None);
    }

    /// A full page of lookups whose pacts hold only failed retries spends at most the
    /// request budget, then every lookup (an honest one too) answers `None`, not an error.
    #[test]
    fn a_page_exhausting_the_budget_degrades_to_none() {
        let f = testutil::store();
        let mut h = 500;
        for n in 1..=3u8 {
            h = put(&f, h, &pact(n), &vec![vec![false; 100]; 9]);
        }
        put(&f, h, &pact(4), &[vec![true]]);
        let v = view(&f);
        let budget = XchainBudget::default();
        let decoded = Cell::new(0);
        for n in 1..=3u8 {
            assert_eq!(lookup(&v, n, &budget, &decoded).unwrap(), None);
        }
        assert_eq!(budget.left(), 0, "the budget is spent");
        assert_eq!(decoded.get(), 0);
        assert_eq!(lookup(&v, 4, &budget, &decoded).unwrap(), None);
        assert_eq!(budget.left(), 0);
    }
}
