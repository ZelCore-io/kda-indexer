//! R-P5P9-62 H7, the window referee for top-level GraphQL lists. It replaces R-P5P9-61 H5
//! (the page cut at a height boundary), whose conditions were narrower.
//!
//! The oracle orders transaction, transfer and event lists by the tx's `creationTime`; ours
//! orders them by block height, node order inside a height (our contract: R-P5P9-62 sort
//! key, R3E-8). Two pages of one list can so hold different rows in a different order. H7
//! explains such a pair only when every row is accounted for:
//!
//! - rows on both sides match by identity: kind, request key, event or transfer index, block
//!   hash, chain and height, as far as the operation selects them (the same fields on both
//!   sides). Never by value alone, and never by `id` text: a relay id is only decoded for
//!   those key fields. Rows without an index within one key match as a multiset on every
//!   selected field (R-P5P9-63 (3)). The matched rows then compare field by field with the
//!   ruled matchers;
//! - an oracle-only row is non-canonical on the node by block (orphan: its block is not the
//!   node's at its height, R-P5P9-63 (2); never by content), an extra copy of a row the node
//!   holds once (duplicate), or canonical and outside our window by our full order: beyond
//!   our boundary height, or at it and listed after our page by our own longer read (one
//!   read of ours, `first` = our rows + 50, no `after`; our page must be its prefix:
//!   R-P5P9-64, I-1);
//! - an ours-only row is canonical on the node, holds the node's content on every selected
//!   field the node's entry has (R-P5P9-64 I-2, our amounts exact: M-3) and matches the
//!   query's own filter (R-P5P9-63 (1)). It is then outside the oracle's window (beyond its
//!   last `creationTime` while its page is cut) or a node-confirmed oracle gap;
//! - ours is in the list's height order: descending, or ascending for `blocksFromHeight`
//!   (R-P5P9-63 (6)); a cut page of ours shorter than `first` × the pages fetched is not a
//!   window (I-3).
//!
//! Every one-sided row gets a single-row node read: `/poll` for its request key (the row's
//! block or height, and its content against the node's entry). A row `/poll` does not know
//! (a coinbase row) is settled by the node's canonical header at its height (its own, or its
//! block's by the node's header-by-hash route, R-P5P9-63 (5)) and that block's coinbase; a
//! block row by the node's canonical header at its height, every selected field compared
//! (R-P5P9-67 I-B). The canonical header is the one on the branch of the node's cut, read
//! once per compare (R-P5P9-67 m-5), never the `/header` range by height, which also holds
//! orphans; above the cut there is none, and the row is unsettled. At most 300 node reads per compare
//! (R-P5P9-63 (4)), shared with H8 and the `payload.data` matcher.
//! A failed read, a spent read cap or a row whose identity cannot be decided leave the pair
//! as it was (unexplained), with the reason in the record's note. A row H7 cannot explain
//! gets its own entry, `/~graphql-h7-row`, which no allowlist entry matches.
//!
//! The pure parts are here; the reads are in [`Ctx::gql_window_referee`].

use std::collections::HashMap;

use serde_json::{json, Map, Value};

use crate::graphql::{Answer, GqlOp};
use crate::graphql_norm::edge_height_key;
use crate::report::{DiffEntry, DiffKind};
use crate::run::Ctx;
use crate::target::Target;

/// Visible hit of H7 (allowlist id `graphql-h7-window-referee`); it carries the evidence.
pub const WINDOW_PATH: &str = "/~graphql-h7-window-referee";
/// A row H7 could not explain (never allowlisted): the row and the reason.
pub const ROW_PATH: &str = "/~graphql-h7-row";
/// The tag that keeps matched rows paired through `normalize_gql`'s tie sort.
pub const ALIGN_KEY: &str = "kdxH7";
/// Node reads per compare at most (single-row reads, both sides together; H8, the
/// `payload.data` matcher and H7 share it): R-P5P9-63 (4).
pub const NODE_READS: usize = 300;
/// Rows past our page in our longer read (R-P5P9-64, I-1).
pub const PREFIX_EXTRA: usize = 50;
/// Visible hit of H8 (allowlist id `graphql-h8-transfer-order-index`); it carries the
/// evidence per request key.
pub const ORDER_INDEX_PATH: &str = "/~graphql-h8-transfer-order-index";
/// Visible hit of the `payload.data` matcher (allowlist id `graphql-r62-payload-data`).
pub const PAYLOAD_DATA_PATH: &str = "/~graphql-r62-payload-data";
/// R-P5P9-67 I-A (a): a cut page of ours shorter than `first` × the pages fetched. No
/// allowlist entry matches it.
pub const SHORT_PAGE_PATH: &str = "/~graphql-h7-short-page";
/// R-P5P9-67 m-6 (with R-P5P9-64 I-1): rows H7 identified but could not settle. No allowlist
/// entry matches it, so the pre-H7 page and tie cuts never excuse what H7 left undecided.
pub const UNDECIDED_PATH: &str = "/~graphql-h7-undecided";
/// Ours' page size cap (Q10): a page of `first` above it holds this many rows.
pub const OURS_PAGE_CAP: u64 = 100;
const ROUTE: &str = "graphql-window-referee";

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The type name and key text of a relay id (`base64("<Type>:<key>")`, standard or
/// url-safe alphabet).
pub fn decode_id(id: &str) -> Option<(String, String)> {
    let url: String = id
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect();
    let raw = kdx_types::hash::b64url_decode(&url).ok()?;
    let s = String::from_utf8(raw).ok()?;
    let (t, k) = s.split_once(':')?;
    Some((t.to_string(), k.to_string()))
}

/// The elements of a relay id key array (strings or numbers) as text.
pub fn id_parts(key: &str) -> Option<Vec<String>> {
    let v: Vec<Value> = serde_json::from_str(key).ok()?;
    v.iter().map(text).collect()
}

/// A `creationTime` (`YYYY-MM-DDTHH:MM:SS[.fff…]Z`) as Unix seconds and nanoseconds; the
/// flag is true when digits past the ninth are not all zero.
pub fn parse_utc(s: &str) -> Option<(i64, u32, bool)> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let (time, frac) = match time.split_once('.') {
        Some((t, f)) => (t, f),
        None => (time, ""),
    };
    let num = |x: &str, n: usize| -> Option<i64> {
        (x.len() == n && x.bytes().all(|b| b.is_ascii_digit()))
            .then(|| x.parse().ok())
            .flatten()
    };
    let d: Vec<&str> = date.split('-').collect();
    let t: Vec<&str> = time.split(':').collect();
    let ([y, m, dd], [hh, mm, ss]) = (d.as_slice(), t.as_slice()) else {
        return None;
    };
    let (y, m, dd) = (num(y, 4)?, num(m, 2)?, num(dd, 2)?);
    let (hh, mm, ss) = (num(hh, 2)?, num(mm, 2)?, num(ss, 2)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&dd) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Days from the civil date (H. Hinnant's algorithm).
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + dd - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let nine: String = frac.chars().chain(std::iter::repeat('0')).take(9).collect();
    let nanos: u32 = nine.parse().ok()?;
    let rest = frac.len() > 9 && frac[9..].bytes().any(|b| b != b'0');
    Some((days * 86_400 + hh * 3_600 + mm * 60 + ss, nanos, rest))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Tx,
    Transfer,
    Event,
    Block,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Tx => "tx",
            Kind::Transfer => "transfer",
            Kind::Event => "event",
            Kind::Block => "block",
        }
    }
}

/// A list row's identity: what the operation selects of it (or its relay id holds).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowId {
    pub kind: Option<Kind>,
    pub chain: Option<u8>,
    pub block: Option<String>,
    pub height: Option<u64>,
    pub rk: Option<String>,
    pub index: Option<i64>,
    /// The oracle's sort key (the tx's `creationTime`), when selected.
    pub ctime: Option<String>,
}

impl RowId {
    /// The identity key; `None` when it cannot be decided (no kind, no request key, or a
    /// block row without its hash).
    pub fn key(&self) -> Option<String> {
        let kind = self.kind?;
        if kind == Kind::Block {
            self.block.as_ref()?;
        } else {
            self.rk.as_ref()?;
        }
        let o = |x: Option<String>| x.unwrap_or_else(|| "-".into());
        Some(format!(
            "{}|{}|{}|{}|{}|{}",
            kind.as_str(),
            o(self.chain.map(|c| c.to_string())),
            o(self.block.clone()),
            o(self.height.map(|h| h.to_string())),
            o(self.rk.clone()),
            o(self.index.map(|i| i.to_string()))
        ))
    }

    /// Whether the key names one row: a tx or a block, or an event or transfer with its index.
    pub fn indexed(&self) -> bool {
        matches!(self.kind, Some(Kind::Tx | Kind::Block)) || self.index.is_some()
    }

    pub fn evidence(&self) -> Value {
        json!({"kind": self.kind.map(Kind::as_str), "chain": self.chain,
            "requestKey": self.rk, "index": self.index, "block": self.block,
            "height": self.height})
    }
}

/// The identity of a root list row (`edges[i]`) in the shapes the operations select.
pub fn row_id(e: &Value) -> RowId {
    let n = e.get("node").unwrap_or(&Value::Null);
    let get = |ps: &[&str]| {
        ps.iter()
            .find_map(|p| n.pointer(p).filter(|v| !v.is_null()))
            .and_then(text)
    };
    let id = n.get("id").and_then(Value::as_str).and_then(decode_id);
    let parts = id
        .as_ref()
        .and_then(|(_, k)| id_parts(k))
        .unwrap_or_default();
    let has = |ks: &[&str]| ks.iter().any(|k| n.get(*k).is_some());
    let kind = match id.as_ref().map(|(t, _)| t.as_str()) {
        Some("Transaction") => Some(Kind::Tx),
        Some("Transfer") => Some(Kind::Transfer),
        Some("Event") => Some(Kind::Event),
        Some("Block") => Some(Kind::Block),
        _ if has(&["cmd", "result"]) => Some(Kind::Tx),
        _ if has(&["senderAccount", "receiverAccount", "amount"]) => Some(Kind::Transfer),
        _ if has(&["qualifiedName", "parameters", "parameterText"]) => Some(Kind::Event),
        _ if !has(&["requestKey"])
            && has(&["hash"])
            && has(&["height", "payloadHash", "powHash", "chainId"]) =>
        {
            Some(Kind::Block)
        }
        _ => None,
    };
    let part = |i: usize| parts.get(i).cloned();
    let (id_block, id_rk, id_index, id_chain) = match kind {
        Some(Kind::Tx) if parts.len() == 2 => (part(0), part(1), None, None),
        Some(Kind::Transfer) if parts.len() == 5 => (part(0), part(4), part(2), part(1)),
        Some(Kind::Event) if parts.len() == 3 => (part(0), part(2), part(1), None),
        Some(Kind::Block) => (
            id.as_ref()
                .filter(|(t, _)| t == "Block")
                .map(|(_, k)| k.clone()),
            None,
            None,
            None,
        ),
        _ => (None, None, None, None),
    };
    let rk = get(&["/requestKey"])
        .or(id_rk)
        .or_else(|| (kind == Some(Kind::Tx)).then(|| get(&["/hash"])).flatten());
    let block = get(&["/blockHash", "/block/hash", "/result/block/hash"])
        .or(id_block)
        .or_else(|| {
            (kind == Some(Kind::Block))
                .then(|| get(&["/hash"]))
                .flatten()
        });
    let chain = get(&[
        "/chainId",
        "/block/chainId",
        "/result/block/chainId",
        "/cmd/meta/chainId",
    ])
    .or(id_chain)
    .and_then(|c| c.parse().ok());
    let index = matches!(kind, Some(Kind::Transfer | Kind::Event))
        .then(|| get(&["/orderIndex"]).or(id_index))
        .flatten()
        .and_then(|i| i.parse().ok());
    RowId {
        kind,
        chain,
        block,
        height: edge_height_key(e).and_then(|h| h.trim_matches('"').parse().ok()),
        rk,
        index,
        ctime: get(&["/creationTime", "/cmd/meta/creationTime"]),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Canon {
    Yes,
    No,
    Unknown,
}

impl Canon {
    pub fn as_str(self) -> &'static str {
        match self {
            Canon::Yes => "canonical",
            Canon::No => "not-canonical",
            Canon::Unknown => "unknown",
        }
    }
}

/// The node's verdict on one row: canonicity, the node's height and block for it, and how
/// the verdict was reached.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub canon: Canon,
    pub height: Option<u64>,
    pub block: Option<String>,
    pub why: String,
    /// The chain the node was read on.
    pub chain: Option<u8>,
    /// For a transfer or event row the node places: the node's event that is the row, and
    /// how many of the transaction's events have the row's content (R-P5P9-63 (3)).
    pub event: Option<Value>,
    pub matches: usize,
    /// The node's TRANSFER events of the row's transaction (a tx row's account filter).
    pub transfers: Vec<Value>,
}

impl Verdict {
    pub fn new(
        canon: Canon,
        height: Option<u64>,
        block: Option<String>,
        why: impl Into<String>,
    ) -> Verdict {
        Verdict {
            canon,
            height,
            block,
            why: why.into(),
            chain: None,
            event: None,
            matches: 0,
            transfers: Vec::new(),
        }
    }

    pub fn unknown(why: impl Into<String>) -> Verdict {
        Verdict::new(Canon::Unknown, None, None, why)
    }

    fn evidence(&self) -> Value {
        json!({"node": self.canon.as_str(), "nodeHeight": self.height,
            "nodeBlock": self.block, "via": self.why})
    }
}

/// What a row whose content the node does not hold is: rejected when it is ours; unknown
/// when it is the oracle's (R-P5P9-63 (2): an orphan is decided by block, never by content).
fn not_held(oracle: bool) -> Canon {
    if oracle {
        Canon::Unknown
    } else {
        Canon::No
    }
}

/// A Pact value's text (`{"decimal": …}` and `{"int": …}` unwrapped).
fn pact_text(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => m.get("decimal").or_else(|| m.get("int")).and_then(text),
        _ => text(v),
    }
}

/// The same amount: equal decimal values, or the same f64 only when `f64_ok` (the oracle's
/// f64-rounded amounts, R-P5P9-60 amendment). Ours must be exact (R-P5P9-64 M-3).
pub fn same_amount(a: &Value, b: &Value, f64_ok: bool) -> bool {
    let (Some(x), Some(y)) = (pact_text(a), pact_text(b)) else {
        return false;
    };
    let dc = crate::diff::decimal_canon;
    if matches!((dc(&x), dc(&y)), (Some(p), Some(q)) if p == q) {
        return true;
    }
    f64_ok
        && matches!((x.parse::<f64>(), y.parse::<f64>()), (Ok(p), Ok(q)) if p.is_finite() && p == q)
}

fn is_number(v: &Value) -> bool {
    match v {
        Value::Number(_) => true,
        Value::Object(m) => m.len() == 1 && (m.contains_key("decimal") || m.contains_key("int")),
        _ => false,
    }
}

/// Two Pact values equal: numbers (and the `{"decimal"}`/`{"int"}` forms) by value, the f64
/// form only when `f64_ok`; everything else exactly.
pub fn same_value(a: &Value, b: &Value, f64_ok: bool) -> bool {
    if is_number(a) && is_number(b) {
        return same_amount(a, b, f64_ok);
    }
    match (a, b) {
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_value(p, q, f64_ok))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, p)| y.get(k).is_some_and(|q| same_value(p, q, f64_ok)))
        }
        _ => a == b,
    }
}

/// A selected JSON-text field (`parameters`, `goodResult`, `continuation`) as its value: the
/// text parsed, else the text itself.
fn json_field(v: &Value) -> Value {
    match v {
        Value::String(s) => serde_json::from_str(s).unwrap_or_else(|_| v.clone()),
        v => v.clone(),
    }
}

fn module_name(ev: &Value) -> Option<String> {
    let m = ev.get("module")?;
    let name = m.get("name")?.as_str()?;
    Some(match m.get("namespace").and_then(Value::as_str) {
        Some(ns) => format!("{ns}.{name}"),
        None => name.to_string(),
    })
}

/// A field the row selects (not null).
fn sel<'a>(n: &'a Value, k: &str) -> Option<&'a Value> {
    n.get(k).filter(|v| !v.is_null())
}

/// Whether the node's event `ev` is the transfer row `n`: every field the row selects that
/// the event holds (sender, receiver, amount, module name and hash; R-P5P9-64 I-2). `oracle`:
/// the row is the oracle's, whose f64-rounded amount is allowed (M-3).
pub fn transfer_matches(n: &Value, ev: &Value, oracle: bool) -> bool {
    let p = |i: usize| ev.pointer(&format!("/params/{i}"));
    ev.get("name").and_then(Value::as_str) == Some("TRANSFER")
        && sel(n, "senderAccount").is_none_or(|x| p(0) == Some(x))
        && sel(n, "receiverAccount").is_none_or(|x| p(1) == Some(x))
        && sel(n, "amount").is_none_or(|x| p(2).is_some_and(|y| same_amount(x, y, oracle)))
        && sel(n, "moduleName").is_none_or(|x| module_name(ev).as_deref() == x.as_str())
        && sel(n, "moduleHash").is_none_or(|x| ev.get("moduleHash") == Some(x))
}

/// Whether the node's event `ev` is the event row `n`: every field the row selects that the
/// event holds (name, qualified name, parameters by value, module hash; R-P5P9-64 I-2).
pub fn event_matches(n: &Value, ev: &Value, oracle: bool) -> bool {
    let name = ev.get("name").and_then(Value::as_str);
    let params = ev.get("params").cloned().unwrap_or(Value::Null);
    sel(n, "name").is_none_or(|x| x.as_str() == name)
        && sel(n, "qualifiedName").is_none_or(|x| {
            module_name(ev)
                .zip(name)
                .is_some_and(|(m, nm)| x.as_str() == Some(format!("{m}.{nm}").as_str()))
        })
        && ["parameters", "parameterText"]
            .iter()
            .all(|k| sel(n, k).is_none_or(|x| same_value(&json_field(x), &params, oracle)))
        && sel(n, "moduleHash").is_none_or(|x| ev.get("moduleHash") == Some(x))
}

/// Whether the node's entry `p` (a `/poll` entry or a coinbase output) is the tx row `n`:
/// the result fields the row selects that the entry holds (status and data, gas, transaction
/// id, continuation, event count; R-P5P9-64 I-2). A failure's error is compared on ours only
/// (the oracle's error text has its own shape).
pub fn tx_matches(n: &Value, p: &Value, oracle: bool) -> bool {
    let Some(r) = n.get("result").filter(|r| r.is_object()) else {
        return true;
    };
    let status = p.pointer("/result/status").and_then(Value::as_str);
    let num = |x: &Value, y: Option<&Value>| match y.filter(|y| !y.is_null()) {
        Some(y) => same_amount(x, y, oracle),
        None => x.is_null(),
    };
    let events = json!(p
        .get("events")
        .and_then(Value::as_array)
        .map_or(0, Vec::len));
    let json_is = |x: &Value, node: Option<&Value>| {
        let node = node.unwrap_or(&Value::Null);
        if x.is_null() {
            node.is_null()
        } else {
            same_value(&json_field(x), node, oracle)
        }
    };
    r.get("gas").is_none_or(|x| num(x, p.get("gas")))
        && r.get("transactionId").is_none_or(|x| num(x, p.get("txId")))
        && r.get("eventCount").is_none_or(|x| num(x, Some(&events)))
        && r.get("goodResult").is_none_or(|x| {
            (status == Some("success")) != x.is_null()
                && (x.is_null() || json_is(x, p.pointer("/result/data")))
        })
        && r.get("badResult").is_none_or(|x| {
            (status == Some("failure")) != x.is_null()
                && (x.is_null() || oracle || json_is(x, p.pointer("/result/error")))
        })
        && r.get("continuation")
            .is_none_or(|x| json_is(x, p.get("continuation")))
}

/// The row's content against the node's entry for its transaction (a `/poll` entry or a
/// coinbase output): a tx row's result; a transfer or event row at its index when it has one
/// (a matching event at another index leaves the verdict unknown). Content the node does not
/// hold is [`not_held`].
fn content_verdict(
    r: &RowId,
    n: &Value,
    entry: &Value,
    oracle: bool,
    v: impl Fn(Canon, &str) -> Verdict,
) -> Verdict {
    let events: Vec<&Value> = entry
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect();
    let with = |mut x: Verdict, event: Option<&Value>, matches: usize| {
        x.event = event.cloned();
        x.matches = matches;
        x.transfers = events
            .iter()
            .filter(|e| e.get("name").and_then(Value::as_str) == Some("TRANSFER"))
            .map(|e| (*e).clone())
            .collect();
        x
    };
    let m: fn(&Value, &Value, bool) -> bool = match r.kind {
        Some(Kind::Transfer) => transfer_matches,
        Some(Kind::Event) => event_matches,
        Some(Kind::Tx) => {
            return if tx_matches(n, entry, oracle) {
                with(v(Canon::Yes, "placed, with the row's result"), None, 1)
            } else {
                with(
                    v(not_held(oracle), "the node's result is not the row's"),
                    None,
                    0,
                )
            };
        }
        _ => return with(v(Canon::Yes, "placed"), None, 1),
    };
    let hits: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, ev)| m(n, ev, oracle))
        .map(|(i, _)| i)
        .collect();
    let at = r
        .index
        .and_then(|i| usize::try_from(i).ok())
        .filter(|i| hits.contains(i));
    match (r.index, hits.first(), at) {
        (_, None, _) => with(
            v(not_held(oracle), "the node's transaction lacks the row"),
            None,
            0,
        ),
        (None, Some(&i), _) => with(
            v(Canon::Yes, "placed, with the row's content"),
            Some(events[i]),
            hits.len(),
        ),
        (Some(_), Some(_), Some(i)) => with(
            v(Canon::Yes, "placed, with the row's content"),
            Some(events[i]),
            1,
        ),
        (Some(_), Some(_), None) => with(
            v(Canon::Unknown, "the row's index is not the node's"),
            None,
            0,
        ),
    }
}

/// The node's verdict on a row from its `/poll` entry `p`. A row in another block than the
/// node's for the key is not canonical only when that block sits at the key's height (by
/// block, R-P5P9-63 (2)); `bh` is the node's height of the row's block when the row selects
/// no height (R-P5P9-63 (5)). Otherwise the row's content must be the node's.
pub fn poll_verdict(r: &RowId, n: &Value, p: &Value, oracle: bool, bh: Option<u64>) -> Verdict {
    let pb = p
        .pointer("/metaData/blockHash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let ph = p.pointer("/metaData/blockHeight").and_then(Value::as_u64);
    if pb.is_none() && ph.is_none() {
        return Verdict::unknown("poll: no block in the entry");
    }
    let v = |canon, why: &str| Verdict::new(canon, ph, pb.clone(), format!("poll: {why}"));
    if let Some(b) = &r.block {
        if pb.as_deref() != Some(b.as_str()) {
            return match r.height.or(bh) {
                Some(h) if Some(h) == ph => v(
                    Canon::No,
                    "the row's block is not the node's block at the key's height",
                ),
                _ => v(
                    not_held(oracle),
                    "the row's block is not the node's for the key, at another height",
                ),
            };
        }
    }
    if r.height.is_some_and(|h| Some(h) != ph) {
        return v(not_held(oracle), "the node's height for the key is another");
    }
    content_verdict(r, n, p, oracle, v)
}

/// A row `/poll` does not know (a coinbase row: its request key is the coinbase's) at height
/// `h` (its own, or the node's height of its block): the node's canonical header at `h` (on
/// the cut's branch, R-P5P9-67 m-5) and its payload (with outputs). Not canonical only by
/// block (R-P5P9-63 (2)).
pub fn coinbase_verdict(
    r: &RowId,
    n: &Value,
    h: u64,
    canonical: Option<&Value>,
    payload: Option<&Value>,
    oracle: bool,
) -> Verdict {
    let Some(hd) = canonical else {
        return Verdict::unknown(
            "coinbase: no canonical block at the height on the cut's branch (above the cut)",
        );
    };
    let hash = hd.get("hash").and_then(Value::as_str).map(str::to_string);
    let v =
        |canon, why: &str| Verdict::new(canon, Some(h), hash.clone(), format!("coinbase: {why}"));
    if r.block.as_ref().is_some_and(|b| Some(b) != hash.as_ref()) {
        return v(
            Canon::No,
            "the row's block is not the node's canonical block at its height",
        );
    }
    let Some(cb) = payload
        .and_then(|p| p.get("coinbase"))
        .and_then(Value::as_str)
        .and_then(|t| kdx_types::hash::b64url_decode(t).ok())
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
    else {
        return Verdict::unknown("coinbase: no payload");
    };
    if cb.get("reqKey").and_then(Value::as_str) != r.rk.as_deref() {
        return v(
            not_held(oracle),
            "the key is neither polled nor the block's coinbase",
        );
    }
    content_verdict(r, n, &cb, oracle, v)
}

/// A block row `n` against the node's canonical header at its height, on the cut's branch
/// (R-P5P9-67 m-5): `None` when there is none (above the cut). Canonical only as that block
/// with every selected field the node's, in ours' encodings (R-P5P9-67 I-B). A field that
/// differs makes ours' row not canonical and leaves the oracle's unsettled; a field the
/// harness cannot compare leaves either unsettled.
pub fn block_verdict(r: &RowId, n: &Value, canonical: Option<&Value>, oracle: bool) -> Verdict {
    let v =
        |canon, why: &str| Verdict::new(canon, r.height, r.block.clone(), format!("header: {why}"));
    let Some(hd) = canonical else {
        return Verdict::unknown(
            "header: no canonical block at the height on the cut's branch (above the cut)",
        );
    };
    if hd.get("hash").and_then(Value::as_str) != r.block.as_deref() {
        return v(
            Canon::No,
            "not the canonical block at the height (on the cut's branch)",
        );
    }
    match block_fields(n, hd, oracle) {
        Ok(()) => v(
            Canon::Yes,
            "the canonical block at the height, with its fields",
        ),
        Err(Field::Differs(f)) if !oracle => {
            v(Canon::No, &format!("the row's {f} is not the node's"))
        }
        Err(Field::Differs(f)) => {
            Verdict::unknown(format!("header: the oracle's {f} is not the node's"))
        }
        Err(Field::Unchecked(f)) => Verdict::unknown(format!(
            "header: the row selects {f}, which the harness cannot compare"
        )),
    }
}

/// Why a block row's field is not settled against the header.
enum Field {
    Differs(String),
    Unchecked(String),
}

/// Whether time text `x` is the header's `micros`: ours' form is milliseconds, truncated
/// (`…SS.mmmZ`); the oracle's may also be whole microseconds.
fn time_is(x: &Value, micros: Option<u64>, oracle: bool) -> bool {
    let (Some(t), Some(us)) = (x.as_str(), micros) else {
        return false;
    };
    let Some((secs, nanos, rest)) = parse_utc(t) else {
        return false;
    };
    let frac = t.rsplit_once('.').map_or(0, |(_, f)| f.len() - 1);
    let (s, sub) = ((us / 1_000_000) as i64, (us % 1_000_000) as u32);
    let ms = sub / 1_000 * 1_000_000;
    secs == s
        && !rest
        && ((frac == 3 && nanos == ms) || (oracle && (nanos == ms || nanos == sub * 1_000)))
}

/// The proof-of-work hash of the node's header (`powHash` as chainweb-data and ours render
/// it), when the header decodes.
fn node_pow_hash(hd: &Value) -> Option<String> {
    let h = kdx_decode::payload::decode_header(hd).ok()?;
    let mut adjacents: Vec<(u8, kdx_types::Hash32)> = Vec::new();
    for c in &h.adjacents_order {
        adjacents.push((*c, *h.adjacents.get(c)?));
    }
    let b = kdx_types::record::BlockRecord {
        hash: h.hash,
        parent: h.parent,
        adjacents,
        chain: h.chain_id,
        height: h.height,
        weight: h.weight,
        target: h.target,
        nonce: h.nonce,
        creation_time: h.creation_time,
        epoch_start: h.epoch_start,
        feature_flags: h.feature_flags,
        payload_hash: h.payload_hash,
        chainweb_version: h.chainweb_version,
        ..Default::default()
    };
    let p = kdx_decode::header::pow_hash(&b)?;
    Some(kdx_decode::header::pow_hash_hex(&p))
}

/// Every field block row `n` selects against the node's header `hd` (object encoding).
fn block_fields(n: &Value, hd: &Value, oracle: bool) -> Result<(), Field> {
    let Some(o) = n.as_object() else {
        return Ok(());
    };
    let node = |k: &str| hd.get(k).and_then(text);
    for (k, x) in o {
        let same = match k.as_str() {
            "__typename" => x.as_str() == Some("Block"),
            "id" => x
                .as_str()
                .and_then(decode_id)
                .is_some_and(|(t, key)| t == "Block" && Some(key) == node("hash")),
            "hash" | "height" | "chainId" | "nonce" | "payloadHash" | "target" | "weight" => {
                text(x).is_some() && text(x) == node(k)
            }
            "creationTime" => time_is(x, hd.get("creationTime").and_then(Value::as_u64), oracle),
            "epoch" => time_is(x, hd.get("epochStart").and_then(Value::as_u64), oracle),
            "difficulty" => {
                let d = node("target")
                    .and_then(|t| kdx_types::hash::b64url_decode(&t).ok())
                    .and_then(|b| <[u8; 32]>::try_from(b).ok())
                    .map(|t| kdx_decode::header::difficulty(&t).min(i128::MAX as u128));
                text(x).is_some() && text(x) == d.map(|d| d.to_string())
            }
            "parent" => {
                if let Some(f) = x.as_object().and_then(|p| p.keys().find(|f| *f != "hash")) {
                    return Err(Field::Unchecked(format!("parent.{f}")));
                }
                let p = x.get("hash").and_then(text);
                p.is_some() && p == node("parent")
            }
            "powHash" => match node_pow_hash(hd) {
                Some(p) => x.as_str() == Some(p.as_str()),
                None => return Err(Field::Unchecked(k.clone())),
            },
            _ => return Err(Field::Unchecked(k.clone())),
        };
        if !same {
            return Err(Field::Differs(k.clone()));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-63 (1): the query's own filter

/// The variable a document passes to argument `arg` (`arg: $v`), else `arg` itself.
pub fn arg_var(query: Option<&str>, arg: &str) -> String {
    let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let mut rest = query.unwrap_or_default();
    while let Some(i) = rest.find(arg) {
        let before = rest[..i].chars().next_back();
        let after = &rest[i + arg.len()..];
        rest = after;
        if before.is_some_and(word) || after.chars().next().is_some_and(word) {
            continue;
        }
        let Some(v) = after
            .trim_start()
            .strip_prefix(':')
            .and_then(|a| a.trim_start().strip_prefix('$'))
        else {
            continue;
        };
        let name: String = v
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            return name;
        }
    }
    arg.to_string()
}

/// The query's own filter (R-P5P9-63 (1)): the root field's arguments an ours-only row is
/// checked against, each from the variable the document passes to it.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Filter {
    pub account: Option<String>,
    pub fungible: Option<String>,
    pub chains: Option<Vec<u8>>,
    pub event: Option<String>,
    /// `parametersFilter` (`{"array_starts_with": …}`, as ours serves it); `Err` when the
    /// harness cannot read it.
    pub params: Option<Result<Vec<Value>, String>>,
    pub min_height: Option<u64>,
    pub max_height: Option<u64>,
    pub block: Option<String>,
    pub rk: Option<String>,
    /// R-P5P9-67 I-C: the root's other arguments with a non-null value or a literal (`first`,
    /// `after`, `last` and `before` aside). The harness does not check them, so they refuse
    /// the ours-only excuse.
    pub unchecked: Vec<String>,
}

/// The root arguments [`Filter`] reads (from variables only).
const READ_ARGS: [&str; 12] = [
    "accountName",
    "fungibleName",
    "chainId",
    "chainIds",
    "qualifiedEventName",
    "parametersFilter",
    "minHeight",
    "maxHeight",
    "startHeight",
    "endHeight",
    "blockHash",
    "requestKey",
];
/// The paging arguments, which are no filter.
const PAGE_ARGS: [&str; 4] = ["first", "after", "last", "before"];

/// The arguments of root field `key` (its alias or its name) in document `query`, as (name,
/// value text); `None` when the field or its arguments cannot be read.
pub fn root_args(query: &str, key: &str) -> Option<Vec<(String, String)>> {
    let b = query.as_bytes();
    let ident = |mut i: usize| {
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        i
    };
    let space = |mut i: usize| {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        i
    };
    let (mut depth, mut paren, mut i) = (0i32, 0i32, 0usize);
    while i < b.len() {
        match b[i] {
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'{' => depth += 1,
            b'}' => depth -= 1,
            b'(' => paren += 1,
            b')' => paren -= 1,
            c if (c.is_ascii_alphabetic() || c == b'_') && depth == 1 && paren == 0 => {
                let j = ident(i);
                if &query[i..j] != key {
                    i = j;
                    continue;
                }
                let mut k = space(j);
                if b.get(k) == Some(&b':') {
                    k = space(ident(space(k + 1)));
                }
                if b.get(k) != Some(&b'(') {
                    return Some(Vec::new());
                }
                return parse_args(query, k + 1);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The `name: value` pairs of an argument list from byte `i` (past its `(`) to its `)`.
fn parse_args(q: &str, mut i: usize) -> Option<Vec<(String, String)>> {
    let b = q.as_bytes();
    let mut out = Vec::new();
    loop {
        while i < b.len() && (b[i].is_ascii_whitespace() || b[i] == b',') {
            i += 1;
        }
        if *b.get(i)? == b')' {
            return Some(out);
        }
        let s = i;
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        let name = q[s..i].to_string();
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if name.is_empty() || b.get(i) != Some(&b':') {
            return None;
        }
        i += 1;
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        let (vs, mut d) = (i, 0i32);
        while i < b.len() {
            match b[i] {
                b'"' => {
                    i += 1;
                    while i < b.len() && b[i] != b'"' {
                        i += if b[i] == b'\\' { 2 } else { 1 };
                    }
                }
                b'[' | b'{' => d += 1,
                b']' | b'}' => d -= 1,
                b',' | b')' if d == 0 => break,
                c if c.is_ascii_whitespace() && d == 0 => break,
                _ => {}
            }
            i += 1;
        }
        if vs == i || i > b.len() {
            return None;
        }
        out.push((name, q[vs..i].to_string()));
    }
}

/// Whether the document's variable `var` has a default other than `null`.
fn var_default(query: &str, var: &str) -> bool {
    let head = query.split('{').next().unwrap_or_default();
    let pat = format!("${var}");
    let mut rest = head;
    while let Some(i) = rest.find(&pat) {
        let after = &rest[i + pat.len()..];
        if after
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            rest = after;
            continue;
        }
        let def = after.split(['$', ')']).next().unwrap_or_default();
        return def
            .split_once('=')
            .is_some_and(|(_, d)| d.trim().trim_end_matches(',').trim() != "null");
    }
    false
}

/// R-P5P9-67 I-C: the arguments of the root `root` the harness does not check: any other than
/// the paging ones with a literal value, or with a variable that is set (or has a default),
/// unless [`Filter`] reads it from that variable. Without a document: none (the tests'
/// entry point).
fn unchecked_args(
    query: Option<&str>,
    root: Option<&str>,
    vars: &Map<String, Value>,
) -> Vec<String> {
    let Some(q) = query else {
        return Vec::new();
    };
    let Some(args) = root.and_then(|r| root_args(q, r)) else {
        return vec!["the root field's arguments, which the harness cannot read".into()];
    };
    let mut out = Vec::new();
    for (name, value) in args {
        if PAGE_ARGS.contains(&name.as_str()) || value == "null" {
            continue;
        }
        let Some(var) = value.strip_prefix('$') else {
            out.push(name);
            continue;
        };
        let set = vars
            .get(var)
            .map_or_else(|| var_default(q, var), |v| !v.is_null());
        let read = READ_ARGS.contains(&name.as_str()) && arg_var(query, &name) == var;
        if set && !read {
            out.push(name);
        }
    }
    out
}

fn params_filter(t: &str) -> Result<Vec<Value>, String> {
    let bad = || format!("a parametersFilter the harness cannot read: {t}");
    let v: Value = serde_json::from_str(t).map_err(|_| bad())?;
    let o = v.as_object().filter(|o| o.len() == 1).ok_or_else(bad)?;
    match o.get("array_starts_with") {
        Some(Value::Array(a)) if !a.is_empty() => Ok(a.clone()),
        Some(Value::Array(_)) | None => Err(bad()),
        Some(x) => Ok(vec![x.clone()]),
    }
}

impl Filter {
    pub fn from_vars(query: Option<&str>, vars: &Map<String, Value>) -> Filter {
        Filter::of(query, None, vars)
    }

    /// The filter of root `root` (its key in `data`), with the root's arguments the harness
    /// does not check ([`Filter::unchecked`]).
    pub fn of(query: Option<&str>, root: Option<&str>, vars: &Map<String, Value>) -> Filter {
        let get = |arg: &str| vars.get(&arg_var(query, arg)).filter(|v| !v.is_null());
        let s = |arg: &str| get(arg).and_then(text);
        let h = |arg: &str| s(arg).and_then(|t| t.parse::<u64>().ok());
        let chains = match (s("chainId"), get("chainIds").and_then(Value::as_array)) {
            (Some(c), _) => Some(c.parse().ok().into_iter().collect()),
            (None, Some(cs)) => Some(
                cs.iter()
                    .filter_map(text)
                    .filter_map(|c| c.parse().ok())
                    .collect(),
            ),
            _ => None,
        };
        Filter {
            account: s("accountName"),
            fungible: s("fungibleName"),
            chains,
            event: s("qualifiedEventName"),
            params: s("parametersFilter").map(|t| params_filter(&t)),
            min_height: h("minHeight").or_else(|| h("startHeight")),
            max_height: h("maxHeight").or_else(|| h("endHeight")),
            block: s("blockHash"),
            rk: s("requestKey"),
            unchecked: root.map_or_else(Vec::new, |_| unchecked_args(query, root, vars)),
        }
    }

    /// Why a canonical ours-only row is outside the filter, if it is: by the row's own fields
    /// where it selects them, else by the node's (its event, its transaction's TRANSFER
    /// events, its chain and block). `h`: the row's height.
    pub fn outside(&self, r: &Row, h: Option<u64>, v: &Verdict) -> Option<String> {
        let n = r.edge.get("node").unwrap_or(&Value::Null);
        let field = |k: &str| sel(n, k).and_then(text);
        let ev = v.event.as_ref();
        let param = |i: usize| {
            ev.and_then(|e| e.pointer(&format!("/params/{i}")))
                .and_then(text)
        };
        let fail = |what: &str| Some(format!("outside the query's {what}"));
        // R-P5P9-67 I-C: every argument with a value must be one the harness checks for the
        // row's kind.
        let kind = r.id.kind;
        let not_read = [
            (
                "accountName",
                self.account.is_some() && !matches!(kind, Some(Kind::Transfer | Kind::Tx)),
            ),
            (
                "fungibleName",
                self.fungible.is_some()
                    && !(kind == Some(Kind::Transfer)
                        || (kind == Some(Kind::Tx) && self.account.is_some())),
            ),
            (
                "qualifiedEventName",
                self.event.is_some() && kind != Some(Kind::Event),
            ),
            (
                "parametersFilter",
                self.params.is_some() && kind != Some(Kind::Event),
            ),
        ];
        if let Some(name) = self
            .unchecked
            .iter()
            .map(String::as_str)
            .chain(not_read.iter().filter(|x| x.1).map(|x| x.0))
            .next()
        {
            return Some(format!("under a filter the harness does not check: {name}"));
        }
        if let Some(cs) = &self.chains {
            if !r.id.chain.or(v.chain).is_some_and(|c| cs.contains(&c)) {
                return fail("chain");
            }
        }
        if self.min_height.is_some_and(|lo| h.is_none_or(|h| h < lo))
            || self.max_height.is_some_and(|hi| h.is_none_or(|h| h > hi))
        {
            return fail("height window");
        }
        if self
            .block
            .as_ref()
            .is_some_and(|b| r.id.block.as_ref().or(v.block.as_ref()) != Some(b))
        {
            return fail("block");
        }
        if self
            .rk
            .as_ref()
            .is_some_and(|k| r.id.rk.as_ref() != Some(k))
        {
            return fail("request key");
        }
        let fungible =
            |m: Option<String>| self.fungible.as_ref().is_none_or(|f| m.as_ref() == Some(f));
        match r.id.kind {
            Some(Kind::Transfer) => {
                if let Some(a) = &self.account {
                    let from = field("senderAccount").or_else(|| param(0));
                    let to = field("receiverAccount").or_else(|| param(1));
                    if from.as_ref() != Some(a) && to.as_ref() != Some(a) {
                        return fail("account");
                    }
                }
                if !fungible(field("moduleName").or_else(|| ev.and_then(module_name))) {
                    return fail("fungible");
                }
            }
            Some(Kind::Event) => {
                let node_name = ev.and_then(|e| {
                    Some(format!("{}.{}", module_name(e)?, e.get("name")?.as_str()?))
                });
                if self
                    .event
                    .as_ref()
                    .is_some_and(|q| field("qualifiedName").or(node_name).as_ref() != Some(q))
                {
                    return fail("event name");
                }
                match &self.params {
                    None => {}
                    Some(Err(w)) => return Some(w.clone()),
                    Some(Ok(pre)) => {
                        let ps = ev.and_then(|e| e.get("params")).and_then(Value::as_array);
                        let starts = ps.is_some_and(|ps| {
                            ps.len() >= pre.len()
                                && pre.iter().zip(ps).all(|(a, b)| same_value(a, b, false))
                        });
                        if !starts {
                            return fail("parametersFilter");
                        }
                    }
                }
            }
            Some(Kind::Tx) => {
                // `transactions(accountName)` lists the txs the account sent (GQ-19: ours
                // filters on `meta.sender`); the sender pays the gas, the transaction's last
                // TRANSFER event. With `fungibleName`, a TRANSFER of that module names it.
                if let Some(a) = &self.account {
                    let p = |e: &Value, i: usize| {
                        e.pointer(&format!("/params/{i}"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    };
                    let payer = v.transfers.last().and_then(|e| p(e, 0));
                    let sender = n.pointer("/cmd/meta/sender").and_then(Value::as_str);
                    if payer.as_ref() != Some(a) || sender.is_some_and(|s| s != a) {
                        return fail("account (the sender)");
                    }
                    let names = |e: &Value| {
                        (p(e, 0).as_ref() == Some(a) || p(e, 1).as_ref() == Some(a))
                            && fungible(module_name(e))
                    };
                    if self.fungible.is_some() && !v.transfers.iter().any(names) {
                        return fail("fungible");
                    }
                }
            }
            _ => {}
        }
        None
    }
}

// ---------------------------------------------------------------------------------------------
// Matching and the decision

/// One row of a side with its identity.
#[derive(Debug, Clone)]
pub struct Row {
    pub id: RowId,
    pub key: String,
    pub edge: Value,
}

fn node_of(r: &Row) -> &Value {
    r.edge.get("node").unwrap_or(&Value::Null)
}

/// Both pages' rows, identified.
#[derive(Debug, Clone)]
pub struct Prep {
    pub a: Vec<Row>,
    pub b: Vec<Row>,
    pub a_cut: bool,
    pub b_cut: bool,
}

fn more(c: &Value) -> bool {
    c.pointer("/pageInfo/hasNextPage") == Some(&Value::Bool(true))
}

/// Keys in first-seen order and each key's row indexes.
fn group<'a>(rows: &'a [Row], live: &[usize]) -> (Vec<&'a str>, HashMap<&'a str, Vec<usize>>) {
    let mut order = Vec::new();
    let mut m: HashMap<&str, Vec<usize>> = HashMap::new();
    for &i in live {
        let k = rows[i].key.as_str();
        m.entry(k)
            .or_insert_with(|| {
                order.push(k);
                Vec::new()
            })
            .push(i);
    }
    (order, m)
}

/// The rows of two root connections (ours `ca`, the oracle's `cb`) with their identities.
/// Fails when a row's identity cannot be decided.
pub fn prepare(ca: &Value, cb: &Value) -> Result<Prep, String> {
    let rows = |c: &Value| -> Result<Vec<Row>, String> {
        let mut out = Vec::new();
        for e in c
            .get("edges")
            .and_then(Value::as_array)
            .ok_or("a page without edges")?
        {
            let id = row_id(e);
            let key = id.key().ok_or_else(|| {
                format!("a row whose identity cannot be decided: {}", id.evidence())
            })?;
            out.push(Row {
                id,
                key,
                edge: e.clone(),
            });
        }
        Ok(out)
    };
    Ok(Prep {
        a: rows(ca)?,
        b: rows(cb)?,
        a_cut: more(ca),
        b_cut: more(cb),
    })
}

/// The rows matched across the sides, per identity key in ours' first-seen order. An indexed
/// key pairs its first rows. Rows without an index within one key match as a multiset on
/// every selected field (R-P5P9-63 (3)): rows equal in every field pair, the rest are
/// one-sided.
struct Matching {
    /// (ours' row, the oracle's row), in ours' order.
    pairs: Vec<(usize, usize)>,
    /// Ours' rows no oracle row matches (an indexed key's first row only).
    ours: Vec<usize>,
    /// The oracle's rows no row of ours matches: an indexed key's rows together, else one
    /// row per group.
    oracle: Vec<Vec<usize>>,
}

fn matching(p: &Prep, live_a: &[usize], live_b: &[usize]) -> Matching {
    let (oa, ga) = group(&p.a, live_a);
    let (ob, gb) = group(&p.b, live_b);
    let mut m = Matching {
        pairs: Vec::new(),
        ours: Vec::new(),
        oracle: Vec::new(),
    };
    let mut left: HashMap<&str, Vec<usize>> = HashMap::new();
    for k in &oa {
        let ia = &ga[k];
        let multiset = !p.a[ia[0]].id.indexed();
        if !multiset {
            match gb.get(k).and_then(|j| j.first()) {
                Some(&j) => m.pairs.push((ia[0], j)),
                None => m.ours.push(ia[0]),
            }
            continue;
        }
        let mut free = gb.get(k).cloned().unwrap_or_default();
        for &i in ia {
            match free
                .iter()
                .position(|&j| node_of(&p.b[j]) == node_of(&p.a[i]))
            {
                Some(x) => m.pairs.push((i, free.remove(x))),
                None => m.ours.push(i),
            }
        }
        left.insert(k, free);
    }
    for k in &ob {
        let rows = left.remove(k).unwrap_or_else(|| {
            if ga.contains_key(k) {
                Vec::new()
            } else {
                gb[k].clone()
            }
        });
        if rows.is_empty() {
            continue;
        }
        if p.b[rows[0]].id.indexed() {
            m.oracle.push(rows);
        } else {
            m.oracle.extend(rows.into_iter().map(|j| vec![j]));
        }
    }
    m
}

impl Prep {
    /// The rows that need a node verdict, as (side: 0 ours, 1 the oracle's; row index):
    /// ours without a height or not matched once, and one row per oracle-only group.
    pub fn reads(&self) -> Vec<(usize, usize)> {
        let all_a: Vec<usize> = (0..self.a.len()).collect();
        let all_b: Vec<usize> = (0..self.b.len()).collect();
        let m = matching(self, &all_a, &all_b);
        let (_, gb) = group(&self.b, &all_b);
        let mut out = Vec::new();
        for (i, r) in self.a.iter().enumerate() {
            let nb = gb.get(r.key.as_str()).map_or(0, Vec::len);
            if r.id.height.is_none() || m.ours.contains(&i) || (r.id.indexed() && nb != 1) {
                out.push((0, i));
            }
        }
        out.extend(m.oracle.iter().map(|g| (1, g[0])));
        out
    }
}

/// What H7 knows of a pair besides its rows: the oracle's ceiling, the list's direction, the
/// query's filter and our longer read (I-1).
pub struct Window<'a> {
    pub max: u64,
    /// The list ascends by height (`blocksFromHeight`); else it descends (R-P5P9-63 (6)).
    pub asc: bool,
    pub filter: &'a Filter,
    /// Our longer read (R-P5P9-64, I-1); `None` while it is not read.
    pub prefix: Option<&'a Result<Longer, String>>,
}

/// Our longer read (R-P5P9-64, I-1): each row's identity key and node, and whether ours
/// capped it (fewer rows than asked, with a next page; R-P5P9-67 m-6).
#[derive(Debug, Clone, Default)]
pub struct Longer {
    pub rows: Vec<(String, Value)>,
    pub capped: bool,
}

/// H7's verdict on a pair.
#[derive(Debug, Default)]
pub struct Decision {
    /// Why the pair cannot be settled (a failed read, a cap, a row without a height): the
    /// pages stay as they were.
    pub undecided: Vec<String>,
    /// Rows the node settles against ours: (ours' row, the oracle's row, why).
    pub rejected: Vec<(Option<Value>, Option<Value>, String)>,
    /// Whether the rows were matched (else the pages stay as they were).
    pub grouped: bool,
    /// Matched rows (ours' index, the oracle's index) in ours' order.
    pub pairs: Vec<(usize, usize)>,
    /// Evidence of the one-sided rows explained, per side.
    pub ours: Vec<Value>,
    pub oracle: Vec<Value>,
    /// Rows above the oracle's ceiling, left out (the stall rule).
    pub above: usize,
    /// Our boundary height (the lowest, or the highest on an ascending list) and the
    /// oracle's last `creationTime` in its order.
    pub lowest: Option<u64>,
    pub oracle_lowest_ctime: Option<String>,
    /// An oracle-only row at our boundary height waits for our longer read (I-1).
    pub needs_prefix: bool,
}

impl Decision {
    pub fn explained(&self) -> bool {
        self.undecided.is_empty() && self.rejected.is_empty()
    }
}

fn evidence(r: &Row, v: Option<&Verdict>, why: &str) -> Value {
    let mut e = r.id.evidence();
    if let (Some(m), Some(Value::Object(x))) = (e.as_object_mut(), v.map(Verdict::evidence)) {
        m.extend(x);
    }
    e["why"] = json!(why);
    e
}

fn why_of(v: Option<&Verdict>) -> String {
    v.map_or_else(|| "no read".into(), |v| v.why.clone())
}

/// Where an oracle-only canonical row lies against our window.
enum Place {
    Outside(&'static str),
    Inside(String),
    Wait(String),
}

/// R-P5P9-64 (I-1): an oracle-only row at our boundary height lies outside our window only
/// when our page (`n` rows, keys `ours`) is a prefix of our longer read and the row appears
/// in that read after our cut.
fn after_our_cut(prefix: Option<&Result<Longer, String>>, ours: &[Row], rb: &Row) -> Place {
    let (rows, capped) = match prefix {
        None => {
            return Place::Wait(
                "an oracle-only row at our boundary height waits for our longer read".into(),
            )
        }
        Some(Err(w)) => return Place::Wait(format!("our longer read: {w}")),
        Some(Ok(l)) => (&l.rows, l.capped),
    };
    if rows.len() < ours.len() {
        return Place::Wait(format!(
            "our longer read holds {} rows, fewer than our page's {}",
            rows.len(),
            ours.len()
        ));
    }
    if ours.iter().zip(rows).any(|(r, (k, _))| r.key != *k) {
        return Place::Inside(
            "our page is not a prefix of our own longer read (a cursor join, or the list moved)"
                .into(),
        );
    }
    let later = rows[ours.len()..]
        .iter()
        .any(|(k, n)| *k == rb.key && (rb.id.indexed() || n == node_of(rb)));
    if later {
        Place::Outside("after-our-cut")
    } else if capped {
        // R-P5P9-67 m-6: ours cut the read short of `first` with a next page; the row may
        // lie past it.
        Place::Wait(format!(
            "undecided: our longer read was capped ({} rows, with a next page)",
            rows.len()
        ))
    } else {
        Place::Inside(
            "an oracle-only canonical row at our boundary height that our longer read does not \
             list after our page: a row we skipped"
                .into(),
        )
    }
}

/// H7 on prepared pages with the node's verdicts (`va` per row of ours, `vb` per row of the
/// oracle's; `None` where none was read).
pub fn decide(p: &Prep, va: &[Option<Verdict>], vb: &[Option<Verdict>], w: &Window) -> Decision {
    let mut d = Decision::default();
    let canon = |v: &Option<Verdict>| v.as_ref().map_or(Canon::Unknown, |v| v.canon);
    // A row's height: its own, else the node's for a row the node places.
    let height = |r: &Row, v: &Option<Verdict>| {
        r.id.height.or_else(|| {
            v.as_ref()
                .filter(|v| v.canon == Canon::Yes)
                .and_then(|v| v.height)
        })
    };
    let ha: Vec<Option<u64>> = p.a.iter().zip(va).map(|(r, v)| height(r, v)).collect();
    let hb: Vec<Option<u64>> = p.b.iter().zip(vb).map(|(r, v)| height(r, v)).collect();
    let above = |h: &Option<u64>| h.is_some_and(|h| h > w.max);
    let live_a: Vec<usize> = (0..p.a.len()).filter(|&i| !above(&ha[i])).collect();
    let live_b: Vec<usize> = (0..p.b.len()).filter(|&j| !above(&hb[j])).collect();
    d.above = p.a.len() + p.b.len() - live_a.len() - live_b.len();
    let not_canonical = |i: usize| {
        format!(
            "a row of ours the node calls not canonical ({})",
            why_of(va[i].as_ref())
        )
    };
    // Ours: a height for every row, in the list's height order (our sort key).
    for &i in &live_a {
        if ha[i].is_none() {
            match canon(&va[i]) {
                Canon::No => d
                    .rejected
                    .push((Some(p.a[i].edge.clone()), None, not_canonical(i))),
                _ => d.undecided.push(format!(
                    "no height for a row of ours ({}): {}",
                    why_of(va[i].as_ref()),
                    p.a[i].id.evidence()
                )),
            }
        }
    }
    if !d.explained() {
        return d;
    }
    let hs: Vec<u64> = live_a.iter().filter_map(|&i| ha[i]).collect();
    let (dir, back) = if w.asc {
        ("ascending", hs.windows(2).any(|x| x[0] > x[1]))
    } else {
        ("descending", hs.windows(2).any(|x| x[0] < x[1]))
    };
    if back {
        d.rejected
            .push((None, None, format!("ours is not in {dir} height order")));
        return d;
    }
    d.lowest = if w.asc {
        hs.iter().max()
    } else {
        hs.iter().min()
    }
    .copied();
    let ct = |r: &Row| r.id.ctime.as_deref().and_then(parse_utc);
    let times = live_b.iter().filter_map(|&j| ct(&p.b[j]).map(|t| (t, j)));
    let b_last = if w.asc { times.max() } else { times.min() };
    d.oracle_lowest_ctime = b_last.and_then(|(_, j)| p.b[j].id.ctime.clone());
    d.grouped = true;
    let m = matching(p, &live_a, &live_b);
    let (_, ga) = group(&p.a, &live_a);
    let (_, gb) = group(&p.b, &live_b);
    let twice = |i: usize| p.a[i].id.indexed() && ga[p.a[i].key.as_str()].len() > 1;
    for &(i, j) in &m.pairs {
        let (ra, vi) = (&p.a[i], va[i].as_ref());
        if twice(i) {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                "ours lists the row twice".into(),
            ));
            continue;
        }
        d.pairs.push((i, j));
        let jb = &gb[ra.key.as_str()];
        if ra.id.indexed() && jb.len() > 1 {
            // The node holds the row once: the oracle's other copies are duplicates.
            match canon(&va[i]) {
                Canon::Yes => d
                    .oracle
                    .extend(jb[1..].iter().map(|&j| evidence(&p.b[j], vi, "duplicate"))),
                Canon::No => d
                    .rejected
                    .push((Some(ra.edge.clone()), None, not_canonical(i))),
                Canon::Unknown => d.undecided.push(format!(
                    "an oracle duplicate the node did not settle ({}): {}",
                    why_of(vi),
                    ra.id.evidence()
                )),
            }
        }
    }
    for &i in &m.ours {
        let (ra, vi) = (&p.a[i], va[i].as_ref());
        if twice(i) {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                "ours lists the row twice".into(),
            ));
            continue;
        }
        let Some(v) = vi.filter(|v| v.canon != Canon::Unknown) else {
            d.undecided.push(format!(
                "an ours-only row the node did not settle ({}): {}",
                why_of(vi),
                ra.id.evidence()
            ));
            continue;
        };
        if v.canon == Canon::No {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!("an ours-only row the node calls not canonical ({})", v.why),
            ));
            continue;
        }
        let outside_filter = w.filter.outside(ra, ha[i], v);
        if let Some(why) = outside_filter {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!("an ours-only canonical row {why}"),
            ));
            continue;
        }
        let copies = ga[ra.key.as_str()]
            .iter()
            .filter(|&&x| node_of(&p.a[x]) == node_of(ra))
            .count();
        if !ra.id.indexed() && copies > v.matches {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!(
                    "ours lists the row {copies} times; the node's transaction holds it {} times",
                    v.matches
                ),
            ));
            continue;
        }
        let outside = p.b_cut
            && match (ct(ra), b_last) {
                (Some(t), Some((lo, _))) => {
                    if w.asc {
                        t >= lo
                    } else {
                        t <= lo
                    }
                }
                _ => false,
            };
        let why = if outside {
            "outside-oracle-window"
        } else {
            "oracle-gap"
        };
        d.ours.push(evidence(ra, vi, why));
    }
    for g in &m.oracle {
        let j = g[0];
        let (rb, vj) = (&p.b[j], vb[j].as_ref());
        match canon(&vb[j]) {
            Canon::No => d
                .oracle
                .extend(g.iter().map(|&j| evidence(&p.b[j], vj, "orphan"))),
            Canon::Yes => {
                let beyond = |h: u64, bd: u64| if w.asc { h > bd } else { h < bd };
                let place = match (p.a_cut, d.lowest, hb[j]) {
                    (false, _, _) => {
                        Place::Inside("an oracle-only canonical row; our list is complete".into())
                    }
                    (true, Some(bd), Some(h)) if beyond(h, bd) => Place::Outside(if w.asc {
                        "above-our-window"
                    } else {
                        "below-our-window"
                    }),
                    (true, Some(bd), Some(h)) if h == bd => after_our_cut(w.prefix, &p.a, rb),
                    (true, Some(_), Some(_)) => Place::Inside(
                        "an oracle-only canonical row strictly inside our window".into(),
                    ),
                    _ => Place::Inside(
                        "an oracle-only canonical row without a height to place".into(),
                    ),
                };
                match place {
                    Place::Outside(why) => {
                        d.oracle.push(evidence(rb, vj, why));
                        d.oracle
                            .extend(g[1..].iter().map(|&j| evidence(&p.b[j], vj, "duplicate")));
                    }
                    Place::Inside(why) => d.rejected.push((None, Some(rb.edge.clone()), why)),
                    Place::Wait(why) => {
                        d.needs_prefix |= w.prefix.is_none();
                        d.undecided.push(format!("{why}: {}", rb.id.evidence()));
                    }
                }
            }
            Canon::Unknown => d.undecided.push(format!(
                "an oracle-only row the node did not settle ({}): {}",
                why_of(vj),
                rb.id.evidence()
            )),
        }
    }
    d
}

/// After `normalize_gql`: when H7 aligned a root list (its rows carry [`ALIGN_KEY`]), puts
/// the rows back in the aligned order and drops the tag. Returns whether it did.
pub fn realign(conn: &mut Value) -> bool {
    let Some(edges) = conn.get_mut("edges").and_then(Value::as_array_mut) else {
        return false;
    };
    if edges.is_empty() || !edges.iter().all(|e| e.get(ALIGN_KEY).is_some()) {
        return false;
    }
    edges.sort_by_key(|e| e[ALIGN_KEY].as_str().unwrap_or_default().to_string());
    for e in edges.iter_mut() {
        if let Some(m) = e.as_object_mut() {
            m.shift_remove(ALIGN_KEY);
        }
    }
    true
}

/// Node and ours reads of one compare, cached per key. H8, the `payload.data` matcher and H7
/// share one, so [`NODE_READS`] caps the compare.
#[derive(Default)]
pub(crate) struct Reads {
    pub(crate) node: usize,
    pub(crate) ours: usize,
    polls: HashMap<(u8, String), Result<Option<Value>, String>>,
    headers: HashMap<(u8, u64), Result<Vec<Value>, String>>,
    /// R-P5P9-67 m-5: the node's cut (one read per compare) and the canonical header per
    /// (chain, height) on its branch.
    cut: Option<Result<HashMap<u8, String>, String>>,
    canonical: HashMap<(u8, u64), Result<Option<Value>, String>>,
    payloads: HashMap<String, Result<Value, String>>,
    by_hash: HashMap<String, Result<Option<u64>, String>>,
    chains: HashMap<String, Option<u8>>,
}

fn entry(path: &str, a: Option<Value>, b: Option<Value>) -> DiffEntry {
    DiffEntry {
        path: path.into(),
        kind: DiffKind::Changed,
        a,
        b,
    }
}

/// The name of the root connection field of `data`.
fn root_name(data: &Value) -> Option<String> {
    data.as_object()?
        .iter()
        .find(|(_, v)| v.get("edges").is_some_and(Value::is_array) && v.get("pageInfo").is_some())
        .map(|(k, _)| k.clone())
}

impl Ctx {
    /// R-P5P9-62 H7 on an oracle pair's root list (see the module docs). When every row is
    /// accounted for, the pages hold the matched rows, paired in ours' order, and the hit
    /// carries the evidence. When the node settles a row against ours, the matched rows are
    /// paired the same way and each such row gets a [`ROW_PATH`] entry. When anything
    /// cannot be settled, the pages stay as they were. The note says why, with the reads.
    /// `op`: the document, for our longer read (I-1) and the filter's variables; without it
    /// a boundary row is not excused.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn gql_window_referee(
        &self,
        ta: &Target,
        op: Option<&GqlOp>,
        vars: &Map<String, Value>,
        a: &mut Answer,
        b: &mut Answer,
        max: u64,
        root_scoped: bool,
        st: &mut Reads,
    ) -> (Vec<DiffEntry>, Option<String>) {
        if root_scoped {
            return (Vec::new(), None);
        }
        let conn = |ans: &mut Answer| {
            ans.body
                .get_mut("data")
                .and_then(crate::graphql::root_connection)
                .cloned()
        };
        let (Some(ca), Some(cb)) = (conn(a), conn(b)) else {
            return (Vec::new(), None);
        };
        // The same rows in the same order (after the tie rules): nothing to referee.
        let norm = |c: &Value| {
            let mut v = json!({"edges": c.get("edges").cloned().unwrap_or(Value::Null)});
            crate::graphql::normalize_gql(&mut v, u64::MAX);
            v
        };
        if norm(&ca) == norm(&cb) {
            return (Vec::new(), None);
        }
        let query = op.map(|o| o.query.as_str());
        let first_var = arg_var(query, "first");
        // R-P5P9-64 I-3, R-P5P9-67 I-A (a): a cut page of ours shorter than first × the pages
        // fetched (a follow page that failed, or a short page) is not a window, and a diff:
        // never the common prefix. The one exception is ours' Q10 cap, a page of exactly
        // min(first, 100) × pages.
        let first = vars.get(&first_var).and_then(Value::as_u64);
        let pages = a.ms.len() as u64;
        let rows_a = ca
            .get("edges")
            .and_then(Value::as_array)
            .map_or(0, Vec::len) as u64;
        if let (true, Some(f)) = (more(&ca) && pages > 0, first) {
            if rows_a < f * pages && rows_a != f.min(OURS_PAGE_CAP) * pages {
                let note = format!(
                    "H7 not explained: our cut page holds {rows_a} rows, fewer than first {f} × \
                     {pages} pages; a short page is not a window"
                );
                let ev = json!({"rows": rows_a, "first": f, "pages": pages});
                return (vec![entry(SHORT_PAGE_PATH, Some(ev), None)], Some(note));
            }
        }
        let prep = match prepare(&ca, &cb) {
            Ok(p) => p,
            Err(why) => {
                let rows = |c: &Value| {
                    c.get("edges")
                        .and_then(Value::as_array)
                        .is_some_and(|e| e.iter().any(|e| row_id(e).kind.is_some()))
                };
                let note = (rows(&ca) || rows(&cb)).then(|| format!("H7 not applied: {why}"));
                return (Vec::new(), note);
            }
        };
        let root = a.body.get("data").and_then(root_name);
        let asc = root.as_deref() == Some("blocksFromHeight");
        let filter = Filter::of(query, root.as_deref().or(Some("")), vars);
        let (node0, ours0) = (st.node, st.ours);
        let mut va: Vec<Option<Verdict>> = vec![None; prep.a.len()];
        let mut vb: Vec<Option<Verdict>> = vec![None; prep.b.len()];
        for (side, i) in prep.reads() {
            let r = if side == 0 { &prep.a[i] } else { &prep.b[i] };
            let v = self.h7_verdict(ta, r, side == 1, st).await;
            if side == 0 {
                va[i] = Some(v);
            } else {
                vb[i] = Some(v);
            }
        }
        let ext;
        let mut w = Window {
            max,
            asc,
            filter: &filter,
            prefix: None,
        };
        let mut d = decide(&prep, &va, &vb, &w);
        if d.needs_prefix {
            ext = match op {
                Some(op) => {
                    self.h7_prefix(ta, op, vars, &first_var, prep.a.len(), st)
                        .await
                }
                None => Err("no document to read it with".into()),
            };
            w.prefix = Some(&ext);
            d = decide(&prep, &va, &vb, &w);
        }
        let (nr, or) = (st.node - node0, st.ours - ours0);
        let reads = format!("{nr} node reads, {or} reads of ours");
        let ev_a = json!({"rows": prep.a.len(), "cut": prep.a_cut, "boundaryHeight": d.lowest,
            "ascending": asc, "oneSided": d.ours});
        let ev_b = json!({"rows": prep.b.len(), "cut": prep.b_cut,
            "lastCreationTime": d.oracle_lowest_ctime, "nodeReads": nr,
            "oursReads": or, "oneSided": d.oracle});
        if !d.undecided.is_empty() || !d.grouped {
            let mut out = Vec::new();
            if !d.undecided.is_empty() {
                let ev = json!({"undecided": d.undecided, "reads": reads});
                out.push(entry(UNDECIDED_PATH, Some(ev), None));
            }
            let mut whys = d.undecided.clone();
            for (ra, rb, why) in &d.rejected {
                out.push(entry(
                    ROW_PATH,
                    Some(json!({"row": ra, "why": why})),
                    Some(json!({"row": rb, "why": why})),
                ));
                whys.push(why.clone());
            }
            let note = format!("H7 not explained ({reads}): {}", whys.join("; "));
            return (out, Some(note));
        }
        let tag = |e: &Value, n: usize| {
            let mut m = Map::new();
            m.insert(ALIGN_KEY.into(), json!(format!("{n:06}")));
            if let Some(o) = e.as_object() {
                m.extend(o.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
            Value::Object(m)
        };
        let ea: Vec<Value> = d
            .pairs
            .iter()
            .enumerate()
            .map(|(n, (i, _))| tag(&prep.a[*i].edge, n))
            .collect();
        let eb: Vec<Value> = d
            .pairs
            .iter()
            .enumerate()
            .map(|(n, (_, j))| tag(&prep.b[*j].edge, n))
            .collect();
        for (ans, edges) in [(a, ea), (b, eb)] {
            if let Some(c) = ans
                .body
                .get_mut("data")
                .and_then(crate::graphql::root_connection)
            {
                c["edges"] = Value::Array(edges);
            }
        }
        let mut out = Vec::new();
        if d.above > 0 {
            out.push(entry(crate::graphql::ORACLE_STALL_PATH, None, None));
        }
        if d.rejected.is_empty() {
            out.push(entry(WINDOW_PATH, Some(ev_a), Some(ev_b)));
            return (out, None);
        }
        let mut whys = Vec::new();
        for (ra, rb, why) in d.rejected {
            out.push(entry(
                ROW_PATH,
                Some(json!({"row": ra, "why": why})),
                Some(json!({"row": rb, "why": why})),
            ));
            whys.push(why);
        }
        let note = format!(
            "H7 not explained ({reads}): {}; evidence: ours {ev_a}, oracle {ev_b}",
            whys.join("; ")
        );
        (out, Some(note))
    }

    /// R-P5P9-64 (I-1): one read of ours with the same document and variables, `first` = the
    /// rows on our page + [`PREFIX_EXTRA`], one page with no `after`: each row's identity key
    /// and node.
    async fn h7_prefix(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        first_var: &str,
        n: usize,
        st: &mut Reads,
    ) -> Result<Longer, String> {
        if !vars.get(first_var).is_some_and(Value::is_u64) {
            return Err(format!("no page size ({first_var}) to read it with"));
        }
        let mut v = vars.clone();
        v.insert(first_var.to_string(), json!(n + PREFIX_EXTRA));
        v.remove(&arg_var(Some(&op.query), "after"));
        st.ours += 1;
        let mut data = self
            .gql_read(ta, &op.query, Value::Object(v))
            .await
            .ok_or("the read failed")?;
        let conn = crate::graphql::root_connection(&mut data).ok_or("the read holds no list")?;
        let more = conn
            .pointer("/pageInfo/hasNextPage")
            .and_then(Value::as_bool);
        let edges = conn
            .get("edges")
            .and_then(Value::as_array)
            .ok_or("the read holds no list")?;
        let rows: Vec<(String, Value)> = edges
            .iter()
            .map(|e| {
                (
                    row_id(e).key().unwrap_or_default(),
                    e.get("node").cloned().unwrap_or(Value::Null),
                )
            })
            .collect();
        // R-P5P9-67 m-6: fewer rows than asked, and a next page (or no word on one): capped.
        let capped = rows.len() < n + PREFIX_EXTRA && more != Some(false);
        Ok(Longer { rows, capped })
    }

    /// One row's node verdict (single-row reads, cached per compare). `oracle`: the row is
    /// the oracle's.
    async fn h7_verdict(&self, ta: &Target, r: &Row, oracle: bool, st: &mut Reads) -> Verdict {
        let chain = match r.id.chain {
            Some(c) => Some(c),
            None => self.h7_chain(ta, &r.id, st).await,
        };
        let Some(chain) = chain else {
            return Verdict::unknown("no chain for the row");
        };
        let mut v = self.h7_verdict_on(chain, r, oracle, st).await;
        v.chain = Some(chain);
        v
    }

    async fn h7_verdict_on(&self, chain: u8, r: &Row, oracle: bool, st: &mut Reads) -> Verdict {
        let id = &r.id;
        let n = r.edge.get("node").unwrap_or(&Value::Null);
        if id.kind == Some(Kind::Block) {
            let Some(h) = id.height else {
                return Verdict::unknown("a block row without a height");
            };
            return match self.h7_canonical(chain, h, st).await {
                Ok(hd) => block_verdict(id, n, hd.as_ref(), oracle),
                Err(w) => Verdict::unknown(w),
            };
        }
        let Some(rk) = id.rk.clone() else {
            return Verdict::unknown("no request key");
        };
        let polled = match self.h7_poll(chain, &rk, st).await {
            Ok(p) => p,
            Err(w) => return Verdict::unknown(w),
        };
        if let Some(p) = polled {
            // A row in another block than the node's, without a height: the node's height of
            // its block decides whether that block is an orphan at the key's height.
            let pb = p.pointer("/metaData/blockHash").and_then(Value::as_str);
            let bh = match (&id.block, id.height) {
                (Some(b), None) if pb != Some(b.as_str()) => {
                    match self.h7_block_height(chain, b, st).await {
                        Ok(h) => h,
                        Err(w) => return Verdict::unknown(w),
                    }
                }
                _ => None,
            };
            return poll_verdict(id, n, &p, oracle, bh);
        }
        // Unknown to /poll: a coinbase row, settled by the node's block at its height (its
        // own, or the node's height of its block: R-P5P9-63 (5)).
        let h = match (id.height, &id.block) {
            (Some(h), _) => h,
            (None, Some(b)) => match self.h7_block_height(chain, b, st).await {
                Ok(Some(h)) => h,
                Ok(None) => return Verdict::unknown("header: the node does not know the block"),
                Err(w) => return Verdict::unknown(w),
            },
            (None, None) => {
                return Verdict::unknown(
                    "poll: key unknown, and no height or block to read a coinbase at",
                )
            }
        };
        let hd = match self.h7_canonical(chain, h, st).await {
            Ok(hd) => hd,
            Err(w) => return Verdict::unknown(w),
        };
        let mut payload = None;
        if let Some(ph) = hd
            .as_ref()
            .and_then(|x| x.get("payloadHash"))
            .and_then(Value::as_str)
        {
            match self.h7_payload(chain, ph, st).await {
                Ok(p) => payload = Some(p),
                Err(w) => return Verdict::unknown(w),
            }
        }
        coinbase_verdict(id, n, h, hd.as_ref(), payload.as_ref(), oracle)
    }

    /// Takes one node read from the compare's cap and the run's budget.
    pub(crate) fn h7_take(&self, st: &mut Reads) -> Result<(), String> {
        if st.node >= NODE_READS {
            return Err(format!(
                "the cap of {NODE_READS} node reads per compare is reached"
            ));
        }
        if !self.gql.take_node_read() {
            return Err("the run's node read cap is reached".into());
        }
        st.node += 1;
        self.count_request(ROUTE, "node", 1);
        Ok(())
    }

    async fn h7_poll(&self, chain: u8, rk: &str, st: &mut Reads) -> Result<Option<Value>, String> {
        let key = (chain, rk.to_string());
        if let Some(v) = st.polls.get(&key) {
            return v.clone();
        }
        self.h7_take(st)?;
        let r = self
            .node
            .poll(chain, std::slice::from_ref(&key.1))
            .await
            .map(|m| m.get(rk).cloned())
            .map_err(|e| format!("poll failed: {e}"));
        st.polls.insert(key, r.clone());
        r
    }

    /// R-P5P9-67 m-5: the node's canonical header at `(chain, h)`, on the branch of the
    /// node's cut (read once per compare); `None` when the branch holds none there (above
    /// the cut). Never the `/header` range by height, which also holds orphans.
    async fn h7_canonical(
        &self,
        chain: u8,
        h: u64,
        st: &mut Reads,
    ) -> Result<Option<Value>, String> {
        if let Some(v) = st.canonical.get(&(chain, h)) {
            return v.clone();
        }
        if st.cut.is_none() {
            let cut = match self.h7_take(st) {
                Ok(()) => self
                    .node
                    .cut()
                    .await
                    .map(|c| c.hashes.into_iter().map(|(c, e)| (c, e.hash)).collect())
                    .map_err(|e| format!("cut read failed: {e}")),
                Err(w) => Err(w),
            };
            st.cut = Some(cut);
        }
        let upper = match st.cut.as_ref() {
            Some(Ok(m)) => m
                .get(&chain)
                .cloned()
                .ok_or_else(|| format!("the node's cut has no chain {chain}"))?,
            Some(Err(w)) => return Err(w.clone()),
            None => return Err("no cut".into()),
        };
        self.h7_take(st)?;
        let r = match self
            .node
            .header_branch(chain, &[], &[upper], Some(h), Some(h))
            .await
        {
            Err(e) => Err(format!("branch read failed: {e}")),
            Ok(p) => match p.items.as_slice() {
                [] => Ok(None),
                [x] if x.get("height").and_then(Value::as_u64) == Some(h) => Ok(Some(x.clone())),
                [_] => Err("the cut's branch answered a header at another height".into()),
                xs => Err(format!(
                    "the cut's branch holds {} headers at the height",
                    xs.len()
                )),
            },
        };
        st.canonical.insert((chain, h), r.clone());
        r
    }

    /// Every header the node stores at `(chain, h)` (orphans included): for a block's payload
    /// hash by its block hash only, never for canonicity.
    async fn h7_headers(&self, chain: u8, h: u64, st: &mut Reads) -> Result<Vec<Value>, String> {
        if let Some(v) = st.headers.get(&(chain, h)) {
            return v.clone();
        }
        self.h7_take(st)?;
        let r = self
            .node
            .headers(chain, h, h, Some(5), None)
            .await
            .map(|p| p.items)
            .map_err(|e| format!("header read failed: {e}"));
        st.headers.insert((chain, h), r.clone());
        r
    }

    async fn h7_payload(&self, chain: u8, ph: &str, st: &mut Reads) -> Result<Value, String> {
        if let Some(v) = st.payloads.get(ph) {
            return v.clone();
        }
        self.h7_take(st)?;
        let r = self
            .node
            .payload_with_outputs(chain, ph)
            .await
            .map_err(|e| format!("payload read failed: {e}"));
        st.payloads.insert(ph.to_string(), r.clone());
        r
    }

    /// R-P5P9-63 (5): the node's height of block `hash`, by the node's header-by-hash route
    /// (`GET /chain/{c}/header/{hash}`, object encoding); `None` when the node does not know
    /// the block. The node client has no such call, so the harness reads it with its own
    /// client at [`crate::config::HEADER_RPS`], one read in flight; the node client runs at
    /// `node_max_rps` less that, so the two stay within `node_max_rps` (R-P5P9-65 (2)).
    pub(crate) async fn h7_block_height(
        &self,
        chain: u8,
        hash: &str,
        st: &mut Reads,
    ) -> Result<Option<u64>, String> {
        if let Some(v) = st.by_hash.get(hash) {
            return v.clone();
        }
        self.h7_take(st)?;
        let (client, limiter) = self.gql.header_reader(self.cfg.node.timeout_ms);
        let path = format!(
            "chainweb/0.0/{}/chain/{chain}/header/{hash}",
            self.cfg.network
        );
        let r = async {
            let url = self
                .node
                .base()
                .join(&path)
                .map_err(|e| format!("header url: {e}"))?;
            let _slot = limiter.slot().await;
            limiter.acquire().await;
            let resp = client
                .get(url)
                .header("Accept", "application/json;blockheader-encoding=object")
                .send()
                .await
                .map_err(|e| format!("header read failed: {e}"))?;
            match resp.status().as_u16() {
                404 => Ok(None),
                200 => {
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| format!("header read failed: {e}"))?;
                    let v: Value = serde_json::from_slice(&body)
                        .map_err(|e| format!("header is not JSON: {e}"))?;
                    Ok(v.get("height").and_then(Value::as_u64))
                }
                s => Err(format!("header read failed: HTTP {s}")),
            }
        }
        .await;
        st.by_hash.insert(hash.to_string(), r.clone());
        r
    }

    /// The chain of a row that selects none: ours' block for its block hash, else ours'
    /// transaction for its request key (one read of ours each, cached per block).
    async fn h7_chain(&self, ta: &Target, id: &RowId, st: &mut Reads) -> Option<u8> {
        let parse = |v: Option<&Value>| v.and_then(text).and_then(|c| c.parse::<u8>().ok());
        if let Some(b) = &id.block {
            if let Some(c) = st.chains.get(b) {
                return *c;
            }
            st.ours += 1;
            let q = "query($h: String!) { block(hash: $h) { chainId } }";
            let c = self
                .gql_read(ta, q, json!({ "h": b }))
                .await
                .and_then(|d| parse(d.pointer("/block/chainId")));
            st.chains.insert(b.clone(), c);
            if c.is_some() {
                return c;
            }
        }
        let rk = id.rk.as_ref()?;
        st.ours += 1;
        let q = "query($k: String!) { transaction(requestKey: $k) { cmd { meta { chainId } } } }";
        self.gql_read(ta, q, json!({ "k": rk }))
            .await
            .and_then(|d| parse(d.pointer("/transaction/cmd/meta/chainId")))
    }
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-62 H8: `Transfer.orderIndex`

/// Request keys H8 reads (one node `/poll` each) per compare at most.
pub const ORDER_INDEX_READS: usize = 20;

/// Whether two transfer rows hold the same transfer: every field both select is equal
/// (the amount by value, or the oracle's f64-rounded form).
pub fn same_transfer(x: &Value, y: &Value) -> bool {
    let s = |n: &Value, k: &str| n.get(k).and_then(text);
    [
        "senderAccount",
        "receiverAccount",
        "moduleName",
        "chainId",
        "requestKey",
    ]
    .iter()
    .all(|k| match (s(x, k), s(y, k)) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }) && match (x.get("amount"), y.get("amount")) {
        (Some(a), Some(b)) => same_amount(a, b, true),
        _ => true,
    }
}

/// H8 on one request key against the node's events of its transaction (R-P5P9-64 M-4): the
/// oracle's value r of a row is the rank of a TRANSFER event among the node's TRANSFER events
/// (event order); that event must have the row's content, and the row must select its amount
/// and an account (with no content selected there is no remap). Returns (the oracle's value,
/// the node's event index, which is ours' numbering) per oracle row, else why not.
pub fn node_rank_map(
    events: &[Value],
    oracle: &[(i64, &Value)],
) -> Result<Vec<(i64, i64)>, String> {
    let xs: Vec<(usize, &Value)> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.get("name").and_then(Value::as_str) == Some("TRANSFER"))
        .collect();
    let mut out = Vec::new();
    for (r, row) in oracle {
        let content = sel(row, "amount").is_some()
            && (sel(row, "senderAccount").is_some() || sel(row, "receiverAccount").is_some());
        if !content {
            return Err("the row selects no amount and account to rank it by".into());
        }
        let Some(&(i, ev)) = usize::try_from(*r).ok().and_then(|u| xs.get(u)) else {
            return Err(format!(
                "the oracle's {r} is past the node's {} TRANSFER events",
                xs.len()
            ));
        };
        if !transfer_matches(row, ev, true) {
            return Err(format!(
                "the oracle's {r} is not the rank of the node's TRANSFER event with its content"
            ));
        }
        out.push((*r, i as i64));
    }
    Ok(out)
}

/// The same JSON type as `like`: a string for a string, else a number.
fn like(like: &Value, i: i64) -> Value {
    match like {
        Value::String(_) => json!(i.to_string()),
        _ => json!(i),
    }
}

/// Puts our index `i` in an oracle transfer row: its `orderIndex` and the index inside its
/// relay id (the other id fields stay the oracle's, so a difference there still shows).
pub fn reindex_transfer(node: &mut Value, i: i64) {
    if let Some(o) = node.get_mut("orderIndex") {
        *o = like(o, i);
    }
    let Some((t, k)) = node.get("id").and_then(Value::as_str).and_then(decode_id) else {
        return;
    };
    let Ok(mut parts) = serde_json::from_str::<Vec<Value>>(&k) else {
        return;
    };
    if t == "Transfer" && parts.len() == 5 {
        parts[2] = like(&parts[2], i);
        let id = format!("Transfer:{}", Value::Array(parts));
        node["id"] = json!(crate::graphql::base64_std(id.as_bytes()));
    }
}

impl Ctx {
    /// R-P5P9-62 H8 on the root list of an oracle pair: for each request key with an oracle
    /// transfer row that ours does not hold at the same identity with the same content (at
    /// most [`ORDER_INDEX_READS`]), one node `/poll` (R-P5P9-64 M-4). When every oracle value
    /// of that key is the rank of the node's TRANSFER event with its content, the oracle's
    /// rows take that event's index (`orderIndex` and the relay id), which is ours' numbering;
    /// H7 or the diff then checks ours against it. The hit lists each key's indexes. Anything
    /// else leaves the rows as they were.
    pub(crate) async fn gql_transfer_order(
        &self,
        ta: &Target,
        a: &Answer,
        b: &mut Answer,
        st: &mut Reads,
    ) -> Option<DiffEntry> {
        let mut da = a.body.get("data")?.clone();
        let ca = crate::graphql::root_connection(&mut da)?;
        let transfer = |e: &Value| {
            let id = row_id(e);
            (id.kind == Some(Kind::Transfer) && id.index.is_some()).then_some(id)
        };
        let ours: HashMap<String, Value> = ca
            .get("edges")?
            .as_array()?
            .iter()
            .filter_map(|e| Some((transfer(e)?.key()?, e.get("node")?.clone())))
            .collect();
        let cb = b
            .body
            .get_mut("data")
            .and_then(crate::graphql::root_connection)?;
        let edges = cb.get_mut("edges")?.as_array_mut()?;
        let mut keys: Vec<(String, RowId)> = Vec::new();
        for e in edges.iter() {
            let Some(id) = transfer(e) else { continue };
            let (Some(k), Some(rk)) = (id.key(), id.rk.clone()) else {
                continue;
            };
            let held = ours
                .get(&k)
                .is_some_and(|n| same_transfer(n, e.get("node").unwrap_or(&Value::Null)));
            if !held && !keys.iter().any(|(x, _)| *x == rk) {
                keys.push((rk, id));
            }
        }
        let mut evidence = Vec::new();
        let mut reads = 0;
        for (rk, id) in keys.into_iter().take(ORDER_INDEX_READS) {
            let chain = match id.chain {
                Some(c) => Some(c),
                None => self.h7_chain(ta, &id, st).await,
            };
            let Some(chain) = chain else { continue };
            reads += 1;
            let Ok(Some(p)) = self.h7_poll(chain, &rk, st).await else {
                continue;
            };
            let events: Vec<Value> = p
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let theirs: Vec<(usize, i64)> = edges
                .iter()
                .enumerate()
                .filter_map(|(j, e)| {
                    let id = transfer(e)?;
                    (id.rk.as_deref() == Some(rk.as_str())).then_some((j, id.index?))
                })
                .collect();
            let rows: Vec<(i64, &Value)> = theirs
                .iter()
                .map(|(j, r)| (*r, edges[*j].get("node").unwrap_or(&Value::Null)))
                .collect();
            match node_rank_map(&events, &rows) {
                Ok(map) => {
                    for ((j, _), (_, i)) in theirs.iter().zip(&map) {
                        if let Some(n) = edges[*j].get_mut("node") {
                            reindex_transfer(n, *i);
                        }
                    }
                    let node: Vec<usize> = events
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| e.get("name").and_then(Value::as_str) == Some("TRANSFER"))
                        .map(|(i, _)| i)
                        .collect();
                    evidence.push(json!({"requestKey": rk, "chain": chain,
                        "nodeTransferIndexes": node,
                        "oracle": map.iter().map(|x| x.0).collect::<Vec<_>>(),
                        "mapped": map.iter().map(|x| x.1).collect::<Vec<_>>()}));
                }
                Err(why) => tracing::debug!(rk, why, "H8 not explained"),
            }
        }
        (!evidence.is_empty()).then(|| {
            entry(
                ORDER_INDEX_PATH,
                Some(json!({"nodePolls": reads})),
                Some(Value::Array(evidence)),
            )
        })
    }
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-62 `payload.data`

/// Transactions the `payload.data` matcher reads the node for per compare at most.
pub const PAYLOAD_DATA_CHECKS: usize = 5;

/// The transactions in an answer that select `cmd.payload.data`: (request key, chain if
/// selected, JSON pointer to `data`).
pub fn payload_data_txs(v: &Value) -> Vec<(String, Option<u8>, String)> {
    fn walk(v: &Value, ptr: &mut String, out: &mut Vec<(String, Option<u8>, String)>) {
        match v {
            Value::Object(m) => {
                let data = m
                    .get("cmd")
                    .and_then(|c| c.get("payload"))
                    .is_some_and(|p| p.get("data").is_some());
                let rk = m
                    .get("hash")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        m.get("id")
                            .and_then(Value::as_str)
                            .and_then(crate::graphql_norm::tx_id_block)
                            .map(|x| x.1)
                    });
                if let (true, Some(rk)) = (data, rk) {
                    let chain = ["/cmd/meta/chainId", "/result/block/chainId"]
                        .iter()
                        .find_map(|q| v.pointer(q))
                        .and_then(text)
                        .and_then(|c| c.parse().ok());
                    out.push((rk, chain, format!("{ptr}/cmd/payload/data")));
                }
                for (k, x) in m {
                    let n = ptr.len();
                    ptr.push('/');
                    ptr.push_str(&k.replace('~', "~0").replace('/', "~1"));
                    walk(x, ptr, out);
                    ptr.truncate(n);
                }
            }
            Value::Array(xs) => {
                for (i, x) in xs.iter().enumerate() {
                    let n = ptr.len();
                    ptr.push_str(&format!("/{i}"));
                    walk(x, ptr, out);
                    ptr.truncate(n);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(v, &mut String::new(), &mut out);
    out
}

/// The node's command for `rk` (decoded) from its block's payload with outputs.
pub fn node_cmd(payload: &Value, rk: &str) -> Option<Value> {
    payload
        .get("transactions")?
        .as_array()?
        .iter()
        .find_map(|t| {
            let raw = kdx_types::hash::b64url_decode(t.get(0)?.as_str()?).ok()?;
            let w: Value = serde_json::from_slice(&raw).ok()?;
            if w.get("hash")?.as_str()? != rk {
                return None;
            }
            serde_json::from_str(w.get("cmd")?.as_str()?).ok()
        })
}

impl Ctx {
    /// R-P5P9-62 `payload.data` (rule (a), exact): where ours sends `"null"` and the oracle
    /// `"{}"` for the same request key, and the node's command for that key is a
    /// continuation whose `cont.data` is null (at most [`PAYLOAD_DATA_CHECKS`] keys, single-row
    /// reads), the oracle's value takes ours. Anything else stays as it was.
    pub(crate) async fn gql_payload_data(
        &self,
        vars: &Map<String, Value>,
        a: &Answer,
        b: &mut Answer,
        st: &mut Reads,
    ) -> Option<DiffEntry> {
        let ours = payload_data_txs(&a.body);
        let theirs = payload_data_txs(&b.body);
        let var_chain = vars
            .get("chainId")
            .and_then(text)
            .and_then(|c| c.parse::<u8>().ok());
        let node0 = st.node;
        let mut checked: HashMap<String, bool> = HashMap::new();
        let mut evidence = Vec::new();
        for (rk, chain_b, ptr_b) in theirs {
            if b.body.pointer(&ptr_b) != Some(&json!("{}")) {
                continue;
            }
            let Some(chain_a) = ours
                .iter()
                .find(|(k, _, p)| *k == rk && a.body.pointer(p) == Some(&json!("null")))
                .map(|x| x.1)
            else {
                continue;
            };
            let ok = match checked.get(&rk) {
                Some(x) => *x,
                None if checked.len() >= PAYLOAD_DATA_CHECKS => continue,
                None => {
                    let chain = chain_a.or(chain_b).or(var_chain);
                    let found = match chain {
                        Some(c) => self.node_cont_data_null(c, &rk, st).await,
                        None => Err("no chain".into()),
                    };
                    match &found {
                        Ok(true) => evidence.push(json!({"requestKey": rk, "chain": chain,
                            "nodeCont": true, "nodeContData": null})),
                        other => tracing::debug!(rk, ?other, "payload.data not explained"),
                    }
                    let x = found == Ok(true);
                    checked.insert(rk.clone(), x);
                    x
                }
            };
            if ok {
                if let Some(v) = b.body.pointer_mut(&ptr_b) {
                    *v = json!("null");
                }
            }
        }
        (!evidence.is_empty()).then(|| {
            entry(
                PAYLOAD_DATA_PATH,
                Some(json!({"nodeReads": st.node - node0})),
                Some(Value::Array(evidence)),
            )
        })
    }

    /// Whether the node's command for `rk` on `chain` is a continuation with `cont.data`
    /// null: `/poll` for its block, the headers at its height, that block's payload.
    async fn node_cont_data_null(
        &self,
        chain: u8,
        rk: &str,
        st: &mut Reads,
    ) -> Result<bool, String> {
        let p = self
            .h7_poll(chain, rk, st)
            .await?
            .ok_or("poll: key unknown")?;
        let block = p
            .pointer("/metaData/blockHash")
            .and_then(Value::as_str)
            .ok_or("poll: no block")?;
        let h = p
            .pointer("/metaData/blockHeight")
            .and_then(Value::as_u64)
            .ok_or("poll: no height")?;
        let hs = self.h7_headers(chain, h, st).await?;
        let ph = hs
            .iter()
            .find(|x| x.get("hash").and_then(Value::as_str) == Some(block))
            .and_then(|x| x.get("payloadHash").and_then(Value::as_str))
            .ok_or("no header for the block")?
            .to_string();
        let payload = self.h7_payload(chain, &ph, st).await?;
        let cmd = node_cmd(&payload, rk).ok_or("the block's payload lacks the key")?;
        Ok(cmd.pointer("/payload/cont/data") == Some(&Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b64(s: &str) -> String {
        crate::graphql::base64_std(s.as_bytes())
    }

    #[test]
    fn row_ids_from_fields_and_relay_ids() {
        // A transfer row with only its relay id: block, chain, index and request key.
        let e = json!({"node": {"id": b64(r#"Transfer:["B1","3","5","mh","rk1"]"#),
            "amount": "1.5"}});
        let r = row_id(&e);
        assert_eq!(
            (
                r.kind,
                r.chain,
                r.block.as_deref(),
                r.index,
                r.rk.as_deref()
            ),
            (
                Some(Kind::Transfer),
                Some(3),
                Some("B1"),
                Some(5),
                Some("rk1")
            )
        );
        // A selected orderIndex wins over the id's.
        let e = json!({"node": {"id": b64(r#"Event:["B1","2","rk1"]"#), "orderIndex": 4,
            "qualifiedName": "coin.TRANSFER", "block": {"hash": "B1", "height": 9}}});
        let r = row_id(&e);
        assert_eq!(
            (r.kind, r.index, r.height),
            (Some(Kind::Event), Some(4), Some(9))
        );
        // A tx row: request key from `hash`, block from the id; a block row: its hash.
        let e = json!({"node": {"id": b64(r#"Transaction:["B2","rk2"]"#), "hash": "rk2"}});
        let r = row_id(&e);
        assert_eq!((r.kind, r.block.as_deref()), (Some(Kind::Tx), Some("B2")));
        let e = json!({"node": {"hash": "B3", "height": 7, "chainId": 1}});
        assert_eq!(row_id(&e).key().unwrap(), "block|1|B3|7|-|-");
        // No kind, or no request key: undecidable.
        assert!(row_id(&json!({"node": {"balance": 1}})).key().is_none());
        assert!(row_id(&json!({"node": {"amount": 1}})).key().is_none());
    }

    #[test]
    fn node_verdicts_by_poll_coinbase_and_header() {
        let n = json!({"senderAccount": "s", "receiverAccount": "r", "amount": 1.5});
        let id = |block: Option<&str>, h: Option<u64>, index: Option<i64>| RowId {
            kind: Some(Kind::Transfer),
            chain: Some(0),
            block: block.map(str::to_string),
            height: h,
            rk: Some("k".into()),
            index,
            ctime: None,
        };
        let poll = |b: &str, h: u64, evs: Value| json!({"metaData": {"blockHash": b, "blockHeight": h}, "events": evs});
        let xfer = json!({"name": "TRANSFER", "params": ["s", "r", {"decimal": "1.50"}]});
        let other = json!({"name": "X", "params": []});
        let p = poll("B", 9, json!([other, xfer]));
        assert_eq!(
            poll_verdict(&id(Some("B"), None, Some(1)), &n, &p, false, None).canon,
            Canon::Yes
        );
        assert_eq!(
            poll_verdict(&id(Some("C"), None, Some(1)), &n, &p, false, None).canon,
            Canon::No
        );
        // The row's index points at another event: not the node's numbering, unknown.
        assert_eq!(
            poll_verdict(&id(None, Some(9), Some(0)), &n, &p, false, None).canon,
            Canon::Unknown
        );
        let p2 = poll("B", 9, json!([other]));
        assert_eq!(
            poll_verdict(&id(None, Some(9), None), &n, &p2, false, None).canon,
            Canon::No
        );
        // A coinbase row: the only header at its height and its coinbase request key.
        let cb = |rk: &str| {
            let t = json!({"reqKey": rk, "events": [xfer]}).to_string();
            json!({"coinbase": kdx_types::hash::b64url_encode(t.as_bytes())})
        };
        let hd = json!({"hash": "B", "payloadHash": "P"});
        let r = id(None, Some(9), Some(0));
        assert_eq!(
            coinbase_verdict(&r, &n, 9, Some(&hd), Some(&cb("k")), false).canon,
            Canon::Yes
        );
        assert_eq!(
            coinbase_verdict(&r, &n, 9, Some(&hd), Some(&cb("j")), false).canon,
            Canon::No
        );
        assert_eq!(
            coinbase_verdict(&r, &n, 9, None, None, false).canon,
            Canon::Unknown
        );
        // A block row: the canonical header at the height (the cut's branch).
        let mut b = id(Some("B"), Some(9), None);
        b.kind = Some(Kind::Block);
        let row = json!({"hash": "B", "payloadHash": "P"});
        assert_eq!(block_verdict(&b, &row, Some(&hd), false).canon, Canon::Yes);
        assert_eq!(block_verdict(&b, &row, None, false).canon, Canon::Unknown);
        let f = json!({"hash": "F"});
        assert_eq!(block_verdict(&b, &row, Some(&f), true).canon, Canon::No);
        let other = json!({"hash": "B", "payloadHash": "Q"});
        assert_eq!(block_verdict(&b, &other, Some(&hd), false).canon, Canon::No);
        assert_eq!(
            block_verdict(&b, &other, Some(&hd), true).canon,
            Canon::Unknown
        );
    }

    #[test]
    fn utc_times_parse_to_seconds_and_nanos() {
        assert_eq!(parse_utc("1970-01-01T00:00:00Z"), Some((0, 0, false)));
        assert_eq!(
            parse_utc("2024-03-01T00:00:01.290Z"),
            Some((1_709_251_201, 290_000_000, false))
        );
        assert_eq!(
            parse_utc("2024-03-01T00:00:01.5000000001Z").map(|t| t.2),
            Some(true)
        );
        assert!(parse_utc("2024-03-01 00:00:01Z").is_none());
        assert!(parse_utc("2024-13-01T00:00:01Z").is_none());
    }
}
