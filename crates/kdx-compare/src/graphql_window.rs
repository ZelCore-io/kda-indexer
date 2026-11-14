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
//!   those key fields. The matched rows then compare field by field with the ruled matchers;
//! - an oracle-only row is non-canonical on the node (orphan), an extra copy of a row the
//!   node holds once (duplicate), or canonical and outside our window: below our lowest
//!   height, or at it while our page is cut (R3E-8);
//! - an ours-only row is canonical on the node. It is then outside the oracle's window (at
//!   or below its lowest `creationTime` while its page is cut) or a node-confirmed oracle gap;
//! - ours is in descending height order (our sort key).
//!
//! Every one-sided row gets a single-row node read: `/poll` for its request key (the row's
//! block or height, and for a transfer or event the node's event at its index). A row
//! `/poll` does not know (a coinbase row) is settled by the node's only header at its height
//! and that block's coinbase request key; a block row by the node's headers at its height.
//! A failed read, a spent read cap or a row whose identity cannot be decided leave the pair
//! as it was (unexplained), with the reason in the record's note. A row H7 cannot explain
//! gets its own entry, `/~graphql-h7-row`, which no allowlist entry matches.
//!
//! The pure parts are here; the reads are in [`Ctx::gql_window_referee`].

use std::collections::HashMap;

use serde_json::{json, Map, Value};

use crate::graphql::Answer;
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
/// Node reads per compare at most (single-row reads, both sides together).
pub const NODE_READS: usize = 100;
/// Visible hit of H8 (allowlist id `graphql-h8-transfer-order-index`); it carries the
/// evidence per request key.
pub const ORDER_INDEX_PATH: &str = "/~graphql-h8-transfer-order-index";
/// Visible hit of the `payload.data` matcher (allowlist id `graphql-r62-payload-data`).
pub const PAYLOAD_DATA_PATH: &str = "/~graphql-r62-payload-data";
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
}

impl Verdict {
    pub fn unknown(why: impl Into<String>) -> Verdict {
        Verdict {
            canon: Canon::Unknown,
            height: None,
            block: None,
            why: why.into(),
        }
    }

    fn evidence(&self) -> Value {
        json!({"node": self.canon.as_str(), "nodeHeight": self.height,
            "nodeBlock": self.block, "via": self.why})
    }
}

/// A Pact value's text (`{"decimal": …}` and `{"int": …}` unwrapped).
fn pact_text(v: &Value) -> Option<String> {
    match v {
        Value::Object(m) => m.get("decimal").or_else(|| m.get("int")).and_then(text),
        _ => text(v),
    }
}

/// The same amount: equal decimal values, or the same f64 (the oracle's f64-rounded
/// amounts, R-P5P9-60 amendment).
pub fn same_amount(a: &Value, b: &Value) -> bool {
    let (Some(x), Some(y)) = (pact_text(a), pact_text(b)) else {
        return false;
    };
    let dc = crate::diff::decimal_canon;
    if matches!((dc(&x), dc(&y)), (Some(p), Some(q)) if p == q) {
        return true;
    }
    matches!((x.parse::<f64>(), y.parse::<f64>()), (Ok(p), Ok(q)) if p.is_finite() && p == q)
}

fn module_name(ev: &Value) -> Option<String> {
    let m = ev.get("module")?;
    let name = m.get("name")?.as_str()?;
    Some(match m.get("namespace").and_then(Value::as_str) {
        Some(ns) => format!("{ns}.{name}"),
        None => name.to_string(),
    })
}

/// Whether the node's event `ev` is the transfer row `n` (each field the row selects).
pub fn transfer_matches(n: &Value, ev: &Value) -> bool {
    let s = |k: &str| n.get(k).and_then(Value::as_str);
    let p = |i: usize| ev.pointer(&format!("/params/{i}"));
    ev.get("name").and_then(Value::as_str) == Some("TRANSFER")
        && s("senderAccount").is_none_or(|x| p(0).and_then(Value::as_str) == Some(x))
        && s("receiverAccount").is_none_or(|x| p(1).and_then(Value::as_str) == Some(x))
        && n.get("amount")
            .is_none_or(|x| p(2).is_some_and(|y| same_amount(x, y)))
        && s("moduleName").is_none_or(|x| module_name(ev).as_deref() == Some(x))
}

/// Whether the node's event `ev` is the event row `n` (its name and qualified name, as
/// selected).
pub fn event_matches(n: &Value, ev: &Value) -> bool {
    let name = ev.get("name").and_then(Value::as_str);
    let s = |k: &str| n.get(k).and_then(Value::as_str);
    s("name").is_none_or(|x| name == Some(x))
        && s("qualifiedName").is_none_or(|x| {
            module_name(ev)
                .zip(name)
                .is_some_and(|(m, nm)| format!("{m}.{nm}") == x)
        })
}

/// The row's content against the node's events of its transaction: at its index when it
/// has one. A matching event at another index leaves the verdict unknown (the index is not
/// the node's).
fn content_verdict(
    r: &RowId,
    n: &Value,
    events: &[&Value],
    v: impl Fn(Canon, &str) -> Verdict,
) -> Verdict {
    let m: fn(&Value, &Value) -> bool = match r.kind {
        Some(Kind::Transfer) => transfer_matches,
        Some(Kind::Event) => event_matches,
        _ => return v(Canon::Yes, "placed"),
    };
    let any = events.iter().any(|ev| m(n, ev));
    let at = r
        .index
        .and_then(|i| usize::try_from(i).ok())
        .and_then(|i| events.get(i))
        .is_some_and(|ev| m(n, ev));
    match (r.index, any, at) {
        (_, false, _) => v(Canon::No, "the node's transaction lacks the row"),
        (None, true, _) | (Some(_), true, true) => v(Canon::Yes, "placed, with the row's content"),
        (Some(_), true, false) => {
            let mut x = v(Canon::Unknown, "the row's index is not the node's");
            x.canon = Canon::Unknown;
            x
        }
    }
}

/// The node's verdict on a row from its `/poll` entry: its block (or height) must be the
/// row's, and a transfer's or event's content must be the node's event at its index.
pub fn poll_verdict(r: &RowId, n: &Value, p: &Value) -> Verdict {
    let pb = p
        .pointer("/metaData/blockHash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let ph = p.pointer("/metaData/blockHeight").and_then(Value::as_u64);
    if pb.is_none() && ph.is_none() {
        return Verdict::unknown("poll: no block in the entry");
    }
    let v = |canon, why: &str| Verdict {
        canon,
        height: ph,
        block: pb.clone(),
        why: format!("poll: {why}"),
    };
    let placed = match (&r.block, r.height) {
        (Some(b), h) => pb.as_deref() == Some(b.as_str()) && h.is_none_or(|h| ph == Some(h)),
        (None, Some(h)) => ph == Some(h),
        (None, None) => true,
    };
    if !placed {
        return v(Canon::No, "the node's block for the key is another");
    }
    let events: Vec<&Value> = p
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect();
    content_verdict(r, n, &events, v)
}

/// A row `/poll` does not know (a coinbase row: its request key is the coinbase's): the
/// node's headers at the row's height and the payload (with outputs) of the only one.
pub fn coinbase_verdict(
    r: &RowId,
    n: &Value,
    headers: &[Value],
    payload: Option<&Value>,
) -> Verdict {
    if headers.len() != 1 {
        return Verdict::unknown(format!("coinbase: {} headers at the height", headers.len()));
    }
    let hash = headers[0]
        .get("hash")
        .and_then(Value::as_str)
        .map(str::to_string);
    let v = |canon, why: &str| Verdict {
        canon,
        height: r.height,
        block: hash.clone(),
        why: format!("coinbase: {why}"),
    };
    if r.block.as_ref().is_some_and(|b| Some(b) != hash.as_ref()) {
        return v(Canon::No, "the node's block at the height is another");
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
            Canon::No,
            "the key is neither polled nor the block's coinbase",
        );
    }
    let events: Vec<&Value> = cb
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect();
    content_verdict(r, n, &events, v)
}

/// A block row: the node's headers at its height (orphans included).
pub fn block_verdict(r: &RowId, headers: &[Value]) -> Verdict {
    let has = headers
        .iter()
        .any(|h| h.get("hash").and_then(Value::as_str) == r.block.as_deref());
    let v = |canon, why: &str| Verdict {
        canon,
        height: r.height,
        block: r.block.clone(),
        why: format!("header: {why}"),
    };
    match (has, headers.len()) {
        (true, 1) => v(Canon::Yes, "the node's only block at the height"),
        (true, _) => Verdict::unknown("header: a fork at the height"),
        (false, 0) => Verdict::unknown("header: none at the height"),
        (false, _) => v(Canon::No, "not among the node's blocks at the height"),
    }
}

/// One row of a side with its identity.
#[derive(Debug, Clone)]
pub struct Row {
    pub id: RowId,
    pub key: String,
    pub edge: Value,
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
/// Fails when a row's identity cannot be decided, or rows without an index share a key
/// (which row is which cannot be told).
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
    let (a, b) = (rows(ca)?, rows(cb)?);
    for side in [&a, &b] {
        let all: Vec<usize> = (0..side.len()).collect();
        let (_, g) = group(side, &all);
        if let Some(r) = side
            .iter()
            .find(|r| !r.id.indexed() && g[r.key.as_str()].len() > 1)
        {
            return Err(format!(
                "rows without an index share one identity: {}",
                r.id.evidence()
            ));
        }
    }
    Ok(Prep {
        a,
        b,
        a_cut: more(ca),
        b_cut: more(cb),
    })
}

impl Prep {
    /// The rows that need a node verdict, as (side: 0 ours, 1 the oracle's; row index):
    /// ours without a height or not matched once, and one row per oracle-only identity.
    pub fn reads(&self) -> Vec<(usize, usize)> {
        let all_a: Vec<usize> = (0..self.a.len()).collect();
        let all_b: Vec<usize> = (0..self.b.len()).collect();
        let (_, ga) = group(&self.a, &all_a);
        let (ob, gb) = group(&self.b, &all_b);
        let mut out = Vec::new();
        for (i, r) in self.a.iter().enumerate() {
            let nb = gb.get(r.key.as_str()).map_or(0, Vec::len);
            if r.id.height.is_none() || nb != 1 {
                out.push((0, i));
            }
        }
        for k in ob {
            if !ga.contains_key(k) {
                out.push((1, gb[k][0]));
            }
        }
        out
    }
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
    /// Our lowest height and the oracle's lowest `creationTime` on the pages.
    pub lowest: Option<u64>,
    pub oracle_lowest_ctime: Option<String>,
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

/// H7 on prepared pages with the node's verdicts (`va` per row of ours, `vb` per row of the
/// oracle's; `None` where none was read). `max`: the oracle's ceiling.
pub fn decide(p: &Prep, va: &[Option<Verdict>], vb: &[Option<Verdict>], max: u64) -> Decision {
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
    let above = |h: &Option<u64>| h.is_some_and(|h| h > max);
    let live_a: Vec<usize> = (0..p.a.len()).filter(|&i| !above(&ha[i])).collect();
    let live_b: Vec<usize> = (0..p.b.len()).filter(|&j| !above(&hb[j])).collect();
    d.above = p.a.len() + p.b.len() - live_a.len() - live_b.len();
    // Ours: a height for every row, in descending order (our sort key).
    for &i in &live_a {
        if ha[i].is_none() {
            match canon(&va[i]) {
                Canon::No => d.rejected.push((
                    Some(p.a[i].edge.clone()),
                    None,
                    format!(
                        "a row of ours the node calls not canonical ({})",
                        why_of(va[i].as_ref())
                    ),
                )),
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
    if hs.windows(2).any(|w| w[0] < w[1]) {
        d.rejected
            .push((None, None, "ours is not in descending height order".into()));
        return d;
    }
    d.lowest = hs.iter().min().copied();
    let ct = |r: &Row| r.id.ctime.as_deref().and_then(parse_utc);
    let b_low = live_b
        .iter()
        .filter_map(|&j| ct(&p.b[j]).map(|t| (t, j)))
        .min();
    d.oracle_lowest_ctime = b_low.and_then(|(_, j)| p.b[j].id.ctime.clone());
    d.grouped = true;
    let (oa, ga) = group(&p.a, &live_a);
    let (ob, gb) = group(&p.b, &live_b);
    for k in &oa {
        let i = ga[k][0];
        let (ra, vi) = (&p.a[i], va[i].as_ref());
        if ga[k].len() > 1 {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                "ours lists the row twice".into(),
            ));
            continue;
        }
        match gb.get(k) {
            Some(jb) => {
                d.pairs.push((i, jb[0]));
                if jb.len() > 1 {
                    // The node holds the row once: the oracle's other copies are duplicates.
                    match canon(&va[i]) {
                        Canon::Yes => d
                            .oracle
                            .extend(jb[1..].iter().map(|&j| evidence(&p.b[j], vi, "duplicate"))),
                        Canon::No => d.rejected.push((
                            Some(ra.edge.clone()),
                            None,
                            format!(
                                "a row of ours the node calls not canonical ({})",
                                why_of(vi)
                            ),
                        )),
                        Canon::Unknown => d.undecided.push(format!(
                            "an oracle duplicate the node did not settle ({}): {}",
                            why_of(vi),
                            ra.id.evidence()
                        )),
                    }
                }
            }
            None => match canon(&va[i]) {
                Canon::Yes => {
                    let outside =
                        p.b_cut && matches!((ct(ra), b_low), (Some(t), Some((lo, _))) if t <= lo);
                    let why = if outside {
                        "outside-oracle-window"
                    } else {
                        "oracle-gap"
                    };
                    d.ours.push(evidence(ra, vi, why));
                }
                Canon::No => d.rejected.push((
                    Some(ra.edge.clone()),
                    None,
                    format!(
                        "an ours-only row the node calls not canonical ({})",
                        why_of(vi)
                    ),
                )),
                Canon::Unknown => d.undecided.push(format!(
                    "an ours-only row the node did not settle ({}): {}",
                    why_of(vi),
                    ra.id.evidence()
                )),
            },
        }
    }
    for k in &ob {
        if ga.contains_key(k) {
            continue;
        }
        let jb = &gb[k];
        let j = jb[0];
        let (rb, vj) = (&p.b[j], vb[j].as_ref());
        match canon(&vb[j]) {
            Canon::No => d
                .oracle
                .extend(jb.iter().map(|&j| evidence(&p.b[j], vj, "orphan"))),
            Canon::Yes => {
                let why = match (p.a_cut, d.lowest, hb[j]) {
                    (false, _, _) => Err("an oracle-only canonical row; our list is complete"),
                    (true, Some(lo), Some(h)) if h < lo => Ok("below-our-window"),
                    (true, Some(lo), Some(h)) if h == lo => Ok("at-our-boundary-height"),
                    (true, Some(_), Some(_)) => {
                        Err("an oracle-only canonical row strictly inside our window")
                    }
                    _ => Err("an oracle-only canonical row without a height to place"),
                };
                match why {
                    Ok(w) => {
                        d.oracle.push(evidence(rb, vj, w));
                        d.oracle
                            .extend(jb[1..].iter().map(|&j| evidence(&p.b[j], vj, "duplicate")));
                    }
                    Err(w) => d
                        .rejected
                        .push((None, Some(rb.edge.clone()), w.to_string())),
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

/// Node and ours reads of one compare, cached per key.
#[derive(Default)]
struct Reads {
    node: usize,
    ours: usize,
    polls: HashMap<(u8, String), Result<Option<Value>, String>>,
    headers: HashMap<(u8, u64), Result<Vec<Value>, String>>,
    payloads: HashMap<String, Result<Value, String>>,
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

impl Ctx {
    /// R-P5P9-62 H7 on an oracle pair's root list (see the module docs). When every row is
    /// accounted for, the pages hold the matched rows, paired in ours' order, and the hit
    /// carries the evidence. When the node settles a row against ours, the matched rows are
    /// paired the same way and each such row gets a [`ROW_PATH`] entry. When anything
    /// cannot be settled, the pages stay as they were. The note says why, with the reads.
    pub(crate) async fn gql_window_referee(
        &self,
        ta: &Target,
        a: &mut Answer,
        b: &mut Answer,
        max: u64,
        root_scoped: bool,
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
        let mut st = Reads::default();
        let mut va: Vec<Option<Verdict>> = vec![None; prep.a.len()];
        let mut vb: Vec<Option<Verdict>> = vec![None; prep.b.len()];
        for (side, i) in prep.reads() {
            let r = if side == 0 { &prep.a[i] } else { &prep.b[i] };
            let v = self.h7_verdict(ta, r, &mut st).await;
            if side == 0 {
                va[i] = Some(v);
            } else {
                vb[i] = Some(v);
            }
        }
        let d = decide(&prep, &va, &vb, max);
        let reads = format!("{} node reads, {} reads of ours", st.node, st.ours);
        let ev_a = json!({"rows": prep.a.len(), "cut": prep.a_cut, "lowestHeight": d.lowest,
            "oneSided": d.ours});
        let ev_b = json!({"rows": prep.b.len(), "cut": prep.b_cut,
            "lowestCreationTime": d.oracle_lowest_ctime, "nodeReads": st.node,
            "oursReads": st.ours, "oneSided": d.oracle});
        if !d.undecided.is_empty() || !d.grouped {
            let mut out = Vec::new();
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

    /// One row's node verdict (single-row reads, cached per compare).
    async fn h7_verdict(&self, ta: &Target, r: &Row, st: &mut Reads) -> Verdict {
        let id = &r.id;
        let n = r.edge.get("node").unwrap_or(&Value::Null);
        let chain = match id.chain {
            Some(c) => Some(c),
            None => self.h7_chain(ta, id, st).await,
        };
        let Some(chain) = chain else {
            return Verdict::unknown("no chain for the row");
        };
        if id.kind == Some(Kind::Block) {
            let Some(h) = id.height else {
                return Verdict::unknown("a block row without a height");
            };
            return match self.h7_headers(chain, h, st).await {
                Ok(hs) => block_verdict(id, &hs),
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
            return poll_verdict(id, n, &p);
        }
        // Unknown to /poll: a coinbase row is settled by the node's block at its height.
        let Some(h) = id.height else {
            return Verdict::unknown("poll: key unknown, and no height to read a coinbase at");
        };
        let hs = match self.h7_headers(chain, h, st).await {
            Ok(hs) => hs,
            Err(w) => return Verdict::unknown(w),
        };
        let mut payload = None;
        if let [hd] = hs.as_slice() {
            if let Some(ph) = hd.get("payloadHash").and_then(Value::as_str) {
                match self.h7_payload(chain, ph, st).await {
                    Ok(p) => payload = Some(p),
                    Err(w) => return Verdict::unknown(w),
                }
            }
        }
        coinbase_verdict(id, n, &hs, payload.as_ref())
    }

    /// Takes one node read from the compare's cap and the run's budget.
    fn h7_take(&self, st: &mut Reads) -> Result<(), String> {
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

/// Request keys H8 reads (one read of ours each) per compare at most.
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
        (Some(a), Some(b)) => same_amount(a, b),
        _ => true,
    }
}

/// H8 on one request key. `ours`: our transaction's transfers (their indexes, which are
/// TRANSFER event indexes, and rows), from our own `transfers(requestKey)`; `oracle`: the
/// oracle's rows of that key with their values. Returns (the oracle's value, our index)
/// per oracle row when each value is the rank, among our indexes, of the transfer with the
/// row's content; else why not.
pub fn rank_map(
    ours: &[(i64, Value)],
    oracle: &[(i64, &Value)],
) -> Result<Vec<(i64, i64)>, String> {
    let mut idx: Vec<&(i64, Value)> = ours.iter().collect();
    idx.sort_by_key(|x| x.0);
    let mut out = Vec::new();
    for (r, row) in oracle {
        let Some((i, mine)) = usize::try_from(*r).ok().and_then(|u| idx.get(u).copied()) else {
            return Err(format!(
                "the oracle's {r} is past our {} transfers",
                idx.len()
            ));
        };
        if !same_transfer(row, mine) {
            return Err(format!(
                "the oracle's {r} is not the rank of our transfer with its content"
            ));
        }
        out.push((*r, *i));
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
    /// most [`ORDER_INDEX_READS`]), one read of our own `transfers(requestKey)`. When every
    /// oracle value of that key is the rank of our transfer with its content, the oracle's
    /// rows take our indexes (`orderIndex` and the relay id); the hit lists each key's
    /// indexes. Anything else leaves the rows as they were.
    pub(crate) async fn gql_transfer_order(
        &self,
        ta: &Target,
        a: &Answer,
        b: &mut Answer,
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
        let mut keys: Vec<(String, Option<u8>)> = Vec::new();
        for e in edges.iter() {
            let Some(id) = transfer(e) else { continue };
            let (Some(k), Some(rk)) = (id.key(), id.rk.clone()) else {
                continue;
            };
            let held = ours
                .get(&k)
                .is_some_and(|n| same_transfer(n, e.get("node").unwrap_or(&Value::Null)));
            if !held && !keys.iter().any(|(x, _)| *x == rk) {
                keys.push((rk, id.chain));
            }
        }
        let q = "query($k: String!) { transfers(requestKey: $k, first: 100) { pageInfo { hasNextPage } edges { node { orderIndex amount senderAccount receiverAccount moduleName chainId blockHash requestKey } } } }";
        let mut evidence = Vec::new();
        let mut reads = 0;
        for (rk, chain) in keys.into_iter().take(ORDER_INDEX_READS) {
            reads += 1;
            let Some(d) = self.gql_read(ta, q, json!({ "k": rk })).await else {
                continue;
            };
            if d.pointer("/transfers/pageInfo/hasNextPage") != Some(&Value::Bool(false)) {
                continue;
            }
            let mine: Vec<(i64, Value)> = d
                .pointer("/transfers/edges")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|e| e.get("node"))
                .filter(|n| {
                    chain.is_none_or(|c| {
                        n.get("chainId").and_then(text).as_deref() == Some(&c.to_string())
                    })
                })
                .filter_map(|n| {
                    Some((n.get("orderIndex").and_then(text)?.parse().ok()?, n.clone()))
                })
                .collect();
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
            match rank_map(&mine, &rows) {
                Ok(map) => {
                    for ((j, _), (_, i)) in theirs.iter().zip(&map) {
                        if let Some(n) = edges[*j].get_mut("node") {
                            reindex_transfer(n, *i);
                        }
                    }
                    let mut idx: Vec<i64> = mine.iter().map(|x| x.0).collect();
                    idx.sort_unstable();
                    evidence.push(json!({"requestKey": rk, "ours": idx,
                        "oracle": map.iter().map(|x| x.0).collect::<Vec<_>>(),
                        "mapped": map.iter().map(|x| x.1).collect::<Vec<_>>()}));
                }
                Err(why) => tracing::debug!(rk, why, "H8 not explained"),
            }
        }
        (!evidence.is_empty()).then(|| {
            entry(
                ORDER_INDEX_PATH,
                Some(json!({"readsOfOurs": reads})),
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
    ) -> Option<DiffEntry> {
        let ours = payload_data_txs(&a.body);
        let theirs = payload_data_txs(&b.body);
        let var_chain = vars
            .get("chainId")
            .and_then(text)
            .and_then(|c| c.parse::<u8>().ok());
        let mut st = Reads::default();
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
                        Some(c) => self.node_cont_data_null(c, &rk, &mut st).await,
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
                Some(json!({"nodeReads": st.node})),
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
            poll_verdict(&id(Some("B"), None, Some(1)), &n, &p).canon,
            Canon::Yes
        );
        assert_eq!(
            poll_verdict(&id(Some("C"), None, Some(1)), &n, &p).canon,
            Canon::No
        );
        // The row's index points at another event: not the node's numbering, unknown.
        assert_eq!(
            poll_verdict(&id(None, Some(9), Some(0)), &n, &p).canon,
            Canon::Unknown
        );
        let p2 = poll("B", 9, json!([other]));
        assert_eq!(
            poll_verdict(&id(None, Some(9), None), &n, &p2).canon,
            Canon::No
        );
        // A coinbase row: the only header at its height and its coinbase request key.
        let cb = |rk: &str| {
            let t = json!({"reqKey": rk, "events": [xfer]}).to_string();
            json!({"coinbase": kdx_types::hash::b64url_encode(t.as_bytes())})
        };
        let hd = vec![json!({"hash": "B", "payloadHash": "P"})];
        let r = id(None, Some(9), Some(0));
        assert_eq!(
            coinbase_verdict(&r, &n, &hd, Some(&cb("k"))).canon,
            Canon::Yes
        );
        assert_eq!(
            coinbase_verdict(&r, &n, &hd, Some(&cb("j"))).canon,
            Canon::No
        );
        let two = vec![hd[0].clone(), json!({"hash": "F"})];
        assert_eq!(coinbase_verdict(&r, &n, &two, None).canon, Canon::Unknown);
        // A block row: the node's only header at the height.
        let mut b = id(Some("B"), Some(9), None);
        b.kind = Some(Kind::Block);
        assert_eq!(block_verdict(&b, &hd).canon, Canon::Yes);
        assert_eq!(block_verdict(&b, &two).canon, Canon::Unknown);
        assert_eq!(block_verdict(&b, &[json!({"hash": "F"})]).canon, Canon::No);
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
