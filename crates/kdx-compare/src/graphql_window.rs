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
//!   read of ours, `first` = our rows + 50, no `after`, paged on by `endCursor` until it
//!   passes our boundary height, else capped (R-P5P9-86 SK); our page must be its prefix:
//!   R-P5P9-64, I-1);
//! - an ours-only row is canonical on the node, holds the node's content on every selected
//!   field the node's entry has (R-P5P9-64 I-2, our amounts exact: M-3) and matches the
//!   query's own filter (R-P5P9-63 (1)). It is then outside the oracle's window (beyond its
//!   last `creationTime` while its page is cut) or a node-confirmed oracle gap;
//!   every field it selects is one the harness checks against the node (a per-kind whitelist;
//!   a transfer's `creationTime` against the node's command) and identity fields it selects
//!   more than once agree (R-P5P9-70 I-1);
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

use std::collections::{BTreeMap, HashMap};

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
/// R-P5P9-86 SK: the pages of ours the longer read takes at most to pass the boundary height.
pub const PREFIX_PAGES: usize = 10;
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
/// R-P5P9-86 TC: visible hit of a totalCount-only difference the count walk settled
/// (allowlist `graphql-count-walk`), with each side's count, walked rows and one-sided rows.
pub const COUNT_WALK_PATH: &str = "/~graphql-count-walk";
/// R-P5P9-86 TC: the walk passed [`COUNT_WALK_ROWS`] rows on a side, "count walk capped"
/// (unexplained, its own reason; never allowlisted).
pub const COUNT_WALK_CAPPED_PATH: &str = "/~graphql-count-walk-capped";
/// R-P5P9-92 (1): visible hit of the count walk's oracle that counts heights (allowlist id
/// `graphql-r75-oracle-counts-heights`, ACCEPTED; scoped to Transactions-9e58b95d).
pub const ORACLE_COUNTS_HEIGHTS_PATH: &str = "/~graphql-r75-oracle-counts-heights";
/// R-P5P9-86 TC: rows a side the count walk reads at most, live.
pub const COUNT_WALK_ROWS: usize = 5_000;
/// R-P5P9-97 TC: rows a side the count walk reads at most in referee mode (the oracle is
/// frozen and offline, so the cost is reads only, under the run's read cap).
pub const REFEREE_COUNT_WALK_ROWS: usize = 20_000;
/// R-P5P9-95 m-10: the count walk's byte budget a side (the rows' JSON text); past it,
/// `count walk capped`.
#[cfg(not(test))]
pub const COUNT_WALK_BYTES: usize = 64 << 20;
#[cfg(test)]
pub const COUNT_WALK_BYTES: usize = 4 << 20;
/// R-P5P9-92 (2): ours' rows above the oracle's height that the count walk polls at most.
pub const ABOVE_TIP_ROWS: usize = 50;
/// R-P5P9-93: the widest query window (in heights) whose node counts the walk reads.
pub const WINDOW_HEIGHTS: u64 = 100;

/// R-P5P9-87 TC: the record's height, the highest row of ours' stored page (its root list).
pub fn highest_row(body: &Value) -> Option<u64> {
    let data = body.get("data")?;
    let root = root_name(data)?;
    data.get(&root)?
        .get("edges")?
        .as_array()?
        .iter()
        .filter_map(|e| prepared_id(e, &root).height)
        .max()
}

/// H7's entries that are never explained: a row it rejected, an undecided row, a short page.
fn never_explained(path: &str) -> bool {
    [ROW_PATH, UNDECIDED_PATH, SHORT_PAGE_PATH].contains(&path)
}
/// R-P5P9-75 N: visible hit of a page cut inside one block, explained by the node's block
/// order (allowlist id `graphql-r75-single-block-cut`); it carries the evidence.
pub const SINGLE_BLOCK_PATH: &str = "/~graphql-r75-single-block-cut";
/// Ours' page size cap (Q10): a page of `first` above it holds this many rows.
pub const OURS_PAGE_CAP: u64 = 100;
/// R-P5P9-75 C: the oracle's base64-encoded coinbase `requestKey` that decodes to ours' key,
/// ours being the node's coinbase key (the block's parent hash). R-P5P9-86 C replaced the
/// premise (ours' key is the node's encoded key, not the bare parent hash): no longer written.
pub const COINBASE_KEY_PATH: &str = "/~graphql-r75-coinbase-request-key";
/// R-P5P9-86 C: an oracle coinbase `requestKey` in one of the oracle's forms (the bare parent
/// hash of the node's canonical header at the row's height, or base64 of that header's own
/// hash), paired with ours' row whose key is the node's coinbase key byte for byte (allowlist
/// id `graphql-coinbase-oracle-key-form`).
pub const COINBASE_FORM_PATH: &str = "/~graphql-coinbase-oracle-key-form";
/// R-P5P9-78 (H2 I-2): next to [`WINDOW_PATH`] when a one-sided oracle row was settled off the
/// node's chain by E ([`NOT_IN_NODE_TX`]) or as an orphan by block (M); it lists those rows
/// (allowlist id `graphql-r75-h7-off-chain`).
pub const OFF_CHAIN_PATH: &str = "/~graphql-r75-h7-off-chain";
/// R-P5P9-78 (H2 I-2): O, H8 on the transfer lists nested in a transaction (allowlist id
/// `graphql-r75-nested-order-index`).
pub const NESTED_ORDER_INDEX_PATH: &str = "/~graphql-r75-nested-order-index";
/// R-P5P9-78 (H2 I-2): K, a tx's exec `cmd.payload.data` against the node's command
/// (allowlist id `graphql-r75-exec-payload-data`).
pub const EXEC_PAYLOAD_DATA_PATH: &str = "/~graphql-r75-exec-payload-data";
/// R-P5P9-81 (m-1): next to [`WINDOW_PATH`] when H7 settled a one-sided row whose identity
/// has no index (R-P5P9-75 D's node-confirmed duplicates, F's key-only rows, an oracle-only
/// row placed outside our window): it lists those rows per side (allowlist id
/// `graphql-r75-h7-unindexed`). E's and M's rows show under [`OFF_CHAIN_PATH`] instead.
pub const UNINDEXED_PATH: &str = "/~graphql-r75-h7-unindexed";
/// R-P5P9-81 (m-1): the settle classes the window hit's evidence always counts, per side.
pub const SETTLE_CLASSES: [&str; 4] = [
    "oracle-gap",
    "outside-oracle-window",
    "duplicate",
    "key-only",
];
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
    /// R-P5P9-86 C: a coinbase row (its key is not a 43-character hash, or is the oracle's
    /// bare parent-hash form of ours' coinbase key). Its identity is its place (chain, block,
    /// height, index), not its key, and it is settled on the coinbase path only, never `/poll`.
    pub coinbase: bool,
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
        let rk = if self.coinbase {
            Some("~coinbase".to_string())
        } else {
            self.rk.clone()
        };
        Some(format!(
            "{}|{}|{}|{}|{}|{}",
            kind.as_str(),
            o(self.chain.map(|c| c.to_string())),
            o(self.block.clone()),
            o(self.height.map(|h| h.to_string())),
            o(rk),
            o(self.index.map(|i| i.to_string()))
        ))
    }

    /// Whether the key names one row: a tx or a block, or an event or transfer with its index.
    pub fn indexed(&self) -> bool {
        matches!(self.kind, Some(Kind::Tx | Kind::Block)) || self.index.is_some()
    }

    pub fn evidence(&self) -> Value {
        let mut e = json!({"kind": self.kind.map(Kind::as_str), "chain": self.chain,
            "requestKey": self.rk, "index": self.index, "block": self.block,
            "height": self.height});
        if self.coinbase {
            e["coinbase"] = json!(true);
        }
        e
    }
}

/// The length of a hash (a tx's request key, a block hash) in unpadded base64url.
pub const HASH_LEN: usize = 43;

/// R-P5P9-86 C: the text a coinbase key form decodes to: base64 (either alphabet, padded or
/// not) over a JSON string (the node's 60-character era) or over bare text (the node's
/// 58-character era, and the oracle's base64 of a block's own hash). `None` for a 43-character
/// hash or text that does not decode.
pub fn decode_coinbase_rk(s: &str) -> Option<String> {
    if s.len() == HASH_LEN {
        return None;
    }
    let url: String = s
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect();
    let b = kdx_types::hash::b64url_decode(&url).ok()?;
    if b.first() == Some(&b'"') {
        return serde_json::from_slice::<String>(&b).ok();
    }
    String::from_utf8(b).ok()
}

/// R-P5P9-86 C: whether `k` is a coinbase key: not a 43-character hash, and base64 over a
/// 43-character hash (bare or JSON-quoted), which covers both of the node's eras and the
/// oracle's base64 form. A key in no such form is not settled as a coinbase: `/poll` refuses
/// it, so it stays unexplained.
pub fn is_coinbase_key(k: &str) -> bool {
    decode_coinbase_rk(k).is_some_and(|t| t.len() == HASH_LEN)
}

/// R-P5P9-86 C: whether the oracle's coinbase key `k` is one of the oracle's forms for the
/// node's canonical header `hd` at the row's height: the bare parent hash, or base64 of the
/// header's own hash. The node's own encodings are not the oracle's forms: an oracle key that
/// is neither stays unexplained.
pub fn oracle_coinbase_form(k: &str, hd: &Value) -> Option<&'static str> {
    let at = |f: &str| hd.get(f).and_then(Value::as_str);
    if k.len() == HASH_LEN {
        return (at("parent") == Some(k)).then_some("the bare parent hash");
    }
    let url: String = k
        .trim_end_matches('=')
        .chars()
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect();
    let own = kdx_types::hash::b64url_decode(&url)
        .ok()
        .and_then(|b| String::from_utf8(b).ok());
    (own.is_some() && own.as_deref() == at("hash")).then_some("base64 of the block's own hash")
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
        coinbase: false,
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
    /// R-P5P9-75 A: the node's TRANSFER_XCHAIN and TRANSFER_XCHAIN_RECD events of the row's
    /// transaction.
    pub xchain: Vec<Value>,
    /// R-P5P9-68 (2): ours-only tx rows' `cmd` fields against the node's signed command
    /// (`None`: not checked).
    pub cmd: Option<CmdCheck>,
    /// R-P5P9-70 (I-1): an ours-only transfer row's `creationTime` against the node's command
    /// (or, for a coinbase, its block's time), in ours' form (`None`: not checked).
    pub ctime: Option<CmdCheck>,
    /// R-P5P9-75 B: whether the block the node placed the row in is the canonical one at its
    /// height on the cut's branch (`None`: not read; `Err`: why it could not be read).
    pub branch: Option<Result<bool, String>>,
    /// R-P5P9-86 U: the public keys of the signers of the node's signed command for an
    /// ours-only tx row under a `publicKey` filter (`None`: not read; `Err`: why not).
    pub signers: Option<Result<Vec<String>, String>>,
}

/// The check of an ours-only tx row's `cmd` fields against the node (R-P5P9-68 (2)).
#[derive(Debug, Clone, PartialEq)]
pub enum CmdCheck {
    /// Every selected field is the node's.
    Same,
    /// The named field is not the node's.
    Differs(String),
    /// The node's command could not be read (why).
    Unsettled(String),
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
            xchain: Vec::new(),
            cmd: None,
            ctime: None,
            branch: None,
            signers: None,
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
    let o = transfer_offset(ev);
    let p = |i: usize| ev.pointer(&format!("/params/{}", i + o));
    ev.get("name").and_then(Value::as_str) == Some("TRANSFER")
        && token_is(n, ev)
        && sel(n, "senderAccount").is_none_or(|x| p(0) == Some(x))
        && sel(n, "receiverAccount").is_none_or(|x| p(1) == Some(x))
        && sel(n, "amount").is_none_or(|x| p(2).is_some_and(|y| same_amount(x, y, oracle)))
        && sel(n, "moduleName").is_none_or(|x| module_name(ev).as_deref() == x.as_str())
        && sel(n, "moduleHash").is_none_or(|x| ev.get("moduleHash") == Some(x))
}

/// R-P5P9-86 T: where a TRANSFER event's sender, receiver and amount start. A fungible
/// TRANSFER is (sender, receiver, amount); a marmalade one (token, sender, receiver, amount).
pub fn transfer_offset(ev: &Value) -> usize {
    match ev.get("params").and_then(Value::as_array) {
        Some(ps) if ps.len() == 4 && ps[0].is_string() => 1,
        _ => 0,
    }
}

/// R-P5P9-86 T: a transfer row's `tokenId` against the node's TRANSFER event: null for a
/// fungible (three-parameter) event, else the marmalade event's token. Not selected: true.
fn token_is(n: &Value, ev: &Value) -> bool {
    let Some(t) = n.get("tokenId") else {
        return true;
    };
    let ps = ev.get("params").and_then(Value::as_array);
    match t {
        Value::Null => ps.is_some_and(|p| p.len() == 3),
        t => transfer_offset(ev) == 1 && ps.and_then(|p| p.first()) == Some(t),
    }
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
                && (x.is_null() || oracle || bad_result_is(x, p.pointer("/result/error")))
        })
        && r.get("continuation")
            .is_none_or(|x| json_is(x, p.get("continuation")))
}

/// R-P5P9-86 NC: ours' `badResult` against the node's error, after the ACCEPTED
/// `failed-tx-error-info` equivalence (`/result/error/info`, changed: both hold an `info`
/// text, and only the text differs). Every other field must be the node's.
fn bad_result_is(x: &Value, node: Option<&Value>) -> bool {
    let node = node.unwrap_or(&Value::Null);
    let ours = json_field(x);
    let info = |v: &Value| v.get("info").filter(|i| i.is_string()).is_some();
    if same_value(&ours, node, false) {
        return true;
    }
    if !(info(&ours) && info(node)) {
        return false;
    }
    // R-P5P9-95 m-7: CG-5's evidence is ours "" against the node's text; nothing wider.
    let oi = ours.get("info");
    if !(oi == Some(&json!("")) || oi == node.get("info")) {
        return false;
    }
    let drop = |v: &Value| {
        let mut v = v.clone();
        if let Some(m) = v.as_object_mut() {
            m.shift_remove("info");
        }
        v
    };
    same_value(&drop(&ours), &drop(node), false)
}

/// The fields of a list row's node the harness checks against the node, per kind and level
/// (R-P5P9-68 (2) for `cmd`, widened to the whole row by R-P5P9-70 I-1): a level lists its
/// checked keys, and a key with its own entry is an object (or, for the signers and their
/// capabilities, a list of objects). Every other selected field is unchecked.
fn checked_keys(kind: Kind, path: &str) -> &'static [&'static str] {
    match (kind, path) {
        // The row's identity (cross-checked by [`id_conflict`]), `cmd` against the node's
        // signed command, `result` against its `/poll` entry or coinbase output.
        (Kind::Tx, "") => &["__typename", "cmd", "hash", "id", "result"],
        (Kind::Tx, "result") => &[
            "badResult",
            "block",
            "continuation",
            "eventCount",
            "gas",
            "goodResult",
            "height",
            "transactionId",
        ],
        // R-P5P9-75 B: `canonical` against H7's own verdict on the cut's branch.
        (Kind::Tx, "result.block") => &["canonical", "chainId", "hash", "height"],
        (Kind::Transfer | Kind::Event, "block") => &["chainId", "hash", "height"],
        (Kind::Tx, "cmd") => &["meta", "networkId", "nonce", "payload", "signers"],
        (Kind::Tx, "cmd.meta") => &[
            "chainId",
            "creationTime",
            "gasLimit",
            "gasPrice",
            "sender",
            "ttl",
        ],
        (Kind::Tx, "cmd.payload") => &["code", "data", "pactId", "proof", "rollback", "step"],
        (Kind::Tx, "cmd.signers") => &["address", "clist", "pubkey"],
        (Kind::Tx, "cmd.signers.clist") => &["args", "name"],
        // [`transfer_matches`] against the node's event, the identity, and `creationTime`
        // against the node's command (or the coinbase's block).
        (Kind::Transfer, "") => &[
            "__typename",
            "amount",
            "block",
            "blockHash",
            "chainId",
            "creationTime",
            "crossChainTransfer",
            "height",
            "id",
            "moduleHash",
            "moduleName",
            "orderIndex",
            "receiverAccount",
            "requestKey",
            "senderAccount",
            // R-P5P9-86 T: [`token_is`], inside [`transfer_matches`].
            "tokenId",
        ],
        // [`event_matches`] against the node's event, and the identity.
        (Kind::Event, "") => &[
            "__typename",
            "block",
            "blockHash",
            "chainId",
            "height",
            "id",
            "moduleHash",
            "name",
            "orderIndex",
            "parameterText",
            "parameters",
            "qualifiedName",
            "requestKey",
        ],
        // [`block_fields`] against the canonical header.
        (Kind::Block, "") => &[
            "__typename",
            "chainId",
            "creationTime",
            "difficulty",
            "epoch",
            "hash",
            "height",
            "id",
            "nonce",
            "parent",
            "payloadHash",
            "powHash",
            "target",
            "weight",
        ],
        (Kind::Block, "parent") => &["hash"],
        // R-P5P9-75 A: [`xchain_mismatch`] against the node's TRANSFER_XCHAIN.
        // R-P5P9-86 T: and `senderAccount`, the receiving half's sender.
        (Kind::Transfer, "crossChainTransfer") => &["chainId", "receiverAccount", "senderAccount"],
        _ => &[],
    }
}

/// The schema type at a checked level of a row's node (R-P5P9-71 (3)), for its
/// `__typename`. `cmd.payload` is a union: its member follows from the fields it selects
/// (`code` an `ExecutionPayload`; `pactId`, `proof`, `rollback` or `step` a
/// `ContinuationPayload`), and is unknown with neither or both. `result` is the mined
/// member of its union, `TransactionResult` (ours has no mempool).
fn type_at(kind: Kind, path: &str, m: &Map<String, Value>) -> Option<&'static str> {
    Some(match (kind, path) {
        (Kind::Tx, "") => "Transaction",
        (Kind::Transfer, "") => "Transfer",
        (Kind::Event, "") => "Event",
        (Kind::Block, "") | (Kind::Tx, "result.block") | (Kind::Block, "parent") => "Block",
        (Kind::Transfer | Kind::Event, "block") => "Block",
        (Kind::Transfer, "crossChainTransfer") => "Transfer",
        (Kind::Tx, "result") => "TransactionResult",
        (Kind::Tx, "cmd") => "TransactionCommand",
        (Kind::Tx, "cmd.meta") => "TransactionMeta",
        (Kind::Tx, "cmd.signers") => "Signer",
        (Kind::Tx, "cmd.signers.clist") => "TransactionCapability",
        (Kind::Tx, "cmd.payload") => {
            let exec = m.contains_key("code");
            let cont = ["pactId", "proof", "rollback", "step"]
                .iter()
                .any(|k| m.contains_key(*k));
            match (exec, cont) {
                (true, false) => "ExecutionPayload",
                (false, true) => "ContinuationPayload",
                _ => return None,
            }
        }
        _ => return None,
    })
}

/// R-P5P9-72 (4): whether a null at the leaf `path` is compared as a value: under a tx's
/// `result` (its checks compare a null) and `cmd` (against the node's signed command).
fn null_is_checked(path: &str) -> bool {
    ["result.", "cmd."].iter().any(|p| path.starts_with(p))
}

/// R-P5P9-75 A: why an ours-only transfer row's `crossChainTransfer` (`x`) is not the node's,
/// or `None` when it is. The row is the node's event `v.event`; the node's TRANSFER_XCHAIN
/// for it has the same sender and amount (a TRANSFER_XCHAIN_RECD the same receiver and
/// amount). Null iff the transaction holds neither for it; otherwise one TRANSFER_XCHAIN,
/// whose target chain and receiver are the selected `chainId` and `receiverAccount`.
pub fn xchain_mismatch(x: &Value, v: &Verdict) -> Option<String> {
    xchain_mismatch_at(x, v.event.as_ref(), v)
}

/// R-P5P9-78 (H1 m-1): [`xchain_mismatch`] for an ours-only transfer row `n` without an index,
/// which is any node TRANSFER with its content: the link must be right against each of them.
/// When they disagree the row is ambiguous, and refused.
pub fn xchain_mismatch_row(x: &Value, n: &Value, v: &Verdict) -> Option<String> {
    let hits: Vec<&Value> = v
        .transfers
        .iter()
        .filter(|e| transfer_matches(n, e, false))
        .collect();
    if hits.len() <= 1 {
        return xchain_mismatch(x, v);
    }
    let whys: Vec<Option<String>> = hits
        .iter()
        .map(|e| xchain_mismatch_at(x, Some(e), v))
        .collect();
    match whys.iter().flatten().next() {
        None => None,
        Some(w) if whys.iter().all(Option::is_some) => Some(w.clone()),
        Some(_) => Some(format!(
            "crossChainTransfer is ambiguous: the row is {} of the node's TRANSFER events, which \
             disagree on its link",
            hits.len()
        )),
    }
}

fn xchain_mismatch_at(x: &Value, ev: Option<&Value>, v: &Verdict) -> Option<String> {
    let Some(ev) = ev else {
        return Some("crossChainTransfer: no node event for the row".into());
    };
    let p = |e: &Value, i: usize| e.pointer(&format!("/params/{i}")).cloned();
    // R-P5P9-95 m-3: a marmalade TRANSFER (token, sender, receiver, amount) and a
    // marmalade cross-chain event (token, sender, receiver, amount, target) are read past
    // their token, and only against an event of the same token.
    let o = transfer_offset(ev);
    let xo = |e: &Value| match e.get("params").and_then(Value::as_array) {
        Some(ps) if ps.len() == 5 => 1,
        _ => 0,
    };
    let pe = |e: &Value, i: usize| p(e, i + xo(e));
    let pt = |i: usize| p(ev, i + o);
    let token = |e: &Value| xo(e) == o && (o == 0 || p(e, 0) == p(ev, 0));
    let same = |e: &Value, i: usize| token(e) && pe(e, i).is_some() && pe(e, i) == pt(i);
    let amount = |e: &Value| {
        pe(e, 2)
            .zip(pt(2))
            .is_some_and(|(a, b)| same_amount(&a, &b, false))
    };
    let name = |e: &Value| e.get("name").and_then(Value::as_str).map(str::to_string);
    let sent: Vec<&Value> = v
        .xchain
        .iter()
        .filter(|e| name(e).as_deref() == Some("TRANSFER_XCHAIN") && same(e, 0) && amount(e))
        .collect();
    let recd = v
        .xchain
        .iter()
        .any(|e| name(e).as_deref() == Some("TRANSFER_XCHAIN_RECD") && same(e, 1) && amount(e));
    if x.is_null() {
        return (!sent.is_empty() || recd).then(|| {
            "crossChainTransfer is null; the node's transaction holds a cross-chain event for \
             the transfer"
                .into()
        });
    }
    let [e] = sent.as_slice() else {
        return Some(format!(
            "crossChainTransfer is set; the node's transaction holds {} TRANSFER_XCHAIN for the \
             transfer",
            sent.len()
        ));
    };
    let ok = |k: &str, i: usize| {
        sel(x, k).is_none_or(|y| text(y).is_some() && text(y) == pe(e, i).as_ref().and_then(text))
    };
    if !ok("chainId", 3) {
        return Some("crossChainTransfer.chainId is not the node's target chain".into());
    }
    if !ok("receiverAccount", 1) {
        return Some("crossChainTransfer.receiverAccount is not the node's receiver".into());
    }
    // R-P5P9-86 T: the receiving half's sender is the sending half's receiver on the node,
    // the empty account of coin's `transfer-crosschain`.
    // R-P5P9-95 m-2: that constant is coin's; another module's sender is refused.
    let leg = pt(1).as_ref().and_then(text);
    let coin = module_name(ev).as_deref() == Some("coin");
    if sel(x, "senderAccount")
        .is_some_and(|y| !coin || text(y).is_none() || text(y) != leg || leg.as_deref() != Some(""))
    {
        return Some(
            "crossChainTransfer.senderAccount is not the node's (the sending leg's receiver)"
                .into(),
        );
    }
    None
}

/// R-P5P9-86 T: whether the document `q` selects `... on ExecutionPayload` and
/// `... on ContinuationPayload`. R-P5P9-95 m-1: `None` (unknown, so a bare payload is
/// refused) when it holds a named spread (`...` and a name other than `on`) or a fragment
/// on either member: the text does not show what those select.
pub fn payload_members(q: &str) -> Option<(bool, bool)> {
    let named = q.split("...").skip(1).any(|f| {
        let id: String = f
            .trim_start()
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        !id.is_empty() && id != "on"
    });
    let on_member = q.split("fragment").skip(1).any(|f| {
        let head = f.split('{').next().unwrap_or("");
        head.contains("ExecutionPayload") || head.contains("ContinuationPayload")
    });
    if named || on_member {
        return None;
    }
    let has = |m: &str| {
        q.split("...")
            .skip(1)
            .any(|f| f.trim_start().starts_with(m))
    };
    Some((has("on ExecutionPayload"), has("on ContinuationPayload")))
}

/// R-P5P9-86 T: why ours' `cmd.payload` `m`, which selects no field of its member (only
/// `__typename`, or nothing), is not the node's command `node`'s, or `None`. Right only when
/// the document's fragments (`members`: whether it selects `... on ExecutionPayload` and
/// `... on ContinuationPayload`) select the other member alone, and `__typename` (when
/// selected) is the node's member.
pub fn bare_payload_mismatch(
    m: &Map<String, Value>,
    node: &Value,
    members: Option<(bool, bool)>,
) -> Option<String> {
    let pl = node.get("payload");
    let (exec, cont) = (
        pl.and_then(|p| p.get("exec")).is_some(),
        pl.and_then(|p| p.get("cont")).is_some(),
    );
    let Some((sel_exec, sel_cont)) = members else {
        return Some("cmd.payload (the document's fragments are unknown)".into());
    };
    let (name, ok) = match (exec, cont) {
        (true, false) => ("ExecutionPayload", !sel_exec && sel_cont),
        (false, true) => ("ContinuationPayload", sel_exec && !sel_cont),
        _ => return Some("cmd.payload (the node's payload has no single member)".into()),
    };
    if !ok {
        return Some(format!(
            "cmd.payload (no field of the node's {name}, which the document selects)"
        ));
    }
    match m.get("__typename") {
        Some(t) if t.as_str() != Some(name) => Some("cmd.payload.__typename".into()),
        _ => None,
    }
}

/// R-P5P9-70 (I-1): the first field a row's node selects that the harness does not check
/// against the node (dotted path, list indices left out), or `None`. An empty object, a null
/// where an object is selected and a list or object where a value is selected are unchecked
/// too (an empty `cmd.payload` among them); so is a row without a kind. R-P5P9-71 (3): a
/// `__typename` at any checked level passes when it is the schema type there ([`type_at`]).
/// R-P5P9-72 (4): so is a null leaf, except under `result` and `cmd` ([`null_is_checked`]).
pub fn unchecked_field(kind: Option<Kind>, n: &Value) -> Option<String> {
    fn walk(kind: Kind, v: &Value, path: &str) -> Option<String> {
        let keys = checked_keys(kind, path);
        match v {
            // R-P5P9-86 T: a payload with no field of its member (the document's fragments
            // select the other member's) is checked by [`bare_payload_mismatch`].
            Value::Object(m)
                if kind == Kind::Tx
                    && path == "cmd.payload"
                    && m.keys().all(|k| k == "__typename") =>
            {
                None
            }
            Value::Object(m) if !m.is_empty() => m.iter().find_map(|(k, x)| {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                if k == "__typename" {
                    let want = type_at(kind, path, m);
                    return (want.is_none() || x.as_str() != want).then_some(p);
                }
                if !keys.contains(&k.as_str()) {
                    return Some(p);
                }
                if x.is_null() && kind == Kind::Transfer && p == "crossChainTransfer" {
                    // R-P5P9-75 A: a null link is checked by [`xchain_mismatch`].
                    None
                } else if x.is_null() && kind == Kind::Transfer && p == "tokenId" {
                    // R-P5P9-86 T: a null token is checked by [`token_is`].
                    None
                } else if !checked_keys(kind, &p).is_empty() {
                    walk(kind, x, &p)
                } else if x.is_object() || x.is_array() {
                    Some(p)
                } else if x.is_null() && !null_is_checked(&p) {
                    // R-P5P9-72 (4): the field checks read a null as "not selected".
                    Some(p)
                } else {
                    None
                }
            }),
            Value::Array(a) if matches!(path, "cmd.signers" | "cmd.signers.clist") => {
                a.iter().find_map(|x| walk(kind, x, path))
            }
            _ if path.is_empty() => Some("node".into()),
            // An empty object, a null where an object is selected, or a scalar there.
            _ => Some(path.to_string()),
        }
    }
    match kind {
        Some(k) => walk(k, n, ""),
        None => Some("node (no kind)".into()),
    }
}

/// R-P5P9-70 (I-1): where a row selects one identity part more than once (its relay `id`'s
/// parts against its `requestKey`/`hash`/`blockHash`/`block.hash`/`orderIndex`/`chainId`
/// fields, and every height field against the others), the first part whose values differ;
/// also an `id` that does not decode as the row's kind. Block rows are compared field by
/// field with the header ([`block_fields`]).
pub fn id_conflict(e: &Value, kind: Option<Kind>) -> Option<String> {
    let n = e.get("node").unwrap_or(&Value::Null);
    let kind = kind.filter(|k| *k != Kind::Block)?;
    let tv = |v: &Value| {
        if v.is_null() {
            Some("null".to_string())
        } else {
            text(v)
        }
    };
    // (part, source, value text; `None` for a list or an object).
    let mut got: Vec<(&str, String, Option<String>)> = Vec::new();
    for (part, ptrs) in [
        ("request key", &["/requestKey", "/hash"][..]),
        (
            "block",
            &["/blockHash", "/block/hash", "/result/block/hash"][..],
        ),
        (
            "chain",
            &[
                "/chainId",
                "/block/chainId",
                "/result/block/chainId",
                "/cmd/meta/chainId",
            ][..],
        ),
        ("index", &["/orderIndex"][..]),
        (
            "height",
            &[
                "/height",
                "/result/height",
                "/block/height",
                "/result/block/height",
            ][..],
        ),
        ("module hash", &["/moduleHash"][..]),
    ] {
        for p in ptrs {
            if *p == "/hash" && kind != Kind::Tx {
                continue;
            }
            if let Some(v) = n.pointer(p) {
                got.push((part, p[1..].replace('/', "."), tv(v)));
            }
        }
    }
    if let Some(id) = n.get("id") {
        let (ty, names): (&str, &[&str]) = match kind {
            Kind::Tx => ("Transaction", &["block", "request key"]),
            Kind::Transfer => (
                "Transfer",
                &["block", "chain", "index", "module hash", "request key"],
            ),
            _ => ("Event", &["block", "index", "request key"]),
        };
        let parts = id
            .as_str()
            .and_then(decode_id)
            .filter(|(t, _)| t == ty)
            .and_then(|(_, k)| id_parts(&k))
            .filter(|p| p.len() == names.len());
        let Some(parts) = parts else {
            return Some(format!("its id does not decode as a {ty} id"));
        };
        for (part, v) in names.iter().zip(parts) {
            got.push((part, "id".into(), Some(v)));
        }
    }
    for (i, (part, src, v)) in got.iter().enumerate() {
        if v.is_none() {
            return Some(format!("its {src} is not a value"));
        }
        if let Some((_, src0, v0)) = got[..i].iter().find(|(p, _, _)| p == part) {
            if v0 != v {
                let o = |x: &Option<String>| x.clone().unwrap_or_default();
                return Some(format!(
                    "its {part} differs: {src0} {}, {src} {}",
                    o(v0),
                    o(v)
                ));
            }
        }
    }
    None
}

/// The module hash a transfer row's relay id holds (its fourth part), when it selects `id`.
fn transfer_id_module(n: &Value) -> Option<String> {
    let (t, k) = n.get("id")?.as_str().and_then(decode_id)?;
    let parts = id_parts(&k).filter(|p| p.len() == 5 && t == "Transfer")?;
    parts.get(3).cloned()
}

/// The request key of a block payload's coinbase output, when it decodes.
fn coinbase_key(payload: &Value) -> Option<String> {
    let cb = payload.get("coinbase")?.as_str()?;
    let b = kdx_types::hash::b64url_decode(cb).ok()?;
    let v: Value = serde_json::from_slice(&b).ok()?;
    v.get("reqKey")?.as_str().map(str::to_string)
}

/// R-P5P9-86 S′: the heights either side of a stored record's height that its window holds.
pub const SKEW_WINDOW: u64 = 10;

/// R-P5P9-86 S′: what the block's coinbase pays `account` (a coin TRANSFER from the empty
/// account), in fixed point.
pub(crate) fn coinbase_paid(payload: &Value, account: &str) -> Option<i128> {
    let cb = payload.get("coinbase")?.as_str()?;
    let b = kdx_types::hash::b64url_decode(cb).ok()?;
    let v: Value = serde_json::from_slice(&b).ok()?;
    v.get("events")?.as_array()?.iter().find_map(|e| {
        let p = e.get("params")?.as_array()?;
        let ok = e.get("name").and_then(Value::as_str) == Some("TRANSFER")
            && module_name(e).as_deref() == Some("coin")
            && p.len() == 3
            && p[0].as_str() == Some("")
            && p[1].as_str() == Some(account);
        ok.then(|| text(&p[2]).as_deref().and_then(crate::rewards::fixed))
            .flatten()
    })
}

/// The node's `meta.creationTime` in whole seconds, as ours renders it (R-P5P9-62 H9: Pact's
/// `ParsedInteger`, a fraction rounded half to even; `{"int": n}` unwrapped).
fn node_secs(v: &Value) -> Option<u64> {
    let v = match v {
        Value::Object(o) => o.get("int")?,
        v => v,
    };
    let t = text(v)?;
    let t = t.trim();
    if let Ok(s) = t.parse::<u64>() {
        return Some(s);
    }
    let digits = |x: &str| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit());
    if let Some((i, f)) = t.split_once('.').filter(|(i, f)| digits(i) && digits(f)) {
        let s: u64 = i.parse().ok()?;
        let f = f.trim_end_matches('0');
        let up = match f.as_bytes().first() {
            Some(b'6'..=b'9') => true,
            Some(b'5') => f.len() > 1 || s % 2 == 1,
            _ => false,
        };
        return s.checked_add(u64::from(up));
    }
    let f = t.parse::<f64>().ok()?;
    ((0.0..9.0e15).contains(&f) && f.fract() == 0.0).then_some(f as u64)
}

/// The first checked `cmd` field of ours' row `c` (its `cmd` object) that is not the node's
/// signed command `node`, in ours' encodings (`code` and the JSON texts quoted, the times as
/// `.mmmZ`, the integers as text); `None` when all are the node's. Run only on a row
/// [`unchecked_field`] passes.
pub fn cmd_mismatch(c: &Value, node: &Value) -> Option<String> {
    let null = Value::Null;
    let at = |v: Option<&Value>, k: &str| v.and_then(|v| v.get(k)).unwrap_or(&null).clone();
    let txt = |x: &Value, y: &Value| x == y || json_field(x) == *y;
    let meta = node.get("meta");
    if let Some(m) = c.get("meta").and_then(Value::as_object) {
        for (k, x) in m {
            let y = at(meta, k);
            let ok = match k.as_str() {
                // R-P5P9-71 (3): checked by [`unchecked_field`] against the schema type.
                "__typename" => true,
                "sender" => *x == y,
                "chainId" | "gasLimit" | "ttl" => same_amount(x, &y, false),
                "gasPrice" => same_amount(x, &y, true),
                "creationTime" => node_secs(&y)
                    .and_then(|s| s.checked_mul(1_000_000))
                    .is_some_and(|us| time_is(x, Some(us), false)),
                _ => false,
            };
            if !ok {
                return Some(format!("cmd.meta.{k}"));
            }
        }
    }
    for k in ["networkId", "nonce"] {
        if c.get(k).is_some_and(|x| *x != at(Some(node), k)) {
            return Some(format!("cmd.{k}"));
        }
    }
    let pl = node.get("payload");
    let exec = pl.and_then(|p| p.get("exec"));
    let cont = pl.and_then(|p| p.get("cont"));
    if let Some(p) = c.get("payload").and_then(Value::as_object) {
        for (k, x) in p {
            let ok = match k.as_str() {
                "__typename" => true,
                "code" => exec.is_some() && json_field(x) == at(exec, "code"),
                "data" => {
                    let y = at(exec.or(cont), "data");
                    same_value(&json_field(x), &y, false)
                }
                "pactId" | "proof" => cont.is_some() && txt(x, &at(cont, k)),
                "step" => cont.is_some() && same_amount(x, &at(cont, "step"), false),
                "rollback" => cont.is_some() && *x == at(cont, "rollback"),
                _ => false,
            };
            if !ok {
                return Some(format!("cmd.payload.{k}"));
            }
        }
    }
    if let Some(ss) = c.get("signers") {
        let ns: &[Value] = node
            .get("signers")
            .and_then(Value::as_array)
            .map_or(&[], Vec::as_slice);
        let Some(ss) = ss.as_array().filter(|s| s.len() == ns.len()) else {
            return Some("cmd.signers".into());
        };
        for (s, n) in ss.iter().zip(ns) {
            for (k, x) in s.as_object().into_iter().flatten() {
                let ok = match k.as_str() {
                    "__typename" => true,
                    "pubkey" => *x == at(Some(n), "pubKey"),
                    "address" => *x == at(Some(n), "addr"),
                    "clist" => {
                        let nc: &[Value] = n
                            .get("clist")
                            .and_then(Value::as_array)
                            .map_or(&[], Vec::as_slice);
                        let Some(xc) = x.as_array().filter(|x| x.len() == nc.len()) else {
                            return Some("cmd.signers.clist".into());
                        };
                        for (cx, cn) in xc.iter().zip(nc) {
                            for (ck, cv) in cx.as_object().into_iter().flatten() {
                                let ok = match ck.as_str() {
                                    "__typename" => true,
                                    "name" => *cv == at(Some(cn), "name"),
                                    "args" => {
                                        same_value(&json_field(cv), &at(Some(cn), "args"), false)
                                    }
                                    _ => false,
                                };
                                if !ok {
                                    return Some(format!("cmd.signers.clist.{ck}"));
                                }
                            }
                        }
                        true
                    }
                    _ => false,
                };
                if !ok {
                    return Some(format!("cmd.signers.{k}"));
                }
            }
        }
    }
    None
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
        x.xchain = events
            .iter()
            .filter(|e| {
                matches!(
                    e.get("name").and_then(Value::as_str),
                    Some("TRANSFER_XCHAIN" | "TRANSFER_XCHAIN_RECD")
                )
            })
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
                // R-P5P9-86 NC: the node placed the key in the row's block, so a content
                // difference is unsettled, never "not canonical".
                with(
                    v(Canon::Unknown, "the node's result is not the row's"),
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
            v(Canon::Unknown, "the node's transaction lacks the row"),
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
    poll_verdict_at(r, n, p, oracle, bh, false)
}

/// [`poll_verdict`], with `gone`: the node does not hold the row's block (a header-by-hash
/// 404, read twice). R-P5P9-86 M: the oracle's row in a block the node does not hold, while the
/// node holds the key in another, is an orphan by block; the oracle's row with only a height,
/// another than the node's for the key, is an orphan by height ([`decide`] then requires ours
/// to list the row at the node's block).
pub fn poll_verdict_at(
    r: &RowId,
    n: &Value,
    p: &Value,
    oracle: bool,
    bh: Option<u64>,
    gone: bool,
) -> Verdict {
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
                // R-P5P9-75 M: the oracle's row in a block the node does not hold the key in,
                // at another known height: an orphan by block.
                Some(_) if oracle => v(
                    Canon::No,
                    "the row's block is not the node's for the key, at another height: an \
                     orphan by block",
                ),
                None if oracle && gone => v(
                    Canon::No,
                    "the node does not hold the row's block (a header-by-hash 404, read twice) \
                     and holds the key in another: an orphan by block",
                ),
                _ => v(
                    not_held(oracle),
                    "the row's block is not the node's for the key, at another height",
                ),
            };
        }
    }
    if r.height.is_some_and(|h| Some(h) != ph) {
        if oracle && r.block.is_none() && ph.is_some() {
            return v(
                Canon::No,
                &format!("{HEIGHT_ORPHAN}: the row has only a height, another than the node's for the key"),
            );
        }
        return v(not_held(oracle), "the node's height for the key is another");
    }
    let mut out = content_verdict(r, n, p, oracle, v);
    // R-P5P9-75 E: an oracle transfer row whose (sender, receiver, amount) the key's canonical
    // transaction on the node does not hold is off the node's chain. R-P5P9-78 (H1 I-2, H2
    // m-3): the triple alone, the module fields left out (a row that differs from the node
    // in its module only is not off the chain).
    let full = ["senderAccount", "receiverAccount", "amount"]
        .iter()
        .all(|k| sel(n, k).is_some());
    let triple = |ev: &Value| {
        let o = transfer_offset(ev);
        let p = |i: usize| ev.pointer(&format!("/params/{}", i + o));
        p(0) == sel(n, "senderAccount")
            && p(1) == sel(n, "receiverAccount")
            && p(2)
                .zip(sel(n, "amount"))
                .is_some_and(|(y, x)| same_amount(x, y, true))
    };
    // R-P5P9-78 (H1 m-2): only on the node's word: an entry without `events` says nothing.
    let events = p.get("events").is_some_and(Value::is_array);
    if oracle
        && full
        && events
        && r.kind == Some(Kind::Transfer)
        && out.canon == Canon::Unknown
        && out.matches == 0
        && out.why.ends_with("the node's transaction lacks the row")
        && !out.transfers.iter().any(triple)
    {
        out.canon = Canon::No;
        out.why = format!("poll: {NOT_IN_NODE_TX}: the node's transaction holds no TRANSFER with the row's sender, receiver and amount");
    }
    out
}

/// R-P5P9-86 M: an oracle row with only a height, another than the node's for its key.
pub const HEIGHT_ORPHAN: &str = "an orphan by height";

/// R-P5P9-75 E: the class of an oracle row off the node's chain by its transaction.
pub const NOT_IN_NODE_TX: &str = "oracle-row-not-in-node-tx";

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
    // R-P5P9-86 C: ours' key is the node's coinbase key byte for byte (in either of the
    // node's eras). The oracle's is excused only in one of its forms for this same header.
    let node_key = cb.get("reqKey").and_then(Value::as_str);
    if node_key != r.rk.as_deref() {
        let form =
            r.rk.as_deref()
                .filter(|_| oracle && r.coinbase)
                .and_then(|k| oracle_coinbase_form(k, hd));
        let Some(form) = form else {
            return v(
                not_held(oracle),
                "the key is neither polled nor the block's coinbase key",
            );
        };
        let out = content_verdict(r, n, &cb, oracle, v);
        return Verdict {
            why: format!("{} (the oracle's coinbase key form: {form})", out.why),
            ..out
        };
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
                // R-P5P9-71 (3): the parent's `__typename` is the constant `Block`.
                let other = |p: &Map<String, Value>| {
                    p.iter()
                        .find(|(f, v)| *f != "hash" && !(*f == "__typename" && *v == "Block"))
                        .map(|(f, _)| f.clone())
                };
                if let Some(f) = x.as_object().and_then(other) {
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
    /// R-P5P9-86 U: `publicKey`, checked against the signers of the node's command.
    pub public_key: Option<String>,
    /// R-P5P9-67 I-C: the root's other arguments with a non-null value or a literal (`first`,
    /// `after`, `last` and `before` aside). The harness does not check them, so they refuse
    /// the ours-only excuse.
    pub unchecked: Vec<String>,
}

/// The root arguments [`Filter`] reads (from variables only).
const READ_ARGS: [&str; 13] = [
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
    "publicKey",
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
    // R-P5P9-81 (m-2): an identifier right after a `:` at depth 1 is the field half of an
    // alias, never a response key.
    let mut field_next = false;
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
            b':' if depth == 1 && paren == 0 => field_next = true,
            c if (c.is_ascii_alphabetic() || c == b'_') && depth == 1 && paren == 0 => {
                let j = ident(i);
                if std::mem::take(&mut field_next) || &query[i..j] != key {
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
            c if !c.is_ascii_whitespace() && c != b',' => field_next = false,
            _ => {}
        }
        i += 1;
    }
    None
}

/// R-P5P9-78 (H1 m-3): root field `key` (its alias or its name) in document `query`: the field
/// it names, and every alias its selection set uses below it (the harness's own `kdxKey`
/// aside; a named fragment spread, which the harness does not read, counts as `...Name`).
/// `None` when the root is not found.
pub fn root_selection(query: &str, key: &str) -> Option<(String, Vec<String>)> {
    let b = query.as_bytes();
    let ident = |mut i: usize| {
        while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
            i += 1;
        }
        i
    };
    let space = |mut i: usize| {
        while i < b.len() && (b[i].is_ascii_whitespace() || b[i] == b',') {
            i += 1;
        }
        i
    };
    // Past a string or a comment at `i`, else `i`.
    let skip = |mut i: usize| match b.get(i) {
        Some(b'"') => {
            i += 1;
            while i < b.len() && b[i] != b'"' {
                i += if b[i] == b'\\' { 2 } else { 1 };
            }
            i + 1
        }
        Some(b'#') => {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            i
        }
        _ => i,
    };
    let (mut depth, mut paren, mut i) = (0i32, 0i32, 0usize);
    // R-P5P9-81 (m-2): an identifier right after a `:` at depth 1 is the field half of an
    // alias, never a response key (as in `root_args`).
    let mut field_next = false;
    let start = loop {
        let j = skip(i);
        if j != i {
            i = j;
            continue;
        }
        match *b.get(i)? {
            b'{' => depth += 1,
            b'}' => depth -= 1,
            b'(' => paren += 1,
            b')' => paren -= 1,
            b':' if depth == 1 && paren == 0 => field_next = true,
            c if (c.is_ascii_alphabetic() || c == b'_') && depth == 1 && paren == 0 => {
                let j = ident(i);
                if !std::mem::take(&mut field_next) && &query[i..j] == key {
                    break j;
                }
                i = j;
                continue;
            }
            c if !c.is_ascii_whitespace() && c != b',' => field_next = false,
            _ => {}
        }
        i += 1;
    };
    let mut k = space(start);
    let mut field = key.to_string();
    if b.get(k) == Some(&b':') {
        let f = space(k + 1);
        k = ident(f);
        field = query[f..k].to_string();
    }
    let mut aliases = Vec::new();
    let (mut depth, mut paren, mut i) = (0i32, 0i32, space(k));
    while i < b.len() {
        let j = skip(i);
        if j != i {
            i = j;
            continue;
        }
        match b[i] {
            b'{' if paren == 0 => depth += 1,
            b'}' if paren == 0 => {
                depth -= 1;
                if depth <= 0 {
                    break;
                }
            }
            b'(' => paren += 1,
            b')' => paren -= 1,
            b'.' if paren == 0 && query[i..].starts_with("...") => {
                let n = space(i + 3);
                let e = ident(n);
                if e > n && &query[n..e] != "on" {
                    aliases.push(format!("...{}", &query[n..e]));
                }
                i = e.max(i + 3);
                continue;
            }
            c if (c.is_ascii_alphabetic() || c == b'_') && paren == 0 => {
                let e = ident(i);
                let name = &query[i..e];
                if depth > 0
                    && b.get(space(e)) == Some(&b':')
                    && name != crate::graphql_scope::KEY_ALIAS
                {
                    aliases.push(name.to_string());
                }
                i = e;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    Some((field, aliases))
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
            public_key: s("publicKey"),
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
            (
                "publicKey",
                self.public_key.is_some() && kind != Some(Kind::Tx),
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
            // R-P5P9-78 (H1 I-3): a row without an index is any node TRANSFER with its
            // content, so the filter holds against those, never against the first one.
            Some(Kind::Transfer) if !r.id.indexed() => {
                if (self.account.is_some() || self.fungible.is_some())
                    && self.transfers_inside(n, v) == 0
                {
                    return fail(if self.account.is_some() {
                        "account: no node TRANSFER with the row's content names it"
                    } else {
                        "fungible: no node TRANSFER with the row's content is of it"
                    });
                }
            }
            Some(Kind::Transfer) => {
                if let Some(a) = &self.account {
                    // R-P5P9-95 m-3: a marmalade TRANSFER's accounts follow its token.
                    let off = ev.map_or(0, transfer_offset);
                    let from = field("senderAccount").or_else(|| param(off));
                    let to = field("receiverAccount").or_else(|| param(off + 1));
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
            Some(Kind::Tx)
                if self.public_key.is_some() && {
                    // R-P5P9-86 U: a signer of the node's command holds the key.
                    let pk = self.public_key.as_ref();
                    !matches!(&v.signers, Some(Ok(ks)) if ks.iter().any(|k| Some(k) == pk))
                } =>
            {
                return match &v.signers {
                    Some(Ok(_)) => fail("publicKey: no signer of the node's transaction holds it"),
                    Some(Err(w)) => Some(format!(
                        "under a publicKey filter the node did not settle ({w})"
                    )),
                    None => Some("under a filter the harness does not check: publicKey".into()),
                };
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

    /// R-P5P9-78 (H1 I-3): how many of the node's TRANSFER events are ours' transfer row `n`
    /// (every field it selects) and lie inside the filter's account and fungible (by the
    /// row's own fields where it selects them, else by the event's).
    fn transfers_inside(&self, n: &Value, v: &Verdict) -> usize {
        let field = |k: &str| sel(n, k).and_then(text);
        let param = |e: &Value, i: usize| e.pointer(&format!("/params/{i}")).and_then(text);
        v.transfers
            .iter()
            .filter(|e| transfer_matches(n, e, false))
            .filter(|e| {
                self.account.as_ref().is_none_or(|a| {
                    let off = transfer_offset(e);
                    field("senderAccount").or_else(|| param(e, off)).as_ref() == Some(a)
                        || field("receiverAccount")
                            .or_else(|| param(e, off + 1))
                            .as_ref()
                            == Some(a)
                })
            })
            .filter(|e| {
                self.fungible.as_ref().is_none_or(|f| {
                    field("moduleName").or_else(|| module_name(e)).as_ref() == Some(f)
                })
            })
            .count()
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

/// R-P5P9-75 D: whether ours' value `x` is the oracle's `y` at `path` (a JSON pointer from
/// the body root), exactly or, at a leaf, by a ruled value matcher of `rules`
/// (`oracle_f64_rounded`, H9's `creation_time_half_even`). With `dec`, an amount field
/// ([`crate::diff::is_amount_key`]) also matches by exact decimal value (R-P5P9-98); the
/// caller then settles ours' row on the node.
fn same_ruled(
    x: &Value,
    y: &Value,
    path: &str,
    rules: &[crate::config::FieldRule],
    dec: bool,
) -> bool {
    use crate::config::FieldRuleKind as K;
    if x == y {
        return true;
    }
    match (x, y) {
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(k, v)| {
                    b.get(k)
                        .is_some_and(|w| same_ruled(v, w, &format!("{path}/{k}"), rules, dec))
                })
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .enumerate()
                    .all(|(i, (v, w))| same_ruled(v, w, &format!("{path}/{i}"), rules, dec))
        }
        _ if dec
            && path
                .rsplit('/')
                .next()
                .is_some_and(crate::diff::is_amount_key)
            && crate::diff::same_exact_decimal(x, y) =>
        {
            true
        }
        _ => {
            let txt = |v: &Value| match v {
                Value::Number(n) => json!(n.to_string()),
                v => v.clone(),
            };
            rules
                .iter()
                .filter(|r| matches!(r.rule, K::OracleF64Rounded | K::CreationTimeHalfEven))
                .filter(|r| crate::graphql_norm::field_matches(path, &r.field))
                .any(|r| crate::graphql_norm::rule_explains(r, &txt(x), &txt(y), false).is_some())
        }
    }
}

/// Both pages' rows, identified.
#[derive(Debug, Clone)]
pub struct Prep {
    pub a: Vec<Row>,
    pub b: Vec<Row>,
    pub a_cut: bool,
    pub b_cut: bool,
    /// R-P5P9-75 D: the oracle-pair field rules whose value matchers rows without an index
    /// match under, and the root list's response key (the rules' paths start there).
    pub rules: Vec<crate::config::FieldRule>,
    pub root: String,
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
    prepare_in(ca, cb, "")
}

/// A row's identity as [`prepare_in`] reads it on its own side under the root list `root`
/// (R-P5P9-75 F's key-only transfer rows; R-P5P9-86 C's coinbase rows).
pub fn prepared_id(e: &Value, root: &str) -> RowId {
    let mut id = row_id(e);
    if id.kind.is_none() && root == "transfers" && id.rk.is_some() {
        id.kind = Some(Kind::Transfer);
    }
    // R-P5P9-86 C: a key that is not a 43-character hash is a coinbase key.
    id.coinbase = id.kind != Some(Kind::Block) && id.rk.as_deref().is_some_and(is_coinbase_key);
    id
}

/// [`prepare`] under the root list `root`. R-P5P9-75 F: a row of the `transfers` root that
/// selects a request key but no field naming its kind is a transfer row without an index
/// (identity by request-key multiset).
pub fn prepare_in(ca: &Value, cb: &Value, root: &str) -> Result<Prep, String> {
    let ids = |c: &Value| -> Result<Vec<(RowId, Value)>, String> {
        let mut out = Vec::new();
        for e in c
            .get("edges")
            .and_then(Value::as_array)
            .ok_or("a page without edges")?
        {
            out.push((prepared_id(e, root), e.clone()));
        }
        Ok(out)
    };
    let (ia, mut ib) = (ids(ca)?, ids(cb)?);
    // R-P5P9-86 C: an oracle key that is the bare parent hash ours' coinbase key decodes to is
    // that coinbase row in the oracle's bare-hash form (its header is checked on the node).
    let parents: std::collections::HashSet<String> = ia
        .iter()
        .filter(|(id, _)| id.coinbase)
        .filter_map(|(id, _)| id.rk.as_deref().and_then(decode_coinbase_rk))
        .collect();
    for (id, _) in ib.iter_mut() {
        if id.kind != Some(Kind::Block) && id.rk.as_ref().is_some_and(|k| parents.contains(k)) {
            id.coinbase = true;
        }
    }
    let rows = |xs: Vec<(RowId, Value)>| -> Result<Vec<Row>, String> {
        let mut out = Vec::new();
        for (id, e) in xs {
            let key = id.key().ok_or_else(|| {
                format!("a row whose identity cannot be decided: {}", id.evidence())
            })?;
            out.push(Row { id, key, edge: e });
        }
        Ok(out)
    };
    Ok(Prep {
        a: rows(ia)?,
        b: rows(ib)?,
        a_cut: more(ca),
        b_cut: more(cb),
        rules: Vec::new(),
        root: String::new(),
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
            match free.iter().position(|&j| p.same(node_of(&p.a[i]), &p.b[j])) {
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
    /// Whether ours' row node `x` is the oracle's row `rb` in every selected field
    /// (R-P5P9-63 (3)), after the ruled value matchers (R-P5P9-75 D). A coinbase row's key is
    /// left out: both sides' keys are checked on the coinbase path (R-P5P9-86 C).
    fn same(&self, x: &Value, rb: &Row) -> bool {
        self.same_by(x, rb, true)
    }

    /// R-P5P9-98: ours' row `x` is the oracle's `rb` only by an amount's exact decimal value
    /// (its text differs): the pair is settled on the node (ours' row's TRANSFER amount).
    fn decimal_only(&self, x: &Value, rb: &Row) -> bool {
        self.same_by(x, rb, true) && !self.same_by(x, rb, false)
    }

    fn same_by(&self, x: &Value, rb: &Row, dec: bool) -> bool {
        let y = node_of(rb);
        let root = self.root.replace('~', "~0").replace('/', "~1");
        let path = format!("/data/{root}/edges/0/node");
        match (x.get("requestKey"), y.get("requestKey")) {
            (Some(kx), Some(_)) if rb.id.coinbase => {
                let mut y = y.clone();
                y["requestKey"] = kx.clone();
                same_ruled(x, &y, &path, &self.rules, dec)
            }
            _ => same_ruled(x, y, &path, &self.rules, dec),
        }
    }

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
        // R-P5P9-98: a pair equal only as exact decimals: ours' row on the node.
        for &(i, j) in &m.pairs {
            if self.decimal_only(node_of(&self.a[i]), &self.b[j]) && !out.contains(&(0, i)) {
                out.push((0, i));
            }
        }
        // R-P5P9-86 C: a coinbase pair whose keys differ: both sides on the node.
        for &(i, j) in &m.pairs {
            if self.b[j].id.coinbase && self.b[j].id.rk != self.a[i].id.rk {
                if !out.contains(&(0, i)) {
                    out.push((0, i));
                }
                out.push((1, j));
            }
        }
        out
    }

    /// Ours' rows the oracle does not list (over all rows): the rows whose `cmd` fields are
    /// checked against the node (R-P5P9-68 (2)).
    pub fn ours_only(&self) -> Vec<usize> {
        let all_a: Vec<usize> = (0..self.a.len()).collect();
        let all_b: Vec<usize> = (0..self.b.len()).collect();
        matching(self, &all_a, &all_b).ours
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
    /// R-P5P9-75 C: paired oracle rows whose encoded coinbase key the node confirmed.
    pub coinbase_keys: Vec<usize>,
    /// R-P5P9-78 (H2 I-2): the oracle rows settled off the node's chain by E or M.
    pub off_chain: Vec<Value>,
    /// R-P5P9-81 (m-1): the one-sided rows settled without an index (D, F), per side.
    pub unindexed_ours: Vec<Value>,
    pub unindexed_oracle: Vec<Value>,
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

/// R-P5P9-81 (m-1): a side's one-sided rows counted per settle class (their `why`):
/// [`SETTLE_CLASSES`] always, any other class when present. `key-only` is the side's rows
/// settled without an index (those [`UNINDEXED_PATH`] lists).
pub fn settle_counts(rows: &[Value], key_only: usize) -> Value {
    let mut m: BTreeMap<String, u64> = SETTLE_CLASSES
        .iter()
        .map(|c| ((*c).to_string(), 0))
        .collect();
    for w in rows
        .iter()
        .filter_map(|r| r.get("why").and_then(Value::as_str))
    {
        *m.entry(w.to_string()).or_default() += 1;
    }
    m.insert("key-only".into(), key_only as u64);
    json!(m)
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
fn after_our_cut(prefix: Option<&Result<Longer, String>>, p: &Prep, rb: &Row) -> Place {
    let ours = &p.a;
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
        .any(|(k, n)| *k == rb.key && (rb.id.indexed() || p.same(n, rb)));
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
    // R-P5P9-97 W: a newest-first cut list whose every row of ours is above the oracle's
    // ceiling (ours near the tip, the oracle frozen) still has a lowest listed row: an oracle
    // row below it is below our window. Every such row of ours has its own height (checked
    // above for the live ones; the others are above the ceiling by that height).
    if d.lowest.is_none() && !w.asc && live_a.is_empty() {
        d.lowest = ha.iter().flatten().min().copied();
    }
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
        // R-P5P9-98: a pair equal only as exact decimals is a pair once the node holds ours'
        // row (its TRANSFER amount equal as an exact decimal); otherwise undecided.
        if p.decimal_only(node_of(ra), &p.b[j]) && canon(&va[i]) != Canon::Yes {
            d.undecided.push(format!(
                "a pair equal only as exact decimals whose row of ours the node did not \
                 settle ({}): {}",
                why_of(vi),
                ra.id.evidence()
            ));
            continue;
        }
        d.pairs.push((i, j));
        // R-P5P9-86 C: a coinbase pair whose keys differ (paired by place): ours' key is the
        // node's coinbase key, and the oracle's is one of its forms for that same header.
        if p.b[j].id.coinbase && p.b[j].id.rk != ra.id.rk {
            let same_block = |v: &Verdict, w: &Verdict| v.block.is_some() && v.block == w.block;
            match (va[i].as_ref(), vb[j].as_ref()) {
                (Some(x), Some(v))
                    if x.canon == Canon::Yes && v.canon == Canon::Yes && same_block(x, v) =>
                {
                    d.coinbase_keys.push(j)
                }
                (Some(x), _) if x.canon == Canon::No => {
                    d.rejected
                        .push((Some(ra.edge.clone()), None, not_canonical(i)))
                }
                (Some(x), _) if x.canon != Canon::Yes => d.undecided.push(format!(
                    "ours' coinbase requestKey the node did not settle ({}): {}",
                    x.why,
                    ra.id.evidence()
                )),
                (_, Some(v)) if v.canon == Canon::No => d.rejected.push((
                    None,
                    Some(p.b[j].edge.clone()),
                    format!(
                        "an oracle coinbase requestKey the node does not hold ({})",
                        v.why
                    ),
                )),
                (_, v) => d.undecided.push(format!(
                    "an oracle coinbase requestKey the node did not settle ({}): {}",
                    why_of(v),
                    p.b[j].id.evidence()
                )),
            }
        }
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
        // R-P5P9-70 (I-1): every field the row selects must have been checked against the
        // node; identity fields selected more than once must agree (R-P5P9-68 (2) for `cmd`).
        if let Some(why) = id_conflict(&ra.edge, ra.id.kind) {
            d.undecided.push(format!(
                "an ours-only row whose identity fields disagree ({why}): {}",
                ra.id.evidence()
            ));
            continue;
        }
        if let Some(f) = unchecked_field(ra.id.kind, node_of(ra)) {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!("an ours-only canonical row: unchecked field: {f}"),
            ));
            continue;
        }
        let n = node_of(ra);
        let checks = [
            (
                ra.id.kind == Some(Kind::Tx) && n.get("cmd").is_some(),
                &v.cmd,
                "cmd",
            ),
            (
                ra.id.kind == Some(Kind::Transfer) && n.get("creationTime").is_some(),
                &v.ctime,
                "creationTime",
            ),
        ];
        let mut settled = true;
        for (_, check, what) in checks.iter().filter(|c| c.0) {
            match check {
                Some(CmdCheck::Same) => continue,
                Some(CmdCheck::Differs(f)) => d.rejected.push((
                    Some(ra.edge.clone()),
                    None,
                    format!("an ours-only row whose {f} is not the node's"),
                )),
                Some(CmdCheck::Unsettled(w)) => d.undecided.push(format!(
                    "an ours-only row whose {what} the node did not settle ({w}): {}",
                    ra.id.evidence()
                )),
                None => d.rejected.push((
                    Some(ra.edge.clone()),
                    None,
                    format!("an ours-only canonical row: unchecked field: {what}"),
                )),
            }
            settled = false;
            break;
        }
        if !settled {
            continue;
        }
        // R-P5P9-75 B: ours' `result.block.canonical` is H7's own verdict on the cut's branch.
        if let Some(x) = n
            .pointer("/result/block/canonical")
            .filter(|_| ra.id.kind == Some(Kind::Tx))
        {
            match (x, &v.branch) {
                (Value::Bool(true), Some(Ok(true))) => {}
                (_, Some(Err(w))) => {
                    d.undecided.push(format!(
                        "an ours-only row whose result.block.canonical the node did not settle \
                         ({w}): {}",
                        ra.id.evidence()
                    ));
                    continue;
                }
                (x, b) => {
                    let h7 = match b {
                        Some(Ok(true)) => "canonical on the cut's branch",
                        Some(Ok(false)) => "not the cut branch's block at its height",
                        _ => "not read",
                    };
                    d.rejected.push((
                        Some(ra.edge.clone()),
                        None,
                        format!(
                            "an ours-only row whose result.block.canonical {x} is not H7's \
                             verdict ({h7})"
                        ),
                    ));
                    continue;
                }
            }
        }
        // R-P5P9-75 A: `crossChainTransfer` against the node's cross-chain events.
        if let Some(why) = n
            .get("crossChainTransfer")
            .filter(|_| ra.id.kind == Some(Kind::Transfer))
            .and_then(|x| {
                if ra.id.indexed() {
                    xchain_mismatch(x, v)
                } else {
                    xchain_mismatch_row(x, n, v)
                }
            })
        {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!("an ours-only row whose {why}"),
            ));
            continue;
        }
        // A transfer's relay id holds its module hash: the node's event's.
        if let Some(mh) = transfer_id_module(n) {
            let node_mh = v.event.as_ref().and_then(|e| e.get("moduleHash"));
            if node_mh.and_then(Value::as_str) != Some(mh.as_str()) {
                d.rejected.push((
                    Some(ra.edge.clone()),
                    None,
                    "an ours-only row whose id (its module hash) is not the node's".into(),
                ));
                continue;
            }
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
        // R-P5P9-78 (H1 I-3): a transfer row counts the node's TRANSFERs inside the filter.
        let held = if ra.id.kind == Some(Kind::Transfer) {
            w.filter.transfers_inside(n, v)
        } else {
            v.matches
        };
        if !ra.id.indexed() && copies > held {
            d.rejected.push((
                Some(ra.edge.clone()),
                None,
                format!(
                    "ours lists the row {copies} times; the node's transaction holds it {held} \
                     times inside the query's filter"
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
        // R-P5P9-81 (m-1): a row without an index (F's key-only identity) also shows apart.
        if !ra.id.indexed() {
            d.unindexed_ours.push(evidence(ra, vi, why));
        }
        d.ours.push(evidence(ra, vi, why));
    }
    // R-P5P9-75 D: per oracle row content without an index, how many of its copies the node
    // does not hold (the oracle's count less the node's): that many leftover copies are
    // node-confirmed duplicates.
    let mut spare: HashMap<(String, String), usize> = HashMap::new();
    for g in &m.oracle {
        let j = g[0];
        let (rb, vj) = (&p.b[j], vb[j].as_ref());
        if !rb.id.indexed() && canon(&vb[j]) == Canon::Yes {
            let sig = (rb.key.clone(), node_of(rb).to_string());
            let left = spare.entry(sig).or_insert_with(|| {
                let copies = gb[rb.key.as_str()]
                    .iter()
                    .filter(|&&x| node_of(&p.b[x]) == node_of(rb))
                    .count();
                copies.saturating_sub(vj.map_or(usize::MAX, |v| v.matches))
            });
            if *left > 0 {
                *left -= 1;
                // R-P5P9-81 (m-1): D's duplicate without an index also shows apart.
                d.unindexed_oracle.push(evidence(rb, vj, "duplicate"));
                d.oracle.push(evidence(rb, vj, "duplicate"));
                continue;
            }
        }
        // R-P5P9-70 (I-1): an identity that disagrees with itself settles nothing.
        if let Some((k, why)) = g
            .iter()
            .find_map(|&k| id_conflict(&p.b[k].edge, p.b[k].id.kind).map(|w| (k, w)))
        {
            d.undecided.push(format!(
                "an oracle-only row whose identity fields disagree ({why}): {}",
                p.b[k].id.evidence()
            ));
            continue;
        }
        match canon(&vb[j]) {
            Canon::No => {
                // R-P5P9-78 (H1 I-1): E's verdict is on g[0]'s content, so it holds only for
                // the rows of the group with that content; a block verdict holds for the key.
                let by_content = vj.is_some_and(|v| v.why.contains(NOT_IN_NODE_TX));
                let class = if by_content { NOT_IN_NODE_TX } else { "orphan" };
                // R-P5P9-86 M: an orphan by height only when ours lists the row at the node's
                // block for the key.
                let by_height = vj.is_some_and(|v| v.why.contains(HEIGHT_ORPHAN));
                // R-P5P9-97 M #1997: ours' row that selects no block hash and was paired (so not
                // read) lists the key at the node's block when it sits on the node's chain at
                // the node's height for the key.
                let ours_at_node = p.a.iter().zip(va).any(|(ra, x)| {
                    ra.id.rk == rb.id.rk
                        && match x {
                            Some(x) => {
                                x.canon == Canon::Yes
                                    && x.block.is_some()
                                    && x.block == vj.and_then(|v| v.block.clone())
                            }
                            None => vj.is_some_and(|v| {
                                ra.id.block.is_none()
                                    && ra.id.height.is_some()
                                    && ra.id.height == v.height
                                    && ra.id.chain.is_some()
                                    && ra.id.chain == v.chain
                            }),
                        }
                });
                if by_height && !ours_at_node {
                    d.undecided.push(format!(
                        "an oracle row with only a height, another than the node's for the key, \
                         that ours does not list at the node's block: {}",
                        rb.id.evidence()
                    ));
                    continue;
                }
                for &k in g {
                    if by_content && node_of(&p.b[k]) != node_of(rb) {
                        d.undecided.push(format!(
                            "an oracle row of other content under the same key and index, \
                             not read on the node: {}",
                            p.b[k].id.evidence()
                        ));
                    } else {
                        d.oracle.push(evidence(&p.b[k], vj, class));
                        if by_content || vj.is_some_and(|v| v.why.contains("an orphan by")) {
                            d.off_chain.push(evidence(&p.b[k], vj, class));
                        }
                    }
                }
            }
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
                    (true, Some(bd), Some(h)) if h == bd => after_our_cut(w.prefix, p, rb),
                    (true, Some(_), Some(_)) => Place::Inside(
                        "an oracle-only canonical row strictly inside our window".into(),
                    ),
                    _ => Place::Inside(
                        "an oracle-only canonical row without a height to place".into(),
                    ),
                };
                match place {
                    Place::Outside(why) => {
                        // R-P5P9-81 (m-1): rows without an index also show apart.
                        if !rb.id.indexed() {
                            d.unindexed_oracle.push(evidence(rb, vj, why));
                            d.unindexed_oracle
                                .extend(g[1..].iter().map(|&j| evidence(&p.b[j], vj, "duplicate")));
                        }
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
    /// R-P5P9-86 T: whether the document selects `... on ExecutionPayload` and
    /// `... on ContinuationPayload` (`None`: no document).
    pub(crate) payload_members: Option<(bool, bool)>,
}

impl Reads {
    /// R-P5P9-70 (m-1): drops the cached cut and the canonical entry at `(chain, h)`, so the
    /// next [`Ctx::h7_canonical`] reads the node's current cut.
    pub(crate) fn forget_cut(&mut self, chain: u8, h: u64) {
        self.cut = None;
        self.canonical.remove(&(chain, h));
    }
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
/// R-P5P9-88 (F): the root list of `body` that selects `edges` but no `pageInfo`.
fn bare_root(body: &Value) -> Option<String> {
    body.get("data")?
        .as_object()?
        .iter()
        .find(|(_, v)| v.get("edges").is_some_and(Value::is_array) && v.get("pageInfo").is_none())
        .map(|(k, _)| k.clone())
}

/// R-P5P9-97 F: gives the root list `root` of `data` (the response's `data` object) that
/// selects `edges` but no `pageInfo` the `pageInfo` of R-P5P9-88's rule for page size
/// `first`: a next page when it holds min(`first`, ours' cap) rows. Anything else is left.
fn bare_page_info(data: &mut Value, root: &str, first: u64) {
    if let Some(c) = data.get_mut(root) {
        if c.get("pageInfo").is_none() && c.get("edges").is_some_and(Value::is_array) {
            let rows = c["edges"].as_array().map_or(0, Vec::len) as u64;
            c["pageInfo"] = json!({"hasNextPage": rows >= first.min(OURS_PAGE_CAP)});
        }
    }
}

/// The page size `first` of root list `root`: its own argument (a literal or a set
/// variable), else the variable the document passes to `first`.
fn root_first(query: Option<&str>, root: &str, vars: &Map<String, Value>) -> Option<u64> {
    match query.and_then(|q| root_args(q, root)) {
        Some(args) => {
            args.iter()
                .find(|(n, _)| n == "first")
                .and_then(|(_, v)| match v.strip_prefix('$') {
                    Some(var) => vars.get(var).and_then(Value::as_u64),
                    None => v.parse::<u64>().ok(),
                })
        }
        None => vars.get(&arg_var(query, "first")).and_then(Value::as_u64),
    }
}

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
        // R-P5P9-88 (F): a root list without `pageInfo` (Transfers-8c8a0f56) is a connection
        // of page size `first`: cut when it holds min(first, ours' cap) rows, else complete.
        // The `pageInfo` stands in for H7 only and is taken off again after it.
        let query = op.map(|o| o.query.as_str());
        let bare = [&*a, &*b].iter().find_map(|x| bare_root(&x.body));
        let first = bare.as_deref().and_then(|r| root_first(query, r, vars));
        let (Some(root), Some(f)) = (bare, first) else {
            return self
                .gql_window_referee_conn(ta, op, vars, a, b, max, st)
                .await;
        };
        let mut added = [false, false];
        for (x, add) in [&mut *a, &mut *b].into_iter().zip(added.iter_mut()) {
            if let Some(c) = x.body.pointer_mut(&format!("/data/{root}")) {
                if c.get("pageInfo").is_none() && c.get("edges").is_some_and(Value::is_array) {
                    let rows = c["edges"].as_array().map_or(0, Vec::len) as u64;
                    let cut = rows >= f.min(OURS_PAGE_CAP);
                    c["pageInfo"] = json!({"hasNextPage": cut, "hasPreviousPage": false});
                    *add = true;
                }
            }
        }
        let out = self
            .gql_window_referee_conn(ta, op, vars, a, b, max, st)
            .await;
        for (x, add) in [&mut *a, &mut *b].into_iter().zip(added) {
            if add {
                if let Some(c) = x
                    .body
                    .pointer_mut(&format!("/data/{root}"))
                    .and_then(Value::as_object_mut)
                {
                    c.remove("pageInfo");
                }
            }
        }
        out
    }

    /// [`Ctx::gql_window_referee`] on a root list that is a connection.
    #[allow(clippy::too_many_arguments)]
    async fn gql_window_referee_conn(
        &self,
        ta: &Target,
        op: Option<&GqlOp>,
        vars: &Map<String, Value>,
        a: &mut Answer,
        b: &mut Answer,
        max: u64,
        st: &mut Reads,
    ) -> (Vec<DiffEntry>, Option<String>) {
        let conn = |ans: &mut Answer| {
            ans.body
                .get_mut("data")
                .and_then(crate::graphql::root_connection)
                .cloned()
        };
        let (Some(ca), Some(cb)) = (conn(a), conn(b)) else {
            // R-P5P9-86 F: a root list without `pageInfo` (Transfers-8c8a0f56) is no window
            // H7 can read; say so.
            let bare = |ans: &Answer| {
                ans.body
                    .get("data")
                    .and_then(Value::as_object)
                    .is_some_and(|d| {
                        d.values().any(|v| {
                            v.get("edges").is_some_and(Value::is_array)
                                && v.get("pageInfo").is_none()
                        })
                    })
            };
            // R-P5P9-88 (F): only when `first` is missing (else it is a connection above).
            let note = (bare(a) || bare(b))
                .then(|| "H7 not applied: the root list selects no pageInfo".to_string());
            return (Vec::new(), note);
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
        st.payload_members = query.and_then(payload_members);
        let first_var = arg_var(query, "first");
        let root = a.body.get("data").and_then(root_name);
        // R-P5P9-64 I-3, R-P5P9-67 I-A (a): a cut page of ours shorter than first × the pages
        // fetched (a follow page that failed, or a short page) is not a window, and a diff:
        // never the common prefix. The one exception is ours' Q10 cap, a page of exactly
        // min(first, 100) × pages. R-P5P9-70 (I-2): `first` is the root's own argument, a
        // literal or a set variable (else, when the root's arguments do not read, the
        // variable the document passes to `first`).
        let first = match query
            .zip(root.as_deref())
            .and_then(|(q, r)| root_args(q, r))
        {
            Some(args) => args.iter().find(|(n, _)| n == "first").and_then(|(_, v)| {
                match v.strip_prefix('$') {
                    Some(var) => vars.get(var).and_then(Value::as_u64),
                    None => v.parse::<u64>().ok(),
                }
            }),
            None => vars.get(&first_var).and_then(Value::as_u64),
        };
        let pages = a.ms.len() as u64;
        let rows = |c: &Value| c.get("edges").and_then(Value::as_array).map_or(0, Vec::len) as u64;
        let (rows_a, rows_b) = (rows(&ca), rows(&cb));
        if more(&ca) {
            match first {
                Some(f)
                    if pages > 0
                        && rows_a < f * pages
                        && rows_a != f.min(OURS_PAGE_CAP) * pages =>
                {
                    let note = format!(
                        "H7 not explained: our cut page holds {rows_a} rows, fewer than first {f} \
                         × {pages} pages; a short page is not a window"
                    );
                    let ev = json!({"rows": rows_a, "first": f, "pages": pages});
                    return (vec![entry(SHORT_PAGE_PATH, Some(ev), None)], Some(note));
                }
                // R-P5P9-70 (I-2): no page size to hold ours' page against.
                None if rows_a < rows_b => {
                    let note = format!(
                        "H7 not explained: page size unknown, and our cut page holds {rows_a} \
                         rows, fewer than the oracle's {rows_b}; a short page is not a window"
                    );
                    let ev = json!({"rows": rows_a, "first": "page size unknown",
                        "oracleRows": rows_b, "pages": pages});
                    return (vec![entry(SHORT_PAGE_PATH, Some(ev), None)], Some(note));
                }
                _ => {}
            }
        }
        // R-P5P9-78 (H1 m-3): E, F and the matchers read the rows by field name, so a row
        // selection with an alias is refused, and F reads the root's field, not its alias.
        let (field, aliases) = query
            .zip(root.as_deref())
            .and_then(|(q, r)| root_selection(q, r))
            .unwrap_or_else(|| (root.clone().unwrap_or_default(), Vec::new()));
        if !aliases.is_empty() {
            return (
                Vec::new(),
                Some(format!(
                    "H7 not applied: the row selection has an alias ({}); H7 reads field names",
                    aliases.join(", ")
                )),
            );
        }
        let prep = match prepare_in(&ca, &cb, &field) {
            Ok(mut p) => {
                // R-P5P9-75 D: rows without an index match after the ruled value matchers.
                p.rules = self.cfg.graphql.field_rules.clone();
                p.root = root.clone().unwrap_or_default();
                p
            }
            // R-P5P9-75 F: always say why H7 did not run.
            Err(why) => return (Vec::new(), Some(format!("H7 not applied: {why}"))),
        };
        let asc = root.as_deref() == Some("blocksFromHeight");
        let filter = Filter::of(query, root.as_deref().or(Some("")), vars);
        let (node0, ours0) = (st.node, st.ours);
        let mut va: Vec<Option<Verdict>> = vec![None; prep.a.len()];
        let mut vb: Vec<Option<Verdict>> = vec![None; prep.b.len()];
        let ours_only = prep.ours_only();
        for (side, i) in prep.reads() {
            let r = if side == 0 { &prep.a[i] } else { &prep.b[i] };
            let mut v = self.h7_verdict(ta, r, side == 1, st).await;
            if side == 0 && ours_only.contains(&i) {
                self.h7_cmd_check(r, &mut v, st).await;
                self.h7_ctime_check(r, &mut v, st).await;
                self.h7_branch_check(r, &mut v, st).await;
                self.h7_signers_check(r, &mut v, &filter, st).await;
            }
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
                    self.h7_prefix(ta, op, vars, &first_var, prep.a.len(), &field, d.lowest, st)
                        .await
                }
                None => Err("no document to read it with".into()),
            };
            w.prefix = Some(&ext);
            d = decide(&prep, &va, &vb, &w);
        }
        // R-P5P9-75 N: a page cut inside one block, where H7 does not explain the pair.
        let mut n_note = String::new();
        if !d.explained() || !d.grouped {
            match self.n_single_block(ta, query, vars, &prep, st).await {
                Ok((pairs, ev)) => {
                    let side = |rows: &[Row], ours: bool| -> Vec<Value> {
                        pairs
                            .iter()
                            .enumerate()
                            .map(|(n, (i, j))| tagged(&rows[if ours { *i } else { *j }].edge, n))
                            .collect()
                    };
                    let (ea, eb) = (side(&prep.a, true), side(&prep.b, false));
                    for (ans, edges) in [(&mut *a, ea), (&mut *b, eb)] {
                        if let Some(c) = ans
                            .body
                            .get_mut("data")
                            .and_then(crate::graphql::root_connection)
                        {
                            c["edges"] = Value::Array(edges);
                        }
                    }
                    let (nr, or) = (st.node - node0, st.ours - ours0);
                    let ours = json!({"rows": prep.a.len(), "inNodeOrder": true});
                    let mut ev = ev;
                    ev["nodeReads"] = json!(nr);
                    ev["oursReads"] = json!(or);
                    return (vec![entry(SINGLE_BLOCK_PATH, Some(ours), Some(ev))], None);
                }
                Err(Some(why)) => n_note = format!("; N not explained: {why}"),
                Err(None) => {}
            }
        }
        let (nr, or) = (st.node - node0, st.ours - ours0);
        let reads = format!("{nr} node reads, {or} reads of ours");
        let ev_a = json!({"rows": prep.a.len(), "cut": prep.a_cut, "boundaryHeight": d.lowest,
            "ascending": asc, "oneSided": d.ours,
            "settled": settle_counts(&d.ours, d.unindexed_ours.len())});
        let ev_b = json!({"rows": prep.b.len(), "cut": prep.b_cut,
            "lastCreationTime": d.oracle_lowest_ctime, "nodeReads": nr,
            "oursReads": or, "oneSided": d.oracle,
            "settled": settle_counts(&d.oracle, d.unindexed_oracle.len())});
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
            let note = format!("H7 not explained ({reads}): {}{n_note}", whys.join("; "));
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
        let mut eb: Vec<Value> = d
            .pairs
            .iter()
            .enumerate()
            .map(|(n, (_, j))| tag(&prep.b[*j].edge, n))
            .collect();
        // R-P5P9-86 C: a confirmed oracle coinbase key form takes ours' key (the node's).
        let mut keys = Vec::new();
        for (n, (i, j)) in d.pairs.iter().enumerate() {
            let (Some(ours), Some(theirs)) = (&prep.a[*i].id.rk, &prep.b[*j].id.rk) else {
                continue;
            };
            let node = eb[n].get_mut("node");
            if let Some(x) = node.and_then(|x| x.get_mut("requestKey")) {
                if d.coinbase_keys.contains(j) && x.as_str() == Some(theirs.as_str()) {
                    *x = json!(ours);
                    keys.push(json!({"oracle": theirs, "ours": ours,
                        "via": why_of(vb[*j].as_ref())}));
                }
            }
        }
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
        if !keys.is_empty() {
            out.push(entry(
                COINBASE_FORM_PATH,
                None,
                Some(json!({ "keys": keys })),
            ));
        }
        if !d.off_chain.is_empty() {
            out.push(entry(
                OFF_CHAIN_PATH,
                None,
                Some(json!({ "rows": d.off_chain })),
            ));
        }
        if !d.unindexed_ours.is_empty() || !d.unindexed_oracle.is_empty() {
            out.push(entry(
                UNINDEXED_PATH,
                Some(json!({ "rows": d.unindexed_ours })),
                Some(json!({ "rows": d.unindexed_oracle })),
            ));
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
            "H7 not explained ({reads}): {}; evidence: ours {ev_a}, oracle {ev_b}{n_note}",
            whys.join("; ")
        );
        (out, Some(note))
    }

    /// R-P5P9-64 (I-1): one read of ours with the same document and variables, `first` = the
    /// rows on our page + [`PREFIX_EXTRA`], one page with no `after`: each row's identity key
    /// and node.
    #[allow(clippy::too_many_arguments)]
    async fn h7_prefix(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        first_var: &str,
        n: usize,
        root: &str,
        bound: Option<u64>,
        st: &mut Reads,
    ) -> Result<Longer, String> {
        if !vars.get(first_var).is_some_and(Value::is_u64) {
            return Err(format!("no page size ({first_var}) to read it with"));
        }
        let after_var = arg_var(Some(&op.query), "after");
        let mut v = vars.clone();
        v.insert(first_var.to_string(), json!(n + PREFIX_EXTRA));
        v.remove(&after_var);
        let mut rows: Vec<(String, Value)> = Vec::new();
        let mut last_h = None;
        let (mut first_page, mut pages) = (true, 0);
        let (more, short) = loop {
            st.ours += 1;
            pages += 1;
            let mut data = self
                .gql_read(ta, &op.query, Value::Object(v.clone()))
                .await
                .ok_or("the read failed")?;
            // R-P5P9-97 F: a root list without `pageInfo` (Transfers-8c8a0f56) follows
            // R-P5P9-88's rule here too: cut when it holds min(first, ours' cap) rows.
            if crate::graphql::root_connection(&mut data).is_none() {
                bare_page_info(&mut data, root, (n + PREFIX_EXTRA) as u64);
            }
            let conn =
                crate::graphql::root_connection(&mut data).ok_or("the read holds no list")?;
            let more = conn
                .pointer("/pageInfo/hasNextPage")
                .and_then(Value::as_bool);
            let end = conn
                .pointer("/pageInfo/endCursor")
                .filter(|c| c.is_string())
                .cloned();
            let edges = conn
                .get("edges")
                .and_then(Value::as_array)
                .ok_or("the read holds no list")?;
            for e in edges {
                last_h = row_id(e).height;
                rows.push((
                    prepared_id(e, root).key().unwrap_or_default(),
                    e.get("node").cloned().unwrap_or(Value::Null),
                ));
            }
            // R-P5P9-67 m-6: fewer rows than asked, and a next page (or no word on one).
            let short = first_page && rows.len() < n + PREFIX_EXTRA && more != Some(false);
            first_page = false;
            // R-P5P9-86 SK: page on by `endCursor` until the read passes our boundary height.
            let (Some(b), Some(end)) = (bound, end) else {
                break (more, short);
            };
            let past = rows.len() > n && last_h.is_some_and(|h| h != b);
            if past || more != Some(true) || pages >= PREFIX_PAGES || edges.is_empty() {
                break (more, short);
            }
            v.insert(after_var.clone(), end);
        };
        let capped = match bound {
            // Not past the boundary height while the list may go on: capped.
            Some(b) => !(rows.len() > n && last_h.is_some_and(|h| h != b)) && more != Some(false),
            None => short,
        };
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
        // R-P5P9-86 C: a coinbase row is settled on the coinbase path, never /poll.
        let polled = if id.coinbase {
            None
        } else {
            match self.h7_poll(chain, &rk, st).await {
                Ok(p) => p,
                Err(w) => return Verdict::unknown(w),
            }
        };
        if let Some(p) = polled {
            // A row in another block than the node's, without a height: the node's height of
            // its block decides whether that block is an orphan at the key's height.
            let pb = p.pointer("/metaData/blockHash").and_then(Value::as_str);
            let (mut bh, mut gone) = (None, false);
            if let (Some(b), None) = (&id.block, id.height) {
                if pb != Some(b.as_str()) {
                    match self.h7_block_height(chain, b, st).await {
                        Ok(Some(h)) => bh = Some(h),
                        // R-P5P9-86 M: a 404 is read once more before the block counts as one
                        // the node does not hold.
                        Ok(None) if oracle => {
                            st.by_hash.remove(b.as_str());
                            match self.h7_block_height(chain, b, st).await {
                                Ok(h) => (bh, gone) = (h, h.is_none()),
                                Err(w) => return Verdict::unknown(w),
                            }
                        }
                        Ok(None) => {}
                        Err(w) => return Verdict::unknown(w),
                    }
                }
            }
            return poll_verdict_at(id, n, &p, oracle, bh, gone);
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

    /// The node's canonical header at `(chain, h)` (on the cut's branch) and its payload, when
    /// that header is block `block` (the one the node placed the row in); else why not. One
    /// cached payload read per block, within the compare's cap.
    async fn h7_branch_payload(
        &self,
        chain: u8,
        h: u64,
        block: Option<&str>,
        st: &mut Reads,
    ) -> Result<(Value, Value), String> {
        let hd = match self.h7_canonical(chain, h, st).await? {
            Some(hd) => hd,
            None => return Err("no canonical header at the height".into()),
        };
        if hd.get("hash").and_then(Value::as_str) != block {
            return Err("the poll's block is not the cut branch's".into());
        }
        let Some(ph) = hd.get("payloadHash").and_then(Value::as_str) else {
            return Err("the header has no payload hash".into());
        };
        let payload = self.h7_payload(chain, ph, st).await?;
        Ok((hd, payload))
    }

    /// R-P5P9-68 (2): an ours-only tx row that the node places (`v`) and that selects only
    /// checked fields ([`unchecked_field`]), its `cmd` against the node's signed command: from
    /// the payload of the block `/poll` placed it in, that block being the cut branch's at its
    /// height. Sets `v.cmd`; the canonical verdict itself is left as it is. Rows with an
    /// unchecked field are refused in [`decide`] without a read.
    async fn h7_cmd_check(&self, r: &Row, v: &mut Verdict, st: &mut Reads) {
        let n = node_of(r);
        let (Some(chain), Some(rk)) = (v.chain, r.id.rk.as_deref()) else {
            return;
        };
        if r.id.kind != Some(Kind::Tx)
            || v.canon != Canon::Yes
            || n.get("cmd").is_none()
            || unchecked_field(r.id.kind, n).is_some()
        {
            return;
        }
        let c = n.get("cmd").unwrap_or(&Value::Null);
        v.cmd = Some(match self.h7_node_cmd(chain, rk, st).await {
            Err(w) => CmdCheck::Unsettled(w),
            Ok(cmd) => {
                // R-P5P9-86 T: a payload with no field of its member, by the document.
                let bare = c
                    .get("payload")
                    .and_then(Value::as_object)
                    .filter(|m| m.keys().all(|k| k == "__typename"))
                    .and_then(|m| bare_payload_mismatch(m, &cmd, st.payload_members));
                match bare.or_else(|| cmd_mismatch(c, &cmd)) {
                    Some(f) => CmdCheck::Differs(f),
                    None => CmdCheck::Same,
                }
            }
        });
    }

    /// The node's signed command for key `rk` on `chain`: from the payload of the block
    /// `/poll` placed it in, that block being the cut branch's at its height; `Err`: why it
    /// could not be read.
    async fn h7_node_cmd(&self, chain: u8, rk: &str, st: &mut Reads) -> Result<Value, String> {
        let p = self
            .h7_poll(chain, rk, st)
            .await?
            .ok_or("the node's poll does not know the key")?;
        let pb = p.pointer("/metaData/blockHash").and_then(Value::as_str);
        let ph = p
            .pointer("/metaData/blockHeight")
            .and_then(Value::as_u64)
            .ok_or("no block height in the poll entry")?;
        let (_, payload) = self.h7_branch_payload(chain, ph, pb, st).await?;
        node_cmd(&payload, rk).ok_or_else(|| "the block's payload lacks the key".into())
    }

    /// R-P5P9-86 U: an ours-only tx row the node places (`v`) under a `publicKey` filter:
    /// the public keys of the signers of the node's signed command. Sets `v.signers`.
    async fn h7_signers_check(&self, r: &Row, v: &mut Verdict, f: &Filter, st: &mut Reads) {
        if f.public_key.is_none() || r.id.kind != Some(Kind::Tx) || v.canon != Canon::Yes {
            return;
        }
        let (Some(chain), Some(rk)) = (v.chain, r.id.rk.as_deref()) else {
            v.signers = Some(Err("no chain or key for the row".into()));
            return;
        };
        v.signers = Some(self.h7_node_cmd(chain, rk, st).await.map(|cmd| {
            cmd.get("signers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|s| s.get("pubKey").and_then(Value::as_str).map(str::to_string))
                .collect()
        }));
    }

    /// R-P5P9-70 (I-1): an ours-only transfer row that the node places (`v`, its block and
    /// height) and that selects `creationTime`: ours serves its tx's `meta.creationTime` in
    /// whole seconds (H9's rounding), and a coinbase's block time cut to whole seconds, both
    /// as `.000Z`. Read from that block, the cut branch's at its height. Sets `v.ctime`.
    async fn h7_ctime_check(&self, r: &Row, v: &mut Verdict, st: &mut Reads) {
        let n = node_of(r);
        let Some(x) = n.get("creationTime") else {
            return;
        };
        if r.id.kind != Some(Kind::Transfer)
            || v.canon != Canon::Yes
            || unchecked_field(r.id.kind, n).is_some()
        {
            return;
        }
        let unsettled = |w: &str| Some(CmdCheck::Unsettled(w.to_string()));
        let (Some(chain), Some(rk), Some(h)) = (v.chain, r.id.rk.as_deref(), v.height) else {
            v.ctime = unsettled("no chain, key or height for the row");
            return;
        };
        let (hd, payload) = match self
            .h7_branch_payload(chain, h, v.block.as_deref(), st)
            .await
        {
            Ok(x) => x,
            Err(w) => {
                v.ctime = unsettled(&w);
                return;
            }
        };
        let secs = if coinbase_key(&payload).as_deref() == Some(rk) {
            hd.get("creationTime")
                .and_then(Value::as_u64)
                .map(|us| us / 1_000_000)
        } else {
            match node_cmd(&payload, rk) {
                Some(cmd) => cmd.pointer("/meta/creationTime").and_then(node_secs),
                None => {
                    v.ctime = unsettled("the block's payload lacks the key");
                    return;
                }
            }
        };
        v.ctime = Some(match secs.and_then(|s| s.checked_mul(1_000_000)) {
            None => CmdCheck::Unsettled("the node's creationTime does not read".into()),
            Some(us) if time_is(x, Some(us), false) => CmdCheck::Same,
            Some(_) => CmdCheck::Differs("creationTime".into()),
        });
    }

    /// R-P5P9-75 B: an ours-only tx row that the node places (`v`) and that selects
    /// `result.block.canonical`: whether the node's block for it is the canonical one at its
    /// height on the cut's branch. Sets `v.branch`.
    async fn h7_branch_check(&self, r: &Row, v: &mut Verdict, st: &mut Reads) {
        let n = node_of(r);
        if r.id.kind != Some(Kind::Tx)
            || v.canon != Canon::Yes
            || n.pointer("/result/block/canonical").is_none()
            || unchecked_field(r.id.kind, n).is_some()
        {
            return;
        }
        let (Some(chain), Some(h), Some(b)) = (v.chain, v.height, v.block.clone()) else {
            v.branch = Some(Err("no chain, height or block for the row".into()));
            return;
        };
        v.branch = Some(match self.h7_canonical(chain, h, st).await {
            Ok(Some(hd)) => Ok(hd.get("hash").and_then(Value::as_str) == Some(b.as_str())),
            Ok(None) => Err("no canonical header at the height on the cut's branch".into()),
            Err(w) => Err(w),
        });
    }

    /// R-P5P9-86 X: each unchecked decimal-zeros candidate
    /// ([`crate::graphql_norm::decimal_zeros_candidates`]) is settled on the node: ours' row
    /// names a request key and chain, the node's `/poll` entry for it holds a TRANSFER event
    /// that is the row (every other selected field), and that event's amount text equals
    /// ours' text exactly. Then the hit is `/~graphql-oracle-decimal-zeros`; otherwise the
    /// entry goes back to the amount difference, with why.
    pub(crate) async fn gql_decimal_zeros(&self, d: &mut [DiffEntry], st: &mut Reads) {
        use crate::graphql_norm::{DECIMAL_ZEROS_PATH, DECIMAL_ZEROS_UNCHECKED_PATH};
        for e in d
            .iter_mut()
            .filter(|e| e.path == DECIMAL_ZEROS_UNCHECKED_PATH)
        {
            let a = e.a.clone().unwrap_or(Value::Null);
            let ours = a["amount"].as_str().unwrap_or_default().to_string();
            match self.decimal_zeros_node(&a["row"], &ours, st).await {
                Ok(ev) => {
                    e.path = DECIMAL_ZEROS_PATH.into();
                    e.a = Some(json!({"amount": ours, "node": ev}));
                }
                Err(why) => {
                    e.path = a["at"].as_str().unwrap_or("/amount").to_string();
                    e.a = Some(json!(ours));
                    e.b = Some(json!({"oracle": e.b, "why": why}));
                }
            }
        }
    }

    async fn decimal_zeros_node(
        &self,
        row: &Value,
        ours: &str,
        st: &mut Reads,
    ) -> Result<Value, String> {
        // R-P5P9-97 X: the request key and chain from any field naming them, the row's relay
        // id included (a Transfer id holds both).
        let id = row_id(&json!({ "node": row }));
        let rk = id.rk.as_deref().ok_or("ours' row selects no requestKey")?;
        let chain = id.chain.ok_or("ours' row selects no chain")?;
        let poll = self
            .h7_poll(chain, rk, st)
            .await?
            .ok_or("the node does not hold the request key")?;
        let mut other = row.clone();
        if let Some(o) = other.as_object_mut() {
            o.remove("amount");
        }
        // R-P5P9-98: the node's amount (`{"decimal": …}` or text) equals ours' as an exact
        // decimal; this replaces R-P5P9-86 X's text equality.
        let hit = poll
            .get("events")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|ev| transfer_matches(&other, ev, false))
            .find_map(|ev| {
                let at = format!("/params/{}", 2 + transfer_offset(ev));
                let n = ev.pointer(&at)?;
                crate::diff::same_exact_decimal(n, &json!(ours))
                    .then(|| pact_text(n))
                    .flatten()
            });
        match hit {
            Some(t) => Ok(json!({"requestKey": rk, "chain": chain, "amount": t})),
            None => Err(format!(
                "no TRANSFER of the row on the node has ours' amount {ours} as an exact decimal"
            )),
        }
    }

    /// R-P5P9-88 (G): the compare's unfinished nested lists that are prefix candidates
    /// ([`crate::graphql_scope::PREFIX_PATH`]): each block's canonical header on the cut's
    /// branch must be the row's block, and its payload must hold ours' rows by count (and by
    /// request key, in order of the multiset, when ours' rows select one). One payload read
    /// per block, within the compare's cap. When every list of the entry passes, the entry
    /// becomes the prefix hit; otherwise it stays unfinished, each failed list saying why.
    pub(crate) async fn gql_h6_prefix(&self, d: &mut [DiffEntry], st: &mut Reads) {
        use crate::graphql_scope::{PREFIX_PATH, UNFINISHED_PATH};
        for e in d.iter_mut() {
            if e.path != UNFINISHED_PATH || e.b.is_some() {
                continue;
            }
            let Some(lists) =
                e.a.as_ref()
                    .and_then(|a| a.get("lists"))
                    .and_then(Value::as_array)
            else {
                continue;
            };
            if lists.is_empty() || !lists.iter().all(|l| l.get("prefix").is_some()) {
                continue;
            }
            let mut out = Vec::new();
            let mut ok = true;
            for l in lists {
                let mut l = l.clone();
                let c = &l["prefix"];
                let (chain, height, hash) = (
                    c["chain"].as_u64().and_then(|x| u8::try_from(x).ok()),
                    c["height"].as_u64(),
                    c["hash"].as_str().map(str::to_string),
                );
                let rows = c["of"].as_u64().unwrap_or(u64::MAX);
                let keys = c["keys"].as_array().cloned();
                let check = match (chain, height, hash) {
                    (Some(ch), Some(h), Some(b)) => {
                        match self.h7_branch_payload(ch, h, Some(&b), st).await {
                            Err(w) => Err(w),
                            Ok((_, payload)) => block_request_keys(&payload).and_then(|nk| {
                                if nk.len() as u64 != rows {
                                    return Err(format!(
                                        "ours lists {rows} rows, the node's block {} txs",
                                        nk.len()
                                    ));
                                }
                                if let Some(k) = &keys {
                                    let mut ours: Vec<String> = k
                                        .iter()
                                        .filter_map(|x| x.as_str().map(str::to_string))
                                        .collect();
                                    let mut node = nk.clone();
                                    ours.sort();
                                    node.sort();
                                    if ours != node {
                                        return Err("ours' request keys are not the node's \
                                            block's"
                                            .into());
                                    }
                                }
                                // R-P5P9-95 m-4: ours' rows past the prefix, by field.
                                if let Some(f) = c["unchecked"].as_str() {
                                    return Err(format!(
                                        "ours' rows past the prefix select {f}, which the \
                                         node check does not read"
                                    ));
                                }
                                if let Some(cmds) = c["cmds"].as_array() {
                                    prefix_cmds_match(&payload, cmds)?;
                                }
                                // R-P5P9-97 G: out of order, both sides as multisets of
                                // the node's block: ours all of it (the count is checked
                                // above), the oracle's rows each a distinct tx of it.
                                if let Some(m) = c["multiset"].as_object() {
                                    for side in ["ours", "oracle"] {
                                        let cmds = m
                                            .get(side)
                                            .and_then(Value::as_array)
                                            .ok_or("a multiset side without its rows")?;
                                        prefix_cmds_match(&payload, cmds)
                                            .map_err(|w| format!("in any order, {side}: {w}"))?;
                                    }
                                    if let Some(k) = m.get("oracleKeys").and_then(Value::as_array) {
                                        if k.iter().any(|x| {
                                            !x.as_str().is_some_and(|x| nk.iter().any(|n| n == x))
                                        }) {
                                            return Err("an oracle request key is not in the \
                                                node's block"
                                                .into());
                                        }
                                    }
                                }
                                Ok(nk.len())
                            }),
                        }
                    }
                    _ => Err("no block to read".into()),
                };
                match check {
                    Ok(n) => l["node"] = json!({"txs": n}),
                    Err(w) => {
                        ok = false;
                        l["why"] = json!(format!(
                            "a cut nested list of the oracle's that is \
                            ours' prefix, not settled on the node: {w}"
                        ));
                    }
                }
                out.push(l);
            }
            if ok {
                e.path = PREFIX_PATH.into();
                e.a = Some(json!({ "lists": out }));
            } else {
                e.a = Some(json!({ "lists": out }));
            }
        }
    }

    /// R-P5P9-86 TC: a record whose only difference outside the hits is its root
    /// `totalCount` (and no unsettled H7 row) is settled by walking both lists to their end,
    /// [`OURS_PAGE_CAP`] rows a page, at most [`COUNT_WALK_ROWS`] rows a side, and settling
    /// every one-sided row by the H7 rules ([`Ctx::gql_window_referee`] on the two whole
    /// lists, `max` the oracle's height). It is explained (`/~graphql-count-walk`, with H7's
    /// hits) only when every one-sided row is and the difference in the counts equals the
    /// difference in one-sided rows. Past the cap the reason is `count walk capped`
    /// (`/~graphql-count-walk-capped`, never allowlisted). R-P5P9-87: the referee walks ours
    /// bounded at `record_height` (the record's height: rows above it are newer than the
    /// record and left out); live, `u64::MAX`. R-P5P9-92 (2): ours' rows above the oracle's
    /// height `max` (up to `record_height`) are in ours' count but not the oracle's: each must
    /// be node-canonical by `/poll` ([`Ctx::above_tip_rows`]), at most [`ABOVE_TIP_ROWS`],
    /// and counts as a settled one-sided row of ours. R-P5P9-93: the oracle counting the
    /// heights of the query's window is the ACCEPTED rule's predicate
    /// ([`crate::graphql_norm::oracle_counts_window_heights`]). Anything else leaves the
    /// count difference, with why in the note.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn gql_count_walk(
        &self,
        ta: &Target,
        tb: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        d: &mut Vec<DiffEntry>,
        note: &mut Option<String>,
        max: u64,
        record_height: u64,
        st: &mut Reads,
    ) {
        if d.iter().any(|e| {
            never_explained(&e.path)
                || crate::graphql_scope::is_h6_entry(&e.path)
                || e.path.starts_with("/~harness-limit/")
        }) {
            return;
        }
        let raw: Vec<usize> = (0..d.len())
            .filter(|&i| !d[i].path.starts_with("/~"))
            .collect();
        let [i] = raw[..] else {
            return;
        };
        let Some(root) = d[i]
            .path
            .strip_prefix("/data/")
            .and_then(|p| p.strip_suffix("/totalCount"))
            .filter(|r| !r.contains('/'))
            .map(str::to_string)
        else {
            return;
        };
        let count = |v: &Option<Value>| v.as_ref().and_then(Value::as_u64);
        let (Some(na), Some(nb)) = (count(&d[i].a), count(&d[i].b)) else {
            return;
        };
        let walked = self
            .count_walk(ta, tb, op, vars, &root, (na, nb), max, record_height, st)
            .await;
        let why = match walked {
            Ok(hits) => {
                d.remove(i);
                d.extend(hits);
                return;
            }
            Err((true, why)) => {
                d.push(entry(COUNT_WALK_CAPPED_PATH, Some(json!(why)), None));
                why
            }
            Err((false, why)) => why,
        };
        *note = Some(match note.take() {
            Some(n) => format!("{n}; {why}"),
            None => why,
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn count_walk(
        &self,
        ta: &Target,
        tb: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        root: &str,
        (na, nb): (u64, u64),
        max: u64,
        record_height: u64,
        st: &mut Reads,
    ) -> Result<Vec<DiffEntry>, (bool, String)> {
        let q = Some(op.query.as_str());
        let (fv, av) = (arg_var(q, "first"), arg_var(q, "after"));
        if fv.is_empty() || av.is_empty() {
            return Err((
                false,
                "count walk not applied: the document passes no first and after variables".into(),
            ));
        }
        let wa = self.walk_list(ta, op, vars, root, &fv, &av).await?;
        let wb = self.walk_list(tb, op, vars, root, &fv, &av).await?;
        let bounded = record_height < u64::MAX;
        let bound = record_height.min(max);
        // Ours' rows: up to the oracle's height (walked against the oracle), above it up to
        // the record's height (R-P5P9-92 (2)), and above the record (newer: left out).
        let (mut wa_below, mut above) = (Vec::new(), Vec::new());
        for e in wa {
            match prepared_id(&e, root).height {
                Some(h) if h > record_height => {}
                Some(h) if h > bound => above.push((h, e)),
                None if bounded => {
                    return Err((
                        false,
                        "count walk: a row of ours has no height to bound the walk at".into(),
                    ))
                }
                _ => wa_below.push(e),
            }
        }
        // R-P5P9-95 m-9: the compare's reads (H7's among them), under its one cap.
        let n_above = above.len() as i64;
        let (la, lb) = (wa_below.len() as i64, wb.len() as i64);
        // R-P5P9-95 m-5: ours' count is ours' walked rows (an overcount of ours stays a
        // diff, R-P5P9-78 H2 I-4).
        if na as i64 != la + n_above {
            return Err((
                false,
                format!(
                    "count walk: ours counts {na} and lists {la} rows plus {n_above} above the \
                     oracle's height"
                ),
            ));
        }
        if !above.is_empty() {
            self.above_tip_rows(ta, root, max, &above, st).await?;
            // R-P5P9-95 I-1: and each gets the checks an ours-only row gets.
            let rows: Vec<Value> = above.iter().map(|(_, e)| e.clone()).collect();
            if let Err(w) = self
                .above_tip_checks(ta, op, vars, root, &rows, &wa_below, st)
                .await
            {
                return Err((
                    false,
                    format!(
                        "count walk: a row of ours above the oracle's height {max} is not \
                         settled as an ours-only row: {w}"
                    ),
                ));
            }
        }
        let walked: Vec<Option<u64>> = wa_below
            .iter()
            .map(|e| prepared_id(e, root).height)
            .collect();
        let page = |edges: Vec<Value>, n: u64| Answer {
            body: json!({"data": {root: {"totalCount": n,
                "pageInfo": {"hasNextPage": false, "hasPreviousPage": false},
                "edges": edges}}}),
            status: 200,
            raw: String::new(),
            ms: vec![0],
        };
        let (mut a, mut b) = (page(wa_below, na), page(wb, nb));
        // R-P5P9-97 W #567: H8 first, as on the first page, so H7 matches the oracle's walked
        // transfer rows at the node's indexes (its hit is the first page's to show).
        let _ = self.gql_transfer_order(ta, &a, &mut b, st).await;
        let (h7, h7_note) = self
            .gql_window_referee(ta, Some(op), vars, &mut a, &mut b, max, false, st)
            .await;
        if let Some(n) = h7_note {
            return Err((false, format!("count walk: {n}")));
        }
        if h7.iter().any(|e| never_explained(&e.path)) {
            return Err((
                false,
                "count walk: H7 left a one-sided row unsettled".into(),
            ));
        }
        let edges = |x: &Answer| {
            x.body
                .pointer(&format!("/data/{root}/edges"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len) as i64
        };
        let (pa, pb) = (edges(&a), edges(&b));
        let (one_a, one_b) = (la - pa + n_above, lb - pb);
        let counts = na as i64 - nb as i64;
        let ev_a = |one: i64| {
            json!({"count": na, "walked": la, "oneSided": one, "aboveOracleTip": n_above,
                "bound": bounded.then_some(record_height)})
        };
        // R-P5P9-93: every walked row is settled (H7 above); the oracle's count is the
        // number of heights in the query's window, and ours' count the walked rows and the
        // node's per-height counts over it: the ACCEPTED oracle-counts-heights predicate.
        let heights_why = if na != nb && n_above == 0 {
            match self
                .window_heights(op, vars, root, (na, nb), &walked, st)
                .await
            {
                Ok(node) => {
                    let mut out = h7;
                    out.push(entry(
                        COUNT_WALK_PATH,
                        Some(ev_a(one_a)),
                        Some(json!({"count": nb, "walked": lb, "oneSided": one_b,
                            "windowHeights": node.len(), "nodeTxsPerHeight": node,
                            "nodeReads": st.node})),
                    ));
                    out.push(entry(
                        ORACLE_COUNTS_HEIGHTS_PATH,
                        Some(json!(na)),
                        Some(json!(nb)),
                    ));
                    return Ok(out);
                }
                Err(w) => Some(w),
            }
        } else {
            None
        };
        if pa != pb || counts != one_a - one_b {
            let heights = heights_why.map_or(String::new(), |w| {
                format!("; the oracle does not count the window's heights: {w}")
            });
            return Err((
                false,
                format!(
                    "count walk: the counts differ by {counts} (ours {na}, the oracle's {nb}), \
                     the one-sided rows by {} (ours {one_a} of {la} plus {n_above} above the \
                     oracle's height, the oracle's {one_b} of {lb}){heights}",
                    one_a - one_b
                ),
            ));
        }
        let mut out = h7;
        out.push(entry(
            COUNT_WALK_PATH,
            Some(ev_a(one_a)),
            Some(json!({"count": nb, "walked": lb, "oneSided": one_b,
                "nodeReads": st.node})),
        ));
        Ok(out)
    }

    /// R-P5P9-93: the node's tx count at each height of the query's window
    /// (`minHeight..=maxHeight` on its one `chainId`), from the canonical header on the
    /// cut's branch and its payload, when [`crate::graphql_norm::oracle_counts_window_heights`]
    /// holds for them; at most [`WINDOW_HEIGHTS`] heights. `Err`: why not.
    async fn window_heights(
        &self,
        op: &GqlOp,
        vars: &Map<String, Value>,
        root: &str,
        counts: (u64, u64),
        walked: &[Option<u64>],
        st: &mut Reads,
    ) -> Result<Vec<u64>, String> {
        use crate::graphql_norm::oracle_counts_window_heights as holds;
        let rules = &self.cfg.graphql.field_rules;
        let f = Filter::of(Some(&op.query), Some(root), vars);
        let window = f.min_height.zip(f.max_height);
        // The predicate without the node first: no node read for a record it cannot explain.
        if let Err(w) = holds(rules, root, counts, walked, window, &[]) {
            if !w.starts_with("the node's counts") {
                return Err(w);
            }
        }
        let Some((lo, hi)) = window else {
            return Err("the query has no minHeight..maxHeight window".into());
        };
        let chain = match f.chains.as_deref() {
            Some([c]) => *c,
            _ => return Err("the query names no single chainId".into()),
        };
        if hi - lo >= WINDOW_HEIGHTS {
            return Err(format!("the window is wider than {WINDOW_HEIGHTS} heights"));
        }
        let mut node = Vec::new();
        for h in lo..=hi {
            let hd = self
                .h7_canonical(chain, h, st)
                .await?
                .ok_or_else(|| format!("no canonical header at {h} on the cut's branch"))?;
            let ph = hd
                .get("payloadHash")
                .and_then(Value::as_str)
                .ok_or("a header without its payload hash")?;
            let p = self.h7_payload(chain, ph, st).await?;
            let n = p
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or("a payload without its transaction list")?
                .len();
            node.push(n as u64);
        }
        holds(rules, root, counts, walked, window, &node)?;
        Ok(node)
    }

    /// R-P5P9-95 I-1: ours' rows `above` the oracle's tip are ours-only rows: each gets the
    /// checks [`decide`] gives one (its identity, `unchecked_field`, `cmd` and
    /// `creationTime` against the node, `result.block.canonical`, `crossChainTransfer`, the
    /// query's filter with U's signers, and duplicates), and none shares its identity with
    /// a row of ours `below` the tip. `Err`: why not.
    #[allow(clippy::too_many_arguments)]
    async fn above_tip_checks(
        &self,
        ta: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        root: &str,
        above: &[Value],
        below: &[Value],
        st: &mut Reads,
    ) -> Result<(), String> {
        let q = op.query.as_str();
        let (field, aliases) =
            root_selection(q, root).unwrap_or_else(|| (root.to_string(), Vec::new()));
        if !aliases.is_empty() {
            return Err(format!(
                "the row selection has an alias ({})",
                aliases.join(", ")
            ));
        }
        let conn = |rows: Vec<Value>| json!({"edges": rows, "pageInfo": {"hasNextPage": false}});
        let empty = conn(Vec::new());
        let mut all = below.to_vec();
        all.extend(above.iter().cloned());
        let whole = prepare_in(&conn(all), &empty, &field)?;
        let (lo, hi) = whole.a.split_at(below.len());
        if let Some(r) = hi.iter().find(|r| lo.iter().any(|x| x.key == r.key)) {
            return Err(format!(
                "ours also lists it below the oracle's height: {}",
                r.id.evidence()
            ));
        }
        let mut prep = prepare_in(&conn(above.to_vec()), &empty, &field)?;
        prep.rules = self.cfg.graphql.field_rules.clone();
        prep.root = root.to_string();
        st.payload_members = payload_members(q);
        let filter = Filter::of(Some(q), Some(root), vars);
        let mut va = Vec::new();
        for r in &prep.a {
            let mut v = self.h7_verdict(ta, r, false, st).await;
            self.h7_cmd_check(r, &mut v, st).await;
            self.h7_ctime_check(r, &mut v, st).await;
            self.h7_branch_check(r, &mut v, st).await;
            self.h7_signers_check(r, &mut v, &filter, st).await;
            va.push(Some(v));
        }
        let w = Window {
            max: u64::MAX,
            asc: root == "blocksFromHeight",
            filter: &filter,
            prefix: None,
        };
        let d = decide(&prep, &va, &[], &w);
        let mut whys = d.undecided.clone();
        whys.extend(d.rejected.iter().map(|x| x.2.clone()));
        if !d.grouped {
            whys.push("its rows do not group".into());
        }
        if whys.is_empty() && d.ours.len() != prep.a.len() {
            whys.push(format!(
                "{} of its {} rows settled",
                d.ours.len(),
                prep.a.len()
            ));
        }
        if whys.is_empty() {
            Ok(())
        } else {
            Err(whys.join("; "))
        }
    }

    /// R-P5P9-92 (2): ours' rows above the oracle's height `max` (`(height, row)`), each
    /// node-canonical by `/poll`: the node places its key at the row's height, in the block
    /// that is the canonical header there on the cut's branch. More than [`ABOVE_TIP_ROWS`]
    /// is `Err((true, "rows above the oracle tip: capped …"))`; a row that is not, why.
    async fn above_tip_rows(
        &self,
        ta: &Target,
        root: &str,
        max: u64,
        above: &[(u64, Value)],
        st: &mut Reads,
    ) -> Result<(), (bool, String)> {
        if above.len() > ABOVE_TIP_ROWS {
            return Err((
                true,
                format!(
                    "rows above the oracle tip: capped: ours lists {} rows above the oracle's \
                     height {max} (more than {ABOVE_TIP_ROWS})",
                    above.len()
                ),
            ));
        }
        for (h, e) in above {
            let id = prepared_id(e, root);
            let what = id.evidence();
            let r = Row {
                id,
                key: String::new(),
                edge: e.clone(),
            };
            let v = self.h7_verdict(ta, &r, false, st).await;
            let fail = |why: String| {
                Err((
                    false,
                    format!(
                        "count walk: a row of ours above the oracle's height {max} is not \
                         node-canonical ({why}): {what}"
                    ),
                ))
            };
            if v.canon != Canon::Yes || v.height != Some(*h) {
                return fail(format!("{:?} at {:?}: {}", v.canon, v.height, v.why));
            }
            let (Some(chain), Some(b)) = (v.chain, v.block.as_deref()) else {
                return fail("no chain or block from the node".into());
            };
            match self.h7_canonical(chain, *h, st).await {
                Ok(Some(hd)) if hd.get("hash").and_then(Value::as_str) == Some(b) => {}
                Ok(_) => return fail("its block is not the cut branch's at its height".into()),
                Err(w) => return fail(w),
            }
        }
        Ok(())
    }

    /// One whole list by the document, `first` = [`OURS_PAGE_CAP`] and `after` = the last
    /// page's `endCursor` (else its last row's `cursor`); `Err((true, _))` past
    /// [`COUNT_WALK_ROWS`] rows.
    async fn walk_list(
        &self,
        t: &Target,
        op: &GqlOp,
        vars: &Map<String, Value>,
        root: &str,
        first_var: &str,
        after_var: &str,
    ) -> Result<Vec<Value>, (bool, String)> {
        let fail = |why: &str| (false, format!("count walk: {} {why}", t.name));
        let mut out: Vec<Value> = Vec::new();
        let mut after: Option<String> = None;
        let mut bytes = 0usize;
        loop {
            // R-P5P9-92 (9): each page of ours' or the oracle's is one read of the run's
            // budget (the referee's `--max-reads`; unbounded live), its own reason past it.
            if !self.gql.take_node_read() {
                return Err((
                    true,
                    format!(
                        "count walk stopped: the run's read cap is reached before {} was \
                         walked ({} rows read)",
                        t.name,
                        out.len()
                    ),
                ));
            }
            let mut v = vars.clone();
            v.insert(first_var.into(), json!(OURS_PAGE_CAP));
            match &after {
                Some(c) => v.insert(after_var.into(), json!(c)),
                None => v.remove(after_var),
            };
            let data = self
                .gql_read(t, &op.query, Value::Object(v))
                .await
                .ok_or_else(|| fail("answered a page of the walk with no data"))?;
            let c = data
                .get(root)
                .ok_or_else(|| fail("answered a page without the root list"))?;
            let edges = c
                .get("edges")
                .and_then(Value::as_array)
                .ok_or_else(|| fail("answered a page without edges"))?;
            out.extend(edges.iter().cloned());
            // R-P5P9-95 m-10: a byte budget a side, as the rows' JSON text.
            bytes += edges.iter().map(|e| e.to_string().len()).sum::<usize>();
            if bytes > COUNT_WALK_BYTES {
                return Err((
                    true,
                    format!(
                        "count walk capped: {} lists more than {COUNT_WALK_BYTES} bytes",
                        t.name
                    ),
                ));
            }
            let cap = self
                .gql
                .walk_rows
                .load(std::sync::atomic::Ordering::Relaxed);
            if out.len() > cap {
                return Err((
                    true,
                    format!("count walk capped: {} lists more than {cap} rows", t.name),
                ));
            }
            let more = c
                .pointer("/pageInfo/hasNextPage")
                .and_then(Value::as_bool)
                .ok_or_else(|| fail("answered a page without hasNextPage"))?;
            if !more {
                return Ok(out);
            }
            let next = c
                .pointer("/pageInfo/endCursor")
                .and_then(Value::as_str)
                .or_else(|| edges.last()?.get("cursor")?.as_str())
                .map(str::to_string)
                .ok_or_else(|| fail("answered a page with no cursor to page on"))?;
            if edges.is_empty() || after.as_deref() == Some(next.as_str()) {
                return Err(fail("answered a page that does not advance"));
            }
            after = Some(next);
        }
    }

    /// R-P5P9-86 S′ with R-P5P9-87, the stored-record form: a stored NodeBalance record's
    /// gap on chain `chain` (ours' GraphQL `ours`, ours' native `native`) at time `ts` (unix
    /// seconds) is miner skew only when the node shows `account` receiving k of that chain's
    /// coinbases (1 <= k <= 3), all of one amount, in the record's height window (the height
    /// whose block time is nearest `ts`, ± [`SKEW_WINDOW`]) and the gap is k × that amount.
    /// `Err`: why not.
    pub(crate) async fn native_skew_stored(
        &self,
        chain: u8,
        account: &str,
        ours: &str,
        native: &str,
        ts: u64,
    ) -> Result<Value, String> {
        use crate::rewards::fixed;
        let mut st = Reads::default();
        let (Some(x), Some(y)) = (fixed(ours), fixed(native)) else {
            return Err("a balance is not a decimal".into());
        };
        // R-P5P9-95 I-4: the live pair reads GraphQL first, so a skew leaves GraphQL below
        // native only: native − GraphQL = +k × reward.
        let gap = y - x;
        if gap == 0 {
            return Err("no gap".into());
        }
        if gap < 0 {
            return Err("GraphQL is above native; a skew leaves it below".into());
        }
        self.h7_take(&mut st)?;
        let tip = self
            .node
            .cut()
            .await
            .map_err(|e| format!("cut read failed: {e}"))?
            .hashes
            .get(&chain)
            .map(|e| e.height)
            .ok_or("the cut lacks the chain")?;
        // The height whose block time is nearest `ts`, by steps of 30 s a block.
        let mut h = tip;
        // R-P5P9-95 m-8: the search must converge (within a block of `ts`).
        let mut converged = false;
        for _ in 0..6 {
            let hd = self
                .h7_canonical(chain, h, &mut st)
                .await?
                .ok_or("no canonical header at the height")?;
            let t = hd
                .get("creationTime")
                .and_then(Value::as_u64)
                .ok_or("a header without creationTime")?
                / 1_000_000;
            let dh = (t as i64 - ts as i64) / 30;
            if dh.abs() <= 1 {
                converged = true;
                break;
            }
            h = (h as i64 - dh).clamp(0, tip as i64) as u64;
        }
        if !converged {
            return Err("the height search did not converge on the record's time".into());
        }
        let mut paid: Vec<(u64, i128)> = Vec::new();
        for hh in h.saturating_sub(SKEW_WINDOW)..=(h + SKEW_WINDOW).min(tip) {
            let Some(hd) = self.h7_canonical(chain, hh, &mut st).await? else {
                return Err(format!("no canonical header at {hh}"));
            };
            let hash = hd.get("hash").and_then(Value::as_str).map(str::to_string);
            let (_, payload) = self
                .h7_branch_payload(chain, hh, hash.as_deref(), &mut st)
                .await?;
            if let Some(a) = coinbase_paid(&payload, account) {
                paid.push((hh, a));
            }
        }
        let amounts: Vec<i128> = paid.iter().map(|p| p.1).collect();
        let one = amounts
            .first()
            .copied()
            .filter(|a| amounts.iter().all(|b| b == a));
        match one {
            Some(r) if r > 0 && gap % r == 0 && (1..=3).contains(&(gap / r)) => {
                let k = gap / r;
                if (paid.len() as i128) < k {
                    return Err(format!(
                        "the gap is {k} coinbases, the node shows {} in the window",
                        paid.len()
                    ));
                }
                Ok(
                    json!({"chain": chain, "k": k, "window": [h.saturating_sub(SKEW_WINDOW),
                    h + SKEW_WINDOW], "coinbases": paid.iter().map(|p| p.0).collect::<Vec<_>>()}),
                )
            }
            Some(_) => Err("the gap is not 1 to 3 of the account's coinbases".into()),
            None if amounts.is_empty() => {
                Err("the node shows no coinbase to the account in the window".into())
            }
            None => Err("the account's coinbases in the window differ in amount".into()),
        }
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
        // R-P5P9-86 C: a coinbase key (not a hash) is never polled.
        if is_coinbase_key(rk) {
            return Err(format!(
                "a coinbase key of {} characters is never polled",
                rk.len()
            ));
        }
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
    pub(crate) async fn h7_canonical(
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
    pub(crate) async fn h7_headers(
        &self,
        chain: u8,
        h: u64,
        st: &mut Reads,
    ) -> Result<Vec<Value>, String> {
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

    pub(crate) async fn h7_payload(
        &self,
        chain: u8,
        ph: &str,
        st: &mut Reads,
    ) -> Result<Value, String> {
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
// R-P5P9-75 N: a page cut inside one block

/// The first field a one-sided row selects that N does not check: N settles a row's identity
/// only (its relay id, request key, block, chain and height), never other content.
fn beyond_identity(e: &Value) -> Option<String> {
    let Some(edge) = e.as_object() else {
        return Some("an edge that is not an object".into());
    };
    if let Some(k) = edge.keys().find(|k| *k != "cursor" && *k != "node") {
        return Some(k.clone());
    }
    let Some(n) = e.get("node").and_then(Value::as_object) else {
        return Some("node".into());
    };
    for (k, v) in n {
        match k.as_str() {
            "id" | "hash" | "requestKey" | "chainId" | "blockHash" | "height" | "__typename" => {}
            "block" => {
                let Some(b) = v.as_object() else {
                    return Some("block".into());
                };
                if let Some(bk) = b
                    .keys()
                    .find(|bk| !matches!(bk.as_str(), "hash" | "height" | "chainId" | "__typename"))
                {
                    return Some(format!("block.{bk}"));
                }
            }
            _ => return Some(k.clone()),
        }
    }
    None
}

/// The request keys of a node payload's transactions, in the node's order.
pub fn block_request_keys(payload: &Value) -> Result<Vec<String>, String> {
    payload
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or("the payload holds no transaction list")?
        .iter()
        .map(|t| {
            let raw = t
                .get(0)
                .and_then(Value::as_str)
                .and_then(|x| kdx_types::hash::b64url_decode(x).ok())
                .ok_or("a payload transaction that does not decode")?;
            let w: Value = serde_json::from_slice(&raw)
                .map_err(|_| "a payload transaction that is not JSON".to_string())?;
            w.get("hash")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| "a payload transaction without a hash".to_string())
        })
        .collect()
}

/// R-P5P9-75 N on prepared pages that lie inside one node block whose transactions are
/// `node` (request keys in the node's order); `start` is ours' cursor position in that order.
/// Explained iff every row is a transaction whose identity agrees with itself, ours' rows are
/// `node[start..]`'s prefix (the node's order from ours' cursor) and every oracle row is in
/// the block. A one-sided row may select its identity only. Returns the pairs (ours' row, the
/// oracle's row) by request key, in ours' order.
pub fn single_block_cut(
    p: &Prep,
    node: &[String],
    start: usize,
) -> Result<Vec<(usize, usize)>, String> {
    for (side, rows) in [("ours", &p.a), ("the oracle's", &p.b)] {
        for r in rows.iter() {
            if r.id.kind != Some(Kind::Tx) || r.id.rk.is_none() {
                return Err(format!(
                    "a row of {side} that is not a transaction: {}",
                    r.id.evidence()
                ));
            }
            if let Some(w) = id_conflict(&r.edge, r.id.kind) {
                return Err(format!(
                    "a row of {side} whose identity fields disagree ({w}): {}",
                    r.id.evidence()
                ));
            }
        }
    }
    let rk = |r: &Row| r.id.rk.clone().unwrap_or_default();
    let want = node.get(start..start + p.a.len()).ok_or_else(|| {
        format!(
            "ours holds {} rows; the node block holds {} after ours' cursor ({start})",
            p.a.len(),
            node.len().saturating_sub(start)
        )
    })?;
    // R-P5P9-86 N: ours in the node's block order, or in exactly its reverse (from the
    // block's end), from ours' cursor. Any other order is not.
    let reversed: Vec<String> = node
        .iter()
        .rev()
        .skip(start)
        .take(p.a.len())
        .cloned()
        .collect();
    let differs = |want: &[String]| {
        p.a.iter()
            .zip(want)
            .enumerate()
            .find(|(_, (r, k))| rk(r) != **k)
            .map(|(i, (r, k))| (i, rk(r), k.clone()))
    };
    if let Some((i, r, k)) = differs(want) {
        if reversed.len() != p.a.len() || differs(&reversed).is_some() {
            return Err(format!(
                "ours is not the node block's order from its cursor, nor its reverse: row {i} \
                 is {r} where the node has {k}"
            ));
        }
    }
    let held: std::collections::HashSet<&str> = node.iter().map(String::as_str).collect();
    if let Some(r) = p.b.iter().find(|r| !held.contains(rk(r).as_str())) {
        return Err(format!(
            "an oracle row not in the node block: {}",
            r.id.evidence()
        ));
    }
    let mut used = vec![false; p.b.len()];
    let mut pairs = Vec::new();
    for (i, r) in p.a.iter().enumerate() {
        match (0..p.b.len()).find(|&j| !used[j] && p.b[j].id.rk == r.id.rk) {
            Some(j) => {
                used[j] = true;
                pairs.push((i, j));
            }
            None => {
                if let Some(f) = beyond_identity(&r.edge) {
                    return Err(format!(
                        "an ours-only row selects {f}, which N does not check"
                    ));
                }
            }
        }
    }
    if let Some(f) =
        p.b.iter()
            .zip(&used)
            .filter(|(_, u)| !**u)
            .find_map(|(r, _)| beyond_identity(&r.edge))
    {
        return Err(format!(
            "an oracle-only row selects {f}, which N does not check"
        ));
    }
    Ok(pairs)
}

/// A row tagged with its pair's place, so `normalize_gql`'s tie sort keeps the pairs.
fn tagged(e: &Value, n: usize) -> Value {
    let mut m = Map::new();
    m.insert(ALIGN_KEY.into(), json!(format!("{n:06}")));
    if let Some(o) = e.as_object() {
        m.extend(o.iter().map(|(k, v)| (k.clone(), v.clone())));
    }
    Value::Object(m)
}

impl Ctx {
    /// R-P5P9-75 N: when every row of both pages lies in one block (one block hash, chain and
    /// height among the rows that select them) and a page is cut, the node's canonical block
    /// there (the cut branch's; the chain from ours by the block hash, the height by the
    /// node's header-by-hash route when the rows select none) and its payload (one cached
    /// read per block) decide by [`single_block_cut`]. Ours' cursor position is the block's
    /// start when the request has no `after`; a request after a cursor is not settled.
    /// `Err(None)`: N does not apply; `Err(Some(why))`: it applies and does not explain.
    pub(crate) async fn n_single_block(
        &self,
        ta: &Target,
        query: Option<&str>,
        vars: &Map<String, Value>,
        p: &Prep,
        st: &mut Reads,
    ) -> Result<(Vec<(usize, usize)>, Value), Option<String>> {
        if !(p.a_cut || p.b_cut) || p.a.is_empty() {
            return Err(None);
        }
        // R-P5P9-78 (H2 m-5): N's reference is the node's whole block, so the request's
        // filter may name nothing beyond the block, the chain and the height.
        let f = Filter::of(query, Some(p.root.as_str()), vars);
        if f.account.is_some()
            || f.fungible.is_some()
            || f.event.is_some()
            || f.params.is_some()
            || f.rk.is_some()
            || !f.unchecked.is_empty()
        {
            return Err(None);
        }
        let rows = || p.a.iter().chain(&p.b);
        let one = |xs: std::collections::BTreeSet<String>| match xs.len() {
            0 => Ok(None),
            1 => Ok(xs.into_iter().next()),
            _ => Err(None),
        };
        let block = one(rows().filter_map(|r| r.id.block.clone()).collect())?;
        let chain = one(rows()
            .filter_map(|r| r.id.chain.map(|c| c.to_string()))
            .collect())?;
        let height = one(rows()
            .filter_map(|r| r.id.height.map(|h| h.to_string()))
            .collect())?;
        if block.is_none() && height.is_none() {
            return Err(None);
        }
        if vars
            .get(&arg_var(query, "after"))
            .is_some_and(|v| !v.is_null())
        {
            return Err(Some(
                "the request starts after a cursor: ours' position in the block is not known"
                    .into(),
            ));
        }
        let chain: u8 = match chain {
            Some(c) => c.parse().map_err(|_| Some(format!("a chain id {c}")))?,
            None => {
                let id = rows().next().map(|r| r.id.clone()).unwrap_or_default();
                self.h7_chain(ta, &id, st)
                    .await
                    .ok_or_else(|| Some("no chain for the block".to_string()))?
            }
        };
        let h: u64 = match (height, &block) {
            (Some(h), _) => h.parse().map_err(|_| Some(format!("a height {h}")))?,
            (None, Some(b)) => self
                .h7_block_height(chain, b, st)
                .await
                .map_err(Some)?
                .ok_or_else(|| Some("the node does not know the block".to_string()))?,
            (None, None) => return Err(None),
        };
        let hd = self
            .h7_canonical(chain, h, st)
            .await
            .map_err(Some)?
            .ok_or_else(|| Some("no canonical header at the height (above the cut)".into()))?;
        let hash = hd
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| Some("the header has no hash".to_string()))?;
        if block.as_deref().is_some_and(|b| b != hash) {
            return Err(Some(
                "the rows' block is not the cut branch's at its height".into(),
            ));
        }
        let ph = hd
            .get("payloadHash")
            .and_then(Value::as_str)
            .ok_or_else(|| Some("the header has no payload hash".to_string()))?;
        let payload = self.h7_payload(chain, ph, st).await.map_err(Some)?;
        let node = block_request_keys(&payload).map_err(Some)?;
        let pairs = single_block_cut(p, &node, 0).map_err(Some)?;
        let ev = json!({"chain": chain, "height": h, "block": hash, "nodeTxs": node.len(),
            "cursorPosition": 0, "ours": p.a.len(), "oracle": p.b.len(),
            "paired": pairs.len()});
        Ok((pairs, ev))
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

/// R-P5P9-95 m-4: ours' rows' `cmd` subtrees `cmds` matched one to one (a multiset) with
/// the signed commands of the node's block `payload`, each by [`cmd_mismatch`] after
/// [`unchecked_field`]. `Err`: why not.
fn prefix_cmds_match(payload: &Value, cmds: &[Value]) -> Result<(), String> {
    let mut node: Vec<Option<Value>> = payload
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or("the payload holds no transaction list")?
        .iter()
        .map(|t| {
            let raw = kdx_types::hash::b64url_decode(t.get(0)?.as_str()?).ok()?;
            let w: Value = serde_json::from_slice(&raw).ok()?;
            serde_json::from_str(w.get("cmd")?.as_str()?).ok()
        })
        .collect();
    for c in cmds {
        if let Some(f) = unchecked_field(Some(Kind::Tx), &json!({ "cmd": c })) {
            return Err(format!("a row past the prefix: unchecked field: {f}"));
        }
        let hit = node
            .iter()
            .position(|n| n.as_ref().is_some_and(|n| cmd_mismatch(c, n).is_none()));
        match hit {
            Some(i) => node[i] = None,
            None => {
                return Err(
                    "a row past the prefix whose cmd is no unmatched command of the \
                    node's block"
                        .into(),
                )
            }
        }
    }
    Ok(())
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
        let cmd = self.node_cmd_for(chain, rk, st).await?;
        Ok(cmd.pointer("/payload/cont/data") == Some(&Value::Null))
    }

    /// The node's command for `rk` on `chain` (decoded): `/poll` for its block, the headers
    /// at its height, that block's payload (single-row reads under the compare's cap).
    pub(crate) async fn node_cmd_for(
        &self,
        chain: u8,
        rk: &str,
        st: &mut Reads,
    ) -> Result<Value, String> {
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
        node_cmd(&payload, rk).ok_or_else(|| "the block's payload lacks the key".to_string())
    }
}

// ---------------------------------------------------------------------------------------------
// R-P5P9-75 J, K, L: transaction fields against the node's command; O: H8 on nested lists

/// Visible hit of J (`payload.rollback`, allowlist id `graphql-r75-payload-rollback`).
pub const ROLLBACK_PATH: &str = "/~graphql-r75-payload-rollback";
/// Visible hit of L (`signers.address`, allowlist id `graphql-r75-signer-address`).
pub const SIGNER_ADDRESS_PATH: &str = "/~graphql-r75-signer-address";

/// The transaction objects in an answer that select `cmd`: (request key, chain if selected,
/// JSON pointer to the object).
pub fn cmd_txs(v: &Value) -> Vec<(String, Option<u8>, String)> {
    payload_data_txs_by(v, |m| m.get("cmd").is_some_and(Value::is_object), "")
}

fn payload_data_txs_by(
    v: &Value,
    pick: fn(&serde_json::Map<String, Value>) -> bool,
    suffix: &str,
) -> Vec<(String, Option<u8>, String)> {
    fn walk(
        v: &Value,
        ptr: &mut String,
        pick: fn(&serde_json::Map<String, Value>) -> bool,
        suffix: &str,
        out: &mut Vec<(String, Option<u8>, String)>,
    ) {
        match v {
            Value::Object(m) => {
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
                if let (true, Some(rk)) = (pick(m), rk) {
                    let chain = ["/cmd/meta/chainId", "/result/block/chainId"]
                        .iter()
                        .find_map(|q| v.pointer(q))
                        .and_then(text)
                        .and_then(|c| c.parse().ok());
                    out.push((rk, chain, format!("{ptr}{suffix}")));
                }
                for (k, x) in m {
                    let n = ptr.len();
                    ptr.push('/');
                    ptr.push_str(&k.replace('~', "~0").replace('/', "~1"));
                    walk(x, ptr, pick, suffix, out);
                    ptr.truncate(n);
                }
            }
            Value::Array(xs) => {
                for (i, x) in xs.iter().enumerate() {
                    let n = ptr.len();
                    ptr.push_str(&format!("/{i}"));
                    walk(x, ptr, pick, suffix, out);
                    ptr.truncate(n);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(v, &mut String::new(), pick, suffix, &mut out);
    out
}

/// The node's value for one field class of a tx (J: `cont.rollback`, K: `exec.data`, L: the
/// signer's `addr`), `None` when the node's command has no such field (a continuation for K,
/// an exec for J, no signer at that index or another public key for L).
fn node_field(cmd: &Value, suffix: &str, ours_tx: &Value) -> Option<Value> {
    if suffix == "/cmd/payload/rollback" {
        return cmd.pointer("/payload/cont/rollback").cloned();
    }
    if suffix == "/cmd/payload/data" {
        let exec = cmd.pointer("/payload/exec")?;
        return Some(exec.get("data").cloned().unwrap_or(Value::Null));
    }
    let i = suffix
        .strip_prefix("/cmd/signers/")?
        .strip_suffix("/address")?;
    let sg = cmd.pointer(&format!("/signers/{i}"))?;
    let pk = ours_tx.pointer(&format!("/cmd/signers/{i}/pubkey"));
    if pk.is_some_and(|k| sg.get("pubKey") != Some(k)) {
        return None;
    }
    Some(sg.get("addr").cloned().unwrap_or(Value::Null))
}

impl Ctx {
    /// R-P5P9-75 J, K and L (rule (a), exact, against the node): where ours and the oracle
    /// differ on a tx's `cmd.payload.rollback`, its exec `cmd.payload.data` or a signer's
    /// `address`, the oracle's value takes ours only when ours equals the node's command for
    /// that request key (`cont.rollback`; `exec.data`, ours' JSON text by value, extending the
    /// R-P5P9-62 matcher; the signer's `addr`, at the same index and public key). At most
    /// [`PAYLOAD_DATA_CHECKS`] keys per compare, single-row reads. Anything else stays.
    pub(crate) async fn gql_node_cmd_fields(
        &self,
        vars: &Map<String, Value>,
        a: &Answer,
        b: &mut Answer,
        st: &mut Reads,
    ) -> Vec<DiffEntry> {
        let ours = cmd_txs(&a.body);
        let theirs = cmd_txs(&b.body);
        let var_chain = vars
            .get("chainId")
            .and_then(text)
            .and_then(|c| c.parse::<u8>().ok());
        let node0 = st.node;
        let mut cmds: HashMap<String, Option<Value>> = HashMap::new();
        let mut ev: [Vec<Value>; 3] = Default::default();
        for (rk, chain_b, pb) in theirs {
            let Some((_, chain_a, pa)) = ours.iter().find(|(k, _, _)| *k == rk) else {
                continue;
            };
            let at = |v: &Value, p: &str, s: &str| v.pointer(&format!("{p}{s}")).cloned();
            let mut fields = vec![
                "/cmd/payload/rollback".to_string(),
                "/cmd/payload/data".into(),
            ];
            let n = a
                .body
                .pointer(&format!("{pa}/cmd/signers"))
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            fields.extend((0..n).map(|i| format!("/cmd/signers/{i}/address")));
            fields.retain(|s| match (at(&a.body, pa, s), at(&b.body, &pb, s)) {
                (Some(x), Some(y)) => x != y,
                _ => false,
            });
            if fields.is_empty() {
                continue;
            }
            let cmd = match cmds.get(&rk) {
                Some(c) => c.clone(),
                None if cmds.len() >= PAYLOAD_DATA_CHECKS => continue,
                None => {
                    let c = match chain_a.or(chain_b).or(var_chain) {
                        Some(c) => self.node_cmd_for(c, &rk, st).await,
                        None => Err("no chain".into()),
                    };
                    if let Err(why) = &c {
                        tracing::debug!(rk, why, "tx fields against the node: no command");
                    }
                    let c = c.ok();
                    cmds.insert(rk.clone(), c.clone());
                    c
                }
            };
            let Some(cmd) = cmd else { continue };
            let ours_tx = a.body.pointer(pa).cloned().unwrap_or(Value::Null);
            for s in fields {
                let mine = at(&a.body, pa, &s).unwrap_or(Value::Null);
                let Some(node) = node_field(&cmd, &s, &ours_tx) else {
                    continue;
                };
                let (class, ok) = match s.as_str() {
                    "/cmd/payload/rollback" => (0, mine == node),
                    "/cmd/payload/data" => (
                        1,
                        mine.as_str()
                            .and_then(|t| serde_json::from_str::<Value>(t).ok())
                            .is_some_and(|o| o == node),
                    ),
                    _ => (2, mine == node),
                };
                if !ok {
                    continue;
                }
                let theirs = at(&b.body, &pb, &s);
                if let Some(v) = b.body.pointer_mut(&format!("{pb}{s}")) {
                    *v = mine.clone();
                }
                ev[class].push(json!({"requestKey": rk, "field": s, "ours": mine,
                    "oracle": theirs, "node": node}));
            }
        }
        let reads = st.node - node0;
        [ROLLBACK_PATH, EXEC_PAYLOAD_DATA_PATH, SIGNER_ADDRESS_PATH]
            .iter()
            .zip(ev)
            .filter(|(_, e)| !e.is_empty())
            .map(|(p, e)| entry(p, Some(json!({"nodeReads": reads})), Some(Value::Array(e))))
            .collect()
    }

    /// R-P5P9-75 O: H8 on the transfer lists nested in a transaction (QTransactionByKey's
    /// `result.transfers`). For each nested list of the oracle's whose rows ours does not hold
    /// at the same place with the same `orderIndex` and content (at most
    /// [`ORDER_INDEX_READS`] keys), one node `/poll` of the parent tx's request key: when every
    /// oracle value is the rank of the node's TRANSFER event with its content, the rows take
    /// that event's index (ours' numbering, R-P5P9-62) and the diff checks ours against it.
    pub(crate) async fn gql_nested_transfer_order(
        &self,
        vars: &Map<String, Value>,
        a: &Answer,
        b: &mut Answer,
        st: &mut Reads,
    ) -> Option<DiffEntry> {
        let lists = payload_data_txs_by(
            &b.body,
            |m| {
                m.get("result")
                    .and_then(|r| r.get("transfers"))
                    .is_some_and(|t| t.get("edges").is_some_and(Value::is_array))
            },
            "/result/transfers/edges",
        );
        let var_chain = vars
            .get("chainId")
            .and_then(text)
            .and_then(|c| c.parse::<u8>().ok());
        let index = |n: &Value| -> Option<i64> {
            match n.get("orderIndex")? {
                Value::String(s) => s.parse().ok(),
                v => v.as_i64(),
            }
        };
        let mut evidence = Vec::new();
        let mut reads = 0;
        for (rk, chain, ptr) in lists {
            let rows: Vec<Value> = b
                .body
                .pointer(&ptr)
                .and_then(Value::as_array)
                .map(|e| {
                    e.iter()
                        .map(|x| x.get("node").cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .unwrap_or_default();
            let mine: Vec<Value> = a
                .body
                .pointer(&ptr)
                .and_then(Value::as_array)
                .map(|e| {
                    e.iter()
                        .map(|x| x.get("node").cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .unwrap_or_default();
            let held = rows.len() == mine.len()
                && rows
                    .iter()
                    .zip(&mine)
                    .all(|(x, y)| index(x) == index(y) && same_transfer(x, y));
            if held || rows.iter().any(|r| index(r).is_none()) || reads >= ORDER_INDEX_READS {
                continue;
            }
            let Some(chain) = chain
                .or_else(|| {
                    rows.iter()
                        .find_map(|r| r.get("chainId").and_then(text)?.parse().ok())
                })
                .or(var_chain)
            else {
                continue;
            };
            reads += 1;
            let Ok(Some(p)) = self.h7_poll(chain, &rk, st).await else {
                continue;
            };
            let events: Vec<Value> = p
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let ranked: Vec<(i64, &Value)> =
                rows.iter().filter_map(|r| Some((index(r)?, r))).collect();
            match node_rank_map(&events, &ranked) {
                Ok(map) => {
                    for (j, (_, i)) in map.iter().enumerate() {
                        if let Some(n) = b.body.pointer_mut(&format!("{ptr}/{j}/node")) {
                            reindex_transfer(n, *i);
                        }
                    }
                    evidence.push(json!({"requestKey": rk, "chain": chain, "nested": ptr,
                        "oracle": map.iter().map(|x| x.0).collect::<Vec<_>>(),
                        "mapped": map.iter().map(|x| x.1).collect::<Vec<_>>()}));
                }
                Err(why) => tracing::debug!(rk, why, "nested H8 not explained"),
            }
        }
        (!evidence.is_empty()).then(|| {
            entry(
                NESTED_ORDER_INDEX_PATH,
                Some(json!({"nodePolls": reads})),
                Some(Value::Array(evidence)),
            )
        })
    }
}

#[cfg(test)]
mod tests {

    /// R-P5P9-86 T (`tokenId`): null against a fungible TRANSFER, the token against a
    /// marmalade one (token, sender, receiver, amount); a wrong token matches no event. A
    /// null `tokenId` is a checked field.
    #[test]
    fn e2_t_token_id_against_the_nodes_transfer() {
        let coin =
            json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", "r", 1.5]});
        let nft = json!({"name": "TRANSFER", "module": {"name": "ledger", "namespace": "marmalade-v2"},
            "params": ["t:abc", "s", "r", 1.0]});
        let row = |t: Value| {
            json!({"senderAccount": "s", "receiverAccount": "r", "amount": 1.5,
            "tokenId": t})
        };
        assert!(transfer_matches(&row(Value::Null), &coin, false));
        assert!(!transfer_matches(&row(json!("t:abc")), &coin, false));
        let nrow = |t: Value| {
            json!({"senderAccount": "s", "receiverAccount": "r", "amount": 1.0,
            "tokenId": t})
        };
        assert!(transfer_matches(&nrow(json!("t:abc")), &nft, false));
        assert!(!transfer_matches(&nrow(json!("t:xyz")), &nft, false));
        assert!(!transfer_matches(&nrow(Value::Null), &nft, false));
        // Not selected: as before.
        let plain = json!({"senderAccount": "s", "receiverAccount": "r", "amount": 1.5});
        assert!(transfer_matches(&plain, &coin, false));
        let n = json!({"__typename": "Transfer", "requestKey": "k", "tokenId": null,
            "senderAccount": "s"});
        assert_eq!(unchecked_field(Some(Kind::Transfer), &n), None);
    }

    /// R-P5P9-86 T (`crossChainTransfer.senderAccount`): the receiving half's sender is the
    /// sending leg's receiver on the node, the empty account; any other value is refused.
    #[test]
    fn e2_t_cross_chain_sender_is_the_sending_legs_receiver() {
        let mut v = Verdict::new(Canon::Yes, Some(10), Some("B".into()), "placed");
        // R-P5P9-95 m-2: coin's transfer (the node's events name their module).
        v.event = Some(json!({"name": "TRANSFER", "module": {"name": "coin"},
            "params": ["s", "", 1.0]}));
        v.xchain = vec![json!({"name": "TRANSFER_XCHAIN", "params": ["s", "r", 1.0, "2"]})];
        let x = |sender: &str| {
            json!({"chainId": "2", "receiverAccount": "r",
            "senderAccount": sender})
        };
        assert_eq!(xchain_mismatch(&x(""), &v), None);
        let why = xchain_mismatch(&x("s"), &v).unwrap_or_default();
        assert!(why.contains("senderAccount"), "{why}");
        let n = json!({"__typename": "Transfer", "requestKey": "k", "crossChainTransfer": x("")});
        assert_eq!(unchecked_field(Some(Kind::Transfer), &n), None);
    }

    /// R-P5P9-71 (3): `cmd.payload`'s `__typename` passes only as the union member its own
    /// fields name; with neither `code` nor a continuation field it is unknown and refused.
    #[test]
    fn c5b_payload_typename_follows_its_member() {
        let row =
            |payload: Value| json!({"__typename": "Transaction", "cmd": {"payload": payload}});
        let cases = [
            (
                json!({"__typename": "ExecutionPayload", "code": "(+ 1 2)"}),
                None,
            ),
            (
                json!({"__typename": "ContinuationPayload", "pactId": "p", "step": 1}),
                None,
            ),
            (
                json!({"__typename": "ContinuationPayload", "code": "(+ 1 2)"}),
                Some("cmd.payload.__typename"),
            ),
            // R-P5P9-86 T: a payload with no field of its member is checked against the
            // node and the document by [`bare_payload_mismatch`], not here.
            (json!({"__typename": "ExecutionPayload"}), None),
            (
                json!({"__typename": "Signer", "code": "x"}),
                Some("cmd.payload.__typename"),
            ),
        ];
        for (payload, want) in cases {
            let got = unchecked_field(Some(Kind::Tx), &row(payload.clone()));
            assert_eq!(got.as_deref(), want, "{payload}");
        }
        let block = json!({"__typename": "Block", "hash": "h", "parent": {"__typename": "Block", "hash": "p"}});
        assert_eq!(unchecked_field(Some(Kind::Block), &block), None);
        let wrong =
            json!({"__typename": "Block", "hash": "h", "parent": {"__typename": "Transfer"}});
        assert_eq!(
            unchecked_field(Some(Kind::Block), &wrong).as_deref(),
            Some("parent.__typename")
        );
    }
    use super::*;

    /// R-P5P9-70 (I-1) with R-P5P9-62 H9: the node's `meta.creationTime` as ours renders it,
    /// whole seconds with a fraction rounded half to even.
    #[test]
    fn node_secs_rounds_as_ours_does() {
        for (v, want) in [
            (json!(1_700_000_000u64), Some(1_700_000_000)),
            (json!("1700000000"), Some(1_700_000_000)),
            (json!({"int": 1_700_000_000u64}), Some(1_700_000_000)),
            (json!(1_700_000_000.0), Some(1_700_000_000)),
            (json!("1700000000.4"), Some(1_700_000_000)),
            (json!("1700000000.5"), Some(1_700_000_000)),
            (json!("1700000001.5"), Some(1_700_000_002)),
            (json!("1700000000.50001"), Some(1_700_000_001)),
            (json!(1_700_000_000.6), Some(1_700_000_001)),
            (json!(-1), None),
            (json!("x"), None),
        ] {
            assert_eq!(node_secs(&v), want, "{v}");
        }
    }

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
            coinbase: false,
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
        // R-P5P9-86 NC: the node's block for the key is the row's, so the node's transaction
        // lacking the row leaves it unsettled, never "not canonical".
        let p2 = poll("B", 9, json!([other]));
        assert_eq!(
            poll_verdict(&id(None, Some(9), None), &n, &p2, false, None).canon,
            Canon::Unknown
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

    /// R-P5P9-81 (m-1): the settle counts list the four named classes always, any other
    /// class present, and `key-only` as given.
    #[test]
    fn h_f_a_bare_root_list_reads_as_a_connection() {
        let page = |n: usize| {
            json!({"transfers": {"edges": (0..n).map(|i| json!({"node": {"i": i}}))
                .collect::<Vec<_>>()}})
        };
        let mut full = page(60);
        bare_page_info(&mut full, "transfers", 60);
        let c = crate::graphql::root_connection(&mut full).expect("a connection");
        assert_eq!(c.pointer("/pageInfo/hasNextPage"), Some(&json!(true)));
        let mut short = page(12);
        bare_page_info(&mut short, "transfers", 60);
        let c = crate::graphql::root_connection(&mut short).expect("a connection");
        assert_eq!(c.pointer("/pageInfo/hasNextPage"), Some(&json!(false)));
        // Another root, or a list that already selects pageInfo, is left as it is.
        let mut other = page(3);
        bare_page_info(&mut other, "blocks", 60);
        assert!(crate::graphql::root_connection(&mut other).is_none());
        let mut own = json!({"transfers": {"edges": [], "pageInfo": {"hasNextPage": true}}});
        bare_page_info(&mut own, "transfers", 60);
        assert_eq!(own["transfers"]["pageInfo"], json!({"hasNextPage": true}));
    }

    #[test]
    fn settle_counts_per_class() {
        let rows = [
            json!({"why": "outside-oracle-window"}),
            json!({"why": "outside-oracle-window"}),
            json!({"why": "oracle-gap"}),
            json!({"why": "below-our-window"}),
        ];
        assert_eq!(
            settle_counts(&rows, 1),
            json!({"oracle-gap": 1, "outside-oracle-window": 2, "duplicate": 0,
                "key-only": 1, "below-our-window": 1})
        );
        assert_eq!(
            settle_counts(&[], 0),
            json!({"oracle-gap": 0, "outside-oracle-window": 0, "duplicate": 0, "key-only": 0})
        );
    }

    /// R-P5P9-81 (m-2): at depth 1, an identifier that directly follows `:` is a field name,
    /// never a response key. In review-fvh-i's probe document the root `transfers` is the
    /// alias of `events`, not x's field, in both `root_selection` and `root_args`.
    #[test]
    fn root_lookups_skip_the_field_half_of_an_alias() {
        let q = "query Q($a: String, $e: String) { x: transfers(accountName: $a) { edges { \
                 node { requestKey } } } transfers: events(qualifiedEventName: $e) { edges { \
                 node { y: requestKey } } } }";
        assert_eq!(
            root_selection(q, "transfers"),
            Some(("events".to_string(), vec!["y".to_string()]))
        );
        assert_eq!(
            root_args(q, "transfers"),
            Some(vec![("qualifiedEventName".to_string(), "$e".to_string())])
        );
        // The same with a comment between the colon and the field name.
        let q2 = "{ x: # the field\n transfers { a } transfers: events { b } }";
        assert_eq!(
            root_selection(q2, "transfers").map(|s| s.0),
            Some("events".into())
        );
        assert_eq!(root_args(q2, "transfers"), Some(Vec::new()));
        // A field named once, and an alias of it, are still found.
        assert_eq!(
            root_selection(q, "x"),
            Some(("transfers".to_string(), Vec::new()))
        );
        assert_eq!(
            root_args(q, "x"),
            Some(vec![("accountName".to_string(), "$a".to_string())])
        );
        // A key that is only ever a field name after an alias is not a root.
        assert_eq!(root_selection("{ x: transfers { a } }", "transfers"), None);
        assert_eq!(
            root_args("{ x: transfers(first: 1) { a } }", "transfers"),
            None
        );
    }

    /// R-P5P9-95 m-1: a named spread (or a fragment on a payload member) hides which member
    /// the document selects: unknown, so a bare payload is refused.
    #[test]
    fn f_m1_named_spreads_leave_the_payload_members_unknown() {
        let inline = "query Q { transactions { edges { node { cmd { payload { \
                      ... on ContinuationPayload { pactId } } } } } } }";
        assert_eq!(payload_members(inline), Some((false, true)));
        let named = "query Q { transactions { edges { node { cmd { payload { \
                     ... on ContinuationPayload { pactId } ...ExecFields } } } } } } \
                     fragment ExecFields on ExecutionPayload { code }";
        assert_eq!(payload_members(named), None);
    }

    /// R-P5P9-95 m-2: `crossChainTransfer.senderAccount` is checked only for coin's
    /// transfer; another module's is refused.
    #[test]
    fn f_m2_cross_chain_sender_only_for_coin() {
        let mut v = Verdict::new(Canon::Yes, Some(10), Some("B".into()), "placed");
        v.event = Some(json!({"name": "TRANSFER", "module": {"name": "coin"},
            "params": ["s", "", 1.0]}));
        v.xchain = vec![json!({"name": "TRANSFER_XCHAIN", "params": ["s", "r", 1.0, "2"]})];
        let x = json!({"chainId": "2", "receiverAccount": "r", "senderAccount": ""});
        assert_eq!(xchain_mismatch(&x, &v), None);
        v.event = Some(json!({"name": "TRANSFER", "module": {"name": "my-token"},
            "params": ["s", "", 1.0]}));
        let why = xchain_mismatch(&x, &v).unwrap_or_default();
        assert!(why.contains("senderAccount"), "{why}");
    }

    /// R-P5P9-95 m-3: a marmalade TRANSFER (token, sender, receiver, amount) is read past its
    /// token: a null `crossChainTransfer` is refused when the node's tx holds the
    /// TRANSFER_XCHAIN for it.
    #[test]
    fn f_m3_marmalade_transfers_in_the_cross_chain_check() {
        let mut v = Verdict::new(Canon::Yes, Some(10), Some("B".into()), "placed");
        v.event = Some(json!({"name": "TRANSFER", "params": ["t:1", "s", "", 1.0]}));
        v.xchain = vec![json!({"name": "TRANSFER_XCHAIN",
            "params": ["t:1", "s", "r", 1.0, "2"]})];
        let why = xchain_mismatch(&Value::Null, &v).unwrap_or_default();
        assert!(why.contains("crossChainTransfer is null"), "{why}");
    }

    /// R-P5P9-95 m-7: NC's equivalence takes ours' `info` "" or the node's text, nothing else.
    #[test]
    fn f_m7_bad_result_info_is_empty_or_the_nodes() {
        let node = json!({"message": "m", "info": "<toplevel>:0"});
        let ours = |info: &str| json!({"message": "m", "info": info}).to_string();
        assert!(bad_result_is(&json!(ours("")), Some(&node)));
        assert!(bad_result_is(&json!(ours("<toplevel>:0")), Some(&node)));
        assert!(!bad_result_is(&json!(ours("other")), Some(&node)));
    }
}
