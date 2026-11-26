//! Build records (record codec version 1, store format v2) from a decoded block (spec §3.4 "Record encoding").
//!
//! Names are interned for the whole block first (`intern_block`; first-seen order: miner
//! account, then coinbase, then user txs in payload order; per tx: meta sender, signer
//! `addr`s, then per output the events' module, event name and module hash, then each
//! event's account strings, see `event_accounts`), and only then are values encoded, so every
//! account string in the block is stored as an account ref. Account strings longer than
//! `MAX_ACCOUNT_LEN` are never interned.

use kdx_types::record::{
    BlockRecord, CbReqKey, EventRec, Sig, TxRecord, ENVELOPE_KEYS, NO_ACCOUNT,
};
use kdx_types::{value, Dict, DictKind, DictMut, Hash32, TxKey};
use serde_json::{Map, Value};

use crate::payload::{exact_u64, DecodedBlock};
use crate::render::{render_miner_bytes, render_output_bytes, render_tx_bytes};
use crate::verify::VerifyReport;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BuildError {
    #[error("height {0} does not fit a TxKey (u32)")]
    Height(u64),
    #[error("block at height {height} has {txs} txs; TxKey positions are u16")]
    Position { height: u64, txs: usize },
}

/// Keep the original leaf bytes when the typed fields do not render them exactly.
fn pin_raw(r: &mut TxRecord, tx_bytes: Option<&[u8]>, output_bytes: &[u8], dict: &dyn Dict) {
    if let Some(orig) = tx_bytes {
        if render_tx_bytes(r, dict).ok().flatten().as_deref() != Some(orig) {
            r.raw_tx = Some(orig.to_vec());
        }
    }
    if render_output_bytes(r, dict).ok().as_deref() != Some(output_bytes) {
        r.raw_output = Some(output_bytes.to_vec());
    }
}

/// Key in `extra` holding the original `sigs` array when an entry is not `{"sig": <string>}`.
pub const EXTRA_TX_SIGS: &str = "tx.sigs";

/// A canonical event: `{"params": [..], "name": s, "module": {"namespace": null|s, "name": s}, "moduleHash": s}`
/// with exactly these keys in this order and no `.` in namespace or module name.
pub struct EventParts<'a> {
    /// `"ns.name"` or `"name"`.
    pub module: String,
    /// `"<module>.<name>"`.
    pub event_name: String,
    pub module_hash: &'a str,
    pub params: &'a Value,
    pub name: &'a str,
}

pub fn parse_event(e: &Value) -> Option<EventParts<'_>> {
    let m = e.as_object()?;
    let keys: Vec<&str> = m.keys().map(String::as_str).collect();
    if keys != ["params", "name", "module", "moduleHash"] {
        return None;
    }
    let params = m.get("params")?;
    if !params.is_array() {
        return None;
    }
    let name = m.get("name")?.as_str()?;
    let module_hash = m.get("moduleHash")?.as_str()?;
    let md = m.get("module")?.as_object()?;
    let mkeys: Vec<&str> = md.keys().map(String::as_str).collect();
    if mkeys != ["namespace", "name"] {
        return None;
    }
    let mname = md.get("name")?.as_str()?;
    if mname.is_empty() || mname.contains('.') {
        return None;
    }
    let module = match md.get("namespace")? {
        Value::Null => mname.to_owned(),
        Value::String(ns) if !ns.is_empty() && !ns.contains('.') => format!("{ns}.{mname}"),
        _ => return None,
    };
    let event_name = format!("{module}.{name}");
    Some(EventParts {
        module,
        event_name,
        module_hash,
        params,
        name,
    })
}

/// Length of the leading rational of `s` as `Data.Text.Read.rational` reads it: an optional
/// `+`/`-`, at least one ASCII digit, an optional `.` followed by at least one digit, and an
/// optional `e`/`E` exponent with an optional sign and at least one digit. A `.` or exponent
/// marker without digits after it ends the number; trailing text is ignored. `None` when `s`
/// does not start with a number.
pub fn rational_prefix_len(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let digits = |mut i: usize| {
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        i
    };
    let mut i = usize::from(matches!(b.first(), Some(b'-' | b'+')));
    let ds = i;
    i = digits(i);
    if i == ds {
        return None;
    }
    if b.get(i) == Some(&b'.') && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
        i = digits(i + 1);
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let es = i + 1 + usize::from(matches!(b.get(i + 1), Some(b'-' | b'+')));
        let j = digits(es);
        if j > es {
            i = j;
        }
    }
    Some(i)
}

/// chainweb-data's `getAmount` succeeds on the amount param: a number, a string with a
/// leading rational, or an object whose `decimal` or `int` member is one of those (other
/// members are ignored).
pub fn transfer_amount_ok(v: &Value) -> bool {
    let plain = |x: &Value| match x {
        Value::Number(_) => true,
        Value::String(s) => rational_prefix_len(s).is_some(),
        _ => false,
    };
    match v {
        Value::Object(o) => ["decimal", "int"]
            .iter()
            .any(|k| o.get(*k).is_some_and(plain)),
        _ => plain(v),
    }
}

/// A transfer row of chainweb-data (`mkTransferRows`): the qualified event name ends in
/// `TRANSFER`, there are exactly three params, `from` and `to` are strings and the amount
/// passes `transfer_amount_ok`. Drives the record's `transfers` list and the `xfer` postings
/// (keyed by the event's module). The `acct` sides use the narrower `acct_transfer_accounts`.
pub fn transfer_accounts<'a>(ev: &EventParts<'a>) -> Option<(&'a str, &'a str)> {
    if !ev.event_name.ends_with("TRANSFER") {
        return None;
    }
    let p = ev.params.as_array()?;
    if p.len() != 3 || !transfer_amount_ok(&p[2]) {
        return None;
    }
    Some((p[0].as_str()?, p[1].as_str()?))
}

/// Stock's amount check (indexer-kadena `utils/transfers.ts` `parseAmount`): a JSON number,
/// or an object whose `decimal` or `integer` member is a non-empty string (JS truthiness; the
/// other members are ignored). Narrower than [`transfer_amount_ok`] (chainweb-data's, which
/// the `xfer` postings keep): a plain string, `{int: ..}` and `{decimal: <number>}` fail here.
/// Decides the holder sides (stock's `Balances` set) and so `balance()`'s zero rows.
pub fn stock_amount_ok(v: &Value) -> bool {
    let truthy_str = |k: &str| {
        v.get(k)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
    };
    match v {
        Value::Number(_) => true,
        Value::Object(_) => truthy_str("decimal") || truthy_str("integer"),
        _ => false,
    }
}

/// JS truthiness of a JSON value (stock's `account && tokenId`).
fn js_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Stock's coin transfer (`getCoinTransfers`): name exactly `TRANSFER`, three params, string
/// from and to, and [`stock_amount_ok`]. The holder sides of a fungible transfer.
pub fn stock_transfer_accounts<'a>(ev: &EventParts<'a>) -> Option<(&'a str, &'a str)> {
    if ev.name != "TRANSFER" {
        return None;
    }
    let p = ev.params.as_array()?;
    if p.len() != 3 || !stock_amount_ok(&p[2]) {
        return None;
    }
    Some((p[0].as_str()?, p[1].as_str()?))
}

/// The transfer sides that give `acct` postings: only an event named exactly `TRANSFER` (the
/// v1 rule) whose params pass `transfer_accounts`. Suffix names such as `WIZ_TRANSFER` carry
/// `(id, from, to)` params; their "sides" are an NFT id and a receiver, not two accounts.
pub fn acct_transfer_accounts<'a>(ev: &EventParts<'a>) -> Option<(&'a str, &'a str)> {
    if ev.name != "TRANSFER" {
        return None;
    }
    transfer_accounts(ev)
}

/// Longest account a module holders row keeps (`balance(module:)` without `accountName`):
/// stock keeps any non-empty string; we bound it at 1024 bytes (review M-4 of PR #54), above
/// `MAX_ACCOUNT_LEN`. Such names are interned too (decode time, so backfill and tail agree).
pub const MAX_HOLDER_ACCOUNT_LEN: usize = 1024;

/// The marmalade ledgers stock adds `MINT` and `TRANSFER` holder rows for (`payload.ts`).
pub const MARMALADE_LEDGERS: [&str; 2] = ["marmalade-v2.ledger", "marmalade.ledger"];

/// Whether `s` may be a holders row's account: non-empty, at most `MAX_HOLDER_ACCOUNT_LEN`.
pub fn holder_account_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_HOLDER_ACCOUNT_LEN
}

/// The accounts an event adds to its module's holders list, as stock's `payload.ts` builds
/// its `Balances` rows (at most three per event, so the work per event is fixed):
/// - a coin-shaped transfer (`getCoinTransfers`): name exactly `TRANSFER`, three params,
///   string from and to, stock's amount forms (`stock_transfer_accounts`): both sides;
/// - an NFT `RECONCILE` (`getNftTransfers`) of a module in the `marmalade-v2` or `marmalade`
///   namespace: four params, a string token id, stock's amount forms, then two objects with
///   an `account` member: both accounts;
/// - on `marmalade-v2.ledger` / `marmalade.ledger`: `MINT` names `params[1]`, `TRANSFER`
///   names its receiver `params[2]`, when the token id `params[0]` is truthy in JS terms
///   (stock's `account && tokenId`: a non-empty string, a non-zero number, `true`, an object
///   or an array; secpass A m-7).
///
/// Empty sides are left out (stock: `!== ''` and `account && tokenId`). One documented
/// divergence: stock would stringify a non-string account (SQL insert); only string
/// accounts are kept here (`docs/comparison.md`). `holder_account_ok` filters the result.
pub fn holder_sides<'a>(ev: &EventParts<'a>, out: &mut Vec<&'a str>) {
    if let Some((from, to)) = stock_transfer_accounts(ev) {
        out.extend([from, to]);
    }
    let Some(p) = ev.params.as_array() else {
        return;
    };
    let ns = ev.module.split_once('.').map(|(ns, _)| ns);
    if ev.name == "RECONCILE"
        && p.len() == 4
        && matches!(ns, Some("marmalade-v2" | "marmalade"))
        && p[0].is_string()
        && stock_amount_ok(&p[1])
    {
        let acct = |v: &'a Value| v.as_object().and_then(|o| o.get("account"));
        if let (Some(a), Some(b)) = (acct(&p[2]), acct(&p[3])) {
            out.extend([a, b].into_iter().filter_map(Value::as_str));
        }
    }
    if MARMALADE_LEDGERS.contains(&ev.module.as_str()) && p.first().is_some_and(js_truthy) {
        let side = match ev.name {
            "MINT" => p.get(1),
            "TRANSFER" => p.get(2),
            _ => None,
        };
        out.extend(side.and_then(Value::as_str));
    }
    out.retain(|a| !a.is_empty());
}

/// All events canonical, or None (then the events go to `extra`).
fn parse_events(o: &Map<String, Value>) -> Option<Vec<EventParts<'_>>> {
    o.get("events")?
        .as_array()?
        .iter()
        .map(parse_event)
        .collect()
}

/// Longest string interned as an account: `coin`'s limit (3..=256 characters). Longer strings
/// are not accounts: no AccountId, no posting; records keep them as plain strings.
pub const MAX_ACCOUNT_LEN: usize = 256;

fn b64url_hash(s: &str) -> bool {
    s.len() == 43
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

/// A non-empty Pact name (keyset name, module or function name): printable ASCII, no spaces.
fn pact_name(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_graphic())
}

/// A syntactically valid Pact principal (Pact `principal` grammar):
/// `k:<64 lowercase hex>`, `w:<hash>:<predicate>`, `r:<keyset>`, `u:<function>:<hash>`,
/// `m:<module>`, `p:<pact id>:<function>`, `c:<hash>` (hash = 43 base64url chars).
pub fn is_principal(s: &str) -> bool {
    if s.len() > MAX_ACCOUNT_LEN || s.len() < 3 || s.as_bytes()[1] != b':' {
        return false;
    }
    let rest = &s[2..];
    match s.as_bytes()[0] {
        b'k' => rest.len() == 64 && rest.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')),
        b'w' => rest
            .split_once(':')
            .is_some_and(|(h, p)| b64url_hash(h) && pact_name(p)),
        b'p' => rest
            .split_once(':')
            .is_some_and(|(h, f)| b64url_hash(h) && pact_name(f)),
        b'u' => rest
            .rsplit_once(':')
            .is_some_and(|(f, h)| pact_name(f) && b64url_hash(h)),
        b'r' | b'm' => pact_name(rest) && !rest.contains(':'),
        b'c' => b64url_hash(rest),
        _ => false,
    }
}

fn principals<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
    match v {
        Value::String(s) if is_principal(s) => out.push(s),
        Value::Array(a) => a.iter().for_each(|x| principals(x, out)),
        Value::Object(m) => m.values().for_each(|x| principals(x, out)),
        _ => {}
    }
}

/// Account strings an event names, in a fixed order (spec §3.6, Acct sources): both sides of
/// a fungible `TRANSFER (from, to, amount)`, of a poly-fungible `TRANSFER (id, from, to,
/// amount)` and of `TRANSFER_XCHAIN`/`TRANSFER_XCHAIN_RECD (from, to, amount, chain)`, then
/// every param string at any depth that is a Pact principal. Empty and over-long strings are
/// included; `account_ok` filters them. The result depends only on the event.
pub fn event_accounts<'a>(e: &'a Value, out: &mut Vec<&'a str>) {
    let Some(params) = e.get("params") else {
        return;
    };
    if let (Some(ev), Some(p)) = (parse_event(e), params.as_array()) {
        if let Some((from, to)) = acct_transfer_accounts(&ev) {
            out.extend([from, to]);
        } else if p.len() == 4 {
            // Each arm checks its own shape: the poly-fungible amount is `p[3]`; the
            // cross-chain amount is `p[2]` and its `p[3]` is the target chain id string.
            let sides = match ev.name {
                "TRANSFER"
                    if p[0].is_string() && matches!(p[3], Value::Number(_) | Value::Object(_)) =>
                {
                    Some((&p[1], &p[2]))
                }
                "TRANSFER_XCHAIN" | "TRANSFER_XCHAIN_RECD"
                    if transfer_amount_ok(&p[2]) && p[3].is_string() =>
                {
                    Some((&p[0], &p[1]))
                }
                _ => None,
            };
            if let Some((Some(a), Some(b))) = sides.map(|(a, b)| (a.as_str(), b.as_str())) {
                out.extend([a, b]);
            }
        }
    }
    principals(params, out);
}

/// Whether `s` is interned as an account: non-empty and at most `MAX_ACCOUNT_LEN` bytes.
pub fn account_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_ACCOUNT_LEN
}

/// Intern `s` as an account if `account_ok`; count an over-long one in `unindexed`.
fn intern_account(dict: &mut impl DictMut, s: &str, unindexed: &mut u32) -> Option<u32> {
    if s.len() > MAX_ACCOUNT_LEN {
        *unindexed += 1;
    }
    account_ok(s).then(|| dict.intern(DictKind::Account, s))
}

fn intern_output(o: &Value, dict: &mut impl DictMut, unindexed: &mut u32) {
    let Some(o) = o.as_object() else { return };
    for ev in parse_events(o).unwrap_or_default() {
        dict.intern(DictKind::Module, &ev.module);
        dict.intern(DictKind::EventName, &ev.event_name);
        dict.intern(DictKind::ModuleHash, ev.module_hash);
    }
    let mut acc = Vec::new();
    for e in o
        .get("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        acc.clear();
        event_accounts(e, &mut acc);
        // The `xfer` sides (`transfer_accounts`, wider than the `acct` rule): a side that is
        // neither an `acct` side nor a principal (a suffix-TRANSFER's id, a non-principal
        // name of an `X_TRANSFER`) is posted by `extract` under `xfer` only. It is interned
        // here too, after the event's `acct` strings, so `encode_records` already sees it.
        let mut sides = Vec::new();
        if let Some(ev) = parse_event(e) {
            if let Some((from, to)) = transfer_accounts(&ev) {
                for s in [from, to] {
                    if !acc.contains(&s) {
                        acc.push(s);
                    }
                }
            }
            holder_sides(&ev, &mut sides);
        }
        for a in &acc {
            // Secpass A m-6: a name over `MAX_ACCOUNT_LEN` that is a holder side is interned
            // for the holders table (`index.rs`), so it is not counted as left unindexed.
            let mut n = 0;
            intern_account(dict, a, &mut n);
            if n > 0 && !(sides.contains(a) && holder_account_ok(a)) {
                *unindexed += n;
            }
        }
    }
}

/// Step 1 of `build_records`: intern every name the block references, in first-seen order
/// (miner account; coinbase output; per user tx: meta sender, signer `addr`s, output; per
/// event of an output: its `event_accounts`, then its `xfer` sides not among them). Every
/// account string `extract` posts (`acct` and `xfer`) is interned here, before the records
/// are encoded, so record bytes and postings depend only on the block and the ids.
/// Returns the number of account strings longer than `MAX_ACCOUNT_LEN` (not interned).
pub fn intern_block(b: &DecodedBlock, dict: &mut impl DictMut) -> u32 {
    let mut unindexed = 0;
    if let Some(a) = b.miner.get("account").and_then(Value::as_str) {
        intern_account(dict, a, &mut unindexed);
    }
    intern_output(&b.coinbase, dict, &mut unindexed);
    for t in &b.txs {
        if let Some(c) = &t.cmd_parsed {
            if let Some(s) = c.pointer("/meta/sender").and_then(Value::as_str) {
                intern_account(dict, s, &mut unindexed);
            }
            for s in c
                .get("signers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(a) = s.get("addr").and_then(Value::as_str) {
                    intern_account(dict, a, &mut unindexed);
                }
            }
        }
        intern_output(&t.output, dict, &mut unindexed);
    }
    unindexed
}

/// Fill the output (CommandResult) part of `r`. `user_hash` is the tx hash for
/// user txs (whose reqKey must equal it) and None for the coinbase.
fn fill_output(
    r: &mut TxRecord,
    out: &Value,
    user_hash: Option<Hash32>,
    dict: &dyn Dict,
    extra: &mut Map<String, Value>,
) {
    let empty = Map::new();
    // decode_block guarantees an object; anything else renders as `{}` plus extra.
    let o = out.as_object().unwrap_or(&empty);
    let mut present = 0u8;
    for (i, k) in ENVELOPE_KEYS.iter().enumerate() {
        if o.contains_key(*k) {
            present |= 1 << i;
        }
    }
    r.present = present;
    for (k, v) in o {
        let fits = match k.as_str() {
            "gas" => exact_u64(v).map(|g| r.gas = g).is_some(),
            "result" => fill_result(r, v, dict),
            "reqKey" => match (user_hash, v.as_str()) {
                (Some(h), Some(s)) => s == h.to_b64url(),
                (None, Some(s)) => {
                    r.cb_req_key = Some(CbReqKey::from_wire(s));
                    true
                }
                _ => false,
            },
            "logs" => match v {
                Value::Null => true,
                Value::String(s) => Hash32::from_b64url(s).map(|h| r.logs = Some(h)).is_ok(),
                _ => false,
            },
            "events" => match parse_events(o) {
                Some(evs) => {
                    for (pos, ev) in evs.iter().enumerate() {
                        if transfer_accounts(ev).is_some() {
                            r.transfers.push(pos as u16);
                        }
                        r.events.push(EventRec {
                            module: dict.find(DictKind::Module, &ev.module).unwrap_or(u32::MAX),
                            name: dict
                                .find(DictKind::EventName, &ev.event_name)
                                .unwrap_or(u32::MAX),
                            module_hash: dict
                                .find(DictKind::ModuleHash, ev.module_hash)
                                .unwrap_or(u32::MAX),
                            params: value::to_vec(ev.params, dict),
                        });
                    }
                    evs.len() <= u16::MAX as usize
                }
                None => false,
            },
            "metaData" => {
                r.metadata = (!v.is_null()).then(|| value::to_vec(v, dict));
                true
            }
            "continuation" => {
                r.continuation = (!v.is_null()).then(|| value::to_vec(v, dict));
                true
            }
            "txId" => match v {
                Value::Null => true,
                _ => exact_u64(v).map(|t| r.pact_tx_id = Some(t)).is_some(),
            },
            _ => false,
        };
        if !fits {
            if k == "events" {
                r.events.clear();
                r.transfers.clear();
            }
            extra.insert(k.clone(), v.clone());
        }
    }
}

fn fill_result(r: &mut TxRecord, v: &Value, dict: &dyn Dict) -> bool {
    let Some(m) = v.as_object() else { return false };
    let keys: Vec<&str> = m.keys().map(String::as_str).collect();
    let status = m.get("status").and_then(Value::as_str);
    match (keys.as_slice(), status) {
        (["status", "data"], Some("success")) => {
            r.status_ok = true;
            r.result = value::to_vec(&m["data"], dict);
            true
        }
        (["status", "error"], Some("failure")) => {
            r.status_ok = false;
            r.result = value::to_vec(&m["error"], dict);
            true
        }
        _ => false,
    }
}

fn finish_extra(r: &mut TxRecord, extra: Map<String, Value>, dict: &dyn Dict) {
    if !extra.is_empty() {
        r.extra = Some(value::to_vec(&Value::Object(extra), dict));
    }
}

/// Build the block record and the tx records (coinbase first). Every record renders its
/// Merkle leaf bytes exactly: typed fields where they reproduce the node's bytes, the raw
/// `raw_*` slots where they do not (see `record.rs`).
///
/// `report` comes from `verify_block`: user txs listed in `cmd_hash_mismatch` get the
/// `cmd_reencoded` bit, and `report.merkle == Some(Verified)` sets `merkle_verified`.
pub fn build_records(
    b: &DecodedBlock,
    report: &VerifyReport,
    dict: &mut impl DictMut,
) -> Result<(BlockRecord, Vec<TxRecord>), BuildError> {
    // 1. Intern everything the block references, in first-seen order.
    intern_block(b, dict);
    // 2. Encode.
    encode_records(b, report, &*dict)
}

/// Step 2 of `build_records`: encode with a dictionary that already holds every name
/// (`intern_block`).
pub fn encode_records(
    b: &DecodedBlock,
    report: &VerifyReport,
    dict: &dyn Dict,
) -> Result<(BlockRecord, Vec<TxRecord>), BuildError> {
    let miner_account = b
        .miner
        .get("account")
        .and_then(Value::as_str)
        .and_then(|a| dict.find(DictKind::Account, a))
        .unwrap_or(NO_ACCOUNT);
    let h = &b.header;
    let height = u32::try_from(h.height).map_err(|_| BuildError::Height(h.height))?;
    if b.txs.len() >= u16::MAX as usize {
        return Err(BuildError::Position {
            height: h.height,
            txs: b.txs.len(),
        });
    }
    let mut block = BlockRecord {
        hash: h.hash,
        parent: h.parent,
        adjacents: h
            .adjacents_order
            .iter()
            .map(|c| (*c, h.adjacents[c]))
            .collect(),
        chain: h.chain_id,
        height: h.height,
        weight: h.weight,
        target: h.target,
        nonce: h.nonce,
        creation_time: h.creation_time,
        epoch_start: h.epoch_start,
        feature_flags: h.feature_flags,
        payload_hash: b.payload_hash,
        transactions_hash: b.transactions_hash,
        outputs_hash: b.outputs_hash,
        chainweb_version: h.chainweb_version.clone(),
        miner_account,
        miner_data: value::to_vec(&b.miner, dict),
        raw_miner: None,
        merkle_verified: report.merkle_verified(),
    };
    if render_miner_bytes(&block, dict).ok().as_deref() != Some(b.miner_bytes.as_slice()) {
        block.raw_miner = Some(b.miner_bytes.clone());
    }

    let mut txs = Vec::with_capacity(b.txs.len() + 1);
    let mut cb = TxRecord {
        key: TxKey::new(height, h.chain_id, 0),
        ..TxRecord::default()
    };
    let mut extra = Map::new();
    fill_output(&mut cb, &b.coinbase, None, dict, &mut extra);
    finish_extra(&mut cb, extra, dict);
    pin_raw(&mut cb, None, &b.coinbase_bytes, dict);
    txs.push(cb);

    for (i, t) in b.txs.iter().enumerate() {
        let pos = (i + 1) as u16; // bounded by the check above
        let mut r = TxRecord {
            key: TxKey::new(height, h.chain_id, pos),
            request_key: Some(t.hash),
            cmd: Some(t.cmd.as_bytes().to_vec()),
            cmd_unparsed: t.flags.cmd_unparsed,
            cmd_reencoded: report.cmd_hash_mismatch.contains(&(pos as usize)),
            ..TxRecord::default()
        };
        let mut extra = Map::new();
        let sigs: Option<Vec<Sig>> = t
            .sigs
            .iter()
            .map(|s| {
                let m = s.as_object().filter(|m| m.len() == 1)?;
                Some(Sig::from_wire(m.get("sig")?.as_str()?))
            })
            .collect();
        match sigs {
            Some(s) => r.sigs = s,
            None => {
                extra.insert(EXTRA_TX_SIGS.to_owned(), Value::Array(t.sigs.clone()));
            }
        }
        fill_output(&mut r, &t.output, Some(t.hash), dict, &mut extra);
        finish_extra(&mut r, extra, dict);
        pin_raw(&mut r, Some(&t.tx_bytes), &t.output_bytes, dict);
        txs.push(r);
    }
    Ok((block, txs))
}

#[cfg(test)]
mod transfer_rule_tests {
    use serde_json::json;

    use super::{
        acct_transfer_accounts, event_accounts, parse_event, rational_prefix_len,
        transfer_accounts, transfer_amount_ok,
    };

    #[test]
    fn rational_prefix() {
        for (s, n) in [
            ("1", Some(1)),
            ("-1.5", Some(4)),
            ("+2e-3x", Some(5)),
            ("5.", Some(1)),
            ("5.e3", Some(1)),
            ("7e", Some(1)),
            ("0.25E+2", Some(7)),
            (".5", None),
            ("-", None),
            ("abc", None),
            ("", None),
        ] {
            assert_eq!(rational_prefix_len(s), n, "{s:?}");
        }
    }

    #[test]
    fn amount_forms() {
        for v in [
            json!(1.5),
            json!({"decimal": 1.5}),
            json!({"decimal": "1.5"}),
            json!({"int": 3}),
            json!({"int": "3"}),
            json!("12.5 KDA"),
            json!({"decimal": "x", "int": 2}),
            json!({"other": 1, "decimal": 2}),
        ] {
            assert!(transfer_amount_ok(&v), "{v}");
        }
        for v in [
            json!("x"),
            json!({"decimal": "x"}),
            json!({"other": 1}),
            json!(null),
            json!([1]),
        ] {
            assert!(!transfer_amount_ok(&v), "{v}");
        }
    }

    #[test]
    fn suffix_rule() {
        let ev = |name: &str, params| {
            json!({"params": params, "name": name,
                   "module": {"namespace": "free", "name": "wiz-arena"}, "moduleHash": "h"})
        };
        let e = ev("WIZ_TRANSFER", json!(["a", "b", "1.0"]));
        assert_eq!(
            transfer_accounts(&parse_event(&e).unwrap()),
            Some(("a", "b"))
        );
        let e = ev("TRANSFER_XCHAIN", json!(["a", "b", 1.0]));
        assert_eq!(transfer_accounts(&parse_event(&e).unwrap()), None);
        let e = ev("TRANSFER", json!(["id", "a", "b", 1.0]));
        assert_eq!(transfer_accounts(&parse_event(&e).unwrap()), None);
        let e = ev("TRANSFER", json!(["a", 5, 1.0]));
        assert_eq!(transfer_accounts(&parse_event(&e).unwrap()), None);
    }

    #[test]
    fn acct_keeps_transfer_only_rule() {
        let ev = |name: &str, params| {
            json!({"params": params, "name": name,
                   "module": {"namespace": "free", "name": "wiz-arena"}, "moduleHash": "h"})
        };
        let sender = format!("k:{}", "ab".repeat(32));
        let receiver = "12abc0ffee";
        // WIZ_TRANSFER (id, from, to): chainweb-data's rule reads params[2] as the amount and
        // "12abc..." has a leading rational, so the xfer / compat row exists with from "83".
        let w = ev("WIZ_TRANSFER", json!(["83", sender, receiver]));
        let parsed = parse_event(&w).unwrap();
        assert_eq!(transfer_accounts(&parsed), Some(("83", sender.as_str())));
        assert_eq!(acct_transfer_accounts(&parsed), None);
        let mut acc = Vec::new();
        event_accounts(&w, &mut acc);
        assert!(!acc.contains(&"83"), "{acc:?}");
        assert!(!acc.contains(&receiver), "{acc:?}");
        assert!(
            acc.contains(&sender.as_str()),
            "principal still listed: {acc:?}"
        );
        // A plain coin TRANSFER gives both sides an acct posting.
        let t = ev("TRANSFER", json!(["alice", "bob", {"decimal": "1.5"}]));
        let parsed = parse_event(&t).unwrap();
        assert_eq!(acct_transfer_accounts(&parsed), Some(("alice", "bob")));
        let mut acc = Vec::new();
        event_accounts(&t, &mut acc);
        assert_eq!(acc[..2], ["alice", "bob"]);
    }

    /// PR #54 review I-2, M-3, M-4: the holders rule is stock's `payload.ts`: exact
    /// `TRANSFER` only (an `XTRANSFER` gives no side), the marmalade ledgers' `MINT` account
    /// and `TRANSFER` receiver, both `RECONCILE` accounts in the marmalade namespaces, no empty
    /// side, and accounts bounded at 1024 bytes.
    #[test]
    fn holder_sides_follow_stock_balances_rows() {
        use super::{holder_account_ok, holder_sides, MAX_HOLDER_ACCOUNT_LEN};
        let ev = |ns: &str, module: &str, name: &str, params: serde_json::Value| {
            json!({"params": params, "name": name,
                   "module": {"namespace": ns, "name": module}, "moduleHash": "h"})
        };
        let sides = |e: &serde_json::Value| {
            let p = parse_event(e).unwrap();
            let mut out = Vec::new();
            holder_sides(&p, &mut out);
            out.into_iter().map(str::to_owned).collect::<Vec<_>>()
        };
        let amt = json!({"decimal": "1.5"});
        assert_eq!(
            sides(&ev("free", "tok", "TRANSFER", json!(["a", "b", amt]))),
            ["a", "b"]
        );
        assert!(sides(&ev("free", "tok", "XTRANSFER", json!(["a", "b", amt]))).is_empty());
        assert!(sides(&ev("free", "tok", "X_TRANSFER", json!(["a", "b", 1]))).is_empty());
        // Coinbase-shaped: the empty sender is left out.
        let cb = json!({"params": ["", "m", 2], "name": "TRANSFER",
                        "module": {"namespace": null, "name": "coin"}, "moduleHash": "h"});
        assert_eq!(sides(&cb), ["m"]);
        // Marmalade ledgers.
        let (v2, v1) = ("marmalade-v2", "marmalade");
        assert_eq!(
            sides(&ev(v2, "ledger", "MINT", json!(["t:1", "alice", 1]))),
            ["alice"]
        );
        assert_eq!(
            sides(&ev(v1, "ledger", "TRANSFER", json!(["t:1", "a", "b", 1]))),
            ["b"]
        );
        assert!(sides(&ev(v2, "ledger", "MINT", json!(["", "alice", 1]))).is_empty());
        assert!(sides(&ev(v2, "ledger", "BURN", json!(["t:1", "alice", 1]))).is_empty());
        assert!(sides(&ev(v2, "other", "MINT", json!(["t:1", "alice", 1]))).is_empty());
        let rec = |a: &str, b: &str| {
            json!(["t:1", 1, {"account": a, "current": 1, "previous": 0},
                   {"account": b, "current": 1, "previous": 0}])
        };
        assert_eq!(
            sides(&ev(v2, "ledger", "RECONCILE", rec("", "bob"))),
            ["bob"]
        );
        assert_eq!(
            sides(&ev(v1, "policy-manager", "RECONCILE", rec("a", "b"))),
            ["a", "b"]
        );
        assert!(sides(&ev("free", "ledger", "RECONCILE", rec("a", "b"))).is_empty());
        let bad = json!(["t:1", 1, {"acct": "a"}, {"account": "b"}]);
        assert!(sides(&ev(v2, "ledger", "RECONCILE", bad)).is_empty());
        // A 3-param TRANSFER on a ledger with a string third: stock's coin rule rejects a
        // string amount (secpass A I-4), so only the marmalade receiver `params[2]` is kept.
        let t3 = sides(&ev(v2, "ledger", "TRANSFER", json!(["t:1", "a", "5"])));
        assert_eq!(t3, ["5"]);
        let t3 = sides(&ev(v2, "ledger", "TRANSFER", json!(["t:1", "a", 5])));
        assert_eq!(t3, ["t:1", "a"]);
        // Accounts up to 1024 bytes.
        assert!(holder_account_ok(&"a".repeat(MAX_HOLDER_ACCOUNT_LEN)));
        assert!(!holder_account_ok(&"a".repeat(MAX_HOLDER_ACCOUNT_LEN + 1)));
        assert!(!holder_account_ok(""));
    }

    /// F-01: a cross-chain event's chain id is a string in `p[3]`; those arms must still run,
    /// and only when `p[2]` is an amount.
    #[test]
    fn xchain_sides_need_an_amount_and_a_string_chain() {
        let sides = |name: &str, p: serde_json::Value| {
            let e = json!({"params": p, "name": name,
                "module": {"namespace": null, "name": "coin"}, "moduleHash": "h"});
            let mut acc = Vec::new();
            event_accounts(&e, &mut acc);
            acc.into_iter().map(str::to_owned).collect::<Vec<_>>()
        };
        assert_eq!(
            sides("TRANSFER_XCHAIN", json!(["alice", "bob", 1.5, "2"])),
            ["alice", "bob"]
        );
        assert_eq!(
            sides(
                "TRANSFER_XCHAIN_RECD",
                json!(["", "bob", {"decimal": "1.5"}, "1"])
            ),
            ["", "bob"]
        );
        assert!(sides("TRANSFER_XCHAIN", json!(["alice", "bob", "x", "2"])).is_empty());
        assert!(sides("TRANSFER_XCHAIN", json!(["alice", "bob", 1.5, 2])).is_empty());
        // The poly-fungible arm keeps its number-or-object `p[3]`.
        assert_eq!(
            sides("TRANSFER", json!(["t:id", "bob", "carol", 1.0])),
            ["bob", "carol"]
        );
        assert!(sides("TRANSFER", json!(["t:id", "bob", "carol", "1"])).is_empty());
    }
}
