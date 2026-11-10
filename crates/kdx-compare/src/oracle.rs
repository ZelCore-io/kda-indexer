//! Node oracle (spec section 7.1): `/poll` plus the canonical `/header` and payload, projected
//! to the same shape as chainweb-data's `TxDetail` so `diff` applies.
//!
//! Projected shape (field names follow chainweb-data's TxDetail; shapes only, no code reused):
//! `requestKey, blockHash, height, chain, blockTime, creationTime, result{status, data|error},
//! gas, logs, txid, continuation, events[{name, params}], code, data, nonce, sender, gasLimit,
//! gasPrice, ttl, signers, sigs, pactId, step, rollback, proof`.
//!
//! Mapping decisions:
//! - `result`: chainweb-data returns the bare value plus `success: bool`; it is rebuilt as the
//!   node's `{status: "success", data}` / `{status: "failure", error}`.
//! - `events`: chainweb-data names events `<module>.<name>` (module with namespace) and has no
//!   `moduleHash`; the node side is projected to that, `moduleHash` is dropped. An absent
//!   `events` key in old node outputs is projected as `[]`.
//! - `blockTime`: block header `creationTime` (microseconds) as ISO-8601 with 6 fraction
//!   digits; chainweb-data trims trailing zeros, `diff` compares at the coarser precision.
//! - `creationTime`: the cmd's `meta.creationTime` (seconds) as ISO-8601 without fraction.
//! - Command fields (`code`, `data`, `nonce`, `sender`, `gasLimit`, `gasPrice`, `ttl`,
//!   `signers`, `pactId`, `step`, `rollback`, `proof`) come from the cmd the node serves in the
//!   block payload; `sigs` from the payload tx. `gasPrice` is a float in chainweb-data (see the
//!   normaliser). For exec txs `pactId/step/rollback/proof` are null, for cont txs `code` is null.
//! - Signers without capabilities: the cmd has no `clist` key, chainweb-data shows `[]`; the
//!   node side gets `clist: []` (normaliser rule).
//! - Failed txs: chainweb-data stores `txid: 0`, the node has no tx id (`null`); a failed row's
//!   `0` is projected as `null` (normaliser rule).
//! - chainweb-data only fields `previousSteps`, `initialCode`, `metadata` and `success` are not
//!   projected (no node counterpart in `/poll`).

use kdx_decode::DecodedTx;
use kdx_types::blake2b_256;
use serde_json::{json, Map, Value};

use crate::normalize::{iso_micros, parse_iso};
use crate::report::iso_secs;

/// Projected keys that come from the signed command (differences there on legacy txs are the
/// `legacy-cmd` category).
pub const CMD_FIELDS: &[&str] = &[
    "code",
    "data",
    "nonce",
    "sender",
    "gasLimit",
    "gasPrice",
    "ttl",
    "creationTime",
    "signers",
    "sigs",
    "pactId",
    "step",
    "rollback",
    "proof",
];

/// Whether a diff path is under one of `CMD_FIELDS`.
pub fn is_cmd_path(path: &str) -> bool {
    let first = path.trim_start_matches('/').split('/').next().unwrap_or("");
    CMD_FIELDS.contains(&first)
}

fn get(v: &Value, k: &str) -> Value {
    v.get(k).cloned().unwrap_or(Value::Null)
}

/// chainweb-data `TxDetail` -> projection.
pub fn project_tx_detail(cw: &Value) -> Value {
    let success = cw.get("success").and_then(Value::as_bool);
    let result = match success {
        Some(true) => json!({"status": "success", "data": get(cw, "result")}),
        Some(false) => json!({"status": "failure", "error": get(cw, "result")}),
        None => get(cw, "result"),
    };
    let events = cw
        .get("events")
        .and_then(Value::as_array)
        .map(|evs| {
            evs.iter()
                .map(|e| json!({"name": get(e, "name"), "params": get(e, "params")}))
                .collect::<Vec<_>>()
        })
        .map(Value::Array)
        .unwrap_or(Value::Null);
    let mut m = Map::new();
    for k in ["requestKey", "blockHash", "height", "chain", "blockTime"] {
        m.insert(k.into(), get(cw, k));
    }
    m.insert("result".into(), result);
    for k in ["gas", "logs", "txid", "continuation"] {
        m.insert(k.into(), get(cw, k));
    }
    // chainweb-data stores txid 0 for failed txs; the node has no txId (null) for them.
    if success == Some(false) && cw.get("txid").and_then(Value::as_u64) == Some(0) {
        m.insert("txid".into(), Value::Null);
    }
    m.insert("events".into(), events);
    for k in CMD_FIELDS {
        m.insert((*k).into(), get(cw, k));
    }
    Value::Object(m)
}

fn qualified_event_name(e: &Value) -> Value {
    let name = e.get("name").and_then(Value::as_str).unwrap_or("");
    let module = e.get("module");
    let mname = module
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match module
        .and_then(|m| m.get("namespace"))
        .and_then(Value::as_str)
    {
        Some(ns) if !ns.is_empty() => Value::String(format!("{ns}.{mname}.{name}")),
        _ => Value::String(format!("{mname}.{name}")),
    }
}

/// Block fields of the projection from a node header (object encoding).
pub fn header_fields(header: &Value) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("blockHash".into(), get(header, "hash"));
    m.insert("height".into(), get(header, "height"));
    m.insert("chain".into(), get(header, "chainId"));
    let bt = header
        .get("creationTime")
        .and_then(Value::as_u64)
        .map(|us| Value::String(iso_micros(us)))
        .unwrap_or(Value::Null);
    m.insert("blockTime".into(), bt);
    m
}

/// Node `/poll` entry + canonical header (+ the payload tx, for the command fields) ->
/// projection.
pub fn from_poll(poll: &Value, header: &Value, tx: Option<&DecodedTx>) -> Value {
    let hf = header_fields(header);
    let mut m = Map::new();
    m.insert("requestKey".into(), get(poll, "reqKey"));
    for k in ["blockHash", "height", "chain", "blockTime"] {
        m.insert(k.into(), hf.get(k).cloned().unwrap_or(Value::Null));
    }
    m.insert("result".into(), get(poll, "result"));
    m.insert("gas".into(), get(poll, "gas"));
    m.insert("logs".into(), get(poll, "logs"));
    m.insert("txid".into(), get(poll, "txId"));
    m.insert("continuation".into(), get(poll, "continuation"));
    let events = poll
        .get("events")
        .and_then(Value::as_array)
        .map(|evs| {
            evs.iter()
                .map(|e| json!({"name": qualified_event_name(e), "params": get(e, "params")}))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    m.insert("events".into(), Value::Array(events));
    if let Some(tx) = tx {
        // An unparseable cmd (see `TxFlags::cmd_unparsed`) projects as nulls.
        let null = Value::Null;
        let c = tx.cmd_parsed.as_ref().unwrap_or(&null);
        let meta = c.get("meta").cloned().unwrap_or(Value::Null);
        let exec = c.pointer("/payload/exec");
        let cont = c.pointer("/payload/cont");
        let p = |k: &str| exec.or(cont).map(|x| get(x, k)).unwrap_or(Value::Null);
        let cp = |k: &str| cont.map(|x| get(x, k)).unwrap_or(Value::Null);
        m.insert(
            "code".into(),
            exec.map(|x| get(x, "code")).unwrap_or(Value::Null),
        );
        m.insert("data".into(), p("data"));
        m.insert("nonce".into(), get(c, "nonce"));
        m.insert("sender".into(), get(&meta, "sender"));
        m.insert("gasLimit".into(), get(&meta, "gasLimit"));
        m.insert("gasPrice".into(), get(&meta, "gasPrice"));
        m.insert("ttl".into(), get(&meta, "ttl"));
        // A fractional `meta.creationTime` (seen live: 1692638383.532) is rounded to whole
        // seconds, as chainweb-data shows it.
        let ct = meta
            .get("creationTime")
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f.round() as u64)))
            .map(|s| Value::String(iso_secs(s)))
            .unwrap_or(Value::Null);
        m.insert("creationTime".into(), ct);
        m.insert("signers".into(), signers_with_clist(get(c, "signers")));
        m.insert("sigs".into(), Value::Array(tx.sigs.clone()));
        m.insert("pactId".into(), cp("pactId"));
        m.insert("step".into(), cp("step"));
        m.insert("rollback".into(), cp("rollback"));
        m.insert("proof".into(), cp("proof"));
    }
    Value::Object(m)
}

/// chainweb-data reports a signer without capabilities as `clist: []`; the cmd omits the key.
fn signers_with_clist(mut signers: Value) -> Value {
    if let Value::Array(xs) = &mut signers {
        for s in xs {
            if let Value::Object(m) = s {
                if !m.contains_key("clist") {
                    m.insert("clist".into(), Value::Array(Vec::new()));
                }
                // An explicit `"addr": null` in the cmd is not shown by chainweb-data or compat.
                if m.get("addr").is_some_and(Value::is_null) {
                    m.remove("addr");
                }
            }
        }
    }
    signers
}

/// Block projection of a chainweb-data row (`/txs/txs` item) for the header compare.
pub fn project_block(cw: &Value) -> Value {
    let mut m = Map::new();
    for k in ["blockHash", "height", "chain", "blockTime"] {
        m.insert(k.into(), get(cw, k));
    }
    Value::Object(m)
}

/// Whether the served cmd hashes to the request key.
pub fn cmd_hashes(tx: &DecodedTx) -> bool {
    blake2b_256(tx.cmd.as_bytes()) == tx.hash
}

/// For a legacy tx: rebuilds the cmd JSON from chainweb-data's fields, using the node's served
/// cmd as the key-order template, and reports whether that text hashes to the request key.
/// Returns `(rebuilt_hashes, template_reserialises_to_served_text)`.
pub fn rehash_from_a(tx: &DecodedTx, cw: &Value) -> (bool, bool) {
    let Some(parsed) = &tx.cmd_parsed else {
        return (false, false);
    };
    let template_ok = serde_json::to_string(parsed).ok().as_deref() == Some(&tx.cmd);
    let mut c = parsed.clone();
    let set = |c: &mut Value, ptr: &str, v: Option<Value>| {
        if let (Some(slot), Some(v)) = (c.pointer_mut(ptr), v) {
            *slot = v;
        }
    };
    let a = |k: &str| cw.get(k).cloned();
    if c.pointer("/payload/exec").is_some() {
        set(&mut c, "/payload/exec/code", a("code"));
        set(&mut c, "/payload/exec/data", a("data"));
    } else {
        for k in ["pactId", "step", "rollback", "proof", "data"] {
            set(&mut c, &format!("/payload/cont/{k}"), a(k));
        }
    }
    set(&mut c, "/signers", a("signers"));
    set(&mut c, "/nonce", a("nonce"));
    for k in ["sender", "gasLimit", "gasPrice", "ttl"] {
        set(&mut c, &format!("/meta/{k}"), a(k));
    }
    let ct = cw
        .get("creationTime")
        .and_then(Value::as_str)
        .and_then(parse_iso)
        .map(|t| json!(t.secs));
    set(&mut c, "/meta/creationTime", ct);
    let text = serde_json::to_string(&c).unwrap_or_default();
    (blake2b_256(text.as_bytes()) == tx.hash, template_ok)
}
