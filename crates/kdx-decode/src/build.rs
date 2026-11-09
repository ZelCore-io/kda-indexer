//! Build v1 records from a decoded block (spec §3.4 "Record encoding").
//!
//! Names are interned for the whole block first (first-seen order: miner
//! account, then coinbase, then user txs in payload order; per output: meta
//! sender, then events' module, event name, module hash and fungible TRANSFER
//! accounts), and only then are values encoded, so every account string in the
//! block is stored as an account ref.

use kdx_types::record::{
    BlockRecord, CbReqKey, EventRec, Sig, TxRecord, ENVELOPE_KEYS, NO_ACCOUNT,
};
use kdx_types::{value, Dict, DictKind, DictMut, Hash32, TxKey};
use serde_json::{Map, Value};

use crate::payload::{exact_u64, DecodedBlock};

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

/// Fungible `TRANSFER`: `[from: str, to: str, amount: number | {"decimal"|"int": ..}]`.
pub fn transfer_accounts<'a>(ev: &EventParts<'a>) -> Option<(&'a str, &'a str)> {
    if ev.name != "TRANSFER" {
        return None;
    }
    let p = ev.params.as_array()?;
    if p.len() != 3 {
        return None;
    }
    let amount_ok = match &p[2] {
        Value::Number(_) => true,
        Value::Object(o) => o.len() == 1 && (o.contains_key("decimal") || o.contains_key("int")),
        _ => false,
    };
    if !amount_ok {
        return None;
    }
    Some((p[0].as_str()?, p[1].as_str()?))
}

/// All events canonical, or None (then the events go to `extra`).
fn parse_events(o: &Map<String, Value>) -> Option<Vec<EventParts<'_>>> {
    o.get("events")?
        .as_array()?
        .iter()
        .map(parse_event)
        .collect()
}

fn intern_output(o: &Value, dict: &mut impl DictMut) {
    let Some(o) = o.as_object() else { return };
    for ev in parse_events(o).unwrap_or_default() {
        dict.intern(DictKind::Module, &ev.module);
        dict.intern(DictKind::EventName, &ev.event_name);
        dict.intern(DictKind::ModuleHash, ev.module_hash);
        if let Some((from, to)) = transfer_accounts(&ev) {
            for a in [from, to] {
                if !a.is_empty() {
                    dict.intern(DictKind::Account, a);
                }
            }
        }
    }
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

pub fn build_records(b: &DecodedBlock, dict: &mut impl DictMut) -> (BlockRecord, Vec<TxRecord>) {
    // 1. Intern everything the block references, in first-seen order.
    let miner_acct = b.miner.get("account").and_then(Value::as_str);
    let miner_account = miner_acct.map_or(NO_ACCOUNT, |a| dict.intern(DictKind::Account, a));
    intern_output(&b.coinbase, dict);
    for t in &b.txs {
        if let Some(s) = t.cmd_parsed.pointer("/meta/sender").and_then(Value::as_str) {
            if !s.is_empty() {
                dict.intern(DictKind::Account, s);
            }
        }
        intern_output(&t.output, dict);
    }
    let dict: &dyn Dict = &*dict;

    // 2. Encode.
    let h = &b.header;
    let height = u32::try_from(h.height).unwrap_or(u32::MAX);
    let block = BlockRecord {
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
    };

    let mut txs = Vec::with_capacity(b.txs.len() + 1);
    let mut cb = TxRecord {
        key: TxKey::new(height, h.chain_id, 0),
        ..TxRecord::default()
    };
    let mut extra = Map::new();
    fill_output(&mut cb, &b.coinbase, None, dict, &mut extra);
    finish_extra(&mut cb, extra, dict);
    txs.push(cb);

    for (i, t) in b.txs.iter().enumerate() {
        let pos = u16::try_from(i + 1).unwrap_or(u16::MAX);
        let mut r = TxRecord {
            key: TxKey::new(height, h.chain_id, pos),
            request_key: Some(t.hash),
            cmd: Some(t.cmd.as_bytes().to_vec()),
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
        txs.push(r);
    }
    (block, txs)
}
