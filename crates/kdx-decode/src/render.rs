//! Re-render records as node JSON (header object encoding, CommandResult, tx).

use kdx_types::hash::b64url_encode;
use kdx_types::record::{BlockRecord, TxRecord, ENVELOPE_KEYS};
use kdx_types::{value, Dict, DictKind};
use serde_json::{Map, Number, Value};

use crate::build::EXTRA_TX_SIGS;
use crate::json::parse_untrusted;

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error(transparent)]
    Codec(#[from] kdx_types::Error),
    #[error("unknown {kind:?} id {id}")]
    Dict { kind: DictKind, id: u32 },
    #[error("record shape: {0}")]
    Shape(String),
    /// The stored raw bytes have no faithful `Value` form; use the `*_bytes` renderers.
    #[error("stored raw bytes are not representable as a JSON value: {0}")]
    Unrepresentable(String),
}

fn raw_value(b: &[u8]) -> Result<Value> {
    parse_untrusted(b).map_err(|e| RenderError::Unrepresentable(e.to_string()))
}

fn to_bytes(v: &Value) -> Vec<u8> {
    serde_json::to_vec(v).expect("Value serialisation cannot fail")
}

type Result<T> = std::result::Result<T, RenderError>;

fn name(dict: &dyn Dict, kind: DictKind, id: u32) -> Result<&str> {
    dict.name(kind, id).ok_or(RenderError::Dict { kind, id })
}

fn extra_map(r: &TxRecord, dict: &dyn Dict) -> Result<Map<String, Value>> {
    match &r.extra {
        None => Ok(Map::new()),
        Some(b) => match value::from_slice(b, dict)? {
            Value::Object(m) => Ok(m),
            _ => Err(RenderError::Shape("extra is not a map".into())),
        },
    }
}

fn opt_value(b: &Option<Vec<u8>>, dict: &dyn Dict) -> Result<Value> {
    Ok(match b {
        Some(b) => value::from_slice(b, dict)?,
        None => Value::Null,
    })
}

/// The CommandResult JSON (from `raw_output` when stored, else envelope keys in node order).
pub fn render_output(r: &TxRecord, dict: &dyn Dict) -> Result<Value> {
    match &r.raw_output {
        Some(b) => raw_value(b),
        None => render_output_typed(r, dict),
    }
}

/// The exact CommandResult bytes (the Merkle leaf of `outputsHash`).
pub fn render_output_bytes(r: &TxRecord, dict: &dyn Dict) -> Result<Vec<u8>> {
    match &r.raw_output {
        Some(b) => Ok(b.clone()),
        None => Ok(to_bytes(&render_output_typed(r, dict)?)),
    }
}

/// The CommandResult rendered from the typed fields only (ignores `raw_output`).
pub fn render_output_typed(r: &TxRecord, dict: &dyn Dict) -> Result<Value> {
    let mut extra = extra_map(r, dict)?;
    let mut o = Map::new();
    for (i, k) in ENVELOPE_KEYS.iter().enumerate() {
        if let Some(v) = extra.shift_remove(*k) {
            o.insert((*k).to_owned(), v);
            continue;
        }
        if r.present & (1 << i) == 0 {
            continue;
        }
        let v = match *k {
            "gas" => Value::Number(Number::from(r.gas)),
            "result" => {
                let mut m = Map::new();
                let v = value::from_slice(&r.result, dict)?;
                if r.status_ok {
                    m.insert("status".into(), "success".into());
                    m.insert("data".into(), v);
                } else {
                    m.insert("status".into(), "failure".into());
                    m.insert("error".into(), v);
                }
                Value::Object(m)
            }
            "reqKey" => match (&r.request_key, &r.cb_req_key) {
                (Some(h), _) => Value::String(h.to_b64url()),
                (None, Some(c)) => Value::String(c.to_wire()),
                (None, None) => {
                    return Err(RenderError::Shape("reqKey present but not stored".into()))
                }
            },
            "logs" => r.logs.map_or(Value::Null, |h| Value::String(h.to_b64url())),
            "events" => {
                let mut a = Vec::with_capacity(r.events.len());
                for e in &r.events {
                    let module = name(dict, DictKind::Module, e.module)?;
                    let full = name(dict, DictKind::EventName, e.name)?;
                    let ev_name = full
                        .strip_prefix(module)
                        .and_then(|s| s.strip_prefix('.'))
                        .ok_or_else(|| {
                            RenderError::Shape(format!("event name {full:?} not under {module:?}"))
                        })?;
                    let (ns, mname) = match module.split_once('.') {
                        Some((ns, n)) => (Value::String(ns.to_owned()), n),
                        None => (Value::Null, module),
                    };
                    let mut md = Map::new();
                    md.insert("namespace".into(), ns);
                    md.insert("name".into(), mname.into());
                    let mut m = Map::new();
                    m.insert("params".into(), value::from_slice(&e.params, dict)?);
                    m.insert("name".into(), ev_name.into());
                    m.insert("module".into(), Value::Object(md));
                    m.insert(
                        "moduleHash".into(),
                        name(dict, DictKind::ModuleHash, e.module_hash)?.into(),
                    );
                    a.push(Value::Object(m));
                }
                Value::Array(a)
            }
            "metaData" => opt_value(&r.metadata, dict)?,
            "continuation" => opt_value(&r.continuation, dict)?,
            "txId" => r
                .pact_tx_id
                .map_or(Value::Null, |t| Value::Number(Number::from(t))),
            _ => unreachable!("ENVELOPE_KEYS"),
        };
        o.insert((*k).to_owned(), v);
    }
    // Unknown envelope keys kept in extra (tx-level `tx.*` entries are not output fields).
    for (k, v) in extra {
        if !k.starts_with("tx.") {
            o.insert(k, v);
        }
    }
    Ok(Value::Object(o))
}

/// The original signed `cmd` bytes (None for coinbase).
pub fn render_cmd(r: &TxRecord) -> Option<&[u8]> {
    r.cmd.as_deref()
}

/// The transaction object `{"hash", "sigs", "cmd"}` (None for coinbase).
pub fn render_tx(r: &TxRecord, dict: &dyn Dict) -> Result<Option<Value>> {
    match &r.raw_tx {
        Some(b) => raw_value(b).map(Some),
        None => render_tx_typed(r, dict),
    }
}

/// The exact tx object bytes (the Merkle leaf of `transactionsHash`; None for coinbase).
pub fn render_tx_bytes(r: &TxRecord, dict: &dyn Dict) -> Result<Option<Vec<u8>>> {
    match &r.raw_tx {
        Some(b) => Ok(Some(b.clone())),
        None => Ok(render_tx_typed(r, dict)?.map(|v| to_bytes(&v))),
    }
}

/// The tx object rendered from the typed fields only (ignores `raw_tx`).
pub fn render_tx_typed(r: &TxRecord, dict: &dyn Dict) -> Result<Option<Value>> {
    let (Some(h), Some(cmd)) = (&r.request_key, &r.cmd) else {
        return Ok(None);
    };
    let cmd =
        std::str::from_utf8(cmd).map_err(|_| RenderError::Shape("cmd is not utf-8".into()))?;
    let sigs = match extra_map(r, dict)?.shift_remove(EXTRA_TX_SIGS) {
        Some(v) => v,
        None => Value::Array(
            r.sigs
                .iter()
                .map(|s| {
                    let mut m = Map::new();
                    m.insert("sig".into(), Value::String(s.to_wire()));
                    Value::Object(m)
                })
                .collect(),
        ),
    };
    let mut m = Map::new();
    m.insert("hash".into(), Value::String(h.to_b64url()));
    m.insert("sigs".into(), sigs);
    m.insert("cmd".into(), Value::String(cmd.to_owned()));
    Ok(Some(Value::Object(m)))
}

/// Header in object encoding, keys in node order.
pub fn render_header(r: &BlockRecord) -> Value {
    let n = |v: u64| Value::Number(Number::from(v));
    let mut adj = Map::new();
    for (c, h) in &r.adjacents {
        adj.insert(c.to_string(), Value::String(h.to_b64url()));
    }
    let mut m = Map::new();
    m.insert("nonce".into(), Value::String(r.nonce.to_string()));
    m.insert("creationTime".into(), n(r.creation_time));
    m.insert("parent".into(), Value::String(r.parent.to_b64url()));
    m.insert("adjacents".into(), Value::Object(adj));
    m.insert("target".into(), Value::String(b64url_encode(&r.target)));
    m.insert(
        "payloadHash".into(),
        Value::String(r.payload_hash.to_b64url()),
    );
    m.insert("chainId".into(), n(r.chain as u64));
    m.insert("weight".into(), Value::String(b64url_encode(&r.weight)));
    m.insert("height".into(), n(r.height));
    m.insert(
        "chainwebVersion".into(),
        Value::String(r.chainweb_version.clone()),
    );
    m.insert("epochStart".into(), n(r.epoch_start));
    m.insert("featureFlags".into(), n(r.feature_flags));
    m.insert("hash".into(), Value::String(r.hash.to_b64url()));
    Value::Object(m)
}

/// The block's `minerData` object.
pub fn render_miner(r: &BlockRecord, dict: &dyn Dict) -> Result<Value> {
    match &r.raw_miner {
        Some(b) => raw_value(b),
        None => Ok(value::from_slice(&r.miner_data, dict)?),
    }
}

/// The exact minerData bytes (a payload Merkle input).
pub fn render_miner_bytes(r: &BlockRecord, dict: &dyn Dict) -> Result<Vec<u8>> {
    match &r.raw_miner {
        Some(b) => Ok(b.clone()),
        None => Ok(to_bytes(&value::from_slice(&r.miner_data, dict)?)),
    }
}

/// Round-trip comparison (Task 5 definition): top-level object keys compared
/// order-insensitively, every nested value compared as `serde_json::to_string`
/// (order and number text exact). Returns the first differing path, or None.
pub fn compare_json(original: &Value, rendered: &Value) -> Option<String> {
    let s = |v: &Value| serde_json::to_string(v).unwrap_or_default();
    match (original.as_object(), rendered.as_object()) {
        (Some(a), Some(b)) => {
            for (k, v) in a {
                match b.get(k) {
                    None => return Some(format!("$.{k} (missing)")),
                    Some(w) if s(v) != s(w) => return Some(format!("$.{k}")),
                    _ => {}
                }
            }
            b.keys()
                .find(|k| !a.contains_key(*k))
                .map(|k| format!("$.{k} (unexpected)"))
        }
        _ => (s(original) != s(rendered)).then(|| "$".to_owned()),
    }
}
