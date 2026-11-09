//! Decode a header (object encoding) and a payload-with-outputs into typed
//! parts. Unknown top-level keys are an error (spec §9.3: loud on new shapes).
//!
//! Every Merkle leaf (tx object, output, coinbase, minerData) keeps its original
//! base64-decoded bytes. Parsing a leaf into a `Value` is best-effort: user-controlled
//! content that serde_json cannot represent faithfully (see `json.rs`) leaves the parsed
//! form empty and sets a flag; the block still decodes and the bytes stay authoritative.

use std::collections::BTreeMap;

use kdx_types::hash::b64url_decode;
use kdx_types::Hash32;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::json::{parse_untrusted, Unrepresentable};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unknown shape at {path}")]
    UnknownShape { path: String },
    #[error("missing field {path}")]
    Missing { path: String },
    #[error("bad field {path}: {reason}")]
    Bad { path: String, reason: String },
}

fn bad(path: impl Into<String>, reason: impl ToString) -> DecodeError {
    DecodeError::Bad {
        path: path.into(),
        reason: reason.to_string(),
    }
}

pub type Result<T> = std::result::Result<T, DecodeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub hash: Hash32,
    pub parent: Hash32,
    pub adjacents: BTreeMap<u8, Hash32>,
    /// Chain ids of `adjacents` in wire (JSON object) order, for exact re-render.
    pub adjacents_order: Vec<u8>,
    pub chain_id: u8,
    pub height: u64,
    /// Raw 32 bytes of the base64url wire value.
    pub weight: [u8; 32],
    pub target: [u8; 32],
    /// Decimal string on the wire.
    pub nonce: u64,
    /// Microseconds.
    pub creation_time: u64,
    pub epoch_start: u64,
    pub feature_flags: u64,
    pub payload_hash: Hash32,
    pub chainweb_version: String,
}

/// Per-tx best-effort parse results (all false for an ordinary tx).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxFlags {
    /// The tx object could not be represented as a `Value`; `hash` and `cmd` were taken
    /// from a loose parse and `sigs` is empty. `tx_bytes` is authoritative.
    pub tx_unrepresentable: bool,
    /// The output could not be represented; `output` is `Null`, `output_bytes` is authoritative.
    pub output_unrepresentable: bool,
    /// `cmd` is not representable JSON; `cmd_parsed` is None (the `cmd` string is kept verbatim).
    pub cmd_unparsed: bool,
}

#[derive(Debug, Clone)]
pub struct DecodedTx {
    pub hash: Hash32,
    /// Empty when `flags.tx_unrepresentable`.
    pub sigs: Vec<Value>,
    /// The exact signed string.
    pub cmd: String,
    /// Best-effort parse of `cmd` (only used for `meta.sender`, `payload.cont`, ...).
    pub cmd_parsed: Option<Value>,
    /// The CommandResult; `Null` when `flags.output_unrepresentable`.
    pub output: Value,
    /// `reqKey` of the output (also recovered by a loose parse when the output is unrepresentable).
    pub out_req_key: Option<String>,
    /// Original base64-decoded tx object bytes (Merkle leaf of `transactionsHash`).
    pub tx_bytes: Vec<u8>,
    /// Original base64-decoded output bytes (Merkle leaf of `outputsHash`).
    pub output_bytes: Vec<u8>,
    pub flags: TxFlags,
}

#[derive(Debug, Clone)]
pub struct DecodedBlock {
    pub header: Header,
    /// `Null` when `miner_unrepresentable`.
    pub miner: Value,
    pub miner_bytes: Vec<u8>,
    pub miner_unrepresentable: bool,
    /// `Null` when `coinbase_unrepresentable`.
    pub coinbase: Value,
    pub coinbase_bytes: Vec<u8>,
    pub coinbase_unrepresentable: bool,
    pub txs: Vec<DecodedTx>,
    pub payload_hash: Hash32,
    pub transactions_hash: Hash32,
    pub outputs_hash: Hash32,
}

const HEADER_KEYS: [&str; 13] = [
    "nonce",
    "creationTime",
    "parent",
    "adjacents",
    "target",
    "payloadHash",
    "chainId",
    "weight",
    "height",
    "chainwebVersion",
    "epochStart",
    "featureFlags",
    "hash",
];
const PAYLOAD_KEYS: [&str; 6] = [
    "transactions",
    "minerData",
    "transactionsHash",
    "outputsHash",
    "payloadHash",
    "coinbase",
];
const TX_KEYS: [&str; 3] = ["hash", "sigs", "cmd"];
const OUTPUT_KEYS: [&str; 8] = kdx_types::record::ENVELOPE_KEYS;

fn obj<'a>(v: &'a Value, path: &str) -> Result<&'a Map<String, Value>> {
    v.as_object().ok_or_else(|| DecodeError::UnknownShape {
        path: path.to_owned(),
    })
}

fn check_keys(m: &Map<String, Value>, allowed: &[&str], path: &str) -> Result<()> {
    for k in m.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(DecodeError::UnknownShape {
                path: format!("{path}.{k}"),
            });
        }
    }
    Ok(())
}

fn field<'a>(m: &'a Map<String, Value>, k: &str, path: &str) -> Result<&'a Value> {
    m.get(k).ok_or_else(|| DecodeError::Missing {
        path: format!("{path}.{k}"),
    })
}

fn str_field<'a>(m: &'a Map<String, Value>, k: &str, path: &str) -> Result<&'a str> {
    field(m, k, path)?
        .as_str()
        .ok_or_else(|| bad(format!("{path}.{k}"), "not a string"))
}

fn hash_field(m: &Map<String, Value>, k: &str, path: &str) -> Result<Hash32> {
    Hash32::from_b64url(str_field(m, k, path)?).map_err(|e| bad(format!("{path}.{k}"), e))
}

/// Exact u64: the number text must be the canonical decimal form.
pub fn exact_u64(v: &Value) -> Option<u64> {
    let n = v.as_number()?;
    let u = n.as_u64()?;
    (n.to_string() == u.to_string()).then_some(u)
}

fn u64_field(m: &Map<String, Value>, k: &str, path: &str) -> Result<u64> {
    exact_u64(field(m, k, path)?).ok_or_else(|| bad(format!("{path}.{k}"), "not an exact u64"))
}

pub fn decode_header(h: &Value) -> Result<Header> {
    let p = "header";
    let m = obj(h, p)?;
    check_keys(m, &HEADER_KEYS, p)?;
    let nonce_s = str_field(m, "nonce", p)?;
    let nonce: u64 = nonce_s.parse().map_err(|e| bad("header.nonce", e))?;
    if nonce.to_string() != nonce_s {
        return Err(bad("header.nonce", "non-canonical decimal"));
    }
    let raw32 = |k: &str| -> Result<[u8; 32]> { Ok(hash_field(m, k, p)?.0) };
    let chain_id = u64_field(m, "chainId", p)?;
    let chain_id = u8::try_from(chain_id).map_err(|e| bad("header.chainId", e))?;
    let adj = obj(field(m, "adjacents", p)?, "header.adjacents")?;
    let mut adjacents = BTreeMap::new();
    let mut adjacents_order = Vec::with_capacity(adj.len());
    for (k, v) in adj {
        let path = format!("header.adjacents.{k}");
        let c: u8 = k.parse().map_err(|e| bad(&path, e))?;
        if c.to_string() != *k {
            return Err(bad(&path, "non-canonical chain id"));
        }
        let h = Hash32::from_b64url(v.as_str().ok_or_else(|| bad(&path, "not a string"))?)
            .map_err(|e| bad(&path, e))?;
        adjacents.insert(c, h);
        adjacents_order.push(c);
    }
    Ok(Header {
        hash: hash_field(m, "hash", p)?,
        parent: hash_field(m, "parent", p)?,
        adjacents,
        adjacents_order,
        chain_id,
        height: u64_field(m, "height", p)?,
        weight: raw32("weight")?,
        target: raw32("target")?,
        nonce,
        creation_time: u64_field(m, "creationTime", p)?,
        epoch_start: u64_field(m, "epochStart", p)?,
        feature_flags: u64_field(m, "featureFlags", p)?,
        payload_hash: hash_field(m, "payloadHash", p)?,
        chainweb_version: str_field(m, "chainwebVersion", p)?.to_owned(),
    })
}

/// Base64url-decode a leaf (malformed base64 from the node is a hard error) and parse it
/// best-effort.
fn leaf(s: &Value, path: &str) -> Result<(Vec<u8>, std::result::Result<Value, Unrepresentable>)> {
    let s = s.as_str().ok_or_else(|| bad(path, "not a string"))?;
    let bytes = b64url_decode(s).map_err(|e| bad(path, e))?;
    let parsed = parse_untrusted(&bytes);
    Ok((bytes, parsed))
}

fn decode_output(v: Value, path: &str) -> Result<Value> {
    check_keys(obj(&v, path)?, &OUTPUT_KEYS, path)?;
    Ok(v)
}

/// Output leaf: `(bytes, value or Null, unrepresentable)`.
fn output_leaf(s: &Value, path: &str) -> Result<(Vec<u8>, Value, bool)> {
    let (bytes, parsed) = leaf(s, path)?;
    Ok(match parsed {
        Ok(v) => (bytes, decode_output(v, path)?, false),
        Err(_) => (bytes, Value::Null, true),
    })
}

#[derive(Deserialize)]
struct TxLoose {
    hash: String,
    cmd: String,
}

#[derive(Deserialize)]
struct OutLoose {
    #[serde(rename = "reqKey")]
    req_key: Option<String>,
}

fn decode_tx(pair: &[Value], i: usize) -> Result<DecodedTx> {
    let pos = i + 1;
    let tp = format!("tx[{pos}]");
    let op = format!("{tp}.output");
    let (tx_bytes, tx_parsed) = leaf(&pair[0], &tp)?;
    let (output_bytes, output, output_unrepresentable) = output_leaf(&pair[1], &op)?;
    let mut flags = TxFlags {
        output_unrepresentable,
        ..TxFlags::default()
    };
    let (hash, sigs, cmd) = match tx_parsed {
        Ok(tx) => {
            let tm = obj(&tx, &tp)?;
            check_keys(tm, &TX_KEYS, &tp)?;
            let sigs = field(tm, "sigs", &tp)?
                .as_array()
                .ok_or_else(|| bad(format!("{tp}.sigs"), "not an array"))?
                .clone();
            (
                hash_field(tm, "hash", &tp)?,
                sigs,
                str_field(tm, "cmd", &tp)?.to_owned(),
            )
        }
        Err(why) => {
            // The node serves `{"hash", "sigs", "cmd"}` with plain strings; only `sigs` could
            // hold something odd. Recover hash and cmd, keep the bytes.
            let l: TxLoose = serde_json::from_slice(&tx_bytes)
                .map_err(|e| bad(&tp, format!("{why}; loose parse: {e}")))?;
            flags.tx_unrepresentable = true;
            let hash = Hash32::from_b64url(&l.hash).map_err(|e| bad(format!("{tp}.hash"), e))?;
            (hash, Vec::new(), l.cmd)
        }
    };
    let cmd_parsed = parse_untrusted(cmd.as_bytes()).ok();
    flags.cmd_unparsed = cmd_parsed.is_none();
    let out_req_key = if output_unrepresentable {
        serde_json::from_slice::<OutLoose>(&output_bytes)
            .ok()
            .and_then(|l| l.req_key)
    } else {
        output
            .get("reqKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    Ok(DecodedTx {
        hash,
        sigs,
        cmd,
        cmd_parsed,
        output,
        out_req_key,
        tx_bytes,
        output_bytes,
        flags,
    })
}

pub fn decode_block(
    header: &Value,
    payload: &Value,
) -> std::result::Result<DecodedBlock, DecodeError> {
    let header = decode_header(header)?;
    let p = "payload";
    let m = obj(payload, p)?;
    check_keys(m, &PAYLOAD_KEYS, p)?;
    let (miner_bytes, miner) = leaf(field(m, "minerData", p)?, "payload.minerData")?;
    let miner_unrepresentable = miner.is_err();
    let miner = miner.unwrap_or(Value::Null);
    let (coinbase_bytes, coinbase, coinbase_unrepresentable) =
        output_leaf(field(m, "coinbase", p)?, "coinbase")?;
    let list = field(m, "transactions", p)?
        .as_array()
        .ok_or_else(|| bad("payload.transactions", "not an array"))?;
    let mut txs = Vec::with_capacity(list.len());
    for (i, pair) in list.iter().enumerate() {
        let pair =
            pair.as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| DecodeError::UnknownShape {
                    path: format!("payload.transactions[{i}]"),
                })?;
        txs.push(decode_tx(pair, i)?);
    }
    Ok(DecodedBlock {
        header,
        miner,
        miner_bytes,
        miner_unrepresentable,
        coinbase,
        coinbase_bytes,
        coinbase_unrepresentable,
        txs,
        payload_hash: hash_field(m, "payloadHash", p)?,
        transactions_hash: hash_field(m, "transactionsHash", p)?,
        outputs_hash: hash_field(m, "outputsHash", p)?,
    })
}
