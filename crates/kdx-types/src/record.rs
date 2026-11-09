//! v1 record codec (spec §3.4 "Record encoding"). Hand-written, field-ordered,
//! LEB128 varints; every record starts with the format-version byte `1`.
//!
//! Notation: `u8` one byte; `[N]` N raw bytes; `varint` LEB128 u64;
//! `bytes` = varint length + raw bytes; `value` = `bytes` holding one
//! value-codec item (see `value.rs`).
//!
//! # TxRecord (format version 1)
//!
//! ```text
//! u8      version = 1
//! u8      flags   b0 request_key  b1 cmd  b2 logs  b3 pact_tx_id
//!                 b4 metadata     b5 continuation  b6 extra  b7 cb_req_key
//! u8      bits    b0 status_ok  b1 raw_tx  b2 raw_output  b3 cmd_unparsed
//!                 b4 cmd_reencoded  (b5..b7 zero)
//! u8      present output-envelope keys present in the CommandResult, bit i
//!                 = ENVELOPE_KEYS[i]: gas result reqKey logs events metaData
//!                 continuation txId (0xff = all present)
//! [8]     key     TxKey, big-endian
//! [32]    request_key                        if flags.b0 (user txs)
//! cb_req_key                                 if flags.b7 (coinbase)
//!         u8 form: 0 Plain [32] | 1 B64Text [32] | 2 B64Quoted [32] | 3 Other bytes(utf-8)
//! bytes   cmd     original signed cmd string  if flags.b1
//! varint  n_sigs, then per sig: u8 tag 0 = Ed25519 [64] | 1 = Other bytes(utf-8)
//! bytes   result  value of `result.data` (success) or `result.error` (failure)
//! varint  gas
//! [32]    logs                               if flags.b2 (absent bit = JSON null)
//! varint  pact_tx_id                         if flags.b3 (absent bit = JSON null)
//! bytes   metadata value                     if flags.b4 (absent bit = JSON null)
//! bytes   continuation value                 if flags.b5 (absent bit = JSON null)
//! varint  n_events, then per event: varint module (ModuleId), varint name
//!         (EventNameId), varint module_hash (ModuleHash id), bytes params value
//! varint  n_transfers, then varint event position each
//! bytes   extra value (a map)                if flags.b6
//! bytes   raw_tx      original tx object bytes   if bits.b1
//! bytes   raw_output  original output bytes      if bits.b2
//! ```
//!
//! `raw_tx` / `raw_output` hold the exact base64-decoded Merkle leaf bytes the node served,
//! set only when rendering the typed fields does not reproduce them byte for byte (escape
//! forms, key order, or content serde_json cannot represent). Render returns them verbatim.
//! `cmd_unparsed` marks a user tx whose `cmd` is not representable JSON (the `cmd` bytes are
//! still stored verbatim; only derived fields such as the sender are missing).
//! `cmd_reencoded` marks a user tx whose served `cmd` does not hash to its request key: the
//! node serves a re-encoded `cmd` for many legacy txs (below about height 2.4M on mainnet01).
//! The served bytes are stored as is; the original bytes are not available from the node.
//!
//! `extra` keeps the record lossless when a field does not fit its typed slot:
//! envelope keys (`gas`, `result`, `reqKey`, `logs`, `events`, `metaData`,
//! `continuation`, `txId`, or any unknown key) map to their original JSON
//! value and win over the typed field on render; `tx.sigs` holds the whole
//! original `sigs` array when an entry is not `{"sig": <string>}`.
//!
//! # BlockRecord (format version 1)
//!
//! ```text
//! u8      version = 1
//! u8      flags   b0 merkle_verified  b1 raw_miner  b2 adj_refs  (b3..b7 zero)
//! [32]    hash, [32] parent
//! u8      chain
//! varint  height
//! [32]    weight, [32] target            (little-endian bytes as on the wire)
//! [8]     nonce, big-endian u64
//! varint  creation_time, epoch_start, feature_flags
//! [32]    payload_hash, [32] transactions_hash, [32] outputs_hash
//! varint  n_adjacents, then per entry: u8 chain, then
//!         [32] hash                          if not flags.b2 (wire order kept)
//!         u8 form, [32] hash if form 0       if flags.b2: form 0 = raw hash, form 1 = ref,
//!                                            the hash equals the stored hash of block
//!                                            (chain, height - 1) (adjacent parents sit one
//!                                            height below the block, like its own parent)
//! bytes   chainweb_version (utf-8)
//! varint  miner_account (AccountId; u32::MAX = minerData has no account string)
//! bytes   miner_data value (the whole minerData object; null when only raw_miner holds it)
//! bytes   raw_miner  original minerData bytes  if flags.b1
//! ```
//!
//! `adj_refs` (flags.b2) is a segment encoding only (`encode_block_with`/`decode_block_with`);
//! the hot store uses the plain form (`encode_block`/`decode_block`) where b2 is never set.
//!
//! `merkle_verified` is set once the payload Merkle roots have been recomputed from the leaves
//! (P5). Until then it is false: the payload contents are NOT verified, only consistent.

use crate::hash::Hash32;
use crate::ids::TxKey;
use crate::varint::{get_bytes, get_u32, get_u64, get_u8, put_bytes, put_u64, take_array};
use crate::{Error, Result};

pub const FORMAT_VERSION: u8 = 1;

/// CommandResult envelope keys in the order the node emits them.
pub const ENVELOPE_KEYS: [&str; 8] = [
    "gas",
    "result",
    "reqKey",
    "logs",
    "events",
    "metaData",
    "continuation",
    "txId",
];
pub const PRESENT_ALL: u8 = 0xff;

/// `miner_account` when minerData carries no account string.
pub const NO_ACCOUNT: u32 = u32::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sig {
    /// Exactly 128 lowercase hex chars on the wire.
    Ed25519([u8; 64]),
    /// Anything else (WebAuthn JSON, uppercase hex, placeholders), kept verbatim.
    Other(String),
}

impl Sig {
    pub fn from_wire(s: &str) -> Sig {
        if s.len() == 128
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            let mut a = [0u8; 64];
            if hex::decode_to_slice(s, &mut a).is_ok() && hex::encode(a) == s {
                return Sig::Ed25519(a);
            }
        }
        Sig::Other(s.to_owned())
    }
    pub fn to_wire(&self) -> String {
        match self {
            Sig::Ed25519(a) => hex::encode(a),
            Sig::Other(s) => s.clone(),
        }
    }
}

/// The coinbase output's `reqKey` (kept first-class, not in `extra`). Mainnet
/// shows three wire forms, all derived from the parent block hash `h`:
/// genesis `b64(h)`, older `b64("\"" + b64(h) + "\"")`, newer `b64(b64(h))`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CbReqKey {
    /// `reqKey` = b64url(h).
    Plain(Hash32),
    /// `reqKey` = b64url(utf8(b64url(h))).
    B64Text(Hash32),
    /// `reqKey` = b64url(utf8("\"" + b64url(h) + "\"")).
    B64Quoted(Hash32),
    /// Any other string, verbatim.
    Other(String),
}

impl CbReqKey {
    pub fn from_wire(s: &str) -> CbReqKey {
        let c = CbReqKey::classify(s);
        debug_assert_eq!(c.to_wire(), s);
        c
    }
    fn classify(s: &str) -> CbReqKey {
        if let Ok(h) = Hash32::from_b64url(s) {
            return CbReqKey::Plain(h);
        }
        if let Ok(inner) = crate::hash::b64url_decode(s) {
            if let Ok(t) = std::str::from_utf8(&inner) {
                let cand = if let Some(q) = t.strip_prefix('"').and_then(|t| t.strip_suffix('"')) {
                    Hash32::from_b64url(q).ok().map(CbReqKey::B64Quoted)
                } else {
                    Hash32::from_b64url(t).ok().map(CbReqKey::B64Text)
                };
                if let Some(c) = cand {
                    if c.to_wire() == s {
                        return c;
                    }
                }
            }
        }
        CbReqKey::Other(s.to_owned())
    }
    pub fn to_wire(&self) -> String {
        use crate::hash::b64url_encode;
        match self {
            CbReqKey::Plain(h) => h.to_b64url(),
            CbReqKey::B64Text(h) => b64url_encode(h.to_b64url().as_bytes()),
            CbReqKey::B64Quoted(h) => b64url_encode(format!("\"{}\"", h.to_b64url()).as_bytes()),
            CbReqKey::Other(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EventRec {
    /// ModuleId of `"ns.name"` or `"name"`.
    pub module: u32,
    /// EventNameId of `"<module>.<name>"`.
    pub name: u32,
    /// Id in `DictKind::ModuleHash`.
    pub module_hash: u32,
    /// Value codec of the `params` array.
    pub params: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRecord {
    pub key: TxKey,
    /// None for coinbase.
    pub request_key: Option<Hash32>,
    /// Coinbase output `reqKey` (None for user txs, whose reqKey is `request_key`).
    pub cb_req_key: Option<CbReqKey>,
    /// Original signed `cmd` string bytes (None for coinbase).
    pub cmd: Option<Vec<u8>>,
    pub sigs: Vec<Sig>,
    pub status_ok: bool,
    /// Value codec of `result.data` or `result.error`.
    pub result: Vec<u8>,
    pub gas: u64,
    pub logs: Option<Hash32>,
    pub pact_tx_id: Option<u64>,
    pub metadata: Option<Vec<u8>>,
    pub continuation: Option<Vec<u8>>,
    pub events: Vec<EventRec>,
    /// Event positions of fungible `TRANSFER` events.
    pub transfers: Vec<u16>,
    /// Bitmask of envelope keys present (bit i = `ENVELOPE_KEYS[i]`).
    pub present: u8,
    /// Value-codec map of fields that did not fit their typed slot.
    pub extra: Option<Vec<u8>>,
    /// Original tx object bytes, when the typed fields do not render them exactly.
    pub raw_tx: Option<Vec<u8>>,
    /// Original output (CommandResult) bytes, when the typed fields do not render them exactly.
    pub raw_output: Option<Vec<u8>>,
    /// `cmd` could not be parsed as representable JSON (derived fields missing).
    pub cmd_unparsed: bool,
    /// The served `cmd` does not hash to the request key (legacy re-encoded `cmd`).
    pub cmd_reencoded: bool,
}

impl Default for TxRecord {
    fn default() -> Self {
        TxRecord {
            key: TxKey::default(),
            request_key: None,
            cb_req_key: None,
            cmd: None,
            sigs: Vec::new(),
            status_ok: false,
            result: Vec::new(),
            gas: 0,
            logs: None,
            pact_tx_id: None,
            metadata: None,
            continuation: None,
            events: Vec::new(),
            transfers: Vec::new(),
            present: PRESENT_ALL,
            extra: None,
            raw_tx: None,
            raw_output: None,
            cmd_unparsed: false,
            cmd_reencoded: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockRecord {
    pub hash: Hash32,
    pub parent: Hash32,
    /// Wire order kept (the node's JSON object order is not sorted).
    pub adjacents: Vec<(u8, Hash32)>,
    pub chain: u8,
    pub height: u64,
    pub weight: [u8; 32],
    pub target: [u8; 32],
    pub nonce: u64,
    pub creation_time: u64,
    pub epoch_start: u64,
    pub feature_flags: u64,
    pub payload_hash: Hash32,
    pub transactions_hash: Hash32,
    pub outputs_hash: Hash32,
    pub chainweb_version: String,
    pub miner_account: u32,
    pub miner_data: Vec<u8>,
    /// Original minerData bytes, when `miner_data` does not render them exactly.
    pub raw_miner: Option<Vec<u8>>,
    /// Payload Merkle roots recomputed from the leaves and matched (P5). False until then.
    pub merkle_verified: bool,
}

fn version(buf: &mut &[u8]) -> Result<()> {
    match get_u8(buf)? {
        FORMAT_VERSION => Ok(()),
        v => Err(Error::Codec(format!(
            "unsupported record format version {v}"
        ))),
    }
}

fn hash(buf: &mut &[u8]) -> Result<Hash32> {
    Ok(Hash32(take_array(buf)?))
}

fn utf8(b: &[u8]) -> Result<String> {
    String::from_utf8(b.to_vec()).map_err(|_| Error::Codec("invalid utf-8".into()))
}

pub fn encode_tx(r: &TxRecord, out: &mut Vec<u8>) {
    let mut flags = 0u8;
    let set = |f: &mut u8, b: u8, on: bool| {
        if on {
            *f |= 1 << b
        }
    };
    set(&mut flags, 0, r.request_key.is_some());
    set(&mut flags, 1, r.cmd.is_some());
    set(&mut flags, 2, r.logs.is_some());
    set(&mut flags, 3, r.pact_tx_id.is_some());
    set(&mut flags, 4, r.metadata.is_some());
    set(&mut flags, 5, r.continuation.is_some());
    set(&mut flags, 6, r.extra.is_some());
    set(&mut flags, 7, r.cb_req_key.is_some());
    let bits = r.status_ok as u8
        | (r.raw_tx.is_some() as u8) << 1
        | (r.raw_output.is_some() as u8) << 2
        | (r.cmd_unparsed as u8) << 3
        | (r.cmd_reencoded as u8) << 4;
    out.push(FORMAT_VERSION);
    out.push(flags);
    out.push(bits);
    out.push(r.present);
    out.extend_from_slice(&r.key.to_be_bytes());
    if let Some(h) = &r.request_key {
        out.extend_from_slice(&h.0);
    }
    if let Some(c) = &r.cb_req_key {
        match c {
            CbReqKey::Plain(h) => {
                out.push(0);
                out.extend_from_slice(&h.0)
            }
            CbReqKey::B64Text(h) => {
                out.push(1);
                out.extend_from_slice(&h.0)
            }
            CbReqKey::B64Quoted(h) => {
                out.push(2);
                out.extend_from_slice(&h.0)
            }
            CbReqKey::Other(s) => {
                out.push(3);
                put_bytes(out, s.as_bytes())
            }
        }
    }
    if let Some(c) = &r.cmd {
        put_bytes(out, c);
    }
    put_u64(out, r.sigs.len() as u64);
    for s in &r.sigs {
        match s {
            Sig::Ed25519(a) => {
                out.push(0);
                out.extend_from_slice(a);
            }
            Sig::Other(s) => {
                out.push(1);
                put_bytes(out, s.as_bytes());
            }
        }
    }
    put_bytes(out, &r.result);
    put_u64(out, r.gas);
    if let Some(h) = &r.logs {
        out.extend_from_slice(&h.0);
    }
    if let Some(t) = r.pact_tx_id {
        put_u64(out, t);
    }
    if let Some(m) = &r.metadata {
        put_bytes(out, m);
    }
    if let Some(c) = &r.continuation {
        put_bytes(out, c);
    }
    put_u64(out, r.events.len() as u64);
    for e in &r.events {
        put_u64(out, e.module as u64);
        put_u64(out, e.name as u64);
        put_u64(out, e.module_hash as u64);
        put_bytes(out, &e.params);
    }
    put_u64(out, r.transfers.len() as u64);
    for t in &r.transfers {
        put_u64(out, *t as u64);
    }
    if let Some(x) = &r.extra {
        put_bytes(out, x);
    }
    if let Some(x) = &r.raw_tx {
        put_bytes(out, x);
    }
    if let Some(x) = &r.raw_output {
        put_bytes(out, x);
    }
}

pub fn decode_tx(mut b: &[u8]) -> Result<TxRecord> {
    let buf = &mut b;
    version(buf)?;
    let flags = get_u8(buf)?;
    let bits = get_u8(buf)?;
    if bits & !0x1f != 0 {
        return Err(Error::Codec(format!("reserved tx bits set: {bits:#x}")));
    }
    let present = get_u8(buf)?;
    let f = |i: u8| flags & (1 << i) != 0;
    let key = TxKey::from_be_bytes(take_array(buf)?);
    let request_key = if f(0) { Some(hash(buf)?) } else { None };
    let cb_req_key = if f(7) {
        Some(match get_u8(buf)? {
            0 => CbReqKey::Plain(hash(buf)?),
            1 => CbReqKey::B64Text(hash(buf)?),
            2 => CbReqKey::B64Quoted(hash(buf)?),
            3 => CbReqKey::Other(utf8(get_bytes(buf)?)?),
            t => return Err(Error::Codec(format!("unknown cb_req_key form {t}"))),
        })
    } else {
        None
    };
    let cmd = if f(1) {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    let n = get_u64(buf)? as usize;
    let mut sigs = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        sigs.push(match get_u8(buf)? {
            0 => Sig::Ed25519(take_array(buf)?),
            1 => Sig::Other(utf8(get_bytes(buf)?)?),
            t => return Err(Error::Codec(format!("unknown sig tag {t}"))),
        });
    }
    let result = get_bytes(buf)?.to_vec();
    let gas = get_u64(buf)?;
    let logs = if f(2) { Some(hash(buf)?) } else { None };
    let pact_tx_id = if f(3) { Some(get_u64(buf)?) } else { None };
    let metadata = if f(4) {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    let continuation = if f(5) {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    let n = get_u64(buf)? as usize;
    let mut events = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        events.push(EventRec {
            module: get_u32(buf)?,
            name: get_u32(buf)?,
            module_hash: get_u32(buf)?,
            params: get_bytes(buf)?.to_vec(),
        });
    }
    let n = get_u64(buf)? as usize;
    let mut transfers = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        let t = get_u64(buf)?;
        transfers.push(u16::try_from(t).map_err(|_| Error::Codec(format!("transfer pos {t}")))?);
    }
    let extra = if f(6) {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    let raw_tx = if bits & 2 != 0 {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    let raw_output = if bits & 4 != 0 {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    if !buf.is_empty() {
        return Err(Error::Codec(format!(
            "{} trailing bytes after tx record",
            buf.len()
        )));
    }
    Ok(TxRecord {
        key,
        request_key,
        cb_req_key,
        cmd,
        sigs,
        status_ok: bits & 1 != 0,
        result,
        gas,
        logs,
        pact_tx_id,
        metadata,
        continuation,
        events,
        transfers,
        present,
        extra,
        raw_tx,
        raw_output,
        cmd_unparsed: bits & 8 != 0,
        cmd_reencoded: bits & 0x10 != 0,
    })
}

/// Plain block encoding (hot store): adjacent hashes stored raw.
pub fn encode_block(r: &BlockRecord, out: &mut Vec<u8>) {
    encode_block_impl(r, out, None)
}

/// Segment block encoding: `adj_is_ref(chain, hash)` says whether `hash` equals the stored hash
/// of block (chain, height - 1); such adjacents are written as a 1-byte ref (flags.b2).
pub fn encode_block_with(
    r: &BlockRecord,
    out: &mut Vec<u8>,
    adj_is_ref: &dyn Fn(u8, &Hash32) -> bool,
) {
    encode_block_impl(r, out, Some(adj_is_ref))
}

/// `adj_is_ref(chain, hash)` predicate of `encode_block_with`.
pub type AdjIsRef<'a> = &'a dyn Fn(u8, &Hash32) -> bool;

fn encode_block_impl(r: &BlockRecord, out: &mut Vec<u8>, adj_is_ref: Option<AdjIsRef<'_>>) {
    out.push(FORMAT_VERSION);
    out.push(
        r.merkle_verified as u8
            | (r.raw_miner.is_some() as u8) << 1
            | (adj_is_ref.is_some() as u8) << 2,
    );
    out.extend_from_slice(&r.hash.0);
    out.extend_from_slice(&r.parent.0);
    out.push(r.chain);
    put_u64(out, r.height);
    out.extend_from_slice(&r.weight);
    out.extend_from_slice(&r.target);
    out.extend_from_slice(&r.nonce.to_be_bytes());
    put_u64(out, r.creation_time);
    put_u64(out, r.epoch_start);
    put_u64(out, r.feature_flags);
    out.extend_from_slice(&r.payload_hash.0);
    out.extend_from_slice(&r.transactions_hash.0);
    out.extend_from_slice(&r.outputs_hash.0);
    put_u64(out, r.adjacents.len() as u64);
    for (c, h) in &r.adjacents {
        out.push(*c);
        match adj_is_ref {
            Some(f) if r.height > 0 && f(*c, h) => out.push(1),
            Some(_) => {
                out.push(0);
                out.extend_from_slice(&h.0);
            }
            None => out.extend_from_slice(&h.0),
        }
    }
    put_bytes(out, r.chainweb_version.as_bytes());
    put_u64(out, r.miner_account as u64);
    put_bytes(out, &r.miner_data);
    if let Some(x) = &r.raw_miner {
        put_bytes(out, x);
    }
}

/// Decode a block record. A record with adjacent refs (flags.b2) decodes only when every
/// adjacent is in raw form; use `decode_block_with` to resolve refs.
pub fn decode_block(b: &[u8]) -> Result<BlockRecord> {
    decode_block_with(b, &|_, _| None)
}

/// Decode a block record, resolving adjacent refs with `resolve(chain, height - 1)`.
pub fn decode_block_with(
    mut b: &[u8],
    resolve: &dyn Fn(u8, u64) -> Option<Hash32>,
) -> Result<BlockRecord> {
    let buf = &mut b;
    version(buf)?;
    let bflags = get_u8(buf)?;
    if bflags & !0x07 != 0 {
        return Err(Error::Codec(format!(
            "reserved block flags set: {bflags:#x}"
        )));
    }
    let hash_ = hash(buf)?;
    let parent = hash(buf)?;
    let chain = get_u8(buf)?;
    let height = get_u64(buf)?;
    let weight = take_array(buf)?;
    let target = take_array(buf)?;
    let nonce = u64::from_be_bytes(take_array(buf)?);
    let creation_time = get_u64(buf)?;
    let epoch_start = get_u64(buf)?;
    let feature_flags = get_u64(buf)?;
    let payload_hash = hash(buf)?;
    let transactions_hash = hash(buf)?;
    let outputs_hash = hash(buf)?;
    let n = get_u64(buf)? as usize;
    let mut adjacents = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        let c = get_u8(buf)?;
        let h = if bflags & 4 == 0 {
            hash(buf)?
        } else {
            match get_u8(buf)? {
                0 => hash(buf)?,
                1 => height
                    .checked_sub(1)
                    .and_then(|p| resolve(c, p))
                    .ok_or_else(|| {
                        Error::Codec(format!(
                            "unresolved adjacent ref: chain {c} height {height} - 1"
                        ))
                    })?,
                f => return Err(Error::Codec(format!("unknown adjacent form {f}"))),
            }
        };
        adjacents.push((c, h));
    }
    let chainweb_version = utf8(get_bytes(buf)?)?;
    let miner_account = get_u32(buf)?;
    let miner_data = get_bytes(buf)?.to_vec();
    let raw_miner = if bflags & 2 != 0 {
        Some(get_bytes(buf)?.to_vec())
    } else {
        None
    };
    if !buf.is_empty() {
        return Err(Error::Codec(format!(
            "{} trailing bytes after block record",
            buf.len()
        )));
    }
    Ok(BlockRecord {
        hash: hash_,
        parent,
        adjacents,
        chain,
        height,
        weight,
        target,
        nonce,
        creation_time,
        epoch_start,
        feature_flags,
        payload_hash,
        transactions_hash,
        outputs_hash,
        chainweb_version,
        miner_account,
        miner_data,
        raw_miner,
        merkle_verified: bflags & 1 != 0,
    })
}
