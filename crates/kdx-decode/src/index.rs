//! Index extraction (spec §3.6): posting entities, defpact ids and code hashes per tx.
//! Shared by backfill and the live tail; called right after `build_records` with the same
//! dictionary, so interning stays in first-seen order.
//!
//! Acct sources: meta sender, signer `addr`, the coinbase miner (on the coinbase tx) and each
//! event's account strings (`build::event_accounts`: TRANSFER / poly-fungible TRANSFER /
//! TRANSFER_XCHAIN sides and every Pact principal in the params, at any depth). All of them
//! are interned by `build::intern_block` before encoding, so the postings are a pure function
//! of the block: identical across backfill, tail, replay and rebuild, whatever the dictionary
//! held before. Non-principal names that appear only in other event params get no posting.
//! Strings longer than `MAX_ACCOUNT_LEN` are never accounts.
//! Pk: signer `pubKey` (interned here). Code: blake2b-128 of `exec.code`.
//! Txs with `cmd_unparsed` get only output-derived postings (events, transfers, defpact).

use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{blake2b_128, DictKind, DictMut, Hash32, TxKey};
use serde_json::Value;

use crate::build::{
    account_ok, encode_records, event_accounts, intern_block, parse_event, transfer_accounts,
    BuildError,
};
use crate::payload::DecodedBlock;
use crate::verify::VerifyReport;

/// Posting family (spec §3.6). The value is the on-disk family byte.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Family {
    Xfer = 0,
    Acct = 1,
    Pk = 2,
    Evt = 3,
    Code = 4,
}

impl Family {
    pub const ALL: [Family; 5] = [
        Family::Xfer,
        Family::Acct,
        Family::Pk,
        Family::Evt,
        Family::Code,
    ];
    pub fn from_u8(b: u8) -> Option<Family> {
        Family::ALL.get(b as usize).copied()
    }
    pub fn name(self) -> &'static str {
        match self {
            Family::Xfer => "xfer",
            Family::Acct => "acct",
            Family::Pk => "pk",
            Family::Evt => "evt",
            Family::Code => "code",
        }
    }
}

/// Entity of the `xfer` family: `(AccountId << 32) | ModuleId`.
pub fn xfer_entity(account: u32, module: u32) -> u64 {
    (account as u64) << 32 | module as u64
}

/// Entity of the `evt` family for the module wildcard (`name = *`): `(1 << 32) | ModuleId`.
pub fn evt_module_entity(module: u32) -> u64 {
    1u64 << 32 | module as u64
}

/// Entity of the `code` family: the first 8 bytes of the CodeHash, big-endian.
pub fn code_entity(code_hash: &[u8; 16]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&code_hash[..8]);
    u64::from_be_bytes(a)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxIndex {
    pub key: TxKey,
    /// Sorted, deduplicated `(family, entity)` pairs.
    pub postings: Vec<(Family, u64)>,
    /// Defpact id: `cont.pactId` for a `cont` tx, `continuation.pactId` of the output for an
    /// `exec` that starts a pact.
    pub pact_id: Option<Hash32>,
    /// blake2b-128 of `exec.code`.
    pub code_hash: Option<[u8; 16]>,
}

/// One block ready for storage: records plus their index entries (Tasks 4, 6, 7).
#[derive(Debug, Clone)]
pub struct IndexedBlock {
    pub block: BlockRecord,
    pub txs: Vec<TxRecord>,
    /// Same order and length as `txs`.
    pub idx: Vec<TxIndex>,
    /// User txs with `cmd_reencoded` set.
    pub cmd_reencoded: u32,
    /// Account strings longer than `MAX_ACCOUNT_LEN` (not indexed).
    pub unindexed_accounts: u32,
}

/// `build_records` + `extract` with one dictionary: the whole per-block decode step.
pub fn index_block(
    b: &DecodedBlock,
    report: &VerifyReport,
    dict: &mut impl DictMut,
) -> Result<IndexedBlock, BuildError> {
    let unindexed_accounts = intern_block(b, dict);
    let (block, txs) = encode_records(b, report, &*dict)?;
    let idx = extract(b, &txs, dict);
    let cmd_reencoded = txs.iter().filter(|t| t.cmd_reencoded).count() as u32;
    Ok(IndexedBlock {
        block,
        txs,
        idx,
        cmd_reencoded,
        unindexed_accounts,
    })
}

fn pact_id_of(v: Option<&Value>) -> Option<Hash32> {
    v.and_then(Value::as_str)
        .and_then(|s| Hash32::from_b64url(s).ok())
}

/// Postings from a CommandResult: events (Evt, module wildcard), fungible transfers (Xfer)
/// and every event's account strings (Acct).
fn output_postings(out: &Value, dict: &mut impl DictMut, p: &mut Vec<(Family, u64)>) {
    let Some(evs) = out.get("events").and_then(Value::as_array) else {
        return;
    };
    for e in evs {
        if let Some(ev) = parse_event(e) {
            let module = dict.intern(DictKind::Module, &ev.module);
            let name = dict.intern(DictKind::EventName, &ev.event_name);
            p.push((Family::Evt, name as u64));
            p.push((Family::Evt, evt_module_entity(module)));
            if let Some((from, to)) = transfer_accounts(&ev) {
                for a in [from, to] {
                    if account_ok(a) {
                        let id = dict.intern(DictKind::Account, a);
                        p.push((Family::Xfer, xfer_entity(id, module)));
                    }
                }
            }
        }
        let mut acc = Vec::new();
        event_accounts(e, &mut acc);
        for a in acc {
            account_posting(a, dict, p);
        }
    }
}

/// Acct posting for `a` (already interned by `intern_block` when `account_ok`).
fn account_posting(a: &str, dict: &mut impl DictMut, p: &mut Vec<(Family, u64)>) {
    if account_ok(a) {
        p.push((Family::Acct, dict.intern(DictKind::Account, a) as u64));
    }
}

/// Index entries for every record of the block (`txs` from `build_records(b, ..)`).
pub fn extract(b: &DecodedBlock, txs: &[TxRecord], dict: &mut impl DictMut) -> Vec<TxIndex> {
    let mut out = Vec::with_capacity(txs.len());
    for r in txs {
        let pos = r.key.pos() as usize;
        let mut p = Vec::new();
        let mut pact_id = None;
        let mut code_hash = None;
        if pos == 0 {
            if let Some(a) = b.miner.get("account").and_then(Value::as_str) {
                account_posting(a, dict, &mut p);
            }
            output_postings(&b.coinbase, dict, &mut p);
        } else if let Some(t) = b.txs.get(pos - 1) {
            output_postings(&t.output, dict, &mut p);
            let out_pact = t.output.get("continuation").and_then(|c| c.get("pactId"));
            if let (Some(c), false) = (&t.cmd_parsed, r.cmd_unparsed) {
                if let Some(s) = c.pointer("/meta/sender").and_then(Value::as_str) {
                    account_posting(s, dict, &mut p);
                }
                for s in c
                    .get("signers")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(pk) = s.get("pubKey").and_then(Value::as_str) {
                        let id = dict.intern(DictKind::PubKey, pk);
                        p.push((Family::Pk, id as u64));
                    }
                    if let Some(a) = s.get("addr").and_then(Value::as_str) {
                        account_posting(a, dict, &mut p);
                    }
                }
                if let Some(code) = c.pointer("/payload/exec/code").and_then(Value::as_str) {
                    let h = blake2b_128(code.as_bytes());
                    p.push((Family::Code, code_entity(&h)));
                    code_hash = Some(h);
                    pact_id = pact_id_of(out_pact);
                } else if let Some(cont) = c.pointer("/payload/cont") {
                    pact_id = pact_id_of(cont.get("pactId")).or_else(|| pact_id_of(out_pact));
                }
            } else {
                pact_id = pact_id_of(out_pact);
            }
        }
        p.sort_unstable();
        p.dedup();
        out.push(TxIndex {
            key: r.key,
            postings: p,
            pact_id,
            code_hash,
        });
    }
    out
}
