//! Name-resolved digest of the hot store (`hot stats --digest`, ruling on 9B-1): two stores
//! that hold the same blocks digest equal even when their dictionaries gave names other ids.
//!
//! - `records`: [`kdx_segment::content::ContentDigest`] over `Headers` + `TailTxs`, blocks
//!   in (height, chain) order, txs in index order.
//! - `TailTxsOrphans` (only when there are any): `TailTxs` rows whose block has no `Headers`
//!   row, as (TxKey, request key) pairs in key order. A sound store has none (m11).
//! - `TailPostings`: (family, resolved entity name, TxKey), sorted.
//! - `Dict<Kind>`: the sorted set of names (ids not used).
//! - every other non-additive table except `Meta`: blake3 over its (key, value) rows, as
//!   before (`StageCheckpoint`, `HeaderByHash`, `TailRk` and `Counters` hold no ids;
//!   `Undo` and the deprecated `Defpacts` keep raw bytes).

use std::collections::BTreeMap;

use anyhow::Result;
use kdx_decode::Family;
use kdx_hot::{HotDict, Table, Txn};
use kdx_segment::content::ContentDigest;
use kdx_types::dict::{Dict, DictKind};
use kdx_types::record::{decode_block, decode_tx, TxRecord};
use kdx_types::TxKey;

/// (rows, blake3 hex) per digest name.
pub type Digest = BTreeMap<String, (u64, String)>;

/// Display name of a posting entity, resolved through the dictionary.
pub fn entity_name(d: &dyn Dict, family: u8, e: u64) -> String {
    let n = |k: DictKind, id: u64| match u32::try_from(id).ok().and_then(|i| d.name(k, i)) {
        Some(s) => s.to_owned(),
        None => format!("?{k:?}#{id}"),
    };
    let lo = e & 0xffff_ffff;
    match Family::ALL.into_iter().find(|f| *f as u8 == family) {
        Some(Family::Acct) => n(DictKind::Account, e),
        Some(Family::Pk) => n(DictKind::PubKey, e),
        Some(Family::Xfer) => format!(
            "{} {}",
            n(DictKind::Account, e >> 32),
            n(DictKind::Module, lo)
        ),
        Some(Family::Evt) if e >> 32 == 1 => format!("{}.*", n(DictKind::Module, lo)),
        Some(Family::Evt) => n(DictKind::EventName, e),
        _ => format!("{e:016x}"),
    }
}

fn rows_digest<'a>(rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>) -> (u64, String) {
    let mut h = blake3::Hasher::new();
    let mut n = 0u64;
    for (k, v) in rows {
        h.update(&(k.len() as u32).to_be_bytes());
        h.update(k);
        h.update(&(v.len() as u32).to_be_bytes());
        h.update(v);
        n += 1;
    }
    (n, h.finalize().to_hex().to_string())
}

/// The name-resolved digest of every non-additive hot table (see the module doc).
pub fn hot_digest(txn: &impl Txn) -> Result<Digest> {
    let dict = HotDict::load(txn)?;
    let mut out = Digest::new();

    let mut txs: BTreeMap<(u32, u8), Vec<TxRecord>> = BTreeMap::new();
    for (k, v) in txn.scan(Table::TailTxs, &[], |_| true)? {
        let key = TxKey::from_be_bytes(k.as_slice().try_into()?);
        txs.entry((key.height(), key.chain()))
            .or_default()
            .push(decode_tx(&v)?);
    }
    let mut blocks = BTreeMap::new();
    for (_, v) in txn.scan(Table::Headers, &[], |_| true)? {
        let b = decode_block(&v)?;
        blocks.insert((u32::try_from(b.height)?, b.chain), b);
    }
    let mut d = ContentDigest::new();
    for (pos, b) in &blocks {
        let mut t = txs.remove(pos).unwrap_or_default();
        t.sort_by_key(|r| r.key.pos());
        d.block(b, &t, &dict)?;
    }
    let c = d.finish();
    out.insert("records".into(), (c.blocks + c.txs, c.blake3));
    // Rows left in `txs` have no header: an undo that removed `Headers` but not `TailTxs`.
    // Name-free (TxKey, request key), so equal orphans digest equal across stores.
    if !txs.is_empty() {
        let rows: Vec<([u8; 8], Vec<u8>)> = txs
            .into_values()
            .flatten()
            .map(|t| {
                let rk = t.request_key.map(|h| h.0.to_vec()).unwrap_or_default();
                (t.key.to_be_bytes(), rk)
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .collect();
        out.insert(
            "TailTxsOrphans".into(),
            rows_digest(rows.iter().map(|(k, v)| (&k[..], v.as_slice()))),
        );
    }

    let mut posts: Vec<(u8, String, Vec<u8>)> = Vec::new();
    for (k, _) in txn.scan(Table::TailPostings, &[], |_| true)? {
        if k.len() < 9 {
            anyhow::bail!("TailPostings key of {} bytes", k.len());
        }
        let e = u64::from_be_bytes(k[1..9].try_into()?);
        posts.push((k[0], entity_name(&dict, k[0], e), k[9..].to_vec()));
    }
    posts.sort();
    let enc: Vec<(Vec<u8>, Vec<u8>)> = posts
        .into_iter()
        .map(|(f, n, t)| {
            let mut key = vec![f];
            key.extend_from_slice(n.as_bytes());
            (key, t)
        })
        .collect();
    out.insert(
        "TailPostings".into(),
        rows_digest(enc.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))),
    );

    for k in DictKind::ALL {
        let mut names: Vec<&str> = (0..dict.len(k) as u32)
            .filter_map(|i| dict.name(k, i))
            .collect();
        names.sort_unstable();
        out.insert(
            format!("Dict{k:?}"),
            rows_digest(names.iter().map(|s| (s.as_bytes(), &[][..]))),
        );
    }

    for t in Table::all() {
        let name = t.name();
        if t == Table::Meta
            || t.is_additive()
            || name.starts_with("Dict")
            || matches!(t, Table::Headers | Table::TailTxs | Table::TailPostings)
        {
            continue;
        }
        let rows = txn.scan(t, &[], |_| true)?;
        out.insert(
            name,
            rows_digest(rows.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))),
        );
    }
    Ok(out)
}
