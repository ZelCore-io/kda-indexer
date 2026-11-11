//! `inspect`: read records back from the hot store (tail) and the frozen segments, rendered
//! as the node serves them, plus `inspect sample [--compare-node]` (spec Review Focus 5).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use kdx_decode::index::{code_entity, evt_module_entity, pact_entity, xfer_entity};
use kdx_decode::{
    compare_json, render_header, render_miner, render_miner_bytes, render_output,
    render_output_bytes, render_tx, render_tx_bytes, Family,
};
use kdx_hot::tables::{block_key, parse_block_key, parse_txkey};
use kdx_hot::{HotDict, HotStore, RoTxn, Table, Txn};
use kdx_segment::Segments;
use kdx_types::record::{decode_block, decode_tx, BlockRecord, TxRecord};
use kdx_types::{Dict, DictKind, Hash32, MemDict, TxKey};
use rand::RngExt;
use serde_json::{json, Value};

use super::hot::hot_dir;
use super::segments::segments_dir;
use crate::config::Config;

pub struct Store {
    _hs: HotStore,
    txn: RoTxn,
    pub dict: Arc<MemDict>,
    pub seg: Segments,
}

impl Store {
    pub fn open(cfg: &Config, data_dir: &Path) -> anyhow::Result<Store> {
        let _ = cfg;
        let hs = HotStore::open_ro(&hot_dir(data_dir))?;
        let txn = hs.ro()?;
        let dict = Arc::new(HotDict::load(&txn)?.mem().clone());
        let seg = Segments::open(&segments_dir(data_dir), dict.clone())?;
        Ok(Store {
            _hs: hs,
            txn,
            dict,
            seg,
        })
    }

    fn hot_tx(&self, k: TxKey) -> anyhow::Result<Option<TxRecord>> {
        match self.txn.get(Table::TailTxs, &k.to_be_bytes())? {
            Some(v) => Ok(Some(decode_tx(&v)?)),
            None => Ok(None),
        }
    }

    fn hot_block(&self, chain: u8, height: u32) -> anyhow::Result<Option<BlockRecord>> {
        match self.txn.get(Table::Headers, &block_key(chain, height))? {
            Some(v) => Ok(Some(decode_block(&v)?)),
            None => Ok(None),
        }
    }

    /// Tx by TxKey: hot first, then frozen. Returns (record, source, txnum).
    pub fn tx_by_key(
        &self,
        k: TxKey,
    ) -> anyhow::Result<Option<(TxRecord, &'static str, Option<u64>)>> {
        if let Some(r) = self.hot_tx(k)? {
            return Ok(Some((r, "hot", None)));
        }
        if let Some(t) = self.seg.txnum_of(k) {
            if let Some(r) = self.seg.tx(t)? {
                return Ok(Some((r, "frozen", Some(t))));
            }
        }
        Ok(None)
    }

    /// Block: hot first, then frozen.
    pub fn block(
        &self,
        chain: u8,
        height: u32,
    ) -> anyhow::Result<Option<(BlockRecord, &'static str)>> {
        if let Some(b) = self.hot_block(chain, height)? {
            return Ok(Some((b, "hot")));
        }
        Ok(self.seg.block(chain, height)?.map(|b| (b, "frozen")))
    }
}

fn tx_json(s: &Store, r: &TxRecord, source: &str, txnum: Option<u64>) -> anyhow::Result<Value> {
    let d: &dyn Dict = s.dict.as_ref();
    Ok(json!({
        "source": source,
        "txkey": r.key.0,
        "txnum": txnum,
        "chain": r.key.chain(),
        "height": r.key.height(),
        "pos": r.key.pos(),
        "reqKey": match (&r.request_key, &r.cb_req_key) {
            (Some(h), _) => Some(h.to_b64url()),
            (None, Some(c)) => Some(c.to_wire()),
            _ => None,
        },
        "cmd_reencoded": r.cmd_reencoded,
        "cmd_unparsed": r.cmd_unparsed,
        "tx": render_tx(r, d)?,
        "output": render_output(r, d)?,
    }))
}

fn block_json(s: &Store, b: &BlockRecord, source: &str) -> anyhow::Result<Value> {
    Ok(json!({
        "source": source,
        "chain": b.chain,
        "height": b.height,
        "header": render_header(b),
        "minerData": render_miner(b, s.dict.as_ref())?,
        "txs": s.seg.txnum_of(TxKey::new(b.height as u32, b.chain, 0)).map(|_| "frozen").unwrap_or(source),
    }))
}

pub fn tx(s: &Store, reqkey: &str) -> anyhow::Result<()> {
    let rk = Hash32::from_b64url(reqkey).context("request key (base64url, 32 bytes)")?;
    let found = if let Some(v) = s.txn.get(Table::TailRk, &rk.0)? {
        let k = parse_txkey(&v).context("TailRk value")?;
        s.hot_tx(k)?.map(|r| (r, "hot", None))
    } else {
        match s.seg.by_rk(&rk)? {
            Some(t) => s.seg.tx(t)?.map(|r| (r, "frozen", Some(t))),
            None => None,
        }
    };
    let Some((r, src, t)) = found else {
        bail!("request key not found");
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&tx_json(s, &r, src, t)?)?
    );
    Ok(())
}

pub fn block(s: &Store, chain: u8, height: u32) -> anyhow::Result<()> {
    let Some((b, src)) = s.block(chain, height)? else {
        bail!("block ({chain}, {height}) not found");
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&block_json(s, &b, src)?)?
    );
    Ok(())
}

pub fn block_hash(s: &Store, hash: &str) -> anyhow::Result<()> {
    let h = Hash32::from_b64url(hash).context("block hash (base64url, 32 bytes)")?;
    let pos = match s.txn.get(Table::HeaderByHash, &h.0)? {
        Some(v) => parse_block_key(&v),
        None => s.seg.by_block_hash(&h)?,
    };
    let Some((c, ht)) = pos else {
        bail!("block hash not found");
    };
    block(s, c, ht)
}

/// Posting entity of `name` in `family` (resolved through the dictionary).
pub fn entity(
    dict: &dyn Dict,
    family: Family,
    name: &str,
    module: Option<&str>,
) -> anyhow::Result<u64> {
    let find = |k: DictKind, n: &str| {
        dict.find(k, n)
            .with_context(|| format!("{k:?} {n:?} not in the dictionary"))
    };
    Ok(match family {
        Family::Acct => find(DictKind::Account, name)? as u64,
        Family::Pk => find(DictKind::PubKey, name)? as u64,
        Family::Xfer => {
            let m = module.context("xfer needs --module (e.g. coin)")?;
            xfer_entity(find(DictKind::Account, name)?, find(DictKind::Module, m)?)
        }
        Family::Evt => match name.strip_suffix(".*") {
            Some(m) => evt_module_entity(find(DictKind::Module, m)?),
            None => find(DictKind::EventName, name)? as u64,
        },
        Family::Code => {
            let b = hex::decode(name).context("code hash hex")?;
            if b.len() < 8 {
                bail!("code hash needs at least 8 bytes of hex");
            }
            let mut a = [0u8; 16];
            a[..8].copy_from_slice(&b[..8]);
            code_entity(&a)
        }
        Family::Pact => pact_entity(&Hash32::from_b64url(name).context("pactId (base64url)")?),
    })
}

/// Request key text of a posting's tx (`<coinbase>` for coinbase, `?` when the record is
/// missing).
fn rk_of(r: Option<TxRecord>) -> String {
    match r {
        Some(r) => match r.request_key {
            Some(h) if !r.key.is_coinbase() => h.to_b64url(),
            _ => "<coinbase>".into(),
        },
        None => "?".into(),
    }
}

pub fn postings(
    s: &Store,
    family: &str,
    name: &str,
    module: Option<&str>,
    limit: usize,
) -> anyhow::Result<()> {
    let fam = Family::ALL
        .into_iter()
        .find(|f| f.name() == family)
        .with_context(|| format!("family {family:?} (acct|xfer|pk|evt|code|pact)"))?;
    let ent = entity(s.dict.as_ref(), fam, name, module)?;
    let mut prefix = vec![fam as u8];
    prefix.extend_from_slice(&ent.to_be_bytes());
    let hot = s
        .txn
        .scan(Table::TailPostings, &prefix, |k| k.starts_with(&prefix))?;
    let mut out = Vec::new();
    for (k, _) in hot.iter().rev() {
        if out.len() >= limit {
            break;
        }
        let tk = parse_txkey(&k[9..]).context("posting key")?;
        let r = match s.txn.get(Table::TailTxs, &tk.to_be_bytes())? {
            Some(v) => Some(decode_tx(&v)?),
            None => None,
        };
        out.push(json!({"source": "hot", "txkey": tk.0, "chain": tk.chain(), "height": tk.height(), "pos": tk.pos(), "rk": rk_of(r)}));
    }
    for t in s.seg.postings(fam, ent) {
        if out.len() >= limit {
            break;
        }
        let tk = s.seg.txkey(t).context("txkey of posting")?;
        let rk = rk_of(s.seg.tx(t)?);
        out.push(json!({"source": "frozen", "txnum": t, "txkey": tk.0, "chain": tk.chain(), "height": tk.height(), "pos": tk.pos(), "rk": rk}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"family": fam.name(), "entity": ent, "hot": hot.len(), "items": out})
        )?
    );
    Ok(())
}

/// `inspect sample`: `count` random frozen txs and `count` random hot txs; with
/// `compare_node`, each block is refetched and the rendered header, miner data, tx and
/// output bytes must equal the node's.
pub async fn sample(
    cfg: &Config,
    s: &Store,
    count: usize,
    compare_node: bool,
) -> anyhow::Result<()> {
    let mut rng = rand::rng();
    let mut picks: Vec<(TxRecord, &'static str, Option<u64>)> = Vec::new();
    let n = s.seg.manifest.next_txnum;
    if n > 0 {
        for _ in 0..count {
            let t = rng.random_range(0..n);
            let r = s.seg.tx(t)?.context("frozen tx missing")?;
            picks.push((r, "frozen", Some(t)));
        }
    }
    let hot = s.txn.scan(Table::TailTxs, &[], |_| true)?;
    if !hot.is_empty() {
        for _ in 0..count {
            let (_, v) = &hot[rng.random_range(0..hot.len())];
            picks.push((decode_tx(v)?, "hot", None));
        }
    }
    eprintln!(
        "sample: {} frozen + {} hot txs",
        picks.iter().filter(|p| p.1 == "frozen").count(),
        picks.iter().filter(|p| p.1 == "hot").count()
    );
    if !compare_node {
        for (r, src, t) in &picks {
            let v = tx_json(s, r, src, *t)?;
            println!(
                "{}",
                serde_json::to_string(
                    &json!({"txkey": v["txkey"], "source": src, "reqKey": v["reqKey"]})
                )?
            );
        }
        return Ok(());
    }
    let nc = super::node_client(cfg)?;
    nc.set_batch_limit(cfg.ingest.batch_size);
    let cut = nc.cut().await?;
    let mut by_block: BTreeMap<(u8, u32), Vec<&TxRecord>> = BTreeMap::new();
    for (r, _, _) in &picks {
        by_block
            .entry((r.key.chain(), r.key.height()))
            .or_default()
            .push(r);
    }
    let d: &dyn Dict = s.dict.as_ref();
    let (mut checked, mut mismatches) = (0u64, 0u64);
    let mut fail = |what: String| {
        mismatches += 1;
        eprintln!("MISMATCH {what}");
    };
    for ((c, h), txs) in &by_block {
        let ch = cut.hashes.get(c).context("chain not in cut")?;
        let hdrs = nc
            .header_branch_all(*c, &ch.hash, *h as u64, *h as u64)
            .await?;
        let hdr = hdrs.first().context("no canonical header")?;
        let ph = hdr
            .get("payloadHash")
            .and_then(Value::as_str)
            .context("payloadHash")?
            .to_owned();
        let payloads = nc
            .payloads_with_outputs(*c, std::slice::from_ref(&ph))
            .await?;
        let payload = payloads.get(&ph).context("payload missing")?;
        let db = kdx_decode::decode_block(hdr, payload)
            .map_err(|e| anyhow::anyhow!("decode ({c}, {h}): {e}"))?;
        let (br, _) = s
            .block(*c, *h)?
            .with_context(|| format!("block ({c}, {h}) not stored"))?;
        if let Some(diff) = compare_json(hdr, &render_header(&br)) {
            fail(format!("({c}, {h}) header: {diff}"));
        }
        if render_miner_bytes(&br, d).ok().as_deref() != Some(&db.miner_bytes[..]) {
            fail(format!("({c}, {h}) minerData bytes"));
        }
        for r in txs {
            checked += 1;
            let p = r.key.pos() as usize;
            let (ob, tb) = if p == 0 {
                (&db.coinbase_bytes, None)
            } else {
                let t = db
                    .txs
                    .get(p - 1)
                    .with_context(|| format!("({c}, {h}) pos {p} past payload"))?;
                (&t.output_bytes, Some(&t.tx_bytes))
            };
            if render_output_bytes(r, d).ok().as_ref() != Some(ob) {
                fail(format!("({c}, {h}) pos {p} output bytes"));
            }
            if let Some(tb) = tb {
                if render_tx_bytes(r, d).ok().flatten().as_ref() != Some(tb) {
                    fail(format!("({c}, {h}) pos {p} tx bytes"));
                }
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "txs_checked": checked, "blocks_checked": by_block.len(), "mismatches": mismatches,
            "requests": nc.stats().iter().map(|r| r.count).sum::<u64>(),
        }))?
    );
    if mismatches > 0 {
        bail!("{mismatches} mismatch(es)");
    }
    Ok(())
}

/// Equal as decimals: `1`, `1.0` and `1.00` are the same balance.
fn same_decimal(a: &str, b: &str) -> bool {
    fn norm(s: &str) -> String {
        let s = s.trim();
        match s.split_once('.') {
            Some((i, f)) => {
                let f = f.trim_end_matches('0');
                if f.is_empty() {
                    i.to_owned()
                } else {
                    format!("{i}.{f}")
                }
            }
            None => s.to_owned(),
        }
    }
    norm(a) == norm(b)
}

/// `inspect nft-check`: `count` random `NftBalances` rows (positive balances first); with
/// `compare_node`, `get-balance` is called for each through the `/local` client at
/// `[balances] local_rps`, and the decimal balance must equal the stored one. A difference
/// on a row that is also in `NftDirty`, or whose `as_of` is below the node answer's height
/// while the chain moved on, is counted as `skew`, not as a mismatch.
pub async fn nft_check(
    cfg: &Config,
    s: &Store,
    count: usize,
    compare_node: bool,
) -> anyhow::Result<()> {
    use kdx_hot::nft::{nft_key, parse_nft_key, NftRow};
    let rows = s.txn.scan(Table::NftBalances, &[], |_| true)?;
    let mut pos: Vec<_> = rows
        .iter()
        .filter_map(|(k, v)| Some((parse_nft_key(k)?, NftRow::decode(v)?)))
        .filter(|(_, r)| r.positive())
        .collect();
    let total = rows.len();
    let mut rng = rand::rng();
    let mut picks = Vec::new();
    while picks.len() < count && !pos.is_empty() {
        picks.push(pos.swap_remove(rng.random_range(0..pos.len())));
    }
    eprintln!("nft-check: {total} rows, {} sampled", picks.len());
    let local = if compare_node {
        Some(kdx_node::LocalClient::new(
            super::local_node_client(cfg)?,
            kdx_node::LocalOpts {
                rps: cfg.balances.local_rps.min(10),
                concurrency: 2,
            },
        ))
    } else {
        None
    };
    let (mut checked, mut mismatches, mut skew) = (0u64, 0u64, 0u64);
    let mut out = Vec::new();
    for (((account, _, chain), row), n) in picks.iter().map(|p| (&p.0, &p.1)).zip(1..) {
        let acct = s.dict.name(DictKind::Account, *account).map(str::to_owned);
        let ledger = s.dict.name(DictKind::Module, row.ledger).map(str::to_owned);
        let (Some(acct), Some(ledger)) = (acct, ledger) else {
            continue;
        };
        let dirty = s
            .txn
            .get(
                Table::NftDirty,
                &nft_key(*account, &kdx_hot::nft::token_hash(&row.token_id), *chain),
            )?
            .is_some();
        let mut item = json!({
            "n": n, "account": acct, "ledger": ledger, "token_id": row.token_id,
            "chain": chain, "balance": row.balance, "as_of": row.as_of,
            "source": row.source, "dirty": dirty,
        });
        if let Some(local) = &local {
            checked += 1;
            match local
                .get_balance(*chain, &ledger, &row.token_id, &acct)
                .await
            {
                Ok(kdx_node::Local::Found { value, height }) => {
                    let eq = same_decimal(&value, &row.balance);
                    item["node_balance"] = json!(value);
                    item["node_height"] = json!(height);
                    if !eq {
                        if dirty || u64::from(row.as_of) < height {
                            skew += 1;
                            item["result"] = json!("skew");
                        } else {
                            mismatches += 1;
                            item["result"] = json!("MISMATCH");
                        }
                    } else {
                        item["result"] = json!("ok");
                    }
                }
                Ok(kdx_node::Local::Absent) => {
                    mismatches += 1;
                    item["result"] = json!("MISMATCH (node: absent)");
                }
                Err(e) => {
                    mismatches += 1;
                    item["result"] = json!(format!("MISMATCH (call failed: {e})"));
                }
            }
        }
        out.push(item);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "rows": total, "sampled": picks.len(), "checked": checked,
            "mismatches": mismatches, "skew": skew, "items": out,
        }))?
    );
    if mismatches > 0 {
        anyhow::bail!("{mismatches} mismatch(es)");
    }
    Ok(())
}
