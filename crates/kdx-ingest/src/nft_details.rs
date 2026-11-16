//! NFT holding guards and token info for GraphQL (GQ-15): the `NftDetails` fill.
//!
//! GraphQL's `NonFungibleTokenBalance.guard` (non-null) and `.info` are not in the ledger
//! events. The reconciler reads them with two fixed `/local` forms (B-1: the token id and the
//! account are `read-msg` data, never code), through the same [`LocalClient`] as balances and
//! NFT rows, so they share its `local_rps` budget and concurrency:
//! - `({ledger}.details (read-msg "t") (read-msg "a"))`: the holding's guard;
//! - `({ledger}.get-token-info (read-msg "t"))`: precision, supply and uri, once per
//!   (ledger, token, chain) and step, shared by every holder.
//!
//! [`step`] walks `NftBalances` in key order from a cursor that wraps, at most [`SCAN_ROWS`]
//! rows and [`MAX_CALLS`] calls per step, so every holding gets its turn and no account or
//! token can hold the queue. Only positive rows of a trusted ledger (`[nft] ledgers`) are read.
//! A row is due when it has no details or its balance row moved past the details' `as_of`:
//! a ledger event, or an `NftDirty` mark reconciled with `get-balance` after a reorg, raises
//! the balance row's `as_of` and so invalidates the details. Holder rows of a zero balance are
//! deleted, so the table never outgrows `NftBalances`.
//!
//! Outcomes: an answer is stored (`as_of` = the node's tip); a missing row is stored absent; a
//! Pact failure, an unusable answer or a B-1 refusal is a per-key failure, stored as failed
//! until the balance row moves (no back-off, no retry loop); a ledger without
//! `get-token-info` is stored undefined (`info` is `null`). A node error stores nothing: the
//! key stays due, and when node errors are at least half of a step's calls the cursor does not
//! move, so an outage parks nothing and skips nothing.

use std::collections::HashSet;

use anyhow::Result;
use futures::future::join_all;
use kdx_hot::nft::{self, HolderDetails, NftRow, TokenDetails};
use kdx_hot::{checkpoint, HotStore, RwTxn, Stage, Table, Txn};
use kdx_node::local::{self, Local, LocalClient, LocalError, TokenInfo};
use kdx_types::DictKind;

/// `NftBalances` rows examined per step (one read txn).
pub const SCAN_ROWS: usize = 4096;
/// `/local` calls per step (guards and token info together).
pub const MAX_CALLS: usize = 32;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DetailsStats {
    pub scanned: u64,
    pub ok: u64,
    pub absent: u64,
    /// Pact failures, unusable answers and B-1 refusals (stored as failed).
    pub failures: u64,
    /// Ledgers without `get-token-info`.
    pub undefined: u64,
    pub node_errors: u64,
    /// Holder rows deleted because the balance is zero.
    pub deleted: u64,
}

impl DetailsStats {
    pub fn calls(&self) -> u64 {
        self.ok + self.absent + self.failures + self.undefined + self.node_errors
    }
    /// Node errors are at least half of the calls: the node is failing, not the keys.
    pub fn node_failing(&self) -> bool {
        self.node_errors > 0 && self.node_errors * 2 >= self.calls()
    }
}

/// One planned call.
#[derive(Debug, Clone)]
enum Job {
    Guard { key: [u8; 25], account: String },
    Info { key: [u8; 22] },
}

#[derive(Debug, Clone)]
struct Planned {
    job: Job,
    chain: u8,
    ledger: String,
    token_id: String,
    /// `as_of` for an answer without a height: the chain's tail, at least the balance row's.
    floor: u32,
}

#[derive(Debug)]
enum Outcome {
    Guard(String, u32),
    Info(TokenInfo, u32),
    Absent,
    Failed,
    Undefined,
    NodeError,
}

fn dict_name(txn: &impl Txn, k: DictKind, id: u32) -> Result<Option<String>> {
    Ok(txn
        .get(Table::DictRev(k), &id.to_be_bytes())?
        .and_then(|v| String::from_utf8(v).ok()))
}

/// Whether the stored details still cover a balance row at `row_as_of`.
fn covers(as_of: Option<u32>, row_as_of: u32) -> bool {
    as_of.is_some_and(|h| h >= row_as_of)
}

/// The calls due among the next rows after `cursor`, and the key to resume after (`None` at
/// the end of the table: the walk wraps). Zero-balance holder rows to delete are returned too.
#[allow(clippy::type_complexity)]
fn plan(
    txn: &impl Txn,
    cursor: Option<&[u8]>,
) -> Result<(Vec<Planned>, Vec<[u8; 26]>, Option<Vec<u8>>, u64)> {
    let rows = txn.scan_chunk(Table::NftBalances, cursor, SCAN_ROWS, |_| true)?;
    let full = rows.len() == SCAN_ROWS;
    let mut jobs = Vec::new();
    let mut gone = Vec::new();
    let mut tokens: HashSet<[u8; 22]> = HashSet::new();
    let mut next: Option<Vec<u8>> = None;
    let mut scanned = 0u64;
    for (k, v) in &rows {
        let (Ok(key), Some((account, ledger, _, chain)), Some(row)) = (
            <[u8; 25]>::try_from(k.as_slice()),
            nft::parse_nft_key(k),
            NftRow::decode(v),
        ) else {
            next = Some(k.clone());
            scanned += 1;
            continue;
        };
        let hk = nft::holder_details_key(&key);
        let held = txn
            .get(Table::NftDetails, &hk)?
            .and_then(|v| HolderDetails::decode(&v));
        let mut due = Vec::new();
        if !row.positive() {
            if held.is_some() {
                gone.push(hk);
            }
        } else if let Some(ledger_name) = dict_name(txn, DictKind::Module, ledger)?
            .filter(|l| ledger == row.ledger && nft::is_ledger(l))
        {
            let floor = checkpoint(txn, Stage::Tail, chain)?
                .map_or(0, |(h, _)| h)
                .max(row.as_of);
            let base = |job| Planned {
                job,
                chain,
                ledger: ledger_name.clone(),
                token_id: row.token_id.clone(),
                floor,
            };
            if !covers(held.map(|h| h.as_of), row.as_of) {
                if let Some(account) = dict_name(txn, DictKind::Account, account)? {
                    due.push(base(Job::Guard { key, account }));
                }
            }
            let tk = nft::token_details_key_of(&key);
            if !tokens.contains(&tk) {
                let info = txn
                    .get(Table::NftDetails, &tk)?
                    .and_then(|v| TokenDetails::decode(&v));
                if !covers(info.map(|t| t.as_of), row.as_of) {
                    due.push(base(Job::Info { key: tk }));
                }
            }
        }
        if jobs.len() + due.len() > MAX_CALLS {
            // This row waits for the next step; resume just before it.
            return Ok((jobs, gone, next, scanned));
        }
        for j in &due {
            if let Job::Info { key } = j.job {
                tokens.insert(key);
            }
        }
        jobs.extend(due);
        next = Some(k.clone());
        scanned += 1;
    }
    Ok((jobs, gone, if full { next } else { None }, scanned))
}

async fn call(local: &LocalClient, p: &Planned) -> Outcome {
    let r = match &p.job {
        Job::Guard { account, .. } => local
            .nft_guard(p.chain, &p.ledger, &p.token_id, account)
            .await
            .map(|r| match r {
                Local::Found { value, height } => {
                    Outcome::Guard(value, u32::try_from(height).unwrap_or(u32::MAX))
                }
                Local::Absent => Outcome::Absent,
            }),
        Job::Info { .. } => local
            .nft_token_info(p.chain, &p.ledger, &p.token_id)
            .await
            .map(|r| match r {
                Local::Found { value, height } => {
                    Outcome::Info(value, u32::try_from(height).unwrap_or(u32::MAX))
                }
                Local::Absent => Outcome::Absent,
            }),
    };
    match r {
        Ok(o) => o,
        Err(LocalError::Failure(m))
            if matches!(p.job, Job::Info { .. })
                && local::is_no_member_message(&m, &p.ledger, "get-token-info") =>
        {
            Outcome::Undefined
        }
        Err(LocalError::Failure(m)) => {
            tracing::debug!(ledger = %p.ledger, message = %m, "nft details: call failed");
            Outcome::Failed
        }
        Err(LocalError::Skipped(m)) => {
            tracing::debug!(ledger = %p.ledger, message = %m, "nft details: refused (B-1)");
            Outcome::Failed
        }
        Err(LocalError::Node(e)) => {
            tracing::debug!(error = %e, "nft details: node error, kept due");
            Outcome::NodeError
        }
    }
}

fn store(txn: &mut RwTxn<'_>, p: &Planned, out: Outcome, st: &mut DetailsStats) -> Result<()> {
    let holder = |status, as_of, guard| HolderDetails {
        status,
        as_of,
        guard,
    };
    let (status, as_of) = match out {
        Outcome::NodeError => {
            st.node_errors += 1;
            return Ok(());
        }
        Outcome::Guard(g, h) => {
            st.ok += 1;
            if let Job::Guard { key, .. } = &p.job {
                nft::put_holder_details(txn, key, &holder(nft::DETAIL_FOUND, h, g))?;
            }
            return Ok(());
        }
        Outcome::Info(i, h) => {
            st.ok += 1;
            if let Job::Info { key } = &p.job {
                let t = TokenDetails {
                    status: nft::DETAIL_FOUND,
                    as_of: h,
                    precision: i.precision,
                    supply: i.supply,
                    uri: i.uri,
                };
                nft::put_token_details(txn, key, &t)?;
            }
            return Ok(());
        }
        Outcome::Absent => {
            st.absent += 1;
            (nft::DETAIL_ABSENT, p.floor)
        }
        Outcome::Failed => {
            st.failures += 1;
            (nft::DETAIL_FAILED, p.floor)
        }
        Outcome::Undefined => {
            st.undefined += 1;
            (nft::DETAIL_UNDEFINED, p.floor)
        }
    };
    match &p.job {
        Job::Guard { key, .. } => {
            nft::put_holder_details(txn, key, &holder(status, as_of, String::new()))
        }
        Job::Info { key } => nft::put_token_details(txn, key, &TokenDetails::empty(status, as_of)),
    }
}

/// One fill step: plan from `cursor`, call, store. The cursor moves past the planned rows
/// unless node errors are at least half of the calls (then the same rows are planned again).
pub async fn step(
    hot: &HotStore,
    local: &LocalClient,
    cursor: &mut Option<Vec<u8>>,
) -> Result<DetailsStats> {
    let (jobs, gone, next, scanned) = {
        let txn = hot.ro()?;
        plan(&txn, cursor.as_deref())?
    };
    let mut st = DetailsStats {
        scanned,
        ..DetailsStats::default()
    };
    let outcomes = join_all(jobs.iter().map(|p| call(local, p))).await;
    let mut txn = hot.rw()?;
    for (p, out) in jobs.iter().zip(outcomes) {
        store(&mut txn, p, out, &mut st)?;
    }
    for hk in &gone {
        // Only if the balance is still zero (an event may have landed since the plan).
        let zero = txn
            .get(Table::NftBalances, &hk[1..])?
            .and_then(|v| NftRow::decode(&v))
            .is_none_or(|r| !r.positive());
        if zero && txn.del(Table::NftDetails, hk)? {
            st.deleted += 1;
        }
    }
    txn.commit()?;
    if !st.node_failing() {
        *cursor = next;
    }
    Ok(st)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use kdx_hot::{Geometry, HotDict};
    use kdx_node::local::LocalOpts;
    use kdx_node::{NodeClient, NodeClientConfig};
    use kdx_types::DictMut;
    use serde_json::{json, Value};

    use super::*;

    const V2: &str = "marmalade-v2.ledger";
    const V1: &str = "marmalade.ledger";
    const HOSTILE: &str = "t:x\") (coin.transfer";

    /// A fake node that answers the two GQ-15 forms as mainnet does (measured): `t:bad` gets
    /// a guard-less answer, `HOSTILE` and `t:gone` no row, `marmalade.ledger` has no
    /// `get-token-info`. Every command is recorded.
    fn fake_node(seen: Arc<Mutex<Vec<Value>>>) -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let Ok(mut s) = s else { return };
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let (head_end, len) = loop {
                    let n = s.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break (0, 0);
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (i + 4, len);
                    }
                };
                while head_end > 0 && buf.len() < head_end + len {
                    let n = s.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let req: Value = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
                let cmd: Value = req
                    .get("cmd")
                    .and_then(Value::as_str)
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or(Value::Null);
                seen.lock().unwrap().push(cmd.clone());
                let code = cmd
                    .pointer("/payload/exec/code")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let t = cmd
                    .pointer("/payload/exec/data/t")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let a = cmd.pointer("/payload/exec/data/a").cloned();
                let fail =
                    |m: &str| json!({"result": {"status": "failure", "error": {"message": m}}});
                let ok = |d: Value| json!({"result": {"status": "success", "data": d}, "metaData": {"blockHeight": 21}});
                let body = if t == HOSTILE || t == "t:gone" {
                    fail("No value found in table marmalade-v2.ledger_ledger for key: t:x\") (coi...")
                } else if code.contains(".get-token-info ") {
                    if code.starts_with(&format!("({V1}.")) {
                        fail(&format!("Module {V1} has no such member: get-token-info"))
                    } else {
                        ok(json!({"id": t, "supply": 1.0, "precision": 0, "uri": "ipfs://x", "policies": []}))
                    }
                } else if t == "t:bad" {
                    ok(json!({"id": t, "account": a, "balance": 1.0}))
                } else {
                    ok(json!({"id": t, "account": a, "balance": 1.0,
                              "guard": {"keys": ["k1"], "pred": "keys-all"}}))
                }
                .to_string();
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        format!("http://{addr}")
    }

    fn client(base: &str) -> LocalClient {
        let node = NodeClient::new(&NodeClientConfig {
            base: base.parse().unwrap(),
            network: "mainnet01".into(),
            timeout: Duration::from_secs(5),
            max_rps: 0,
            concurrency: 4,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 20,
        })
        .unwrap();
        LocalClient::new(
            node,
            LocalOpts {
                rps: 0,
                concurrency: 4,
            },
        )
    }

    /// Holdings `(account, ledger, token, balance)` on chain 0 at height 10.
    fn store(dir: &std::path::Path, rows: &[(&str, &str, &str, &str)]) -> HotStore {
        let hot = HotStore::open(dir, Geometry::default()).unwrap();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let ids: Vec<(u32, u32)> = rows
            .iter()
            .map(|(a, l, _, _)| {
                (
                    dict.intern(DictKind::Account, a),
                    dict.intern(DictKind::Module, l),
                )
            })
            .collect();
        dict.persist(&mut txn).unwrap();
        for ((a, l), (_, _, t, b)) in ids.iter().zip(rows) {
            nft::upsert_event(&mut txn, *a, t, 0, *l, 10, b).unwrap();
        }
        txn.commit().unwrap();
        hot
    }

    fn details_of(hot: &HotStore, account: u32, ledger: u32, token: &str) -> nft::NftDetailsPair {
        let key = nft::nft_key(account, ledger, &nft::token_hash(token), 0);
        nft::details(&hot.ro().unwrap(), &key).unwrap()
    }

    /// A valid answer fills the guard and the shared token info; an unusable answer is a
    /// per-key failure (no back-off, the rest of the step goes on); a ledger without
    /// `get-token-info` is undefined; a hostile token id is sent as data and answered absent;
    /// a zero balance makes no call. A second step makes no call until a balance row moves.
    #[tokio::test]
    async fn a_step_fills_details_and_an_invalid_answer_fails_only_its_key() {
        let d = tempfile::tempdir().unwrap();
        let hot = store(
            d.path(),
            &[
                ("k:alice", V2, "t:1", "1.0"),
                ("k:bob", V2, "t:1", "2.0"),
                ("k:carol", V2, "t:bad", "1.0"),
                ("k:dave", V1, "t:old", "1.0"),
                ("k:erin", V2, "t:z", "0.0"),
                ("k:frank", V2, HOSTILE, "1.0"),
            ],
        );
        // Interned in row order: accounts 0..=5; modules V2 = 0, V1 = 1.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let local = client(&fake_node(seen.clone()));
        let mut cursor = None;
        let st = step(&hot, &local, &mut cursor).await.unwrap();
        // Guards: alice, bob, carol, dave, frank (5). Info: t:1 once, t:bad, t:old, HOSTILE (4).
        assert_eq!(
            (st.ok, st.absent, st.failures, st.undefined, st.node_errors),
            (5, 2, 1, 1, 0),
            "{st:?}"
        );
        assert!(!st.node_failing());
        assert_eq!(cursor, None, "a short table wraps");
        let calls = seen.lock().unwrap().clone();
        assert_eq!(calls.len(), 9);
        for c in &calls {
            let code = c["payload"]["exec"]["code"].as_str().unwrap();
            assert!(
                code == format!("({V2}.details (read-msg \"t\") (read-msg \"a\"))")
                    || code == format!("({V1}.details (read-msg \"t\") (read-msg \"a\"))")
                    || code == format!("({V2}.get-token-info (read-msg \"t\"))")
                    || code == format!("({V1}.get-token-info (read-msg \"t\"))"),
                "{code}"
            );
        }
        assert!(calls
            .iter()
            .any(|c| c["payload"]["exec"]["data"]["t"] == HOSTILE));

        let (g, i) = details_of(&hot, 0, 0, "t:1");
        let g = g.unwrap();
        assert_eq!((g.status, g.as_of), (nft::DETAIL_FOUND, 20));
        assert_eq!(g.guard, r#"{"keys":["k1"],"pred":"keys-all"}"#);
        let i = i.unwrap();
        assert_eq!(
            (i.status, i.precision, i.supply.as_str(), i.uri.as_str()),
            (nft::DETAIL_FOUND, 0, "1.0", "ipfs://x")
        );
        assert_eq!(
            details_of(&hot, 1, 0, "t:1").0.unwrap().status,
            nft::DETAIL_FOUND
        );
        let (g, i) = details_of(&hot, 2, 0, "t:bad");
        assert_eq!(
            (g.unwrap().status, i.unwrap().status),
            (nft::DETAIL_FAILED, nft::DETAIL_FOUND)
        );
        let (g, i) = details_of(&hot, 3, 1, "t:old");
        assert_eq!(
            (g.unwrap().status, i.unwrap().status),
            (nft::DETAIL_FOUND, nft::DETAIL_UNDEFINED)
        );
        assert_eq!(details_of(&hot, 4, 0, "t:z"), (None, None));
        let (g, i) = details_of(&hot, 5, 0, HOSTILE);
        assert_eq!(
            (g.unwrap().status, i.unwrap().status),
            (nft::DETAIL_ABSENT, nft::DETAIL_ABSENT)
        );

        // Everything is covered: no call.
        let st = step(&hot, &local, &mut cursor).await.unwrap();
        assert_eq!(st.calls(), 0, "{st:?}");
        // A ledger event at 30 moves alice's row: her guard and the token info are due again.
        let mut txn = hot.rw().unwrap();
        nft::upsert_event(&mut txn, 0, "t:1", 0, 0, 30, "3.0").unwrap();
        txn.commit().unwrap();
        let n = seen.lock().unwrap().len();
        let st = step(&hot, &local, &mut cursor).await.unwrap();
        assert_eq!((st.ok, st.calls()), (2, 2), "{st:?}");
        assert_eq!(seen.lock().unwrap().len(), n + 2);
        // A balance that falls to zero drops its guard row.
        let mut txn = hot.rw().unwrap();
        nft::upsert_event(&mut txn, 1, "t:1", 0, 0, 40, "0.0").unwrap();
        txn.commit().unwrap();
        let st = step(&hot, &local, &mut cursor).await.unwrap();
        assert_eq!(st.deleted, 1, "{st:?}");
        assert!(details_of(&hot, 1, 0, "t:1").0.is_none());
    }

    /// An outage (connection refused) stores nothing, parks nothing and keeps the cursor; the
    /// next step against a live node fills every row.
    #[tokio::test]
    async fn a_node_outage_parks_nothing() {
        let d = tempfile::tempdir().unwrap();
        let hot = store(
            d.path(),
            &[("k:alice", V2, "t:1", "1.0"), ("k:bob", V2, "t:2", "1.0")],
        );
        let dead = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        let mut cursor = Some(vec![0u8]);
        let st = step(&hot, &client(&dead), &mut cursor).await.unwrap();
        assert_eq!((st.node_errors, st.calls()), (4, 4), "{st:?}");
        assert!(st.node_failing());
        assert_eq!(cursor, Some(vec![0u8]), "the cursor stays");
        assert_eq!(hot.ro().unwrap().count(Table::NftDetails).unwrap(), 0);
        let local = client(&fake_node(Arc::new(Mutex::new(Vec::new()))));
        let st = step(&hot, &local, &mut cursor).await.unwrap();
        assert_eq!((st.ok, st.node_errors), (4, 0), "{st:?}");
        assert_eq!(hot.ro().unwrap().count(Table::NftDetails).unwrap(), 4);
    }
}
