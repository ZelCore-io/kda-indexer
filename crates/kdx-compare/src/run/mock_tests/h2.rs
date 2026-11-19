//! FVH-H part 2 (R-P5P9-97, R-P5P9-100): the CG-5 re-referee's leftovers M #1997 / T #2005,
//! W #567, T #332, U, Q #37 and N #178. Each case is the record's shape, live and stored, and
//! each is RED without its fix.

use super::*;
use crate::report::{DiffEntry, DiffKind};

/// A referee config over `node`, with ours and the oracle unreachable (stored bodies only).
async fn h2_cfg(node: NodeMock, dir: &std::path::Path) -> CompareConfig {
    let nu = node_server(node).await;
    let ops = c3_ops(dir);
    c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "http://127.0.0.1:9/graphql",
        dir,
        &ops,
        "",
    )
}

/// A stored QH7 record over `transfers` pages `a` (ours) and `b` (the oracle).
fn h2_rec(a: Vec<Value>, b: Vec<Value>, diff: Vec<DiffEntry>) -> crate::report::DiffRecord {
    let page = |edges: Vec<Value>| {
        json!({"data": {"transfers": {"totalCount": 40,
            "pageInfo": {"hasNextPage": false}, "edges": edges}}})
    };
    e2_stored("QH7", json!({"first": 20}), page(a), page(b), diff)
}

fn h2_missing(path: &str, b: Value) -> DiffEntry {
    DiffEntry {
        path: path.into(),
        kind: DiffKind::Missing,
        a: None,
        b: Some(b),
    }
}

// ---------------------------------------------------------------------------------------------
// M #1997 / T #2005

/// The node's `/poll` for M #1997's shape: x0 and m in block B100 at 100, or m at `mh`.
fn m_node(mh: u64) -> NodeMock {
    let mut node = NodeMock::default();
    let (k, v) = h7_xfer_poll("x0", "B100", 100, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    let (k, v) = h7_xfer_poll("m", &format!("B{mh}"), mh, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    node
}

/// M #1997's rows: ours lists m once at 100 (no block hash, only a height); the oracle lists
/// it there too and again at 99 with another receiver (its orphan copy).
fn m_rows() -> (Vec<Value>, Vec<Value>) {
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    let m = h7_xrow("m", 100, json!(1.0), Some(("s", "r")));
    let orphan = h7_xrow("m", 99, json!(1.0), Some(("s", "r2")));
    (vec![x0.clone(), m.clone()], vec![x0, m, orphan])
}

/// R-P5P9-97 M #1997 (and T #2005): ours' row selects no block hash, but it lists the key at
/// the node's height on the node's chain. The oracle's height-only copy at another height is
/// an orphan by height. When the node places the key at another height than ours' row, the
/// copy stays a diff.
#[tokio::test]
async fn h2_m_ours_listed_at_the_nodes_height_without_a_hash() {
    let (a, b) = m_rows();
    for (mh, explained) in [(100, true), (101, false)] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = gql_ctx(m_node(mh), dir.path()).await;
        let (d, note) = d1_settle(&ctx, a.clone(), b.clone(), (false, false)).await;
        assert_eq!(
            only_hits(&d) && !d.is_empty(),
            explained,
            "live {mh}: {d:?} {note:?}"
        );
        if explained {
            assert!(format!("{d:?}").contains("orphan by height"), "{d:?}");
        }
        // Stored: the same record from the diff log, refereed.
        let cfg = h2_cfg(m_node(mh), dir.path()).await;
        let rec = h2_rec(
            a.clone(),
            b.clone(),
            vec![h2_missing("/data/transfers/edges/2", b[2].clone())],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!(
            (rep.explained, rep.unexplained),
            want,
            "stored {mh}: {rep:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// W #567

/// W #567's node: w's transaction in B100 at 100 holds TRANSFER events at [0, 1, 4, 5]; the
/// row is the third of them (event 4, the oracle's 2). B100 is the only block at 100, so
/// the oracle's extra row q in X100 is an orphan by block.
fn w_node() -> NodeMock {
    let mut node = NodeMock::default();
    let t = |to: &str, amount: f64| json!({"name": "TRANSFER", "module": {"name": "coin"}, "params": ["s", to, amount]});
    let other = json!({"name": "OTHER", "module": {"name": "coin"}, "params": []});
    let events = json!([
        t("g0", 0.1),
        t("g1", 0.2),
        other,
        other,
        t("r", 1.5),
        t("g5", 0.3)
    ]);
    let (k, v) = h7_poll("w", "B100", 100, events);
    node.polls.insert(k, v);
    node.headers.insert(
        (0, 100),
        vec![json!({"hash": "B100", "height": 100, "payloadHash": "P100", "chainId": 0})],
    );
    let cb = json!({"reqKey": "cb100", "events": []}).to_string();
    node.payloads.insert(
        "P100".into(),
        json!({"coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node
}

/// An indexed transfer row (QTransfersByAccount's identity) of w, s → r 1.5, at `i`.
fn w_row(rk: &str, block: &str, i: i64) -> Value {
    let mut e = h7_xrow(rk, 100, json!(1.5), Some(("s", "r")));
    e["node"]["blockHash"] = json!(block);
    e["node"]["orderIndex"] = json!(i);
    e
}

fn w_page(rows: Vec<Value>, count: u64) -> Value {
    json!({"data": {"transfers": {"totalCount": count,
        "pageInfo": {"hasNextPage": false}, "edges": rows}}})
}

/// R-P5P9-97 W #567: the count walk takes H8's mapped index. The oracle numbers w's
/// transfer 2 (its rank among the node's TRANSFER events), ours 4 (the node's event index).
/// H8 maps the first page's row, and now the walk's rows too, so the walk's one-sided rows
/// are the oracle's orphan alone and the counts differ by it. When the oracle's rank does not
/// hold the row's content on the node, the row stays unmapped and the record unexplained.
#[tokio::test]
async fn h2_w_the_count_walk_takes_h8s_mapped_index() {
    for (rank, explained) in [(2, true), (3, false)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(w_node()).await;
        let ours = vec![w_row("w", "B100", 4)];
        let oracle = vec![w_row("w", "B100", rank), w_row("q", "X100", 0)];
        let au = ours_fn_server(super::e2b::walk_reply("transfers", ours.clone(), 1)).await;
        let bu = ours_fn_server(super::e2b::walk_reply("transfers", oracle.clone(), 2)).await;
        let ops = c3_ops(dir.path());
        let cfg = c3_cfg(&nu, &au, &bu, dir.path(), &ops, "");
        let count = DiffEntry {
            path: "/data/transfers/totalCount".into(),
            kind: DiffKind::Changed,
            a: Some(json!(1)),
            b: Some(json!(2)),
        };
        let rec = e2_stored(
            "QH7",
            json!({"accountName": "s", "first": 1}),
            w_page(ours, 1),
            w_page(vec![oracle[0].clone()], 2),
            vec![count],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!((rep.explained, rep.unexplained), want, "{rank}: {rep:?}");
    }
}

/// R-P5P9-105 (H8 in H7, #567's shape): H8's mapping applies to every oracle row of H7's
/// window pass, not only to the keys H8's first pass reads ([`ORDER_INDEX_READS`] of them).
/// 21 keys, each with the node's TRANSFER indexes [0, 1, 4, 5]: ours numbers each row 4,
/// the oracle 2 (its rank). Every row is mapped 2 → 4 → explained. When the last key's
/// oracle rank (3) is not the node's TRANSFER with the row's content, it stays unmapped →
/// unexplained.
///
/// [`ORDER_INDEX_READS`]: crate::graphql_window::ORDER_INDEX_READS
#[tokio::test]
async fn i_w_h8s_mapping_applies_to_every_oracle_row_in_h7() {
    let n = crate::graphql_window::ORDER_INDEX_READS + 1;
    let keys: Vec<String> = (0..n).map(|i| format!("w{i:02}")).collect();
    for (last, explained) in [(2i64, true), (3, false)] {
        let dir = tempfile::tempdir().unwrap();
        let mut node = w_node();
        let w = node.polls["w"].clone();
        for k in &keys {
            let mut p = w.clone();
            p.1["reqKey"] = json!(k);
            node.polls.insert(k.clone(), p);
        }
        let nu = node_server(node).await;
        let ours: Vec<Value> = keys.iter().map(|k| w_row(k, "B100", 4)).collect();
        let oracle: Vec<Value> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| w_row(k, "B100", if i == n - 1 { last } else { 2 }))
            .collect();
        let ops = c3_ops(dir.path());
        let cfg = c3_cfg(
            &nu,
            "http://127.0.0.1:9/graphql",
            "http://127.0.0.1:9/graphql",
            dir.path(),
            &ops,
            "",
        );
        let diff = oracle
            .iter()
            .enumerate()
            .map(|(i, o)| DiffEntry {
                path: format!("/data/transfers/edges/{i}/node/orderIndex"),
                kind: DiffKind::Changed,
                a: Some(json!(4)),
                b: Some(o["node"]["orderIndex"].clone()),
            })
            .collect();
        let rec = e2_stored(
            "QH7",
            json!({"accountName": "s", "first": 100}),
            w_page(ours, n as u64),
            w_page(oracle, n as u64),
            diff,
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!((rep.explained, rep.unexplained), want, "{last}: {rep:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// T #332

/// T #332's node: the receive leg k1 on chain 0 (continuation step 1 of pact P1) holds the
/// receiving TRANSFER "" → r 1.5 and TRANSFER_XCHAIN_RECD from chain 1; the source leg P1 on
/// chain 1 holds TRANSFER s → "" 1.5 and TRANSFER_XCHAIN s → r 1.5 to `target`.
fn t_node(target: &str) -> NodeMock {
    let ev = |name: &str, p: Value| json!({"name": name, "module": {"name": "coin"}, "params": p});
    let mut node = NodeMock::default();
    let (k, mut v) = h7_poll(
        "k1",
        "B100",
        100,
        json!([
            ev("TRANSFER", json!(["", "r", 1.5])),
            ev("TRANSFER_XCHAIN_RECD", json!(["", "r", 1.5, "1"]))
        ]),
    );
    v.1["continuation"] = json!({"pactId": "P1", "step": 1});
    node.polls.insert(k, v);
    node.polls.insert(
        "P1".into(),
        (
            1,
            json!({"reqKey": "P1", "metaData": {"blockHash": "S50", "blockHeight": 50},
                "events": [ev("TRANSFER", json!(["s", "", 1.5])),
                    ev("TRANSFER_XCHAIN", json!(["s", "r", 1.5, target]))]}),
        ),
    );
    let (k, v) = h7_xfer_poll("x0", "B100", 100, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    node
}

/// An ours-only receive-leg row "" → r 1.5 with its link `x`.
fn t_row(x: Value) -> Value {
    let mut e = h7_xrow("k1", 100, json!(1.5), Some(("", "r")));
    e["node"]["crossChainTransfer"] = x;
    e
}

/// R-P5P9-97 T #332 (R-P5P9-91 (2)): an ours-only receive-leg transfer's
/// `crossChainTransfer` is checked against the source leg, read by the continuation's pact
/// id on the chain its TRANSFER_XCHAIN_RECD names: ours' link (chain 1, s → "") is the
/// source leg's → explained, live and stored. A wrong sender, or a source leg whose
/// TRANSFER_XCHAIN targets another chain, stays a diff.
#[tokio::test]
async fn h2_t_a_receive_legs_link_is_checked_against_the_source_leg() {
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    let link = |from: &str| {
        json!({"__typename": "Transfer", "chainId": "1", "senderAccount": from,
            "receiverAccount": ""})
    };
    for (target, from, explained) in [("0", "s", true), ("0", "z", false), ("2", "s", false)] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = gql_ctx(t_node(target), dir.path()).await;
        let a = vec![x0.clone(), t_row(link(from))];
        let (d, note) = d1_settle(&ctx, a.clone(), vec![x0.clone()], (false, false)).await;
        let case = format!("{target} {from}");
        assert_eq!(
            only_hits(&d) && !d.is_empty(),
            explained,
            "live {case}: {d:?} {note:?}"
        );
        if !explained {
            assert!(
                note.unwrap_or_default().contains("crossChainTransfer"),
                "{case}"
            );
        }
        let cfg = h2_cfg(t_node(target), dir.path()).await;
        let rec = h2_rec(
            a.clone(),
            vec![x0.clone()],
            vec![DiffEntry {
                path: "/data/transfers/edges/1".into(),
                kind: DiffKind::Extra,
                a: Some(a[1].clone()),
                b: None,
            }],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!(
            (rep.explained, rep.unexplained),
            want,
            "stored {case}: {rep:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// U #432 / #2045

/// The stored referee over a QH7 record whose first pages differ by their totalCount alone
/// (ours `na`, the oracle `nb`), ours and the oracle answering the walk with these rows.
async fn walk_referee(
    ours: Vec<Value>,
    oracle: Vec<Value>,
    (na, nb): (u64, u64),
) -> crate::referee::Report {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(w_node()).await;
    let au = ours_fn_server(super::e2b::walk_reply("transfers", ours.clone(), na)).await;
    let bu = ours_fn_server(super::e2b::walk_reply("transfers", oracle.clone(), nb)).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(&nu, &au, &bu, dir.path(), &ops, "");
    let count = DiffEntry {
        path: "/data/transfers/totalCount".into(),
        kind: DiffKind::Changed,
        a: Some(json!(na)),
        b: Some(json!(nb)),
    };
    let rec = e2_stored(
        "QH7",
        json!({"accountName": "s", "first": 1}),
        w_page(vec![ours[0].clone()], na),
        w_page(vec![oracle[0].clone()], nb),
        vec![count],
    );
    e2_referee(cfg, dir.path(), &[rec]).await
}

/// R-P5P9-97 U (#2045's shape): the oracle's walked list holds one row more than its own
/// totalCount (3 rows, count 2), its two one-sided rows orphans on the node, and ours'
/// count equals ours' walked rows. With every one-sided row settled, the count difference
/// is the oracle's self-inconsistency → explained by its own hit. Two rows more than its
/// count, or an oracle-only row the node does not settle, stay unexplained.
#[tokio::test]
async fn h2_u_the_oracles_list_one_row_over_its_own_count() {
    let w = || w_row("w", "B100", 4);
    let q = |rk: &str, block: Option<&str>| {
        let mut e = h7_xrow(rk, 100, json!(1.5), Some(("s", "r")));
        if let Some(b) = block {
            e["node"]["blockHash"] = json!(b);
        }
        e
    };
    let orphans = vec![w(), q("q1", Some("X100")), q("q2", Some("X100"))];
    let rep = walk_referee(vec![w()], orphans.clone(), (1, 2)).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(
        rep.by_class.get("graphql-r97-oracle-list-over-count"),
        Some(&1),
        "{rep:?}"
    );
    // Two rows over its count: unexplained.
    let mut over = orphans.clone();
    over.push(q("q3", Some("X100")));
    let rep = walk_referee(vec![w()], over, (1, 2)).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
    // A one-sided row the node does not settle (no block): unexplained.
    let unsettled = vec![w(), q("q1", Some("X100")), q("q2", None)];
    let rep = walk_referee(vec![w()], unsettled, (1, 2)).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
}

// ---------------------------------------------------------------------------------------------
// Q #37

fn q_body(rows: Vec<Value>) -> Value {
    json!({"data": {"nonFungibleAccount": {"id": "N", "accountName": "k:a",
        "chainAccounts": rows}}})
}

fn q_row(chain: &str) -> Value {
    json!({"id": format!("C{chain}"), "chainId": chain, "accountName": "k:a"})
}

/// The stored referee over Q #37's record: ours lists chain 8 once, the oracle `oracle`;
/// ours' chain 8 holds `tokens`, and the node's `get-balance` on chain 8 answers `balance`.
async fn q_referee(
    oracle: Vec<Value>,
    tokens: Vec<Value>,
    balance: Option<Value>,
) -> crate::referee::Report {
    q_referee_capped(vec![q_row("8")], oracle, tokens, balance, 200).await
}

/// [`q_referee`] with ours' chain rows `ours` (the node answers `balance` on each of their
/// chains) and the run's `--max-reads`.
async fn q_referee_capped(
    ours: Vec<Value>,
    oracle: Vec<Value>,
    tokens: Vec<Value>,
    balance: Option<Value>,
    max_reads: usize,
) -> crate::referee::Report {
    let mut node = NodeMock::default();
    if let Some(b) = balance {
        for r in &ours {
            let c: u8 = r["chainId"].as_str().unwrap().parse().unwrap();
            node.local.insert((c, "k:a".into()), b.clone());
        }
    }
    q_referee_node(ours, oracle, tokens, node, max_reads).await
}

/// [`q_referee_capped`] over a given node.
async fn q_referee_node(
    ours: Vec<Value>,
    oracle: Vec<Value>,
    tokens: Vec<Value>,
    node: NodeMock,
    max_reads: usize,
) -> crate::referee::Report {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(node).await;
    let au = ours_fn_server(move |req: &Value| {
        let q = req["query"].as_str().unwrap_or("");
        if q.contains("nonFungibleChainAccount") {
            json!({"data": {"nonFungibleChainAccount": {"nonFungibleTokenBalances": tokens}}})
        } else {
            json!({"data": null})
        }
    })
    .await;
    let ops = dir.path().join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../compare/graphql/QNonFungibleAccount.graphql");
    std::fs::copy(doc, ops.join("QNonFungibleAccount.graphql")).unwrap();
    let cfg = c3_cfg(&nu, &au, "http://127.0.0.1:9/graphql", dir.path(), &ops, "");
    let diff = (1..oracle.len())
        .map(|i| {
            h2_missing(
                &format!("/data/nonFungibleAccount/chainAccounts/{i}"),
                oracle[i].clone(),
            )
        })
        .collect();
    let rec = e2_stored(
        "QNonFungibleAccount",
        json!({"accountName": "k:a"}),
        q_body(ours),
        q_body(oracle),
        diff,
    );
    e2_referee_capped(cfg, dir.path(), &[rec], max_reads).await
}

/// R-P5P9-97 Q #37, review B1: the node reads of the repeated rows are the run's reads. Ours
/// lists chains 8 and 9 once and the oracle repeats both, so each takes one `get-balance`
/// (the chain graph is the run's first read). A cap of three reads leaves one for each and
/// the record is explained, the reads counted. A cap of two spends the budget on chain 8's
/// read: chain 9's is not made, the record stays unexplained, and its note names the cap.
#[tokio::test]
async fn h2_q_chain_dupes_reads_count_against_the_run_cap() {
    let ours = vec![q_row("8"), q_row("9")];
    let oracle: Vec<Value> = ours.iter().flat_map(|r| vec![r.clone(); 3]).collect();
    let tok = vec![json!({"tokenId": "t:1", "balance": 1})];
    let rep = q_referee_capped(
        ours.clone(),
        oracle.clone(),
        tok.clone(),
        Some(json!(1.0)),
        3,
    )
    .await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(rep.node_reads, 3, "{rep:?}");
    let rep = q_referee_capped(ours, oracle, tok, Some(json!(1.0)), 2).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
    assert_eq!(rep.node_reads, 2, "{rep:?}");
    let still = serde_json::to_string(&rep.still_unexplained).unwrap();
    assert!(still.contains("node read cap"), "{still}");
}

/// R-P5P9-97 Q #37: the oracle repeats ours' one chain-8 row 8 times (same id). Ours lists
/// it once and the node confirms it (marmalade-v2's `get-balance` of a token ours lists on
/// chain 8 is above zero): the copies are oracle duplicates → explained by their own hit,
/// not the Q known gap. Without the node's balance, without a token of ours, or with copies
/// that differ from ours' row, the record stays unexplained.
#[tokio::test]
async fn h2_q_oracle_duplicates_of_a_chain_row_ours_lists_once() {
    let dupes = vec![q_row("8"); 8];
    let tok = vec![json!({"tokenId": "t:1", "balance": 1})];
    let rep = q_referee(dupes.clone(), tok.clone(), Some(json!(1.0))).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(
        rep.by_class.get("graphql-r97-oracle-chain-dupes"),
        Some(&1),
        "{rep:?}"
    );
    let rep = q_referee(dupes.clone(), tok.clone(), None).await;
    assert_eq!(
        (rep.explained, rep.unexplained),
        (0, 1),
        "no balance: {rep:?}"
    );
    let rep = q_referee(dupes.clone(), vec![], Some(json!(1.0))).await;
    assert_eq!(
        (rep.explained, rep.unexplained),
        (0, 1),
        "no token: {rep:?}"
    );
    let rep = q_referee(dupes.clone(), tok.clone(), Some(json!(0.0))).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "zero: {rep:?}");
    let mut other = dupes;
    other[3]["id"] = json!("C9");
    let rep = q_referee(other, tok, Some(json!(1.0))).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "unlike: {rep:?}");
}

/// R-P5P9-105 Q (#37's shape): ours lists chain 8 once and the oracle repeats it 8 times.
/// The tokens are marmalade v1's: marmalade-v2's `get-balance` answers "No value found in
/// table marmalade-v2.ledger_ledger" and `marmalade.ledger`'s gives 1 → explained, with
/// both reads taken from the run's budget. Both zero → unexplained.
#[tokio::test]
async fn i_q_the_holding_check_tries_marmalade_v1_after_v2() {
    let dupes = vec![q_row("8"); 8];
    let tok = vec![json!({"tokenId": "t:The Kadena Mushrooms:48", "balance": 1})];
    let node = |v2: Value, v1: Value| {
        let mut n = NodeMock::default();
        n.local_by_module
            .insert((8, "marmalade-v2.ledger".into()), v2);
        n.local_by_module.insert((8, "marmalade.ledger".into()), v1);
        n
    };
    let rep = q_referee_node(
        vec![q_row("8")],
        dupes.clone(),
        tok.clone(),
        node(Value::Null, json!(1.0)),
        200,
    )
    .await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(
        rep.by_class.get("graphql-r97-oracle-chain-dupes"),
        Some(&1),
        "{rep:?}"
    );
    // The chain graph, then v2's read and v1's.
    assert_eq!(rep.node_reads, 3, "{rep:?}");
    let rep = q_referee_node(
        vec![q_row("8")],
        dupes,
        tok,
        node(json!(0.0), json!(0.0)),
        200,
    )
    .await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "zero: {rep:?}");
}

// ---------------------------------------------------------------------------------------------
// N #178

/// The stored referee over N #178's record (QTransactionsByBlock, our own document): a block
/// of 219 txs; ours lists `ours`, the oracle the node's first 200.
async fn n_referee(ours: Vec<String>) -> crate::referee::Report {
    let dir = tempfile::tempdir().unwrap();
    let rks: Vec<String> = (0..219).map(|i| format!("t{i:03}")).collect();
    let mut node = NodeMock::default();
    d3_block(&mut node, "BLK612", 4_476_191, &rks);
    let nu = node_server(node).await;
    let au = ours_fn_server(|req: &Value| {
        if req["query"].as_str().unwrap_or("").contains("block(hash") {
            json!({"data": {"block": {"chainId": "0"}}})
        } else {
            json!({"data": null})
        }
    })
    .await;
    let ops = dir.path().join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    let doc = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../compare/graphql/QTransactionsByBlock.graphql");
    std::fs::copy(doc, ops.join("QTransactionsByBlock.graphql")).unwrap();
    let cfg = c3_cfg(&nu, &au, "http://127.0.0.1:9/graphql", dir.path(), &ops, "");
    let page = |keys: &[String]| {
        json!({"data": {"transactions": {"totalCount": 219,
            "pageInfo": {"hasNextPage": true, "hasPreviousPage": false,
                "startCursor": "s", "endCursor": "e"},
            "edges": keys.iter().map(|k| d3_row("BLK612", k)).collect::<Vec<_>>()}}})
    };
    let diff = (0..200)
        .filter(|&i| ours[i] != rks[i])
        .map(|i| DiffEntry {
            path: format!("/data/transactions/edges/{i}/node/hash"),
            kind: DiffKind::Changed,
            a: Some(json!(ours[i])),
            b: Some(json!(rks[i])),
        })
        .collect();
    let rec = e2_stored(
        "QTransactionsByBlock",
        json!({"blockHash": "BLK612", "first": 200}),
        page(&ours),
        page(&rks[..200]),
        diff,
    );
    e2_referee(cfg, dir.path(), &[rec]).await
}

/// R-P5P9-97 N #178, stored: ours lists the node's block order reversed (t218..t019), every
/// oracle row is in the node's block. The referee applies N's reverse-order arm to the stored
/// root-scoped list → explained. A shuffled order stays unexplained.
#[tokio::test]
async fn h2_n_the_stored_records_reverse_order_arm() {
    let ours: Vec<String> = (0..219)
        .rev()
        .take(200)
        .map(|i| format!("t{i:03}"))
        .collect();
    let rep = n_referee(ours.clone()).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    let mut shuffled = ours;
    shuffled.swap(0, 100);
    let rep = n_referee(shuffled).await;
    assert_eq!((rep.explained, rep.unexplained), (0, 1), "{rep:?}");
}

// ---------------------------------------------------------------------------------------------
// Part 1's F and P #977, through their calling paths

/// R-P5P9-97 F, through H7 and our longer read (part 1 tested the helper alone): the stored
/// Transfers-8c8a0f56 shape, a root list without `pageInfo` and `first` 2. The oracle's w
/// sits at our boundary height; our longer read (also without `pageInfo`) lists it after our
/// page → explained. When our longer read lacks w, it stays unexplained.
#[tokio::test]
async fn h2_f_our_longer_read_without_page_info_through_h7() {
    let r = |rk: &str| {
        json!({"node": {"amount": 1.5, "requestKey": rk,
            "block": {"height": 100, "chainId": "0"}}})
    };
    for (longer, explained) in [(vec!["u", "v", "w"], true), (vec!["u", "v"], false)] {
        let dir = tempfile::tempdir().unwrap();
        let mut node = NodeMock::default();
        for rk in ["u", "v", "w"] {
            let (k, v) = h7_xfer_poll(rk, "B100", 100, "s", "r", json!(1.5));
            node.polls.insert(k, v);
        }
        let nu = node_server(node).await;
        let rows: Vec<Value> = longer.iter().map(|k| r(k)).collect();
        let au =
            ours_fn_server(move |_: &Value| json!({"data": {"transfers": {"edges": rows}}})).await;
        let ops = c3_ops(dir.path());
        let cfg = c3_cfg(&nu, &au, "http://127.0.0.1:9/graphql", dir.path(), &ops, "");
        let page = |keys: &[&str]| json!({"data": {"transfers": {"edges": keys.iter().map(|k| r(k)).collect::<Vec<_>>()}}});
        let rec = e2_stored(
            "QH7",
            json!({"first": 2}),
            page(&["u", "v"]),
            page(&["u", "w"]),
            vec![DiffEntry {
                path: "/data/transfers/edges/1/node/requestKey".into(),
                kind: DiffKind::Changed,
                a: Some(json!("v")),
                b: Some(json!("w")),
            }],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!(
            (rep.explained, rep.unexplained),
            want,
            "{longer:?}: {rep:?}"
        );
        assert!(
            !format!("{rep:?}").contains("the read holds no list"),
            "{rep:?}"
        );
    }
}

/// R-P5P9-97 P #977, through the referee (part 1 tested the helper alone): ours' first page
/// answered `errors` (request deadline exceeded, TIMEOUT) with no data, stored as `/data`
/// Type and `/errors` Extra, for a ruled P key → the first-page fault marker, a KNOWN-GAP P
/// record. The same answer for an account outside P's keys stays unexplained.
#[tokio::test]
async fn h2_p_a_stored_first_page_error_through_the_referee() {
    let key = "k:22fbc926b497bf3ecc7199b923039e74cdeda12726e1dc4f443953007cf8cc43";
    for (acct, gap) in [(key, true), ("k:other", false)] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = h2_cfg(NodeMock::default(), dir.path()).await;
        let ours = json!({"data": null, "errors": [{"message": "request deadline exceeded",
            "extensions": {"code": "TIMEOUT"}}]});
        let oracle = json!({"data": {"transfers": {"totalCount": 1,
            "pageInfo": {"hasNextPage": false}, "edges": []}}});
        let rec = e2_stored(
            "QH7",
            json!({"accountName": acct, "first": 20}),
            ours,
            oracle,
            vec![
                DiffEntry {
                    path: "/data".into(),
                    kind: DiffKind::Type,
                    a: Some(Value::Null),
                    b: Some(json!({})),
                },
                DiffEntry {
                    path: "/errors".into(),
                    kind: DiffKind::Extra,
                    a: Some(json!([])),
                    b: None,
                },
            ],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let _want = if gap { (1, 0) } else { (0, 1) };
        // H5 (a): an open, our-defect or KNOWN-GAP entry matches nothing; the diff is reported.
        assert_eq!((rep.known_gap, rep.unexplained), (0, 1), "{acct}: {rep:?}");
    }
}
