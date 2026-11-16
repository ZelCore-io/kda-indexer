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
