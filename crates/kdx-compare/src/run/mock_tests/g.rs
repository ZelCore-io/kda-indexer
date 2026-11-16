//! FVH-G (R-P5P9-96): ours' fullness read from ours' own page in every rule, ours' tip and
//! lowest height on REST records, and the tests FVH-F's fixes lacked.

use super::e2b::{acct_pair, acct_row, rest_cfg, rest_node, rest_record, walk_reply, ACCT_REQ};
use super::*;
use crate::graphql::H6;
use crate::report::{DiffEntry, DiffKind};

/// A server that answers every request with `body` and a `Chainweb-Next` (a page cut at the
/// harness's follow depth).
async fn next_server(body: Value) -> String {
    use axum::response::IntoResponse;
    let app = Router::new().fallback(move || {
        let body = body.clone();
        async move { ([("Chainweb-Next", "next")], Json(body)).into_response() }
    });
    serve(app).await.trim_end_matches('/').to_string()
}

/// R-P5P9-96 (1), the reviewer's I-2 case with no coinbase row: ours is complete (80 rows
/// of limit 100, no `Chainweb-Next`) and lacks 19 older rows that chainweb-data lists before
/// its page is cut at its depth. R3E-14's node-gap cut and R3E-11's lag cut read "ours
/// full" from the page cut, so the older rows were cut as `window_cut` (cwd-missing-node).
/// Ours' own page is not full: a diff.
#[tokio::test]
async fn g_1_a_page_cut_never_makes_ours_full() {
    let shared: Vec<Value> = (0..80u64)
        .map(|i| acct_row(&format!("s{i}"), 7_000_199 - i, "1.0"))
        .collect();
    let mut cwd = shared.clone();
    cwd.extend((0..19u64).map(|i| acct_row(&format!("o{i}"), 7_000_119 - i, "1.0")));
    let req = Request::new(
        "/txs/account/k:5fd26d23?token=coin&chain=9&limit=100".into(),
        Origin::Seed,
        None,
    );
    // Without a tip (R3E-14) and with one whose lag window holds ours' newest rows (R3E-11).
    for tip in [None, Some(7_000_190 + crate::run::ORACLE_HORIZON)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(rest_node()).await;
        let au = super::e2b::reads_server(vec![Value::Array(shared.clone())]).await;
        let bu = next_server(Value::Array(cwd.clone())).await;
        let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "chainweb_data"))
            .await
            .unwrap();
        if let Some(t) = tip {
            ctx.tips.set_a_tip(t);
        }
        let o = ctx
            .vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
            .await;
        assert_eq!(
            o.class,
            "diff",
            "{tip:?}: {:?} {:?}",
            o.allowlist_ids,
            o.record.map(|r| r.diff)
        );
    }
}

/// R-P5P9-96 (2a): a live REST record carries ours' tip as the compare read it and ours'
/// lowest listed height (7000100, before any rule dropped rows).
#[tokio::test]
async fn g_2_live_rest_records_carry_ours_tip_and_lowest_height() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = acct_pair(7_000_102);
    let nu = node_server(rest_node()).await;
    let au = super::e2b::reads_server(vec![a]).await;
    let bu = super::e2b::reads_server(vec![b]).await;
    let ctx = Ctx::new(rest_cfg(&nu, &au, &bu, dir.path(), "chainweb_data"))
        .await
        .unwrap();
    ctx.tips.set_a_tip(8_000_000);
    let req = Request::new(ACCT_REQ.into(), Origin::Seed, None);
    let o = ctx
        .vs_target(&req, &ctx.targets["a"], &ctx.targets["b"], None)
        .await;
    assert_eq!(o.class, "diff", "{:?}", o.allowlist_ids);
    let r = o.record.unwrap();
    assert_eq!(
        (r.a_tip_height, r.a_lowest_height),
        (Some(8_000_000), Some(7_000_100))
    );
}

/// R-P5P9-96 (2a), the stored form: #1854's shape with the chainweb-data-only row at ours'
/// lowest height (7000100). A record carrying ours' tip and lowest height is bounded the way
/// live is: inside the lag horizon (tip less 1,440 at or below the row) it is ours' limit
/// edge and the record is explained by the rules combined; one height past it, it is not.
/// A record without them (CG-5's), or whose lowest height is not the stored page's, keeps
/// today's behaviour: unexplained.
#[tokio::test]
async fn g_2_stored_rest_records_bound_the_edge_by_ours_tip() {
    let low = 7_000_100u64;
    let h = crate::run::ORACLE_HORIZON;
    for (tip, lowest, explained) in [
        (Some(low + h), Some(low), 1usize),
        (Some(low + h + 1), Some(low), 0),
        (None, None, 0),
        (Some(low + h), None, 0),
        (Some(low + h), Some(low - 1), 0),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = acct_pair(low);
        let nu = node_server(rest_node()).await;
        let cfg = rest_cfg(
            &nu,
            "http://127.0.0.1:9",
            "http://127.0.0.1:9",
            dir.path(),
            "chainweb_data",
        );
        let mut rec = rest_record("account", ACCT_REQ, &a, &b);
        rec.a_tip_height = tip;
        rec.a_lowest_height = lowest;
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let case = format!("{tip:?} {lowest:?}: {rep:?}");
        assert_eq!((rep.refereed, rep.explained), (1, explained), "{case}");
        if explained == 1 {
            for id in ["ours-limit-edge", "rest-compose", "cwd-missing-node"] {
                assert_eq!(rep.by_class.get(id), Some(&1), "{id} {case}");
            }
        }
    }
}

/// A `transactionsByPublicKey` row of tx `rk` at `h` in block `B<h>` (chain 0).
fn pk_tx(rk: &str, h: u64) -> Value {
    let id = crate::graphql::base64_std(format!("Transaction:[\"B{h}\",\"{rk}\"]").as_bytes());
    json!({"cursor": "c", "node": {"id": id, "hash": rk, "result": {"height": h,
        "block": {"hash": format!("B{h}"), "height": h, "chainId": 0}}}})
}

/// The node for the walk: t2 in B100 at 100, and u0 in B`top` at `top`, whose payload holds
/// u0's signed command with `signers`.
fn pk_node(top: u64, signers: &[&str]) -> NodeMock {
    let mut node = NodeMock::default();
    for (rk, h) in [("t2", 100), ("u0", top)] {
        let (k, v) = h7_poll(rk, &format!("B{h}"), h, json!([]));
        node.polls.insert(k, v);
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("B{h}"), "height": h, "chainId": 0,
                "payloadHash": format!("P{h}")})],
        );
    }
    let sig: Vec<Value> = signers.iter().map(|k| json!({"pubKey": k})).collect();
    let cmd = json!({"networkId": "mainnet01", "nonce": "n", "meta": {"chainId": "0"},
        "payload": {"exec": {"code": "(+ 1 2)", "data": {}}}, "signers": sig});
    let w = json!({"hash": "u0", "sigs": [], "cmd": cmd.to_string()}).to_string();
    let cb = json!({"reqKey": "cb", "events": []}).to_string();
    node.payloads.insert(
        format!("P{top}"),
        json!({"transactions": [[kdx_types::hash::b64url_encode(w.as_bytes()), ""]],
            "coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node.payloads.insert(
        "P100".into(),
        json!({"transactions": [],
            "coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
    );
    node
}

/// R-P5P9-96 (4), I-1 through U's `publicKey` signers: QTransactionsByPublicKey, ours lists
/// u0 above the oracle's height (7,000,000) and counts it. The node places u0 canonical, and
/// u0 counts in the walk only when the node's command has the queried key among its
/// signers: signed by pk1 it is explained; signed by pk0 alone it is outside the filter and
/// the count stays unexplained.
#[tokio::test]
async fn g_i1_an_above_tip_tx_outside_the_public_key_is_unexplained() {
    let top = 7_000_001u64;
    let doc = "query QPK($publicKey: String!, $first: Int, $after: String) { \
               transactionsByPublicKey(publicKey: $publicKey, first: $first, after: $after) { \
               totalCount pageInfo { hasNextPage endCursor } edges { cursor node { id hash \
               result { ... on TransactionResult { height block { hash height chainId } } } } } \
               } }";
    let op = crate::graphql::parse_op("QPK", doc, false).unwrap();
    let root = "transactionsByPublicKey";
    for (signer, explained) in [("pk1", true), ("pk0", false)] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(pk_node(top, &[signer])).await;
        let ours = vec![pk_tx("u0", top), pk_tx("t2", 100)];
        let au = ours_fn_server(walk_reply(root, ours, 2)).await;
        let bu = ours_fn_server(walk_reply(root, vec![pk_tx("t2", 100)], 1)).await;
        let ops = c3_ops(dir.path());
        let ctx = Ctx::new(c3_cfg(&nu, &au, &bu, dir.path(), &ops, ""))
            .await
            .unwrap();
        let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
        let vars = h7_vars(&[("publicKey", json!("pk1")), ("first", json!(100))]);
        let count = format!("/data/{root}/totalCount");
        let mut d = vec![DiffEntry {
            path: count.clone(),
            kind: DiffKind::Changed,
            a: Some(json!(2)),
            b: Some(json!(1)),
        }];
        let mut note = None;
        let mut st = Reads::default();
        ctx.gql_count_walk(
            ta,
            tb,
            &op,
            &vars,
            &mut d,
            &mut note,
            7_000_000,
            u64::MAX,
            &mut st,
        )
        .await;
        let un = c3_unexplained(&ctx, &d);
        let case = format!("{signer}: {un:?} {d:?} {note:?}");
        assert_eq!(un.is_empty(), explained, "{case}");
        if !explained {
            assert!(un.contains(&count), "{case}");
            assert!(note.unwrap_or_default().contains("publicKey"), "{case}");
        }
    }
}

/// R-P5P9-96 (4), m-9: one node read cap per compare, shared by H7 and the count walk.
/// #E3b's walk (u0 above the oracle's height, placed canonical by the node) is explained on
/// the compare's fresh reads; on reads that H7 has already taken to the cap, the walk reads
/// nothing more and the count stays unexplained, called directly and through the compare's
/// rules.
#[tokio::test]
async fn g_m9_the_count_walk_shares_the_compares_read_cap() {
    let top = 7_000_001u64;
    let row = |rk: &str, h: u64| {
        let mut e = h7_xrow(rk, h, json!(1.5), Some(("s", "r")));
        e["node"]["blockHash"] = json!(format!("B{h}"));
        e
    };
    let mut node = NodeMock::default();
    for (rk, h) in [("t2", 100u64), ("u0", top)] {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), h, "s", "r", json!(1.5));
        node.polls.insert(k, v);
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("B{h}"), "height": h, "chainId": 0,
                "payloadHash": format!("P{h}")})],
        );
        let cb = json!({"reqKey": format!("cb{h}"), "events": []}).to_string();
        node.payloads.insert(
            format!("P{h}"),
            json!({"transactions": [],
                "coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(node).await;
    let ours = vec![row("u0", top), row("t2", 100)];
    let au = ours_fn_server(walk_reply("transfers", ours.clone(), 2)).await;
    let bu = ours_fn_server(walk_reply("transfers", vec![row("t2", 100)], 1)).await;
    let ops = c3_ops(dir.path());
    let ctx = Ctx::new(c3_cfg(&nu, &au, &bu, dir.path(), &ops, ""))
        .await
        .unwrap();
    let op = ctx.gql.ops.iter().find(|o| o.stem == "QH7").unwrap();
    let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
    let vars = h7_vars(&[("accountName", json!("s")), ("first", json!(100))]);
    let count = "/data/transfers/totalCount".to_string();
    let walk_hit = |d: &[DiffEntry]| {
        d.iter()
            .any(|e| e.path == crate::graphql_window::COUNT_WALK_PATH)
    };
    for (spent, explained) in [(0usize, true), (crate::graphql_window::NODE_READS, false)] {
        // The walk alone, on the compare's reads.
        let mut d = vec![DiffEntry {
            path: count.clone(),
            kind: DiffKind::Changed,
            a: Some(json!(2)),
            b: Some(json!(1)),
        }];
        let mut note = None;
        let mut st = Reads::default();
        st.node = spent;
        ctx.gql_count_walk(
            ta,
            tb,
            op,
            &vars,
            &mut d,
            &mut note,
            7_000_000,
            u64::MAX,
            &mut st,
        )
        .await;
        let un = c3_unexplained(&ctx, &d);
        let case = format!("walk {spent}: {un:?} {d:?} {note:?}");
        assert_eq!(un.is_empty(), explained, "{case}");
        assert_eq!(walk_hit(&d), explained, "{case}");
        if !explained {
            assert!(
                note.unwrap_or_default()
                    .contains("node reads per compare is reached"),
                "{case}"
            );
        }
        // Through the compare's rules: the walk takes the compare's reads, not fresh ones.
        let (mut a, mut b) = (
            crate::graphql::Answer {
                body: json!({"data": {"transfers": {"totalCount": 2,
                    "pageInfo": {"hasNextPage": false}, "edges": ours.clone()}}}),
                status: 200,
                raw: String::new(),
                ms: vec![0],
            },
            crate::graphql::Answer {
                body: json!({"data": {"transfers": {"totalCount": 1,
                    "pageInfo": {"hasNextPage": false}, "edges": [row("t2", 100)]}}}),
                status: 200,
                raw: String::new(),
                ms: vec![0],
            },
        );
        let mut st = Reads::default();
        st.node = spent;
        let (d, note) = ctx
            .gql_rules_st(
                ta,
                H6::Live(tb),
                op,
                &vars,
                &mut a,
                &mut b,
                7_000_000,
                &mut st,
            )
            .await;
        let case = format!("rules {spent}: {d:?} {note:?}");
        assert_eq!(walk_hit(&d), explained, "{case}");
        if !explained {
            assert!(!c3_unexplained(&ctx, &d).is_empty(), "{case}");
        }
    }
}
