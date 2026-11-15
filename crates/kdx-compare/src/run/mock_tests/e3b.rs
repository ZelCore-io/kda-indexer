//! FVH-E3b (R-P5P9-92 (2), R-P5P9-93; R-P5P9-88 W; R-P5P9-87 stored forms).

use super::*;
use crate::graphql::H6;
use crate::report::{DiffEntry, DiffKind};

/// A list server for the count walk (as E2b's): `rows` under `transfers`, `first` a page
/// from the `after` cursor (a row index), with `count` as totalCount.
fn walk(rows: Vec<Value>, count: u64) -> impl Fn(&Value) -> Value + Clone + Send + Sync {
    move |req: &Value| {
        let first = req["variables"]["first"].as_u64().unwrap_or(100) as usize;
        let at = req["variables"]["after"]
            .as_str()
            .and_then(|c| c.parse::<usize>().ok())
            .unwrap_or(0);
        let page: Vec<Value> = rows.iter().skip(at).take(first).cloned().collect();
        let end = at + page.len();
        json!({"data": {"transfers": {"totalCount": count,
            "pageInfo": {"hasNextPage": end < rows.len(), "endCursor": end.to_string()},
            "edges": page}}})
    }
}

fn page(rows: Vec<Value>, count: u64, more: bool) -> Value {
    json!({"data": {"transfers": {"totalCount": count,
        "pageInfo": {"hasNextPage": more}, "edges": rows}}})
}

fn answer(body: Value) -> crate::graphql::Answer {
    crate::graphql::Answer {
        body,
        status: 200,
        raw: String::new(),
        ms: vec![0],
    }
}

/// A transfer row at `h` in block `b` (chain 0).
fn xrow(rk: &str, h: u64, b: &str) -> Value {
    let mut e = h7_xrow(rk, h, json!(1.5), Some(("s", "r")));
    e["node"]["blockHash"] = json!(b);
    e
}

fn count_diff(a: u64, b: u64) -> DiffEntry {
    DiffEntry {
        path: "/data/transfers/totalCount".into(),
        kind: DiffKind::Changed,
        a: Some(json!(a)),
        b: Some(json!(b)),
    }
}

/// The node: each `(rk, height)` placed by `/poll` in block `B<height>`, the only (so the
/// canonical) header there, its payload holding `txs[h - lo]` transactions from `lo` on.
fn node_of(rows: &[(&str, u64)], lo: u64, txs: &[usize]) -> NodeMock {
    let mut node = NodeMock::default();
    for (rk, h) in rows {
        let (k, v) = h7_xfer_poll(rk, &format!("B{h}"), *h, "s", "r", json!(1.5));
        node.polls.insert(k, v);
    }
    let mut hs: Vec<u64> = rows.iter().map(|r| r.1).collect();
    hs.extend((0..txs.len() as u64).map(|i| lo + i));
    for h in hs {
        let n = h.checked_sub(lo).and_then(|i| txs.get(i as usize)).copied();
        node.headers.insert(
            (0, h),
            vec![json!({"hash": format!("B{h}"), "height": h,
                "payloadHash": format!("P{h}"), "chainId": 0})],
        );
        let cb = json!({"reqKey": format!("cb{h}"), "events": []}).to_string();
        let t: Vec<Value> = (0..n.unwrap_or(0))
            .map(|i| json!([format!("t{i}"), ""]))
            .collect();
        node.payloads.insert(
            format!("P{h}"),
            json!({"transactions": t,
                "coinbase": kdx_types::hash::b64url_encode(cb.as_bytes())}),
        );
    }
    node
}

/// The operation the oracle-counts-heights entry is scoped to, from its entry (no capture
/// stem in the sources, M1).
fn tc243() -> String {
    d2_shipped()
        .entries
        .iter()
        .find(|e| e.id == "graphql-r75-oracle-counts-heights")
        .and_then(|e| e.request_contains.clone())
        .and_then(|r| r.strip_prefix("graphql:").map(str::to_string))
        .unwrap()
}

/// That operation's shape over `transfers`: one chain and a height window.
fn tc243_ops(dir: &std::path::Path) -> std::path::PathBuf {
    let ops = dir.join("ops");
    std::fs::create_dir_all(&ops).unwrap();
    std::fs::write(
        ops.join(format!("{}.graphql", tc243())),
        "# kdx: oracle\nquery Q($chainId: String, $minHeight: Int, $maxHeight: Int, \
         $first: Int, $after: String) { transfers(chainId: $chainId, minHeight: $minHeight, \
         maxHeight: $maxHeight, first: $first, after: $after) { totalCount pageInfo { \
         hasNextPage endCursor } edges { cursor node { requestKey } } } }\n",
    )
    .unwrap();
    ops
}

fn with_rule(mut cfg: CompareConfig, on: bool) -> CompareConfig {
    use crate::config::{FieldRule, FieldRuleKind};
    if on {
        cfg.graphql.field_rules.push(FieldRule::new(
            "transfers.totalCount",
            FieldRuleKind::OracleCountsHeights,
        ));
    }
    cfg
}

fn unexplained_on(ctx: &Ctx, op: &str, d: &[DiffEntry]) -> Vec<String> {
    let (un, _) = ctx.allow.classify_request(
        &crate::normalize::RouteKind::Other,
        &format!("graphql:{op}"),
        &[],
        d,
    );
    un.into_iter().map(|e| e.path).collect()
}

/// R-P5P9-93: #243's exact shape. Ours lists 2 rows over the window 100..=102 on chain 0 =
/// the node's [2, 0, 0] txs, and the oracle counts the window's 3 heights over the same 2
/// rows: explained by the ACCEPTED oracle-counts-heights predicate, live and from the stored
/// record. The node's [2, 1, 0], an oracle count of 4 and no configured rule stay
/// unexplained.
#[tokio::test]
async fn e3b_tc_243_the_oracle_counts_the_windows_heights() {
    let rows = || vec![xrow("t2", 100, "B100"), xrow("t1", 100, "B100")];
    let op_name = tc243();
    let vars = h7_vars(&[
        ("chainId", json!("0")),
        ("minHeight", json!(100)),
        ("maxHeight", json!(102)),
        ("first", json!(10)),
    ]);
    let placed = [("t2", 100), ("t1", 100)];
    for (txs, oracle_count, rule, explained) in [
        ([2usize, 0, 0], 3u64, true, true),
        ([2, 1, 0], 3, true, false),
        ([2, 0, 0], 4, true, false),
        ([2, 0, 0], 3, false, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let nu = node_server(node_of(&placed, 100, &txs)).await;
        let au = ours_fn_server(walk(rows(), 2)).await;
        let bu = ours_fn_server(walk(rows(), oracle_count)).await;
        let ops = tc243_ops(dir.path());
        let cfg = with_rule(c3_cfg(&nu, &au, &bu, dir.path(), &ops, ""), rule);
        let ctx = Ctx::new(cfg.clone()).await.unwrap();
        let op = ctx.gql.ops.iter().find(|o| o.stem == op_name).unwrap();
        let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
        let (mut a, mut b) = (
            answer(page(rows(), 2, false)),
            answer(page(rows(), oracle_count, false)),
        );
        let (d, note) = ctx
            .gql_rules(ta, H6::Live(tb), op, &vars, &mut a, &mut b, 7_000_000)
            .await;
        let un = unexplained_on(&ctx, &op_name, &d);
        let case = format!("{txs:?} {oracle_count} {rule}: {un:?} {d:?} {note:?}");
        assert_eq!(un.is_empty(), explained, "{case}");
        let hit = d
            .iter()
            .any(|e| e.path == crate::graphql_window::ORACLE_COUNTS_HEIGHTS_PATH);
        assert_eq!(hit, explained, "{case}");
        if !explained {
            assert!(
                un.contains(&"/data/transfers/totalCount".to_string()),
                "{case}"
            );
        }
        // The stored record (R-P5P9-87): the node and the frozen oracle as they are now.
        let rec = e2_stored(
            &op_name,
            Value::Object(vars.clone()),
            page(rows(), 2, false),
            page(rows(), oracle_count, false),
            vec![count_diff(2, oracle_count)],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        assert_eq!(rep.explained == 1, explained, "{case} {rep:?}");
        if explained {
            assert_eq!(
                rep.by_class.get("graphql-r75-oracle-counts-heights"),
                Some(&1),
                "{rep:?}"
            );
        }
    }
}

/// R-P5P9-92 (2), live: ours lists a row above the oracle's height (7,000,000) that the
/// oracle cannot hold. It is in ours' count, and explained only when the node places it
/// canonical at its height (`aboveOracleTip` 1). Unplaced on the node it stays unexplained,
/// and past 50 such rows the reason is "rows above the oracle tip: capped".
#[tokio::test]
async fn e3b_tc_rows_above_the_oracle_tip_are_node_canonical_up_to_50() {
    let top = 7_000_001u64;
    for (above, placed, want) in [(1usize, true, "ok"), (1, false, "row"), (51, true, "cap")] {
        let dir = tempfile::tempdir().unwrap();
        let ups: Vec<Value> = (0..above)
            .map(|i| xrow(&format!("u{i}"), top, &format!("B{top}")))
            .collect();
        let mut ours = ups.clone();
        ours.push(xrow("t2", 100, "B100"));
        let n = ours.len() as u64;
        let mut keys = vec![("t2", 100u64)];
        let names: Vec<String> = (0..above).map(|i| format!("u{i}")).collect();
        if placed {
            keys.extend(names.iter().map(|k| (k.as_str(), top)));
        }
        let nu = node_server(node_of(&keys, 0, &[])).await;
        let au = ours_fn_server(walk(ours.clone(), n)).await;
        let bu = ours_fn_server(walk(vec![xrow("t2", 100, "B100")], 1)).await;
        let ops = c3_ops(dir.path());
        let ctx = Ctx::new(c3_cfg(&nu, &au, &bu, dir.path(), &ops, ""))
            .await
            .unwrap();
        let op = ctx.gql.ops.iter().find(|o| o.stem == "QH7").unwrap();
        let (ta, tb) = (&ctx.targets["a"], &ctx.targets["b"]);
        let vars = h7_vars(&[("accountName", json!("s")), ("first", json!(100))]);
        let (mut a, mut b) = (
            answer(page(ours, n, false)),
            answer(page(vec![xrow("t2", 100, "B100")], 1, false)),
        );
        let (d, note) = ctx
            .gql_rules(ta, H6::Live(tb), op, &vars, &mut a, &mut b, 7_000_000)
            .await;
        let un = c3_unexplained(&ctx, &d);
        let case = format!("{above} {placed}: {un:?} {d:?} {note:?}");
        match want {
            "ok" => {
                assert!(un.is_empty(), "{case}");
                let w = d
                    .iter()
                    .find(|e| e.path == crate::graphql_window::COUNT_WALK_PATH)
                    .expect("the walk's hit");
                assert_eq!(w.a.as_ref().unwrap()["aboveOracleTip"], json!(1), "{case}");
            }
            "row" => {
                assert!(
                    un.contains(&"/data/transfers/totalCount".to_string()),
                    "{case}"
                );
                assert!(
                    note.unwrap_or_default().contains("is not node-canonical"),
                    "{case}"
                );
            }
            _ => {
                assert!(
                    un.contains(&crate::graphql_window::COUNT_WALK_CAPPED_PATH.to_string()),
                    "{case}"
                );
                assert!(
                    note.unwrap_or_default()
                        .contains("rows above the oracle tip: capped"),
                    "{case}"
                );
            }
        }
    }
}

/// R-P5P9-92 (2) with R-P5P9-87, the stored form: the record's height (7,000,001, ours'
/// highest stored row) is above the oracle's (7,000,000). Ours' row there was in ours'
/// count at the time and is polled node-canonical; ours' newer row at 7,000,009 (above the
/// record) is left out: explained. Before, the bound at the oracle's height dropped both
/// and the counts disagreed.
#[tokio::test]
async fn e3b_tc_the_referee_counts_ours_rows_between_the_oracle_and_the_record() {
    let top = 7_000_001u64;
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(node_of(&[("t2", 100), ("u0", top)], 0, &[])).await;
    let now = vec![
        xrow("n9", 7_000_009, "B7000009"),
        xrow("u0", top, &format!("B{top}")),
        xrow("t2", 100, "B100"),
    ];
    let au = ours_fn_server(walk(now, 3)).await;
    let bu = ours_fn_server(walk(vec![xrow("t2", 100, "B100")], 1)).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(&nu, &au, &bu, dir.path(), &ops, "");
    let stored = vec![xrow("u0", top, &format!("B{top}")), xrow("t2", 100, "B100")];
    let rec = e2_stored(
        "QH7",
        json!({"accountName": "s", "first": 100}),
        page(stored, 2, false),
        page(vec![xrow("t2", 100, "B100")], 1, false),
        vec![count_diff(2, 1)],
    );
    let rep = e2_referee(cfg, dir.path(), &[rec]).await;
    assert_eq!((rep.explained, rep.unexplained), (1, 0), "{rep:?}");
    assert_eq!(rep.by_class.get("graphql-count-walk"), Some(&1), "{rep:?}");
}

/// R-P5P9-88 (3b), W: a stored record whose rows carry no height (#404's shape: the
/// document on the host selected none). The referee reads the node's height for each row's
/// request key (`/poll`) and applies the normal rules: the oracle's extra row below ours'
/// cut (100, ours' lowest 101) is outside ours' window, explained. The same row at 102
/// (inside ours' window) stays unexplained, and so does one the node does not place.
#[tokio::test]
async fn e3b_w_the_referee_places_height_less_rows_by_poll() {
    let bare = |rk: &str| {
        json!({"cursor": "c", "node": {"requestKey": rk,
        "block": {"chainId": "0"}}})
    };
    for (extra_at, placed, explained) in [
        (100u64, true, true),
        (102, true, false),
        (100, false, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut keys = vec![("t3", 103u64), ("t2", 101)];
        if placed {
            keys.push(("t1", extra_at));
        }
        let nu = node_server(node_of(&keys, 0, &[])).await;
        let ops = c3_ops(dir.path());
        let cfg = c3_cfg(&nu, &nu, &nu, dir.path(), &ops, "");
        let ours = page(vec![bare("t3"), bare("t2")], 40, true);
        let oracle = page(vec![bare("t3"), bare("t2"), bare("t1")], 40, true);
        let added = DiffEntry {
            path: "/data/transfers/edges/2".into(),
            kind: DiffKind::Missing,
            a: None,
            b: Some(bare("t1")),
        };
        let rec = e2_stored(
            "QH7",
            json!({"accountName": "s", "first": 2}),
            ours,
            oracle,
            vec![added],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let case = format!("{extra_at} {placed}: {rep:?}");
        assert_eq!(rep.explained == 1, explained, "{case}");
        assert!(
            !format!("{rep:?}").contains("without a height to place"),
            "{case}"
        );
    }
}

/// R-P5P9-92 (9) with R-P5P9-93: the /v1/txs edge's stored form reads ours and kda-txs now,
/// and each of those reads takes one read of `--max-reads`. #1871's shape: with the reads
/// the record needs it is explained; one read fewer leaves the re-read of kda-txs past the
/// cap, unexplained with that reason.
#[tokio::test]
async fn e3b_the_v1txs_stored_reread_counts_in_max_reads() {
    let (a0, b0) = super::e2b::v1_first();
    let all: Vec<Value> = std::iter::once(super::e2b::v1_row("r546", 7_006_546))
        .chain(b0.as_array().unwrap().iter().cloned())
        .collect();
    let run = |max_reads: usize| {
        let (a0, b0, all) = (a0.clone(), b0.clone(), all.clone());
        async move {
            let dir = tempfile::tempdir().unwrap();
            let nu = node_server(super::e2b::v1_node(None)).await;
            let au = super::e2b::reads_server(vec![Value::Array(all.clone())]).await;
            let bu = super::e2b::reads_server(vec![Value::Array(all)]).await;
            let cfg = super::e2b::rest_cfg(&nu, &au, &bu, dir.path(), "kda_txs");
            let rec = super::e2b::rest_record("v1txs", super::e2b::V1_REQ, &a0, &b0);
            let log = dir.path().join("diffs-v1.jsonl");
            std::fs::write(&log, serde_json::to_string(&rec.bounded()).unwrap() + "\n").unwrap();
            let opts = crate::referee::Opts {
                logs: vec![log],
                max_reads,
                rps: 100,
            };
            crate::referee::run(cfg, &opts).await.unwrap()
        }
    };
    let full = run(200).await;
    assert_eq!(full.explained, 1, "{full:?}");
    let need = full.node_reads;
    let rep = run(need).await;
    assert_eq!(rep.explained, 1, "{need}: {rep:?}");
    let short = run(need - 1).await;
    assert_eq!(short.explained, 0, "{need}: {short:?}");
    assert!(
        format!("{short:?}").contains("the run's read cap is reached before the re-read"),
        "{short:?}"
    );
}

/// R-P5P9-88 (3a): at start the harness takes its assets hash and build sha into the day's
/// summary (and the log), from the allowlist and the documents it loads.
#[tokio::test]
async fn e3b_the_harness_records_its_assets_at_start() {
    let dir = tempfile::tempdir().unwrap();
    let nu = node_server(NodeMock::default()).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(&nu, &nu, &nu, dir.path(), &ops, "");
    let want = crate::assets::assets_line(&cfg.allowlist, &ops).unwrap();
    let ctx = Ctx::new(cfg).await.unwrap();
    let got = ctx.summary.lock().unwrap().assets.clone();
    assert_eq!(got.into_iter().collect::<Vec<_>>(), vec![want]);
}

/// R-P5P9-86 (diff log volume): the past days' diff logs rotate to zstd. Every record is
/// kept (the rotated file decompresses to the raw bytes, and the referee reads all of its
/// records), the day's bytes are recorded in its summary, today's log stays as it is, and
/// `kda-compare summary` prints the bytes per day from the rotated files.
#[tokio::test]
async fn e3b_diff_logs_rotate_daily_and_keep_every_record() {
    let dir = tempfile::tempdir().unwrap();
    let rec = |i: u64| {
        let mut r = e2_stored(
            "QH7",
            json!({"accountName": "s"}),
            json!({}),
            json!({}),
            vec![],
        );
        r.request = format!("QH7 {{\"n\": {i}}}");
        serde_json::to_string(&r.bounded()).unwrap()
    };
    let old: String = (0..300).map(|i| rec(i) + "\n").collect();
    std::fs::write(dir.path().join("diffs-2026-11-14.jsonl"), &old).unwrap();
    std::fs::write(dir.path().join("diffs-2026-11-15.jsonl"), rec(7) + "\n").unwrap();
    let days = crate::report::rotate_logs(dir.path(), "2026-11-15").unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(
        (days[0].0.as_str(), days[0].1),
        ("2026-11-14", old.len() as u64)
    );
    assert!(days[0].2 < days[0].1, "{days:?}");
    assert!(!dir.path().join("diffs-2026-11-14.jsonl").exists());
    assert!(dir.path().join("diffs-2026-11-15.jsonl").exists());
    let zst = dir.path().join("diffs-2026-11-14.jsonl.zst");
    let back = zstd::decode_all(std::fs::read(&zst).unwrap().as_slice()).unwrap();
    assert_eq!(back, old.as_bytes());
    let day: crate::report::DailySummary =
        serde_json::from_slice(&std::fs::read(dir.path().join("summary-2026-11-14.json")).unwrap())
            .unwrap();
    assert_eq!(day.diff_log_bytes["raw"], old.len() as u64);
    assert_eq!(day.diff_log_bytes["zst"], days[0].2);
    // The referee reads the rotated file's every record.
    let nu = node_server(NodeMock::default()).await;
    let ops = c3_ops(dir.path());
    let cfg = c3_cfg(&nu, &nu, &nu, dir.path(), &ops, "");
    for log in [zst, dir.path().join("diffs-2026-11-14.jsonl")] {
        // The rotated file, and the raw name the re-run's command line gives.
        let opts = crate::referee::Opts {
            logs: vec![log],
            max_reads: 10,
            rps: 100,
        };
        let rep = crate::referee::run(cfg.clone(), &opts).await.unwrap();
        assert_eq!(rep.records, 300, "{rep:?}");
    }
    // The referee's start rotates nothing (today's log is read in place).
    assert!(dir.path().join("diffs-2026-11-15.jsonl").exists());
    // The summary prints the bytes per day from the rotated files.
    let (text, _) =
        crate::summary::run_text(dir.path(), "2026-11-14T00:00:00Z", "must", "small").unwrap();
    let mb = |b: u64| format!("{:.1} MB", b as f64 / 1e6);
    assert!(
        text.contains(&format!(
            "  2026-11-14: raw {}, zst {}",
            mb(days[0].1),
            mb(days[0].2)
        )),
        "{text}"
    );
    assert!(
        text.contains("  2026-11-15: raw 0.0 MB, not rotated"),
        "{text}"
    );
    // Nothing more to rotate.
    assert!(crate::report::rotate_logs(dir.path(), "2026-11-15")
        .unwrap()
        .is_empty());
}
