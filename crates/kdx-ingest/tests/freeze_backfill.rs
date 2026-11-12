//! Freeze = backfill (Task 14, replay determinism): the steps the freezer writes from the
//! hot store, and the range it merges from ten of them, are byte-equal to what `backfill`
//! writes for the same heights fetched from a node. The node is a local stand-in serving the
//! pool blocks (`/cut`, `header/branch`, `payload/outputs/batch`), so the backfill runs its
//! own fetch, decode, build and range-writer path end to end.

mod common;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use common::*;
use serde_json::{json, Value};

/// A minimal chainweb stand-in for chain 0 holding `chain` (ascending, linear).
fn fake_node(pool: Vec<Value>, chain: Vec<Place>) -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let headers: Vec<Value> = chain.iter().map(|p| header(&pool, *p)).collect();
    let payloads: BTreeMap<String, Value> = chain
        .iter()
        .map(|p| {
            let v = pool[p.pool]["payload"].clone();
            (v["payloadHash"].as_str().unwrap().to_owned(), v)
        })
        .collect();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let mut buf = Vec::new();
            let mut tmp = [0u8; 8192];
            let (head, body) = loop {
                let n = s.read(&mut tmp).unwrap_or(0);
                if n == 0 {
                    break (String::new(), Vec::new());
                }
                buf.extend_from_slice(&tmp[..n]);
                let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                    continue;
                };
                let head = String::from_utf8_lossy(&buf[..i]).into_owned();
                let len = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if buf.len() >= i + 4 + len {
                    break (head, buf[i + 4..i + 4 + len].to_vec());
                }
            };
            let line = head.lines().next().unwrap_or("").to_owned();
            let path = line.split(' ').nth(1).unwrap_or("").to_owned();
            let (route, query) = path.split_once('?').unwrap_or((&path, ""));
            let q: BTreeMap<&str, u64> = query
                .split('&')
                .filter_map(|kv| {
                    let (k, v) = kv.split_once('=')?;
                    Some((k, v.parse().ok()?))
                })
                .collect();
            let resp = if route.ends_with("/cut") {
                let tip = headers.last().unwrap();
                json!({"hashes": {"0": {"height": tip["height"], "hash": tip["hash"]}},
                       "height": tip["height"], "weight": "AAAA", "origin": null,
                       "instance": "mainnet01", "id": "x"})
            } else if route.ends_with("/header/branch") {
                let (lo, hi) = (
                    q.get("minheight").copied().unwrap_or(0),
                    q.get("maxheight").copied().unwrap_or(u64::MAX),
                );
                let items: Vec<&Value> = headers
                    .iter()
                    .rev()
                    .filter(|h| (lo..=hi).contains(&h["height"].as_u64().unwrap()))
                    .collect();
                json!({"items": items, "limit": items.len(), "next": null})
            } else if route.ends_with("/payload/outputs/batch") {
                let want: Vec<String> = serde_json::from_slice(&body).unwrap_or_default();
                Value::Array(
                    want.iter()
                        .filter_map(|h| payloads.get(h).cloned())
                        .collect(),
                )
            } else {
                Value::Null
            };
            let (status, b) = if resp.is_null() {
                ("404 Not Found", b"{}".to_vec())
            } else {
                ("200 OK", serde_json::to_vec(&resp).unwrap())
            };
            let _ = write!(
                s,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                b.len()
            );
            let _ = s.write_all(&b);
        }
    });
    format!("http://{addr}")
}

fn files(dir: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                blake3::hash(&std::fs::read(&p).unwrap())
                    .to_hex()
                    .to_string(),
            )
        })
        .collect()
}

/// Freeze with `range` (merge on when 10 × step fits), then backfill the frozen heights
/// from the stand-in node into a fresh data dir; the segment dirs must be equal.
async fn freeze_equals_backfill(range: u64) {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let chain = branch(&all, 0, 0, 0);

    let df = tempfile::tempdir().unwrap();
    let frozen = {
        let c = ctx(df.path(), 2, range, 2);
        apply_linear(&c, &pool, &chain, 5);
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        kdx_segment::manifest::Manifest::load(&c.segdir)
            .unwrap()
            .unwrap()
    };
    assert_eq!(frozen.frontier, 20);

    let base = fake_node(pool, chain);
    let db = tempfile::tempdir().unwrap();
    let c = Arc::new(ctx_with_node(db.path(), 2, range, 2, &base));
    kdx_ingest::backfill::run(c.clone(), 0, 19, kdx_ingest::HeaderMode::Cut)
        .await
        .unwrap();
    let backfilled = kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap();
    assert_eq!(backfilled, frozen);
    let (ff, fb) = (
        files(&df.path().join("segments")),
        files(&db.path().join("segments")),
    );
    assert!(ff.len() >= 15, "{ff:?}");
    assert_eq!(fb, ff);
}

/// Ten steps of 2 heights, no merge: the step files are byte-equal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frozen_steps_equal_backfilled_steps() {
    freeze_equals_backfill(1000).await;
}

/// Ten steps merged into the range 0..20: byte-equal to a backfilled range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merged_range_equals_a_backfilled_range() {
    freeze_equals_backfill(20).await;
}

/// Two chains, the same blocks, committed per height chain 0 first in one store and chain 1
/// first in the other (the tail commits chains in arrival order; backfill builds in
/// (height, chain) order). New names get ids in commit order and the freezer copies the hot
/// bytes, so the frozen steps differ, and at most one of the two orders can equal a backfill.
#[test]
#[ignore = "finding 9B-2: frozen step bytes depend on the cross-chain tail commit order (dictionary ids in arrival order), so freeze = backfill holds byte for byte only when the tail happened to commit in (height, chain) order"]
fn frozen_steps_do_not_depend_on_the_cross_chain_commit_order() {
    let pool = pool();
    let run = |dir: &Path, order: [u8; 2]| {
        let c = ctx_full(dir, 2, 1000, 2, "http://127.0.0.1:9", &[0, 1]);
        for h in 0..12u32 {
            for chain in order {
                let p = Place {
                    pool: h as usize * 2 + chain as usize,
                    height: h,
                    br: chain,
                    parent_br: chain,
                };
                let mut hd = header(&pool, p);
                hd["chainId"] = json!(chain);
                let b =
                    kdx_ingest::pipeline::prepare_value(chain, h, &hd, &pool[p.pool]["payload"])
                        .unwrap();
                kdx_ingest::apply::commit(
                    &c,
                    &[kdx_ingest::apply::Batch {
                        chain,
                        fork: None,
                        blocks: vec![b],
                        halted: false,
                        halt_reason: "",
                    }],
                )
                .unwrap();
            }
        }
        while kdx_ingest::freezer::tick(&c).unwrap().is_some() {}
        files(&dir.join("segments"))
    };
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = run(da.path(), [0, 1]);
    assert!(a.len() > 15, "{a:?}");
    assert_eq!(run(db.path(), [1, 0]), a);
}
