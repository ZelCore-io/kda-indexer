//! A `next` token minted by stock chainweb-data resumes each paged compat route on the
//! page stock serves after it: the rows strictly after its position in stock's order
//! (ties on every field skipped), its offset skipped, and the same rows as resuming from
//! our cursor at that row.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{Page, Paging, Params, SearchSettings};
use crate::cursor::{Cursor, StockCursor};
use crate::search::SearchCtx;
use crate::store::{Store, View};
use crate::ApiResult;

/// A block on `chain` at `height` with `n` txs from `k:alice`, each with a transfer to
/// `k:bob` and one to herself (two account rows with one idx), and a coinbase paying
/// `k:alice` (coinbase rows of every chain tie at a height).
fn block(chain: u8, height: u32, n: usize) -> kdx_decode::DecodedBlock {
    use kdx_types::hash::b64url_encode;
    let h32 = |tag: u8| {
        let mut b = [tag; 32];
        b[0] = (height >> 8) as u8;
        b[1] = height as u8;
        b[2] = chain;
        b
    };
    let ev = |from: &str, to: &str, amt: f64| {
        json!({"params": [from, to, amt], "name": "TRANSFER",
            "module": {"namespace": null, "name": "coin"},
            "moduleHash": b64url_encode(&[11; 32])})
    };
    let txs: Vec<_> = (0..n)
        .map(|i| {
            let cmd = json!({
                "payload": {"exec": {"data": null,
                    "code": format!("(coin.transfer \"k:alice\" \"k:bob\" {height}.{i})")}},
                "meta": {"sender": "k:alice", "chainId": chain.to_string(),
                         "creationTime": 1_700_000_000u64 + u64::from(height),
                         "gasLimit": 1000, "gasPrice": 1e-8, "ttl": 600},
                "signers": [{"pubKey": "alice-key"}],
                "nonce": format!("n-{chain}-{height}-{i}"),
                "networkId": "mainnet01",
            })
            .to_string();
            let rk = kdx_types::blake2b_256(cmd.as_bytes()).to_b64url();
            let tx = serde_json::to_vec(&json!({"hash": rk, "sigs": [{"sig": "ab"}], "cmd": cmd}))
                .unwrap();
            let out = serde_json::to_vec(&json!({"gas": 1,
                "result": {"status": "success", "data": 1}, "reqKey": rk, "logs": null,
                "events": [ev("k:alice", "k:bob", 1.5), ev("k:alice", "k:alice", 2.5)],
                "metaData": null, "continuation": null, "txId": null}))
            .unwrap();
            json!([b64url_encode(&tx), b64url_encode(&out)])
        })
        .collect();
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": b64url_encode(&h32(0xCB)), "logs": null, "events": [ev("", "k:alice", 1.0)],
        "metaData": null, "continuation": null, "txId": null}),
    )
    .unwrap();
    let miner = br#"{"account":"k:alice","predicate":"keys-all","public-keys":["m"]}"#;
    let payload_hash = b64url_encode(&h32(0xA4));
    let parent = {
        let mut b = h32(0xB0);
        b[0] = (height.saturating_sub(1) >> 8) as u8;
        b[1] = height.saturating_sub(1) as u8;
        b
    };
    let header = json!({
        "nonce": "1", "creationTime": 1_700_000_000_000_000u64 + u64::from(height) * 30_000_000,
        "parent": b64url_encode(&parent), "adjacents": {"5": b64url_encode(&h32(0xAD))},
        "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": chain,
        "weight": b64url_encode(&[1; 32]), "height": height, "chainwebVersion": "mainnet01",
        "epochStart": 1, "featureFlags": 0, "hash": b64url_encode(&h32(0xB0))
    });
    let payload = json!({
        "transactions": txs, "minerData": b64url_encode(miner),
        "transactionsHash": b64url_encode(&h32(0xA7)),
        "outputsHash": b64url_encode(&h32(0xA8)),
        "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
    });
    kdx_decode::decode_block(&header, &payload).expect("block decodes")
}

/// Heights 1..=4 on chains 0..=2, three txs a block.
fn store() -> (tempfile::TempDir, Arc<Store>) {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    {
        let mut w = hot.rw().unwrap();
        let mut dict = kdx_hot::HotDict::load(&w).unwrap();
        for h in 1..=4 {
            for c in 0..=2 {
                let ib = kdx_decode::index_block(
                    &block(c, h, 3),
                    &kdx_decode::VerifyReport::default(),
                    &mut dict,
                )
                .unwrap();
                kdx_hot::tail::apply(&mut w, &ib, &mut dict).unwrap();
            }
        }
        dict.persist(&mut w).unwrap();
        w.commit().unwrap();
    }
    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    (d, store)
}

fn pg(limit: usize, next: Option<Cursor>, stock: Option<StockCursor>) -> Paging {
    Paging {
        limit,
        offset: 0,
        next,
        stock,
        min_height: None,
        max_height: None,
        empty: false,
    }
}

/// A row's place in stock's order: (height, request key text, idx).
fn place(v: &Value) -> (u32, Vec<u8>, Option<i64>) {
    let rk = match v["requestKey"].as_str().unwrap() {
        "<coinbase>" => "cb",
        s => s,
    };
    (
        v["height"].as_u64().unwrap() as u32,
        rk.as_bytes().to_vec(),
        v["idx"].as_i64(),
    )
}

/// The stock token stock would mint for row `v` with `offset` still owed, as text.
fn stock_token(v: &Value, offset: u64, with_idx: bool) -> String {
    use base64::Engine;
    let (h, rk, idx) = place(v);
    let rk = String::from_utf8(rk).unwrap();
    let pos = if with_idx {
        format!("({h},\"{rk}\",{})", idx.unwrap())
    } else {
        format!("({h},\"{rk}\")")
    };
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(format!("({pos},{offset})"))
}

/// For every row `i` of the full listing: the stock token at `i` (decoded from the text
/// stock mints) resumes on the rows after `i` in stock's order, equal to the rows after our
/// cursor at `i` once ties with `i` are dropped; its offset is skipped; and our
/// `Chainweb-Next` after a stock-resumed page continues it with no row lost or repeated.
fn check(fetch: &dyn Fn(&Paging) -> ApiResult<Page>, with_idx: bool, min_ties: usize) {
    let full = fetch(&pg(250, None, None)).unwrap();
    assert!(full.next.is_none());
    let full = full.rows;
    assert!(full.len() > 30, "{}", full.len());
    let mut ties = 0;
    for i in 0..full.len() {
        let tok = stock_token(&full[i], 0, with_idx);
        let s = StockCursor::decode(&tok).unwrap();
        let after = |v: &Value| {
            let (h, rk, idx) = place(v);
            s.admits(h, &rk, idx.unwrap_or(0))
        };
        let want: Vec<Value> = full[i + 1..].iter().filter(|v| after(v)).cloned().collect();
        if want.len() != full.len() - i - 1 {
            ties += 1;
        }
        let got = fetch(&pg(250, None, Some(s))).unwrap();
        assert_eq!(got.rows, want, "row {i}");
        // Our cursor at row `i`, then the same filter.
        let ours = fetch(&pg(i + 1, None, None)).unwrap().next.unwrap();
        let from_ours = fetch(&pg(250, Some(ours), None)).unwrap().rows;
        assert_eq!(from_ours, full[i + 1..].to_vec(), "row {i}");
        let from_ours: Vec<Value> = from_ours.into_iter().filter(|v| after(v)).collect();
        assert_eq!(got.rows, from_ours, "row {i}");
        // An owed offset, a short page, and our token after it.
        let s3 = StockCursor::decode(&stock_token(&full[i], 3, with_idx)).unwrap();
        assert_eq!(s3.offset, 3);
        let p = fetch(&pg(4, None, Some(s3))).unwrap();
        let skip = want.len().min(3);
        assert_eq!(
            p.rows,
            want[skip..want.len().min(skip + 4)].to_vec(),
            "row {i}"
        );
        if let Some(n) = p.next {
            let rest = fetch(&pg(250, Some(n), None)).unwrap().rows;
            assert_eq!(rest, want[skip + 4..].to_vec(), "row {i}");
        } else {
            assert!(want.len() <= skip + 4, "row {i}");
        }
    }
    assert!(ties >= min_ties, "{ties} tied positions");
    // A position between stored rows (the real case's token names a request key no row
    // has): the rows after it in stock's order.
    for (i, r) in full.iter().enumerate().step_by(3) {
        let (h, mut rk, idx) = place(r);
        rk.push(b'+');
        let s =
            StockCursor::new(i64::from(h), &rk, with_idx.then_some(idx.unwrap_or(0)), 0).unwrap();
        let want: Vec<Value> = full
            .iter()
            .filter(|v| {
                let (h, rk, idx) = place(v);
                s.admits(h, &rk, idx.unwrap_or(0))
            })
            .cloned()
            .collect();
        assert_eq!(
            fetch(&pg(250, None, Some(s))).unwrap().rows,
            want,
            "row {i}"
        );
    }
    // Past the top (a height above every row, or beyond u32) is the whole listing; below 0
    // nothing.
    for (h, rows) in [
        (1_000, full.clone()),
        (i64::MAX, full.clone()),
        (-1, vec![]),
    ] {
        let s = StockCursor::new(h, b"zz", with_idx.then_some(0), 0).unwrap();
        assert_eq!(fetch(&pg(250, None, Some(s))).unwrap().rows, rows, "{h}");
    }
}

fn view(store: &Arc<Store>) -> View {
    store
        .view(Instant::now() + Duration::from_secs(60))
        .unwrap()
}

#[test]
fn account_resumes_from_stock_tokens() {
    let (_d, store) = store();
    let v = view(&store);
    // Ties: both rows of a transfer to oneself, and the coinbase rows of a height.
    check(
        &|p| super::account::page(&v, "k:alice", "coin", None, p),
        true,
        12,
    );
}

#[test]
fn events_resume_from_stock_tokens() {
    let (_d, store) = store();
    let v = view(&store);
    let q = |q: &[(&str, &str)]| {
        Params::new(
            q.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
        )
    };
    let s = SearchSettings::default();
    // Module posting path and the bounded scan.
    for f in [
        vec![("modulename", "coin"), ("name", "TRANSFER")],
        vec![("search", "alice")],
    ] {
        check(
            &|p| {
                super::events::page(
                    &v,
                    super::events::Filter::from_params(&q(&f)),
                    p,
                    s,
                    1000,
                    false,
                )
            },
            true,
            4,
        );
    }
}

#[test]
fn search_resumes_from_stock_tokens() {
    let (_d, store) = store();
    let v = view(&store);
    let ctx = SearchCtx::new(SearchSettings::default(), None);
    check(
        &|p| super::search::search_page(&v, &ctx, "k:alice", p),
        false,
        0,
    );
}

/// The `next` parameter: ours first, then stock's in the shape the route takes; garbage and
/// the other route's shape keep today's 400.
#[test]
#[allow(clippy::result_large_err)] // `paging`'s Err is the finished answer
fn next_parameter_takes_both_formats() {
    let p = |next: &str, idx: bool| {
        super::paging(&Params::new(vec![("next".into(), next.into())]), idx)
    };
    let real =
        "KCg3NDQwNjk4LCJmZWUrbzhFT2xKYS1PRzdHaTM5N0NWcjhmd2NMTUJYcHVBMGI0MkNCOHdWblVtbyIsMSksMCk";
    let g = p(real, true).ok().unwrap();
    assert!(g.next.is_none());
    let s = g.stock.unwrap();
    assert_eq!((s.height, s.idx, s.offset), (7_440_698, Some(1), 0));
    let ours = Cursor {
        key: kdx_types::TxKey::new(10, 2, 1),
        sub: 0,
        fp: 7,
    };
    // Ours is taken whichever shape the route wants.
    for idx in [true, false] {
        let g = p(&ours.encode(), idx).ok().unwrap();
        assert_eq!((g.next, g.stock), (Some(ours), None));
    }
    for bad in ["AQBx", "garbage!", &real[..20]] {
        for idx in [true, false] {
            invalid_next(p(bad, idx).err().unwrap(), bad);
        }
    }
    // `next` with `offset` stays refused.
    let both = super::paging(
        &Params::new(vec![
            ("next".into(), real.into()),
            ("offset".into(), "1".into()),
        ]),
        true,
    );
    assert!(both.is_err());
}

/// A stock token of the other route's shape gets the 400 garbage gets: account and events
/// take `((height, request key, idx), offset)`, search `((height, request key), offset)`.
#[test]
#[allow(clippy::result_large_err)] // `paging`'s Err is the finished answer
fn stock_token_shape_is_per_route() {
    let p = |next: &str, idx: bool| {
        super::paging(&Params::new(vec![("next".into(), next.into())]), idx)
    };
    // Real tokens: an account/events one with idx, a search one without.
    let with_idx =
        "KCg3NDQwNjk4LCJmZWUrbzhFT2xKYS1PRzdHaTM5N0NWcjhmd2NMTUJYcHVBMGI0MkNCOHdWblVtbyIsMSksMCk";
    let without = "KCg3NDQzOTAzLCItZThfOElaQU9ub2RNQUFkNzJON21WN2JfWEpLVTJQSXpXS05yOXA0bDZ3IiksMCk";
    assert!(p(with_idx, true).unwrap().stock.is_some());
    assert!(p(without, false).unwrap().stock.is_some());
    invalid_next(p(with_idx, false).err().unwrap(), with_idx);
    invalid_next(p(without, true).err().unwrap(), without);
}

/// The 400 `Invalid next token: <token>` answer.
fn invalid_next(r: axum::response::Response, tok: &str) {
    assert_eq!(r.status(), axum::http::StatusCode::BAD_REQUEST, "{tok}");
    let body = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(axum::body::to_bytes(r.into_body(), 1 << 16))
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.starts_with("Invalid next token: "), "{body}");
    assert!(
        body.contains(&tok.chars().take(100).collect::<String>()),
        "{body}"
    );
}

/// A bounded scan cut while a stock token is active and part of its offset is still owed:
/// each cut page hands back our token with the owed rows, and following them lists exactly
/// the rows after the stock offset, none lost or repeated.
#[test]
fn bounded_scan_cut_keeps_the_owed_stock_offset() {
    let (_d, store) = store();
    let v = view(&store);
    let q = Params::new(vec![("search".into(), "alice".into())]);
    let one = SearchSettings {
        max_scan_heights: 1,
        ..SearchSettings::default()
    };
    let fetch = |p: &Paging| {
        super::events::page(
            &v,
            super::events::Filter::from_params(&q),
            p,
            one,
            1000,
            false,
        )
    };
    let all = SearchSettings::default();
    let full = super::events::page(
        &v,
        super::events::Filter::from_params(&q),
        &pg(250, None, None),
        all,
        1000,
        false,
    )
    .unwrap()
    .rows;
    // A position above every row, with more offset owed than the top height holds.
    let top = place(&full[0]).0;
    let at_top = full.iter().filter(|r| place(r).0 == top).count();
    assert!(at_top < full.len());
    let owed = at_top + 2;
    let s = StockCursor::new(i64::from(top) + 1, b"~", Some(0), owed as u64).unwrap();
    let mut page = fetch(&pg(250, None, Some(s))).unwrap();
    assert!(page.truncated);
    assert!(page.rows.is_empty());
    let mut got = vec![];
    let mut cuts = 1;
    loop {
        got.extend(page.rows);
        let Some(n) = page.next else { break };
        page = fetch(&pg(250, Some(n), None)).unwrap();
        cuts += 1;
        assert!(cuts < 50);
    }
    assert!(cuts > 2);
    assert_eq!(got, full[owed..].to_vec());
}
