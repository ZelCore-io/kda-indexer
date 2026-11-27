//! The read budget prices stock's DEX lists at the rows their resolvers serve: for every
//! argument shape (absent, null, zero, negative, a page, an unbound variable; no cursor,
//! `after`, `before`, both, an unbound cursor; mismatched cursor and count), the rows a list
//! answers are never more than the rows the budget priced. Served rows come from the real
//! resolvers over a small store; priced rows from `limits::budget` on the same document.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use kdx_hot::dex::{self as hd, Amount, DexEvent, Kind};
use kdx_hot::HotStore;
use kdx_types::TxKey;
use serde_json::{json, Value};
use tower::ServiceExt;

const SUSHI: &str = hd::SUSHI_EXCHANGE;
const DAY: u64 = 86_400;
/// Listable pools in the store (more than any default page).
const POOLS: usize = 30;
/// Swaps in the first pool (more than any default page).
const SWAPS: u64 = 30;

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn state(d: &tempfile::TempDir) -> (Arc<HotStore>, Arc<ApiState>) {
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(HotStore::open(&d.path().join("hot"), g).unwrap());
    let store = Arc::new(Store::open(hot.clone(), d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        v1txs_deadline: Duration::from_secs(30),
        rate_per_ip: 1_000_000,
        rate_burst: 1_000_000,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    (hot, Api::new(store, settings, TipWatch::default()).state())
}

struct W<'a> {
    txn: kdx_hot::RwTxn<'a>,
    pos: u16,
}

impl W<'_> {
    fn ev(&mut self, pair: &str, kind: Kind, t: u64, fields: &[&str]) {
        self.pos += 1;
        let key = TxKey::new(1_000_000 + (t / 30) as u32, 2, self.pos);
        let e = DexEvent {
            kind,
            pair: pair.into(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
        };
        assert!(hd::store_event(&mut self.txn, key, 0, t * 1_000_000, t, &e).unwrap());
    }
}

/// `POOLS` funded coin pools, each with an LP position of `alice`; the first one with
/// `SWAPS` swaps.
fn fixture(hot: &HotStore) -> String {
    let y = now() / DAY * DAY - DAY + 60;
    let mut w = W {
        txn: hot.rw().unwrap(),
        pos: 0,
    };
    for i in 0..POOLS {
        let pair = format!("coin:free.p{i:02}");
        let r1 = format!("{}", 1000 + i);
        w.ev(&pair, Kind::CreatePair, y, &[&pair, "acct"]);
        w.ev(
            &pair,
            Kind::AddLiquidity,
            y,
            &["lp", "lp", "10", &r1, "100"],
        );
        w.ev(&pair, Kind::Mint, y, &["alice", "100"]);
        w.ev(&pair, Kind::Update, y, &["10", &r1]);
    }
    let first = "coin:free.p00";
    for s in 0..SWAPS {
        w.ev(
            first,
            Kind::Swap,
            y + 60 + s,
            &["bob", "bob", "1", "0", "0", "1"],
        );
        w.ev(first, Kind::Update, y + 60 + s, &["10", "1000"]);
    }
    w.txn
        .set_meta(hd::meta::DEX_FILLED, &1u32.to_be_bytes())
        .unwrap();
    for h in (y / 3600)..=(now() / 3600) {
        kdx_hot::dex_usd::put(&mut w.txn, h as u32, &Amount::parse("0.005").unwrap()).unwrap();
    }
    w.txn.commit().unwrap();
    format!("{SUSHI}-{first}")
}

async fn query(st: &Arc<ApiState>, q: &str) -> Value {
    let app =
        router(build_schema(st.clone(), GraphqlSettings::default(), None)).with_state(st.clone());
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": q }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 16 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn complexity(q: &str) -> u64 {
    let doc = async_graphql::parser::parse_query(q).unwrap();
    kdx_graphql::limits::budget(
        &doc,
        &async_graphql::Variables::default(),
        &GraphqlSettings::default(),
    )
    .map(|c| c.complexity)
    .unwrap_or(u64::MAX)
}

/// A list under test: the document around its page (`{EDGES}` stands for the page's
/// `edges` selection) and the path of its connection in `data`.
struct List {
    doc: String,
    path: &'static [&'static str],
}

impl List {
    fn doc(&self, args: &str, edges: &str) -> String {
        let doc = if args.is_empty() {
            self.doc.replace("({ARGS})", "")
        } else {
            self.doc.clone()
        };
        let body = doc.replace("{ARGS}", args).replace("{EDGES}", edges);
        let mut vars = Vec::new();
        for (v, t) in [
            ("$u", "Int"),
            ("$c", "String"),
            ("$t", "PoolTransactionType"),
        ] {
            if body.contains(v) {
                vars.push(format!("{v}: {t}"));
            }
        }
        if vars.is_empty() {
            format!("query {body}")
        } else {
            format!("query Q({}) {body}", vars.join(", "))
        }
    }

    /// The rows the budget prices for the page: the cost of a second leaf per edge.
    fn priced(&self, args: &str) -> u64 {
        let one = complexity(&self.doc(args, "edges { cursor }"));
        let two = complexity(&self.doc(args, "edges { cursor c2: cursor }"));
        if one == u64::MAX || two == u64::MAX {
            // Refused before it runs: nothing is served.
            return u64::MAX;
        }
        two - one
    }

    /// The rows the resolver serves (0 when the request is refused).
    async fn served(&self, st: &Arc<ApiState>, args: &str) -> u64 {
        let v = query(st, &self.doc(args, "edges { cursor }")).await;
        let mut at = &v["data"];
        for p in self.path {
            at = &at[p];
        }
        at["edges"].as_array().map_or(0, |e| e.len() as u64)
    }

    async fn cursors(&self, st: &Arc<ApiState>) -> Vec<String> {
        let q = self.doc("first: 1000", "edges { cursor }");
        let v = query(st, &q).await;
        let mut at = &v["data"];
        for p in self.path {
            at = &at[p];
        }
        at["edges"]
            .as_array()
            .unwrap_or_else(|| panic!("{q}: {v}"))
            .iter()
            .map(|e| e["cursor"].as_str().unwrap().to_string())
            .collect()
    }
}

fn lists(id: &str) -> Vec<(&'static str, List)> {
    let l = |doc: String, path: &'static [&'static str]| List { doc, path };
    vec![
        ("pools", l("{ pools({ARGS}) { {EDGES} } }".into(), &["pools"])),
        (
            "liquidityPositions",
            l(
                "{ liquidityPositions(walletAddress: \"alice\", {ARGS}) { {EDGES} } }".into(),
                &["liquidityPositions"],
            ),
        ),
        (
            "poolTransactions",
            l(
                format!("{{ poolTransactions(pairId: \"{id}\", {{ARGS}}) {{ {{EDGES}} }} }}"),
                &["poolTransactions"],
            ),
        ),
        (
            "Pool.transactions",
            l(
                format!("{{ pool(id: \"{id}\") {{ transactions({{ARGS}}) {{ {{EDGES}} }} }} }}"),
                &["pool", "transactions"],
            ),
        ),
        // The page given to `pool` is served by its `transactions` when that field gives
        // no argument of its own: none, null, or an unbound variable.
        (
            "pool > transactions",
            l(
                format!("{{ pool(id: \"{id}\", {{ARGS}}) {{ transactions {{ {{EDGES}} }} }} }}"),
                &["pool", "transactions"],
            ),
        ),
        (
            "pool > transactions(type: null)",
            l(
                format!(
                    "{{ pool(id: \"{id}\", {{ARGS}}) {{ transactions(type: null) {{ {{EDGES}} }} }} }}"
                ),
                &["pool", "transactions"],
            ),
        ),
        (
            "pool > transactions(type: $t)",
            l(
                format!(
                    "{{ pool(id: \"{id}\", {{ARGS}}) {{ transactions(type: $t, first: $u) {{ {{EDGES}} }} }} }}"
                ),
                &["pool", "transactions"],
            ),
        ),
    ]
}

/// One request served more rows than it was priced at.
async fn check(st: &Arc<ApiState>, name: &str, list: &List, args: &str) -> Option<String> {
    let served = list.served(st, args).await;
    let priced = list.priced(args);
    (served > priced).then(|| format!("{name}({args}): served {served}, priced {priced}"))
}

/// `pool(id, first: 25) { transactions(type: $t) }` with no value for `$t`: the field takes
/// `pool`'s page (25 rows) and is priced at it, not at the default page.
#[tokio::test]
async fn unbound_variable_on_pool_transactions_is_priced_at_the_forwarded_page() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let id = fixture(&hot);
    let list = List {
        doc: format!(
            "{{ pool(id: \"{id}\", first: 25) {{ transactions(type: $t) {{ {{EDGES}} }} }} }}"
        ),
        path: &["pool", "transactions"],
    };
    assert_eq!(list.served(&st, "").await, 25);
    assert!(list.priced("") >= 25, "{}", list.priced(""));
}

/// Stock's DEX lists take the cursor's own count: `after` with `last: 1` and `before` with
/// `first: 1` serve the default page (as stock does), and are priced at it.
#[tokio::test]
async fn dex_cursor_with_the_other_count_is_priced_at_the_default_page() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let id = fixture(&hot);
    let all = lists(&id);
    let pools = &all[0].1;
    let c = pools.cursors(&st).await;
    assert_eq!(c.len(), POOLS);
    for args in [
        format!("after: \"{}\", last: 1", c[0]),
        format!("before: \"{}\", first: 1", c[POOLS - 1]),
    ] {
        assert_eq!(pools.served(&st, &args).await, 20, "{args}");
        assert!(pools.priced(&args) >= 20, "{args}: {}", pools.priced(&args));
    }
}

/// For a grid of argument shapes on every DEX list, the rows served are never more than the
/// rows priced.
#[tokio::test]
async fn dex_lists_never_serve_more_rows_than_priced() {
    let d = tempfile::tempdir().unwrap();
    let (hot, st) = state(&d);
    let id = fixture(&hot);
    let counts = ["", "null", "0", "1", "5", "25", "-1", "$u"];
    let mut bad = Vec::new();
    let mut runs = 0;
    for (name, list) in lists(&id) {
        // Cursors of the list itself (the pool's transactions for `pool > ...`).
        let c = if name.starts_with("pool >") {
            lists(&id)[3].1.cursors(&st).await
        } else {
            list.cursors(&st).await
        };
        assert!(c.len() >= 25, "{name}: {} rows", c.len());
        let (a, b) = (&c[0], &c[c.len() - 1]);
        let cursors = [
            String::new(),
            format!("after: \"{a}\""),
            format!("before: \"{b}\""),
            format!("after: \"{a}\", before: \"{b}\""),
            "after: $c".into(),
            "before: $c".into(),
        ];
        for f in counts {
            for l in counts {
                for cur in &cursors {
                    let mut args: Vec<String> = Vec::new();
                    if !f.is_empty() {
                        args.push(format!("first: {f}"));
                    }
                    if !l.is_empty() {
                        args.push(format!("last: {l}"));
                    }
                    if !cur.is_empty() {
                        args.push(cur.clone());
                    }
                    let args = args.join(", ");
                    runs += 1;
                    if let Some(e) = check(&st, name, &list, &args).await {
                        bad.push(e);
                    }
                }
            }
        }
    }
    assert!(runs > 2000, "{runs}");
    assert!(bad.is_empty(), "{} shapes:\n{}", bad.len(), bad.join("\n"));
}
