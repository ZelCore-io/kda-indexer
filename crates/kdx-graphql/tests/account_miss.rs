//! Gap Q: `fungibleAccount`, `fungibleChainAccount(s)` and `FungibleAccount.chainAccounts`
//! read the node once for an asked chain the store has no row for (as stock reads every
//! chain live), with a mock node: found rows served as stock and fed back, negatives
//! cached, limits per request, per client and in flight, timeout and errors never a wrong
//! answer.

use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_graphql::Request;
use kdx_api::{Api, ApiSettings, Store, TipWatch};
use kdx_graphql::miss::{
    DetailsFn, FoundRow, FoundSink, MissBudget, MissReader, MissSettings, NodeRow, ReadErr,
};
use kdx_graphql::{build_schema_with_miss, ClientIp, GraphqlSettings, KdxSchema};
use kdx_hot::balances::{bal_key, BalanceRow};
use kdx_hot::{HotDict, Table};
use kdx_types::{DictKind, DictMut};
use serde_json::{json, Value};

const ALICE: &str = "k:alice";
/// An account created inside module code (`free.crankkc.create-card-for-address` style):
/// no event, no literal, so no stored row.
const CARD: &str = "CCAR-0f3a9c1e-55b2-4b7e-9d0a-2c6e8f1b7a44";
const KEY: &str = "5c2b14f1e9a07d3c88aa0f1d2e3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d";

/// What the mock node answers for `(chain, module, account)`.
#[derive(Clone, Copy)]
enum Mode {
    /// CARD exists on chain 12, ALICE on chain 7, nothing else.
    Chain,
    Error,
    Hang,
}

struct Mock {
    mode: Mutex<Mode>,
    calls: AtomicUsize,
    now: AtomicUsize,
    peak: AtomicUsize,
}

fn mock(mode: Mode) -> (Arc<Mock>, DetailsFn) {
    let m = Arc::new(Mock {
        mode: Mutex::new(mode),
        calls: AtomicUsize::new(0),
        now: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mm = m.clone();
    let f: DetailsFn = Arc::new(move |chain, module, account| {
        let m = mm.clone();
        Box::pin(async move {
            m.calls.fetch_add(1, Ordering::SeqCst);
            let n = m.now.fetch_add(1, Ordering::SeqCst) + 1;
            m.peak.fetch_max(n, Ordering::SeqCst);
            let mode = *m.mode.lock().unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            let r = match mode {
                Mode::Error => Err(ReadErr::Node),
                Mode::Hang => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Err(ReadErr::Node)
                }
                Mode::Chain => {
                    let found = module == "coin"
                        && ((account == CARD && chain == 12) || (account == ALICE && chain == 7));
                    Ok(if found {
                        NodeRow::Found {
                            balance: "0.0".into(),
                            guard: json!({"keys": [KEY], "pred": "keys-all"}),
                            height: 2_500_000,
                        }
                    } else {
                        NodeRow::Absent
                    })
                }
            };
            m.now.fetch_sub(1, Ordering::SeqCst);
            r
        })
    });
    (m, f)
}

struct Fx {
    _dir: tempfile::TempDir,
    schema: KdxSchema,
    reader: Arc<MissReader>,
    mock: Arc<Mock>,
    fed: Arc<Mutex<Vec<FoundRow>>>,
}

/// A store with ALICE's coin row on chain 0 (5.0) and `free.tok` known; the reader over the
/// mock node.
fn fixture(mode: Mode, s: MissSettings) -> Fx {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    let mut t = hot.rw().unwrap();
    let mut dict = HotDict::load(&t).unwrap();
    let alice = dict.intern(DictKind::Account, ALICE);
    let coin = dict.intern(DictKind::Module, "coin");
    dict.intern(DictKind::Module, "free.tok");
    dict.persist(&mut t).unwrap();
    let row = BalanceRow {
        absent: false,
        balance: "5.0".into(),
        guard: [0; 16],
        as_of: 12,
        reconciled_at: 12,
        unverified: false,
    };
    t.put(Table::Balances, &bal_key(alice, coin, 0), &row.encode())
        .unwrap();
    t.commit().unwrap();
    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        v1txs_deadline: Duration::from_secs(30),
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    let (mock, details) = mock(mode);
    let fed = Arc::new(Mutex::new(Vec::new()));
    let f2 = fed.clone();
    let sink: FoundSink = Arc::new(move |r| f2.lock().unwrap().push(r));
    let reader = Arc::new(MissReader::new(details, Some(sink), s));
    let schema = build_schema_with_miss(
        Some(st),
        GraphqlSettings::default(),
        None,
        None,
        None,
        Some(reader.clone()),
    );
    Fx {
        _dir: d,
        schema,
        reader,
        mock,
        fed,
    }
}

fn settings() -> MissSettings {
    MissSettings {
        per_ip_rps: 0,
        ..MissSettings::default()
    }
}

/// One request as the HTTP layer sends it: its own budget, a client address.
async fn exec(fx: &Fx, q: &str, ip: [u8; 4]) -> Value {
    let r = fx
        .schema
        .execute(
            Request::new(q)
                .data(MissBudget::default())
                .data(ClientIp(IpAddr::from(ip))),
        )
        .await;
    assert!(r.errors.is_empty(), "{q}: {:?}", r.errors);
    serde_json::to_value(&r).unwrap()
}

fn acct(a: &str) -> String {
    format!(
        "{{ fungibleAccount(accountName: \"{a}\") {{ accountName totalBalance \
         chainAccounts {{ chainId balance guard {{ ... on KeysetGuard {{ keys predicate }} }} }} }} }}"
    )
}

fn chain_ids(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|r| r["chainId"].as_str().unwrap_or("").to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn a_module_internal_create_is_found_by_the_miss_read_and_fed_back() {
    let fx = fixture(Mode::Chain, settings());
    let v = exec(&fx, &acct(CARD), [10, 0, 0, 1]).await;
    let a = &v["data"]["fungibleAccount"];
    assert_eq!(a["accountName"], CARD, "{v}");
    assert_eq!(chain_ids(&a["chainAccounts"]), ["12"], "{v}");
    let row = &a["chainAccounts"][0];
    assert_eq!(row["balance"].as_f64(), Some(0.0), "{v}");
    assert_eq!(
        row["guard"],
        json!({"keys": [KEY], "predicate": "keys-all"})
    );
    assert_eq!(a["totalBalance"].as_f64(), Some(0.0));
    assert!(v
        .get("extensions")
        .is_none_or(|e| e.get("nodeReadIncomplete").is_none()));
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        20,
        "every chain missed once"
    );
    assert_eq!(
        *fx.fed.lock().unwrap(),
        [FoundRow {
            account: CARD.into(),
            module: "coin".into(),
            chain: 12,
            height: 2_500_000,
        }]
    );
    // Stored rows and found rows together, in chain order; only missed chains are read.
    let v = exec(&fx, &acct(ALICE), [10, 0, 0, 1]).await;
    let a = &v["data"]["fungibleAccount"];
    assert_eq!(chain_ids(&a["chainAccounts"]), ["0", "7"], "{v}");
    assert_eq!(a["totalBalance"].as_f64(), Some(5.0));
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20 + 19);
    // fungibleChainAccounts / fungibleChainAccount: the asked chains only, in asked order.
    let q = format!(
        "{{ fungibleChainAccounts(accountName: \"{ALICE}\", chainIds: [\"7\", \"0\", \"3\"]) {{ chainId balance }} \
         one: fungibleChainAccount(accountName: \"{CARD}\", chainId: \"12\") {{ chainId balance }} }}"
    );
    let v = exec(&fx, &q, [10, 0, 0, 1]).await;
    assert_eq!(
        chain_ids(&v["data"]["fungibleChainAccounts"]),
        ["7", "0"],
        "{v}"
    );
    assert_eq!(v["data"]["one"]["chainId"], "12", "{v}");
    // All from the cache (found and absent answers): no new call.
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 39);
}

#[tokio::test]
async fn an_unknown_account_is_null_and_its_negatives_are_cached() {
    let fx = fixture(Mode::Chain, settings());
    let v = exec(&fx, &acct("k:nobody-at-all"), [10, 0, 0, 2]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null, "{v}");
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20);
    let v = exec(&fx, &acct("k:nobody-at-all"), [10, 0, 0, 2]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20, "negatives cached");
    // No read for a name that cannot be an account, or a module the store does not know.
    exec(&fx, &acct("ab"), [10, 0, 0, 2]).await;
    let q = "{ fungibleAccount(accountName: \"k:x-y-z\", fungibleName: \"free.unknown\") { accountName } }";
    let v = exec(&fx, q, [10, 0, 0, 2]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20);
    assert!(fx.fed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_node_error_answers_the_stored_rows_flagged_and_is_not_cached() {
    let fx = fixture(Mode::Error, settings());
    let v = exec(&fx, &acct(ALICE), [10, 0, 0, 3]).await;
    let a = &v["data"]["fungibleAccount"];
    assert_eq!(chain_ids(&a["chainAccounts"]), ["0"], "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true, "{v}");
    let v = exec(&fx, &acct(CARD), [10, 0, 0, 3]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null, "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.reader.cached(), 0, "failures are never cached");
    // The node is back: the next lookup reads again and finds the row.
    *fx.mock.mode.lock().unwrap() = Mode::Chain;
    let v = exec(&fx, &acct(CARD), [10, 0, 0, 3]).await;
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["12"]
    );
}

#[tokio::test]
async fn a_hanging_node_times_out_within_the_read_timeout() {
    let s = MissSettings {
        timeout: Duration::from_millis(200),
        ..settings()
    };
    let fx = fixture(Mode::Hang, s);
    let t = std::time::Instant::now();
    let v = exec(&fx, &acct(ALICE), [10, 0, 0, 4]).await;
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0"]
    );
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.reader.cached(), 0);
}

#[tokio::test]
async fn many_aliases_and_chains_in_one_request_stay_within_the_budget() {
    let fx = fixture(Mode::Chain, settings());
    // Five aliases of random accounts (20 misses each) and a 100-entry chainIds list.
    let mut q = String::from("{ ");
    for i in 0..5 {
        q.push_str(&format!(
            "a{i}: fungibleAccount(accountName: \"k:random-{i}\") {{ accountName }} "
        ));
    }
    let ids: Vec<String> = (0..100).map(|i| format!("\"{}\"", i % 20)).collect();
    q.push_str(&format!(
        "c: fungibleChainAccounts(accountName: \"k:random-x\", chainIds: [{}]) {{ chainId }} }}",
        ids.join(",")
    ));
    let v = exec(&fx, &q, [10, 0, 0, 5]).await;
    let max = MissSettings::default().max_reads_per_request;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), max, "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    // A list of one chain 100 times reads that chain once.
    let fx = fixture(Mode::Chain, settings());
    let ids: Vec<&str> = (0..100).map(|_| "\"4\"").collect();
    let q = format!(
        "{{ fungibleChainAccounts(accountName: \"k:random-y\", chainIds: [{}]) {{ chainId }} }}",
        ids.join(",")
    );
    exec(&fx, &q, [10, 0, 0, 5]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_client_over_its_rate_gets_no_reads_and_others_are_unaffected() {
    let s = MissSettings {
        per_ip_rps: 1,
        per_ip_burst: 20,
        ..MissSettings::default()
    };
    let fx = fixture(Mode::Chain, s);
    exec(&fx, &acct("k:first-random"), [10, 0, 0, 6]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20);
    let v = exec(&fx, &acct("k:second-random"), [10, 0, 0, 6]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20, "refused: {v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    // A cached answer needs no token; another client has its own bucket.
    exec(&fx, &acct("k:first-random"), [10, 0, 0, 6]).await;
    exec(&fx, &acct(CARD), [10, 0, 0, 7]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 40);
}

#[tokio::test]
async fn reads_in_flight_stay_under_the_global_cap_and_the_cache_is_bounded() {
    let s = MissSettings {
        max_in_flight: 2,
        cache_entries: 16,
        ..settings()
    };
    let fx = Arc::new(fixture(Mode::Chain, s));
    let mut hs = Vec::new();
    for i in 0..4u8 {
        let fx = fx.clone();
        hs.push(tokio::spawn(async move {
            exec(&fx, &acct(&format!("k:parallel-{i}")), [10, 0, 1, i]).await
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 80);
    assert!(
        fx.mock.peak.load(Ordering::SeqCst) <= 2,
        "peak {}",
        fx.mock.peak.load(Ordering::SeqCst)
    );
    assert!(fx.reader.cached() <= 16, "{}", fx.reader.cached());
}
