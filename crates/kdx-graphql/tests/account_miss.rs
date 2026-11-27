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
use kdx_decode::{decode_block, index_block, VerifyReport};
use kdx_graphql::miss::{
    DetailsFn, FoundRow, FoundSink, MissBudget, MissReader, MissSettings, NodeRow, ReadErr,
};
use kdx_graphql::{build_schema_with_miss, ClientIp, GraphqlSettings, KdxSchema};
use kdx_hot::balances::{bal_key, BalanceRow};
use kdx_hot::{HotDict, Table};
use kdx_types::hash::b64url_encode;
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
    /// As `Chain`, after 100 ms.
    Slow,
    /// As `Chain`, but this chain hangs.
    HangOn(u8),
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
                Mode::HangOn(c) if c == chain => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Err(ReadErr::Node)
                }
                Mode::Chain | Mode::Slow | Mode::HangOn(_) => {
                    if matches!(mode, Mode::Slow) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
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
    for (c, miner) in MINERS {
        let ib = index_block(&miner_block(c, miner), &VerifyReport::default(), &mut dict)
            .expect("block indexes");
        kdx_hot::tail::apply(&mut t, &ib, &mut dict).unwrap();
    }
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

/// Blocks at height 1 with no user tx, by chain and miner: CARD (no stored row, on chain on
/// 12 per the mock), ALICE on chain 0 (stored row), a name the node has no row for, and one
/// that is no account name at all (two characters).
const MINERS: [(u8, &str); 4] = [(12, CARD), (0, ALICE), (3, "k:nobody"), (4, "ab")];

fn miner_hash(c: u8) -> String {
    b64url_encode(&[0xB0 ^ c; 32])
}

fn miner_block(c: u8, miner: &str) -> kdx_decode::DecodedBlock {
    let cb = serde_json::to_vec(
        &json!({"gas": 0, "result": {"status": "success", "data": "ok"},
        "reqKey": b64url_encode(&[0xCB ^ c; 32]), "logs": null, "events": [],
        "metaData": null, "continuation": null, "txId": null}),
    )
    .unwrap();
    let md = serde_json::to_vec(&json!({"account": miner, "predicate": "keys-all",
        "public-keys": []}))
    .unwrap();
    let payload_hash = b64url_encode(&[0xA4 ^ c; 32]);
    let header = json!({
        "nonce": "1", "creationTime": 1_700_000_000_000_000u64,
        "parent": b64url_encode(&[0xB1 ^ c; 32]),
        "adjacents": {"5": b64url_encode(&[0xAD ^ c; 32])},
        "target": b64url_encode(&[0xff; 32]), "payloadHash": payload_hash, "chainId": c,
        "weight": b64url_encode(&[1; 32]), "height": 1, "chainwebVersion": "mainnet01",
        "epochStart": 1, "featureFlags": 0, "hash": miner_hash(c)
    });
    let payload = json!({
        "transactions": [], "minerData": b64url_encode(&md),
        "transactionsHash": b64url_encode(&[0xA7 ^ c; 32]),
        "outputsHash": b64url_encode(&[0xA8 ^ c; 32]),
        "payloadHash": payload_hash, "coinbase": b64url_encode(&cb),
    });
    decode_block(&header, &payload).expect("block decodes")
}

fn miner_q(c: u8) -> String {
    format!(
        "{{ block(hash: \"{}\") {{ minerAccount {{ accountName chainId balance \
         guard {{ __typename raw }} }} }} }}",
        miner_hash(c)
    )
}

fn settings() -> MissSettings {
    MissSettings {
        per_ip_rps: 0,
        node_rps: 0,
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
async fn a_node_error_answers_the_stored_rows_flagged_and_is_cached_briefly() {
    let s = MissSettings {
        ttl_error: Duration::from_millis(300),
        ..settings()
    };
    let fx = fixture(Mode::Error, s);
    let v = exec(&fx, &acct(ALICE), [10, 0, 0, 3]).await;
    let a = &v["data"]["fungibleAccount"];
    assert_eq!(chain_ids(&a["chainAccounts"]), ["0"], "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true, "{v}");
    let v = exec(&fx, &acct(CARD), [10, 0, 0, 3]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null, "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 39);
    // PR #56 m-4: a failing module does not cost its 20 reads on every request; the cached
    // error still answers flagged.
    *fx.mock.mode.lock().unwrap() = Mode::Chain;
    let v = exec(&fx, &acct(CARD), [10, 0, 0, 3]).await;
    assert_eq!(v["data"]["fungibleAccount"], Value::Null, "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 39, "errors cached");
    // The error entries expire: the next lookup reads again and finds the row.
    tokio::time::sleep(Duration::from_millis(350)).await;
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
        node_rps: 0,
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
        slot_wait: Duration::from_secs(2),
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

/// A query that reads one chain (12) of CARD.
const CARD12: &str =
    "{ fungibleChainAccount(accountName: \"CCAR-0f3a9c1e-55b2-4b7e-9d0a-2c6e8f1b7a44\", chainId: \"12\") { chainId balance } }";

/// PR #56 I-1: with the node rate spent, a lookup is refused at once (no wait for a token),
/// answers store-only and flagged.
#[tokio::test]
async fn a_spent_node_rate_answers_at_once_store_only() {
    let s = MissSettings {
        node_rps: 1,
        node_burst: 20,
        per_ip_rps: 0,
        ..MissSettings::default()
    };
    let fx = fixture(Mode::Chain, s);
    exec(&fx, &acct("k:first-random"), [10, 0, 2, 1]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20);
    let t = std::time::Instant::now();
    let v = exec(&fx, &acct(ALICE), [10, 0, 2, 2]).await;
    assert!(
        t.elapsed() < Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0"]
    );
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert!(fx.mock.calls.load(Ordering::SeqCst) <= 21);
}

/// PR #56 I-1: with every in-flight slot held by hanging reads, another lookup waits at most
/// the slot wait, not the 3 s read timeout.
#[tokio::test]
async fn saturated_slots_answer_within_the_slot_wait() {
    let s = MissSettings {
        max_in_flight: 2,
        ..settings()
    };
    let fx = Arc::new(fixture(Mode::Hang, s));
    let f2 = fx.clone();
    let hung = tokio::spawn(async move { exec(&f2, &acct("k:hangs"), [10, 0, 3, 1]).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(fx.mock.now.load(Ordering::SeqCst), 2, "both slots held");
    let t = std::time::Instant::now();
    let v = exec(&fx, &acct(ALICE), [10, 0, 3, 2]).await;
    assert!(
        t.elapsed() < Duration::from_millis(250),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0"]
    );
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 2);
    hung.abort();
}

/// PR #56 I-1: ten concurrent identical lookups share one node call.
#[tokio::test]
async fn concurrent_identical_lookups_make_one_node_call() {
    let fx = fixture(Mode::Slow, settings());
    let runs = (0..10u8).map(|i| exec(&fx, CARD12, [10, 0, 4, i]));
    let vs = futures::future::join_all(runs).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 1);
    for v in vs {
        assert_eq!(v["data"]["fungibleChainAccount"]["chainId"], "12", "{v}");
        assert!(v["extensions"]["nodeReadIncomplete"].is_null(), "{v}");
    }
    assert_eq!(fx.reader.in_flight(), 0);
}

/// A cancelled first caller does not strand the callers that joined its read; and a read
/// every caller abandons is dropped, its entry and slot released.
#[tokio::test]
async fn a_cancelled_leader_does_not_strand_its_followers() {
    let fx = Arc::new(fixture(Mode::Slow, settings()));
    let f1 = fx.clone();
    let leader = tokio::spawn(async move { exec(&f1, CARD12, [10, 0, 5, 1]).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let f2 = fx.clone();
    let follower = tokio::spawn(async move { exec(&f2, CARD12, [10, 0, 5, 2]).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    leader.abort();
    let v = follower.await.unwrap();
    assert_eq!(v["data"]["fungibleChainAccount"]["chainId"], "12", "{v}");
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fx.reader.in_flight(), 0);
    // Abandoned by its only caller: the entry goes, and the next caller reads afresh.
    let f3 = fx.clone();
    let lone = tokio::spawn(async move { exec(&f3, &acct("k:abandoned"), [10, 0, 5, 3]).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(fx.reader.in_flight() > 0);
    lone.abort();
    let _ = lone.await;
    assert_eq!(fx.reader.in_flight(), 0);
}

/// PR #56 m-7: with the per-client limit on, a request with no peer address gets no node
/// read (no shared fallback bucket); it answers store-only and flagged.
#[tokio::test]
async fn a_request_with_no_peer_address_gets_no_node_read() {
    let s = MissSettings {
        node_rps: 0,
        ..MissSettings::default()
    };
    let fx = fixture(Mode::Chain, s);
    let r = fx
        .schema
        .execute(Request::new(acct(ALICE)).data(MissBudget::default()))
        .await;
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0"]
    );
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 0);
}

/// PR #56 m-3: a client over its rate spends none of the request budget.
#[tokio::test]
async fn a_refused_client_spends_no_request_budget() {
    let s = MissSettings {
        per_ip_rps: 1,
        per_ip_burst: 20,
        node_rps: 0,
        ..MissSettings::default()
    };
    let fx = fixture(Mode::Chain, s);
    exec(&fx, &acct("k:spends-the-bucket"), [10, 0, 6, 1]).await;
    let budget = MissBudget::default();
    let r = fx
        .schema
        .execute(
            Request::new(acct("k:refused"))
                .data(budget.clone())
                .data(ClientIp(IpAddr::from([10, 0, 6, 1]))),
        )
        .await;
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true);
    assert_eq!(budget.used(), 0);
}

/// Chain mask: ALICE has a stored row on chain 0, so a fresh lookup reads the other 19
/// chains (7 found, 18 absent); a repeat within the TTLs reads none. Her absent answers are
/// one cache entry, besides the found row's.
#[tokio::test]
async fn a_fresh_lookup_reads_19_chains_and_a_repeat_none() {
    let fx = fixture(Mode::Chain, settings());
    let v = exec(&fx, &acct(ALICE), [10, 0, 5, 1]).await;
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0", "7"],
        "{v}"
    );
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 19);
    assert_eq!(fx.reader.cached(), 2, "one absent mask + one found row");
    let v = exec(&fx, &acct(ALICE), [10, 0, 5, 1]).await;
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["0", "7"]
    );
    assert_eq!(v["extensions"]["nodeReadIncomplete"], Value::Null, "{v}");
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        19,
        "repeat reads nothing"
    );
}

/// A chain that timed out is not marked absent: the next lookup reads it (and only it).
#[tokio::test]
async fn a_timed_out_chain_is_not_marked_absent_and_is_read_again() {
    let s = MissSettings {
        timeout: Duration::from_millis(200),
        ..settings()
    };
    let fx = fixture(Mode::HangOn(5), s);
    let v = exec(&fx, &acct(ALICE), [10, 0, 5, 2]).await;
    assert_eq!(v["extensions"]["nodeReadIncomplete"], true, "{v}");
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 19);
    *fx.mock.mode.lock().unwrap() = Mode::Chain;
    let v = exec(&fx, &acct(ALICE), [10, 0, 5, 2]).await;
    assert_eq!(v["extensions"]["nodeReadIncomplete"], Value::Null, "{v}");
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        20,
        "chain 5 read again"
    );
    exec(&fx, &acct(ALICE), [10, 0, 5, 2]).await;
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        20,
        "now cached absent"
    );
}

/// Memory per account is O(1): accounts absent on every chain cost one entry each, not 20.
#[tokio::test]
async fn absent_accounts_cost_one_cache_entry_each() {
    let fx = fixture(Mode::Chain, settings());
    for i in 0..5u8 {
        let v = exec(&fx, &acct(&format!("k:nobody-{i}")), [10, 0, 5, 3]).await;
        assert_eq!(v["data"]["fungibleAccount"], Value::Null, "{v}");
    }
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 100);
    assert_eq!(fx.reader.cached(), 5);
}

/// Absent bits expire with their TTL: after it the lookup reads every chain again.
#[tokio::test]
async fn absent_answers_expire_after_their_ttl() {
    let s = MissSettings {
        ttl_absent: Duration::from_millis(300),
        ..settings()
    };
    let fx = fixture(Mode::Chain, s);
    exec(&fx, &acct("k:nobody-ttl"), [10, 0, 5, 4]).await;
    exec(&fx, &acct("k:nobody-ttl"), [10, 0, 5, 4]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 20);
    tokio::time::sleep(Duration::from_millis(350)).await;
    exec(&fx, &acct("k:nobody-ttl"), [10, 0, 5, 4]).await;
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 40);
}

/// Secpass B I-1: at the default limits one client, at its maximum rate, cannot spend the
/// node budget other clients need: another client's fresh lookup is still read in full.
#[tokio::test]
async fn one_client_at_its_maximum_rate_leaves_another_lookup_fully_read() {
    let fx = fixture(Mode::Chain, MissSettings::default());
    let hostile = [10, 0, 9, 1];
    let start = std::time::Instant::now();
    let mut i = 0u32;
    while start.elapsed() < Duration::from_millis(1_200) {
        exec(&fx, &acct(&format!("k:hostile-{i}")), hostile).await;
        i += 1;
    }
    let v = exec(&fx, &acct(CARD), [10, 0, 9, 2]).await;
    assert_eq!(v.get("extensions"), None, "fully read: {v}");
    assert_eq!(
        chain_ids(&v["data"]["fungibleAccount"]["chainAccounts"]),
        ["12"],
        "{v}"
    );
}

/// Secpass B I-1: cache churn from one client is bounded by its own read limit (each cache
/// insert is one of its node reads), so one client cannot rotate the cache's generations.
#[tokio::test]
async fn cache_churn_from_one_client_is_bounded() {
    let s = MissSettings::default();
    let fx = fixture(Mode::Chain, s.clone());
    let start = std::time::Instant::now();
    for i in 0..300u32 {
        let q = format!(
            "{{ fungibleChainAccount(chainId: \"3\", accountName: \"k:churn-{i}\") {{ balance }} }}"
        );
        exec(&fx, &q, [10, 0, 9, 3]).await;
    }
    let secs = start.elapsed().as_secs_f64();
    let bound = f64::from(s.per_ip_burst) + f64::from(s.per_ip_rps) * secs + 1.0;
    let cached = fx.reader.cached();
    assert!(
        (cached as f64) <= bound && cached <= 25,
        "{cached} entries in {secs:.2}s"
    );
    // The default read rate cannot turn a generation over within the absent TTL.
    let turnover = (s.cache_entries / 2) as f64 / f64::from(s.per_ip_rps);
    assert!(turnover >= s.ttl_absent.as_secs_f64(), "{turnover}");
}

/// `Block.minerAccount` as stock answers it (`(coin.details "<miner>")` live, zero on any
/// failure): the stored row; else the node's row, read once and cached; else balance 0 with
/// a `raw: "null"` guard, for a name the node has no row for, a name that is no account
/// (never sent to the node) and a failed read (flagged), never an error.
#[tokio::test]
async fn miner_account_reads_the_node_on_a_miss_and_answers_zero_as_stock() {
    let fx = fixture(Mode::Chain, settings());
    let zero = |c: u8, a: &str| {
        json!({"accountName": a, "chainId": c.to_string(), "balance": 0.0,
               "guard": {"__typename": "RawGuard", "raw": "null"}})
    };
    let v = exec(&fx, &miner_q(0), [10, 0, 0, 1]).await;
    assert_eq!(
        v["data"]["block"]["minerAccount"]["balance"],
        json!(5.0),
        "{v}"
    );
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        0,
        "stored row: no read"
    );
    let v = exec(&fx, &miner_q(12), [10, 0, 0, 1]).await;
    assert_eq!(
        v["data"]["block"]["minerAccount"],
        json!({"accountName": CARD, "chainId": "12", "balance": 0.0,
               "guard": {"__typename": "KeysetGuard",
                         "raw": format!("{{\"keys\":[\"{KEY}\"],\"pred\":\"keys-all\"}}")}}),
        "{v}"
    );
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 1);
    let v = exec(&fx, &miner_q(3), [10, 0, 0, 1]).await;
    assert_eq!(
        v["data"]["block"]["minerAccount"],
        zero(3, "k:nobody"),
        "{v}"
    );
    assert_eq!(fx.mock.calls.load(Ordering::SeqCst), 2);
    let v = exec(&fx, &miner_q(4), [10, 0, 0, 1]).await;
    assert_eq!(v["data"]["block"]["minerAccount"], zero(4, "ab"), "{v}");
    assert_eq!(
        fx.mock.calls.load(Ordering::SeqCst),
        2,
        "no account name: no read"
    );
    assert!(v.get("extensions").is_none(), "{v}");

    let fx = fixture(Mode::Error, settings());
    let v = exec(&fx, &miner_q(12), [10, 0, 0, 1]).await;
    assert_eq!(v["data"]["block"]["minerAccount"], zero(12, CARD), "{v}");
    assert_eq!(v["extensions"]["nodeReadIncomplete"], json!(true), "{v}");
}
