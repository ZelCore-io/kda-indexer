//! GQ-15: `NonFungibleTokenBalance.guard` and `.info` answer from `NftDetails`; a holding with
//! no usable details gets an error on that field only, never a `null` account.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use kdx_api::{Api, ApiSettings, Store, TipWatch};
use kdx_graphql::{build_schema, router, GraphqlSettings};
use kdx_hot::nft::{self, HolderDetails, TokenDetails};
use kdx_hot::HotDict;
use kdx_types::{DictKind, DictMut};
use serde_json::{json, Value};
use tower::ServiceExt;

const V2: &str = "marmalade-v2.ledger";
const V1: &str = "marmalade.ledger";

/// One holder with four holdings at height 10:
/// - `t:1` (v2): guard and info read at 20;
/// - `t:2` (v2): nothing read yet;
/// - `t:3` (v1): guard read at 5, before the row moved (stale), and no `get-token-info`;
/// - `t:4` (v2): the node had no ledger row, and its info call failed.
async fn answer(q: &str) -> Value {
    let d = tempfile::tempdir().unwrap();
    let g = kdx_hot::Geometry {
        max_gb: 1,
        growth_mb: 16,
    };
    let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
    {
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let a = dict.intern(DictKind::Account, "k:holder");
        let v2 = dict.intern(DictKind::Module, V2);
        let v1 = dict.intern(DictKind::Module, V1);
        dict.persist(&mut txn).unwrap();
        let guard = r#"{"keys":["k1"],"pred":"keys-all"}"#;
        for (ledger, t) in [(v2, "t:1"), (v2, "t:2"), (v1, "t:3"), (v2, "t:4")] {
            nft::upsert_event(&mut txn, a, t, 0, ledger, 10, "1.0").unwrap();
        }
        let key = |ledger, t: &str| nft::nft_key(a, ledger, &nft::token_hash(t), 0);
        let holder = |status, as_of, guard: &str| HolderDetails {
            status,
            as_of,
            guard: guard.into(),
        };
        let k = key(v2, "t:1");
        nft::put_holder_details(&mut txn, &k, &holder(nft::DETAIL_FOUND, 20, guard)).unwrap();
        let info = TokenDetails {
            status: nft::DETAIL_FOUND,
            as_of: 20,
            precision: 0,
            supply: "1.0".into(),
            uri: "ipfs://x".into(),
        };
        nft::put_token_details(&mut txn, &nft::token_details_key_of(&k), &info).unwrap();
        let k = key(v1, "t:3");
        nft::put_holder_details(&mut txn, &k, &holder(nft::DETAIL_FOUND, 5, guard)).unwrap();
        let undefined = TokenDetails::empty(nft::DETAIL_UNDEFINED, 10);
        nft::put_token_details(&mut txn, &nft::token_details_key_of(&k), &undefined).unwrap();
        let k = key(v2, "t:4");
        nft::put_holder_details(&mut txn, &k, &holder(nft::DETAIL_ABSENT, 10, "")).unwrap();
        let failed = TokenDetails::empty(nft::DETAIL_FAILED, 10);
        nft::put_token_details(&mut txn, &nft::token_details_key_of(&k), &failed).unwrap();
        txn.commit().unwrap();
    }
    let store = Arc::new(Store::open(hot, d.path()).unwrap());
    let settings = ApiSettings {
        network: "mainnet01".into(),
        deadline: Duration::from_secs(5),
        max_in_flight: 16,
        max_body_bytes: 8 << 20,
        v1txs_max_body_bytes: 8 << 20,
        rate_per_ip: 20,
        rate_burst: 40,
        trusted_proxies: Vec::new(),
        read_threads: 2,
        ready_max_lag: 10,
        max_event_lists: 64,
    };
    let st = Api::new(store, settings, TipWatch::default()).state();
    let app = router(build_schema(st.clone(), GraphqlSettings::default(), None)).with_state(st);
    let req = Request::post("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": q }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn nft_guard_and_info_resolve_and_missing_details_error_per_field() {
    let v = answer(
        "{ nonFungibleAccount(accountName: \"k:holder\") { accountName \
           nonFungibleTokenBalances { tokenId version balance \
             guard { raw ... on KeysetGuard { keys predicate } } \
             info { precision supply uri } } } }",
    )
    .await;
    let acct = &v["data"]["nonFungibleAccount"];
    assert_eq!(acct["accountName"], "k:holder", "{v}");
    let items = acct["nonFungibleTokenBalances"].as_array().unwrap();
    assert_eq!(items.len(), 4, "{v}");
    let at = |t: &str| {
        items
            .iter()
            .position(|i| i["tokenId"] == t)
            .unwrap_or_else(|| panic!("{t} missing: {v}"))
    };
    // Read details answer.
    let i1 = &items[at("t:1")];
    assert_eq!(i1["guard"]["keys"], json!(["k1"]), "{v}");
    assert_eq!(i1["guard"]["predicate"], "keys-all");
    assert_eq!(i1["guard"]["raw"], r#"{"keys":["k1"],"pred":"keys-all"}"#);
    assert_eq!(
        i1["info"],
        json!({"precision": 0, "supply": 1, "uri": "ipfs://x"})
    );
    // Stale details are served; a v1 token has no info and no error.
    let i3 = &items[at("t:3")];
    assert_eq!(
        (i3["version"].as_str(), &i3["guard"]["keys"]),
        (Some("v1"), &json!(["k1"]))
    );
    assert_eq!(i3["info"], Value::Null);
    // Missing and unusable details: an empty guard and a null info beside a per-field error.
    for t in ["t:2", "t:4"] {
        let i = &items[at(t)];
        assert_eq!(i["guard"]["raw"], "", "{v}");
        assert!(i["guard"].get("keys").is_none(), "not a KeysetGuard: {v}");
        assert_eq!(i["info"], Value::Null);
        assert_eq!(i["balance"], 1);
    }
    let errors = v["errors"].as_array().unwrap();
    let paths: Vec<(Value, String)> = errors
        .iter()
        .map(|e| {
            assert_eq!(e["extensions"]["code"], "NOT_AVAILABLE", "{e}");
            (e["path"].clone(), e["message"].as_str().unwrap().to_owned())
        })
        .collect();
    assert_eq!(paths.len(), 4, "{v}");
    let msg = |t: &str, f: &str| {
        let p = json!(["nonFungibleAccount", "nonFungibleTokenBalances", at(t), f]);
        paths
            .iter()
            .find(|(x, _)| *x == p)
            .map(|(_, m)| m.clone())
            .unwrap_or_else(|| panic!("no error at {p}: {v}"))
    };
    assert!(msg("t:2", "guard").ends_with("not read from the node yet"));
    assert!(msg("t:2", "info").ends_with("not read from the node yet"));
    assert!(msg("t:4", "guard").ends_with("the ledger has no such row"));
    assert!(msg("t:4", "info").ends_with("could not be used"));

    // The chain view answers the same way.
    let v = answer(
        "{ nonFungibleChainAccount(accountName: \"k:holder\", chainId: \"0\") { chainId \
           nonFungibleTokenBalances { tokenId guard { raw } } } }",
    )
    .await;
    let items = v["data"]["nonFungibleChainAccount"]["nonFungibleTokenBalances"]
        .as_array()
        .unwrap_or_else(|| panic!("{v}"));
    assert_eq!(items.len(), 4);
    assert_eq!(v["errors"].as_array().map(Vec::len), Some(2), "{v}");
}
