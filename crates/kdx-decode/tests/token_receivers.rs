//! D3: balance receivers no TRANSFER names, for every fungible module (coin included). Shapes
//! follow the KDA-03 post-D2 evidence (account names replaced):
//! - (a) chain 0, height 49,220: an injected miner account
//!   `<key>" (read-keyset "miner-keyset") 23045.23)(coin.coinbase "<key>`; the coinbase credits
//!   `<key>`, a name the block never states on its own;
//! - (b) chain 0, height 1,129,925: `(user.utxo1.tx ..)` below the events fork, the two
//!   receivers only in `result.data`;
//! - (c) token cross-chain finishes (`free.wiza.transfer-crosschain` step 1, only a
//!   `pact.X_RESUME` event), `kaddex.kdx.UNWRAP` credits with no TRANSFER;
//! - (d) `kaddex.alchemist.WRAP` minting `kaddex.skdx`, named only by a `fungible-v2` modref.

use kdx_decode::receivers::{
    injected_miner_receivers, token_receivers, MAINNET01, MAX_EVENT_PAIRS_PER_TX,
};
use serde_json::{json, Value};

const KEY: &str = "4069e72f62deb9dde8ee0686ee2908580131128b0a350e23890e9b46e61f166d";
const R1: &str = "8ac30e497031ba4eeee9ff2b2c240c5e4f394d82bad1d7b108613a02c062a4c2";
const R2: &str = "82dee4edb188765f45c2e26d53cf03da27f9451af03ede10b307bd19f75be205";
const ACC: &str = "k:0189b768305ec1a404b1579091bfc1e2eae68f3d1444eea0c207de7efe2a7846";
const SRC: &str = "k:c47b68281941d166ea7cab675ac08a16586d5b8064576f810db9b464c268a096";

fn tokens(m: &str) -> bool {
    matches!(m, "free.wiza" | "kaddex.kdx" | "free.anedak")
}

fn ev(module: &str, name: &str, params: Value) -> Value {
    let (ns, n) = match module.split_once('.') {
        Some((a, b)) => (Value::from(a), b),
        None => (Value::Null, module),
    };
    json!({"params": params, "name": name, "module": {"namespace": ns, "name": n},
        "moduleHash": "h"})
}

fn out(data: Value, events: Value, cont: Value) -> Value {
    json!({"gas": 1, "result": {"status": "success", "data": data}, "reqKey": "r", "logs": "l",
        "events": events, "metaData": null, "continuation": cont, "txId": 1})
}

fn pairs(v: &[(String, String)]) -> Vec<(&str, &str)> {
    v.iter().map(|(a, m)| (a.as_str(), m.as_str())).collect()
}

#[test]
fn an_injected_miner_account_gives_the_credited_key() {
    let name = format!(r#"{KEY}" (read-keyset "miner-keyset") 23045.23)(coin.coinbase "{KEY}"#);
    let got = injected_miner_receivers(&name);
    assert!(got.contains(&KEY.to_owned()), "{got:?}");
    assert!(!got.contains(&name));
    // A plain miner account is its own posting: nothing extra.
    assert!(injected_miner_receivers(KEY).is_empty());
    assert!(injected_miner_receivers("k:abc").is_empty());
}

#[test]
fn receivers_in_the_result_of_a_pre_event_exec_count_for_coin() {
    let cmd = json!({"payload": {"exec": {"data": {"k": ["b6f4"]},
        "code": "(user.utxo1.tx 1606279502 coin [\"b6f4\"] [{ 'g: (read-keyset 'k), 'q: 2.0 }])"}}});
    let o = out(json!([R1, R2]), json!([]), Value::Null);
    let r = token_receivers(Some(&cmd), &o, 1_129_925, MAINNET01, &tokens);
    let p = pairs(&r.pairs);
    assert!(
        p.contains(&(R1, "coin")) && p.contains(&(R2, "coin")),
        "{p:?}"
    );
    // From the events fork on, an exec's coin credits have events: nothing from the result.
    let r = token_receivers(Some(&cmd), &o, 1_138_000, MAINNET01, &tokens);
    assert!(!pairs(&r.pairs).contains(&(R1, "coin")));
}

#[test]
fn a_pre_event_exec_counts_for_the_tokens_its_code_calls() {
    let cmd = json!({"payload": {"exec": {"data": {},
        "code": format!(r#"(free.anedak.transfer-create "{SRC}" "{ACC}" (read-keyset "ks") 1.0)"#)}}});
    let o = out(json!("Write succeeded"), json!([]), Value::Null);
    let r = token_receivers(Some(&cmd), &o, 927_225, MAINNET01, &tokens);
    let all: Vec<(String, String)> = r.pairs.iter().chain(&r.event_pairs).cloned().collect();
    let p = pairs(&all);
    assert!(
        p.contains(&(ACC, "free.anedak")) && p.contains(&(ACC, "coin")),
        "{p:?}"
    );
    // A module the code calls that is not a known token gives nothing for it.
    assert!(!p.iter().any(|(_, m)| *m == "user.utxo1"));
}

#[test]
fn a_token_cross_chain_finish_names_its_receiver_and_the_start_its_sender() {
    let cmd = json!({"payload": {"cont": {"pactId": "p", "step": 1, "rollback": false,
        "data": {}, "proof": "x"}}});
    let cont = |step: u64| {
        json!({"pactId": "p", "step": step, "stepCount": 2, "executed": null,
        "stepHasRollback": false, "yield": null,
        "continuation": {"def": "free.wiza.transfer-crosschain",
            "args": [SRC, ACC, {"pred": "keys-all", "keys": ["ba55"]}, "1", 5.0]}})
    };
    let resume = json!([ev(
        "pact",
        "X_RESUME",
        json!(["1", "free.wiza.transfer-crosschain",
        [SRC, ACC, {"pred": "keys-all", "keys": ["ba55"]}, "1", 5.0]])
    )]);
    let r = token_receivers(
        Some(&cmd),
        &out(json!("ok"), resume, cont(1)),
        5_371_851,
        MAINNET01,
        &tokens,
    );
    assert_eq!(pairs(&r.pairs), vec![(ACC, "free.wiza")]);
    assert_eq!(r.discovered, vec!["free.wiza".to_owned()]);
    // Step 0 (on the source chain) debits the sender without a TRANSFER.
    let start =
        json!({"payload": {"exec": {"data": {}, "code": "(free.wiza.transfer-crosschain ..)"}}});
    let r = token_receivers(
        Some(&start),
        &out(json!("ok"), json!([]), cont(0)),
        5_371_800,
        MAINNET01,
        &tokens,
    );
    assert!(
        pairs(&r.pairs).contains(&(SRC, "free.wiza")),
        "{:?}",
        r.pairs
    );
}

#[test]
fn non_transfer_events_of_a_token_name_its_credited_accounts() {
    let cmd = json!({"payload": {"exec": {"data": {}, "code": "(kaddex.staking.claim ..)"}}});
    let evs = json!([
        ev(
            "coin",
            "TRANSFER",
            json!(["kaddex-free-gas", "miner", 0.0005])
        ),
        ev(
            "kaddex.kdx",
            "UNWRAP",
            json!(["kaddex.skdx", "kdx-staking", ACC, 17.2])
        ),
        ev("kaddex.kdx", "TRANSFER", json!([SRC, "k:zz", 1.0])),
        ev(
            "kaddex.aggregator",
            "PRIVILEGE-GUARD",
            json!(["aggregate-unstake"])
        ),
    ]);
    let r = token_receivers(
        Some(&cmd),
        &out(json!(true), evs, Value::Null),
        3_635_131,
        MAINNET01,
        &tokens,
    );
    let p = pairs(&r.event_pairs);
    assert!(
        p.contains(&(ACC, "kaddex.kdx")) && p.contains(&("kdx-staking", "kaddex.kdx")),
        "{p:?}"
    );
    // A well-formed TRANSFER already marks its sides; coin and non-token events give nothing.
    assert!(!p.contains(&(SRC, "kaddex.kdx")));
    assert!(
        !p.iter()
            .any(|(_, m)| *m == "coin" || *m == "kaddex.aggregator"),
        "{p:?}"
    );
}

#[test]
fn a_fungible_modref_in_an_event_names_and_discovers_its_module() {
    let skdx = json!({"refSpec": [{"namespace": null, "name": "fungible-v2"},
        {"namespace": "kaddex", "name": "supply-control-v1"}],
        "refName": {"namespace": "kaddex", "name": "skdx"}});
    let nft = json!({"refSpec": [{"namespace": "kip", "name": "poly-fungible-v2"}],
        "refName": {"namespace": "free", "name": "nft"}});
    let cmd = json!({"payload": {"exec": {"data": {}, "code": "(kaddex.staking.stake ..)"}}});
    let evs = json!([
        ev("kaddex.alchemist", "WRAP", json!([skdx, 1778.79, ACC, ACC])),
        ev("free.market", "SOLD", json!([nft, SRC])),
    ]);
    let r = token_receivers(
        Some(&cmd),
        &out(json!("ok"), evs, Value::Null),
        4_901_480,
        MAINNET01,
        &tokens,
    );
    assert_eq!(pairs(&r.event_pairs), vec![(ACC, "kaddex.skdx")]);
    assert_eq!(r.discovered, vec!["kaddex.skdx".to_owned()]);
}

#[test]
fn an_exec_calling_a_token_counts_for_it_at_any_height() {
    // A token credit with no event of the token at all (only the gas TRANSFER), after the
    // events fork: the code calls the token, the receiver is a literal.
    let cmd = json!({"payload": {"exec": {"data": {"r": SRC},
        "code": format!(r#"(free.anedak.mint "{ACC}" 10.0)"#)}}});
    let evs = json!([ev("coin", "TRANSFER", json!([SRC, "miner", 0.0001]))]);
    let o = out(json!("Write succeeded"), evs, Value::Null);
    let r = token_receivers(Some(&cmd), &o, 4_000_000, MAINNET01, &tokens);
    let all: Vec<(String, String)> = r.pairs.iter().chain(&r.event_pairs).cloned().collect();
    let p = pairs(&all);
    assert!(
        p.contains(&(ACC, "free.anedak")) && p.contains(&(SRC, "free.anedak")),
        "{p:?}"
    );
    // Above the events fork coin credits have events: no coin candidates from an exec.
    assert!(!p.iter().any(|(_, m)| *m == "coin"), "{p:?}");
}

fn modref(ns: Option<&str>, name: &str) -> Value {
    json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}],
        "refName": {"namespace": ns, "name": name}})
}

#[test]
fn a_hostile_event_is_capped_and_never_gives_coin_rows() {
    let cmd = json!({"payload": {"exec": {"data": {}, "code": "(free.evil.go)"}}});
    let strings: Vec<Value> = (0..1000)
        .map(|i| json!(format!("k:victim-{i:04}")))
        .collect();
    let mut refs: Vec<Value> = (0..100)
        .map(|i| modref(Some("free"), &format!("tok{i}")))
        .collect();
    refs.push(modref(None, "coin"));
    // An untracked module in an open namespace referencing other modules: nothing at all.
    let evs = json!([ev("free.evil", "SPAM", json!([refs, strings]))]);
    let o = out(json!("ok"), evs, Value::Null);
    let r = token_receivers(Some(&cmd), &o, 5_000_000, MAINNET01, &tokens);
    assert!(
        r.pairs.is_empty() && r.event_pairs.is_empty() && r.discovered.is_empty(),
        "{r:?}"
    );
    // A tracked token's event: at most the per-tx cap, and a coin modref gives no coin rows.
    let evs = json!([ev("free.wiza", "SPAM", json!([refs, strings]))]);
    let o = out(json!("ok"), evs, Value::Null);
    let r = token_receivers(Some(&cmd), &o, 5_000_000, MAINNET01, &tokens);
    assert_eq!(
        r.event_pairs.len(),
        MAX_EVENT_PAIRS_PER_TX,
        "{}",
        r.event_pairs.len()
    );
    assert!(!r.event_pairs.iter().any(|(_, m)| m == "coin"));
    assert!(!r.discovered.iter().any(|m| m == "coin"));
}

#[test]
fn a_modref_counts_only_from_its_module_a_token_or_its_governed_namespace() {
    let cmd = json!({"payload": {"exec": {"data": {}, "code": "(x)"}}});
    let run = |emitter: &str, (ns, n): (Option<&str>, &str)| {
        let evs = json!([ev(emitter, "WRAP", json!([modref(ns, n), 1.0, ACC]))]);
        token_receivers(
            Some(&cmd),
            &out(json!("ok"), evs, Value::Null),
            5_000_000,
            MAINNET01,
            &tokens,
        )
    };
    // kaddex.alchemist mints kaddex.skdx: same governed namespace.
    assert_eq!(
        pairs(&run("kaddex.alchemist", (Some("kaddex"), "skdx")).event_pairs),
        vec![(ACC, "kaddex.skdx")]
    );
    // The referenced module itself.
    assert_eq!(
        run("free.gold", (Some("free"), "gold")).event_pairs.len(),
        1
    );
    // Anyone in `free` naming another `free` token: nothing.
    let r = run("free.evil", (Some("free"), "gold"));
    assert!(r.event_pairs.is_empty() && r.discovered.is_empty(), "{r:?}");
}

#[test]
fn a_hostile_exec_stays_within_the_cap_and_failed_txs_give_nothing() {
    // Many token calls and strings in one exec: at most the per-tx cap of token rows.
    let mut code = String::new();
    for i in 0..200 {
        code.push_str(&format!(
            r#"(free.wiza.f "k:victim-{i:04}") (kaddex.kdx.f "k:other-{i:04}") "#
        ));
    }
    let cmd = json!({"payload": {"exec": {"data": {}, "code": code}}});
    let o = out(json!("ok"), json!([]), Value::Null);
    let r = token_receivers(Some(&cmd), &o, 5_000_000, MAINNET01, &tokens);
    assert_eq!(r.event_pairs.len(), MAX_EVENT_PAIRS_PER_TX);
    assert_eq!(r.event_emitters.len(), r.event_pairs.len());
    assert!(
        r.pairs.is_empty(),
        "token exec rows are all capped: {:?}",
        r.pairs.len()
    );
    // A module named only inside a literal or a comment is not a call.
    let cmd = json!({"payload": {"exec": {"data": {},
        "code": format!("(user.f \"free.wiza.x\" \"{ACC}\") ; free.wiza.y\n")}}});
    let r = token_receivers(Some(&cmd), &o, 5_000_000, MAINNET01, &tokens);
    assert!(r.event_pairs.is_empty(), "{:?}", r.event_pairs);
    // A failed tx gives nothing, whatever it holds.
    let cmd =
        json!({"payload": {"exec": {"data": {}, "code": format!(r#"(free.wiza.mint "{ACC}")"#)}}});
    let mut failed = out(json!({"message": "boom"}), json!([]), Value::Null);
    failed["result"]["status"] = json!("failure");
    let r = token_receivers(Some(&cmd), &failed, 900_000, MAINNET01, &tokens);
    assert!(r.pairs.is_empty() && r.event_pairs.is_empty(), "{r:?}");
}
