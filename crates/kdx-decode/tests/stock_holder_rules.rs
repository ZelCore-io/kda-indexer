//! Secpass A I-4 / m-7: the holders sides follow stock's `getCoinTransfers`,
//! `getNftTransfers` and `payload.ts` marmalade rules exactly (indexer-kadena
//! `utils/transfers.ts`, `services/payload.ts`).

use kdx_decode::build::{holder_sides, parse_event, stock_amount_ok, transfer_amount_ok};
use serde_json::{json, Value};

fn ev(module: &str, name: &str, params: Value) -> Value {
    let (ns, n) = match module.rsplit_once('.') {
        Some((a, b)) => (Value::from(a), b),
        None => (Value::Null, module),
    };
    json!({"params": params, "name": name, "module": {"namespace": ns, "name": n},
        "moduleHash": "h"})
}

fn sides(e: &Value) -> Vec<String> {
    let ev = parse_event(e).expect("canonical event");
    let mut out = Vec::new();
    holder_sides(&ev, &mut out);
    out.into_iter().map(str::to_owned).collect()
}

/// Stock's `parseAmount`: a JSON number, `{decimal: <non-empty string>}` or
/// `{integer: <non-empty string>}` (either member, the other ignored). Everything
/// chainweb-data also takes (a plain string, `{int: ..}`, `{decimal: <number>}`) is not one.
#[test]
fn the_amount_check_is_stocks() {
    for ok in [
        json!(1),
        json!(1.5),
        json!(0),
        json!({"decimal": "1.0"}),
        json!({"integer": "5"}),
        json!({"decimal": "0"}),
        json!({"decimal": 1, "integer": "5"}),
        json!({"decimal": "x"}),
    ] {
        assert!(stock_amount_ok(&ok), "{ok}");
    }
    for bad in [
        json!("1.0"),
        json!({"int": 5}),
        json!({"int": "5"}),
        json!({"decimal": 1.0}),
        json!({"decimal": ""}),
        json!({"integer": 5}),
        json!({"integer": ""}),
        json!(null),
        json!(true),
        json!([1]),
    ] {
        assert!(!stock_amount_ok(&bad), "{bad}");
    }
    // chainweb-data's rule (the `xfer` postings) is unchanged.
    assert!(transfer_amount_ok(&json!("1.0")) && transfer_amount_ok(&json!({"int": 5})));
}

#[test]
fn a_transfer_gives_holder_sides_only_with_a_stock_amount() {
    let t = |amt: Value| sides(&ev("coin", "TRANSFER", json!(["k:a", "k:b", amt])));
    assert_eq!(t(json!(1.0)), ["k:a", "k:b"]);
    assert_eq!(t(json!({"decimal": "1.0"})), ["k:a", "k:b"]);
    assert_eq!(t(json!({"integer": "1"})), ["k:a", "k:b"]);
    assert!(t(json!({"int": 5})).is_empty());
    assert!(t(json!("1.0")).is_empty());
    assert!(t(json!({"decimal": 1.0})).is_empty());
    // Exact name only.
    assert!(sides(&ev("free.wiz", "WIZ_TRANSFER", json!(["k:a", "k:b", 1.0]))).is_empty());
}

#[test]
fn a_reconcile_gives_holder_sides_only_with_a_stock_amount() {
    let r = |amt: Value| {
        sides(&ev(
            "marmalade-v2.ledger",
            "RECONCILE",
            json!(["t:1", amt, {"account": "k:a"}, {"account": "k:b"}]),
        ))
    };
    assert_eq!(r(json!(1)), ["k:a", "k:b"]);
    assert_eq!(r(json!({"decimal": "1.0"})), ["k:a", "k:b"]);
    assert!(r(json!("1.0")).is_empty());
    assert!(r(json!({"int": 1})).is_empty());
}

/// m-7: stock's marmalade rule takes any truthy token id (`account && tokenId`): a non-empty
/// string, a non-zero number, `true`, an object or an array. A falsy one (`""`, `0`, `false`,
/// `null`) gives no row.
#[test]
fn marmalade_ledger_rows_take_any_truthy_token_id() {
    let mint = |id: Value| {
        sides(&ev(
            "marmalade-v2.ledger",
            "MINT",
            json!([id, "k:minter", 1.0]),
        ))
    };
    for truthy in [
        json!("t:1"),
        json!(5),
        json!(-1.5),
        json!(true),
        json!({}),
        json!([]),
    ] {
        assert_eq!(mint(truthy.clone()), ["k:minter"], "{truthy}");
    }
    for falsy in [json!(""), json!(0), json!(0.0), json!(false), json!(null)] {
        assert!(mint(falsy.clone()).is_empty(), "{falsy}");
    }
    let xfer = sides(&ev(
        "marmalade.ledger",
        "TRANSFER",
        json!([7, "k:from", "k:to", 1.0]),
    ));
    assert_eq!(xfer, ["k:to"]);
}
