//! `dex.rs` tests: exact amounts, decoding (a real chain-2 Sushi `SWAP` and stock's param
//! layouts), trust, bounds, and apply / unwind / fill exactness against a replay model.

use kdx_decode::{IndexedBlock, TxIndex};
use kdx_types::record::{BlockRecord, EventRec};
use kdx_types::{value, DictMut, Hash32, MemDict};
use proptest::prelude::*;
use serde_json::{json, Value};

use super::*;
use crate::{Geometry, HotDict, HotStore};

const H: u32 = SUSHI_DEPLOY_HEIGHT + 1_000;
/// One hour of block time, µs.
const HOUR: u64 = 3_600_000_000;

fn mref(ns: Option<&str>, name: &str) -> Value {
    json!({"refSpec": [{"namespace": null, "name": "fungible-v2"}], "refName": {"namespace": ns, "name": name}})
}

fn tok(q: &str) -> Value {
    match q.rsplit_once('.') {
        Some((ns, n)) => mref(Some(ns), n),
        None => mref(None, q),
    }
}

/// A real chain-2 Sushi `SWAP` (height 7,326,575, request key
/// `-pO8z7iyzSH9tk1VpcACSqhYdJQaWby0zqwiRCwUnis`; the same capture as
/// `kdx-graphql/tests/data/token-swaps-2026-11-24.jsonl`): free.wiza in, coin out.
fn real_swap() -> Value {
    let acct = "k:f87a7d0b5c81ebba21e975fe3c4cb04cdc478ec15f421d0170b91947436d7fa2";
    json!([
        acct,
        acct,
        388.578959440305,
        {"refSpec":[{"namespace":null,"name":"fungible-v2"},{"namespace":"free","name":"wiza1-interface-v4"}],"refName":{"namespace":"free","name":"wiza"}},
        0.31738093163,
        {"refSpec":[{"namespace":null,"name":"fungible-v2"},{"namespace":null,"name":"fungible-xchain-v1"}],"refName":{"namespace":null,"name":"coin"}}
    ])
}

fn ev(dict: &mut MemDict, kind: Kind, params: &Value) -> EventRec {
    let (module, name) = kind.module_and_name();
    EventRec {
        module: dict.intern(DictKind::Module, module),
        name: dict.intern(DictKind::EventName, &format!("{module}.{name}")),
        module_hash: 0,
        params: value::to_vec(params, &*dict),
    }
}

fn tx(key: TxKey, events: Vec<EventRec>) -> TxRecord {
    TxRecord {
        key,
        status_ok: true,
        events,
        ..Default::default()
    }
}

fn store() -> (tempfile::TempDir, HotStore) {
    let d = tempfile::tempdir().unwrap();
    let hot = HotStore::open(d.path(), Geometry::default()).unwrap();
    (d, hot)
}

type Snapshot = Vec<(Table, Vec<(Vec<u8>, Vec<u8>)>)>;

fn snapshot(txn: &impl Txn) -> Snapshot {
    TABLES
        .iter()
        .map(|&t| (t, txn.scan(t, &[], |_| true).unwrap()))
        .collect()
}

fn a(s: &str) -> Amount {
    Amount::parse(s).unwrap_or_else(|| panic!("{s}"))
}

#[test]
fn amounts_are_exact() {
    for (i, o) in [
        ("388.578959440305", "388.578959440305"),
        ("0.31738093163", "0.31738093163"),
        ("1e-5", "0.00001"),
        ("1.5E+3", "1500"),
        ("-0", "0"),
        ("-0.000", "0"),
        (".5", "0.5"),
        ("5.", "5"),
        ("007.10", "7.1"),
        ("1.0000000000000000000000000000", "1"),
        ("0.000000000000000000000001", "0.000000000000000000000001"),
        (
            "123456789012345678901234567890",
            "123456789012345678901234567890",
        ),
    ] {
        assert_eq!(a(i).to_text(), o, "{i}");
    }
    for bad in [
        "",
        "-",
        ".",
        "abc",
        "1e",
        "1e65",
        "1e-65",
        "1e99999",
        "--1",
        "1.2.3",
        " 1",
        "0x10",
        "0.0000000000000000000000001", // 25 places, not exact at SCALE
        &"9".repeat(MAX_AMOUNT_TEXT + 1),
    ] {
        assert_eq!(Amount::parse(bad), None, "{bad:?}");
    }
    assert_eq!(a("0.1").add(&a("0.2"), false).to_text(), "0.3");
    assert_eq!(a("1").add(&a("2"), true).to_text(), "-1");
    assert_eq!(a("-1").add(&a("-2"), true).to_text(), "1");
    assert_eq!(
        a("999999999.999999999999999999999999")
            .add(&a("0.000000000000000000000001"), false)
            .to_text(),
        "1000000000"
    );
    assert!(a("5").add(&a("5"), true).is_zero());
    // Huge values never overflow: 10^40 + 10^40.
    assert_eq!(
        a("1e40").add(&a("1e40"), false).to_text(),
        format!("2{}", "0".repeat(40))
    );
    // Event amounts: numbers, {decimal}, {int}; negatives refused.
    assert_eq!(amount_of(&json!(1.25)).unwrap().to_text(), "1.25");
    assert_eq!(
        amount_of(&json!({"decimal": "0.5"})).unwrap().to_text(),
        "0.5"
    );
    assert_eq!(amount_of(&json!({"int": 7})).unwrap().to_text(), "7");
    assert_eq!(amount_of(&json!(-1)), None);
    assert_eq!(amount_of(&json!({"decimal": "-0.5"})), None);
    assert_eq!(amount_of(&json!("1")), None);
}

#[test]
fn decodes_a_real_sushi_swap() {
    let e = decode_event(Kind::Swap, &real_swap()).unwrap();
    assert_eq!(e.pair, "coin:free.wiza");
    let acct = "k:f87a7d0b5c81ebba21e975fe3c4cb04cdc478ec15f421d0170b91947436d7fa2";
    // token0 = coin (out), token1 = free.wiza (in).
    assert_eq!(
        e.fields,
        [acct, acct, "0", "388.578959440305", "0.31738093163", "0"]
    );
}

#[test]
fn decodes_stock_layouts() {
    let (wiza, coin) = (tok("free.wiza"), tok("coin"));
    let e = decode_event(
        Kind::CreatePair,
        &json!([coin, wiza, "coin:free.wiza", "pair-account"]),
    )
    .unwrap();
    assert_eq!(
        (e.pair.as_str(), e.fields.clone()),
        (
            "coin:free.wiza",
            vec!["coin:free.wiza".to_owned(), "pair-account".to_owned()]
        )
    );
    // A key naming other tokens than the refs is refused.
    assert_eq!(
        decode_event(
            Kind::CreatePair,
            &json!([coin, wiza, "coin:free.other", "x"])
        ),
        None
    );
    // UPDATE in key order `free.wiza:coin`: reserves are re-oriented to coin first.
    let e = decode_event(
        Kind::Update,
        &json!(["free.wiza:coin", {"decimal": "100.5"}, 2]),
    )
    .unwrap();
    assert_eq!(e.fields, ["2", "100.5"]);
    let e = decode_event(
        Kind::AddLiquidity,
        &json!(["k:a", "k:a", wiza, coin, 10, {"decimal": "0.5"}, {"int": 3}]),
    )
    .unwrap();
    assert_eq!(
        (e.pair.as_str(), &e.fields[2..]),
        (
            "coin:free.wiza",
            &["0.5".to_owned(), "10".to_owned(), "3".to_owned()][..]
        )
    );
    let e = decode_event(Kind::Mint, &json!(["coin:free.wiza", "k:a", 3])).unwrap();
    assert_eq!(e.fields, ["k:a", "3"]);
    let e = decode_event(
        Kind::Transfer,
        &json!(["free.wiza:coin", "k:a", "k:b", 1.5]),
    )
    .unwrap();
    assert_eq!(
        (e.pair.as_str(), e.fields.clone()),
        (
            "coin:free.wiza",
            vec!["k:a".to_owned(), "k:b".to_owned(), "1.5".to_owned()]
        )
    );
    // Malformed: wrong arity, same token twice, bad names, empty or long accounts.
    for (k, p) in [
        (Kind::Swap, json!(["k:a", "k:a", 1, coin, 1])),
        (Kind::Swap, json!(["k:a", "k:a", 1, coin, 1, coin])),
        (Kind::Swap, json!(["", "k:a", 1, coin, 1, wiza])),
        (
            Kind::Swap,
            json!(["x".repeat(257), "k:a", 1, coin, 1, wiza]),
        ),
        (Kind::Swap, json!(["k:a", "k:a", -1, coin, 1, wiza])),
        (Kind::Swap, json!(["k:a", "k:a", "1e999", coin, 1, wiza])),
        (Kind::Update, json!(["coin", 1, 2])),
        (Kind::Update, json!(["coin:bad name", 1, 2])),
        (Kind::Mint, json!(["coin:free.wiza", "k:a"])),
        (Kind::Burn, json!([1, "k:a", 1])),
        (
            Kind::Transfer,
            json!(["coin:free.wiza", "k:a", "k:b", null]),
        ),
        (
            Kind::AddLiquidity,
            json!(["k:a", "k:a", coin, coin, 1, 1, 1]),
        ),
    ] {
        assert_eq!(decode_event(k, &p), None, "{k:?} {p}");
    }
}

/// Forged look-alike events: another module's `SWAP`, a Sushi event name under another
/// module id, the right event on another chain, below the deploy height, in a failed tx, or
/// in a coinbase: nothing is stored.
#[test]
fn forged_and_out_of_scope_events_are_ignored() {
    let (_d, hot) = store();
    let mut txn = hot.rw().unwrap();
    let mut dict = MemDict::default();
    let genuine = ev(&mut dict, Kind::Swap, &real_swap());
    let fake_mod = EventRec {
        module: dict.intern(DictKind::Module, "free.sushi-exchange"),
        name: dict.intern(DictKind::EventName, "free.sushi-exchange.SWAP"),
        ..genuine.clone()
    };
    let wrong_module_id = EventRec {
        module: dict.intern(DictKind::Module, "free.attacker"),
        ..genuine.clone()
    };
    let tokens_name_on_exchange = EventRec {
        module: dict.intern(DictKind::Module, SUSHI_EXCHANGE),
        name: dict.intern(DictKind::EventName, EVENT_NAMES[Kind::Mint as usize]),
        ..genuine.clone()
    };
    for r in [
        tx(
            TxKey::new(H, 2, 1),
            vec![fake_mod, wrong_module_id, tokens_name_on_exchange],
        ),
        tx(TxKey::new(H, 1, 1), vec![genuine.clone()]),
        tx(
            TxKey::new(SUSHI_DEPLOY_HEIGHT - 1, 2, 1),
            vec![genuine.clone()],
        ),
        TxRecord {
            status_ok: false,
            ..tx(TxKey::new(H, 2, 2), vec![genuine.clone()])
        },
        tx(TxKey::new(H, 2, 0), vec![genuine.clone()]),
    ] {
        assert_eq!(
            apply_tx(&mut txn, &dict, &r, 0).unwrap(),
            ApplyStats::default()
        );
    }
    assert!(snapshot(&txn).iter().all(|(_, rows)| rows.is_empty()));
    // The genuine one is stored.
    let st = apply_tx(&mut txn, &dict, &tx(TxKey::new(H, 2, 3), vec![genuine]), 0).unwrap();
    assert_eq!(st.stored, 1);
}

/// At most MAX_EVENTS_PER_TX trusted events per tx; malformed ones are counted, not stored.
#[test]
fn per_tx_cap_and_rejections() {
    let (_d, hot) = store();
    let mut txn = hot.rw().unwrap();
    let mut dict = MemDict::default();
    let mut evs: Vec<EventRec> = (0..MAX_EVENTS_PER_TX + 6)
        .map(|_| ev(&mut dict, Kind::Swap, &real_swap()))
        .collect();
    evs.insert(0, ev(&mut dict, Kind::Swap, &json!(["k:a"])));
    let st = apply_tx(&mut txn, &dict, &tx(TxKey::new(H, 2, 1), evs), 0).unwrap();
    assert_eq!(st.stored, MAX_EVENTS_PER_TX as u64 - 1);
    // One malformed and the 7 trusted events past the cap (71 in all).
    assert_eq!(st.rejected, 8);
    assert_eq!(txn.count(Table::DexByTx).unwrap(), MAX_EVENTS_PER_TX - 1);
}

// ---------------------------------------------------------------------------------------------
// Replay model.

const PAIRS: [(&str, &str); 3] = [("coin", "free.a"), ("free.b", "coin"), ("free.a", "free.b")];
const ACCTS: [&str; 3] = ["k:1", "k:2", "k:3"];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn amount(&mut self) -> Value {
        let int = self.below(1_000_000);
        let frac = self.below(1_000_000_000_000);
        match self.below(3) {
            0 => json!(int),
            1 => json!({"decimal": format!("{int}.{frac:012}")}),
            _ => Value::Number(format!("{int}.{frac}").parse().unwrap()),
        }
    }
}

fn random_params(rng: &mut Rng) -> (Kind, Value) {
    let (t0, t1) = PAIRS[rng.below(3) as usize];
    let (t0, t1) = if rng.below(2) == 0 {
        (t0, t1)
    } else {
        (t1, t0)
    };
    let acct = |rng: &mut Rng| ACCTS[rng.below(3) as usize];
    let key = format!("{t0}:{t1}");
    let kind = Kind::ALL[rng.below(8) as usize];
    let p = match kind {
        Kind::CreatePair => json!([tok(t0), tok(t1), key, "pair-acct"]),
        Kind::Update => json!([key, rng.amount(), rng.amount()]),
        Kind::Swap => json!([
            acct(rng),
            acct(rng),
            rng.amount(),
            tok(t0),
            rng.amount(),
            tok(t1)
        ]),
        Kind::AddLiquidity | Kind::RemoveLiquidity => json!([
            acct(rng),
            acct(rng),
            tok(t0),
            tok(t1),
            rng.amount(),
            rng.amount(),
            rng.amount()
        ]),
        Kind::Mint | Kind::Burn => json!([key, acct(rng), rng.amount()]),
        Kind::Transfer => json!([key, acct(rng), acct(rng), rng.amount()]),
    };
    (kind, p)
}

/// Blocks of txs: `(height, block time, txs)`.
fn random_chain(seed: u64, dict: &mut MemDict) -> Vec<(u32, u64, Vec<TxRecord>)> {
    let mut rng = Rng(seed | 1);
    let n = 2 + rng.below(8) as u32;
    (0..n)
        .map(|i| {
            let h = H + i;
            let time = 1_790_000_000_000_000 + u64::from(i) * HOUR / 3 + rng.below(HOUR);
            let txs = (1..=1 + rng.below(3) as u16)
                .map(|pos| {
                    let evs = (0..1 + rng.below(4))
                        .map(|_| {
                            let (k, p) = random_params(&mut rng);
                            ev(dict, k, &p)
                        })
                        .collect();
                    tx(TxKey::new(h, SUSHI_CHAIN, pos), evs)
                })
                .collect();
            (h, time, txs)
        })
        .collect()
}

fn apply_blocks(hot: &HotStore, dict: &MemDict, blocks: &[(u32, u64, Vec<TxRecord>)]) {
    for (_, time, txs) in blocks {
        let mut txn = hot.rw().unwrap();
        for r in txs {
            apply_tx(&mut txn, dict, r, *time).unwrap();
        }
        txn.commit().unwrap();
    }
}

/// LP tokens are conserved: each pair's supply equals the sum of its balances.
fn assert_supply_matches_balances(txn: &impl Txn) {
    for (p0, p1) in PAIRS {
        let (pair, _) = pair_of(p0, p1).unwrap();
        let supply = txn
            .get(Table::DexAgg, &supply_key(&pair))
            .unwrap()
            .map(|v| a(std::str::from_utf8(&v).unwrap()))
            .unwrap_or_default();
        let mut prefix = pair.clone().into_bytes();
        prefix.push(0);
        let sum = txn
            .scan(Table::DexLp, &prefix, |k| k.starts_with(&prefix))
            .unwrap()
            .iter()
            .fold(Amount::zero(), |s, (_, v)| {
                s.add(&a(std::str::from_utf8(v).unwrap()), false)
            });
        assert_eq!(supply, sum, "{pair}");
    }
}

fn check_chain(seed: u64, unwind: usize) {
    let mut dict = MemDict::default();
    let chain = random_chain(seed, &mut dict);
    let unwind = unwind.min(chain.len());
    let keep = chain.len() - unwind;

    // Ingest all, then unwind the top `unwind` blocks, newest first.
    let (_d1, hot) = store();
    apply_blocks(&hot, &dict, &chain);
    let full = snapshot(&hot.ro().unwrap());
    {
        let mut txn = hot.rw().unwrap();
        for (h, _, _) in chain[keep..].iter().rev() {
            undo_block(&mut txn, SUSHI_CHAIN, *h).unwrap();
        }
        txn.commit().unwrap();
    }
    let unwound = snapshot(&hot.ro().unwrap());
    assert_supply_matches_balances(&hot.ro().unwrap());

    // Replay model: a fresh store with only the kept blocks.
    let (_d2, model) = store();
    apply_blocks(&model, &dict, &chain[..keep]);
    assert_eq!(
        unwound,
        snapshot(&model.ro().unwrap()),
        "seed {seed} unwind {unwind}"
    );

    // Re-applying the unwound blocks restores the full state exactly.
    apply_blocks(&hot, &dict, &chain[keep..]);
    assert_eq!(snapshot(&hot.ro().unwrap()), full);

    // History fill model: every tx in another order, some twice, gives the same bytes.
    let (_d3, fill) = store();
    let mut all: Vec<(u64, TxRecord)> = chain
        .iter()
        .flat_map(|(_, t, txs)| txs.iter().map(move |r| (*t, r.clone())))
        .collect();
    let mut rng = Rng(seed.rotate_left(17) | 1);
    for i in (1..all.len()).rev() {
        all.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let dup: Vec<(u64, TxRecord)> = all.iter().take(2).cloned().collect();
    all.extend(dup);
    {
        let mut txn = fill.rw().unwrap();
        for (t, r) in &all {
            apply_tx(&mut txn, &dict, r, *t).unwrap();
        }
        txn.commit().unwrap();
    }
    assert_eq!(snapshot(&fill.ro().unwrap()), full, "fill seed {seed}");
}

#[test]
fn apply_unwind_and_fill_match_the_replay_fixed_seeds() {
    for seed in [1, 2, 3, 42, 0xdead_beef] {
        for unwind in [0, 1, 3, 99] {
            check_chain(seed, unwind);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]
    #[test]
    fn apply_unwind_and_fill_match_the_replay(seed in any::<u64>(), unwind in 0usize..6) {
        check_chain(seed, unwind);
    }
}

/// Exact aggregates for a known sequence: bucket sums, counts, LP supply and balances.
#[test]
fn aggregates_are_exact() {
    let (_d, hot) = store();
    let mut dict = MemDict::default();
    let (coin, wiza) = (tok("coin"), tok("free.wiza"));
    let t0 = 1_790_000_000_000_000u64;
    let evs = vec![
        ev(
            &mut dict,
            Kind::CreatePair,
            &json!([coin, wiza, "coin:free.wiza", "p"]),
        ),
        ev(
            &mut dict,
            Kind::Mint,
            &json!(["coin:free.wiza", "k:a", {"decimal": "10.000000000001"}]),
        ),
        ev(&mut dict, Kind::Swap, &real_swap()),
        ev(
            &mut dict,
            Kind::Swap,
            &json!(["k:b", "k:b", 0.1, coin, 20, wiza]),
        ),
        ev(
            &mut dict,
            Kind::Transfer,
            &json!(["coin:free.wiza", "k:a", "k:b", 4]),
        ),
        ev(&mut dict, Kind::Burn, &json!(["coin:free.wiza", "k:b", 4])),
        ev(
            &mut dict,
            Kind::AddLiquidity,
            &json!(["k:a", "k:a", coin, wiza, 1, 2, 3]),
        ),
    ];
    let mut txn = hot.rw().unwrap();
    apply_tx(&mut txn, &dict, &tx(TxKey::new(H, 2, 1), evs), t0).unwrap();
    let pair = "coin:free.wiza";
    let b = Bucket::decode(
        &txn.get(Table::DexAgg, &bucket_key(pair, hour_of(t0)))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!((b.swaps, b.adds, b.removes), (2, 1, 0));
    let sums: Vec<String> = b.sums.iter().map(Amount::to_text).collect();
    assert_eq!(sums, ["0.1", "388.578959440305", "0.31738093163", "20"]);
    let txt = |v: Option<Vec<u8>>| v.map(|v| String::from_utf8(v).unwrap());
    assert_eq!(
        txt(txn.get(Table::DexAgg, &supply_key(pair)).unwrap()).as_deref(),
        Some("6.000000000001")
    );
    assert_eq!(
        txt(txn.get(Table::DexLp, &lp_key(pair, "k:a")).unwrap()).as_deref(),
        Some("6.000000000001")
    );
    // k:b received 4 and burned 4: no row.
    assert_eq!(txn.get(Table::DexLp, &lp_key(pair, "k:b")).unwrap(), None);
    assert_eq!(txn.count(Table::DexPairs).unwrap(), 1);
    // The swap row keeps the maker (stock writes "" on every pool transaction).
    let swaps = txn
        .scan(Table::DexEvents, &event_prefix(pair, Kind::Swap), |k| {
            k.starts_with(&event_prefix(pair, Kind::Swap))
        })
        .unwrap();
    let first = StoredEvent::decode(&swaps[0].1).unwrap();
    assert!(first.fields[0].starts_with("k:f87a7d0b"));
    assert_eq!(first.time, t0);
    assert_eq!(undo_block(&mut txn, 2, H).unwrap(), 7);
    assert!(snapshot(&txn).iter().all(|(_, rows)| rows.is_empty()));
}

/// Through the tail: `tail::apply` stores a block's Sushi events and `unwind_to` removes them.
#[test]
fn tail_apply_and_unwind() {
    let dir = tempfile::tempdir().unwrap();
    let hot = HotStore::open(dir.path(), Geometry::default()).unwrap();
    let mut txn = hot.rw().unwrap();
    let mut dict = HotDict::load(&txn).unwrap();
    let block = |dict: &mut HotDict, height: u32| {
        let key = TxKey::new(height, 2, 1);
        let (module, name) = Kind::Swap.module_and_name();
        let e = EventRec {
            module: dict.intern(DictKind::Module, module),
            name: dict.intern(DictKind::EventName, &format!("{module}.{name}")),
            module_hash: 0,
            params: value::to_vec(&real_swap(), &*dict),
        };
        IndexedBlock {
            block: BlockRecord {
                hash: Hash32([height as u8; 32]),
                chain: 2,
                height: u64::from(height),
                creation_time: 1_790_000_000_000_000 + u64::from(height) * HOUR,
                ..Default::default()
            },
            txs: vec![tx(key, vec![e])],
            idx: vec![TxIndex {
                key,
                postings: vec![],
                pact_id: None,
                code_hash: None,
                d3: Default::default(),
            }],
            cmd_reencoded: 0,
            unindexed_accounts: 0,
        }
    };
    let b1 = block(&mut dict, H);
    crate::tail::apply(&mut txn, &b1, &mut dict).unwrap();
    let one = snapshot(&txn);
    let b2 = block(&mut dict, H + 1);
    crate::tail::apply(&mut txn, &b2, &mut dict).unwrap();
    assert_eq!(txn.count(Table::DexEvents).unwrap(), 2);
    assert_eq!(txn.count(Table::DexAgg).unwrap(), 2); // two hour buckets
    assert_eq!(crate::tail::unwind_to(&mut txn, 2, H).unwrap(), 1);
    assert_eq!(snapshot(&txn), one);
    assert_eq!(crate::tail::unwind_to(&mut txn, 2, H - 1).unwrap(), 1);
    assert!(snapshot(&txn).iter().all(|(_, rows)| rows.is_empty()));
    txn.commit().unwrap();
}
