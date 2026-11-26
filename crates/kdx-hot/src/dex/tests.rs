//! `dex.rs` tests: exact amounts, decoding (a real chain-2 Sushi `SWAP` and stock's param
//! layouts), trust, bounds, and apply / unwind / fill exactness against a replay model.

use kdx_decode::{IndexedBlock, TxIndex};
use kdx_types::record::{BlockRecord, EventRec};
use kdx_types::{value, DictMut, Hash32, MemDict};
use proptest::prelude::*;
use serde_json::{json, Value};

use super::*;
use crate::{Geometry, HotDict, HotStore};

const H: u32 = 6_300_000;
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

// ---------------------------------------------------------------------------------------------
// Fix round 1: real history scope (C1) and hostile values at the limits (C2).

/// Real chain-2 Sushi events (public chain data only): all 20 `CREATE_PAIR`s and the oldest
/// and newest few of every other kind, one JSON object per line.
const CAPTURED: &str = include_str!("testdata/sushi-chain2-events.jsonl");

/// The captured events as txs, grouped by (height, request key), each event at its real index
/// (earlier indices hold an untrusted `coin.TRANSFER`). Returns `(height, block time, tx)`.
/// Captured events of one tx: block time and `(event index, kind, params)`.
type CapturedTx = (u64, Vec<(usize, Kind, Value)>);

fn captured_txs(dict: &mut MemDict) -> Vec<(u32, u64, TxRecord)> {
    let mut by_tx: std::collections::BTreeMap<(u32, String), CapturedTx> = Default::default();
    for line in CAPTURED.lines() {
        let v: Value = serde_json::from_str(line).unwrap();
        let kind = kind_of_name(&format!(
            "{}.{}",
            v["module"].as_str().unwrap(),
            v["name"].as_str().unwrap()
        ))
        .expect("captured event is a Sushi event");
        assert_eq!(v["module"].as_str().unwrap(), kind.module_and_name().0);
        let e = by_tx
            .entry((
                v["height"].as_u64().unwrap() as u32,
                v["requestKey"].as_str().unwrap().to_owned(),
            ))
            .or_insert((v["blockTimeUs"].as_u64().unwrap(), Vec::new()));
        e.1.push((
            v["orderIndex"].as_u64().unwrap() as usize,
            kind,
            v["params"].clone(),
        ));
    }
    let filler = EventRec {
        module: dict.intern(DictKind::Module, "coin"),
        name: dict.intern(DictKind::EventName, "coin.TRANSFER"),
        module_hash: 0,
        params: value::to_vec(&json!([]), &*dict),
    };
    let mut out = Vec::new();
    let mut pos: std::collections::BTreeMap<u32, u16> = Default::default();
    for ((height, _), (time, evs)) in by_tx {
        let p = pos.entry(height).or_insert(0);
        *p += 1;
        let n = evs.iter().map(|(i, _, _)| i + 1).max().unwrap();
        let mut events = vec![filler.clone(); n];
        for (i, kind, params) in &evs {
            events[*i] = ev(dict, *kind, params);
        }
        out.push((
            height,
            time,
            tx(TxKey::new(height, SUSHI_CHAIN, *p), events),
        ));
    }
    out
}

/// C1: stock applies no height floor, and the first `CREATE_PAIR` is at 6,121,854. Every
/// captured event (19 of the 20 pairs are below the old floor) is stored.
#[test]
fn captured_history_is_stored_from_the_first_pair() {
    let mut dict = MemDict::default();
    let txs = captured_txs(&mut dict);
    assert_eq!(txs.first().unwrap().0, 6_121_854, "first CREATE_PAIR");
    let (_d, hot) = store();
    let mut txn = hot.rw().unwrap();
    let mut st = ApplyStats::default();
    for (_, time, r) in &txs {
        assert!(in_scope(r), "{:?}", r.key);
        let s = apply_tx(&mut txn, &dict, r, *time).unwrap();
        st.stored += s.stored;
        st.rejected += s.rejected;
    }
    assert_eq!(st.rejected, 0);
    assert_eq!(st.stored, CAPTURED.lines().count() as u64);
    assert_eq!(txn.count(Table::DexPairs).unwrap(), 20);
    let adds = txn
        .scan(Table::DexEvents, &[], |_| true)
        .unwrap()
        .iter()
        .filter(|(_, v)| v[0] == Kind::AddLiquidity as u8)
        .count();
    assert_eq!(adds, 5, "every captured ADD_LIQUIDITY");
    // The first pair (kb-USDC, height 6,121,854) and its first add are there.
    let first = txn
        .scan(Table::DexPairs, &[], |_| true)
        .unwrap()
        .into_iter()
        .map(|(k, _)| {
            let z = k.iter().position(|b| *b == 0).unwrap();
            TxKey::from_be_bytes(k[z + 1..z + 9].try_into().unwrap()).height()
        })
        .min()
        .unwrap();
    assert_eq!(first, 6_121_854);
    // Height 0 on chain 2 is in scope too (no floor at all).
    assert!(in_scope(&tx(
        TxKey::new(0, SUSHI_CHAIN, 1),
        vec![filler_event(&mut dict)]
    )));
}

fn filler_event(dict: &mut MemDict) -> EventRec {
    ev(dict, Kind::Swap, &real_swap())
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// Rows not about `pair` (key or value naming it).
fn without_pair(s: &Snapshot, pair: &str) -> Snapshot {
    s.iter()
        .map(|(t, rows)| {
            (
                *t,
                rows.iter()
                    .filter(|(k, v)| !contains(k, pair.as_bytes()) && !contains(v, pair.as_bytes()))
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}

/// A hostile pair of junk tokens with amounts at the input limit: blocks of
/// `(height, time, txs)`, each block also holding one genuine swap on another pair.
fn hostile_chain(dict: &mut MemDict) -> (String, Vec<(u32, u64, Vec<TxRecord>)>) {
    let (j0, j1) = ("free.junk-a", "free.junk-b");
    let (pair, _) = pair_of(j0, j1).unwrap();
    // 96 characters: the longest accepted input.
    let big = format!("6{}", "0".repeat(MAX_AMOUNT_TEXT - 1));
    assert!(Amount::parse(&big).is_some());
    // The largest exponent: 92 digits × 10^64 (stored text well over 96 bytes).
    let huge = format!("{}e64", "9".repeat(MAX_AMOUNT_TEXT - 4));
    assert!(Amount::parse(&huge).is_some());
    let (alice, bob) = ("k:alice", "k:bob");
    let d = |s: &str| json!({"decimal": s});
    let mint = |acct: &str, amt: &str| (Kind::Mint, json!([pair, acct, d(amt)]));
    let burn = |acct: &str, amt: &str| (Kind::Burn, json!([pair, acct, d(amt)]));
    let xfer = |amt: &str| (Kind::Transfer, json!([pair, alice, bob, d(amt)]));
    let swap = |amt: &str| {
        (
            Kind::Swap,
            json!([alice, alice, d(amt), tok(j0), d(amt), tok(j1)]),
        )
    };
    let liq = |k: Kind, amt: &str| {
        (
            k,
            json!([alice, alice, tok(j0), tok(j1), d(amt), d(amt), d(amt)]),
        )
    };
    let blocks: Vec<Vec<(Kind, Value)>> = vec![
        vec![
            (
                Kind::CreatePair,
                json!([tok(j0), tok(j1), pair, "junk-account"]),
            ),
            mint(alice, &big),
        ],
        vec![mint(alice, &big)],
        // Supply and alice's balance are now 1.2e96 (97 characters).
        vec![mint(bob, &big), swap(&big), swap(&big)],
        vec![
            swap(&big),
            swap(&huge),
            liq(Kind::AddLiquidity, &big),
            xfer(&big),
            burn(alice, &big),
            mint(alice, &huge),
            mint(alice, &huge),
        ],
        vec![
            swap(&huge),
            liq(Kind::RemoveLiquidity, &huge),
            xfer(&huge),
            (Kind::Update, json!([pair, d(&huge), d(&big)])),
        ],
    ];
    let mut out = Vec::new();
    for (i, evs) in blocks.into_iter().enumerate() {
        let h = H + i as u32;
        let time = 1_790_000_000_000_000 + u64::from(h) * 60_000_000;
        let hostile: Vec<EventRec> = evs.iter().map(|(k, p)| ev(dict, *k, p)).collect();
        let genuine = vec![ev(dict, Kind::Swap, &real_swap())];
        out.push((
            h,
            time,
            vec![
                tx(TxKey::new(h, SUSHI_CHAIN, 1), hostile),
                tx(TxKey::new(h, SUSHI_CHAIN, 2), genuine),
            ],
        ));
    }
    (pair, out)
}

/// C2: sums past the input limit are read back; the tail keeps applying, unwind works, the
/// other pair is unaffected, and any order of the same txs gives the same bytes.
#[test]
fn hostile_pair_at_the_limit_never_halts_apply_or_unwind() {
    let mut dict = MemDict::default();
    let (pair, chain) = hostile_chain(&mut dict);
    let (_d, hot) = store();
    for (_, time, txs) in &chain {
        let mut txn = hot.rw().unwrap();
        for r in txs {
            let st = apply_tx(&mut txn, &dict, r, *time).unwrap();
            assert_eq!(st.rejected, 0, "{:?}", r.key);
        }
        txn.commit().unwrap();
    }
    let full = snapshot(&hot.ro().unwrap());
    // The hostile sums really are past the input limit.
    let supply = hot
        .ro()
        .unwrap()
        .get(Table::DexAgg, &supply_key(&pair))
        .unwrap()
        .unwrap();
    assert!(supply.len() > MAX_AMOUNT_TEXT, "{}", supply.len());
    // Exact sums: nothing is poisoned.
    assert!(!poisoned(&hot.ro().unwrap(), &pair).unwrap());
    assert_supply_matches_balances(&hot.ro().unwrap());

    // The genuine pair is exactly what it is without the hostile one.
    let (_m, model) = store();
    {
        let mut txn = model.rw().unwrap();
        for (_, time, txs) in &chain {
            apply_tx(&mut txn, &dict, &txs[1], *time).unwrap();
        }
        txn.commit().unwrap();
    }
    assert_eq!(without_pair(&full, &pair), snapshot(&model.ro().unwrap()));

    // Any order (and replays) gives the same bytes.
    for seed in [1u64, 7, 99] {
        let mut all: Vec<(u64, TxRecord)> = chain
            .iter()
            .flat_map(|(_, t, txs)| txs.iter().map(move |r| (*t, r.clone())))
            .collect();
        let mut rng = Rng(seed);
        for i in (1..all.len()).rev() {
            all.swap(i, rng.below(i as u64 + 1) as usize);
        }
        all.push(all[0].clone());
        let (_f, fill) = store();
        let mut txn = fill.rw().unwrap();
        for (t, r) in &all {
            apply_tx(&mut txn, &dict, r, *t).unwrap();
        }
        assert_eq!(snapshot(&txn), full, "order seed {seed}");
    }

    // Unwind every block, newest first: nothing is left.
    let mut txn = hot.rw().unwrap();
    for (h, _, _) in chain.iter().rev() {
        assert!(undo_block(&mut txn, SUSHI_CHAIN, *h).unwrap() > 0);
    }
    assert!(snapshot(&txn).iter().all(|(_, rows)| rows.is_empty()));
}

/// The read-back bound covers every sum the writer can produce: the largest input (both
/// forms) added 2^64 times, either sign, stays within MAX_STORED_TEXT and reads back exactly.
#[test]
fn stored_sums_of_capped_inputs_read_back() {
    for top in [
        format!("{}e{MAX_EXP}", "9".repeat(MAX_AMOUNT_TEXT - 3)),
        "9".repeat(MAX_AMOUNT_TEXT),
        format!("0.{}", "9".repeat(SCALE)),
    ] {
        let mut x = a(&top);
        for _ in 0..64 {
            x = x.add(&x, false);
        }
        for v in [x.clone(), Amount::zero().add(&x, true)] {
            let t = v.to_text();
            assert!(t.len() <= MAX_STORED_TEXT, "{} bytes", t.len());
            assert_eq!(Amount::parse_stored(&t), Some(v), "{t}");
        }
    }
    assert_eq!(Amount::parse_stored(&"9".repeat(MAX_STORED_TEXT + 1)), None);
}

fn junk_swap(j0: &str, j1: &str) -> Value {
    json!(["k:a", "k:a", 1.5, tok(j0), 2.5, tok(j1)])
}

/// C2 containment: damaged stored values (unreadable sums, bucket, event row, DexByTx row,
/// a sum past MAX_STORED_TEXT) never fail apply or unwind. The pair is poisoned, its events
/// still store and unwind, and the other pair is exactly as without it.
#[test]
fn unreadable_stored_values_poison_only_their_pair() {
    let mut dict = MemDict::default();
    let (j0, j1) = ("free.junk-a", "free.junk-b");
    let (pair, _) = pair_of(j0, j1).unwrap();
    let mint = json!([pair, "k:a", 3]);
    let block = |dict: &mut MemDict, h: u32| {
        vec![
            tx(
                TxKey::new(h, SUSHI_CHAIN, 1),
                vec![
                    ev(dict, Kind::Mint, &mint),
                    ev(dict, Kind::Swap, &junk_swap(j0, j1)),
                ],
            ),
            tx(
                TxKey::new(h, SUSHI_CHAIN, 2),
                vec![ev(dict, Kind::Swap, &real_swap())],
            ),
        ]
    };
    let time = |h: u32| 1_790_000_000_000_000 + u64::from(h) * HOUR;
    let damage: [&dyn Fn(&mut RwTxn<'_>); 5] = [
        &|t| {
            t.put(Table::DexAgg, &supply_key(&pair), b"not a number")
                .unwrap()
        },
        &|t| {
            t.put(
                Table::DexLp,
                &lp_key(&pair, "k:a"),
                "9".repeat(MAX_STORED_TEXT + 1).as_bytes(),
            )
            .unwrap()
        },
        &|t| {
            for (k, _) in t.scan(Table::DexAgg, &[2], |k| k[0] == 2).unwrap() {
                if contains(&k, pair.as_bytes()) {
                    t.put(Table::DexAgg, &k, &[1, 2, 3]).unwrap();
                }
            }
        },
        &|t| {
            for (k, _) in t.scan(Table::DexEvents, &[], |_| true).unwrap() {
                if k.starts_with(pair.as_bytes()) {
                    t.put(Table::DexEvents, &k, &[0xff]).unwrap();
                }
            }
        },
        &|t| {
            for (k, v) in t.scan(Table::DexByTx, &[], |_| true).unwrap() {
                if contains(&v, pair.as_bytes()) {
                    t.put(Table::DexByTx, &k, &[0xee]).unwrap();
                }
            }
        },
    ];
    for (i, hurt) in damage.iter().enumerate() {
        let (_d, hot) = store();
        let (_m, model) = store();
        let mut txn = hot.rw().unwrap();
        let mut mtxn = model.rw().unwrap();
        for h in H..H + 2 {
            let txs = block(&mut dict, h);
            for r in &txs {
                apply_tx(&mut txn, &dict, r, time(h)).unwrap();
            }
            apply_tx(&mut mtxn, &dict, &txs[1], time(h)).unwrap();
        }
        hurt(&mut txn);
        // The tail keeps applying, on both pairs.
        for h in H + 2..H + 4 {
            let txs = block(&mut dict, h);
            for r in &txs {
                let st = apply_tx(&mut txn, &dict, r, time(h)).unwrap();
                assert_eq!(st.stored, r.events.len() as u64, "case {i}");
            }
            apply_tx(&mut mtxn, &dict, &txs[1], time(h)).unwrap();
        }
        // Unwind works for every block.
        for h in (H..H + 4).rev() {
            assert!(
                undo_block(&mut txn, SUSHI_CHAIN, h).unwrap() >= 1,
                "case {i}"
            );
            undo_block(&mut mtxn, SUSHI_CHAIN, h).unwrap();
            // Damaged DexByTx rows of blocks not unwound yet no longer name the pair.
            let mut left = without_pair(&snapshot(&txn), &pair);
            for (_, rows) in &mut left {
                rows.retain(|(_, v)| v[..] != [0xee]);
            }
            assert_eq!(left, snapshot(&mtxn), "case {i} at {h}");
        }
        assert!(poisoned(&txn, &pair).unwrap(), "case {i}");
        assert!(!pair_status(&txn, &pair).unwrap().listable());
        // Nothing but damage and the poison mark is left: every event row is gone.
        for t in [Table::DexEvents, Table::DexByTx, Table::DexPairs] {
            assert_eq!(txn.count(t).unwrap(), 0, "case {i} {t:?}");
        }
    }
}

/// M1: each row keeps the tx's `meta.creationTime` (stock's `timestamp`) beside the block time.
#[test]
fn rows_keep_the_tx_creation_time() {
    let (_d, hot) = store();
    let mut txn = hot.rw().unwrap();
    let mut dict = MemDict::default();
    let r = TxRecord {
        cmd: Some(br#"{"meta":{"creationTime":1764000000}}"#.to_vec()),
        ..tx(
            TxKey::new(H, 2, 1),
            vec![ev(&mut dict, Kind::Swap, &real_swap())],
        )
    };
    apply_tx(&mut txn, &dict, &r, 42).unwrap();
    let rows = txn.scan(Table::DexEvents, &[], |_| true).unwrap();
    let e = StoredEvent::decode(&rows[0].1).unwrap();
    assert_eq!((e.time, e.tx_time), (42, 1_764_000_000));
    assert_eq!(StoredEvent::decode(&e.encode()).unwrap(), e);
}

/// M5: the junk-pair rule reads what ingest stores; ingest itself keeps every pair.
#[test]
fn pair_status_supports_the_junk_rule() {
    let (_d, hot) = store();
    let mut txn = hot.rw().unwrap();
    let mut dict = MemDict::default();
    let mut add = |txn: &mut RwTxn<'_>, pos: u16, kind: Kind, p: Value| {
        let r = tx(TxKey::new(H, 2, pos), vec![ev(&mut dict, kind, &p)]);
        assert_eq!(apply_tx(txn, &dict, &r, 0).unwrap().stored, 1);
    };
    let (w, junk) = ("free.wiza", "free.junk");
    add(
        &mut txn,
        1,
        Kind::CreatePair,
        json!([tok("coin"), tok(w), "coin:free.wiza", "a"]),
    );
    add(
        &mut txn,
        2,
        Kind::CreatePair,
        json!([tok(junk), tok(w), "free.junk:free.wiza", "b"]),
    );
    let coin = pair_of("coin", w).unwrap().0;
    let jp = pair_of(junk, w).unwrap().0;
    assert_eq!(txn.count(Table::DexPairs).unwrap(), 2);
    let s = pair_status(&txn, &coin).unwrap();
    assert!(s.created && s.coin_leg && !s.poisoned && s.reserves.is_none() && !s.listable());
    add(&mut txn, 3, Kind::Update, json!(["coin:free.wiza", 5, 0]));
    add(&mut txn, 4, Kind::Update, json!(["free.wiza:coin", 7, 2]));
    let s = pair_status(&txn, &coin).unwrap();
    // The latest UPDATE wins, oriented to the pair (coin < free.wiza).
    assert_eq!(s.reserves, Some((a("2"), a("7"))));
    assert!(s.listable());
    add(
        &mut txn,
        5,
        Kind::Update,
        json!(["free.junk:free.wiza", 7, 2]),
    );
    let s = pair_status(&txn, &jp).unwrap();
    assert!(s.created && !s.coin_leg && !s.listable());
    assert_eq!(pair_status(&txn, "x:y").unwrap(), PairStatus::default());
}
