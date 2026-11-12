//! Stateful equivalence (Task 14) on real mainnet blocks (see `common/mod.rs`):
//! - apply then undo to a random height = apply only the prefix;
//! - a reorg through the follower's fork batch = a linear apply of the winning branch, and
//!   the balance dirty marks cover every fungible TRANSFER side of either branch (B-3), with
//!   a fresh mark sequence on every key the winner touches (M-3);
//! - the freezer is replay-deterministic: a history with reorgs, other commit sizes and
//!   freezer ticks in between freezes and merges to the same segment files as a linear apply,
//!   and ten merged steps carry the same blocks as one wide step.
//!
//! The comparisons go through table digests and segment file hashes of a second store built
//! another way, never through the code under test re-run on the same input.

mod common;

use std::collections::BTreeMap;
use std::path::Path;

use common::*;
use kdx_hot::balances::{bal_key, Mark};
use kdx_hot::{HotDict, Table, Txn};
use kdx_types::{Dict, DictKind};

const CASES: u64 = 12;

#[test]
fn apply_then_undo_equals_the_prefix() {
    let pool = pool();
    for seed in 1..=CASES {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ seed);
        let n = rng.range(3, 16) as usize;
        let mut ids: Vec<usize> = (0..POOL_LEN).collect();
        rng.shuffle(&mut ids);
        let places = branch(&ids[..n], 0, 0, 0);
        let keep = rng.range(0, n as u64 - 2) as u32;

        let da = tempfile::tempdir().unwrap();
        let a = ctx(da.path(), 100, 1000, 100);
        apply_linear(&a, &pool, &places, rng.range(1, 5) as usize);
        let mut txn = a.hot.rw().unwrap();
        let undone = kdx_hot::tail::unwind_to(&mut txn, 0, keep).unwrap();
        txn.commit().unwrap();
        assert_eq!(undone as usize, n - 1 - keep as usize, "seed {seed}");

        let dp = tempfile::tempdir().unwrap();
        let p = ctx(dp.path(), 100, 1000, 100);
        apply_linear(
            &p,
            &pool,
            &places[..=keep as usize],
            rng.range(1, 5) as usize,
        );
        assert_same_state(&a.hot, &p.hot, &format!("seed {seed} n {n} keep {keep}"));
    }
}

/// Dirty-mark keys of the fungible TRANSFER sides of `ids`, looked up in `hot`'s dictionary.
fn side_keys(
    hot: &kdx_hot::HotStore,
    pool: &[serde_json::Value],
    ids: &[usize],
) -> Vec<([u8; 9], String)> {
    let dict = HotDict::load(&hot.ro().unwrap()).unwrap();
    let mut out = Vec::new();
    for &i in ids {
        for (acct, module) in transfer_sides(pool, i) {
            let a = dict.find(DictKind::Account, &acct);
            let m = dict.find(DictKind::Module, &module);
            let (Some(a), Some(m)) = (a, m) else {
                panic!("pool {i}: {acct} / {module} not in the dictionary");
            };
            out.push((bal_key(a, m, 0), format!("{acct} {module}")));
        }
    }
    out
}

fn marks(hot: &kdx_hot::HotStore) -> BTreeMap<Vec<u8>, Mark> {
    hot.ro()
        .unwrap()
        .scan(Table::BalanceDirty, &[], |_| true)
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k, Mark::decode(&v).unwrap()))
        .collect()
}

/// Strict: the loser holds the first blocks of the winner (same payloads under other
/// headers, the common case of a chainweb reorg), so both histories intern the same names in
/// the same order and the digests must be byte-equal.
#[test]
fn a_reorg_with_the_same_payloads_equals_the_linear_apply() {
    reorg_cases(false);
}

/// General: a random loser. Equal modulo dictionary ids, with full mark coverage (B-3, M-3).
#[test]
fn a_reorg_equals_the_linear_apply_of_the_winner_and_marks_both_branches() {
    reorg_cases(true);
}

/// The general case, compared byte for byte. Names that only the losing branch interned
/// keep their ids (by design, review M1), so every new name of the winner gets a later id
/// than in a linear apply: the hot rows (and the step files frozen from them) differ in
/// bytes, not in content.
#[test]
#[ignore = "finding 9B-1: after a reorg whose losing branch interned new names, hot rows and frozen step bytes differ from a linear replay (dictionary ids shift); content is equal (see the modulo-ids test)"]
fn a_reorg_with_new_names_on_the_loser_is_byte_equal_to_the_linear_apply() {
    let pool = pool();
    let mut ids: Vec<usize> = (0..POOL_LEN).collect();
    Rng(7).shuffle(&mut ids);
    let (pre, lose, win) = (&ids[..3], &ids[3..6], &ids[6..10]);
    let dr = tempfile::tempdir().unwrap();
    let r = ctx(dr.path(), 100, 1000, 100);
    apply_linear(&r, &pool, &branch(pre, 0, 0, 0), 3);
    apply_linear(&r, &pool, &branch(lose, 3, 1, 0), 3);
    apply_fork(&r, &pool, 2, 3, &branch(win, 3, 2, 0));
    let dl = tempfile::tempdir().unwrap();
    let l = ctx(dl.path(), 100, 1000, 100);
    apply_linear(&l, &pool, &branch(pre, 0, 0, 0), 3);
    apply_linear(&l, &pool, &branch(win, 3, 2, 0), 4);
    assert_same_state(&r.hot, &l.hot, "random loser");
}

fn reorg_cases(general: bool) {
    let pool = pool();
    let mut sides_checked = 0usize;
    for seed in 1..=CASES {
        let mut rng = Rng(0xd1b5_4a32_d192_ed03 ^ seed);
        let mut ids: Vec<usize> = (0..POOL_LEN).collect();
        rng.shuffle(&mut ids);
        let p = rng.range(1, 7) as usize;
        let a = rng.range(1, 6) as usize;
        let b = rng.range(1, 6) as usize;
        let prefix = ids[..p].to_vec();
        let lose = ids[p..p + a].to_vec();
        // The winner re-includes some of the loser's blocks (same txs, other heights).
        let (b, again) = if general {
            (b, rng.range(0, a.min(b) as u64) as usize)
        } else {
            (a + b, a)
        };
        let mut win: Vec<usize> = lose[..again].to_vec();
        win.extend(&ids[p + a..p + a + (b - again)]);
        if general {
            rng.shuffle(&mut win);
        }
        let fork = p as u32 - 1;
        let pre = branch(&prefix, 0, 0, 0);
        let lose_pl = branch(&lose, p as u32, 1, 0);
        let win_pl = branch(&win, p as u32, 2, 0);
        let what = format!("seed {seed} prefix {p} lose {a} win {b} again {again}");

        let dr = tempfile::tempdir().unwrap();
        let r = ctx(dr.path(), 100, 1000, 100);
        apply_linear(&r, &pool, &pre, rng.range(1, 4) as usize);
        apply_linear(&r, &pool, &lose_pl, rng.range(1, 4) as usize);
        let before = marks(&r.hot);
        apply_fork(&r, &pool, fork, a as u32, &win_pl);

        let dl = tempfile::tempdir().unwrap();
        let l = ctx(dl.path(), 100, 1000, 100);
        apply_linear(&l, &pool, &pre, rng.range(1, 4) as usize);
        apply_linear(&l, &pool, &win_pl, rng.range(1, 4) as usize);
        if general {
            assert_same_state_modulo_ids(&r.hot, &l.hot, &what);
        } else {
            assert_same_state(&r.hot, &l.hot, &what);
        }

        // B-3: every side of either branch is marked after the reorg.
        let after = marks(&r.hot);
        let mut both = lose.clone();
        both.extend(&win);
        for (k, name) in side_keys(&r.hot, &pool, &both) {
            assert!(after.contains_key(&k[..]), "{what}: no mark for {name}");
            sides_checked += 1;
        }
        // M-3: a key the winner touches carries a newer mark sequence than before the reorg,
        // even where the height did not rise, so a reconciler answer read before it cannot
        // clear it.
        for (k, name) in side_keys(&r.hot, &pool, &win) {
            if let Some(old) = before.get(&k[..]) {
                assert!(
                    after[&k[..]].seq > old.seq,
                    "{what}: mark of {name} kept its seq"
                );
            }
        }
    }
    assert!(
        sides_checked > 50,
        "too few TRANSFER sides: {sides_checked}"
    );
}

/// (file name, blake3) of every file under `dir`, recursively.
fn files(dir: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                let name = p.strip_prefix(dir).unwrap().to_string_lossy().into_owned();
                let h = blake3::hash(&std::fs::read(&p).unwrap())
                    .to_hex()
                    .to_string();
                out.insert(name, h);
            }
        }
    }
    out
}

fn tick_all(c: &kdx_ingest::Ctx) -> usize {
    let mut n = 0;
    while kdx_ingest::freezer::tick(c).unwrap().is_some() {
        n += 1;
    }
    n
}

fn manifest(c: &kdx_ingest::Ctx) -> kdx_segment::manifest::Manifest {
    kdx_segment::manifest::Manifest::load(&c.segdir)
        .unwrap()
        .unwrap()
}

#[test]
fn freeze_and_merge_are_replay_deterministic() {
    let pool = pool();
    let all: Vec<usize> = (0..POOL_LEN).collect();
    let chain = branch(&all, 0, 0, 0);
    // step 2, range 20 (ten steps per range), finality 2: heights 0..20 freeze and merge.
    let dl = tempfile::tempdir().unwrap();
    let l = ctx(dl.path(), 2, 20, 2);
    apply_linear(&l, &pool, &chain, 5);
    let ticks = tick_all(&l);
    assert_eq!(ticks, 11, "ten steps and one merge");
    let ml = manifest(&l);
    assert_eq!(ml.entries.len(), 1);
    assert_eq!((ml.entries[0].start, ml.entries[0].end), (0, 20));

    for seed in 1..=4u64 {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ seed);
        let dr = tempfile::tempdir().unwrap();
        let r = ctx(dr.path(), 2, 20, 2);
        // Ticks while the tail runs, a reorg above the frontier that replaces blocks 10..14
        // by the same payloads under other headers, and other commit sizes.
        let cut = rng.range(9, 12) as usize;
        apply_linear(&r, &pool, &chain[..cut], rng.range(1, 4) as usize);
        tick_all(&r);
        let lose_to = cut + rng.range(1, 4) as usize;
        let lose = branch(&all[cut..lose_to], cut as u32, 1, 0);
        apply_linear(&r, &pool, &lose, 1);
        let win_to = lose_to + rng.range(0, 3) as usize;
        apply_fork(
            &r,
            &pool,
            cut as u32 - 1,
            (lose_to - cut) as u32,
            &chain[cut..win_to],
        );
        tick_all(&r);
        apply_linear(&r, &pool, &chain[win_to..], rng.range(1, 6) as usize);
        tick_all(&r);
        assert_eq!(manifest(&r), ml, "seed {seed}");
        assert_eq!(files(&r.segdir), files(&l.segdir), "seed {seed}");
        assert_same_state(&r.hot, &l.hot, &format!("seed {seed}"));
    }

    // Ten merged steps carry what one 20-height step carries.
    let dw = tempfile::tempdir().unwrap();
    let w = ctx(dw.path(), 20, 200, 2);
    apply_linear(&w, &pool, &chain, 24);
    assert_eq!(tick_all(&w), 1);
    let mw = manifest(&w);
    let (el, ew) = (&ml.entries[0], &mw.entries[0]);
    assert_eq!(
        (
            ew.start,
            ew.end,
            ew.first_txnum,
            ew.tx_count,
            ew.block_count
        ),
        (
            el.start,
            el.end,
            el.first_txnum,
            el.tx_count,
            el.block_count
        )
    );
    // Every data file is byte-equal; only the range descriptor and the manifest name the
    // kind (range vs step).
    let data = |m: BTreeMap<String, String>| -> BTreeMap<String, String> {
        m.into_iter()
            .filter(|(n, _)| n != "manifest.json" && !n.contains("-range-"))
            .collect()
    };
    let (fl, fw) = (data(files(&l.segdir)), data(files(&w.segdir)));
    assert!(fl.len() >= 14, "{fl:?}");
    assert_eq!(fl, fw);
}
