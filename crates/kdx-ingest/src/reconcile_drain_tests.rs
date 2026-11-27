//! Refill windows ([`RefillWindow`], [`refill_window`]) and the drain bench (`drain_bench`,
//! ignored: run it by name, see its docs).

use std::collections::HashSet;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use kdx_hot::{Geometry, HotDict, HotStore};
use kdx_types::DictMut;

use super::tests::{client_on, put_header, tip_hash};
use super::*;

/// `accounts` (`a0`, `a1`, …) dirty for `coin` on chain 0, the mark of `a{i}` at height
/// `1 + i % 9` (all under the fake node's answer height, 10).
fn store(dir: &std::path::Path, accounts: usize) -> (Arc<HotStore>, u32, Vec<u32>) {
    let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
    let mut txn = hot.rw().unwrap();
    let mut dict = HotDict::load(&txn).unwrap();
    let coin = dict.intern(DictKind::Module, "coin");
    let ids: Vec<u32> = (0..accounts)
        .map(|i| dict.intern(DictKind::Account, &format!("a{i}")))
        .collect();
    dict.persist(&mut txn).unwrap();
    for (i, &a) in ids.iter().enumerate() {
        balances::mark_dirty(&mut txn, a, coin, 0, 1 + (i % 9) as u32).unwrap();
    }
    txn.commit().unwrap();
    put_header(&hot, 0, 10, tip_hash());
    (hot, coin, ids)
}

fn mark_now(hot: &HotStore, key: &[u8; 9]) -> Option<Mark> {
    hot.ro()
        .unwrap()
        .get(Table::BalanceDirty, key)
        .unwrap()
        .and_then(|v| Mark::decode(&v))
}

/// A window of `k` batches is one refill of `k × n` keys: the same keys, handed out `n` at a
/// time (each batch its own tip share, then the rest in order), each key once and with
/// exactly the mark in the table.
#[test]
fn window_hands_out_one_refill_with_exact_marks() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, _, _) = store(dir.path(), 300);
    let back = HashMap::new();
    let keys = refill_window(&hot, 20, 4, 8, &back).unwrap();
    assert_eq!(keys, refill_with(&hot, 160, 32, &back).unwrap());
    assert_eq!(keys.len(), 160);
    let mut w = RefillWindow::default();
    w.set(keys.clone(), Instant::now());
    let mut seen = HashSet::new();
    let mut all = Vec::new();
    for _ in 0..8 {
        let b = w.next_batch(20);
        assert_eq!(b.len(), 20);
        for d in &b {
            assert!(seen.insert(d.key), "a key twice in one window");
            assert_eq!(
                mark_now(&hot, &d.key),
                Some(d.mark),
                "the mark read at refill"
            );
        }
        all.extend(b);
    }
    let sorted = |v: &[Dirty]| {
        let mut v = v.to_vec();
        v.sort_by_key(|d| d.key);
        v
    };
    assert_eq!(sorted(&all), sorted(&keys));
    // The non-tip keys in the refill's order.
    let rest = |v: &[Dirty]| v.iter().filter(|d| !d.tip).copied().collect::<Vec<_>>();
    assert_eq!(rest(&all), rest(&keys));
    assert_eq!(w.left(), 0);
    assert!(w.next_batch(20).is_empty());
    assert!(w.due(Instant::now(), WINDOW_MAX_AGE));
    // `k` is clamped to 1..=MAX_REFILL_WINDOW.
    assert_eq!(refill_window(&hot, 20, 4, 0, &back).unwrap().len(), 20);
    assert_eq!(
        refill_window(&hot, 5, 1, 1000, &back).unwrap(),
        refill_tip(
            &hot,
            5 * MAX_REFILL_WINDOW,
            MAX_REFILL_WINDOW,
            tip_share(5) * MAX_REFILL_WINDOW,
            &back
        )
        .unwrap()
    );
}

/// A key the tail marks again while its window is in use keeps the new mark when its batch
/// answers (compare-and-clear with the mark read at refill); the batch's other keys clear.
#[tokio::test]
async fn key_marked_again_during_window_keeps_new_mark() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, coin, ids) = store(dir.path(), 8);
    let (ctx, local) = client_on(
        dir.path(),
        &hot,
        Arc::new(Mutex::new(tip_hash().to_b64url())),
    );
    let mut w = RefillWindow::default();
    w.set(
        refill_window(&hot, 4, 32, 2, &HashMap::new()).unwrap(),
        Instant::now(),
    );
    let first = w.next_batch(4);
    let st = reconcile_batch(&ctx, &local, &first, &Caps::default())
        .await
        .unwrap();
    assert_eq!(st.ok, 4);
    let second = w.next_batch(4);
    assert_eq!(second.len(), 4);
    // The tail marks one key of the second batch again, at a height the answer covers.
    let again = second[1];
    let (acct, ..) = balances::parse_bal_key(&again.key).unwrap();
    assert!(ids.contains(&acct));
    let new = {
        let mut txn = hot.rw().unwrap();
        let m = balances::mark_dirty(&mut txn, acct, coin, 0, again.mark.height).unwrap();
        txn.commit().unwrap();
        m
    };
    assert_ne!(new, again.mark);
    let st = reconcile_batch(&ctx, &local, &second, &Caps::default())
        .await
        .unwrap();
    assert_eq!(st.ok, 4);
    assert_eq!(mark_now(&hot, &again.key), Some(new), "the new mark stays");
    for d in first.iter().chain(&second).filter(|d| d.key != again.key) {
        assert_eq!(mark_now(&hot, &d.key), None, "an answered key is cleared");
    }
    // The row was written from the answer all the same.
    assert!(hot
        .ro()
        .unwrap()
        .get(Table::Balances, &again.key)
        .unwrap()
        .is_some());
    // The next refill takes it again.
    let next = refill_window(&hot, 4, 32, 2, &HashMap::new()).unwrap();
    assert_eq!(
        next,
        vec![Dirty {
            key: again.key,
            mark: new,
            seed: false,
            tip: true,
        }]
    );
}

/// The tip share still leads a window: the newest marks of a chain come in its first batches,
/// one tip share a batch (as a refill per batch takes them), ahead of an older backlog many
/// windows long.
#[test]
fn window_starts_with_the_tip_share() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, coin, _) = store(dir.path(), 400);
    // Three keys of another chain marked at the tip, newer than every backlog mark.
    let tip: Vec<[u8; 9]> = {
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let ids: Vec<u32> = (0..3)
            .map(|i| dict.intern(DictKind::Account, &format!("tip{i}")))
            .collect();
        dict.persist(&mut txn).unwrap();
        let keys = ids
            .iter()
            .map(|&a| {
                balances::mark_dirty(&mut txn, a, coin, 3, 5_000).unwrap();
                balances::bal_key(a, coin, 3)
            })
            .collect();
        txn.commit().unwrap();
        keys
    };
    let mut w = RefillWindow::default();
    w.set(
        refill_window(&hot, 16, 4, 8, &HashMap::new()).unwrap(),
        Instant::now(),
    );
    // Chain 0's newest backlog marks share the tip round-robin with chain 3: a batch of 16
    // has 4 tip slots, chain 3 takes 2 of them, as in a refill of one batch.
    let one: HashSet<[u8; 9]> = refill(&hot, 16, 4)
        .unwrap()
        .iter()
        .filter(|d| tip.contains(&d.key))
        .map(|d| d.key)
        .collect();
    assert_eq!(one.len(), 2);
    let first: HashSet<[u8; 9]> = w.next_batch(16).iter().map(|d| d.key).collect();
    assert!(one.is_subset(&first), "the tip marks of one refill");
    let second: HashSet<[u8; 9]> = w.next_batch(16).iter().map(|d| d.key).collect();
    for k in &tip {
        assert!(
            first.contains(k) || second.contains(k),
            "a tip mark in the window's first two batches"
        );
    }
    for _ in 2..8 {
        assert!(w.next_batch(16).iter().all(|d| !tip.contains(&d.key)));
    }
}

/// A window is refilled when it is used up or [`WINDOW_MAX_AGE`] old, not before; an empty
/// or cleared window is always due.
/// When the backlog runs out and tip keys fill the rest of a batch, those surplus tip keys
/// are marked `tip: false` (they go through `pick_batch` like a per-batch refill's), while the
/// batch's own tip share keeps `tip: true`.
#[test]
fn surplus_tip_keys_lose_the_tip_flag() {
    let k = |i: u8, tip: bool| Dirty {
        key: [i; 9],
        mark: Mark { height: 1, seq: 1 },
        seed: false,
        tip,
    };
    let mut w = RefillWindow::default();
    // 2 backlog keys and 10 tip keys; a batch of 8 has a tip share of 2.
    let mut keys: Vec<Dirty> = (0..10).map(|i| k(i, true)).collect();
    keys.extend((10..12).map(|i| k(i, false)));
    w.set(keys, Instant::now());
    let b = w.next_batch(8);
    assert_eq!(b.len(), 8);
    assert_eq!(b.iter().filter(|d| d.tip).count(), tip_share(8));
    assert!(b[..tip_share(8)].iter().all(|d| d.tip));
    assert!(b[tip_share(8)..].iter().all(|d| !d.tip), "surplus");
    assert_eq!(
        b.iter().map(|d| d.key[0]).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5, 10, 11],
        "the tip keys, then the whole backlog"
    );
}

#[test]
fn window_is_due_when_used_up_or_old() {
    let t = Instant::now();
    let mut w = RefillWindow::default();
    assert!(w.due(t, WINDOW_MAX_AGE));
    let d = |i: u8| Dirty {
        key: [i; 9],
        mark: Mark { height: 1, seq: 1 },
        seed: false,
        tip: false,
    };
    w.set(vec![d(1), d(2), d(3)], t);
    assert!(!w.due(t, WINDOW_MAX_AGE));
    assert_eq!(WINDOW_MAX_AGE, Duration::from_secs(5));
    assert!(!w.due(t + Duration::from_millis(4_999), WINDOW_MAX_AGE));
    assert!(w.due(t + Duration::from_millis(5_001), WINDOW_MAX_AGE));
    assert!(
        w.due(t + WINDOW_MAX_AGE, WINDOW_MAX_AGE),
        "old: refilled with keys left"
    );
    assert_eq!(w.next_batch(2), vec![d(1), d(2)]);
    assert!(!w.due(t, WINDOW_MAX_AGE));
    assert_eq!(w.left(), 1);
    assert_eq!(w.next_batch(2), vec![d(3)]);
    assert!(w.due(t, WINDOW_MAX_AGE), "used up");
    // A window refilled while a batch was in flight leaves that batch's keys out.
    w.set_without(vec![d(1), d(2), d(3), d(4)], t, &[d(2), d(4)]);
    assert_eq!(w.left(), 2);
    assert_eq!(w.next_batch(5), vec![d(1), d(3)]);
    w.set(vec![d(4)], t);
    w.clear();
    assert!(w.due(t, WINDOW_MAX_AGE));
    assert!(w.next_batch(1).is_empty());
}
/// A `/local` node for the bench: every call takes `delay`; `details` of `absent_pct` % of
/// the backlog accounts (`a…`, by a hash of the number) and of `tip_absent_pct` % of the tip
/// accounts (`t…`, [`mark_tip`]) is absent (a `try` marker in a batch, `row not found`
/// alone), every other one has a balance and an inline keyset. Answers read height 10 on our
/// stored tip. Two rates, because the newest marks can be far more often absent than the old
/// backlog (KDA mainnet: about 50% against 12%), and only then does a batch's composition
/// show in its absent rate.
struct BenchNode {
    delay: Duration,
    absent_pct: u64,
    tip_absent_pct: u64,
    calls: AtomicUsize,
    /// Calls per kind: batched `details`, one-key `details`, fungible checks, other.
    kinds: [AtomicUsize; 4],
    /// Keys asked in batched `details` calls.
    batched_keys: AtomicUsize,
}

impl BenchNode {
    fn new(delay: Duration, absent_pct: u64, tip_absent_pct: u64) -> Arc<BenchNode> {
        Arc::new(BenchNode {
            delay,
            absent_pct,
            tip_absent_pct,
            calls: AtomicUsize::new(0),
            kinds: Default::default(),
            batched_keys: AtomicUsize::new(0),
        })
    }

    fn absent(&self, acct: &str) -> bool {
        // Hashed: which keys a refill takes first follows the account number.
        let (pct, num) = match acct.strip_prefix('t') {
            Some(n) => (self.tip_absent_pct, n),
            None => (self.absent_pct, acct.trim_start_matches('a')),
        };
        let n = num.parse::<u64>().unwrap_or(0);
        (n.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) % 100 < pct
    }

    fn answer(&self, cmd: &Value) -> Value {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.delay);
        let meta = json!({"blockHeight": 11, "prevBlockHash": tip_hash().to_b64url()});
        let data = cmd
            .pointer("/payload/exec/data")
            .cloned()
            .unwrap_or(Value::Null);
        let entry = |a: &str| json!({"account": a, "balance": 1.5, "guard": {"keys": ["k1"], "pred": "keys-all"}});
        let ok = |d: Value| json!({"result": {"status": "success", "data": d}, "metaData": meta});
        let kind = if data.get("m").is_some() {
            2
        } else if data.get("as").is_some() {
            0
        } else if data.get("a").is_some() {
            1
        } else {
            3
        };
        self.kinds[kind].fetch_add(1, Ordering::Relaxed);
        if data.get("m").is_some() {
            return ok(json!(true));
        }
        if let Some(list) = data.get("as").and_then(Value::as_array) {
            self.batched_keys.fetch_add(list.len(), Ordering::Relaxed);
            let out: Vec<Value> = list
                .iter()
                .map(|a| {
                    let a = a.as_str().unwrap_or("");
                    if self.absent(a) {
                        json!({"account": a, "absent": true})
                    } else {
                        entry(a)
                    }
                })
                .collect();
            return ok(Value::Array(out));
        }
        let a = data.get("a").and_then(Value::as_str).unwrap_or("");
        if self.absent(a) {
            json!({"result": {"status": "failure",
                   "error": {"message": format!("with-read: row not found: {a}")}}})
        } else {
            ok(entry(a))
        }
    }
}

fn serve(node: Arc<BenchNode>) -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { return };
            let node = node.clone();
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 8192];
                let (head_end, len) = loop {
                    let n = s.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break (0, 0);
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (i + 4, len);
                    }
                };
                while head_end > 0 && buf.len() < head_end + len {
                    let n = s.read(&mut tmp).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let req: Value = serde_json::from_slice(&buf[head_end..]).unwrap_or(Value::Null);
                let cmd: Value = req
                    .get("cmd")
                    .and_then(Value::as_str)
                    .and_then(|c| serde_json::from_str(c).ok())
                    .unwrap_or(Value::Null);
                let body = node.answer(&cmd).to_string();
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            });
        }
    });
    format!("http://{addr}")
}

fn bench_client(url: &str, rps: u32) -> Arc<LocalClient> {
    let node = NodeClient::new(&kdx_node::NodeClientConfig {
        base: url.parse().unwrap(),
        network: "mainnet01".into(),
        timeout: Duration::from_secs(10),
        max_rps: 0,
        concurrency: 16,
        p2p_url: None,
        p2p_cert_sha256: None,
        max_body_bytes: 1 << 22,
        max_sse_event_bytes: 1 << 20,
    })
    .unwrap();
    Arc::new(LocalClient::new(
        node,
        LocalOpts {
            rps,
            concurrency: 8,
        },
    ))
}

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Our tail at height 10 on `chains` chains, with the header the nodes name: an absent answer
/// (`as_of` = the tail) then clears marks up to 10, as on a live host.
fn tail_at_10(hot: &HotStore, chains: u8) {
    let mut txn = hot.rw().unwrap();
    for c in 0..chains {
        kdx_hot::set_checkpoint(&mut txn, kdx_hot::Stage::Tail, c, 10, tip_hash()).unwrap();
    }
    txn.commit().unwrap();
    for c in 0..chains {
        put_header(hot, c, 10, tip_hash());
    }
}

/// Mark `per_chain` `coin` keys of tip accounts (`t0..`) on each of `chains` chains at height
/// 10, newer than every backlog mark (write sequence from `seq`): the newest marks, which the
/// tip share takes first. Run again with a higher `seq`, it marks them again (as the tail
/// would).
fn mark_tip(hot: &HotStore, chains: u8, per_chain: u32, seq: u64) -> Vec<[u8; 9]> {
    let mut txn = hot.rw().unwrap();
    let mut dict = HotDict::load(&txn).unwrap();
    let coin = dict.intern(DictKind::Module, "coin");
    let accts: Vec<u32> = (0..per_chain)
        .map(|i| dict.intern(DictKind::Account, &format!("t{i}")))
        .collect();
    dict.persist(&mut txn).unwrap();
    let mut keys = Vec::new();
    let mut s = seq;
    for c in 0..chains {
        for &a in &accts {
            let key = balances::bal_key(a, coin, c);
            let mark = Mark { height: 10, seq: s };
            s += 1;
            txn.put(Table::BalanceDirty, &key, &mark.encode()).unwrap();
            keys.push(key);
        }
    }
    txn.commit().unwrap();
    keys
}

/// What one [`drain`] run did.
#[derive(Default)]
struct Drained {
    done: u64,
    batches: u32,
    refills: u32,
    refill_secs: f64,
    tot: BatchStats,
    ph: Phases,
    /// Most tip-share keys in one batch.
    tip_max: usize,
}

/// `reconcile_loop`'s refill, batch and bookkeeping steps for windows of `k` batches of `n`
/// keys, a window taken again once `max_age` old, until `batches` batches ran, `secs`
/// seconds passed or the table is drained.
#[allow(clippy::too_many_arguments)]
async fn drain(
    hot: &Arc<HotStore>,
    ctx: &Ctx,
    primary: &LocalClient,
    pool: &ReadPool,
    caps: &Caps,
    (n, k): (usize, usize),
    max_age: Duration,
    (batches, secs): (u32, Duration),
) -> Drained {
    let mut r = Drained::default();
    let (mut back, mut fails, mut retries) = (HashMap::new(), HashMap::new(), HashMap::new());
    let mut w = RefillWindow::default();
    let refill =
        |b: &HashMap<[u8; 9], Mark>| spawn_refill(hot.clone(), n, caps.module_share, k, b.clone());
    let mut batch_secs = 0.0;
    let start = Instant::now();
    while r.batches < batches && start.elapsed() < secs {
        if w.due(Instant::now(), max_age) {
            let (keys, s, ended) = refill(&back).await.unwrap().unwrap();
            r.refill_secs += s;
            r.refills += 1;
            w.set(keys, ended);
        }
        let batch = w.next_batch(n);
        if batch.is_empty() {
            break;
        }
        r.tip_max = r.tip_max.max(batch.iter().filter(|d| d.tip).count());
        let next = (k > 1
            && (w.left() == 0
                || w.due(
                    Instant::now() + Duration::from_secs_f64(batch_secs),
                    max_age,
                )))
        .then(|| refill(&back));
        let t0 = Instant::now();
        let st = reconcile_batch_with(ctx, primary, Some(pool), &batch, caps)
            .await
            .unwrap();
        batch_secs = secs_since(t0);
        after_batch(hot, &mut fails, &mut retries, &batch, &st).unwrap();
        note_remarked(&mut back, &batch, &st);
        if let Some(next) = next {
            let (keys, s, ended) = next.await.unwrap().unwrap();
            r.refill_secs += s;
            r.refills += 1;
            w.set_without(keys, ended, &batch);
        }
        r.done += st.ok + st.absent + st.skipped + st.failures + st.dropped;
        r.batches += 1;
        let t = &mut r.tot;
        t.ok += st.ok;
        t.absent += st.absent;
        t.dropped += st.dropped;
        t.skipped += st.skipped;
        t.failures += st.failures;
        t.invalid += st.invalid;
        t.node_errors += st.node_errors;
        t.other_branch += st.other_branch;
        let (p, ph) = (st.phases, &mut r.ph);
        ph.prep += p.prep;
        ph.fungible += p.fungible;
        ph.reads_batched += p.reads_batched;
        ph.reads_rereads += p.reads_rereads;
        ph.txn_wait += p.txn_wait;
        ph.branch += p.branch;
        ph.write += p.write;
        ph.clear += p.clear;
        ph.commit += p.commit;
    }
    r
}

/// Three bench nodes (the primary first) at `rps` each, and the pool over them.
fn bench_pool(nodes: &[Arc<BenchNode>; 3], rps: u32) -> (Arc<LocalClient>, ReadPool) {
    let urls: Vec<String> = nodes.iter().map(|n| serve(n.clone())).collect();
    let primary = bench_client(&urls[0], rps);
    let pool = ReadPool::new(
        vec![
            ReadNode::new("n0".into(), primary.clone(), true),
            ReadNode::new("n1".into(), bench_client(&urls[1], rps), false),
            ReadNode::new("n2".into(), bench_client(&urls[2], rps), false),
        ],
        25,
    );
    (primary, pool)
}

/// More tip candidates than one batch holds (20 chains × 20 newest marks, a batch of 64):
/// every batch of a window takes at most its own tip share ([`TIP_SHARE_PCT`] of the batch),
/// and fills the rest from the old backlog, oldest first. A window that led with its whole
/// tip share sent batches of tip keys only (KDA-03 after PR #70: 1,024 of 1,024).
#[test]
fn every_batch_of_a_window_keeps_its_tip_share() {
    let dir = tempfile::tempdir().unwrap();
    let (hot, _, _) = store(dir.path(), 2_000);
    let tip: HashSet<[u8; 9]> = mark_tip(&hot, 20, 20, 1 << 40).into_iter().collect();
    let (n, k) = (64, 8);
    let share = n * TIP_SHARE_PCT / 100;
    // The old backlog in the order a one-batch refill takes it: oldest first.
    let mut oldest: Vec<Dirty> = refill_with(&hot, 2_000 + tip.len(), 1_000, &HashMap::new())
        .unwrap()
        .into_iter()
        .filter(|d| !tip.contains(&d.key))
        .collect();
    oldest.sort_by_key(|d| (d.mark, d.key));
    let mut w = RefillWindow::default();
    w.set(
        refill_window(&hot, n, 4, k, &HashMap::new()).unwrap(),
        Instant::now(),
    );
    let mut backlog = Vec::new();
    for i in 0..k {
        let b = w.next_batch(n);
        assert_eq!(b.len(), n, "batch {i} is full");
        let t = b.iter().filter(|d| tip.contains(&d.key)).count();
        assert!(t <= share, "batch {i}: {t} tip keys, more than {share}");
        assert_eq!(t, b.iter().filter(|d| d.tip).count());
        backlog.extend(b.into_iter().filter(|d| !d.tip));
    }
    // The rest is the oldest backlog, in order across the window's batches.
    let want: Vec<[u8; 9]> = oldest.iter().take(backlog.len()).map(|d| d.key).collect();
    let got: Vec<[u8; 9]> = backlog.iter().map(|d| d.key).collect();
    assert_eq!(got.len(), k * (n - share));
    assert_eq!(
        got.iter().collect::<HashSet<_>>(),
        want.iter().collect::<HashSet<_>>(),
        "the oldest backlog marks"
    );
    // The first batch's backlog keys are the ones a one-batch refill takes.
    let one: HashSet<[u8; 9]> = refill(&hot, n, 4)
        .unwrap()
        .into_iter()
        .filter(|d| !d.tip)
        .map(|d| d.key)
        .collect();
    assert_eq!(
        got[..n - share].iter().copied().collect::<HashSet<_>>(),
        one
    );
}

/// The reconcile loop's steps against three mock nodes (the primary first), with the newest
/// marks 60% absent and the old backlog 10%: a window of 8 batches drains like a window of 1.
/// It clears at least as much of the oldest backlog, its absent rate stays within a few
/// points, and the primary reads at most about the tip share of the batched keys (tip chunks
/// may go to any node, backlog chunks spare the primary). Windows are taken again before
/// every batch here (`max_age` 0, as when batches are slower than [`WINDOW_MAX_AGE`]), and
/// once at [`WINDOW_MAX_AGE`].
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_window_keeps_batch_composition_with_an_absent_heavy_tip() {
    const CHAINS: u8 = 20;
    const BACKLOG: u32 = 4_000;
    let (n, batches) = (64usize, 8u32);
    let run = |k: usize, max_age: Duration| async move {
        let dir = tempfile::tempdir().unwrap();
        let hot = Arc::new(HotStore::open(&dir.path().join("hot"), Geometry::default()).unwrap());
        tail_at_10(&hot, CHAINS);
        // The old backlog: `a0..` over 20 chains at heights 1..=9, write sequence in order.
        let backlog: Vec<([u8; 9], Mark)> = {
            let mut txn = hot.rw().unwrap();
            let mut dict = HotDict::load(&txn).unwrap();
            let coin = dict.intern(DictKind::Module, "coin");
            let accts: Vec<u32> = (0..BACKLOG / u32::from(CHAINS))
                .map(|a| dict.intern(DictKind::Account, &format!("a{a}")))
                .collect();
            dict.persist(&mut txn).unwrap();
            let mut out = Vec::new();
            for i in 0..BACKLOG {
                let a = accts[(i / u32::from(CHAINS)) as usize];
                let key = balances::bal_key(a, coin, (i % u32::from(CHAINS)) as u8);
                let mark = Mark {
                    height: 1 + i * 9 / BACKLOG,
                    seq: u64::from(i),
                };
                txn.put(Table::BalanceDirty, &key, &mark.encode()).unwrap();
                out.push((key, mark));
            }
            txn.commit().unwrap();
            out
        };
        mark_tip(&hot, CHAINS, 40, 1 << 40);
        let nodes = [
            BenchNode::new(Duration::from_millis(1), 10, 60),
            BenchNode::new(Duration::from_millis(1), 10, 60),
            BenchNode::new(Duration::from_millis(1), 10, 60),
        ];
        let (primary, pool) = bench_pool(&nodes, 500);
        let (ctx, _) = client_on(
            dir.path(),
            &hot,
            Arc::new(Mutex::new(tip_hash().to_b64url())),
        );
        let r = drain(
            &hot,
            &ctx,
            &primary,
            &pool,
            &Caps::default(),
            (n, k),
            max_age,
            (batches, Duration::from_secs(120)),
        )
        .await;
        assert_eq!(r.batches, batches);
        // Of the oldest backlog marks one batch's backlog share at a time would take, how
        // many are cleared.
        let oldest = batches as usize * (n - n * TIP_SHARE_PCT / 100);
        let ro = hot.ro().unwrap();
        let cleared = backlog[..oldest]
            .iter()
            .filter(|(key, _)| ro.get(Table::BalanceDirty, key).unwrap().is_none())
            .count();
        let keys: Vec<usize> = nodes
            .iter()
            .map(|n| n.batched_keys.load(Ordering::Relaxed))
            .collect();
        let absent = r.tot.absent as f64 / r.done.max(1) as f64;
        let primary_share = keys[0] as f64 / keys.iter().sum::<usize>().max(1) as f64;
        (r, cleared, absent, primary_share)
    };
    for max_age in [Duration::ZERO, WINDOW_MAX_AGE] {
        let (one, one_cleared, one_absent, _) = run(1, max_age).await;
        let (win, cleared, absent, primary_share) = run(8, max_age).await;
        let label = format!("max_age {max_age:?}");
        eprintln!(
            "{label}: window 1 cleared {one_cleared}, absent {one_absent:.3}; window 8 cleared \
             {cleared}, absent {absent:.3}, primary {primary_share:.3}, most tip keys {}",
            win.tip_max
        );
        assert!(
            win.tip_max <= n * TIP_SHARE_PCT / 100,
            "{label}: {} tip keys in one batch",
            win.tip_max
        );
        assert!(
            cleared >= one_cleared,
            "{label}: window 8 cleared {cleared} of the oldest backlog, window 1 {one_cleared}"
        );
        assert!(
            (absent - one_absent).abs() <= 0.05,
            "{label}: absent {absent:.3} with window 8, {one_absent:.3} with window 1"
        );
        assert!(
            primary_share <= 0.25,
            "{label}: the primary read {primary_share:.3} of the batched keys"
        );
        assert_eq!(one.tot.failures + win.tot.failures, 0);
    }
}

/// The drain bench (not a check): the reconcile loop's refill, batches and bookkeeping over a
/// large `BalanceDirty` against three mock `/local` nodes (the primary among them), each at
/// `local_rps` 50 and concurrency 8, every call `KDX_DRAIN_DELAY_MS` (20) long, with
/// `KDX_DRAIN_ABSENT_PCT` (12) % of the backlog keys absent. Before each run it marks
/// `KDX_DRAIN_TIP` (2,000) `coin` keys per chain at the tip, newest of all, `KDX_DRAIN_TIP_ABSENT_PCT`
/// (60) % of them absent: more tip candidates than a batch, absent-heavy, as on KDA mainnet.
/// Keys per second of the whole loop, per window size in `KDX_DRAIN_K` (`1,8`; 1 is the loop
/// before windows), and the batch's time per phase. The table (`KDX_DRAIN_ROWS`, 2,000,000)
/// is kept in `KDX_DRAIN_DIR` (a temp dir by default) for the next run.
///
/// It mirrors `reconcile_loop`'s steps (refill, prefetch, batch, `after_batch`,
/// `note_remarked`, `set_without`) in [`drain`] rather than calling the loop, so a change to
/// the loop's order must be copied there; `the_loop_drains_a_table_with_a_refill_window` runs
/// the loop itself.
///
/// `cargo test --release -p kdx-ingest drain_bench -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench: run by name"]
async fn drain_bench() {
    let rows: u32 = env("KDX_DRAIN_ROWS", 2_000_000);
    let secs: u64 = env("KDX_DRAIN_SECS", 20);
    let delay: u64 = env("KDX_DRAIN_DELAY_MS", 20);
    let absent_pct: u64 = env("KDX_DRAIN_ABSENT_PCT", 12);
    let tip_per_chain: u32 = env("KDX_DRAIN_TIP", 2_000);
    let tip_absent_pct: u64 = env("KDX_DRAIN_TIP_ABSENT_PCT", 60);
    let ks: Vec<usize> = env::<String>("KDX_DRAIN_K", "1,8".into())
        .split(',')
        .filter_map(|k| k.trim().parse().ok())
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let dir = std::env::var("KDX_DRAIN_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| tmp.path().to_path_buf());
    let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
    tail_at_10(&hot, 20);
    // Accounts `a0..`, 20 chains, about 2/3 `coin` rows and 400 other modules.
    const ACCOUNTS: u32 = 500_000;
    let have = balances::dirty_count(&hot.ro().unwrap()).unwrap() as u32;
    if have < rows {
        let t = Instant::now();
        let mut txn = hot.rw().unwrap();
        let mut dict = HotDict::load(&txn).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let mods: Vec<u32> = (0..400)
            .map(|m| dict.intern(DictKind::Module, &format!("free.m{m}")))
            .collect();
        let accts: Vec<u32> = (0..ACCOUNTS)
            .map(|a| dict.intern(DictKind::Account, &format!("a{a}")))
            .collect();
        dict.persist(&mut txn).unwrap();
        txn.commit().unwrap();
        let mut i = have;
        while i < rows {
            let mut txn = hot.rw().unwrap();
            for _ in 0..200_000 {
                if i >= rows {
                    break;
                }
                let a = accts[(i % ACCOUNTS) as usize];
                let c = ((i / ACCOUNTS) % 20) as u8;
                let m = if i.is_multiple_of(3) {
                    mods[(i % 400) as usize]
                } else {
                    coin
                };
                let mark = Mark {
                    height: 1 + i % 10,
                    seq: u64::from(i),
                };
                txn.put(
                    Table::BalanceDirty,
                    &balances::bal_key(a, m, c),
                    &mark.encode(),
                )
                .unwrap();
                i += 1;
            }
            txn.commit().unwrap();
        }
        eprintln!("filled {rows} dirty rows in {:?}", t.elapsed());
    }
    let nodes = [
        BenchNode::new(Duration::from_millis(delay), absent_pct, tip_absent_pct),
        BenchNode::new(Duration::from_millis(delay), absent_pct, tip_absent_pct),
        BenchNode::new(Duration::from_millis(delay), absent_pct, tip_absent_pct),
    ];
    let (primary, pool) = bench_pool(&nodes, 50);
    let (ctx, _) = client_on(&dir, &hot, Arc::new(Mutex::new(tip_hash().to_b64url())));
    let caps = Caps::default();
    for (run, k) in ks.into_iter().enumerate() {
        if tip_per_chain > 0 {
            mark_tip(&hot, 20, tip_per_chain, (1 << 40) + ((run as u64) << 32));
        }
        let calls0: Vec<usize> = nodes
            .iter()
            .map(|n| n.calls.load(Ordering::Relaxed))
            .collect();
        let keys0: Vec<usize> = nodes
            .iter()
            .map(|n| n.batched_keys.load(Ordering::Relaxed))
            .collect();
        let start = Instant::now();
        let r = drain(
            &hot,
            &ctx,
            &primary,
            &pool,
            &caps,
            (1024, k),
            WINDOW_MAX_AGE,
            (u32::MAX, Duration::from_secs(secs)),
        )
        .await;
        let wall = start.elapsed().as_secs_f64();
        let calls: Vec<usize> = nodes
            .iter()
            .zip(&calls0)
            .map(|(n, c0)| n.calls.load(Ordering::Relaxed) - c0)
            .collect();
        let keys: Vec<usize> = nodes
            .iter()
            .zip(&keys0)
            .map(|(n, c0)| n.batched_keys.load(Ordering::Relaxed) - c0)
            .collect();
        let b = f64::from(r.batches.max(1));
        let kinds: Vec<Vec<usize>> = nodes
            .iter()
            .map(|n| n.kinds.iter().map(|c| c.load(Ordering::Relaxed)).collect())
            .collect();
        let tot = &r.tot;
        eprintln!(
            "  totals: ok {} absent {} dropped {} skipped {} failures {} invalid {} node_errors {} \
             other_branch {}; calls [batched, single, fungible, other] per node so far {kinds:?}",
            tot.ok,
            tot.absent,
            tot.dropped,
            tot.skipped,
            tot.failures,
            tot.invalid,
            tot.node_errors,
            tot.other_branch
        );
        eprintln!(
            "K={k}: {} keys in {wall:.1} s = {:.0} keys/s; absent {:.1}%; {} batches, {} \
             refills ({:.2} s each); most tip keys in a batch {}; calls per node {calls:?}; \
             batched keys per node {keys:?}",
            r.done,
            r.done as f64 / wall,
            100.0 * tot.absent as f64 / r.done.max(1) as f64,
            r.batches,
            r.refills,
            r.refill_secs / f64::from(r.refills.max(1)),
            r.tip_max,
        );
        let ph = &r.ph;
        eprintln!(
            "  per batch: prep {:.3} fungible {:.3} reads_batched {:.3} reads_rereads {:.3} \
             txn_wait {:.3} branch {:.3} write {:.3} clear {:.3} commit {:.3}",
            ph.prep / b,
            ph.fungible / b,
            ph.reads_batched / b,
            ph.reads_rereads / b,
            ph.txn_wait / b,
            ph.branch / b,
            ph.write / b,
            ph.clear / b,
            ph.commit / b,
        );
    }
}
