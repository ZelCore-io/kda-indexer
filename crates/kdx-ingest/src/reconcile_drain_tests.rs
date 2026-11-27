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

/// A window of `k` batches is one refill of `k × n` keys: the same keys in the same order,
/// handed out `n` at a time, each key once and with exactly the mark in the table.
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
    assert_eq!(all, keys);
    assert_eq!(w.left(), 0);
    assert!(w.next_batch(20).is_empty());
    assert!(w.due(Instant::now(), WINDOW_MAX_AGE));
    // `k` is clamped to 1..=MAX_REFILL_WINDOW.
    assert_eq!(refill_window(&hot, 20, 4, 0, &back).unwrap().len(), 20);
    assert_eq!(
        refill_window(&hot, 5, 1, 1000, &back).unwrap(),
        refill_with(&hot, 5 * MAX_REFILL_WINDOW, MAX_REFILL_WINDOW, &back).unwrap()
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
            seed: false
        }]
    );
}

/// The tip share still leads a window: the newest marks of a chain come in its first batch,
/// ahead of an older backlog many windows long.
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
    let first: HashSet<[u8; 9]> = w.next_batch(16).iter().map(|d| d.key).collect();
    for k in &tip {
        assert!(first.contains(k), "a tip mark in the window's first batch");
    }
    for _ in 1..8 {
        assert!(w.next_batch(16).iter().all(|d| !tip.contains(&d.key)));
    }
}

/// A window is refilled when it is used up or [`WINDOW_MAX_AGE`] old, not before; an empty
/// or cleared window is always due.
#[test]
fn window_is_due_when_used_up_or_old() {
    let t = Instant::now();
    let mut w = RefillWindow::default();
    assert!(w.due(t, WINDOW_MAX_AGE));
    let d = |i: u8| Dirty {
        key: [i; 9],
        mark: Mark { height: 1, seq: 1 },
        seed: false,
    };
    w.set(vec![d(1), d(2), d(3)], t);
    assert!(!w.due(t, WINDOW_MAX_AGE));
    assert!(!w.due(t + Duration::from_millis(4_999), WINDOW_MAX_AGE));
    assert!(
        w.due(t + WINDOW_MAX_AGE, WINDOW_MAX_AGE),
        "old: refilled with keys left"
    );
    assert_eq!(w.next_batch(2), vec![d(1), d(2)]);
    assert!(!w.due(t, WINDOW_MAX_AGE));
    assert_eq!(w.left(), 1);
    assert_eq!(w.next_batch(2), vec![d(3)]);
    assert!(w.due(t, WINDOW_MAX_AGE), "used up");
    w.set(vec![d(4)], t);
    w.clear();
    assert!(w.due(t, WINDOW_MAX_AGE));
    assert!(w.next_batch(1).is_empty());
}

/// A `/local` node for the bench: every call takes `delay`; `details` of `absent_pct` % of
/// the accounts (by a hash of the number) is absent (a `try` marker in a batch, `row not
/// found` alone), every other one has a balance and an inline keyset. Answers read height
/// 10 on our stored tip.
struct BenchNode {
    delay: Duration,
    absent_pct: u64,
    calls: AtomicUsize,
    /// Calls per kind: batched `details`, one-key `details`, fungible checks, other.
    kinds: [AtomicUsize; 4],
}

impl BenchNode {
    fn absent(&self, acct: &str) -> bool {
        // Hashed: which keys a refill takes first follows the account number.
        let n = acct.trim_start_matches('a').parse::<u64>().unwrap_or(0);
        (n.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) % 100 < self.absent_pct
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

/// The drain bench (not a check): the reconcile loop's refill, batches and bookkeeping over a
/// large `BalanceDirty` against three mock `/local` nodes (the primary among them), each at
/// `local_rps` 50 and concurrency 8, every call `KDX_DRAIN_DELAY_MS` (20) long, with
/// `KDX_DRAIN_ABSENT_PCT` (12) % of the keys absent. Keys per second of the whole loop, per
/// window size in `KDX_DRAIN_K` (`1,8`; 1 is the loop before windows), and the batch's time
/// per phase. The table (`KDX_DRAIN_ROWS`, 2,000,000) is kept in `KDX_DRAIN_DIR` (a temp dir
/// by default) for the next run.
///
/// `cargo test --release -p kdx-ingest drain_bench -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bench: run by name"]
async fn drain_bench() {
    let rows: u32 = env("KDX_DRAIN_ROWS", 2_000_000);
    let secs: u64 = env("KDX_DRAIN_SECS", 20);
    let delay: u64 = env("KDX_DRAIN_DELAY_MS", 20);
    let absent_pct: u64 = env("KDX_DRAIN_ABSENT_PCT", 12);
    let ks: Vec<usize> = env::<String>("KDX_DRAIN_K", "1,8".into())
        .split(',')
        .filter_map(|k| k.trim().parse().ok())
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let dir = std::env::var("KDX_DRAIN_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| tmp.path().to_path_buf());
    let hot = Arc::new(HotStore::open(&dir.join("hot"), Geometry::default()).unwrap());
    // Our tail at height 10 on every chain, with the header the nodes name: an absent answer
    // (`as_of` = the tail) then clears marks up to 10, as on a live host.
    {
        let mut txn = hot.rw().unwrap();
        for c in 0..20u8 {
            kdx_hot::set_checkpoint(&mut txn, kdx_hot::Stage::Tail, c, 10, tip_hash()).unwrap();
        }
        txn.commit().unwrap();
        for c in 0..20u8 {
            put_header(&hot, c, 10, tip_hash());
        }
    }
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
                let m = if i % 3 == 0 {
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
    let mk = || {
        Arc::new(BenchNode {
            delay: Duration::from_millis(delay),
            absent_pct,
            calls: AtomicUsize::new(0),
            kinds: Default::default(),
        })
    };
    let nodes = [mk(), mk(), mk()];
    let urls: Vec<String> = nodes.iter().map(|n| serve(n.clone())).collect();
    let primary = bench_client(&urls[0], 50);
    let (ctx, _) = client_on(&dir, &hot, Arc::new(Mutex::new(tip_hash().to_b64url())));
    let pool = ReadPool::new(
        vec![
            ReadNode::new("n0".into(), primary.clone(), true),
            ReadNode::new("n1".into(), bench_client(&urls[1], 50), false),
            ReadNode::new("n2".into(), bench_client(&urls[2], 50), false),
        ],
        25,
    );
    let caps = Caps::default();
    for k in ks {
        let (mut done, mut batches, mut refills, mut refill_secs) = (0u64, 0u32, 0u32, 0.0f64);
        let mut ph = Phases::default();
        let mut tot = BatchStats::default();
        let (mut back, mut fails, mut retries) = (HashMap::new(), HashMap::new(), HashMap::new());
        let mut w = RefillWindow::default();
        let calls0: Vec<usize> = nodes
            .iter()
            .map(|n| n.calls.load(Ordering::Relaxed))
            .collect();
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(secs) {
            if w.due(Instant::now(), WINDOW_MAX_AGE) {
                let (h, b) = (hot.clone(), std::mem::take(&mut back));
                let t = Instant::now();
                let (keys, b) = tokio::task::spawn_blocking(move || {
                    refill_window(&h, 1024, caps.module_share, k, &b).map(|r| (r, b))
                })
                .await
                .unwrap()
                .unwrap();
                back = b;
                refill_secs += secs_since(t);
                refills += 1;
                w.set(keys, Instant::now());
            }
            let batch = w.next_batch(1024);
            if batch.is_empty() {
                break;
            }
            let st = reconcile_batch_with(&ctx, &primary, Some(&pool), &batch, &caps)
                .await
                .unwrap();
            after_batch(&hot, &mut fails, &mut retries, &batch, &st).unwrap();
            note_remarked(&mut back, &batch, &st);
            done += st.ok + st.absent + st.skipped + st.failures + st.dropped;
            batches += 1;
            tot.ok += st.ok;
            tot.absent += st.absent;
            tot.dropped += st.dropped;
            tot.skipped += st.skipped;
            tot.failures += st.failures;
            tot.invalid += st.invalid;
            tot.node_errors += st.node_errors;
            tot.other_branch += st.other_branch;
            let p = st.phases;
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
        let wall = start.elapsed().as_secs_f64();
        let calls: Vec<usize> = nodes
            .iter()
            .zip(&calls0)
            .map(|(n, c0)| n.calls.load(Ordering::Relaxed) - c0)
            .collect();
        let b = f64::from(batches.max(1));
        let kinds: Vec<Vec<usize>> = nodes
            .iter()
            .map(|n| n.kinds.iter().map(|c| c.load(Ordering::Relaxed)).collect())
            .collect();
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
            "K={k}: {done} keys in {wall:.1} s = {:.0} keys/s; {batches} batches, {refills} \
             refills ({:.2} s each); calls per node {calls:?}",
            done as f64 / wall,
            refill_secs / f64::from(refills.max(1)),
        );
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
