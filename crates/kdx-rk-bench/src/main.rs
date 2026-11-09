//! `kda-rk-bench` (B6, formerly `kda-indexer rk-bench`): PtrHash MPHF + slot table versus a sorted prefix array for the
//! request-key index (spec §3.7).
//!
//! Keys: the real user-tx request keys of the records files, padded with
//! `blake2b-256(counter)` up to `--synthetic`. Structures:
//! (a) `ptr_hash` minimal perfect hash over the 8-byte key prefix, slot table
//!     `(TxNum offset u32, fingerprint u32)` in MPHF order;
//! (b) sorted array of `(rk prefix 8 B, TxNum 5 B)`, binary-searched.
//! "Probe" is the structure alone (candidate TxNum or none after the fingerprint / prefix check).
//! "Confirmed" adds the comparison with the full 32-byte key held in a separate array, which
//! stands in for the key stored in the record.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::bail;
use kdx_types::blake2b_256;
use kdx_types::record::decode_tx;
use ptr_hash::{DefaultPtrHash, PtrHashParams};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde_json::{json, Value};

use clap::Parser;
use kda_indexer::cmd::frames::{read_frames, KIND_USER, KIND_USER_LEGACY};

const QUERIES: usize = 1_000_000;

/// Request-key index benchmark (B6). A separate binary so the release `kda-indexer` does not
/// link `ptr_hash`'s non-optional MPL-2.0 dependency (`colored`); not a default workspace member.
#[derive(Parser)]
#[command(name = "kda-rk-bench")]
struct RkBenchArgs {
    /// Records file(s) from `kda-indexer dump`; their real request keys are used (repeatable).
    #[arg(long, required = true)]
    records: Vec<PathBuf>,
    /// Total key count: real keys padded with blake2b-256(counter).
    #[arg(long, default_value_t = 3_000_000)]
    synthetic: usize,
    /// JSON report path.
    #[arg(long)]
    out: PathBuf,
}

fn main() -> anyhow::Result<()> {
    run(RkBenchArgs::parse())
}

type Key = [u8; 32];

fn prefix(k: &Key) -> u64 {
    u64::from_le_bytes(k[..8].try_into().unwrap())
}

fn fingerprint(k: &Key) -> u32 {
    u32::from_le_bytes(k[8..12].try_into().unwrap())
}

struct Ptr {
    mph: DefaultPtrHash,
    slots: Vec<(u32, u32)>,
}

impl Ptr {
    fn probe(&self, k: &Key) -> Option<u32> {
        let i = self.mph.index(&prefix(k));
        let (off, fp) = self.slots[i];
        (fp == fingerprint(k)).then_some(off)
    }
}

struct Sorted {
    /// `[prefix 8 B big-endian][TxNum 5 B little-endian]`, sorted by prefix.
    entries: Vec<[u8; 13]>,
}

impl Sorted {
    fn probe(&self, k: &Key) -> Option<u32> {
        let p = &k[..8];
        let i = self.entries.partition_point(|e| e[..8] < *p);
        let e = self.entries.get(i)?;
        if e[..8] != *p {
            return None;
        }
        let mut n = [0u8; 8];
        n[..5].copy_from_slice(&e[8..13]);
        Some(u64::from_le_bytes(n) as u32)
    }
}

struct Timing {
    /// Independent queries in a tight loop (the CPU overlaps cache misses).
    mean_ns: f64,
    /// Mean and p99 of single timed queries (timer resolution is about 42 ns on Apple silicon,
    /// the mean timer-pair cost is subtracted).
    timed_mean_ns: f64,
    p99_ns: f64,
    confirmed_mean_ns: f64,
}

fn percentile99(v: &mut [u32]) -> f64 {
    v.sort_unstable();
    v[(v.len() as f64 * 0.99) as usize] as f64
}

/// Cost of one `Instant::now()` pair, subtracted from the per-op samples.
fn timer_overhead() -> f64 {
    let total: u64 = (0..200_000)
        .map(|_| {
            let t = Instant::now();
            black_box(());
            t.elapsed().as_nanos() as u64
        })
        .sum();
    total as f64 / 200_000.0
}

fn time_queries(
    qs: &[Key],
    probe: &dyn Fn(&Key) -> Option<u32>,
    keys: &[Key],
    overhead: f64,
) -> Timing {
    let t = Instant::now();
    let mut acc = 0u64;
    for q in qs {
        if let Some(v) = probe(q) {
            acc = acc.wrapping_add(v as u64);
        }
    }
    black_box(acc);
    let mean_ns = t.elapsed().as_nanos() as f64 / qs.len() as f64;

    let mut samples: Vec<u32> = Vec::with_capacity(qs.len());
    for q in qs {
        let t = Instant::now();
        black_box(probe(q));
        samples.push(t.elapsed().as_nanos() as u32);
    }
    let timed_mean_ns =
        (samples.iter().map(|&x| x as f64).sum::<f64>() / samples.len() as f64 - overhead).max(0.0);
    let p99_ns = (percentile99(&mut samples) - overhead).max(0.0);

    let t = Instant::now();
    let mut acc = 0u64;
    for q in qs {
        if let Some(v) = probe(q) {
            if keys[v as usize] == *q {
                acc = acc.wrapping_add(v as u64);
            }
        }
    }
    black_box(acc);
    let confirmed_mean_ns = t.elapsed().as_nanos() as f64 / qs.len() as f64;
    Timing {
        mean_ns,
        timed_mean_ns,
        p99_ns,
        confirmed_mean_ns,
    }
}

#[allow(clippy::too_many_arguments)]
fn report(
    name: &str,
    build_s: f64,
    bytes_per_key: f64,
    probe: &dyn Fn(&Key) -> Option<u32>,
    keys: &[Key],
    hits: &[(Key, u32)],
    misses: &[Key],
    overhead: f64,
) -> Value {
    let hit_keys: Vec<Key> = hits.iter().map(|h| h.0).collect();
    let th = time_queries(&hit_keys, probe, keys, overhead);
    let tm = time_queries(misses, probe, keys, overhead);
    let wrong_hits = hits
        .iter()
        .filter(|(k, want)| probe(k).filter(|&o| keys[o as usize] == *k) != Some(*want))
        .count();
    let miss_after_probe = misses.iter().filter(|k| probe(k).is_some()).count();
    let miss_after_confirm = misses
        .iter()
        .filter(|k| probe(k).is_some_and(|o| keys[o as usize] == **k))
        .count();
    json!({
        "structure": name,
        "build_seconds": (build_s * 1000.0).round() / 1000.0,
        "bytes_per_key": (bytes_per_key * 100.0).round() / 100.0,
        "hit_probe_throughput_mean_ns": th.mean_ns.round(),
        "hit_probe_timed_mean_ns": th.timed_mean_ns.round(),
        "hit_probe_timed_p99_ns": th.p99_ns.round(),
        "hit_confirmed_throughput_mean_ns": th.confirmed_mean_ns.round(),
        "miss_probe_throughput_mean_ns": tm.mean_ns.round(),
        "miss_probe_timed_mean_ns": tm.timed_mean_ns.round(),
        "miss_probe_timed_p99_ns": tm.p99_ns.round(),
        "miss_confirmed_throughput_mean_ns": tm.confirmed_mean_ns.round(),
        "wrong_hit_answers": wrong_hits,
        "miss_false_positives_after_fingerprint_or_prefix": miss_after_probe,
        "miss_false_positives_after_full_key": miss_after_confirm,
    })
}

fn run(args: RkBenchArgs) -> anyhow::Result<()> {
    // Real keys.
    let mut real: Vec<Key> = Vec::new();
    for p in &args.records {
        for (kind, bytes) in read_frames(p)? {
            if kind == KIND_USER || kind == KIND_USER_LEGACY {
                if let Some(h) = decode_tx(&bytes)?.request_key {
                    real.push(*h.as_bytes());
                }
            }
        }
    }
    if real.is_empty() {
        bail!("no real request keys in the records files");
    }
    let n_real = real.len();
    // Pad with blake2b-256(counter); drop keys whose 8-byte prefix is already taken (the
    // MPHF input must be distinct; the production index would reseed instead).
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut keys: Vec<Key> = Vec::with_capacity(args.synthetic.max(n_real));
    let mut prefix_collisions = 0usize;
    for k in real {
        if seen.insert(prefix(&k)) {
            keys.push(k);
        } else {
            prefix_collisions += 1;
        }
    }
    let n_real_kept = keys.len();
    let mut counter = 0u64;
    while keys.len() < args.synthetic {
        let k = *blake2b_256(&counter.to_le_bytes()).as_bytes();
        counter += 1;
        if seen.insert(prefix(&k)) {
            keys.push(k);
        } else {
            prefix_collisions += 1;
        }
    }
    let n = keys.len();
    eprintln!(
        "rk-bench: {n} keys ({n_real_kept} real, {} synthetic)",
        n - n_real_kept
    );

    // Queries.
    let mut rng = StdRng::seed_from_u64(0x6b64615f696e6478);
    let hits: Vec<(Key, u32)> = (0..QUERIES)
        .map(|_| {
            let i = rng.random_range(0..n);
            (keys[i], i as u32)
        })
        .collect();
    let misses: Vec<Key> = (0..QUERIES as u64)
        .map(|i| *blake2b_256(&(u64::MAX - i).to_le_bytes()).as_bytes())
        .collect();
    let overhead = timer_overhead();

    // (a) PtrHash + slot table.
    let t = Instant::now();
    let pk: Vec<u64> = keys.iter().map(prefix).collect();
    let mph = <DefaultPtrHash>::new(&pk, PtrHashParams::default());
    let mut slots = vec![(0u32, 0u32); n];
    for (i, k) in keys.iter().enumerate() {
        slots[mph.index(&prefix(k))] = (i as u32, fingerprint(k));
    }
    let build_a = t.elapsed().as_secs_f64();
    let (b1, b2) = mph.bits_per_element();
    let mph_bytes_per_key = (b1 + b2) / 8.0;
    let ptr = Ptr { mph, slots };
    let ra = report(
        "ptrhash + slot table (offset u32, fingerprint u32)",
        build_a,
        mph_bytes_per_key + 8.0,
        &|k| ptr.probe(k),
        &keys,
        &hits,
        &misses,
        overhead,
    );

    // (b) sorted array.
    let t = Instant::now();
    let mut entries: Vec<[u8; 13]> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let mut e = [0u8; 13];
            e[..8].copy_from_slice(&k[..8]);
            e[8..13].copy_from_slice(&(i as u64).to_le_bytes()[..5]);
            e
        })
        .collect();
    entries.sort_unstable_by(|a, b| a[..8].cmp(&b[..8]));
    let build_b = t.elapsed().as_secs_f64();
    let sorted = Sorted { entries };
    let rb = report(
        "sorted array (prefix 8 B, TxNum 5 B)",
        build_b,
        13.0,
        &|k| sorted.probe(k),
        &keys,
        &hits,
        &misses,
        overhead,
    );

    let wrong = |r: &Value| {
        r["wrong_hit_answers"].as_u64().unwrap_or(0)
            + r["miss_false_positives_after_full_key"]
                .as_u64()
                .unwrap_or(0)
    };
    let report = json!({
        "keys": n,
        "real_keys": n_real_kept,
        "synthetic_keys": n - n_real_kept,
        "prefix_collisions_dropped": prefix_collisions,
        "queries_per_kind": QUERIES,
        "timer_overhead_ns": overhead,
        "mphf_bytes_per_key": (mph_bytes_per_key * 1000.0).round() / 1000.0,
        "results": [ra, rb],
        "wrong_answers_total": wrong(&ra) + wrong(&rb),
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(d) = args.out.parent() {
        std::fs::create_dir_all(d)?;
    }
    std::fs::write(&args.out, serde_json::to_vec_pretty(&report)?)?;
    if wrong(&ra) + wrong(&rb) > 0 {
        bail!("wrong answers found");
    }
    Ok(())
}
