//! `probe` command: node route availability (B1), network id (B2), batch behaviour and limits (B11).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use futures::StreamExt;
use rand::RngExt;
use serde::Serialize;
use serde_json::Value;

use crate::config::Config;
use kdx_node::{NodeClient, NodeError};

const BATCH_SIZES: [usize; 6] = [50, 100, 200, 500, 1000, 2000];
const SAMPLE_HEADERS: u64 = 2000;

#[derive(Debug, Serialize)]
pub struct RouteProbe {
    pub route: String,
    /// "service" or "p2p".
    pub base: String,
    /// HTTP status, 0 for transport errors or timeouts.
    pub status: u16,
    pub ok: bool,
    pub ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BatchAttempt {
    pub n: usize,
    pub status: u16,
    pub returned: usize,
    pub ms: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchProbe {
    /// True if a hash sent twice in one request comes back exactly once.
    pub dup_hashes_returned_once: bool,
    /// Entries returned for `[h1, h1, h2]`.
    pub dup_entries_returned: usize,
    /// "omitted", "error <status>" or "null entry".
    pub missing_hash_behaviour: String,
    /// For `[h1, <missing>, h2]`: payload hashes of the returned entries in order (null entries as null).
    pub mixed_order_returned: Vec<Option<String>>,
    /// Largest batch that returned 2xx with one entry per (distinct) hash.
    pub max_ok_batch: u32,
    pub first_fail_batch: Option<u32>,
    pub fail_status: Option<u16>,
    /// Set when a 2xx response held fewer entries than hashes sent (silent truncation).
    pub silent_cap_entries: Option<usize>,
    pub attempts: Vec<BatchAttempt>,
}

#[derive(Debug, Serialize)]
pub struct ProbeReport {
    pub network_from_info: String,
    pub config_network_matches: bool,
    pub node_version: String,
    pub chains_at_tip: usize,
    pub chains_below_graph_change: usize,
    pub routes: Vec<RouteProbe>,
    pub block_route: bool,
    /// Items returned by header/branch for lower = tip-10, upper = tip (expected 10).
    pub branch_items: usize,
    /// `block` over a 1000-height window: items in the first page and whether a `next` token came back.
    pub block_page_items: usize,
    pub block_page_has_next: bool,
    pub batch: BatchProbe,
    pub notes: Vec<String>,
}

pub async fn run(cfg: &Config, smoke: bool, out: Option<PathBuf>) -> anyhow::Result<()> {
    if smoke {
        return smoke_run(cfg).await;
    }
    let client = super::node_client(cfg)?;
    let info = client.info().await.context("info")?;
    let matches = info.chainweb_version == cfg.network;
    if !matches {
        eprintln!(
            "network mismatch: config says {:?}, node /info says {:?}",
            cfg.network, info.chainweb_version
        );
        std::process::exit(2);
    }
    let graph = kdx_node::ChainGraph::from_info(&info);
    let cut = client.cut().await.context("cut")?;
    let tip = cut.hashes.get(&0).context("cut has no chain 0")?.height;

    let mut notes = Vec::new();
    let mut routes = Vec::new();

    // Sample: the most recent 2000 headers of chain 0, ending just below the tip.
    let hi = tip.saturating_sub(1);
    let lo = hi.saturating_sub(SAMPLE_HEADERS - 1);
    let headers = client
        .headers_all(0, lo, hi)
        .await
        .context("sample headers")?;
    let mut hashes: Vec<String> = Vec::new();
    for h in &headers {
        if let Some(p) = h.get("payloadHash").and_then(Value::as_str) {
            if !hashes.iter().any(|x| x == p) {
                hashes.push(p.to_string());
            }
        }
    }
    if hashes.len() < SAMPLE_HEADERS as usize {
        notes.push(format!("sample has {} distinct payload hashes from {} headers; batch sizes above that are capped", hashes.len(), headers.len()));
    }

    routes.extend(probe_routes(&client, "service", tip, &hashes, &mut notes).await);
    if let Some(p2p) = client.p2p_client() {
        routes.extend(probe_routes(&p2p, "p2p", tip, &hashes, &mut notes).await);
    } else {
        notes.push("no p2p_url configured: P2P fallback not probed".to_string());
    }
    let block_route = routes
        .iter()
        .any(|r| r.route == "block" && r.base == "service" && r.ok);

    // header/branch: expect 10 items between tip-10 (lower) and tip (upper).
    let seg = client
        .headers_all(0, tip - 10, tip)
        .await
        .context("branch sample")?;
    let branch_items = match (seg.first(), seg.last()) {
        (Some(l), Some(u)) => {
            let lower = vec![l["hash"].as_str().unwrap_or_default().to_string()];
            let upper = vec![u["hash"].as_str().unwrap_or_default().to_string()];
            client
                .header_branch(0, &lower, &upper, None, None)
                .await
                .map(|p| p.items.len())
                .unwrap_or(0)
        }
        _ => 0,
    };

    let (block_page_items, block_page_has_next) = match client.block(0, tip - 1001, tip - 1).await {
        Ok(Some(p)) => (p.items.len(), p.next.is_some()),
        _ => (0, false),
    };

    let batch = probe_batch(&client, &hashes, &mut notes).await?;

    let report = ProbeReport {
        network_from_info: info.chainweb_version.clone(),
        config_network_matches: matches,
        node_version: info.node_version.clone(),
        chains_at_tip: graph.chains_at(cut.height).len(),
        chains_below_graph_change: graph
            .chains_at(
                graph
                    .change_heights()
                    .get(1)
                    .copied()
                    .unwrap_or(1)
                    .saturating_sub(1),
            )
            .len(),
        routes,
        block_route,
        branch_items,
        block_page_items,
        block_page_has_next,
        batch,
        notes,
    };
    let path =
        out.unwrap_or_else(|| PathBuf::from(format!("test-runs/probe-{}.json", cfg.network)));
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    print_summary(&report, &path);
    Ok(())
}

fn status_of<T>(r: &Result<T, NodeError>) -> u16 {
    match r {
        Ok(_) => 200,
        Err(NodeError::Http { status, .. }) => *status,
        Err(_) => 0,
    }
}

fn route(name: &str, base: &str, status: u16, ok: bool, t0: Instant) -> RouteProbe {
    RouteProbe {
        route: name.to_string(),
        base: base.to_string(),
        status,
        ok,
        ms: t0.elapsed().as_millis() as u64,
    }
}

/// Request key of the first transaction in a payload-with-outputs value.
fn first_request_key(payload: &Value) -> Option<String> {
    let txs = payload.get("transactions")?.as_array()?;
    for pair in txs {
        let out_b64 = pair.as_array()?.get(1)?.as_str()?;
        let bytes = URL_SAFE_NO_PAD
            .decode(out_b64)
            .or_else(|_| URL_SAFE.decode(out_b64))
            .ok()?;
        let v: Value = serde_json::from_slice(&bytes).ok()?;
        if let Some(k) = v.get("reqKey").and_then(Value::as_str) {
            return Some(k.to_string());
        }
    }
    None
}

async fn probe_routes(
    c: &NodeClient,
    base: &str,
    tip: u64,
    hashes: &[String],
    notes: &mut Vec<String>,
) -> Vec<RouteProbe> {
    let mut out = Vec::new();
    let t = Instant::now();
    let r = c.info().await;
    out.push(route("info", base, status_of(&r), r.is_ok(), t));
    let t = Instant::now();
    let r = c.cut().await;
    out.push(route("cut", base, status_of(&r), r.is_ok(), t));
    let t = Instant::now();
    let hdrs = c.headers(0, tip - 12, tip - 2, Some(10), None).await;
    out.push(route("header", base, status_of(&hdrs), hdrs.is_ok(), t));
    if let Ok(p) = &hdrs {
        if let (Some(l), Some(u)) = (p.items.first(), p.items.last()) {
            let t = Instant::now();
            let lower = vec![l["hash"].as_str().unwrap_or_default().to_string()];
            let upper = vec![u["hash"].as_str().unwrap_or_default().to_string()];
            let r = c.header_branch(0, &lower, &upper, None, None).await;
            out.push(route("header/branch", base, status_of(&r), r.is_ok(), t));
        }
    }
    let sample: Vec<String> = hashes.iter().take(5).cloned().collect();
    let t = Instant::now();
    let batch = c.payload_batch_raw(0, &sample).await;
    out.push(route(
        "payload/outputs/batch",
        base,
        status_of(&batch),
        batch.is_ok(),
        t,
    ));
    let t = Instant::now();
    let single = match sample.first() {
        Some(h) => c.payload_with_outputs(0, h).await,
        None => Err(NodeError::Missing {
            hash: String::new(),
        }),
    };
    out.push(route(
        "payload/{hash}/outputs",
        base,
        status_of(&single),
        single.is_ok(),
        t,
    ));
    let t = Instant::now();
    let blk = c.block(0, tip - 5, tip - 4).await;
    let (st, ok) = match &blk {
        Ok(Some(_)) => (200, true),
        Ok(None) => (404, false),
        Err(e) => (status_of::<()>(&Err(clone_err(e))), false),
    };
    out.push(route("block", base, st, ok, t));

    // poll one known request key (found in the sample payloads)
    let t = Instant::now();
    let mut key = None;
    if let Ok(entries) = &batch {
        key = entries.iter().find_map(first_request_key);
    }
    if key.is_none() {
        for h in hashes.iter().take(60) {
            if let Ok(v) = c.payload_with_outputs(0, h).await {
                if let Some(k) = first_request_key(&v) {
                    key = Some(k);
                    break;
                }
            }
        }
    }
    match key {
        Some(k) => {
            let t = Instant::now();
            let r = c.poll(0, std::slice::from_ref(&k)).await;
            let found = r.as_ref().map(|m| m.contains_key(&k)).unwrap_or(false);
            if r.is_ok() && !found {
                notes.push(format!(
                    "{base}: poll answered 2xx but did not return the known request key"
                ));
            }
            out.push(route(
                "pact/api/v1/poll",
                base,
                status_of(&r),
                r.is_ok() && found,
                t,
            ));
        }
        None => {
            notes.push(format!(
                "{base}: no transaction found in the sample payloads, poll not probed"
            ));
            out.push(route("pact/api/v1/poll", base, 0, false, t));
        }
    }
    out
}

fn clone_err(e: &NodeError) -> NodeError {
    match e {
        NodeError::Http {
            status,
            body_prefix,
        } => NodeError::Http {
            status: *status,
            body_prefix: body_prefix.clone(),
        },
        other => NodeError::Transport(other.to_string()),
    }
}

async fn probe_batch(
    c: &NodeClient,
    hashes: &[String],
    notes: &mut Vec<String>,
) -> anyhow::Result<BatchProbe> {
    let mut b = BatchProbe::default();
    if hashes.len() < 3 {
        bail!("need at least 3 distinct payload hashes for the batch probe");
    }
    let (h1, h2) = (hashes[0].clone(), hashes[1].clone());

    // duplicates: [h1, h1, h2]
    let r = c
        .payload_batch_raw(0, &[h1.clone(), h1.clone(), h2.clone()])
        .await
        .context("duplicate-hash batch")?;
    b.dup_entries_returned = r.len();
    let h1_count = r
        .iter()
        .filter(|e| e.get("payloadHash").and_then(Value::as_str) == Some(h1.as_str()))
        .count();
    b.dup_hashes_returned_once = h1_count == 1;

    // missing: a valid-format random 32-byte hash
    let mut raw = [0u8; 32];
    rand::rng().fill(&mut raw[..]);
    let missing = URL_SAFE_NO_PAD.encode(raw);
    let alone = c.payload_batch_raw(0, std::slice::from_ref(&missing)).await;
    b.missing_hash_behaviour = match &alone {
        Ok(v) if v.is_empty() => "omitted".into(),
        Ok(v) if v.iter().all(Value::is_null) => "null entry".into(),
        Ok(v) => format!("unexpected: {} entries", v.len()),
        Err(NodeError::Http { status, .. }) => format!("error {status}"),
        Err(e) => format!("error: {e}"),
    };
    // position behaviour: [h1, missing, h2]
    if let Ok(v) = c
        .payload_batch_raw(0, &[h1.clone(), missing.clone(), h2.clone()])
        .await
    {
        b.mixed_order_returned = v
            .iter()
            .map(|e| {
                e.get("payloadHash")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();
    }

    // size ladder; one request per size, stop at the first non-2xx
    for &n in BATCH_SIZES.iter() {
        if n > hashes.len() {
            notes.push(format!(
                "batch ladder stopped at {} (only {} distinct hashes in sample)",
                n,
                hashes.len()
            ));
            break;
        }
        let t = Instant::now();
        let r = c.payload_batch_raw(0, &hashes[..n]).await;
        let status = status_of(&r);
        let returned = r.as_ref().map(Vec::len).unwrap_or(0);
        b.attempts.push(BatchAttempt {
            n,
            status,
            returned,
            ms: t.elapsed().as_millis() as u64,
        });
        match r {
            Ok(_) if returned == n => b.max_ok_batch = n as u32,
            Ok(_) => {
                b.first_fail_batch = Some(n as u32);
                b.fail_status = Some(status);
                b.silent_cap_entries = Some(returned);
                notes.push(format!("batch of {n} distinct hashes returned {returned} entries with status {status}: silent truncation"));
                break;
            }
            Err(e) => {
                b.first_fail_batch = Some(n as u32);
                b.fail_status = Some(status);
                notes.push(format!("batch of {n} failed: {e}"));
                break;
            }
        }
    }
    Ok(b)
}

fn print_summary(r: &ProbeReport, path: &std::path::Path) {
    println!(
        "network: {} (config match: {})  node {}",
        r.network_from_info, r.config_network_matches, r.node_version
    );
    println!(
        "chains: {} at tip, {} below the graph change",
        r.chains_at_tip, r.chains_below_graph_change
    );
    println!("routes:");
    for x in &r.routes {
        println!(
            "  {:<8} {:<26} status {:<3} {} ({} ms)",
            x.base,
            x.route,
            x.status,
            if x.ok { "ok" } else { "FAIL" },
            x.ms
        );
    }
    println!("block route (B1): {}", r.block_route);
    println!("header/branch items (expect 10): {}", r.branch_items);
    println!(
        "block page over 1000 heights: {} items, next token: {}",
        r.block_page_items, r.block_page_has_next
    );
    let b = &r.batch;
    println!(
        "batch (B11): dup returned once={} ({} entries for [h1,h1,h2]); missing hash: {}; mixed order present flags: {:?}",
        b.dup_hashes_returned_once,
        b.dup_entries_returned,
        b.missing_hash_behaviour,
        b.mixed_order_returned.iter().map(|o| o.is_some()).collect::<Vec<_>>()
    );
    for a in &b.attempts {
        println!(
            "  batch {:>5}: status {} returned {} ({} ms)",
            a.n, a.status, a.returned, a.ms
        );
    }
    println!(
        "max ok batch: {}  first fail: {:?} (status {:?}, silent cap {:?})",
        b.max_ok_batch, b.first_fail_batch, b.fail_status, b.silent_cap_entries
    );
    for n in &r.notes {
        println!("note: {n}");
    }
    println!("report: {}", path.display());
}

/// Calls each client method once and prints sizes.
async fn smoke_run(cfg: &Config) -> anyhow::Result<()> {
    let client = super::node_client(cfg)?;
    let info = client.info().await.context("info")?;
    println!(
        "node version: {} (network {})",
        info.node_version, info.chainweb_version
    );
    let cut = client.cut().await.context("cut")?;
    let graph = kdx_node::ChainGraph::from_info(&info);
    let tip = cut.hashes.get(&0).context("cut has no chain 0")?.height;
    println!(
        "chains at tip: {} (cut lists {}), chain 0 tip {}",
        graph.chains_at(cut.height).len(),
        cut.hashes.len(),
        tip
    );
    let page = client
        .headers(0, tip - 20, tip - 10, Some(10), None)
        .await
        .context("headers")?;
    println!(
        "header page: {} items, limit {}, next {}",
        page.items.len(),
        page.limit,
        page.next.is_some()
    );
    let hashes: Vec<String> = page
        .items
        .iter()
        .filter_map(|h| {
            h.get("payloadHash")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    let got = client
        .payloads_with_outputs(0, &hashes)
        .await
        .context("payloads")?;
    println!("batch: {} requested, {} returned", hashes.len(), got.len());
    println!("SSE: waiting up to 60 s for one BlockHeader event");
    let mut stream = client.header_updates().await.context("header_updates")?;
    match tokio::time::timeout(Duration::from_secs(60), stream.next()).await {
        Ok(Some(ev)) => {
            let ev = ev.context("sse event")?;
            let h = ev.get("header").unwrap_or(&ev);
            println!(
                "SSE event: chain {} height {}",
                h.get("chainId").unwrap_or(&Value::Null),
                h.get("height").unwrap_or(&Value::Null)
            );
        }
        Ok(None) => bail!("SSE stream ended before an event"),
        Err(_) => bail!("no SSE event within 60 s"),
    }
    Ok(())
}
