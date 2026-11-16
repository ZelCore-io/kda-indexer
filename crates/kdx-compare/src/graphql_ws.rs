//! `kda-compare ws-soak`: GraphQL subscriptions (`graphql-transport-ws`) held open for a time.
//! Every kind of block subscription gets `subs` subscriptions on each of `conns` connections.
//! Each `newBlocks` item is checked against the node's header at its height (once per block),
//! duplicates and per-chain height gaps are counted, and the delivery delay (the block's
//! creation time to receipt) is summarised.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::config::CompareConfig;
use crate::normalize::parse_iso;

/// The subscription documents of the soak, by kind.
const KINDS: &[(&str, &str, &str)] = &[
    (
        "newBlocks",
        "subscription { newBlocks { id hash height chainId creationTime } }",
        "newBlocks",
    ),
    (
        "newBlocksFromDepth",
        "subscription { newBlocksFromDepth(minimumDepth: 3) { id hash height chainId creationTime } }",
        "newBlocksFromDepth",
    ),
    (
        "events",
        "subscription { events(qualifiedEventName: \"coin.TRANSFER\") { id name qualifiedName } }",
        "events",
    ),
];

#[derive(Default)]
struct Stats {
    items: HashMap<&'static str, u64>,
    duplicates: u64,
    misses: u64,
    bad_headers: u64,
    checked: u64,
    errors: u64,
    delays_ms: Vec<f64>,
}

struct Shared {
    stats: Mutex<Stats>,
    verified: Mutex<HashSet<(String, String)>>,
}

fn pct(v: &mut [f64], q: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    Some(v[((v.len() - 1) as f64 * q).round() as usize])
}

fn unix_secs_of(iso: &str) -> Option<f64> {
    let t = parse_iso(iso)?;
    let frac: f64 = format!("0.{}", if t.frac.is_empty() { "0" } else { &t.frac })
        .parse()
        .ok()?;
    Some(t.secs as f64 + frac)
}

/// Runs the soak and prints a text and a JSON summary. Returns whether it saw no duplicate, miss,
/// bad header or error.
pub async fn soak(
    cfg: &CompareConfig,
    url: &str,
    conns: usize,
    subs: usize,
    minutes: f64,
) -> anyhow::Result<bool> {
    let node = cfg.node_client()?;
    let shared = Arc::new(Shared {
        stats: Mutex::new(Stats::default()),
        verified: Mutex::new(HashSet::new()),
    });
    let deadline = Instant::now() + Duration::from_secs_f64(minutes * 60.0);
    let mut tasks = Vec::new();
    for c in 0..conns {
        let (url, shared, node) = (url.to_string(), shared.clone(), node.clone());
        tasks.push(tokio::spawn(async move {
            if let Err(e) = connection(&url, c, subs, deadline, &shared, &node).await {
                tracing::warn!(conn = c, error = %format!("{e:#}"), "connection ended");
                shared.stats.lock().expect("stats").errors += 1;
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let mut s = shared.stats.lock().expect("stats");
    let (p50, p99) = (pct(&mut s.delays_ms, 0.5), pct(&mut s.delays_ms, 0.99));
    let summary = json!({
        "conns": conns, "subs_per_conn": subs, "minutes": minutes,
        "items": s.items, "duplicates": s.duplicates, "misses": s.misses,
        "headers_checked": s.checked, "headers_bad": s.bad_headers, "errors": s.errors,
        "delay_ms_p50": p50, "delay_ms_p99": p99,
    });
    println!(
        "ws-soak: {} items, {} duplicates, {} misses, {} headers checked ({} bad), {} errors, delay p50 {} ms p99 {} ms",
        s.items.values().sum::<u64>(), s.duplicates, s.misses, s.checked, s.bad_headers, s.errors,
        p50.map_or("-".into(), |x| format!("{x:.0}")), p99.map_or("-".into(), |x| format!("{x:.0}")),
    );
    println!("{summary}");
    Ok(s.duplicates == 0 && s.misses == 0 && s.bad_headers == 0 && s.errors == 0)
}

async fn connection(
    url: &str,
    conn: usize,
    subs: usize,
    deadline: Instant,
    shared: &Shared,
    node: &kdx_node::NodeClient,
) -> anyhow::Result<()> {
    let mut req = url.into_client_request().context("url")?;
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", "graphql-transport-ws".parse()?);
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .context("connect")?;
    ws.send(Message::text(
        json!({"type": "connection_init", "payload": {}}).to_string(),
    ))
    .await?;
    loop {
        let m = tokio::time::timeout(Duration::from_secs(15), ws.next())
            .await
            .context("no connection_ack")?
            .context("closed before ack")??;
        if let Message::Text(t) = m {
            if serde_json::from_str::<Value>(t.as_str())?
                .get("type")
                .and_then(Value::as_str)
                == Some("connection_ack")
            {
                break;
            }
        }
    }
    // id -> kind index
    let mut kinds: HashMap<String, usize> = HashMap::new();
    for i in 0..subs {
        let k = (conn + i) % KINDS.len();
        let id = format!("s{i}");
        ws.send(Message::text(
            json!({"id": id, "type": "subscribe", "payload": {"query": KINDS[k].1}}).to_string(),
        ))
        .await?;
        kinds.insert(id, k);
    }
    let mut seen: HashMap<String, HashSet<(String, String)>> = HashMap::new();
    let mut last_height: HashMap<(String, String), u64> = HashMap::new();
    while Instant::now() < deadline {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(next) = tokio::time::timeout(left.min(Duration::from_secs(60)), ws.next()).await
        else {
            continue;
        };
        let Some(m) = next else {
            anyhow::bail!("server closed the connection")
        };
        let Message::Text(t) = m? else { continue };
        let v: Value = serde_json::from_str(t.as_str()).context("frame is not JSON")?;
        match v.get("type").and_then(Value::as_str) {
            Some("ping") => {
                ws.send(Message::text(json!({"type": "pong"}).to_string()))
                    .await?
            }
            Some("error") => {
                tracing::warn!(frame = %v, "subscription error");
                shared.stats.lock().expect("stats").errors += 1;
            }
            Some("next") => {
                let id = v
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let Some(&k) = kinds.get(&id) else { continue };
                let (kind, _, field) = KINDS[k];
                let data = v.pointer(&format!("/payload/data/{field}"));
                let items: Vec<&Value> = match data {
                    Some(Value::Array(a)) => a.iter().collect(),
                    Some(x) if !x.is_null() => vec![x],
                    _ => Vec::new(),
                };
                for it in items {
                    handle_item(kind, &id, it, shared, node, &mut seen, &mut last_height).await;
                }
            }
            _ => {}
        }
    }
    for id in kinds.keys() {
        let _ = ws
            .send(Message::text(
                json!({"id": id, "type": "complete"}).to_string(),
            ))
            .await;
    }
    let _ = ws.close(None).await;
    Ok(())
}

async fn handle_item(
    kind: &'static str,
    sub: &str,
    it: &Value,
    shared: &Shared,
    node: &kdx_node::NodeClient,
    seen: &mut HashMap<String, HashSet<(String, String)>>,
    last_height: &mut HashMap<(String, String), u64>,
) {
    *shared
        .stats
        .lock()
        .expect("stats")
        .items
        .entry(kind)
        .or_default() += 1;
    if kind == "events" {
        // Events: duplicates by id only.
        let id = it
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if !seen
            .entry(sub.to_string())
            .or_default()
            .insert((id, String::new()))
        {
            shared.stats.lock().expect("stats").duplicates += 1;
        }
        return;
    }
    let hash = it
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let chain = it
        .get("chainId")
        .map(|c| c.to_string().trim_matches('"').to_string())
        .unwrap_or_default();
    let height = it.get("height").and_then(Value::as_u64).unwrap_or(0);
    if !seen
        .entry(sub.to_string())
        .or_default()
        .insert((chain.clone(), hash.clone()))
    {
        shared.stats.lock().expect("stats").duplicates += 1;
        return;
    }
    // A height jump on a chain is a miss (a fork at the same height is not).
    let key = (sub.to_string(), chain.clone());
    if let Some(&prev) = last_height.get(&key) {
        if height > prev + 1 {
            shared.stats.lock().expect("stats").misses += height - prev - 1;
        }
    }
    let e = last_height.entry(key).or_insert(height);
    *e = (*e).max(height);
    if let Some(ct) = it
        .get("creationTime")
        .and_then(Value::as_str)
        .and_then(unix_secs_of)
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        shared
            .stats
            .lock()
            .expect("stats")
            .delays_ms
            .push((now - ct) * 1000.0);
    }
    // Once per block across all subscriptions: the node's header at that height on that branch.
    let first = shared
        .verified
        .lock()
        .expect("verified")
        .insert((chain.clone(), hash.clone()));
    if !first {
        return;
    }
    let Ok(c) = chain.parse::<u8>() else { return };
    let ok = match node.header_branch_all(c, &hash, height, height).await {
        Ok(h) => h
            .first()
            .is_some_and(|x| x.get("hash").and_then(Value::as_str) == Some(hash.as_str())),
        Err(_) => return,
    };
    let mut s = shared.stats.lock().expect("stats");
    s.checked += 1;
    if !ok {
        s.bad_headers += 1;
    }
}
