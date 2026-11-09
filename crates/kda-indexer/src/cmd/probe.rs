//! `probe` command.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use futures::StreamExt;
use serde_json::Value;

use crate::config::Config;

pub async fn run(cfg: &Config, smoke: bool, out: Option<PathBuf>) -> anyhow::Result<()> {
    let _ = out;
    if smoke {
        return smoke_run(cfg).await;
    }
    bail!("probe: not implemented yet")
}

/// Calls each client method once and prints sizes.
async fn smoke_run(cfg: &Config) -> anyhow::Result<()> {
    let client = super::node_client(cfg)?;
    let info = client.info().await.context("info")?;
    println!("node version: {} (network {})", info.node_version, info.chainweb_version);
    let cut = client.cut().await.context("cut")?;
    let graph = kdx_node::ChainGraph::from_info(&info);
    let tip = cut.hashes.get(&0).context("cut has no chain 0")?.height;
    println!("chains at tip: {} (cut lists {}), chain 0 tip {}", graph.chains_at(cut.height).len(), cut.hashes.len(), tip);
    let page = client.headers(0, tip - 20, tip - 10, Some(10), None).await.context("headers")?;
    println!("header page: {} items, limit {}, next {}", page.items.len(), page.limit, page.next.is_some());
    let hashes: Vec<String> = page.items.iter().filter_map(|h| h.get("payloadHash").and_then(Value::as_str).map(str::to_owned)).collect();
    let got = client.payloads_with_outputs(0, &hashes).await.context("payloads")?;
    println!("batch: {} requested, {} returned", hashes.len(), got.len());
    println!("SSE: waiting up to 60 s for one BlockHeader event");
    let mut stream = client.header_updates().await.context("header_updates")?;
    match tokio::time::timeout(Duration::from_secs(60), stream.next()).await {
        Ok(Some(ev)) => {
            let ev = ev.context("sse event")?;
            let h = ev.get("header").unwrap_or(&ev);
            println!("SSE event: chain {} height {}", h.get("chainId").unwrap_or(&Value::Null), h.get("height").unwrap_or(&Value::Null));
        }
        Ok(None) => bail!("SSE stream ended before an event"),
        Err(_) => bail!("no SSE event within 60 s"),
    }
    Ok(())
}
