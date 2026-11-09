//! Block bodies for canonical headers (`payload/outputs/batch`, ≤ 1000 per call, mapped by
//! `payloadHash`, missing ones refetched singly: `NodeClient::payloads_with_outputs`). The
//! client's semaphore and rate limiter bound concurrency and requests per second.

use anyhow::{Context, Result};
use kdx_node::NodeClient;
use serde_json::Value;

/// `(header, payload with outputs)` per header, in the headers' order.
pub async fn bodies(
    node: &NodeClient,
    chain: u8,
    headers: Vec<Value>,
) -> Result<Vec<(Value, Value)>> {
    let hashes: Vec<String> = headers
        .iter()
        .map(|h| {
            h.get("payloadHash")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("header without payloadHash")
        })
        .collect::<Result<_>>()?;
    let payloads = node.payloads_with_outputs(chain, &hashes).await?;
    headers
        .into_iter()
        .zip(hashes)
        .map(|(h, ph)| {
            let p = payloads
                .get(&ph)
                .cloned()
                .with_context(|| format!("chain {chain}: payload {ph} missing"))?;
            Ok((h, p))
        })
        .collect()
}
