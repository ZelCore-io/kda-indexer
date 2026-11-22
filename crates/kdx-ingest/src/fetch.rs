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
                .and_then(|ph| {
                    // F-10: the hash goes into a node URL; only a real hash may.
                    kdx_types::Hash32::from_b64url(&ph)
                        .with_context(|| format!("chain {chain}: invalid payloadHash {ph:?}"))?;
                    Ok(ph)
                })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// F-10: a header whose `payloadHash` is not a 32-byte base64url hash is refused before
    /// any request (the base points at a closed port, so a request would be a transport error).
    #[tokio::test]
    async fn bad_payload_hash_is_refused() {
        let node = NodeClient::new(&kdx_node::NodeClientConfig {
            base: "http://127.0.0.1:9/".parse().unwrap(),
            network: "mainnet01".into(),
            timeout: std::time::Duration::from_millis(200),
            max_rps: 0,
            concurrency: 1,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 20,
        })
        .unwrap();
        let e = bodies(&node, 0, vec![json!({"payloadHash": "../../info"})])
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("invalid payloadHash"), "{e:#}");
    }
}
