//! Status JSON (native `/status`, admin `/status`, `/readyz` reasons).

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use kdx_hot::Txn;
use serde_json::{json, Value};

use crate::server::{json, ApiState};
use crate::ApiResult;

/// Per-chain halted counter (same name as `kdx_ingest::apply::counters::halted`).
fn halted_key(chain: u8) -> String {
    format!("halted.{chain}")
}

#[derive(Debug, Clone)]
pub struct ChainStatus {
    pub chain: u8,
    pub height: u32,
    pub hash: String,
    pub node_height: Option<u64>,
    pub lag: Option<u64>,
    pub halted: bool,
}

#[derive(Debug, Clone)]
pub struct Status {
    pub chains: Vec<ChainStatus>,
    pub seg_frontier: u32,
    pub hot_frontier: u32,
    pub max_indexed_height: Option<u32>,
    pub node_known: bool,
    pub backfill: bool,
    pub read_only: bool,
}

impl Status {
    pub async fn load(st: &Arc<ApiState>) -> ApiResult<Status> {
        let tips = st.tips.get();
        let backfill = st.tips.backfill();
        let read_only = st.store.hot.is_read_only();
        let node_known = tips.is_some();
        st.read(move |v| {
            let mut chains = Vec::new();
            for (c, h, hash) in v.chain_tips()? {
                let node_height = tips.as_ref().and_then(|t| t.heights.get(&c).copied());
                chains.push(ChainStatus {
                    chain: c,
                    height: h,
                    hash: URL_SAFE_NO_PAD.encode(hash.0),
                    node_height,
                    lag: node_height.map(|n| n.saturating_sub(u64::from(h))),
                    halted: v.txn.counter(&halted_key(c))? > 0,
                });
            }
            Ok(Status {
                chains,
                seg_frontier: v.seg_frontier,
                hot_frontier: v.hot_frontier,
                max_indexed_height: v.max_indexed_height()?,
                node_known,
                backfill,
                read_only,
            })
        })
        .await
    }

    /// Why the service is not ready (None = ready).
    pub fn not_ready(&self, max_lag: u32) -> Option<String> {
        if self.backfill {
            return Some("backfill running".into());
        }
        if !self.node_known {
            return Some("node tip unknown".into());
        }
        if self.chains.is_empty() {
            return Some("store holds no chain tips".into());
        }
        let lagging: Vec<String> = self
            .chains
            .iter()
            .filter(|c| !c.halted && c.lag.is_none_or(|l| l > u64::from(max_lag)))
            .map(|c| format!("{}:{}", c.chain, c.lag.map_or("?".into(), |l| l.to_string())))
            .collect();
        (!lagging.is_empty()).then(|| format!("chains lag more than {max_lag}: {}", lagging.join(",")))
    }

    pub fn to_json(&self, network: &str, max_lag: u32) -> Value {
        json!({
            "network": network,
            "ready": self.not_ready(max_lag).is_none(),
            "backfill": self.backfill,
            "read_only": self.read_only,
            "frozen_frontier": self.seg_frontier,
            "hot_frontier": self.hot_frontier,
            "max_indexed_height": self.max_indexed_height,
            "node_tip": if self.node_known {
                Value::from(self.chains.iter().filter_map(|c| c.node_height).max())
            } else {
                Value::Null
            },
            "chains": self.chains.iter().map(|c| json!({
                "chain": c.chain,
                "height": c.height,
                "hash": c.hash,
                "node_height": c.node_height,
                "lag": c.lag,
                "halted": c.halted,
            })).collect::<Vec<_>>(),
        })
    }
}

/// Network, per-chain node and indexed tips, lag, halted chains, frozen frontier.
#[utoipa::path(
    get,
    path = "/api/v1/status",
    responses((status = 200, description = "Indexer status"))
)]
pub async fn handler(State(st): State<Arc<ApiState>>) -> Response {
    match Status::load(&st).await {
        Ok(s) => json(&s.to_json(&st.settings.network, st.settings.ready_max_lag)),
        Err(e) => e.into_response(),
    }
}
