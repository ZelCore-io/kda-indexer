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
    /// Fresh node tips (younger than `TIPS_STALE`).
    pub node_known: bool,
    /// Age in seconds of the tips lag is judged against; None when there are none within
    /// `TIPS_READY_GRACE`.
    pub node_tips_age: Option<u64>,
    pub backfill: bool,
    pub read_only: bool,
}

impl Status {
    pub async fn load(st: &Arc<ApiState>) -> ApiResult<Status> {
        let node_known = st.tips.get().is_some();
        // Stale tips still judge lag for a grace period: a node restart does not make the
        // served data unready (R10).
        let last = st
            .tips
            .last()
            .filter(|(_, age)| *age < crate::server::TIPS_READY_GRACE);
        let node_tips_age = last.as_ref().map(|(_, a)| a.as_secs());
        let tips = last.map(|(t, _)| t);
        let backfill = st.tips.backfill();
        let read_only = st.store.hot.is_read_only();
        st.read_status(move |v| {
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
                node_tips_age,
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
        if self.node_tips_age.is_none() {
            return Some("node tip unknown".into());
        }
        if self.chains.is_empty() {
            return Some("store holds no chain tips".into());
        }
        // A halted chain serves data frozen in time: not ready (R10).
        let halted: Vec<String> = self
            .chains
            .iter()
            .filter(|c| c.halted)
            .map(|c| c.chain.to_string())
            .collect();
        if !halted.is_empty() {
            return Some(format!("chains halted: {}", halted.join(",")));
        }
        let lagging: Vec<String> = self
            .chains
            .iter()
            .filter(|c| c.lag.is_none_or(|l| l > u64::from(max_lag)))
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
            "node_tips_age_secs": self.node_tips_age,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn st(age: Option<u64>, halted: bool, lag: u64) -> Status {
        Status {
            chains: vec![ChainStatus {
                chain: 3,
                height: 10,
                hash: String::new(),
                node_height: Some(10 + lag),
                lag: Some(lag),
                halted,
            }],
            seg_frontier: 0,
            hot_frontier: 0,
            max_indexed_height: Some(10),
            node_known: age.is_some_and(|a| a < 60),
            node_tips_age: age,
            backfill: false,
            read_only: false,
        }
    }

    /// R10: stale tips within the grace keep readiness; a halted chain is not ready.
    #[test]
    fn readiness() {
        assert_eq!(st(Some(5), false, 0).not_ready(20), None);
        assert_eq!(st(Some(300), false, 0).not_ready(20), None);
        assert_eq!(st(None, false, 0).not_ready(20).as_deref(), Some("node tip unknown"));
        assert_eq!(st(Some(5), true, 0).not_ready(20).as_deref(), Some("chains halted: 3"));
        assert!(st(Some(5), false, 50).not_ready(20).is_some());
    }
}
