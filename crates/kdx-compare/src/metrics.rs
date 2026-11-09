//! Prometheus counters, served at `GET /metrics` when `metrics_bind` is set.

use std::sync::{Arc, Mutex};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReqLabels {
    pub route: String,
    pub target: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DiffLabels {
    pub route: String,
    pub class: String,
}

#[derive(Clone)]
pub struct Metrics {
    pub requests: Family<ReqLabels, Counter>,
    pub diffs: Family<DiffLabels, Counter>,
    registry: Arc<Mutex<Registry>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Metrics {
        let mut registry = Registry::default();
        let requests = Family::<ReqLabels, Counter>::default();
        let diffs = Family::<DiffLabels, Counter>::default();
        // prometheus-client appends `_total` to counters.
        registry.register(
            "kdx_compare_requests",
            "Requests sent, per route and target",
            requests.clone(),
        );
        registry.register(
            "kdx_compare_diffs",
            "Compared requests per route and result class",
            diffs.clone(),
        );
        Metrics {
            requests,
            diffs,
            registry: Arc::new(Mutex::new(registry)),
        }
    }

    pub fn request(&self, route: &str, target: &str) {
        self.requests
            .get_or_create(&ReqLabels {
                route: route.to_string(),
                target: target.to_string(),
            })
            .inc();
    }

    pub fn diff(&self, route: &str, class: &str) {
        self.diffs
            .get_or_create(&DiffLabels {
                route: route.to_string(),
                class: class.to_string(),
            })
            .inc();
    }

    pub fn encode(&self) -> String {
        let mut out = String::new();
        let reg = self.registry.lock().expect("metrics registry poisoned");
        let _ = prometheus_client::encoding::text::encode(&mut out, &reg);
        out
    }

    /// Serves `GET /metrics` on `bind` until the process exits.
    pub async fn serve(self, bind: String) -> anyhow::Result<()> {
        use axum::{routing::get, Router};
        let m = self.clone();
        let app = Router::new().route(
            "/metrics",
            get(move || {
                let m = m.clone();
                async move {
                    (
                        [(
                            "content-type",
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )],
                        m.encode(),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind(&bind).await?;
        tracing::info!(%bind, "metrics listening");
        axum::serve(listener, app).await?;
        Ok(())
    }
}
