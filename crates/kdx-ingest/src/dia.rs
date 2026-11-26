//! The KDA/USD feed for the DEX USD fields (stock-gap plan B3): a background task polls DIA's
//! KDA quotation (stock's endpoint, `price-updater.service.ts`:
//! `https://api.diadata.org/v1/assetQuotation/Kadena/0x0000000000000000000000000000000000000000`)
//! every 5 minutes and stores the last good rate of each hour in `DexUsd`
//! (`kdx_hot::dex_usd`). Off by default; an operator turns it on in `[dex_usd]`.
//!
//! Safety (an off-chain input that feeds served numbers):
//! - The URL comes only from config. Redirects are never followed (a 3xx is a failed poll),
//!   so a compromised or misconfigured endpoint cannot point the poller at another host.
//! - One total timeout per poll (connect, headers and body), so a slow-loris server costs one
//!   poll; the body is read in chunks and dropped past `max_response_bytes`.
//! - The price must be a JSON number (or numeric text) in `[min_price, max_price]`, and the
//!   answer's `Symbol`, when present, must be `KDA`. A value more than `max_jump_pct` away from
//!   the last accepted one is held back until `confirm_polls` polls in a row agree with it
//!   (within the same band), so one poisoned answer cannot move the served USD values.
//! - Any failure keeps the last good value (logged with its age). Nothing here panics, and the
//!   task never touches ingest: its write is one short txn of one row, in its own blocking task.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kdx_hot::dex::Amount;
use kdx_hot::HotStore;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

/// Stock's endpoint.
pub const DIA_KDA_URL: &str =
    "https://api.diadata.org/v1/assetQuotation/Kadena/0x0000000000000000000000000000000000000000";

#[derive(Debug, Clone)]
pub struct DiaSettings {
    pub url: String,
    pub interval: Duration,
    pub timeout: Duration,
    /// A generic client name (no operator details).
    pub user_agent: String,
    pub max_response_bytes: usize,
    pub min_price: f64,
    pub max_price: f64,
    pub max_jump_pct: f64,
    pub confirm_polls: u32,
}

impl Default for DiaSettings {
    fn default() -> Self {
        Self {
            url: DIA_KDA_URL.into(),
            interval: Duration::from_secs(300),
            timeout: Duration::from_secs(10),
            user_agent: "kda-indexer".into(),
            max_response_bytes: 16 * 1024,
            min_price: 0.000_001,
            max_price: 10_000.0,
            max_jump_pct: 50.0,
            confirm_polls: 3,
        }
    }
}

/// Why a poll did not produce an accepted rate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollError {
    Http(String),
    Status(u16),
    TooLarge,
    Garbage(String),
    OutOfRange(String),
    /// A jump past `max_jump_pct`, not yet confirmed (polls seen so far).
    Held(u32),
}

impl std::fmt::Display for PollError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollError::Http(e) => write!(f, "request failed: {e}"),
            PollError::Status(s) => write!(f, "HTTP status {s}"),
            PollError::TooLarge => write!(f, "answer larger than the cap"),
            PollError::Garbage(e) => write!(f, "unreadable answer: {e}"),
            PollError::OutOfRange(p) => write!(f, "price {p} outside the accepted range"),
            PollError::Held(n) => write!(f, "price jump held back ({n} poll(s) agree so far)"),
        }
    }
}

/// The HTTP client: no redirects, one total timeout, a generic User-Agent.
pub fn client(s: &DiaSettings) -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(s.timeout)
        .connect_timeout(s.timeout)
        .user_agent(s.user_agent.clone())
        .build()?)
}

/// Fetch and read one price (no jump check).
pub async fn fetch(c: &reqwest::Client, s: &DiaSettings) -> Result<Amount, PollError> {
    let mut resp = c
        .get(&s.url)
        .header("accept", "application/json")
        .send()
        .await
        .map_err(|e| PollError::Http(e.without_url().to_string()))?;
    if !resp.status().is_success() {
        return Err(PollError::Status(resp.status().as_u16()));
    }
    if resp
        .content_length()
        .is_some_and(|n| n > s.max_response_bytes as u64)
    {
        return Err(PollError::TooLarge);
    }
    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| PollError::Http(e.without_url().to_string()))?
    {
        if body.len() + chunk.len() > s.max_response_bytes {
            return Err(PollError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    parse_price(&body, s)
}

/// Read `Price` from a DIA answer and check its range.
pub fn parse_price(body: &[u8], s: &DiaSettings) -> Result<Amount, PollError> {
    let v: Value = serde_json::from_slice(body).map_err(|e| PollError::Garbage(e.to_string()))?;
    if let Some(sym) = v.get("Symbol") {
        if sym.as_str() != Some("KDA") {
            return Err(PollError::Garbage("Symbol is not KDA".into()));
        }
    }
    let text = match v.get("Price") {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(t)) if t.len() <= 64 => t.clone(),
        _ => return Err(PollError::Garbage("no numeric Price".into())),
    };
    let f: f64 = text
        .parse()
        .map_err(|_| PollError::Garbage("Price is not a number".into()))?;
    if !f.is_finite() || f < s.min_price || f > s.max_price {
        return Err(PollError::OutOfRange(text.chars().take(32).collect()));
    }
    // Exact text when it has at most 24 decimals; otherwise the double's 20-place expansion
    // (DIA sends a JSON double, so nothing past its own precision is lost).
    Amount::parse(&text)
        .or_else(|| Amount::parse(&format!("{f:.20}")))
        .filter(|a| !a.is_zero() && !a.is_negative())
        .ok_or_else(|| PollError::Garbage("Price not representable".into()))
}

/// Jump guard over accepted prices.
#[derive(Debug, Clone, Default)]
pub struct Guard {
    last: Option<(Amount, f64)>,
    pending: Option<(f64, u32)>,
}

impl Guard {
    /// The last accepted price.
    pub fn last(&self) -> Option<&Amount> {
        self.last.as_ref().map(|l| &l.0)
    }

    /// Accept `p` unless it jumps more than `max_jump_pct` from the last accepted price and
    /// fewer than `confirm_polls` polls in a row (each within the band of the first) agree.
    pub fn check(&mut self, p: Amount, s: &DiaSettings) -> Result<Amount, PollError> {
        let f: f64 = p.to_text().parse().unwrap_or(f64::NAN);
        let within = |a: f64, b: f64| (a - b).abs() * 100.0 <= b * s.max_jump_pct;
        match &self.last {
            Some((_, lf)) if !within(f, *lf) => {
                let n = match self.pending {
                    Some((pf, n)) if within(f, pf) => n + 1,
                    _ => 1,
                };
                let anchor = match self.pending {
                    Some((pf, _)) if n > 1 => pf,
                    _ => f,
                };
                if n < s.confirm_polls.max(1) {
                    self.pending = Some((anchor, n));
                    return Err(PollError::Held(n));
                }
            }
            _ => {}
        }
        self.pending = None;
        self.last = Some((p.clone(), f));
        Ok(p)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Store `rate` as the rate of the hour of `secs`.
pub fn store(hot: &HotStore, secs: u64, rate: &Amount) -> anyhow::Result<()> {
    let hour = u32::try_from(secs / 3600).unwrap_or(u32::MAX);
    let mut w = hot.rw()?;
    kdx_hot::dex_usd::put(&mut w, hour, rate)?;
    w.commit()
}

/// The poll loop (until `stop`). Seeds the guard with the newest stored rate so a restart
/// does not accept a poisoned first answer.
pub async fn run(hot: Arc<HotStore>, s: DiaSettings, stop: CancellationToken) {
    let c = match client(&s) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "dia: client setup failed; DEX USD feed off");
            return;
        }
    };
    let mut guard = Guard::default();
    let mut good_at: Option<u64> = None;
    if let Ok(Ok(Some((hour, r)))) = hot.ro().map(|t| kdx_hot::dex_usd::latest(&t)) {
        let _ = guard.check(r, &s);
        good_at = Some(u64::from(hour) * 3600);
    }
    let mut tick = tokio::time::interval(s.interval.max(Duration::from_secs(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tick.tick() => {}
        }
        let got = match fetch(&c, &s).await {
            Ok(p) => guard.check(p, &s),
            Err(e) => Err(e),
        };
        match got {
            Ok(rate) => {
                let secs = now_secs();
                let h = hot.clone();
                let r = rate.clone();
                match tokio::task::spawn_blocking(move || store(&h, secs, &r)).await {
                    Ok(Ok(())) => good_at = Some(secs),
                    Ok(Err(e)) => tracing::warn!(error = %e, "dia: storing the rate failed"),
                    Err(e) => tracing::warn!(error = %e, "dia: store task"),
                }
            }
            Err(e) => {
                let age = good_at.map(|t| now_secs().saturating_sub(t));
                tracing::warn!(
                    error = %e,
                    last_good_age_s = ?age,
                    "dia: KDA/USD poll rejected; keeping the last good rate"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;

    fn s(url: String) -> DiaSettings {
        DiaSettings {
            url,
            timeout: Duration::from_millis(400),
            max_response_bytes: 2048,
            ..DiaSettings::default()
        }
    }

    async fn mock() -> String {
        let ok = r#"{"Symbol":"KDA","Price":0.0049771300444402785,"Time":"2026-11-26T12:57:42Z"}"#;
        let app = axum::Router::new()
            .route("/ok", get(move || async move { ok }))
            .route("/garbage", get(|| async { "<html>nope" }))
            .route("/neg", get(|| async { r#"{"Price":-1}"# }))
            .route("/huge", get(|| async { r#"{"Price":1e300}"# }))
            .route("/str", get(|| async { r#"{"Price":"abc"}"# }))
            .route(
                "/sym",
                get(|| async { r#"{"Symbol":"BTC","Price":0.005}"# }),
            )
            .route(
                "/500",
                get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "x").into_response() }),
            )
            .route(
                "/redirect",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "http://169.254.169.254/latest")],
                    )
                        .into_response()
                }),
            )
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    r#"{"Price":0.005}"#
                }),
            )
            .route(
                "/big",
                get(|| async { format!(r#"{{"Price":0.005,"pad":"{}"}}"#, "a".repeat(10_000)) }),
            );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn feed_failure_modes() {
        let base = mock().await;
        let go = |p: &str| {
            let st = s(format!("{base}{p}"));
            async move { fetch(&client(&st).unwrap(), &st).await }
        };
        assert_eq!(go("/ok").await.unwrap().to_text(), "0.0049771300444402785");
        assert!(matches!(go("/garbage").await, Err(PollError::Garbage(_))));
        assert!(matches!(go("/neg").await, Err(PollError::OutOfRange(_))));
        assert!(matches!(go("/huge").await, Err(PollError::OutOfRange(_))));
        assert!(matches!(go("/str").await, Err(PollError::Garbage(_))));
        assert!(matches!(go("/sym").await, Err(PollError::Garbage(_))));
        assert_eq!(go("/500").await, Err(PollError::Status(500)));
        // Never followed: the 302 itself is the failure.
        assert_eq!(go("/redirect").await, Err(PollError::Status(302)));
        let t = std::time::Instant::now();
        assert!(matches!(go("/slow").await, Err(PollError::Http(_))));
        assert!(
            t.elapsed() < Duration::from_secs(3),
            "total timeout bounds a slow server"
        );
        assert_eq!(go("/big").await, Err(PollError::TooLarge));
        // Nothing listens there.
        let dead = s("http://127.0.0.1:9/x".into());
        assert!(matches!(
            fetch(&client(&dead).unwrap(), &dead).await,
            Err(PollError::Http(_))
        ));
    }

    #[test]
    fn jump_is_held_until_it_persists() {
        let st = DiaSettings::default();
        let a = |t: &str| Amount::parse(t).unwrap();
        let mut g = Guard::default();
        assert!(g.check(a("0.005"), &st).is_ok(), "first value seeds");
        assert!(g.check(a("0.006"), &st).is_ok(), "+20% passes");
        assert_eq!(g.check(a("0.05"), &st), Err(PollError::Held(1)));
        assert_eq!(g.last().unwrap().to_text(), "0.006", "last good kept");
        // A different outlier restarts the count.
        assert_eq!(g.check(a("5"), &st), Err(PollError::Held(1)));
        assert_eq!(g.check(a("0.05"), &st), Err(PollError::Held(1)));
        assert_eq!(g.check(a("0.051"), &st), Err(PollError::Held(2)));
        assert_eq!(
            g.check(a("0.05"), &st).unwrap().to_text(),
            "0.05",
            "third agrees"
        );
        assert_eq!(g.last().unwrap().to_text(), "0.05");
        // Back in band clears a pending jump.
        assert_eq!(g.check(a("0.2"), &st), Err(PollError::Held(1)));
        assert!(g.check(a("0.051"), &st).is_ok());
    }

    #[test]
    fn price_text_forms() {
        let st = DiaSettings::default();
        let p = |b: &str| parse_price(b.as_bytes(), &st);
        assert_eq!(p(r#"{"Price":"0.0051"}"#).unwrap().to_text(), "0.0051");
        assert_eq!(p(r#"{"Price":5.1e-3}"#).unwrap().to_text(), "0.0051");
        assert!(p(r#"{"Price":0}"#).is_err());
        assert!(p(r#"{"Price":null}"#).is_err());
        assert!(p(r#"[]"#).is_err());
        assert!(p(&format!(r#"{{"Price":"{}"}}"#, "9".repeat(80))).is_err());
    }

    #[test]
    fn stored_rate_is_read_back() {
        let d = tempfile::tempdir().unwrap();
        let hot = HotStore::open(
            &d.path().join("hot"),
            kdx_hot::Geometry {
                max_gb: 1,
                growth_mb: 16,
            },
        )
        .unwrap();
        store(&hot, 7200 * 10, &Amount::parse("0.005").unwrap()).unwrap();
        let ro = hot.ro().unwrap();
        let (h, r) = kdx_hot::dex_usd::latest(&ro).unwrap().unwrap();
        assert_eq!((h, r.to_text().as_str()), (20, "0.005"));
    }
}
