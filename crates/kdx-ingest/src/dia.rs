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
//!   answer's `Symbol`, when present, must be `KDA`.
//! - The answer's `Time` (RFC 3339) must be within `max_age` of now (not more than 5 minutes
//!   ahead) and newer than the last accepted answer's, so a stale, cached or replayed answer
//!   is not taken as current.
//! - Jump guard, cumulative: a value more than `max_jump_pct` away from the *anchor* is held
//!   back until `confirm_polls` polls in a row agree with it (within the same band). The anchor
//!   is a stored rate: it moves only after a rate is persisted, and then only once an hour (or
//!   to a confirmed jump), so a run of smaller steps cannot walk the price past the band
//!   within the hour either.
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
    /// Oldest accepted answer `Time`.
    pub max_age: Duration,
}

/// How long the jump guard's anchor stays put once set.
pub const ANCHOR_SECS: u64 = 3_600;
/// How far ahead of our clock an answer's `Time` may be.
pub const MAX_AHEAD_SECS: u64 = 300;

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
            max_age: Duration::from_secs(2 * 3_600),
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
    /// The answer's `Time` is older than `max_age` or ahead of our clock.
    Stale(String),
    /// The answer's `Time` is not newer than the last accepted one.
    Replay(String),
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
            PollError::Stale(t) => write!(f, "answer time {t} is not current"),
            PollError::Replay(t) => write!(f, "answer time {t} is not newer than the last one"),
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

/// A DIA quotation: the price and its `Time` (unix seconds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quote {
    pub price: Amount,
    pub time: u64,
}

/// Fetch and read one quotation (no time or jump check).
pub async fn fetch(c: &reqwest::Client, s: &DiaSettings) -> Result<Quote, PollError> {
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
    let price = parse_price(&body, s)?;
    let time = parse_time(&body)?;
    Ok(Quote { price, time })
}

/// Days from 1970-01-01 to a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// RFC 3339 UTC time (`YYYY-MM-DDTHH:MM:SS[.frac](Z|+00:00)`, as DIA sends) to unix seconds.
pub fn rfc3339_secs(t: &str) -> Option<u64> {
    let b = t.as_bytes();
    if b.len() < 20
        || b.len() > 40
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let n = |r: std::ops::Range<usize>| -> Option<i64> {
        let x = t.get(r)?;
        x.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| x.parse().ok())?
    };
    let (y, mo, d, h, mi, se) = (
        n(0..4)?,
        n(5..7)?,
        n(8..10)?,
        n(11..13)?,
        n(14..16)?,
        n(17..19)?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let mut rest = &t[19..];
    if let Some(f) = rest.strip_prefix('.') {
        let k = f.bytes().take_while(u8::is_ascii_digit).count();
        if k == 0 {
            return None;
        }
        rest = &f[k..];
    }
    if !matches!(rest, "Z" | "z" | "+00:00") {
        return None;
    }
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + se;
    u64::try_from(secs).ok()
}

/// Read `Time` from a DIA answer.
pub fn parse_time(body: &[u8]) -> Result<u64, PollError> {
    let v: Value = serde_json::from_slice(body).map_err(|e| PollError::Garbage(e.to_string()))?;
    v.get("Time")
        .and_then(Value::as_str)
        .and_then(rfc3339_secs)
        .ok_or_else(|| PollError::Garbage("no RFC 3339 Time".into()))
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

/// Freshness and jump guard over quotations.
#[derive(Debug, Clone, Default)]
pub struct Guard {
    /// The anchor: a persisted rate and when it became the anchor (unix s).
    anchor: Option<(Amount, f64, u64)>,
    /// The `Time` of the last accepted answer.
    last_time: Option<u64>,
    pending: Option<(f64, u32)>,
    /// The last check accepted a confirmed jump (the anchor moves to it once persisted).
    jumped: bool,
}

fn as_f64(p: &Amount) -> f64 {
    p.to_text().parse().unwrap_or(f64::NAN)
}

impl Guard {
    /// The anchor rate.
    pub fn anchor(&self) -> Option<&Amount> {
        self.anchor.as_ref().map(|l| &l.0)
    }

    /// Seed from a stored rate (restart): it is the anchor, set at `at`.
    pub fn seed(&mut self, p: Amount, at: u64) {
        let f = as_f64(&p);
        self.anchor = Some((p, f, at));
    }

    /// Seed after a restart from the newest stored rate `p`, of hour `hour`: it is the
    /// anchor (set at the hour's start), and every answer whose `Time` could have been
    /// accepted before the restart counts as already seen. An answer accepted in that hour
    /// had `Time` ≤ its poll time + [`MAX_AHEAD_SECS`] < `(hour + 1) * 3600 +
    /// MAX_AHEAD_SECS`, so that is the last accepted time: a replayed pre-restart answer is
    /// refused. The cost: the rest of that hour (plus [`MAX_AHEAD_SECS`]) refuses fresh
    /// answers too, so the hour keeps its pre-restart rate.
    pub fn seed_restart(&mut self, p: Amount, hour: u32) {
        let start = u64::from(hour) * 3600;
        self.seed(p, start);
        self.last_time = Some(start + 3600 + MAX_AHEAD_SECS);
    }

    /// Accept `q` at `now` unless its `Time` is stale, ahead of `now` or not newer than the
    /// last accepted one, or its price is more than `max_jump_pct` from the anchor and fewer
    /// than `confirm_polls` polls in a row (each within the band of the first) agree.
    /// Nothing moves the anchor here: call [`Guard::persisted`] once the rate is stored.
    pub fn check(&mut self, q: Quote, now: u64, s: &DiaSettings) -> Result<Amount, PollError> {
        let shown = || q.time.to_string();
        if q.time.saturating_add(s.max_age.as_secs()) < now
            || q.time > now.saturating_add(MAX_AHEAD_SECS)
        {
            return Err(PollError::Stale(shown()));
        }
        if self.last_time.is_some_and(|t| q.time <= t) {
            return Err(PollError::Replay(shown()));
        }
        let f = as_f64(&q.price);
        if !f.is_finite() {
            return Err(PollError::Garbage("Price not representable".into()));
        }
        let within = |a: f64, b: f64| (a - b).abs() * 100.0 <= b * s.max_jump_pct;
        self.jumped = false;
        match &self.anchor {
            Some((_, af, _)) if !within(f, *af) => {
                let n = match self.pending {
                    Some((pf, n)) if within(f, pf) => n + 1,
                    _ => 1,
                };
                let first = match self.pending {
                    Some((pf, _)) if n > 1 => pf,
                    _ => f,
                };
                if n < s.confirm_polls.max(1) {
                    self.pending = Some((first, n));
                    return Err(PollError::Held(n));
                }
                self.jumped = true;
            }
            _ => {}
        }
        self.pending = None;
        self.last_time = Some(q.time);
        Ok(q.price)
    }

    /// The rate `p` accepted by the last [`Guard::check`] was stored at `now`: it becomes the
    /// anchor when there is none, the anchor is an hour old, or `p` was a confirmed jump.
    pub fn persisted(&mut self, p: &Amount, now: u64) {
        let due = match &self.anchor {
            None => true,
            Some((_, _, at)) => now.saturating_sub(*at) >= ANCHOR_SECS,
        };
        if due || self.jumped {
            self.seed(p.clone(), now);
        }
        self.jumped = false;
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

/// The poll loop (until `stop`). Seeds the guard with the newest stored rate
/// ([`Guard::seed_restart`]) so a restart neither accepts a poisoned first answer nor a
/// replay of one accepted before it.
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
        guard.seed_restart(r, hour);
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
            Ok(q) => guard.check(q, now_secs(), &s),
            Err(e) => Err(e),
        };
        match got {
            Ok(rate) => {
                let secs = now_secs();
                let h = hot.clone();
                let r = rate.clone();
                match tokio::task::spawn_blocking(move || store(&h, secs, &r)).await {
                    Ok(Ok(())) => {
                        guard.persisted(&rate, secs);
                        good_at = Some(secs);
                    }
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
            .route(
                "/notime",
                get(|| async { r#"{"Symbol":"KDA","Price":0.005}"# }),
            )
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
        let q = go("/ok").await.unwrap();
        assert_eq!(q.price.to_text(), "0.0049771300444402785");
        assert_eq!(q.time, rfc3339_secs("2026-11-26T12:57:42Z").unwrap());
        assert!(matches!(go("/notime").await, Err(PollError::Garbage(_))));
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

    fn q(p: &str, time: u64) -> Quote {
        Quote {
            price: Amount::parse(p).unwrap(),
            time,
        }
    }

    /// Accept and persist, as the poll loop does.
    fn take(g: &mut Guard, p: &str, t: u64, st: &DiaSettings) -> Result<String, PollError> {
        let r = g.check(q(p, t), t, st)?;
        g.persisted(&r, t);
        Ok(r.to_text())
    }

    #[test]
    fn jump_is_held_until_it_persists() {
        let st = DiaSettings::default();
        let mut g = Guard::default();
        let t = 1_790_000_000;
        assert!(take(&mut g, "0.005", t, &st).is_ok(), "first value anchors");
        assert!(take(&mut g, "0.006", t + 300, &st).is_ok(), "+20% passes");
        assert_eq!(take(&mut g, "0.05", t + 600, &st), Err(PollError::Held(1)));
        assert_eq!(g.anchor().unwrap().to_text(), "0.005", "anchor kept");
        // A different outlier restarts the count.
        assert_eq!(take(&mut g, "5", t + 900, &st), Err(PollError::Held(1)));
        assert_eq!(take(&mut g, "0.05", t + 1200, &st), Err(PollError::Held(1)));
        assert_eq!(
            take(&mut g, "0.051", t + 1500, &st),
            Err(PollError::Held(2))
        );
        assert_eq!(
            take(&mut g, "0.05", t + 1800, &st).unwrap(),
            "0.05",
            "third agrees"
        );
        assert_eq!(
            g.anchor().unwrap().to_text(),
            "0.05",
            "a confirmed jump re-anchors"
        );
        // Back in band clears a pending jump.
        assert_eq!(take(&mut g, "0.2", t + 2100, &st), Err(PollError::Held(1)));
        assert!(take(&mut g, "0.051", t + 2400, &st).is_ok());
    }

    /// Steps under the band each are held once they add up past it within the hour.
    #[test]
    fn stepwise_moves_are_cumulative() {
        let st = DiaSettings::default();
        let mut g = Guard::default();
        let t = 1_790_000_000;
        take(&mut g, "1", t, &st).unwrap();
        assert_eq!(take(&mut g, "1.49", t + 300, &st).unwrap(), "1.49");
        // +49% again is +122% from the anchor (1): held.
        assert_eq!(take(&mut g, "2.22", t + 600, &st), Err(PollError::Held(1)));
        assert_eq!(g.anchor().unwrap().to_text(), "1");
        // A check that is never persisted does not move the anchor either.
        g.check(q("1.4", t + 900), t + 900, &st).unwrap();
        assert_eq!(g.anchor().unwrap().to_text(), "1");
        // After an hour the anchor moves to the stored rate: one more band from there.
        assert_eq!(take(&mut g, "1.45", t + 3_600, &st).unwrap(), "1.45");
        assert_eq!(g.anchor().unwrap().to_text(), "1.45");
        assert!(take(&mut g, "2.1", t + 3_900, &st).is_ok());
        // Seeded from the store on restart: the same band applies.
        let mut g = Guard::default();
        g.seed(Amount::parse("1").unwrap(), t);
        assert_eq!(take(&mut g, "1.6", t + 60, &st), Err(PollError::Held(1)));
    }

    /// Stale, future and replayed answers are refused; the last good rate stays.
    #[test]
    fn stale_and_replayed_answers_are_refused() {
        let st = DiaSettings::default();
        let mut g = Guard::default();
        let now = 1_790_000_000;
        assert!(matches!(
            g.check(q("1", now - 3 * 3_600), now, &st),
            Err(PollError::Stale(_))
        ));
        assert!(matches!(
            g.check(q("1", now + 3_600), now, &st),
            Err(PollError::Stale(_))
        ));
        assert!(g.check(q("1", now - 60), now, &st).is_ok());
        assert!(matches!(
            g.check(q("1", now - 60), now + 300, &st),
            Err(PollError::Replay(_))
        ));
        assert!(matches!(
            g.check(q("1", now - 120), now + 300, &st),
            Err(PollError::Replay(_))
        ));
        assert!(g.check(q("1", now + 200), now + 300, &st).is_ok());
        for bad in [
            "2026-11-26 12:57:42Z",
            "2026-11-26T12:57:42",
            "2026-13-26T12:57:42Z",
            "x",
        ] {
            assert_eq!(rfc3339_secs(bad), None, "{bad}");
        }
        assert_eq!(rfc3339_secs("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            rfc3339_secs("2026-11-26T12:57:42.123Z"),
            rfc3339_secs("2026-11-26T12:57:42Z")
        );
        assert_eq!(rfc3339_secs("2000-03-01T00:00:00Z"), Some(951_868_800));
    }

    /// After a restart, an answer the previous run could have accepted in the stored hour
    /// is a replay; one past that hour (+ the ahead allowance) is taken.
    #[test]
    fn restart_refuses_a_replay_of_the_stored_hour() {
        let st = DiaSettings::default();
        let hour: u32 = 497_222;
        let start = u64::from(hour) * 3600;
        let mut g = Guard::default();
        g.seed_restart(Amount::parse("0.005").unwrap(), hour);
        assert_eq!(g.anchor().unwrap().to_text(), "0.005");
        let now = start + 3_000;
        for t in [start + 10, now, now + MAX_AHEAD_SECS] {
            assert!(
                matches!(g.check(q("0.005", t), now, &st), Err(PollError::Replay(_))),
                "{t}"
            );
        }
        let later = start + 3600 + MAX_AHEAD_SECS + 1;
        assert!(g.check(q("0.005", later), later, &st).is_ok());
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
