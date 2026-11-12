//! Request limits (spec §5.4): internal-address check, trusted-proxy client IP, per-IP token
//! bucket. The in-flight cap, deadline and body cap live in `server::guard`.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Mutex;
use std::time::Instant;

use axum::http::HeaderMap;

/// Loopback or private (RFC 1918, RFC 4193) address: where native and admin may bind.
pub fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback() || v4.is_private(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_internal(IpAddr::V4(v4));
            }
            v6.is_loopback() || (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

/// An IP or CIDR block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNet {
    addr: IpAddr,
    prefix: u8,
}

impl IpNet {
    pub fn parse(s: &str) -> Option<IpNet> {
        let (a, p) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = a.trim().parse().ok()?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match p {
            Some(p) => p.trim().parse::<u8>().ok().filter(|&p| p <= max)?,
            None => max,
        };
        Some(IpNet { addr, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
            v4 => v4,
        };
        match (self.addr, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => mask4(n, self.prefix) == mask4(a, self.prefix),
            (IpAddr::V6(n), IpAddr::V6(a)) => mask6(n, self.prefix) == mask6(a, self.prefix),
            _ => false,
        }
    }
}

fn mask4(a: Ipv4Addr, p: u8) -> u32 {
    let m = if p == 0 {
        0
    } else {
        u32::MAX << (32 - p as u32)
    };
    u32::from(a) & m
}

fn mask6(a: Ipv6Addr, p: u8) -> u128 {
    let m = if p == 0 {
        0
    } else {
        u128::MAX << (128 - p as u32)
    };
    u128::from(a) & m
}

/// One `X-Forwarded-For` entry as an IP: `ip`, `ip:port`, `[v6]` or `[v6]:port`.
fn parse_forwarded(s: &str) -> Option<IpAddr> {
    let s = s.trim();
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(sa) = s.parse::<SocketAddr>() {
        return Some(sa.ip());
    }
    s.strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .and_then(|v6| v6.parse::<Ipv6Addr>().ok())
        .map(IpAddr::V6)
}

/// The client's IP: the peer, or, when the peer is a trusted proxy, the right-most
/// `X-Forwarded-For` entry that is not itself a trusted proxy. The walk goes right to left
/// and stops at the first untrusted entry (the client) or the first unparsable one (the
/// peer is used then): entries left of an untrusted or malformed one are client-controlled.
/// When every entry is a trusted proxy, the left-most one is used.
pub fn client_ip(peer: SocketAddr, headers: &HeaderMap, trusted: &[IpNet]) -> IpAddr {
    let is_trusted = |ip: IpAddr| trusted.iter().any(|n| n.contains(ip));
    let peer_ip = peer.ip();
    if trusted.is_empty() || !is_trusted(peer_ip) {
        return peer_ip;
    }
    let mut lines = Vec::new();
    for v in headers.get_all("x-forwarded-for") {
        match v.to_str() {
            Ok(l) => lines.push(l),
            Err(_) => return peer_ip, // non-visible-ASCII: unparsable
        }
    }
    let mut leftmost = None;
    for entry in lines.iter().rev().flat_map(|l| l.rsplit(',')) {
        let Some(ip) = parse_forwarded(entry) else {
            return peer_ip;
        };
        if !is_trusted(ip) {
            return ip;
        }
        leftmost = Some(ip);
    }
    leftmost.unwrap_or(peer_ip)
}

/// Bucket key: an IPv4 address (v4-mapped v6 included), or the /64 of an IPv6 address
/// (one host usually holds a whole /64, so per-address keys would let it rotate freely).
pub(crate) fn bucket_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(Ipv6Addr::from(mask6(v6, 64))),
        },
    }
}

/// Overflow key: the /24 of an IPv4 address, the /48 of an IPv6 address (a typical site
/// allocation), so a client can drain only its own prefix's overflow share.
fn overflow_key(ip: IpAddr) -> IpAddr {
    match bucket_key(ip) {
        IpAddr::V4(v4) => IpAddr::V4(Ipv4Addr::from(mask4(v4, 24))),
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(mask6(v6, 48))),
    }
}

/// A capped table of token buckets.
struct Buckets {
    /// key -> (tokens, last refill, insertion sequence)
    map: HashMap<IpAddr, (f64, Instant, u64)>,
    /// Keys in insertion order with their sequence; pairs whose key was evicted and
    /// re-added are skipped on eviction.
    order: VecDeque<(IpAddr, u64)>,
    seq: u64,
    cap: usize,
}

/// Queue entries looked at per insert for a refilled bucket to evict.
const EVICT_SCAN: usize = 16;

impl Buckets {
    fn new(cap: usize) -> Buckets {
        Buckets {
            map: HashMap::new(),
            order: VecDeque::new(),
            seq: 0,
            cap: cap.max(1),
        }
    }

    /// Evict one bucket: a refilled one among the first `EVICT_SCAN` queue entries (others
    /// rotate to the back), else the oldest. True when a non-full bucket was evicted.
    fn make_room(&mut self, now: Instant, rate: f64, burst: f64) -> bool {
        for _ in 0..EVICT_SCAN {
            let Some((k, seq)) = self.order.pop_front() else {
                break;
            };
            let Some(&(t, at, s)) = self.map.get(&k) else {
                continue;
            };
            if s != seq {
                continue;
            }
            if t + now.saturating_duration_since(at).as_secs_f64() * rate >= burst {
                self.map.remove(&k);
                return false;
            }
            self.order.push_back((k, seq));
        }
        while let Some((k, seq)) = self.order.pop_front() {
            if self.map.get(&k).is_some_and(|e| e.2 == seq) {
                self.map.remove(&k);
                return true;
            }
        }
        false
    }

    /// The refilled bucket of `key`, created when absent. A bucket created by evicting a
    /// non-full one starts empty (`true` returned): eviction never refills a drained key.
    fn bucket(&mut self, key: IpAddr, now: Instant, rate: f64, burst: f64) -> (&mut f64, bool) {
        let mut pressured = false;
        if !self.map.contains_key(&key) {
            if self.map.len() >= self.cap {
                pressured = self.make_room(now, rate, burst);
            }
            self.seq += 1;
            let start = if pressured { 0.0 } else { burst };
            self.map.insert(key, (start, now, self.seq));
            self.order.push_back((key, self.seq));
        }
        let (tokens, at, _) = self.map.get_mut(&key).expect("inserted above");
        *tokens = (*tokens + now.saturating_duration_since(*at).as_secs_f64() * rate).min(burst);
        *at = now;
        (tokens, pressured)
    }
}

/// Per-IP token bucket: `rate` tokens per second, at most `burst`, keyed by
/// [`bucket_key`]. At most `cap` buckets; a new key over the cap evicts a refilled (full)
/// bucket among the oldest few, else the oldest one. Evicting a full bucket loses nothing;
/// when a non-full one had to go, the new key starts empty and its first token comes from
/// the overflow bucket of its prefix ([`overflow_key`], own capped table, same rule), so
/// cycling keys never refills a drained key and draining overflow hurts only that prefix.
/// O(1) per request, no full scan.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    tables: Mutex<(Buckets, Buckets)>,
}

/// Buckets kept before eviction.
const MAX_BUCKETS: usize = 65_536;
/// Overflow (prefix) buckets kept before eviction.
const MAX_OVERFLOW: usize = 4_096;

impl RateLimiter {
    pub fn new(rate: u32, burst: u32) -> RateLimiter {
        RateLimiter::with_cap(rate, burst, MAX_BUCKETS)
    }

    pub fn with_cap(rate: u32, burst: u32, cap: usize) -> RateLimiter {
        RateLimiter {
            rate: rate.max(1) as f64,
            burst: burst.max(1) as f64,
            tables: Mutex::new((
                Buckets::new(cap),
                Buckets::new(MAX_OVERFLOW.min(cap.max(16))),
            )),
        }
    }

    /// Buckets held (not counting overflow buckets).
    pub fn len(&self) -> usize {
        self.tables
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .map
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Take one token for `ip`; false when the bucket is empty.
    pub fn allow(&self, ip: IpAddr) -> bool {
        self.allow_at(ip, Instant::now())
    }

    /// Take `n` tokens at once (a GraphQL batch of N takes N); none are taken when fewer
    /// than `n` are left.
    pub fn allow_n(&self, ip: IpAddr, n: u32) -> bool {
        self.allow_n_at(ip, n, Instant::now())
    }

    fn allow_at(&self, ip: IpAddr, now: Instant) -> bool {
        self.allow_n_at(ip, 1, now)
    }

    fn allow_n_at(&self, ip: IpAddr, n: u32, now: Instant) -> bool {
        let need = n.max(1) as f64;
        let (rate, burst) = (self.rate, self.burst);
        let mut g = self.tables.lock().unwrap_or_else(|e| e.into_inner());
        let (main, over) = &mut *g;
        let (tokens, pressured) = main.bucket(bucket_key(ip), now, rate, burst);
        let tokens = if pressured {
            over.bucket(overflow_key(ip), now, rate, burst).0
        } else {
            tokens
        };
        if *tokens >= need {
            *tokens -= need;
            true
        } else {
            false
        }
    }
}

/// One counter of a [`ConnLimiter`]: the client key (tag 0) or an IPv6 /48 (tag 1). The tag
/// keeps a /64 whose bits 48..64 are zero apart from its own /48.
type SlotKey = (u8, IpAddr);

/// The IPv6 /48 of `ip` (a typical site allocation), `None` for an IPv4 client.
fn prefix48(ip: IpAddr) -> Option<IpAddr> {
    match bucket_key(ip) {
        IpAddr::V4(_) => None,
        IpAddr::V6(v6) => Some(IpAddr::V6(Ipv6Addr::from(mask6(v6, 48)))),
    }
}

/// Open slots per client (GraphQL websocket connections and subscriptions, GQ-9, M-2),
/// keyed like the rate limiter ([`bucket_key`]: an IPv4 address, or the /64 of an IPv6
/// address), and per IPv6 /48 as well, so one site cannot rotate /64s past the per-client
/// cap. A slot is held by a [`ConnGuard`] and released when the guard drops (close, error
/// or task end).
pub struct ConnLimiter {
    per_key: usize,
    per_prefix: usize,
    open: std::sync::Arc<Mutex<HashMap<SlotKey, usize>>>,
}

/// One open slot in a [`ConnLimiter`].
pub struct ConnGuard {
    keys: [Option<SlotKey>; 2],
    open: std::sync::Arc<Mutex<HashMap<SlotKey, usize>>>,
}

impl ConnLimiter {
    /// At most `per_key` slots per client key and `per_prefix` per IPv6 /48.
    pub fn new(per_key: usize, per_prefix: usize) -> ConnLimiter {
        ConnLimiter {
            per_key,
            per_prefix,
            open: Default::default(),
        }
    }

    /// A slot for `ip`, or `None` when its client key already holds `per_key` slots or its
    /// /48 holds `per_prefix`.
    pub fn try_acquire(&self, ip: IpAddr) -> Option<ConnGuard> {
        let keys = [Some((0, bucket_key(ip))), prefix48(ip).map(|p| (1, p))];
        let cap = |k: &SlotKey| {
            if k.0 == 0 {
                self.per_key
            } else {
                self.per_prefix
            }
        };
        let mut m = self.open.lock().unwrap_or_else(|e| e.into_inner());
        if keys
            .iter()
            .flatten()
            .any(|k| m.get(k).copied().unwrap_or(0) >= cap(k))
        {
            return None;
        }
        for k in keys.iter().flatten() {
            *m.entry(*k).or_insert(0) += 1;
        }
        Some(ConnGuard {
            keys,
            open: self.open.clone(),
        })
    }

    /// Slots open for `ip`'s client key.
    pub fn open(&self, ip: IpAddr) -> usize {
        let m = self.open.lock().unwrap_or_else(|e| e.into_inner());
        m.get(&(0, bucket_key(ip))).copied().unwrap_or(0)
    }

    /// Slots open for `ip`'s IPv6 /48 (0 for an IPv4 client).
    pub fn open_prefix(&self, ip: IpAddr) -> usize {
        let m = self.open.lock().unwrap_or_else(|e| e.into_inner());
        prefix48(ip).map_or(0, |p| m.get(&(1, p)).copied().unwrap_or(0))
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        let mut m = self.open.lock().unwrap_or_else(|e| e.into_inner());
        for k in self.keys.iter().flatten() {
            if let Some(n) = m.get_mut(k) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    m.remove(k);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A GraphQL batch of N takes N tokens at once; a refusal takes none.
    #[test]
    fn allow_n_takes_all_or_nothing() {
        let l = RateLimiter::new(1, 5);
        let a: IpAddr = "198.51.100.7".parse().unwrap();
        assert!(l.allow_n(a, 5));
        assert!(!l.allow(a));
        let b: IpAddr = "198.51.100.8".parse().unwrap();
        assert!(!l.allow_n(b, 6));
        assert!(l.allow_n(b, 5), "the refused batch took no tokens");
    }

    /// The token bucket refills at `rate` per second up to `burst`, on injected time (no
    /// sleeps): every step is checked against an exact integer model in millitokens. Steps
    /// are quarter seconds, so the `f64` arithmetic of the limiter is exact too.
    mod refill {
        use super::*;
        use proptest::prelude::*;
        use std::time::Duration;

        #[derive(Debug, Clone)]
        enum Op {
            Wait(u32),
            Take(u32),
        }

        fn ops() -> impl Strategy<Value = Vec<Op>> {
            proptest::collection::vec(
                prop_oneof![(0u32..60).prop_map(Op::Wait), (1u32..8).prop_map(Op::Take)],
                1..60,
            )
        }

        proptest! {
            #![proptest_config(ProptestConfig { cases: 256, failure_persistence: None, ..ProptestConfig::default() })]

            #[test]
            fn refill_matches_the_integer_model(rate in 1u32..20, burst in 1u32..30, ops in ops()) {
                let l = RateLimiter::new(rate, burst);
                let ip: IpAddr = "198.51.100.1".parse().unwrap();
                let t0 = Instant::now();
                let mut now = t0;
                // Millitokens: a quarter second adds `rate * 250`.
                let cap = i64::from(burst) * 1000;
                let mut tokens = cap;
                for op in ops {
                    match op {
                        Op::Wait(q) => {
                            now += Duration::from_millis(250 * u64::from(q));
                            tokens = (tokens + i64::from(rate) * 250 * i64::from(q)).min(cap);
                        }
                        Op::Take(n) => {
                            let want = i64::from(n) * 1000;
                            let allowed = l.allow_n_at(ip, n, now);
                            prop_assert_eq!(allowed, tokens >= want, "tokens {} want {}", tokens, want);
                            if allowed {
                                tokens -= want;
                            }
                        }
                    }
                }
            }
        }

        /// A drained bucket yields nothing until time passes, one token after `1/rate` s, and
        /// never more than `burst` after a long idle.
        #[test]
        fn drained_bucket_refills_one_token_per_interval() {
            let l = RateLimiter::new(2, 4);
            let ip: IpAddr = "198.51.100.2".parse().unwrap();
            let t0 = Instant::now();
            assert!(l.allow_n_at(ip, 4, t0));
            assert!(!l.allow_n_at(ip, 1, t0));
            assert!(!l.allow_n_at(ip, 1, t0 + Duration::from_millis(250)));
            assert!(l.allow_n_at(ip, 1, t0 + Duration::from_millis(500)));
            assert!(!l.allow_n_at(ip, 1, t0 + Duration::from_millis(500)));
            let idle = t0 + Duration::from_secs(3600);
            assert!(l.allow_n_at(ip, 4, idle));
            assert!(
                !l.allow_n_at(ip, 1, idle),
                "idle time never banks more than burst"
            );
        }
    }

    #[test]
    fn internal_and_cidr() {
        for a in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.1",
            "::1",
            "fd00::1",
            "::ffff:10.0.0.1",
        ] {
            assert!(is_internal(a.parse().unwrap()), "{a}");
        }
        for a in ["0.0.0.0", "8.8.8.8", "172.32.0.1", "::", "2001:db8::1"] {
            assert!(!is_internal(a.parse().unwrap()), "{a}");
        }
        let n = IpNet::parse("10.0.0.0/8").unwrap();
        assert!(n.contains("10.9.9.9".parse().unwrap()));
        assert!(!n.contains("11.0.0.1".parse().unwrap()));
        assert!(IpNet::parse("10.0.0.0/33").is_none());
        assert!(IpNet::parse("nope").is_none());
    }

    #[test]
    fn forwarded_for_only_from_trusted() {
        let trusted = vec![IpNet::parse("127.0.0.1").unwrap()];
        let mut h = HeaderMap::new();
        h.insert(
            "x-forwarded-for",
            "1.1.1.1, 2.2.2.2, 127.0.0.1".parse().unwrap(),
        );
        let local: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let other: SocketAddr = "9.9.9.9:5000".parse().unwrap();
        assert_eq!(
            client_ip(local, &h, &trusted),
            "2.2.2.2".parse::<IpAddr>().unwrap()
        );
        assert_eq!(client_ip(other, &h, &trusted), other.ip());
        assert_eq!(client_ip(local, &h, &[]), local.ip());
    }

    #[test]
    fn forwarded_for_stops_at_unparsable_or_untrusted() {
        let trusted = vec![
            IpNet::parse("127.0.0.1").unwrap(),
            IpNet::parse("10.0.0.0/8").unwrap(),
        ];
        let local: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let xff = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert("x-forwarded-for", v.parse().unwrap());
            client_ip(local, &h, &trusted)
        };
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        // Ports and brackets are stripped.
        assert_eq!(xff("6.6.6.6, 2.2.2.2:4433"), ip("2.2.2.2"));
        assert_eq!(xff("6.6.6.6, [2001:db8::7]:4433"), ip("2001:db8::7"));
        assert_eq!(xff("6.6.6.6, [2001:db8::7]"), ip("2001:db8::7"));
        assert_eq!(xff("6.6.6.6, 2001:db8::7, 10.0.0.2"), ip("2001:db8::7"));
        // An unparsable entry ends the walk at the peer: never skip into client-set entries.
        assert_eq!(xff("6.6.6.6, garbage"), local.ip());
        assert_eq!(xff("6.6.6.6, unknown, 10.0.0.2"), local.ip());
        assert_eq!(xff("6.6.6.6, , 10.0.0.2"), local.ip());
        // Several header lines read as one list, right-most line last.
        let mut h = HeaderMap::new();
        h.append("x-forwarded-for", "6.6.6.6".parse().unwrap());
        h.append("x-forwarded-for", "3.3.3.3, 10.0.0.9".parse().unwrap());
        assert_eq!(client_ip(local, &h, &trusted), ip("3.3.3.3"));
        // All trusted: the left-most entry; no header: the peer.
        assert_eq!(xff("10.0.0.3, 10.0.0.2"), ip("10.0.0.3"));
        assert_eq!(client_ip(local, &HeaderMap::new(), &trusted), local.ip());
    }

    #[test]
    fn bucket_v6_keyed_on_64() {
        let r = RateLimiter::new(1, 2);
        let a: IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let other: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert!(r.allow(a));
        assert!(r.allow(b));
        assert!(!r.allow(a), "same /64 shares one bucket");
        assert!(r.allow(other));
        // v4-mapped v6 shares the v4 bucket.
        let v4: IpAddr = "1.2.3.4".parse().unwrap();
        let mapped: IpAddr = "::ffff:1.2.3.4".parse().unwrap();
        assert!(r.allow(v4) && r.allow(mapped));
        assert!(!r.allow(v4));
    }

    #[test]
    fn bucket_hard_cap_evicts_oldest() {
        let r = RateLimiter::with_cap(1, 1, 3);
        let ip = |n: u8| IpAddr::from([1, 1, 1, n]);
        for n in 1..=3 {
            assert!(r.allow(ip(n)));
            assert!(!r.allow(ip(n)));
        }
        // A fourth key evicts the oldest (1) only; 2 and 3 stay empty.
        assert!(r.allow(ip(4)));
        assert_eq!(r.len(), 3);
        assert!(!r.allow(ip(2)));
        assert!(!r.allow(ip(3)));
        assert!(!r.allow(ip(1)), "a drained, evicted key comes back empty");
        assert_eq!(r.len(), 3);
        for n in 10..200 {
            r.allow(ip(n));
        }
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn eviction_prefers_refilled_buckets() {
        let r = RateLimiter::with_cap(1, 2, 2);
        let t0 = Instant::now();
        let t5 = t0 + std::time::Duration::from_secs(5);
        let ip = |n: u8| IpAddr::from([1, 1, 1, n]);
        assert!(r.allow_at(ip(2), t0));
        assert!(r.allow_at(ip(1), t0) && r.allow_at(ip(1), t0));
        assert!(r.allow_at(ip(2), t5) && r.allow_at(ip(2), t5));
        // 2 is oldest but drained at t5; 1 refilled by t5: 1 goes, 2 stays drained.
        assert!(
            r.allow_at(ip(3), t5) && r.allow_at(ip(3), t5),
            "a free slot starts full"
        );
        assert!(!r.allow_at(ip(2), t5));
    }

    #[test]
    fn eviction_never_grants_a_drained_key_a_full_burst() {
        let r = RateLimiter::with_cap(1, 2, 2);
        let t0 = Instant::now();
        let ip = |n: u8| IpAddr::from([1, 1, 1, n]);
        for n in [1, 2] {
            assert!(r.allow_at(ip(n), t0) && r.allow_at(ip(n), t0));
            assert!(!r.allow_at(ip(n), t0));
        }
        // Table full of drained buckets: new keys start empty and draw on the shared
        // overflow bucket (burst 2), so cycling keys gains nothing.
        assert!(r.allow_at(ip(3), t0), "overflow token 1");
        assert!(r.allow_at(ip(1), t0), "overflow token 2 (1 was evicted)");
        assert!(!r.allow_at(ip(1), t0), "1 came back empty");
        assert!(!r.allow_at(ip(4), t0), "overflow drained");
        assert!(!r.allow_at(ip(2), t0));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn overflow_is_per_prefix() {
        let r = RateLimiter::with_cap(1, 2, 2);
        let t0 = Instant::now();
        let v6 = |s: &str| s.parse::<IpAddr>().unwrap();
        // The attacker fills the table with drained /64s of 2001:db8:aaaa::/48.
        for k in ["2001:db8:aaaa:1::1", "2001:db8:aaaa:2::1"] {
            while r.allow_at(v6(k), t0) {}
        }
        // Rotating /64s inside one /48 gets at most that /48's overflow burst.
        let served = (0..100u16)
            .filter(|n| {
                r.allow_at(
                    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0xaaaa, 0x100 + n, 0, 0, 0, 1)),
                    t0,
                )
            })
            .count();
        assert_eq!(served, 2);
        // A client from another /48 is still served under the same pressure.
        assert!(r.allow_at(v6("2001:db8:cccc:1::1"), t0));
        assert!(r.allow_at(v6("2001:db8:dddd:1::1"), t0));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn bucket() {
        let r = RateLimiter::new(1, 3);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        assert!((0..3).all(|_| r.allow(ip)));
        assert!(!r.allow(ip));
    }

    #[test]
    fn conn_limiter_counts_per_key_and_releases_on_drop() {
        let l = ConnLimiter::new(2, 100);
        let a: IpAddr = "2001:db8::1".parse().unwrap();
        let b: IpAddr = "2001:db8::2".parse().unwrap(); // same /64
        let c: IpAddr = "192.0.2.7".parse().unwrap();
        let g1 = l.try_acquire(a).unwrap();
        let g2 = l.try_acquire(b).unwrap();
        assert!(l.try_acquire(a).is_none());
        assert!(l.try_acquire(c).is_some());
        drop(g1);
        let g3 = l.try_acquire(b).unwrap();
        drop((g2, g3));
        assert_eq!(l.open(a), 0);
        assert!(l.open.lock().unwrap().is_empty());
    }

    /// M-2: /64s of one /48 share the /48's cap; another /48 and IPv4 clients do not.
    #[test]
    fn conn_limiter_caps_each_ipv6_48() {
        let l = ConnLimiter::new(2, 3);
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        let a = l.try_acquire(ip("2001:db8:1:1::1")).unwrap();
        let b = l.try_acquire(ip("2001:db8:1:2::1")).unwrap();
        let c = l.try_acquire(ip("2001:db8:1:3::1")).unwrap();
        // A fresh /64 of the same /48 is refused; one of another /48 is not.
        assert!(l.try_acquire(ip("2001:db8:1:4::1")).is_none());
        assert_eq!(l.open_prefix(ip("2001:db8:1:ffff::9")), 3);
        let d = l.try_acquire(ip("2001:db8:2::1")).unwrap();
        // A /64 whose bits 48..64 are zero is counted apart from its /48.
        let e = l.try_acquire(ip("2001:db8:3::1")).unwrap();
        assert_eq!(
            (
                l.open(ip("2001:db8:3::5")),
                l.open_prefix(ip("2001:db8:3::5"))
            ),
            (1, 1)
        );
        let v4: Vec<_> = (0..2)
            .map(|_| l.try_acquire(ip("192.0.2.1")).unwrap())
            .collect();
        assert!(l.try_acquire(ip("192.0.2.1")).is_none());
        assert!(l.try_acquire(ip("192.0.2.2")).is_some());
        drop(b);
        let f = l.try_acquire(ip("2001:db8:1:4::1")).unwrap();
        drop((a, c, d, e, f, v4));
        assert!(l.open.lock().unwrap().is_empty());
    }
}
