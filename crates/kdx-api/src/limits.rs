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
fn bucket_key(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(_) => ip,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(Ipv6Addr::from(mask6(v6, 64))),
        },
    }
}

struct Buckets {
    /// key -> (tokens, last refill, insertion sequence)
    map: HashMap<IpAddr, (f64, Instant, u64)>,
    /// Keys in insertion order with their sequence; stale pairs (key evicted and re-added)
    /// are skipped on eviction.
    order: VecDeque<(IpAddr, u64)>,
    seq: u64,
    /// Shared bucket charged for keys created by evicting a non-full bucket.
    overflow: Option<(f64, Instant)>,
}

/// Queue entries looked at per insert for a refilled bucket to evict.
const EVICT_SCAN: usize = 16;

/// Per-IP token bucket: `rate` tokens per second, at most `burst`. At most `cap` buckets;
/// a new key over the cap evicts a refilled (full) bucket among the oldest few, else the
/// oldest one. Evicting a full bucket loses nothing; when a non-full one had to go, the new
/// key starts empty and its first token comes from a shared overflow bucket, so cycling
/// keys through the table never refills a drained key. O(1) per insert, no full scan.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    cap: usize,
    buckets: Mutex<Buckets>,
}

/// Buckets kept before the oldest are evicted.
const MAX_BUCKETS: usize = 65_536;

impl RateLimiter {
    pub fn new(rate: u32, burst: u32) -> RateLimiter {
        RateLimiter::with_cap(rate, burst, MAX_BUCKETS)
    }

    pub fn with_cap(rate: u32, burst: u32, cap: usize) -> RateLimiter {
        RateLimiter {
            rate: rate.max(1) as f64,
            burst: burst.max(1) as f64,
            cap: cap.max(1),
            buckets: Mutex::new(Buckets {
                map: HashMap::new(),
                order: VecDeque::new(),
                seq: 0,
                overflow: None,
            }),
        }
    }

    /// Buckets held.
    pub fn len(&self) -> usize {
        self.buckets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Evict one bucket: a refilled one among the first `EVICT_SCAN` queue entries (others
    /// rotate to the back), else the oldest. True when a non-full bucket was evicted.
    fn make_room(&self, b: &mut Buckets, now: Instant) -> bool {
        for _ in 0..EVICT_SCAN {
            let Some((k, seq)) = b.order.pop_front() else {
                break;
            };
            let Some(&(t, at, s)) = b.map.get(&k) else {
                continue;
            };
            if s != seq {
                continue;
            }
            if t + now.saturating_duration_since(at).as_secs_f64() * self.rate >= self.burst {
                b.map.remove(&k);
                return false;
            }
            b.order.push_back((k, seq));
        }
        while let Some((k, seq)) = b.order.pop_front() {
            if b.map.get(&k).is_some_and(|e| e.2 == seq) {
                b.map.remove(&k);
                return true;
            }
        }
        false
    }

    /// Take one token for `ip`; false when the bucket is empty.
    pub fn allow(&self, ip: IpAddr) -> bool {
        self.allow_at(ip, Instant::now())
    }

    fn allow_at(&self, ip: IpAddr, now: Instant) -> bool {
        let key = bucket_key(ip);
        let mut g = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = &mut *g;
        let mut pressured = false;
        if !b.map.contains_key(&key) {
            if b.map.len() >= self.cap {
                pressured = self.make_room(b, now);
            }
            b.seq += 1;
            let start = if pressured { 0.0 } else { self.burst };
            b.map.insert(key, (start, now, b.seq));
            b.order.push_back((key, b.seq));
        }
        let (tokens, at, _) = b.map.get_mut(&key).expect("inserted above");
        *tokens = (*tokens + now.saturating_duration_since(*at).as_secs_f64() * self.rate)
            .min(self.burst);
        *at = now;
        if pressured {
            let (t, at) = b.overflow.get_or_insert((self.burst, now));
            *t =
                (*t + now.saturating_duration_since(*at).as_secs_f64() * self.rate).min(self.burst);
            *at = now;
            if *t >= 1.0 {
                *t -= 1.0;
                return true;
            }
            return false;
        }
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn bucket() {
        let r = RateLimiter::new(1, 3);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        assert!((0..3).all(|_| r.allow(ip)));
        assert!(!r.allow(ip));
    }
}
