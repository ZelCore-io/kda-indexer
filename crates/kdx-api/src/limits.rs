//! Request limits (spec §5.4): internal-address check, trusted-proxy client IP, per-IP token
//! bucket. The in-flight cap, deadline and body cap live in `server::guard`.

use std::collections::HashMap;
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
    let m = if p == 0 { 0 } else { u32::MAX << (32 - p as u32) };
    u32::from(a) & m
}

fn mask6(a: Ipv6Addr, p: u8) -> u128 {
    let m = if p == 0 { 0 } else { u128::MAX << (128 - p as u32) };
    u128::from(a) & m
}

/// The client's IP: the peer, or, when the peer is a trusted proxy, the right-most
/// `X-Forwarded-For` entry that is not itself a trusted proxy.
pub fn client_ip(peer: SocketAddr, headers: &HeaderMap, trusted: &[IpNet]) -> IpAddr {
    let is_trusted = |ip: IpAddr| trusted.iter().any(|n| n.contains(ip));
    let peer_ip = peer.ip();
    if trusted.is_empty() || !is_trusted(peer_ip) {
        return peer_ip;
    }
    let mut chain: Vec<IpAddr> = Vec::new();
    for v in headers.get_all("x-forwarded-for") {
        if let Ok(s) = v.to_str() {
            chain.extend(s.split(',').filter_map(|x| x.trim().parse::<IpAddr>().ok()));
        }
    }
    chain
        .iter()
        .rev()
        .find(|&&ip| !is_trusted(ip))
        .or(chain.first())
        .copied()
        .unwrap_or(peer_ip)
}

/// Per-IP token bucket: `rate` tokens per second, at most `burst`.
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    buckets: Mutex<HashMap<IpAddr, (f64, Instant)>>,
}

/// Buckets kept before idle (full) ones are dropped.
const MAX_BUCKETS: usize = 65_536;

impl RateLimiter {
    pub fn new(rate: u32, burst: u32) -> RateLimiter {
        RateLimiter {
            rate: rate.max(1) as f64,
            burst: burst.max(1) as f64,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Take one token for `ip`; false when the bucket is empty.
    pub fn allow(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut m = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() >= MAX_BUCKETS && !m.contains_key(&ip) {
            let (rate, burst) = (self.rate, self.burst);
            m.retain(|_, (t, at)| *t + now.duration_since(*at).as_secs_f64() * rate < burst);
        }
        let (tokens, at) = m.entry(ip).or_insert((self.burst, now));
        *tokens = (*tokens + now.duration_since(*at).as_secs_f64() * self.rate).min(self.burst);
        *at = now;
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
        for a in ["127.0.0.1", "10.1.2.3", "172.16.0.1", "172.31.255.1", "::1", "fd00::1", "::ffff:10.0.0.1"] {
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
        h.insert("x-forwarded-for", "1.1.1.1, 2.2.2.2, 127.0.0.1".parse().unwrap());
        let local: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let other: SocketAddr = "9.9.9.9:5000".parse().unwrap();
        assert_eq!(client_ip(local, &h, &trusted), "2.2.2.2".parse::<IpAddr>().unwrap());
        assert_eq!(client_ip(other, &h, &trusted), other.ip());
        assert_eq!(client_ip(local, &h, &[]), local.ip());
    }

    #[test]
    fn bucket() {
        let r = RateLimiter::new(1, 3);
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        assert!((0..3).all(|_| r.allow(ip)));
        assert!(!r.allow(ip));
    }
}
