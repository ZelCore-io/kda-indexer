#![no_main]
//! `client_ip`: the peer, the trusted-proxy list and `X-Forwarded-For` header lines.
//! Input: `peer-selector u8 | header bytes` (a 0x0a splits the value into repeated header lines).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use kdx_api::limits::{client_ip, IpNet};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let trusted: Vec<IpNet> = ["10.0.0.0/8", "127.0.0.1", "::1", "fd00::/8", "192.0.2.0/24"]
        .iter()
        .filter_map(|s| IpNet::parse(s))
        .collect();
    let peer = match sel & 3 {
        0 => IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
        1 => IpAddr::V4(Ipv4Addr::LOCALHOST),
        2 => IpAddr::V6(Ipv6Addr::LOCALHOST),
        _ => IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
    };
    let mut headers = HeaderMap::new();
    for line in rest.split(|&b| b == b'\n') {
        if let Ok(v) = HeaderValue::from_bytes(line) {
            headers.append(HeaderName::from_static("x-forwarded-for"), v);
        }
    }
    let ip = client_ip(SocketAddr::new(peer, 4000), &headers, &trusted);
    // The answer is the peer or an address from the header; an untrusted peer is never replaced.
    if !trusted.iter().any(|n| n.contains(peer)) {
        assert_eq!(ip, peer);
    }
});
