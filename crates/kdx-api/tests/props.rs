//! Property tests for the request-facing pieces of kdx-api (plan Task 13, T-1): cursor tokens,
//! `X-Forwarded-For` parsing, CIDR matching, the per-IP token bucket and the compat number
//! rendering. Each compares with an independent model written here.

#[macro_use]
#[path = "../../kdx-types/tests/common/prop.rs"]
mod prop;
use prop::*;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use axum::http::{HeaderMap, HeaderValue};
use kdx_api::compat::shapes::aeson_number;
use kdx_api::cursor::Cursor;
use kdx_api::limits::{client_ip, IpNet, RateLimiter};
use kdx_types::TxKey;

// ---------------------------------------------------------------- cursor tokens

fn cursor() -> impl Strategy<Value = Cursor> {
    (any::<u64>(), any::<u16>(), any::<u32>()).prop_map(|(k, sub, fp)| Cursor {
        key: TxKey(k),
        sub,
        fp,
    })
}

props! {
    fn cursor_roundtrip(c in cursor()) {
        let t = c.encode();
        prop_assert_eq!(Cursor::decode(&t).unwrap(), c);
        // Independent length check: 15 bytes in base64url without padding is 20 characters.
        prop_assert_eq!(t.len(), 20);
        prop_assert!(t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    }

    fn cursor_truncation_and_extension_are_400(c in cursor(), cut in any::<proptest::sample::Index>(), extra in "[A-Za-z0-9_-]{1,8}") {
        let t = c.encode();
        let k = cut.index(t.len());
        prop_assert!(Cursor::decode(&t[..k]).is_err());
        let longer = [t.as_str(), extra.as_str()].concat();
        prop_assert!(Cursor::decode(&longer).is_err());
    }

    fn cursor_flips_never_panic_and_never_alias(c in cursor(), at in any::<proptest::sample::Index>(), to in "[A-Za-z0-9_-]") {
        // A flipped character is either refused or a different cursor, never the same one and
        // never a panic. (The token has no checksum: a flipped key is a valid other key; the
        // block fingerprint inside it is what rejects a cursor after a reorg.)
        let mut t = c.encode().into_bytes();
        let i = at.index(t.len());
        let to = to.as_bytes()[0];
        let changed = t[i] != to;
        t[i] = to;
        let t = String::from_utf8(t).unwrap();
        if let Ok(d) = Cursor::decode(&t) {
            prop_assert!(!changed || d != c || i == 19);
        }
        // The first byte is the version: any other value is refused.
    }

    fn cursor_garbage_never_panics(s in "\\PC{0,100}") {
        let _ = Cursor::decode(&s);
    }
}

// ---------------------------------------------------------------- X-Forwarded-For

#[derive(Debug, Clone)]
enum Entry {
    Ip(IpAddr, Form),
    Junk(String),
}

#[derive(Debug, Clone, Copy)]
enum Form {
    Bare,
    Port,
    Padded,
}

fn render(e: &Entry) -> String {
    match e {
        Entry::Junk(s) => s.clone(),
        Entry::Ip(ip, Form::Bare) => match ip {
            IpAddr::V4(_) => ip.to_string(),
            IpAddr::V6(_) => ip.to_string(),
        },
        Entry::Ip(ip, Form::Port) => SocketAddr::new(*ip, 4433).to_string(),
        Entry::Ip(ip, Form::Padded) => format!("  {ip} "),
    }
}

fn ip() -> impl Strategy<Value = IpAddr> {
    prop_oneof![
        // A few fixed addresses so that "trusted" and "untrusted" collide often.
        proptest::sample::select(vec![
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9)),
            IpAddr::V6("fd00::1".parse().unwrap()),
            IpAddr::V6("2001:db8::5".parse().unwrap()),
        ]),
        any::<[u8; 4]>().prop_map(|b| IpAddr::V4(Ipv4Addr::from(b))),
        any::<[u8; 16]>().prop_map(|b| IpAddr::V6(Ipv6Addr::from(b))),
    ]
}

fn entry() -> impl Strategy<Value = Entry> {
    prop_oneof![
        8 => (ip(), prop_oneof![Just(Form::Bare), Just(Form::Port), Just(Form::Padded)])
            .prop_map(|(i, f)| Entry::Ip(i, f)),
        1 => proptest::sample::select(vec!["unknown", "", "999.1.1.1", "1.2.3", "_hidden", "[::1", "10.0.0.1:99999"])
            .prop_map(|s| Entry::Junk(s.to_string())),
    ]
}

fn trusted_nets() -> Vec<IpNet> {
    ["10.0.0.0/8", "fd00::/8", "198.51.100.9"]
        .iter()
        .map(|s| IpNet::parse(s).unwrap())
        .collect()
}

fn is_trusted(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => v.octets()[0] == 10 || v == Ipv4Addr::new(198, 51, 100, 9),
        IpAddr::V6(v) => (v.segments()[0] >> 8) == 0xfd,
    }
}

props! {
    fn xff_walks_right_to_left_and_stops_at_the_first_untrusted(
        peer in ip(),
        lines in proptest::collection::vec(proptest::collection::vec(entry(), 1..4), 0..3),
    ) {
        let trusted = trusted_nets();
        let mut h = HeaderMap::new();
        for l in &lines {
            let text = l.iter().map(render).collect::<Vec<_>>().join(",");
            h.append("x-forwarded-for", HeaderValue::from_str(&text).unwrap());
        }
        let got = client_ip(SocketAddr::new(peer, 1), &h, &trusted);
        // Model: entries from the right-most header line's right-most entry, leftwards.
        let want = if !is_trusted(peer) {
            peer
        } else {
            let mut leftmost = None;
            let mut out = None;
            'walk: for l in lines.iter().rev() {
                for e in l.iter().rev() {
                    match e {
                        Entry::Junk(_) => {
                            out = Some(peer);
                            break 'walk;
                        }
                        Entry::Ip(ip, _) => {
                            if !is_trusted(*ip) {
                                out = Some(*ip);
                                break 'walk;
                            }
                            leftmost = Some(*ip);
                        }
                    }
                }
            }
            out.or(leftmost).unwrap_or(peer)
        };
        prop_assert_eq!(got, want);
    }

    fn xff_is_ignored_from_an_untrusted_peer(peer in ip(), forged in ip()) {
        if is_trusted(peer) {
            return Ok(());
        }
        let mut h = HeaderMap::new();
        h.append("x-forwarded-for", HeaderValue::from_str(&forged.to_string()).unwrap());
        prop_assert_eq!(client_ip(SocketAddr::new(peer, 1), &h, &trusted_nets()), peer);
        // And with no trusted proxies configured at all.
        prop_assert_eq!(client_ip(SocketAddr::new(peer, 1), &h, &[]), peer);
    }
}

// ---------------------------------------------------------------- CIDR matching

props! {
    fn cidr_v4_matches_a_shift_reference(net in any::<u32>(), p in 0u32..=32, probe in any::<u32>(), same_prefix in any::<bool>()) {
        let spec = format!("{}/{}", Ipv4Addr::from(net), p);
        let n = IpNet::parse(&spec).unwrap();
        // Reference: compare the top `p` bits with a 64-bit shift.
        let top = |x: u32| (x as u64) >> (32 - p);
        let probe = if same_prefix { (net & !(u32::MAX.checked_shr(p).unwrap_or(0))) | (probe & u32::MAX.checked_shr(p).unwrap_or(0)) } else { probe };
        prop_assert_eq!(n.contains(IpAddr::V4(Ipv4Addr::from(probe))), top(net) == top(probe));
        // A v4-mapped v6 address is the v4 address.
        prop_assert_eq!(n.contains(IpAddr::V6(Ipv4Addr::from(probe).to_ipv6_mapped())), top(net) == top(probe));
        let too_wide = format!("{}/{}", Ipv4Addr::from(net), 33 + p);
        prop_assert!(IpNet::parse(&too_wide).is_none());
    }

    fn cidr_v6_matches_a_shift_reference(net in any::<u128>(), p in 0u32..=128, probe in any::<u128>(), same_prefix in any::<bool>()) {
        let n = IpNet::parse(&format!("{}/{}", Ipv6Addr::from(net), p)).unwrap();
        let top = |x: u128| if p == 0 { 0 } else { x >> (128 - p) };
        let low_mask = if p == 128 { 0 } else { u128::MAX >> p };
        let probe = if same_prefix { (net & !low_mask) | (probe & low_mask) } else { probe };
        // An address in the v4-mapped block is matched as its v4 address, not as v6.
        if Ipv6Addr::from(probe).to_ipv4_mapped().is_some() {
            return Ok(());
        }
        prop_assert_eq!(n.contains(IpAddr::V6(Ipv6Addr::from(probe))), top(net) == top(probe));
        let too_wide = format!("{}/{}", Ipv6Addr::from(net), 129 + p);
        prop_assert!(IpNet::parse(&too_wide).is_none());
    }

    fn cidr_garbage_never_panics(s in "\\PC{0,40}") {
        let _ = IpNet::parse(&s);
    }
}

// ---------------------------------------------------------------- per-IP token bucket

props! {
    fn one_v6_slash64_shares_one_bucket(hi in any::<u64>(), lows in proptest::collection::vec(any::<u64>(), 40), burst in 1u32..12) {
        // S2: rotating the low 64 bits does not mint new buckets. The rate is 1/s and the loop
        // takes microseconds, so at most `burst` (plus one refill) requests pass.
        let lim = RateLimiter::new(1, burst);
        let mut allowed = 0;
        for l in &lows {
            let a = Ipv6Addr::from((hi as u128) << 64 | *l as u128);
            allowed += lim.allow(IpAddr::V6(a)) as u32;
        }
        prop_assert!(allowed >= burst.min(40) && allowed <= burst + 1, "allowed {} of burst {}", allowed, burst);
        prop_assert_eq!(lim.len(), 1);
    }

    fn v4_mapped_v6_and_v4_share_a_bucket(b in any::<[u8; 4]>(), burst in 1u32..8) {
        let lim = RateLimiter::new(1, burst);
        let v4 = Ipv4Addr::from(b);
        let mut allowed = 0;
        for i in 0..(burst * 2 + 4) {
            let ip = if i % 2 == 0 { IpAddr::V4(v4) } else { IpAddr::V6(v4.to_ipv6_mapped()) };
            allowed += lim.allow(ip) as u32;
        }
        prop_assert!(allowed >= burst && allowed <= burst + 1);
        prop_assert_eq!(lim.len(), 1);
    }

    fn the_table_is_capped_and_cycling_keys_does_not_refill_a_drained_one(
        first in any::<[u8; 4]>(),
        others in proptest::collection::vec(any::<[u8; 4]>(), 100),
        cap in 16usize..40,
    ) {
        let lim = RateLimiter::with_cap(1, 3, cap);
        let victim = IpAddr::V4(Ipv4Addr::from(first));
        while lim.allow(victim) {}
        for o in others {
            let o = IpAddr::V4(Ipv4Addr::from(o));
            let _ = lim.allow(o);
            prop_assert!(lim.len() <= cap, "table grew to {} (cap {})", lim.len(), cap);
        }
        // Eviction never refills a drained key (S2): the victim is still empty.
        prop_assert!(!lim.allow(victim));
    }
}

// ---------------------------------------------------------------- compat number text (R12)

fn decimal_text() -> impl Strategy<Value = String> {
    (
        any::<bool>(),
        "0|[1-9][0-9]{0,5}",
        proptest::option::of("[0-9]{1,8}"),
        proptest::option::of((any::<bool>(), 0u32..40)),
    )
        .prop_map(|(neg, int, frac, exp)| {
            let mut s = String::new();
            if neg {
                s.push('-');
            }
            s.push_str(&int);
            if let Some(f) = frac {
                s.push('.');
                s.push_str(&f);
            }
            if let Some((eneg, e)) = exp {
                s.push('e');
                if eneg {
                    s.push('-');
                }
                s.push_str(&e.to_string());
            }
            s
        })
}

props! {
    fn aeson_number_is_the_same_number_in_haskell_show_form(t in decimal_text()) {
        let out = aeson_number(&t);
        let v: f64 = t.parse().unwrap();
        // Same value (both parse to the same nearest double).
        prop_assert_eq!(out.parse::<f64>().unwrap(), v, "{} -> {}", t, out);
        // Shape: digits, optional fraction, optional `e<int>`; no `E`, no `+`.
        let re = regex::Regex::new(r"^-?[0-9]+(\.[0-9]+)?(e-?[0-9]+)?$").unwrap();
        prop_assert!(re.is_match(&out), "{} -> {}", t, out);
        // Haskell/aeson: a Scientific whose effective exponent (text exponent minus fraction
        // digits) is >= 0 is an integer and prints as one; otherwise `show` uses the exponent
        // form exactly below 0.1 or from 1e7 up, and plain decimals in between.
        let (mant, exp) = t.split_once(['e', 'E']).map_or((t.as_str(), 0i64), |(m, e)| (m, e.parse().unwrap()));
        let frac_len = mant.split_once('.').map_or(0, |(_, f)| f.len()) as i64;
        if exp - frac_len >= 0 {
            prop_assert!(!out.contains(['.', 'e']), "{} -> {}", t, out);
        } else {
            let a = v.abs();
            let sci = a != 0.0 && !(0.1..1e7).contains(&a);
            prop_assert_eq!(out.contains('e'), sci, "{} -> {}", t, out);
        }
    }
}

#[test]
fn compat_number_edge_table_matches_chainweb_data() {
    // R12: values as chainweb-data (aeson) renders them.
    for (input, want) in [
        ("3.34e-6", "3.34e-6"),
        ("0.00000334", "3.34e-6"),
        ("0.90404266", "0.90404266"),
        ("1.0", "1.0"),
        ("0.0", "0.0"),
        ("0", "0"),
        ("100", "100"),
        ("1e2", "100"),
        ("100.0", "100.0"),
        ("0.1", "0.1"),
        ("0.09999999", "9.999999e-2"),
        ("9999999.0", "9999999.0"),
        ("10000000.0", "1.0e7"),
        ("12345678.5", "1.23456785e7"),
        ("-0.05", "-5.0e-2"),
        ("1.5e-9223372036854775808", "1.5e-9223372036854775808"),
        ("1.5e9223372036854775807", "1.5e9223372036854775807"),
    ] {
        assert_eq!(aeson_number(input), want, "input {input}");
    }
}
