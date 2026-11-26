//! Exact non-float decimals for the DEX fields (B3): an arbitrary-precision magnitude in
//! base-10⁹ limbs with a fixed [`SCALE`] of fractional digits. Products and quotients
//! truncate past `SCALE` digits; [`Dec::round`] rounds half away from zero for output. No
//! operation panics: division by zero answers `None`.

use std::cmp::Ordering;

use kdx_hot::dex::Amount;

/// Fractional digits kept.
pub const SCALE: usize = 30;
const BASE: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dec {
    neg: bool,
    /// `value × 10^SCALE`, little-endian limbs, no high zero limbs (zero is empty).
    mag: Vec<u32>,
}

fn trim(v: &mut Vec<u32>) {
    while v.last() == Some(&0) {
        v.pop();
    }
}

fn cmp_mag(a: &[u32], b: &[u32]) -> Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.iter().rev().cmp(b.iter().rev()))
}

fn add_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().max(b.len()) + 1);
    let mut carry = 0u64;
    for i in 0..a.len().max(b.len()) {
        let s = carry + *a.get(i).unwrap_or(&0) as u64 + *b.get(i).unwrap_or(&0) as u64;
        out.push((s % BASE) as u32);
        carry = s / BASE;
    }
    if carry > 0 {
        out.push(carry as u32);
    }
    out
}

/// `a − b`, `a ≥ b`.
fn sub_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len());
    let mut borrow = 0i64;
    for (i, x) in a.iter().enumerate() {
        let mut d = *x as i64 - borrow - *b.get(i).unwrap_or(&0) as i64;
        borrow = 0;
        if d < 0 {
            d += BASE as i64;
            borrow = 1;
        }
        out.push(d as u32);
    }
    trim(&mut out);
    out
}

fn mul_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let mut acc = vec![0u64; a.len() + b.len() + 1];
    for (i, x) in a.iter().enumerate() {
        let mut carry = 0u64;
        for (j, y) in b.iter().enumerate() {
            let t = acc[i + j] + *x as u64 * *y as u64 + carry;
            acc[i + j] = t % BASE;
            carry = t / BASE;
        }
        let mut k = i + b.len();
        while carry > 0 {
            let t = acc[k] + carry;
            acc[k] = t % BASE;
            carry = t / BASE;
            k += 1;
        }
    }
    let mut out: Vec<u32> = acc.into_iter().map(|x| x as u32).collect();
    trim(&mut out);
    out
}

fn mul_small(a: &[u32], m: u32) -> Vec<u32> {
    mul_mag(a, &[m]).into_iter().collect()
}

/// `(a / d, a % d)` for a small divisor.
fn divmod_small(a: &[u32], d: u32) -> (Vec<u32>, u32) {
    let mut q = vec![0u32; a.len()];
    let mut r = 0u64;
    for i in (0..a.len()).rev() {
        let cur = r * BASE + a[i] as u64;
        q[i] = (cur / d as u64) as u32;
        r = cur % d as u64;
    }
    trim(&mut q);
    (q, r as u32)
}

/// `a / b` (truncated), `b` non-zero: limb-wise long division with a binary search per limb.
fn div_mag(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut q = vec![0u32; a.len()];
    let mut rem: Vec<u32> = Vec::new();
    for i in (0..a.len()).rev() {
        rem.insert(0, a[i]);
        trim(&mut rem);
        let (mut lo, mut hi) = (0u64, BASE - 1);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if cmp_mag(&mul_small(b, mid as u32), &rem) != Ordering::Greater {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        q[i] = lo as u32;
        if lo > 0 {
            rem = sub_mag(&rem, &mul_small(b, lo as u32));
        }
    }
    trim(&mut q);
    q
}

fn pow10(n: usize) -> Vec<u32> {
    let mut v = vec![1u32];
    for _ in 0..n / 9 {
        v.insert(0, 0);
    }
    let mut r = v;
    for _ in 0..n % 9 {
        r = mul_small(&r, 10);
    }
    r
}

fn digits(mag: &[u32]) -> String {
    if mag.is_empty() {
        return "0".into();
    }
    let mut d = String::new();
    for (i, l) in mag.iter().rev().enumerate() {
        if i == 0 {
            d.push_str(&l.to_string());
        } else {
            d.push_str(&format!("{l:09}"));
        }
    }
    d
}

impl Dec {
    pub fn zero() -> Dec {
        Dec::default()
    }

    pub fn is_zero(&self) -> bool {
        self.mag.is_empty()
    }

    pub fn is_negative(&self) -> bool {
        self.neg
    }

    pub fn int(n: u64) -> Dec {
        Dec::parse(&n.to_string()).unwrap_or_default()
    }

    /// `-?digits(.digits)?`, at most 256 bytes; digits past [`SCALE`] places are dropped.
    pub fn parse(s: &str) -> Option<Dec> {
        if s.is_empty() || s.len() > 256 {
            return None;
        }
        let (neg, body) = match s.strip_prefix('-') {
            Some(b) => (true, b),
            None => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if int.is_empty()
            || !int.bytes().all(|c| c.is_ascii_digit())
            || !frac.bytes().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        let frac: String = frac.chars().take(SCALE).collect();
        let all = format!("{int}{frac}{}", "0".repeat(SCALE - frac.len()));
        let mut mag = Vec::new();
        let bytes = all.as_bytes();
        let mut end = bytes.len();
        while end > 0 {
            let start = end.saturating_sub(9);
            let chunk = std::str::from_utf8(&bytes[start..end]).ok()?;
            mag.push(chunk.parse::<u32>().ok()?);
            end = start;
        }
        trim(&mut mag);
        let neg = neg && !mag.is_empty();
        Some(Dec { neg, mag })
    }

    pub fn from_amount(a: &Amount) -> Dec {
        Dec::parse(&a.to_text()).unwrap_or_default()
    }

    pub fn add(&self, o: &Dec) -> Dec {
        self.signed_add(o, false)
    }

    pub fn sub(&self, o: &Dec) -> Dec {
        self.signed_add(o, true)
    }

    fn signed_add(&self, o: &Dec, sub: bool) -> Dec {
        let oneg = o.neg != sub && !o.mag.is_empty();
        let (neg, mag) = if self.neg == oneg {
            (self.neg, add_mag(&self.mag, &o.mag))
        } else if cmp_mag(&self.mag, &o.mag) != Ordering::Less {
            (self.neg, sub_mag(&self.mag, &o.mag))
        } else {
            (oneg, sub_mag(&o.mag, &self.mag))
        };
        Dec {
            neg: neg && !mag.is_empty(),
            mag,
        }
    }

    pub fn mul(&self, o: &Dec) -> Dec {
        let p = mul_mag(&self.mag, &o.mag);
        let mag = div_mag(&p, &pow10(SCALE));
        Dec {
            neg: (self.neg != o.neg) && !mag.is_empty(),
            mag,
        }
    }

    pub fn mul_int(&self, n: u32) -> Dec {
        let mag = mul_small(&self.mag, n);
        Dec {
            neg: self.neg && !mag.is_empty(),
            mag,
        }
    }

    pub fn div_int(&self, n: u32) -> Option<Dec> {
        if n == 0 {
            return None;
        }
        let (mag, _) = divmod_small(&self.mag, n);
        Some(Dec {
            neg: self.neg && !mag.is_empty(),
            mag,
        })
    }

    /// `self / o`, `None` when `o` is zero.
    pub fn div(&self, o: &Dec) -> Option<Dec> {
        if o.is_zero() {
            return None;
        }
        let num = mul_mag(&self.mag, &pow10(SCALE));
        let mag = div_mag(&num, &o.mag);
        Some(Dec {
            neg: (self.neg != o.neg) && !mag.is_empty(),
            mag,
        })
    }

    /// Rounded half away from zero to `places` (≤ [`SCALE`]) fractional digits.
    pub fn round(&self, places: usize) -> Dec {
        let places = places.min(SCALE);
        let unit = pow10(SCALE - places);
        let (q, r) = if SCALE == places {
            (self.mag.clone(), Vec::new())
        } else {
            let q = div_mag(&self.mag, &unit);
            let r = sub_mag(&self.mag, &mul_mag(&q, &unit));
            (q, r)
        };
        let up = cmp_mag(&mul_small(&r, 2), &unit) != Ordering::Less && !r.is_empty();
        let q = if up { add_mag(&q, &[1]) } else { q };
        let mag = mul_mag(&q, &unit);
        Dec {
            neg: self.neg && !mag.is_empty(),
            mag,
        }
    }

    /// Plain text with exactly `places` fractional digits (rounded).
    pub fn fixed(&self, places: usize) -> String {
        let places = places.min(SCALE);
        let r = self.round(places);
        let mut d = digits(&r.mag);
        if d.len() <= SCALE {
            d = format!("{}{d}", "0".repeat(SCALE + 1 - d.len()));
        }
        let (int, frac) = d.split_at(d.len() - SCALE);
        let sign = if r.neg { "-" } else { "" };
        if places == 0 {
            format!("{sign}{int}")
        } else {
            format!("{sign}{int}.{}", &frac[..places])
        }
    }

    /// Plain text, no trailing fraction zeros (`0` for zero).
    pub fn text(&self) -> String {
        let t = self.fixed(SCALE);
        let t = if t.contains('.') {
            t.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            t
        };
        if t == "-0" {
            "0".into()
        } else {
            t
        }
    }

    /// The nearest double (for `Float` fields).
    pub fn to_f64(&self) -> f64 {
        self.text().parse().unwrap_or(0.0)
    }
}

impl PartialOrd for Dec {
    fn partial_cmp(&self, o: &Dec) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for Dec {
    fn cmp(&self, o: &Dec) -> Ordering {
        match (self.neg, o.neg) {
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
            (false, false) => cmp_mag(&self.mag, &o.mag),
            (true, true) => cmp_mag(&o.mag, &self.mag),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn exact_ops() {
        assert_eq!(d("0.1").add(&d("0.2")).text(), "0.3");
        assert_eq!(d("1").sub(&d("3.5")).text(), "-2.5");
        assert_eq!(d("1.5").mul(&d("-2")).text(), "-3");
        assert_eq!(
            d("0.000038262709")
                .div(&d("0.034398447103"))
                .unwrap()
                .fixed(19),
            "0.0011123382658940724"
        );
        assert!(d("1").div(&Dec::zero()).is_none(), "no panic on zero");
        assert!(d("1").div_int(0).is_none());
        assert_eq!(d("10").div_int(4).unwrap().text(), "2.5");
        assert_eq!(d("0.000000005").round(8).text(), "0.00000001");
        assert_eq!(d("-0.000000005").fixed(8), "-0.00000001");
        assert_eq!(d("0.0000000049").fixed(8), "0.00000000");
        assert_eq!(d("12.3").fixed(10), "12.3000000000");
        assert_eq!(Dec::zero().fixed(2), "0.00");
        assert_eq!(d("-0").text(), "0");
        assert_eq!(d("3").cmp(&d("-4")), Ordering::Greater);
        assert_eq!(d("-3").cmp(&d("-4")), Ordering::Greater);
        // Huge operands (reserves of 10^60 × rates) stay exact.
        let big = d(&format!("{}.5", "9".repeat(60)));
        assert_eq!(big.mul(&d("2")).text(), format!("1{}", "9".repeat(60)));
        assert!(Dec::parse("1e5").is_none() && Dec::parse("").is_none());
        assert_eq!(d("1.123456789012345678901234567891234").text().len(), 32);
    }
}
