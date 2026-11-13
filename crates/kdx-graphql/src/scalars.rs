//! Custom scalars of the Kadena schema. Output formats follow Kadena graph's scalar
//! resolvers (graphql-scalars `BigInt`, `DateTime`, `NonNegativeFloat` for `Decimal`) until
//! captured responses say otherwise (GQ-8):
//! - `BigInt`: a JSON number while it is a safe integer (|n| < 2^53), else a string. The
//!   deployed oracle serializes some `BigInt` fields as strings (`gasLimit`, `ttl`, `gas`,
//!   `transactionId`) and `Block.difficulty` always as a number (R-P5P9-19); a value carries
//!   the wire form its field needs (`BigInt::text`, `BigInt::number`);
//! - `DateTime`: `YYYY-MM-DDTHH:MM:SS.mmmZ` (JavaScript `toISOString`);
//! - `Decimal`: a JSON number carrying the exact decimal text (no float rounding); the oracle
//!   sends `Transfer.amount` and `Block.nonce` as strings (`Decimal::into_text`).

use std::borrow::Cow;
use std::str::FromStr;

use async_graphql::parser::types::Field;
use async_graphql::registry::Registry;
use async_graphql::{
    ContextSelectionSet, InputValueError, InputValueResult, Number, OutputType, Positioned, Scalar,
    ScalarType, ServerResult, Value,
};

/// Largest integer a JavaScript number holds exactly.
const SAFE_INT: i128 = (1 << 53) - 1;

/// How a `BigInt` goes on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BigWire {
    /// Number while safe, else string (Kadena's `BigInt` scalar).
    Safe,
    /// Always a JSON string (the oracle's `gasLimit`, `ttl`, `gas`, `transactionId`).
    Text,
    /// Always a JSON number (the oracle's `Block.difficulty`).
    Number,
}

/// A whole number (heights, chain ids, gas).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BigInt {
    pub value: i128,
    wire: BigWire,
}

impl BigInt {
    /// A number while safe, else a string.
    pub fn new(value: i128) -> BigInt {
        BigInt {
            value,
            wire: BigWire::Safe,
        }
    }

    /// Always a JSON string.
    pub fn text(value: i128) -> BigInt {
        BigInt {
            value,
            wire: BigWire::Text,
        }
    }

    /// Always a JSON number (an f64 beyond u64 / i64 range).
    pub fn number(value: i128) -> BigInt {
        BigInt {
            value,
            wire: BigWire::Number,
        }
    }
}

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        let text = match &value {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => return Err(InputValueError::expected_type(value)),
        };
        text.parse::<i128>()
            .map(BigInt::new)
            .map_err(|_| InputValueError::custom("not a whole number"))
    }

    fn to_value(&self) -> Value {
        let v = self.value;
        match self.wire {
            BigWire::Text => Value::String(v.to_string()),
            BigWire::Number => i64::try_from(v)
                .map(|n| Value::Number(Number::from(n)))
                .or_else(|_| u64::try_from(v).map(|n| Value::Number(Number::from(n))))
                .ok()
                .or_else(|| Number::from_f64(v as f64).map(Value::Number))
                .unwrap_or_else(|| Value::String(v.to_string())),
            BigWire::Safe if v.abs() <= SAFE_INT => Value::Number(Number::from(v as i64)),
            BigWire::Safe => Value::String(v.to_string()),
        }
    }
}

impl From<u64> for BigInt {
    fn from(v: u64) -> Self {
        BigInt::new(v as i128)
    }
}

impl From<i64> for BigInt {
    fn from(v: i64) -> Self {
        BigInt::new(v as i128)
    }
}

/// A UTC time in microseconds since the POSIX epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTime(pub i64);

impl DateTime {
    /// `YYYY-MM-DDTHH:MM:SS.mmmZ` (milliseconds, truncated).
    pub fn text(&self) -> String {
        let secs = self.0.div_euclid(1_000_000);
        let ms = self.0.rem_euclid(1_000_000) / 1000;
        let mut s = kdx_api::compat::shapes::utc_secs(secs);
        s.pop(); // 'Z'
        format!("{s}.{ms:03}Z")
    }
}

#[Scalar(name = "DateTime")]
impl ScalarType for DateTime {
    fn parse(value: Value) -> InputValueResult<Self> {
        // No argument of the schema takes a DateTime.
        Err(InputValueError::expected_type(value))
    }

    fn to_value(&self) -> Value {
        Value::String(self.text())
    }
}

/// Largest exponent magnitude a decimal text may carry, and the longest plain expansion
/// (SR7-4): beyond it the text is refused on input (`Decimal::new`, so `BAD_USER_INPUT`) and
/// left as written on output (`plain`), never expanded.
const MAX_EXP: i64 = 1000;
const MAX_PLAIN: usize = 1100;

/// The exponent of a decimal text, `Some(0)` without one, `None` when it is not a number or
/// its magnitude passes [`MAX_EXP`] (a long digit string never reaches an integer parse).
fn exponent(t: &str) -> Option<i64> {
    let Some((_, exp)) = t.split_once(['e', 'E']) else {
        return Some(0);
    };
    let digits = exp.strip_prefix(['+', '-']).unwrap_or(exp);
    if digits.is_empty() || digits.len() > 6 || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let n: i64 = exp.parse().ok()?;
    (n.abs() <= MAX_EXP).then_some(n)
}

/// A decimal text without an exponent: `2.36e-6` is `0.00000236`. A text whose exponent is
/// out of range or whose expansion would pass [`MAX_PLAIN`] characters is returned as is.
fn plain(t: &str) -> String {
    let Some((mant, _)) = t.split_once(['e', 'E']) else {
        return t.to_owned();
    };
    let Some(exp) = exponent(t).filter(|_| mant.len() <= MAX_PLAIN) else {
        return t.to_owned();
    };
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant.strip_prefix('+').unwrap_or(mant)),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    let digits = format!("{int}{frac}");
    let Some(point) = (int.len() as i64).checked_add(exp) else {
        return t.to_owned();
    };
    let zeros = if point <= 0 {
        point.unsigned_abs()
    } else {
        (point as u64).saturating_sub(digits.len() as u64)
    };
    if zeros as usize > MAX_PLAIN || digits.len() > MAX_PLAIN {
        return t.to_owned();
    }
    let body = if point <= 0 {
        format!("0.{}{}", "0".repeat(zeros as usize), digits)
    } else if point as usize >= digits.len() {
        format!("{digits}{}", "0".repeat(zeros as usize))
    } else {
        format!(
            "{}.{}",
            &digits[..point as usize],
            &digits[point as usize..]
        )
    };
    let body = if body.contains('.') {
        body.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        body
    };
    let body = body.trim_start_matches('0');
    let body = match body {
        "" => "0".to_owned(),
        b if b.starts_with('.') => format!("0{b}"),
        b => b.to_owned(),
    };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// A plain decimal text without trailing fraction zeros (`1.50` is `1.5`, `2.0` is `2`).
/// Texts that still carry an exponent (out of `plain`'s range) are left alone.
fn trim_zeros(t: String) -> String {
    if !t.contains('.') || t.contains(['e', 'E']) {
        return t;
    }
    let s = t.trim_end_matches('0').trim_end_matches('.');
    match s {
        "-0" | "" => "0".to_owned(),
        s => s.to_owned(),
    }
}

/// A JSON number's text as JavaScript's `Number.prototype.toString` lays it out, over the
/// exact digits (no rounding to f64): plain notation for 1e-7 < |x| < 1e21 (`2.36e-6` is
/// `0.00000236`, `1.0e-5` is `0.00001`, `5.0` is `5`), exponent notation beyond
/// (`1e-7`, `1.5e+21`). The oracle renders event parameters this way (R-P5P9-57 F9).
/// A text that is not a plain JSON number is returned as is.
pub fn js_number(t: &str) -> String {
    let (neg, body) = match t.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, t),
    };
    let (mant, exp) = match body.split_once(['e', 'E']) {
        Some((m, e)) => {
            let d = e.strip_prefix(['+', '-']).unwrap_or(e);
            if d.is_empty() || d.len() > 9 || !d.bytes().all(|c| c.is_ascii_digit()) {
                return t.to_owned();
            }
            match e.parse::<i64>() {
                Ok(n) => (m, n),
                Err(_) => return t.to_owned(),
            }
        }
        None => (body, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if int.is_empty() || !int.bytes().chain(frac.bytes()).all(|c| c.is_ascii_digit()) {
        return t.to_owned();
    }
    // value = 0.<digits> x 10^n, digits without leading or trailing zeros
    let all = format!("{int}{frac}");
    let digits = all.trim_start_matches('0');
    if digits.is_empty() {
        return "0".to_owned();
    }
    let skipped = (all.len() - digits.len()) as i64;
    let n = int.len() as i64 + exp - skipped;
    let digits = digits.trim_end_matches('0');
    let k = digits.len() as i64;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let e = n - 1;
        let sign = if e < 0 { '-' } else { '+' };
        if k == 1 {
            format!("{digits}e{sign}{}", e.unsigned_abs())
        } else {
            format!(
                "{}.{}e{sign}{}",
                &digits[..1],
                &digits[1..],
                e.unsigned_abs()
            )
        }
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// An exact decimal (amounts, balances), kept as its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    text: String,
    as_string: bool,
}

impl Decimal {
    /// A decimal from its text; `None` unless it is a plain JSON number whose exponent (if
    /// any) is at most 1000 in magnitude (SR7-4). Serialized as a JSON number.
    pub fn new(text: &str) -> Option<Decimal> {
        exponent(text)?;
        Number::from_str(text).ok().map(|_| Decimal {
            text: text.to_owned(),
            as_string: false,
        })
    }

    /// The same decimal serialized as a JSON string (the oracle's `Transfer.amount`), in
    /// plain notation (`0.00000236`, never `2.36e-6`) and without trailing fraction zeros
    /// (`26.57382000` is `26.57382`, R-P5P9-57).
    pub fn into_text(mut self) -> Decimal {
        self.as_string = true;
        self.text = trim_zeros(plain(&self.text));
        self
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

#[Scalar(name = "Decimal")]
impl ScalarType for Decimal {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::Number(n) => {
                Decimal::new(&n.to_string()).ok_or_else(|| InputValueError::custom("not a decimal"))
            }
            Value::String(s) => {
                Decimal::new(s).ok_or_else(|| InputValueError::custom("not a decimal"))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        if self.as_string {
            return Value::String(self.text.clone());
        }
        Number::from_str(&self.text)
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(self.text.clone()))
    }
}

/// A `Float` field the oracle sends as a decimal string (`meta.gasPrice`, "0.00000001").
/// The schema type stays `Float` (the registry entry is `f64`'s); only the value is text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloatText(pub String);

impl OutputType for FloatText {
    fn type_name() -> Cow<'static, str> {
        <f64 as OutputType>::type_name()
    }

    fn create_type_info(registry: &mut Registry) -> String {
        <f64 as OutputType>::create_type_info(registry)
    }

    async fn resolve(
        &self,
        _ctx: &ContextSelectionSet<'_>,
        _field: &Positioned<Field>,
    ) -> ServerResult<Value> {
        Ok(Value::String(self.0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_is_a_number_while_safe() {
        assert_eq!(BigInt::new(7).to_value(), Value::Number(7.into()));
        assert_eq!(
            BigInt::new(1 << 60).to_value(),
            Value::String((1i128 << 60).to_string())
        );
        assert_eq!(
            BigInt::parse(Value::String("12".into())).unwrap(),
            BigInt::new(12)
        );
        assert!(BigInt::parse(Value::String("1.5".into())).is_err());
    }

    #[test]
    fn datetime_is_iso_with_millis() {
        assert_eq!(
            DateTime(1_572_471_343_123_456).text(),
            "2019-10-30T21:35:43.123Z"
        );
        assert_eq!(DateTime(0).text(), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn decimal_keeps_its_digits() {
        let d = Decimal::new("123456789.123456789012").unwrap();
        assert_eq!(
            serde_json::to_string(&d.to_value()).unwrap(),
            "123456789.123456789012"
        );
        assert!(Decimal::new("1e").is_none());
    }

    #[test]
    fn bigint_wire_forms() {
        // gasLimit / ttl / gas / transactionId: a string, whatever the size.
        assert_eq!(BigInt::text(600).to_value(), Value::String("600".into()));
        // Block.difficulty: a number, even above 2^53 (the oracle's 32876630560156508).
        assert_eq!(
            serde_json::to_string(&BigInt::number(32_876_630_560_156_508).to_value()).unwrap(),
            "32876630560156508"
        );
        assert_eq!(
            serde_json::to_string(&BigInt::number(u64::MAX as i128).to_value()).unwrap(),
            "18446744073709551615"
        );
        // Default stays Kadena's: number while safe, string beyond.
        assert_eq!(BigInt::new(5).to_value(), Value::Number(5.into()));
    }

    #[test]
    fn huge_exponents_are_bounded() {
        let long = format!("1e{}", "9".repeat(30));
        let long_neg = format!("1e-{}", "9".repeat(30));
        for t in [
            "1e2000000000",
            "1e-2000000000",
            "1e2147483647",
            "1e-2147483648",
            long.as_str(),
            long_neg.as_str(),
            "1e1001",
            "1e-1001",
        ] {
            // Input: refused (BAD_USER_INPUT through the scalar), never expanded.
            assert!(Decimal::new(t).is_none(), "{t}");
            assert!(Decimal::parse(Value::String(t.into())).is_err(), "{t}");
            // Output path: left as written.
            assert_eq!(plain(t), t, "{t}");
        }
        // The cap itself: 1000 is accepted on input and expands within the bound.
        assert!(Decimal::new("1e1000").is_some() && Decimal::new("1e-1000").is_some());
        let up = Decimal::new("1e1000").unwrap().into_text();
        assert_eq!(up.as_str().len(), 1001);
        assert!(up.as_str().starts_with("1000"));
        let down = plain("1e-1000");
        assert_eq!(down.len(), 1002);
        assert!(down.starts_with("0.000") && down.ends_with("01"));
        // Just beyond the cap is out of range on both signs.
        assert_eq!(plain("1e1001"), "1e1001");
        assert_eq!(plain("1e-1001"), "1e-1001");
        // A long mantissa with an exponent is left alone too.
        let wide = format!("{}e5", "1".repeat(MAX_PLAIN + 1));
        assert_eq!(plain(&wide), wide);
    }

    #[test]
    fn decimal_text_form() {
        let d = Decimal::new("0.00000236").unwrap().into_text();
        assert_eq!(d.to_value(), Value::String("0.00000236".into()));
        assert_eq!(d.as_str(), "0.00000236");
        // No exponent notation on the wire.
        for (i, o) in [
            ("2.36e-6", "0.00000236"),
            ("1e-8", "0.00000001"),
            ("1.5e3", "1500"),
            ("-2.5E-3", "-0.0025"),
            ("0.5", "0.5"),
            ("12", "12"),
            ("1.0", "1"),
            ("26.57382000000000000000000000", "26.57382"),
            ("377.763370000000000000000000000000", "377.76337"),
            ("6000", "6000"),
            ("100.500", "100.5"),
            ("0.0", "0"),
            ("-0.10", "-0.1"),
        ] {
            assert_eq!(Decimal::new(i).unwrap().into_text().as_str(), o, "{i}");
        }
    }

    #[test]
    fn js_number_layout() {
        // R-P5P9-57 F9: the oracle's event-parameter numbers (JavaScript layout).
        for (i, o) in [
            ("2.36e-6", "0.00000236"),
            ("1.0e-5", "0.00001"),
            ("6.0e-6", "0.000006"),
            ("9.05e-4", "0.000905"),
            ("1.0e-3", "0.001"),
            ("1.5e-3", "0.0015"),
            ("5.0", "5"),
            ("5", "5"),
            ("0", "0"),
            ("0.0", "0"),
            ("-0.0", "0"),
            ("0.9830265", "0.9830265"),
            ("26.57382000000000000000000000", "26.57382"),
            ("1500.0", "1500"),
            ("1.5e3", "1500"),
            ("-2.5E-3", "-0.0025"),
            ("1e-7", "1e-7"),
            ("1.0e-7", "1e-7"),
            ("1.25e-7", "1.25e-7"),
            ("0.000000125", "1.25e-7"),
            ("1e21", "1e+21"),
            ("1.5e21", "1.5e+21"),
            ("123456789012345678901", "123456789012345678901"),
            ("1234567890123456789012", "1.234567890123456789012e+21"),
            ("0.123456789012345678901", "0.123456789012345678901"),
            ("007.50", "7.5"),
            ("0.00100", "0.001"),
            ("-12", "-12"),
        ] {
            assert_eq!(js_number(i), o, "{i}");
        }
        // Not a plain JSON number: left alone, never expanded.
        for t in ["abc", "1e", "1e999999999999", "--1", ".5", "1.2.3"] {
            assert_eq!(js_number(t), t, "{t}");
        }
    }
}
