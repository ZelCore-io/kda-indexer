//! Custom scalars of the Kadena schema. Output formats follow Kadena graph's scalar
//! resolvers (graphql-scalars `BigInt`, `DateTime`, `NonNegativeFloat` for `Decimal`) until
//! captured responses say otherwise (GQ-8):
//! - `BigInt`: a JSON number while it is a safe integer (|n| < 2^53), else a string;
//! - `DateTime`: `YYYY-MM-DDTHH:MM:SS.mmmZ` (JavaScript `toISOString`);
//! - `Decimal`: a JSON number carrying the exact decimal text (no float rounding).

use std::str::FromStr;

use async_graphql::{InputValueError, InputValueResult, Number, Scalar, ScalarType, Value};

/// Largest integer a JavaScript number holds exactly.
const SAFE_INT: i128 = (1 << 53) - 1;

/// A whole number (heights, chain ids, gas).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BigInt(pub i128);

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        let text = match &value {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => return Err(InputValueError::expected_type(value)),
        };
        text.parse::<i128>()
            .map(BigInt)
            .map_err(|_| InputValueError::custom("not a whole number"))
    }

    fn to_value(&self) -> Value {
        if self.0.abs() <= SAFE_INT {
            Value::Number(Number::from(self.0 as i64))
        } else {
            Value::String(self.0.to_string())
        }
    }
}

impl From<u64> for BigInt {
    fn from(v: u64) -> Self {
        BigInt(v as i128)
    }
}

impl From<i64> for BigInt {
    fn from(v: i64) -> Self {
        BigInt(v as i128)
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

/// An exact decimal (amounts, balances), kept as its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal(String);

impl Decimal {
    /// A decimal from its text; `None` unless it is a plain JSON number.
    pub fn new(text: &str) -> Option<Decimal> {
        Number::from_str(text)
            .ok()
            .map(|_| Decimal(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[Scalar(name = "Decimal")]
impl ScalarType for Decimal {
    fn parse(value: Value) -> InputValueResult<Self> {
        match &value {
            Value::Number(n) => Ok(Decimal(n.to_string())),
            Value::String(s) => {
                Decimal::new(s).ok_or_else(|| InputValueError::custom("not a decimal"))
            }
            _ => Err(InputValueError::expected_type(value)),
        }
    }

    fn to_value(&self) -> Value {
        Number::from_str(&self.0)
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(self.0.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_is_a_number_while_safe() {
        assert_eq!(BigInt(7).to_value(), Value::Number(7.into()));
        assert_eq!(
            BigInt(1 << 60).to_value(),
            Value::String((1i128 << 60).to_string())
        );
        assert_eq!(
            BigInt::parse(Value::String("12".into())).unwrap(),
            BigInt(12)
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
}
