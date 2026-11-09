//! Best-effort parsing of untrusted JSON (tx objects, `cmd`, outputs, minerData).
//!
//! The workspace enables serde_json `arbitrary_precision`, which keeps number text exact but
//! gives one object key a special meaning: `{"$serde_json::private::Number": "1.5"}` parses as
//! the number `1.5` (and fails the whole parse for a non-number string). Anyone can put such an
//! object inside a signed `cmd` or make a contract echo it into an output, so untrusted bytes go
//! through [`parse_untrusted`], which refuses any input holding a string equal to a
//! `$serde_json::private::*` token (after unescaping) instead of letting serde_json rewrite it.
//!
//! Inputs serde_json cannot represent for other reasons (nesting deeper than its limit of 128,
//! lone surrogate escapes, ...) also come back as [`Unrepresentable`]. Callers keep the original
//! bytes as the authoritative copy and carry on; nothing here fails a block.

use serde_json::Value;

/// Prefix of serde_json's private magic tokens (`...::Number`, `...::RawValue`).
pub const SERDE_PRIVATE_PREFIX: &str = "$serde_json::private::";

/// Why a JSON input has no faithful `serde_json::Value` form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unrepresentable {
    /// A string equal to a serde_json private token (would be rewritten or rejected).
    MagicToken,
    /// serde_json rejected the input (depth limit, lone surrogate, invalid JSON, ...).
    Parse(String),
}

impl std::fmt::Display for Unrepresentable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unrepresentable::MagicToken => write!(f, "contains a serde_json private token"),
            Unrepresentable::Parse(e) => write!(f, "{e}"),
        }
    }
}

/// Parse `bytes` into a `Value` only when the result is a faithful copy of the input's
/// structure and number text.
pub fn parse_untrusted(bytes: &[u8]) -> Result<Value, Unrepresentable> {
    if has_magic_token(bytes) {
        return Err(Unrepresentable::MagicToken);
    }
    serde_json::from_slice(bytes).map_err(|e| Unrepresentable::Parse(e.to_string()))
}

/// True when any JSON string token in `b` (key or value) unescapes to something starting with
/// [`SERDE_PRIVATE_PREFIX`]. Conservative: a plain string value with that text also counts.
pub fn has_magic_token(b: &[u8]) -> bool {
    let needle = SERDE_PRIVATE_PREFIX.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'"' {
            i += 1;
            continue;
        }
        let start = i;
        let mut escaped = false;
        i += 1;
        while i < b.len() && b[i] != b'"' {
            if b[i] == b'\\' {
                escaped = true;
                i += 1;
            }
            i += 1;
        }
        let end = i.min(b.len());
        let inner = &b[start + 1..end];
        if escaped {
            // Unescape just this token; an invalid token is left for the real parse to reject.
            if let Ok(s) = serde_json::from_slice::<String>(&b[start..(end + 1).min(b.len())]) {
                if s.as_bytes().starts_with(needle) {
                    return true;
                }
            }
        } else if inner.starts_with(needle) {
            return true;
        }
        i += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_number_key_is_unrepresentable() {
        for src in [
            r#"{"$serde_json::private::Number":"1.5"}"#,
            r#"{"$serde_json::private::Number":"x"}"#,
            r#"{"a":[1,{"$serde_json::private::Number":"2"}]}"#,
            r#"{"\u0024serde_json::private::Number":"1.5"}"#,
            r#"{"$serde_json::private\u003a:Number":"1.5"}"#,
            r#"{"$serde_json::private::RawValue":"1"}"#,
        ] {
            assert_eq!(
                parse_untrusted(src.as_bytes()),
                Err(Unrepresentable::MagicToken),
                "{src}"
            );
        }
        // Inside a string (e.g. a signed cmd inside the tx object) it is just text.
        let tx = r#"{"cmd":"{\"$serde_json::private::Number\":\"1.5\"}"}"#;
        assert!(parse_untrusted(tx.as_bytes()).is_ok());
    }

    #[test]
    fn deep_and_surrogate_inputs_are_unrepresentable() {
        let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        assert!(matches!(
            parse_untrusted(deep.as_bytes()),
            Err(Unrepresentable::Parse(_))
        ));
        assert!(matches!(
            parse_untrusted(br#"{"a":"\ud800"}"#),
            Err(Unrepresentable::Parse(_))
        ));
        assert!(matches!(
            parse_untrusted(b"{\"a\":"),
            Err(Unrepresentable::Parse(_))
        ));
    }

    #[test]
    fn ordinary_json_parses() {
        let src = r#"{"b":1.0,"a":"$not_magic","c":"\"quoted\"","d":[-1.5e-3]}"#;
        let v = parse_untrusted(src.as_bytes()).unwrap();
        assert_eq!(serde_json::to_string(&v).unwrap(), src);
        // Not every number text survives: serde_json writes `1e3` back as `1e+3`. The byte
        // check at build time catches this and keeps the raw leaf bytes.
        let v = parse_untrusted(b"[1e3]").unwrap();
        assert_eq!(serde_json::to_string(&v).unwrap(), "[1e+3]");
    }
}
