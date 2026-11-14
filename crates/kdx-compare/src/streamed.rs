//! `/v1/txs` answers over the body cap (R-P5P9-52 concern 5; R-P5P9-66 harness T-items; the
//! 250-row page over 8 MB of run 6E, the I-6 residual).
//!
//! Such an answer is never cut and passed, and never set aside as an error. Each side is read
//! as a stream of rows. Every row is normalized as the in-memory compare normalizes it
//! ([`crate::normalize::normalize`] for `/v1/txs`, and numbers at f64 precision against
//! kda-txs) and hashed (SHA-256 of its canonical text). A `/v1/txs` answer is a set keyed by
//! request key, so the sides compare by their sorted row hashes; only those (40 bytes a row)
//! are kept, never the body.
//!
//! Any difference is a diff with [`HASH_PATH`], which no allowlist entry matches. None of the
//! in-memory compare's row rules (oracle lag, limit edge, node checks, event order) apply to
//! a streamed compare, so they cannot explain it. A bounded second read then looks for the
//! first differing row on each side ([`Finder`]) and the record names it under [`ROW_PATH`].

use std::collections::HashMap;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::normalize::{normalize, RouteKind};
use crate::target::HttpCapture;

/// The entry of a streamed compare that differs (unexplained: no allowlist entry matches it).
pub const HASH_PATH: &str = "/~v1txs-streamed-hash";
/// The entry naming the first differing row: `ROW_PATH/<request key>`.
pub const ROW_PATH: &str = "/~v1txs-streamed-row";
/// Bytes read per side at most; past it the side is a fault (a diff).
pub const STREAM_CAP: usize = 256 << 20;
/// Bytes of one row at most; a longer row is a fault (a diff).
pub const ROW_CAP: usize = 8 << 20;
/// The second read keeps a row of at most this many bytes.
pub const SHOWN_ROW_CAP: usize = 1 << 20;

/// Splits a JSON array, fed in chunks, into the texts of its elements.
pub struct Splitter {
    /// 0 before the `[`, 1 inside the array, 2 after its `]`.
    state: u8,
    depth: u32,
    in_str: bool,
    esc: bool,
    after_comma: bool,
    cur: Vec<u8>,
    row_cap: usize,
}

impl Splitter {
    pub fn new(row_cap: usize) -> Splitter {
        Splitter {
            state: 0,
            depth: 0,
            in_str: false,
            esc: false,
            after_comma: false,
            cur: Vec::new(),
            row_cap,
        }
    }

    fn emit(&mut self, row: &mut dyn FnMut(&[u8]) -> Result<bool, String>) -> Result<bool, String> {
        let t = self.cur.trim_ascii();
        if t.is_empty() {
            return Err("an empty element in the JSON array".into());
        }
        let stop = row(t)?;
        self.cur.clear();
        Ok(stop)
    }

    /// Feeds `chunk`; `row` gets each complete element and returns `true` to stop. Returns
    /// whether to stop.
    pub fn feed(
        &mut self,
        chunk: &[u8],
        row: &mut dyn FnMut(&[u8]) -> Result<bool, String>,
    ) -> Result<bool, String> {
        for &c in chunk {
            match self.state {
                0 if c == b'[' => self.state = 1,
                0 if c.is_ascii_whitespace() => {}
                0 => return Err("the answer is not a JSON array".into()),
                2 if c.is_ascii_whitespace() => {}
                2 => return Err("bytes after the JSON array".into()),
                _ => {
                    if self.in_str {
                        self.cur.push(c);
                        if self.esc {
                            self.esc = false;
                        } else if c == b'\\' {
                            self.esc = true;
                        } else if c == b'"' {
                            self.in_str = false;
                        }
                    } else {
                        match c {
                            b',' if self.depth == 0 => {
                                self.after_comma = true;
                                if self.emit(row)? {
                                    return Ok(true);
                                }
                                continue;
                            }
                            b']' if self.depth == 0 => {
                                self.state = 2;
                                if (!self.cur.trim_ascii().is_empty() || self.after_comma)
                                    && self.emit(row)?
                                {
                                    return Ok(true);
                                }
                                continue;
                            }
                            b'"' => self.in_str = true,
                            b'{' | b'[' => self.depth += 1,
                            b'}' | b']' => self.depth = self.depth.saturating_sub(1),
                            _ => {}
                        }
                        self.cur.push(c);
                    }
                    if self.cur.len() > self.row_cap {
                        return Err(format!("a row over {} bytes", self.row_cap));
                    }
                }
            }
        }
        Ok(false)
    }

    /// Whether the array was closed.
    pub fn finish(&self) -> Result<(), String> {
        if self.state == 2 {
            Ok(())
        } else {
            Err("the answer ended inside its JSON array".into())
        }
    }
}

/// One row as the in-memory compare normalizes it: its request key and the row.
pub fn norm_row(text: &[u8], f64: bool) -> Result<(String, Value), String> {
    let v: Value = serde_json::from_slice(text).map_err(|e| format!("a row is not JSON: {e}"))?;
    let mut arr = Value::Array(vec![v]);
    normalize(&RouteKind::V1Txs, &mut arr);
    if f64 {
        crate::run::numbers_as_f64(&mut arr);
    }
    let row = match arr {
        Value::Array(mut a) => a.pop().unwrap_or(Value::Null),
        v => v,
    };
    let key = row
        .get("requestKey")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok((key, row))
}

/// A value's canonical text: object keys sorted at every level (the normalizer's key order
/// does not survive its removal of `_id`), numbers as their text.
pub fn canon_text(v: &Value) -> String {
    fn go(v: &Value, out: &mut String) {
        match v {
            Value::Object(m) => {
                let mut ks: Vec<&String> = m.keys().collect();
                ks.sort();
                out.push('{');
                for (i, k) in ks.into_iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(k.clone()).to_string());
                    out.push(':');
                    go(&m[k], out);
                }
                out.push('}');
            }
            Value::Array(a) => {
                out.push('[');
                for (i, x) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    go(x, out);
                }
                out.push(']');
            }
            v => out.push_str(&v.to_string()),
        }
    }
    let mut out = String::new();
    go(v, &mut out);
    out
}

fn sha(b: &[u8]) -> [u8; 32] {
    let d = Sha256::digest(b);
    let mut o = [0u8; 32];
    o.copy_from_slice(&d[..]);
    o
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// A side of a streamed compare: its status, headers, size and row hashes, or what kept it
/// from being hashed.
#[derive(Debug, Clone)]
pub struct Side {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub bytes: usize,
    pub ms: u64,
    pub rows: Vec<(String, [u8; 32])>,
    pub fault: Option<String>,
}

impl Side {
    fn sorted(&self) -> Vec<(String, [u8; 32])> {
        let mut r = self.rows.clone();
        r.sort();
        r
    }

    /// SHA-256 over the sorted (request key, row hash) pairs.
    pub fn total(&self) -> String {
        let mut h = Sha256::new();
        for (k, d) in self.sorted() {
            h.update(k.as_bytes());
            h.update([0u8]);
            h.update(d);
        }
        hex(&h.finalize()[..])
    }

    pub fn summary(&self) -> Value {
        json!({"status": self.status, "bytes": self.bytes, "rows": self.rows.len(),
            "sha256": self.total(), "fault": self.fault})
    }
}

/// Hashes the rows of an answer fed in chunks.
pub struct Hasher {
    split: Splitter,
    f64: bool,
    rows: Vec<(String, [u8; 32])>,
    fault: Option<String>,
    bytes: usize,
    status: u16,
}

impl Hasher {
    /// A hasher of an answer of `status`: anything but 200 is a fault (no rows).
    pub fn new(status: u16, f64: bool) -> Hasher {
        Hasher {
            split: Splitter::new(ROW_CAP),
            f64,
            rows: Vec::new(),
            fault: (status != 200).then(|| format!("HTTP {status}")),
            bytes: 0,
            status,
        }
    }

    /// Feeds a chunk; `true` once the hasher stops reading (a fault).
    pub fn feed(&mut self, chunk: &[u8]) -> bool {
        self.bytes += chunk.len();
        if self.fault.is_some() {
            return true;
        }
        if self.bytes > STREAM_CAP {
            self.fault = Some(format!("the answer is over {STREAM_CAP} bytes"));
            return true;
        }
        let Hasher {
            split, f64, rows, ..
        } = self;
        let f64 = *f64;
        let r = split.feed(chunk, &mut |t| {
            let (k, row) = norm_row(t, f64)?;
            rows.push((k, sha(canon_text(&row).as_bytes())));
            Ok(false)
        });
        if let Err(e) = r {
            self.fault = Some(e);
            return true;
        }
        false
    }

    pub fn finish(mut self, headers: Vec<(String, String)>, ms: u64) -> Side {
        if self.fault.is_none() {
            if let Err(e) = self.split.finish() {
                self.fault = Some(e);
            }
        }
        Side {
            status: self.status,
            headers,
            bytes: self.bytes,
            ms,
            rows: self.rows,
            fault: self.fault,
        }
    }
}

/// The hashed side of an answer already in memory.
pub fn hash_capture(c: &HttpCapture, f64: bool) -> Side {
    let mut h = Hasher::new(c.status, f64);
    h.feed(&c.body);
    h.finish(c.headers.clone(), c.ms)
}

/// Both sides answered 200 and hold the same rows.
pub fn equal(a: &Side, b: &Side) -> bool {
    a.fault.is_none()
        && b.fault.is_none()
        && a.status == 200
        && b.status == 200
        && a.sorted() == b.sorted()
}

/// The request key of the first row that differs: in A's order the first row whose (key,
/// hash) B lacks, else in B's order the first row A lacks.
pub fn first_differing(a: &Side, b: &Side) -> Option<String> {
    fn first(x: &Side, y: &Side) -> Option<String> {
        let mut left: HashMap<&(String, [u8; 32]), usize> = HashMap::new();
        for r in &y.rows {
            *left.entry(r).or_default() += 1;
        }
        x.rows.iter().find_map(|r| match left.get_mut(r) {
            Some(n) if *n > 0 => {
                *n -= 1;
                None
            }
            _ => Some(r.0.clone()),
        })
    }
    first(a, b).or_else(|| first(b, a))
}

/// The bounded second read: finds the first row with request key `key`, of at most
/// [`SHOWN_ROW_CAP`] bytes, and stops there.
pub struct Finder {
    split: Splitter,
    key: String,
    f64: bool,
    pub found: Option<Value>,
}

impl Finder {
    pub fn new(key: &str, f64: bool) -> Finder {
        Finder {
            split: Splitter::new(ROW_CAP),
            key: key.to_string(),
            f64,
            found: None,
        }
    }

    /// Feeds a chunk; `true` once the row is found or the answer cannot be read.
    pub fn feed(&mut self, chunk: &[u8]) -> bool {
        let Finder {
            split,
            key,
            f64,
            found,
        } = self;
        let needle = key.as_bytes();
        let r = split.feed(chunk, &mut |t| {
            if t.len() > SHOWN_ROW_CAP || !t.windows(needle.len().max(1)).any(|w| w == needle) {
                return Ok(false);
            }
            let (k, row) = norm_row(t, *f64)?;
            if k == *key {
                *found = Some(row);
                return Ok(true);
            }
            Ok(false)
        });
        r.unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split_all(chunks: &[&[u8]]) -> Result<Vec<String>, String> {
        let mut s = Splitter::new(1 << 10);
        let mut out = Vec::new();
        for c in chunks {
            s.feed(c, &mut |t| {
                out.push(String::from_utf8_lossy(t).into_owned());
                Ok(false)
            })?;
        }
        s.finish()?;
        Ok(out)
    }

    #[test]
    fn the_splitter_finds_elements_across_chunks_strings_and_nesting() {
        let got = split_all(&[b" [ {\"a\": \"x,]\\\"\"}", b", [1,[2]] ,", b"3 ]\n"]).unwrap();
        assert_eq!(got, vec!["{\"a\": \"x,]\\\"\"}", "[1,[2]]", "3"]);
        assert_eq!(split_all(&[b"[]"]).unwrap(), Vec::<String>::new());
        assert!(split_all(&[b"{}"]).is_err(), "not an array");
        assert!(split_all(&[b"[1,]"]).is_err(), "a trailing comma");
        assert!(split_all(&[b"[1,,2]"]).is_err(), "an empty element");
        assert!(split_all(&[b"[1,2"]).is_err(), "cut inside the array");
        assert!(split_all(&[b"[1] x"]).is_err(), "bytes after the array");
        assert!(
            split_all(&[b"[\"", &[b'y'; 2000], b"\"]"]).is_err(),
            "a row over the cap"
        );
    }

    #[test]
    fn rows_hash_as_a_set_after_normalizing() {
        let cap = |s: &str| HttpCapture {
            status: 200,
            headers: Vec::new(),
            body: bytes::Bytes::from(s.to_string()),
            ms: 0,
        };
        let a = hash_capture(
            &cap(r#"[{"requestKey":"k1","x":1},{"requestKey":"k2","x":2}]"#),
            false,
        );
        let b = hash_capture(
            &cap(r#"[ {"x":2,"requestKey":"k2","_id":"c"}, {"requestKey":"k1","x":1} ]"#),
            false,
        );
        assert!(equal(&a, &b), "{a:?} {b:?}");
        assert_eq!(a.total(), b.total());
        let c = hash_capture(
            &cap(r#"[{"requestKey":"k1","x":1},{"requestKey":"k2","x":3}]"#),
            false,
        );
        assert!(!equal(&a, &c));
        assert_eq!(first_differing(&a, &c).as_deref(), Some("k2"));
        let d = hash_capture(&cap(r#"[{"requestKey":"k1","x":1}]"#), false);
        assert_eq!(first_differing(&d, &a).as_deref(), Some("k2"));
        // A duplicate row is not the same set.
        let e = hash_capture(
            &cap(r#"[{"requestKey":"k1","x":1},{"requestKey":"k1","x":1}]"#),
            false,
        );
        assert!(!equal(&d, &e));
        // Not 200, or not an array: never equal.
        let mut f = cap("[]");
        f.status = 404;
        assert!(!equal(&hash_capture(&f, false), &hash_capture(&f, false)));
        let g = hash_capture(&cap("{}"), false);
        assert!(!equal(&g, &g));
        let mut fi = Finder::new("k2", false);
        assert!(fi.feed(br#"[{"requestKey":"k1"},{"requestKey":"k2","x":3}, oops"#));
        assert_eq!(fi.found, Some(json!({"requestKey": "k2", "x": 3})));
    }
}
