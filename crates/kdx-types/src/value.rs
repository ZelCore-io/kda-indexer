//! Lossless JSON value codec (spec §2.2 "Lossless storage of JSON values").
//!
//! MessagePack written with `rmp`'s low-level writers:
//! - null, bool, array, map: native msgpack. Maps keep insertion order
//!   (`preserve_order`); keys are always native strings.
//! - numbers: **ext type 1** holding the exact number text kept by
//!   `arbitrary_precision` (`1.0` stays `1.0`, `2.36e-3` stays `2.36e-3`,
//!   big ints stay exact). Never native ints/floats.
//! - strings: if the string is an interned account (`Dict::find(Account, s)`),
//!   **ext type 2** whose payload is the LEB128 account id; otherwise a native str.
//!
//! `serde_json::to_string(&decode(encode(v)))` equals `serde_json::to_string(v)`
//! as long as `decode` uses a dictionary that contains every account id the
//! encoder referenced (ids are never deleted, so any later state of it works).

use serde_json::{Map, Number, Value};

use crate::dict::{Dict, DictKind};
use crate::varint;
use crate::{Error, Result};

pub const EXT_NUMBER: i8 = 1;
pub const EXT_ACCOUNT: i8 = 2;

pub fn encode(v: &Value, dict: &dyn Dict, out: &mut Vec<u8>) {
    use rmp::encode as e;
    const W: &str = "write to Vec cannot fail";
    match v {
        Value::Null => e::write_nil(out).expect(W),
        Value::Bool(b) => e::write_bool(out, *b).expect(W),
        Value::Number(n) => {
            let t = n.to_string();
            e::write_ext_meta(out, t.len() as u32, EXT_NUMBER).expect(W);
            out.extend_from_slice(t.as_bytes());
        }
        Value::String(s) => match dict.find(DictKind::Account, s) {
            Some(id) => {
                let mut p = Vec::with_capacity(5);
                varint::put_u64(&mut p, id as u64);
                e::write_ext_meta(out, p.len() as u32, EXT_ACCOUNT).expect(W);
                out.extend_from_slice(&p);
            }
            None => e::write_str(out, s).expect(W),
        },
        Value::Array(a) => {
            e::write_array_len(out, a.len() as u32).expect(W);
            for x in a {
                encode(x, dict, out);
            }
        }
        Value::Object(m) => {
            e::write_map_len(out, m.len() as u32).expect(W);
            for (k, x) in m {
                e::write_str(out, k).expect(W);
                encode(x, dict, out);
            }
        }
    }
}

/// Convenience: encode into a fresh buffer.
pub fn to_vec(v: &Value, dict: &dyn Dict) -> Vec<u8> {
    let mut out = Vec::new();
    encode(v, dict, &mut out);
    out
}

/// Convenience: decode a buffer that must contain exactly one value.
pub fn from_slice(mut b: &[u8], dict: &dyn Dict) -> Result<Value> {
    let v = decode(&mut b, dict)?;
    if !b.is_empty() {
        return Err(Error::Codec(format!(
            "{} trailing bytes after value",
            b.len()
        )));
    }
    Ok(v)
}

fn be_u16(buf: &mut &[u8]) -> Result<usize> {
    Ok(u16::from_be_bytes(varint::take_array(buf)?) as usize)
}
fn be_u32(buf: &mut &[u8]) -> Result<usize> {
    Ok(u32::from_be_bytes(varint::take_array(buf)?) as usize)
}

pub fn decode(buf: &mut &[u8], dict: &dyn Dict) -> Result<Value> {
    use rmp::Marker as M;
    let m = M::from_u8(varint::get_u8(buf)?);
    Ok(match m {
        M::Null => Value::Null,
        M::True => Value::Bool(true),
        M::False => Value::Bool(false),
        M::FixStr(n) => Value::String(str_of(buf, n as usize)?),
        M::Str8 => {
            let n = varint::get_u8(buf)? as usize;
            Value::String(str_of(buf, n)?)
        }
        M::Str16 => {
            let n = be_u16(buf)?;
            Value::String(str_of(buf, n)?)
        }
        M::Str32 => {
            let n = be_u32(buf)?;
            Value::String(str_of(buf, n)?)
        }
        M::FixArray(n) => array(buf, n as usize, dict)?,
        M::Array16 => {
            let n = be_u16(buf)?;
            array(buf, n, dict)?
        }
        M::Array32 => {
            let n = be_u32(buf)?;
            array(buf, n, dict)?
        }
        M::FixMap(n) => map(buf, n as usize, dict)?,
        M::Map16 => {
            let n = be_u16(buf)?;
            map(buf, n, dict)?
        }
        M::Map32 => {
            let n = be_u32(buf)?;
            map(buf, n, dict)?
        }
        M::FixExt1 => ext(buf, 1, dict)?,
        M::FixExt2 => ext(buf, 2, dict)?,
        M::FixExt4 => ext(buf, 4, dict)?,
        M::FixExt8 => ext(buf, 8, dict)?,
        M::FixExt16 => ext(buf, 16, dict)?,
        M::Ext8 => {
            let n = varint::get_u8(buf)? as usize;
            ext(buf, n, dict)?
        }
        M::Ext16 => {
            let n = be_u16(buf)?;
            ext(buf, n, dict)?
        }
        M::Ext32 => {
            let n = be_u32(buf)?;
            ext(buf, n, dict)?
        }
        other => return Err(Error::Codec(format!("unexpected msgpack marker {other:?}"))),
    })
}

fn str_of(buf: &mut &[u8], n: usize) -> Result<String> {
    let b = varint::take(buf, n)?;
    String::from_utf8(b.to_vec()).map_err(|_| Error::Codec("invalid utf-8 in str".into()))
}

fn array(buf: &mut &[u8], n: usize, dict: &dyn Dict) -> Result<Value> {
    let mut a = Vec::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        a.push(decode(buf, dict)?);
    }
    Ok(Value::Array(a))
}

fn map(buf: &mut &[u8], n: usize, dict: &dyn Dict) -> Result<Value> {
    let mut m = Map::with_capacity(n.min(buf.len()));
    for _ in 0..n {
        let k = match decode(buf, dict)? {
            Value::String(s) => s,
            other => return Err(Error::Codec(format!("non-string map key {other}"))),
        };
        let v = decode(buf, dict)?;
        if m.insert(k, v).is_some() {
            return Err(Error::Codec("duplicate map key".into()));
        }
    }
    Ok(Value::Object(m))
}

fn ext(buf: &mut &[u8], n: usize, dict: &dyn Dict) -> Result<Value> {
    let ty = varint::get_u8(buf)? as i8;
    let mut data = varint::take(buf, n)?;
    match ty {
        EXT_NUMBER => {
            let t = std::str::from_utf8(data)
                .map_err(|_| Error::Codec("invalid utf-8 in number".into()))?;
            let num: Number = serde_json::from_str(t)
                .map_err(|e| Error::Codec(format!("bad number text {t:?}: {e}")))?;
            if num.to_string() != t {
                return Err(Error::Codec(format!(
                    "number text {t:?} does not round-trip"
                )));
            }
            Ok(Value::Number(num))
        }
        EXT_ACCOUNT => {
            let id = varint::get_u32(&mut data)?;
            if !data.is_empty() {
                return Err(Error::Codec("trailing bytes in account ref".into()));
            }
            dict.name(DictKind::Account, id)
                .map(|s| Value::String(s.to_owned()))
                .ok_or(Error::UnknownDictId {
                    kind: DictKind::Account,
                    id,
                })
        }
        t => Err(Error::Codec(format!("unknown ext type {t}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::{DictMut, MemDict};

    #[test]
    fn value_roundtrip_smoke() {
        let src = r#"{"b":1.0,"a":1,"d":{"decimal":"0.000000000001"},"i":{"int":"123456789012345678901234567890"},"big":123456789012345678901234567890,"neg":-0.0,"acct":"k:abc","s":"é\n"}"#;
        let v: Value = serde_json::from_str(src).unwrap();
        let mut dict = MemDict::new();
        let id = dict.intern(DictKind::Account, "k:abc");
        let buf = to_vec(&v, &dict);
        // "acct" stored as ext 2: fixext1 marker 0xd4, type 2, varint id.
        let needle = [b'a', b'c', b'c', b't', 0xd4, EXT_ACCOUNT as u8, id as u8];
        assert!(
            buf.windows(needle.len()).any(|w| w == needle),
            "acct not stored as ext 2"
        );
        let back = from_slice(&buf, &dict).unwrap();
        assert_eq!(
            serde_json::to_string(&back).unwrap(),
            serde_json::to_string(&v).unwrap()
        );
        assert_eq!(serde_json::to_string(&back).unwrap(), src);
    }
}
