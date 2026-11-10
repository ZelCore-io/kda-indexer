//! chainweb-data response shapes (`TxDetail`, `EventDetail`, `TransferDetail` of
//! chainweb-api) and the exact text forms its Haskell encoders produce: objects with keys in
//! code-point order at every depth, compact output, numbers in the `Scientific` forms aeson
//! and `show` print, timestamps as aeson prints `UTCTime`.

use kdx_decode::json::parse_untrusted;
use kdx_decode::render::{render_cmd, render_output, render_tx};
use kdx_types::record::{BlockRecord, TxRecord};
use kdx_types::{Hash32, TxKey};
use serde_json::{Map, Value};

use crate::store::View;
use crate::{ApiError, ApiResult};

/// Request key text of coinbase rows in API output (`getTxHash` of chainweb-data).
pub const COINBASE_RK_TEXT: &str = "<coinbase>";
/// Request key of coinbase rows in chainweb-data's ordering (`show RKCB_Coinbase`).
pub const COINBASE_RK_ORDER: &[u8] = b"cb";

// ---------------------------------------------------------------------------------------
// Number and time text

/// Decimal digits and exponent of a JSON number text: value = digits × 10^exp, digits
/// without leading zeros ("" for zero).
fn parse_decimal(s: &str) -> Option<(bool, String, i64)> {
    let (neg, s) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    // Exponents beyond ±10^15 are refused (callers fall back to the raw text), so the
    // digit-count arithmetic below and in the formatters cannot overflow (R12).
    let (mant, exp) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i64>().ok()?),
        None => (s, 0),
    };
    if exp.unsigned_abs() > MAX_EXP {
        return None;
    }
    let (int, frac) = match mant.split_once('.') {
        Some((a, b)) => (a, b),
        None => (mant, ""),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits: String = int.chars().chain(frac.chars()).collect();
    let digits = digits.trim_start_matches('0').to_owned();
    let exp = exp.checked_sub(i64::try_from(frac.len()).ok()?)?;
    Some((neg, digits, exp))
}

/// Largest decimal exponent magnitude `parse_decimal` accepts.
const MAX_EXP: u64 = 1_000_000_000_000_000;

/// `formatScientific Generic` (the form of Haskell `show` for `Scientific`, and of aeson
/// for a non-integral number): fixed notation when the decimal exponent is in 0..=7,
/// else `d.ddde<n>`.
fn generic(neg: bool, digits: &str, exp: i64) -> String {
    let ds = digits.trim_end_matches('0');
    let mut out = String::new();
    if neg && !ds.is_empty() {
        out.push('-');
    }
    if ds.is_empty() {
        out.push_str("0.0");
        return out;
    }
    let exp = exp + (digits.len() - ds.len()) as i64;
    // value = 0.ds × 10^e
    let e = ds.len() as i64 + exp;
    if !(0..=7).contains(&e) {
        let (h, t) = ds.split_at(1);
        out.push_str(h);
        out.push('.');
        out.push_str(if t.is_empty() { "0" } else { t });
        out.push('e');
        out.push_str(&(e - 1).to_string());
    } else if e == 0 {
        out.push_str("0.");
        out.push_str(ds);
    } else if ds.len() as i64 <= e {
        out.push_str(ds);
        out.extend(std::iter::repeat_n('0', (e - ds.len() as i64) as usize));
        out.push_str(".0");
    } else {
        let (h, t) = ds.split_at(e as usize);
        out.push_str(h);
        out.push('.');
        out.push_str(t);
    }
    out
}

/// aeson's encoding of a number given as JSON text: an integer when the exponent is not
/// negative, else the generic form (`1.0e-8`, `0.5`).
pub fn aeson_number(text: &str) -> String {
    match parse_decimal(text) {
        None => text.to_owned(),
        Some((neg, digits, exp)) if (0..=1024).contains(&exp) => {
            if digits.is_empty() {
                return "0".into();
            }
            let mut s = String::new();
            if neg {
                s.push('-');
            }
            s.push_str(&digits);
            s.extend(std::iter::repeat_n('0', exp as usize));
            s
        }
        Some((neg, digits, exp)) => generic(neg, &digits, exp),
    }
}

/// Haskell `show` of a `Scientific` (`100.0`, `0.5`, `5.0e-2`).
pub fn show_scientific(text: &str) -> Option<String> {
    let (neg, digits, exp) = parse_decimal(text)?;
    Some(generic(neg, &digits, exp))
}

/// aeson's number for a `Double` (shortest round-trip digits).
pub fn aeson_double(f: f64) -> Value {
    num_value(&format!("{f:e}"))
}

fn num_value(s: &str) -> Value {
    // Arbitrary precision keeps the text; the writer re-formats it.
    serde_json::from_str::<Value>(s).unwrap_or(Value::Null)
}

/// aeson's `UTCTime` text of a microsecond POSIX time: `YYYY-MM-DDTHH:MM:SS[.f]Z`, the
/// fraction without trailing zeros.
pub fn utc_micros(us: u64) -> String {
    let secs = us / 1_000_000;
    let frac = us % 1_000_000;
    let mut s = utc_secs(secs as i64);
    s.pop(); // 'Z'
    if frac != 0 {
        let f = format!("{frac:06}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

/// aeson's `UTCTime` text of whole POSIX seconds.
pub fn utc_secs(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil date from days since 1970-01-01 (Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

/// A Pact `ParsedInteger` (meta `creationTime`, `ttl`, `gasLimit`, cont `step`): a JSON
/// number, a string holding a number (`"600"`), or `{"int": ...}`. Pact accepts all three
/// in a command, so stored commands carry all three. A fraction rounds like Haskell
/// `round` (F2: chainweb-data and kda-txs show 1692638383.532 as ...:44).
fn parsed_integer(v: &Value) -> Option<i64> {
    let s = match v {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_owned(),
        Value::Object(o) => {
            return match o.get("int") {
                Some(i @ (Value::Number(_) | Value::String(_))) => parsed_integer(i),
                _ => None,
            }
        }
        _ => return None,
    };
    let (neg, digits, exp) = parse_decimal(&s)?;
    let digits = if digits.is_empty() {
        "0".to_owned()
    } else {
        digits
    };
    let (int, frac): (String, &str) = if exp >= 0 {
        let mut d = digits.clone();
        d.extend(std::iter::repeat_n('0', exp.min(30) as usize));
        (d, "")
    } else {
        let cut = digits.len() as i64 + exp;
        if cut <= 0 {
            // value = 0.000ddd: below one half unless the zero padding is empty.
            let pad = (-cut) as usize;
            ("0".into(), if pad == 0 { digits.as_str() } else { "0" })
        } else {
            (digits[..cut as usize].to_owned(), &digits[cut as usize..])
        }
    };
    let mut n: i64 = int.parse().ok()?;
    if round_up_half_even(frac, n) {
        n = n.checked_add(1)?;
    }
    Some(if neg { -n } else { n })
}

/// Haskell `round` (what Pact's `ParsedInteger` applies to a JSON number, and what
/// chainweb-data shows for a fractional `creationTime`): nearest, ties to even. `frac` is
/// the fraction digits after the point, `int` the integer part's magnitude.
fn round_up_half_even(frac: &str, int: i64) -> bool {
    let mut b = frac.bytes();
    match b.next() {
        Some(d) if d > b'5' => true,
        Some(b'5') => b.any(|d| d != b'0') || int % 2 == 1,
        _ => false,
    }
}

/// A Pact `ParsedDecimal` (meta `gasPrice`) as number text: a JSON number, a string
/// holding a number, or `{"decimal": ...}`.
fn parsed_decimal_text(v: &Value) -> Option<String> {
    match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => parse_decimal(s.trim()).map(|_| s.trim().to_owned()),
        Value::Object(o) => match o.get("decimal") {
            Some(d @ (Value::Number(_) | Value::String(_))) => parsed_decimal_text(d),
            _ => None,
        },
        _ => None,
    }
}

// ---------------------------------------------------------------------------------------
// aeson text

/// Compact aeson text of `v`: keys sorted by code point at every depth, aeson numbers and
/// string escapes.
pub fn write_aeson(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => out.extend_from_slice(aeson_number(&n.to_string()).as_bytes()),
        Value::String(s) => write_str(s, out),
        Value::Array(a) => {
            out.push(b'[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_aeson(x, out);
            }
            out.push(b']');
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_unstable();
            out.push(b'{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_str(k, out);
                out.push(b':');
                write_aeson(&m[k], out);
            }
            out.push(b'}');
        }
    }
}

fn write_str(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes())
            }
            c => {
                let mut b = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// `paramtext` of chainweb-data: aeson's encoding of the params array.
pub fn param_text(params: &Value) -> String {
    let mut b = Vec::new();
    write_aeson(params, &mut b);
    String::from_utf8(b).unwrap_or_default()
}

/// chainweb-data's `censorJSON`: NUL characters in strings become `<NUL>` (Postgres jsonb
/// cannot hold them).
fn censor(v: Value) -> Value {
    match v {
        Value::String(s) if s.contains('\0') => Value::String(s.replace('\0', "<NUL>")),
        Value::Array(a) => Value::Array(a.into_iter().map(censor).collect()),
        Value::Object(m) => Value::Object(m.into_iter().map(|(k, v)| (k, censor(v))).collect()),
        v => v,
    }
}

// ---------------------------------------------------------------------------------------
// Parsed tx pieces

/// One event of a tx as chainweb-data stores it.
#[derive(Debug, Clone)]
pub struct Ev {
    /// Event position in the tx output.
    pub pos: usize,
    /// `module.NAME` (`_ev_qualName`).
    pub qual_name: String,
    /// `ns.module` or `module` (`_ev_module`), "" when absent.
    pub module: String,
    pub module_hash: String,
    pub params: Value,
}

/// The rendered CommandResult of `r`.
pub fn output(view: &View, r: &TxRecord) -> ApiResult<Value> {
    render_output(r, &view.dict()).map_err(|e| ApiError::Internal(e.into()))
}

/// Events of a rendered CommandResult, as chainweb-data's `mkEvent` reads them.
pub fn events(out: &Value) -> Vec<Ev> {
    let Some(a) = out.get("events").and_then(Value::as_array) else {
        return Vec::new();
    };
    a.iter()
        .enumerate()
        .map(|(pos, e)| {
            let name = e.get("name").and_then(Value::as_str).unwrap_or("");
            let module = e.get("module").and_then(|m| {
                let n = m.get("name")?.as_str()?;
                Some(match m.get("namespace").and_then(Value::as_str) {
                    Some(ns) => format!("{ns}.{n}"),
                    None => n.to_owned(),
                })
            });
            let qual_name = match &module {
                Some(m) => format!("{m}.{name}"),
                None => name.to_owned(),
            };
            Ev {
                pos,
                qual_name,
                module: module.unwrap_or_default(),
                module_hash: e
                    .get("moduleHash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                params: match e.get("params") {
                    Some(p @ Value::Array(_)) => p.clone(),
                    _ => Value::Array(Vec::new()),
                },
            }
        })
        .collect()
}

/// Request key text of `r` in API output.
pub fn rk_text(r: &TxRecord) -> String {
    match &r.request_key {
        Some(h) if !r.key.is_coinbase() => h.to_b64url(),
        _ => COINBASE_RK_TEXT.into(),
    }
}

/// Request key bytes of `r` in chainweb-data's row order (`cb` for coinbase).
pub fn rk_order(r: &TxRecord) -> Vec<u8> {
    match &r.request_key {
        Some(h) if !r.key.is_coinbase() => h.to_b64url().into_bytes(),
        _ => COINBASE_RK_ORDER.to_vec(),
    }
}

/// Event `idx` of chainweb-data: the position in the tx, 0 for every coinbase event.
pub fn ev_idx(k: TxKey, pos: usize) -> i64 {
    if k.is_coinbase() {
        0
    } else {
        pos as i64
    }
}

fn block_hash_text(b: &BlockRecord) -> String {
    b.hash.to_b64url()
}

// ---------------------------------------------------------------------------------------
// EventDetail

pub fn event_detail(r: &TxRecord, b: &BlockRecord, e: &Ev) -> Value {
    let mut m = Map::new();
    m.insert("name".into(), e.qual_name.clone().into());
    m.insert("params".into(), e.params.clone());
    m.insert("moduleHash".into(), e.module_hash.clone().into());
    m.insert("chain".into(), (r.key.chain() as u64).into());
    m.insert("height".into(), (r.key.height() as u64).into());
    m.insert("blockTime".into(), utc_micros(b.creation_time).into());
    m.insert("blockHash".into(), block_hash_text(b).into());
    m.insert("requestKey".into(), rk_text(r).into());
    m.insert("idx".into(), ev_idx(r.key, e.pos).into());
    Value::Object(m)
}

// ---------------------------------------------------------------------------------------
// TransferDetail

/// A transfer row of chainweb-data (`mkTransfer`): an event whose qualified name ends in
/// `TRANSFER`, with three params, string accounts and a readable amount.
#[derive(Debug, Clone)]
pub struct Transfer {
    pub pos: usize,
    pub token: String,
    pub from: String,
    pub to: String,
    /// `show` of the amount.
    pub amount: String,
}

/// Leading rational of a text (`Data.Text.Read.rational`): sign, digits, optional
/// fraction and exponent; trailing text is ignored.
fn text_rational(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let ds = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == ds {
        return None;
    }
    if i + 1 < b.len() && b[i] == b'.' && b[i + 1].is_ascii_digit() {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'-' || b[j] == b'+') {
            j += 1;
        }
        let es = j;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j > es {
            i = j;
        }
    }
    Some(s[..i].to_owned())
}

fn amount_of(v: &Value) -> Option<String> {
    let num = |v: &Value| match v {
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    };
    let txt = |v: &Value| v.as_str().and_then(text_rational);
    let t = v
        .get("decimal")
        .and_then(num)
        .or_else(|| v.get("decimal").and_then(txt))
        .or_else(|| v.get("int").and_then(num))
        .or_else(|| v.get("int").and_then(txt))
        .or_else(|| num(v))
        .or_else(|| txt(v))?;
    show_scientific(&t)
}

pub fn transfers(evs: &[Ev]) -> Vec<Transfer> {
    evs.iter()
        .filter_map(|e| {
            if !e.qual_name.ends_with("TRANSFER") {
                return None;
            }
            let p = e.params.as_array()?;
            if p.len() != 3 {
                return None;
            }
            Some(Transfer {
                pos: e.pos,
                token: e.module.clone(),
                from: p[0].as_str()?.to_owned(),
                to: p[1].as_str()?.to_owned(),
                amount: amount_of(&p[2])?,
            })
        })
        .collect()
}

/// `params->>i`: a string param unquoted, anything else as its JSON text.
fn param_txt(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        v => Some(param_text(v)),
    }
}

fn param_int(v: Option<&Value>) -> Option<i64> {
    param_txt(v)?.trim().parse().ok()
}

/// `joinXChainInfo`: the other side of a cross-chain coin transfer.
fn xchain(r: &TxRecord, evs: &[Ev], t: &Transfer) -> (Option<String>, Option<i64>) {
    if t.token != "coin" {
        return (None, None);
    }
    let idx = ev_idx(r.key, t.pos);
    if t.to.is_empty() {
        let src = evs.iter().find(|e| {
            ev_idx(r.key, e.pos) == idx - 1
                && e.qual_name == "coin.TRANSFER_XCHAIN"
                && param_txt(e.params.get(0)).as_deref() == Some(t.from.as_str())
        });
        if let Some(e) = src {
            return (param_txt(e.params.get(1)), param_int(e.params.get(3)));
        }
    }
    if t.from.is_empty() && !r.key.is_coinbase() {
        let dst = evs.iter().find(|e| {
            e.qual_name == "pact.X_RESUME"
                && param_txt(e.params.get(1)).as_deref() == Some("coin.transfer-crosschain")
                && param_txt(e.params.get(2).and_then(|a| a.get(1))).as_deref()
                    == Some(t.to.as_str())
        });
        if let Some(e) = dst {
            return (
                param_txt(e.params.get(2).and_then(|a| a.get(0))),
                param_int(e.params.get(0)),
            );
        }
    }
    (None, None)
}

pub fn transfer_detail(r: &TxRecord, b: &BlockRecord, evs: &[Ev], t: &Transfer) -> Value {
    let (xa, xc) = xchain(r, evs, t);
    let mut m = Map::new();
    m.insert("blockHash".into(), block_hash_text(b).into());
    m.insert("requestKey".into(), rk_text(r).into());
    m.insert("chain".into(), (r.key.chain() as u64).into());
    m.insert("height".into(), (r.key.height() as u64).into());
    m.insert("idx".into(), ev_idx(r.key, t.pos).into());
    m.insert("token".into(), t.token.clone().into());
    m.insert("fromAccount".into(), t.from.clone().into());
    m.insert("toAccount".into(), t.to.clone().into());
    m.insert(
        "crossChainAccount".into(),
        xa.map_or(Value::Null, Value::String),
    );
    m.insert("crossChainId".into(), xc.map_or(Value::Null, Value::from));
    m.insert("amount".into(), t.amount.clone().into());
    m.insert("blockTime".into(), utc_micros(b.creation_time).into());
    Value::Object(m)
}

// ---------------------------------------------------------------------------------------
// TxDetail

/// The parsed signed `cmd` of a user tx (None for coinbase or an unparsable cmd).
pub fn cmd_value(r: &TxRecord) -> Option<Value> {
    if r.cmd_unparsed {
        return None;
    }
    parse_untrusted(render_cmd(r)?).ok()
}

fn int_of(v: Option<&Value>) -> Value {
    match v.and_then(parsed_integer) {
        Some(i) => i.into(),
        None => Value::Null,
    }
}

/// `pactId` of a cont tx.
pub fn pact_id(cmd: &Value) -> Option<Hash32> {
    cmd.pointer("/payload/cont/pactId")
        .and_then(Value::as_str)
        .and_then(|s| Hash32::from_b64url(s).ok())
}

/// Exec code of a user tx.
pub fn exec_code(cmd: &Value) -> Option<String> {
    cmd.pointer("/payload/exec/code")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// `joinContinuationHistory`: walk from the tx whose request key is `pact_id` through
/// `pactId` links; the first code found and the request keys walked (outermost first).
fn continuation_history(view: &View, pact: &Hash32) -> ApiResult<(Option<String>, Vec<String>)> {
    let mut steps = Vec::new();
    let mut code = None;
    let mut cur = Some(*pact);
    for _ in 0..8 {
        let Some(rk) = cur.take() else { break };
        // Genesis txs count: chainweb-data's join sees them (G1).
        let Some((_, r)) = view.tx_by_rk(&rk)? else {
            break;
        };
        steps.push(rk.to_b64url());
        let cmd = cmd_value(&r);
        if code.is_none() {
            code = cmd.as_ref().and_then(exec_code);
        }
        cur = cmd.as_ref().and_then(pact_id).filter(|p| *p != rk);
    }
    Ok((code, steps))
}

fn signer(s: &Value) -> Option<Value> {
    let o = s.as_object()?;
    let mut m = Map::new();
    if let Some(a) = o.get("addr").and_then(Value::as_str) {
        m.insert("addr".into(), a.into());
    }
    if let Some(a) = o.get("scheme").and_then(Value::as_str) {
        m.insert("scheme".into(), a.into());
    }
    m.insert("pubKey".into(), o.get("pubKey")?.as_str()?.into());
    let caps = o
        .get("clist")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|c| {
                    let mut cm = Map::new();
                    cm.insert("name".into(), c.get("name")?.as_str()?.into());
                    cm.insert("args".into(), c.get("args")?.as_array()?.clone().into());
                    Some(Value::Object(cm))
                })
                .collect()
        })
        .unwrap_or_default();
    m.insert("clist".into(), Value::Array(caps));
    Some(Value::Object(m))
}

/// `TxDetail` of a user tx, or None when its cmd cannot be read (chainweb-data would not
/// have stored it).
pub fn tx_detail(view: &View, r: &TxRecord, b: &BlockRecord) -> ApiResult<Option<Value>> {
    if r.request_key.is_none() {
        return Ok(None);
    }
    tx_detail_with(view, r, b, output(view, r)?)
}

/// `tx_detail` with the tx's output already rendered.
pub fn tx_detail_with(
    view: &View,
    r: &TxRecord,
    b: &BlockRecord,
    out: Value,
) -> ApiResult<Option<Value>> {
    let (Some(rk), Some(cmd)) = (r.request_key, cmd_value(r)) else {
        return Ok(None);
    };
    let meta = cmd.get("meta").cloned().unwrap_or(Value::Null);
    let cont = cmd.pointer("/payload/cont");
    let exec = cmd.pointer("/payload/exec");
    let pact = cont.and_then(|c| c.get("pactId")).and_then(Value::as_str);
    let success = out.pointer("/result/status").and_then(Value::as_str) == Some("success");
    let result = if success {
        out.pointer("/result/data").cloned()
    } else {
        out.pointer("/result/error").cloned()
    }
    .unwrap_or(Value::Null);

    let (initial, prev) = match pact.and_then(|p| Hash32::from_b64url(p).ok()) {
        Some(p) => {
            let (code, steps) = continuation_history(view, &p)?;
            match code {
                Some(c) => (
                    Value::String(c),
                    Value::Array(steps.into_iter().map(Value::String).collect()),
                ),
                None => (Value::Null, Value::Null),
            }
        }
        None => (Value::Null, Value::Null),
    };

    let signers: Vec<Value> = cmd
        .get("signers")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(signer).collect())
        .unwrap_or_default();
    let sigs: Vec<Value> = render_tx(r, &view.dict())
        .ok()
        .flatten()
        .and_then(|t| t.get("sigs").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|s| {
            let mut m = Map::new();
            m.insert("sig".into(), s.get("sig")?.as_str()?.into());
            Some(Value::Object(m))
        })
        .collect();
    // chainweb-data zips signers with sigs (one row each).
    let n = signers.len().min(sigs.len());

    let gas_price = meta
        .get("gasPrice")
        .and_then(parsed_decimal_text)
        .and_then(|n| n.parse::<f64>().ok())
        .map_or(Value::Null, aeson_double);
    let txid = match out.get("txId") {
        Some(Value::Number(n)) => Value::Number(n.clone()),
        _ => 0.into(),
    };

    let mut m = Map::new();
    m.insert("ttl".into(), int_of(meta.get("ttl")));
    m.insert("gasLimit".into(), int_of(meta.get("gasLimit")));
    m.insert("gasPrice".into(), gas_price);
    m.insert(
        "nonce".into(),
        cmd.get("nonce").cloned().unwrap_or(Value::Null),
    );
    m.insert("pactId".into(), pact.map_or(Value::Null, Value::from));
    m.insert(
        "rollback".into(),
        cont.and_then(|c| c.get("rollback"))
            .cloned()
            .unwrap_or(Value::Null),
    );
    m.insert("step".into(), int_of(cont.and_then(|c| c.get("step"))));
    let data = match (cont, exec) {
        (Some(c), _) => c.get("data").cloned().unwrap_or(Value::Null),
        (None, Some(e)) => e.get("data").cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    };
    m.insert("data".into(), censor(data));
    m.insert(
        "proof".into(),
        cont.and_then(|c| c.get("proof"))
            .and_then(Value::as_str)
            .map_or(Value::Null, Value::from),
    );
    m.insert("gas".into(), out.get("gas").cloned().unwrap_or(Value::Null));
    m.insert("result".into(), censor(result));
    m.insert(
        "logs".into(),
        out.get("logs").and_then(Value::as_str).unwrap_or("").into(),
    );
    m.insert(
        "metadata".into(),
        censor(out.get("metaData").cloned().unwrap_or(Value::Null)),
    );
    m.insert(
        "continuation".into(),
        censor(out.get("continuation").cloned().unwrap_or(Value::Null)),
    );
    m.insert("txid".into(), txid);
    m.insert("chain".into(), (r.key.chain() as u64).into());
    m.insert("height".into(), (r.key.height() as u64).into());
    m.insert("blockTime".into(), utc_micros(b.creation_time).into());
    m.insert("blockHash".into(), block_hash_text(b).into());
    m.insert(
        "creationTime".into(),
        meta.get("creationTime")
            .and_then(parsed_integer)
            .map_or(Value::Null, |s| utc_secs(s).into()),
    );
    m.insert("requestKey".into(), rk.to_b64url().into());
    m.insert(
        "sender".into(),
        meta.get("sender").cloned().unwrap_or(Value::Null),
    );
    m.insert(
        "code".into(),
        exec.and_then(|e| e.get("code"))
            .cloned()
            .unwrap_or(Value::Null),
    );
    m.insert("success".into(), success.into());
    m.insert(
        "events".into(),
        Value::Array(
            events(&out)
                .into_iter()
                .map(|e| {
                    let mut em = Map::new();
                    em.insert("name".into(), e.qual_name.into());
                    em.insert("params".into(), e.params);
                    Value::Object(em)
                })
                .collect(),
        ),
    );
    m.insert("initialCode".into(), initial);
    m.insert("previousSteps".into(), prev);
    m.insert(
        "signers".into(),
        Value::Array(signers.into_iter().take(n).collect()),
    );
    m.insert(
        "sigs".into(),
        Value::Array(sigs.into_iter().take(n).collect()),
    );
    Ok(Some(Value::Object(m)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// F1: Pact accepts `gasLimit`/`ttl` as a number, a numeric string or `{"int": n}`.
    #[test]
    fn parsed_integer_forms() {
        assert_eq!(parsed_integer(&json!(600)), Some(600));
        assert_eq!(parsed_integer(&json!("600")), Some(600));
        assert_eq!(parsed_integer(&json!({"int": 600})), Some(600));
        assert_eq!(parsed_integer(&json!({"int": "600"})), Some(600));
        assert_eq!(parsed_integer(&json!(600.0)), Some(600));
        assert_eq!(parsed_integer(&json!("x")), None);
        // F2: rounded like Haskell `round`, not floored.
        assert_eq!(parsed_integer(&json!(1692638383.532)), Some(1692638384));
        assert_eq!(parsed_integer(&json!(1692638383.4)), Some(1692638383));
        assert_eq!(parsed_integer(&json!(2.5)), Some(2));
        assert_eq!(parsed_integer(&json!(3.5)), Some(4));
        assert_eq!(parsed_integer(&json!(2.51)), Some(3));
        assert_eq!(parsed_integer(&json!(0.5)), Some(0));
        assert_eq!(parsed_integer(&json!(0.7)), Some(1));
        assert_eq!(parsed_integer(&json!(0.07)), Some(0));
        assert_eq!(parsed_integer(&json!(-1.7)), Some(-2));
        assert_eq!(parsed_integer(&json!(null)), None);
        assert_eq!(int_of(Some(&json!("600"))), json!(600));
        assert_eq!(
            parsed_decimal_text(&json!("1.0e-8")).as_deref(),
            Some("1.0e-8")
        );
        assert_eq!(
            parsed_decimal_text(&json!({"decimal": "0.00001"})).as_deref(),
            Some("0.00001")
        );
        assert_eq!(parsed_decimal_text(&json!(1e-8)).as_deref(), Some("1e-8"));
    }

    #[test]
    fn numbers() {
        assert_eq!(aeson_number("3.34e-6"), "3.34e-6");
        assert_eq!(aeson_number("0.00000334"), "3.34e-6");
        assert_eq!(aeson_number("0.90404266"), "0.90404266");
        assert_eq!(aeson_number("1.0"), "1.0");
        // R12: huge exponents fall back to the raw text instead of overflowing.
        assert_eq!(
            aeson_number("1.5e-9223372036854775808"),
            "1.5e-9223372036854775808"
        );
        assert_eq!(
            aeson_number("1.5e9223372036854775807"),
            "1.5e9223372036854775807"
        );
        assert_eq!(show_scientific("1.5e-9223372036854775808"), None);
        assert_eq!(
            parsed_integer(&serde_json::json!("1e-9223372036854775808")),
            None
        );
        assert_eq!(aeson_number("100"), "100");
        assert_eq!(aeson_number("1e2"), "100");
        assert_eq!(aeson_number("100.0"), "100.0");
        assert_eq!(aeson_number("12345678.5"), "1.23456785e7");
        assert_eq!(aeson_number("-0.05"), "-5.0e-2");
        assert_eq!(aeson_number("0.0"), "0.0");
        assert_eq!(aeson_number("0"), "0");
        assert_eq!(serde_json::to_string(&aeson_double(1e-8)).unwrap(), "1e-8");
        let mut b = Vec::new();
        write_aeson(&aeson_double(1e-8), &mut b);
        assert_eq!(b, b"1.0e-8");
        b.clear();
        write_aeson(&aeson_double(1500.0), &mut b);
        assert_eq!(b, b"1500");
        assert_eq!(show_scientific("100").unwrap(), "100.0");
        assert_eq!(show_scientific("0.90404265").unwrap(), "0.90404265");
        assert_eq!(show_scientific("0.01").unwrap(), "1.0e-2");
        assert_eq!(text_rational("1.5abc").as_deref(), Some("1.5"));
    }

    #[test]
    fn times() {
        assert_eq!(
            utc_micros(1_794_277_459_770_120),
            "2026-11-10T02:24:19.77012Z"
        );
        assert_eq!(utc_micros(1_794_277_459_000_000), "2026-11-10T02:24:19Z");
        assert_eq!(utc_secs(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_secs(951_782_400), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn aeson_text() {
        let v: Value = serde_json::from_str(r#"{"b":[1.0,"x\"\u0001"],"a":null}"#).unwrap();
        let mut b = Vec::new();
        write_aeson(&v, &mut b);
        assert_eq!(
            String::from_utf8(b).unwrap(),
            r#"{"a":null,"b":[1.0,"x\"\u0001"]}"#
        );
    }
}
