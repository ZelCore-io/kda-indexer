//! The chainweb-data row rules shared by the API and the stored row counts (`kdx_hot::rowcount`):
//! the events of a rendered CommandResult (`mkEvent`), its transfer rows (`mkTransfer`) and
//! the number forms they need. One copy, so a list walk and a stored count apply the same rule.

use serde_json::Value;

/// Decimal digits and exponent of a JSON number text: value = digits × 10^exp, digits
/// without leading zeros ("" for zero).
pub fn parse_decimal(s: &str) -> Option<(bool, String, i64)> {
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
pub fn generic(neg: bool, digits: &str, exp: i64) -> String {
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

/// Haskell `show` of a `Scientific` (`100.0`, `0.5`, `5.0e-2`).
pub fn show_scientific(text: &str) -> Option<String> {
    let (neg, digits, exp) = parse_decimal(text)?;
    Some(generic(neg, &digits, exp))
}

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
/// fraction and exponent; trailing text is ignored. Same rule as the index's
/// `transfer_amount_ok`, so an `xfer` posting renders a row unless `show_scientific` cannot
/// render the amount (an exponent beyond its bound, such as `1e9999999999999999`, which
/// chainweb-data cannot store either): that transfer has its posting but no compat row.
pub fn text_rational(s: &str) -> Option<String> {
    crate::build::rational_prefix_len(s).map(|i| s[..i].to_owned())
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
