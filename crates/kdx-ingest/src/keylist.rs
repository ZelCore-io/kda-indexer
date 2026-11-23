//! D3 gap groups 3 and 4: the key listing of tracked tokens ("system accounts").
//!
//! Some funded rows of a token are named in no tx: the token's own `init` writes an account
//! that is a string literal of the module code (`free.shatter` `ROOT` on 18 chains, `free.KAYC`
//! `ROOT`, `free.ba-t3` `CORE`), or the module writes a name built at run time with no event
//! (the heron fee account `u:…heron-fees.require-WALLET:…`, the `kaddex.kdx` pool account).
//! No tx-local rule can find them. The node can: `/local` allows `(keys <module>.<table>)` on
//! any module's table (measured on mainnet01, 2026-11-23, flat 40,000 gas).
//!
//! - Which table: the module's `deftable`s whose `defschema` has a `balance` field
//!   ([`ledger_tables`], from `describe-module`'s code on that chain).
//! - Which tokens: every tracked token (`Tokens` row, not a not-fungible verdict, not at its
//!   row cap, not `coin`). A token never listed comes first, so a newly tracked one (D3
//!   discovery, first posting) is listed within a refresh; then each again after
//!   [`SWEEP_SECS`], and a token whose last listing failed on some chain after [`RETRY_SECS`].
//! - Per step, one `(token, chain)`: one `describe-module` read, then one `keys` read per
//!   ledger table. At most one step per [`STEP_EVERY`], inside the shared `/local` rate limit.
//!   Progress (`Meta balances_keylist/<module>`: when last listed, next chain, chains that
//!   failed) is kept per step, so a restart resumes.
//! - Marks: only keys we hold nothing for (no `Balances` row, no dirty or seed mark) are marked,
//!   through `balances::mark_row` (the postings' mark path, so the row caps apply) at the height
//!   the node read. The normal reconcile then reads them like any dirty row.
//! - Fail-closed: a listing over [`kdx_node::local::MAX_LISTED_KEYS`], an unusable answer, a
//!   module with no ledger table or a node error that repeats [`NODE_TRIES`] times marks nothing
//!   for that chain, is logged with the module and chain, and counts as failed (retried after
//!   [`RETRY_SECS`]). Totals are in `Meta balances_keylist_stats` (`reconcile-balances
//!   --status`).
//! - Refused (like the receivers pass, review L3) while the store has no tail yet or its
//!   backfill is incomplete: the listing interns account names.

use std::time::Duration;

use kdx_node::local::valid_ident;

/// Seconds after which a listed token is listed again (the periodic sweep).
pub const SWEEP_SECS: u32 = 7 * 86_400;
/// Seconds after which a token whose last listing failed on some chain is listed again.
pub const RETRY_SECS: u32 = 86_400;
/// Least time between two steps (one `(token, chain)` each: 1 + tables `/local` reads).
pub const STEP_EVERY: Duration = Duration::from_secs(1);
/// How often the queue of due tokens is rebuilt from the store (newly tracked tokens join).
pub const REFRESH: Duration = Duration::from_secs(600);
/// Node errors in a row on one `(token, chain)` before it is given up as failed.
pub const NODE_TRIES: u32 = 3;
/// Pause after a node error before the same step is tried again.
pub const NODE_RETRY: Duration = Duration::from_secs(30);
/// Most ledger tables listed per module and chain.
pub const MAX_TABLES: usize = 4;

/// Replace string literals by `""` and drop `;` comments, so the scan below sees only code.
/// A Pact string may span lines (`\` continuations); an unterminated one runs to the end.
fn code_only(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut it = code.chars();
    while let Some(c) = it.next() {
        match c {
            '"' => {
                out.push_str("\"\"");
                while let Some(c) = it.next() {
                    match c {
                        '\\' => {
                            it.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            ';' => {
                for c in it.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// The tokens of a code text: parentheses, brackets and braces alone, other runs split at
/// whitespace.
fn tokens(code: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in code.char_indices() {
        let delim = matches!(c, '(' | ')' | '[' | ']' | '{' | '}');
        if c.is_whitespace() || delim {
            if let Some(s) = start.take() {
                out.push(&code[s..i]);
            }
            if delim {
                out.push(&code[i..i + 1]);
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        out.push(&code[s..]);
    }
    out
}

/// The last segment of a possibly qualified name (`ns.mod.schema` → `schema`).
fn last_segment(s: &str) -> &str {
    s.rsplit('.').next().unwrap_or(s)
}

/// The ledger tables of a module's code: each `(deftable T:{S})` whose `(defschema S …)` has a
/// `balance` field at its top level (a `balance` inside `@model` does not count). In code
/// order, at most [`MAX_TABLES`], each a valid identifier.
pub fn ledger_tables(code: &str) -> Vec<String> {
    let clean = code_only(code);
    let t = tokens(&clean);
    let mut schemas: Vec<&str> = Vec::new();
    let mut tables: Vec<(&str, &str)> = Vec::new();
    let mut i = 0;
    while i + 1 < t.len() {
        if t[i] != "(" {
            i += 1;
            continue;
        }
        match t[i + 1] {
            "defschema" if i + 2 < t.len() => {
                let name = t[i + 2];
                let (mut depth, mut j, mut has) = (1usize, i + 3, false);
                while j < t.len() && depth > 0 {
                    match t[j] {
                        "(" | "[" | "{" => depth += 1,
                        ")" | "]" | "}" => depth -= 1,
                        f if depth == 1 => {
                            has |= f.starts_with("balance:")
                                || (f == "balance"
                                    && t.get(j + 1).is_some_and(|n| n.starts_with(':')));
                        }
                        _ => {}
                    }
                    j += 1;
                }
                if has {
                    schemas.push(name);
                }
                i = j;
            }
            "deftable" if i + 2 < t.len() => {
                // `T:{S}`, `T: {S}` or `T :{S}`: the name, then `{`, the schema, `}`.
                let name = t[i + 2].trim_end_matches(':');
                let mut j = i + 3;
                while j < t.len() && t[j] == ":" {
                    j += 1;
                }
                if t.get(j) == Some(&"{") {
                    if let Some(s) = t.get(j + 1) {
                        tables.push((name, last_segment(s)));
                    }
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    let mut out: Vec<String> = Vec::new();
    for (name, schema) in tables {
        if schemas.contains(&schema) && valid_ident(name) && !out.iter().any(|o| o == name) {
            out.push(name.to_owned());
        }
    }
    out.truncate(MAX_TABLES);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `free.KAYC` (chain 0, D3 gap group 3), as `describe-module` answered it.
    const KAYC: &str = r#"(module KAYC GOVERNANCE
  @doc " KoolApes is the koolest token. "
  (defconst ROOT_ACCOUNT_ID:string 'ROOT)
  (defschema token-schema
    @doc " An account, holding a token balance. \
         \ \
         \ ROW KEY: accountId. "
    balance:decimal
    guard:guard
  )
  (defschema crosschain-schema
    @doc " Schema for yielded value in cross-chain transfers "
    receiver:string
    receiver-guard:guard
    amount:decimal
  )
  (deftable token-table:{token-schema})
)"#;

    /// The heron token (D3 gap group 4): its table is `coin-table`, with an `@model` that
    /// names `balance` before the field.
    const HERON: &str = r#"(module heron GOV
  (defschema coin-schema
    @doc "The coin contract token schema"
    @model [ (invariant (>= balance 0.0)) ]

    balance:decimal
    guard:guard)
  (defschema crosschain-schema
  @doc "Schema for yielded value in cross-chain transfers"
  receiver:string
  receiver-guard:guard
  amount:decimal
  source-chain:string)
  (deftable coin-table:{coin-schema})
)"#;

    /// `kaddex.kdx` (D3 gap group 4): six tables, one ledger.
    const KDX: &str = r#"(module kdx GOVERNANCE
  @model
    [ (defproperty conserves-mass
        (= (column-delta token-table 'balance) 0.0))
    ]
  (defschema token-schema
    @doc "The KDX contract token schema"
    @model [ (invariant (>= balance 0.0)) ]

    balance:decimal
    guard:guard)
  (defschema token-supply
      total-burned:decimal
      total-minted:decimal)
  (defschema privilege
    guard:guard)
  (defschema special-account
    account:string)
  (defschema mint-cap
    maximum:decimal)
  (defschema contract-lock-status
      lock:bool)
  (deftable token-table:{token-schema})
  (deftable supply-table:{token-supply})
  (deftable privileges:{privilege})
  (deftable special-accounts:{special-account})
  (deftable mint-cap-table:{mint-cap})
  (deftable contract-lock:{contract-lock-status})
)"#;

    #[test]
    fn the_ledger_table_of_real_tokens() {
        assert_eq!(ledger_tables(KAYC), ["token-table"]);
        assert_eq!(ledger_tables(HERON), ["coin-table"]);
        assert_eq!(ledger_tables(KDX), ["token-table"]);
    }

    /// `balance` only in a comment, a string or `@model` is no field; a table name that is
    /// not an identifier is never returned (it is written into code).
    #[test]
    fn only_real_balance_fields_count() {
        let code = r#"(module m G
  ; (defschema fake balance:decimal) (deftable fake-table:{fake})
  (defschema s1 @doc "balance:decimal (deftable t:{s1})" @model [ (invariant (>= balance 0.0)) ] guard:guard)
  (defschema s2 balance : decimal)
  (defschema s3 balance:decimal)
  (deftable t1:{s1})
  (deftable t2 : {s2})
  (deftable t3:{free.m.s3})
  (deftable t3:{s3})
)"#;
        assert_eq!(ledger_tables(code), ["t2", "t3"]);
        assert!(
            ledger_tables("(module m G (defschema s balance:decimal) (deftable \"x\":{s}))")
                .is_empty()
        );
        assert!(ledger_tables("").is_empty());
        assert!(ledger_tables("(defschema").is_empty());
    }
}
