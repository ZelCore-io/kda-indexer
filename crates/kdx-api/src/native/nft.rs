//! NFT holdings from the additive `NftBalances` table (plan Task 11): `/nft/accounts/{a}`.
//! Rows come from marmalade v2 `RECONCILE` events (`source: "event"`) or from `/local`
//! `get-balance` (`source: "local"`); only balances above zero are listed. `stale: true` when
//! the row is also in `NftDirty`. A store without the tables (A-1) answers an empty list.

use axum::extract::Path;
use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use kdx_hot::nft::{self, nft_key, parse_nft_key, NftRow, SOURCE_EVENT};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};
use serde_json::json;

use super::{account_name, list_response, parse_path, parse_query, run, St, Q};
use crate::{ApiError, ApiResult};

type PathS = Result<Path<String>, axum::extract::rejection::PathRejection>;
type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// Cursor: 42 hex digits, the key tail (ledger, token hash, chain) of the last row listed.
fn parse_cursor(c: &str) -> ApiResult<(u32, [u8; 16], u8)> {
    let bad = || ApiError::BadRequest("invalid cursor".into());
    if c.len() != 42 || !c.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad());
    }
    let byte = |i: usize| u8::from_str_radix(&c[i * 2..i * 2 + 2], 16).map_err(|_| bad());
    let ledger = u32::from_str_radix(&c[..8], 16).map_err(|_| bad())?;
    let mut t = [0u8; 16];
    for (i, b) in t.iter_mut().enumerate() {
        *b = byte(4 + i)?;
    }
    Ok((ledger, t, byte(20)?))
}

fn cursor_text(ledger: u32, t: &[u8; 16], chain: u8) -> String {
    let mut s = format!("{ledger:08x}");
    s.extend(t.iter().map(|b| format!("{b:02x}")));
    s.push_str(&format!("{chain:02x}"));
    s
}

/// NFTs held by an account, per token and chain.
#[utoipa::path(
    get,
    path = "/api/v1/nft/accounts/{account}",
    params(
        ("account" = String, Path, description = "Account name"),
        ("chain" = Option<u8>, Query, description = "Chain id"),
        ("limit" = Option<u32>, Query, description = "1..=100, default 20"),
        ("cursor" = Option<String>, Query, description = "Opaque page cursor"),
    ),
    responses((status = 200, description = "`items` of {token_id, ledger, chain, balance, as_of_height, source, stale}, `next_cursor`, `as_of`"))
)]
pub async fn account(st: St, path: PathS, q: QueryQ) -> Response {
    let (account, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    run(st.0, move |view| {
        let account = account_name(&account)?;
        if q.chain
            .is_some_and(|c| u32::from(c) >= kdx_segment::SLOT_WIDTH)
        {
            return Err(ApiError::BadRequest("chain out of range".into()));
        }
        let limit = q.limit.unwrap_or(super::DEFAULT_LIMIT);
        if limit == 0 || limit > super::MAX_LIMIT {
            return Err(ApiError::BadRequest(format!(
                "limit must be 1..={}",
                super::MAX_LIMIT
            )));
        }
        let after = q.cursor.as_deref().map(parse_cursor).transpose()?;
        let d = view.dict();
        let Some(a) = d.find(DictKind::Account, account) else {
            return list_response(view, vec![], None);
        };
        let prefix = a.to_be_bytes();
        let from = match after {
            // The smallest key above the cursor row.
            Some((l, t, c)) => {
                let mut k = nft_key(a, l, &t, c).to_vec();
                k.push(0);
                k
            }
            None => prefix.to_vec(),
        };
        let mut items = Vec::new();
        let mut last = None;
        let mut more = false;
        let mut from = from;
        'pages: loop {
            // Chunks, so zero rows and other chains do not cost a whole page of reads.
            let rows = view
                .txn
                .scan_limit(Table::NftBalances, &from, 256, |k| k.starts_with(&prefix))?;
            let n = rows.len();
            for (k, v) in &rows {
                view.check_deadline()?;
                let (Some((_, lg, th, c)), Some(r)) = (parse_nft_key(k), NftRow::decode(v)) else {
                    continue;
                };
                if !r.positive() || q.chain.is_some_and(|x| x != c) {
                    continue;
                }
                if items.len() == limit as usize {
                    more = true;
                    break 'pages;
                }
                last = Some(cursor_text(lg, &th, c));
                items.push(json!({
                    "token_id": r.token_id,
                    "ledger": d.name(DictKind::Module, r.ledger).unwrap_or_default(),
                    "chain": c,
                    "balance": r.balance,
                    "as_of_height": r.as_of,
                    "source": if r.source == SOURCE_EVENT { "event" } else { "local" },
                    "stale": nft::is_dirty(&view.txn, a, lg, &r.token_id, c)?,
                }));
            }
            match rows.last() {
                Some((k, _)) if n == 256 => {
                    from = k.clone();
                    from.push(0);
                }
                _ => break,
            }
        }
        list_response(view, items, more.then_some(last).flatten())
    })
    .await
}
