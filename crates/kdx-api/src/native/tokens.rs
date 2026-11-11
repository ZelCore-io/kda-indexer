//! Fungible tokens from the additive `Tokens` table (spec §3.9, plan Task 9): `/tokens` and
//! `/tokens/{module}`. A module is listed once it is the module of a `TRANSFER` event (an
//! `xfer` posting). `holders` counts reconciled rows with a balance above zero; it is
//! approximate (`holders_approx: true`). A store without the table (A-1) lists nothing.

use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use kdx_hot::balances::TokenRow;
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};
use serde_json::{json, Value};

use super::{list_response, parse_path, resource, run, St};
use crate::{ApiError, ApiResult};

type PathS = Result<Path<String>, axum::extract::rejection::PathRejection>;

/// Most tokens listed (the table holds one row per fungible module).
const MAX_TOKENS: usize = 10_000;

fn token_json(module: &str, t: &TokenRow) -> Value {
    json!({
        "module": module,
        "first_height": (t.first_height != u32::MAX).then_some(t.first_height),
        "holders": t.holders,
        "holders_approx": true,
    })
}

/// Every fungible token, by module name.
#[utoipa::path(
    get,
    path = "/api/v1/tokens",
    responses((status = 200, description = "`items` of {module, first_height, holders, holders_approx}, `as_of`"))
)]
pub async fn list(st: St) -> Response {
    run(st.0, move |view| {
        let d = view.dict();
        let mut items: Vec<(String, Value)> = Vec::new();
        for (k, v) in view
            .txn
            .scan_limit(Table::Tokens, &[], MAX_TOKENS, |_| true)?
        {
            let (Ok(id), Some(t)) = (<[u8; 4]>::try_from(k.as_slice()), TokenRow::decode(&v))
            else {
                continue;
            };
            let Some(name) = d.name(DictKind::Module, u32::from_be_bytes(id)) else {
                continue;
            };
            items.push((name.to_owned(), token_json(name, &t)));
        }
        items.sort_by(|a, b| a.0.cmp(&b.0));
        list_response(view, items.into_iter().map(|x| x.1).collect(), None)
    })
    .await
}

/// One fungible token.
#[utoipa::path(
    get,
    path = "/api/v1/tokens/{module}",
    params(("module" = String, Path, description = "Token module, e.g. `coin`")),
    responses(
        (status = 200, description = "{module, first_height, holders, holders_approx, as_of}"),
        (status = 404, description = "Not a known token")
    )
)]
pub async fn get(st: St, path: PathS) -> Response {
    let module = match parse_path(path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| -> ApiResult<Response> {
        let not_found = || ApiError::NotFound(format!("token {module} not found"));
        let id = view.dict().module_id(&module).ok_or_else(not_found)?;
        let t = view
            .txn
            .get(Table::Tokens, &id.to_be_bytes())?
            .as_deref()
            .and_then(TokenRow::decode)
            .ok_or_else(not_found)?;
        resource(view, token_json(&module, &t))
    })
    .await
}
