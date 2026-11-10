//! `GET /api/v1/events?module&name&chain&param{N}=`: exact module and event name, exact match
//! of parameter N (an account reference or any string), newest first.

use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use kdx_decode::index::evt_module_entity;
use kdx_decode::Family;
use kdx_types::{Dict, DictKind};
use serde_json::{Map, Value};

use super::txs::event_json;
use super::{
    candidates, list_params, list_response, page_rows, parse_query, run, tx_context, ListParams, Q,
    St,
};
use crate::compat::shapes;
use crate::ApiError;

fn params_of(q: &Q) -> Vec<(usize, &str)> {
    [&q.param0, &q.param1, &q.param2, &q.param3, &q.param4, &q.param5, &q.param6, &q.param7, &q.param8, &q.param9]
        .iter()
        .enumerate()
        .filter_map(|(i, p)| p.as_deref().map(|s| (i, s)))
        .collect()
}

/// Events by module and name (a qualified `name` such as `coin.TRANSFER` is split at its last dot
/// when `module` is absent). At least one of `module`, `name`, `param{N}` is required.
#[utoipa::path(
    get,
    path = "/api/v1/events",
    params(
        ListParams,
        ("module" = Option<String>, Query, description = "Exact module, e.g. `coin` or `free.token`"),
        ("name" = Option<String>, Query, description = "Exact event name, e.g. `TRANSFER`; needs `module`, or write it qualified (`coin.TRANSFER`)"),
        ("param0" = Option<String>, Query, description = "Exact string value of event parameter 0 (param0..param9)"),
    ),
    responses((status = 200, description = "`items` of events with their tx context, `next_cursor`, `as_of`"))
)]
pub async fn list(st: St, q: Result<Query<Q>, axum::extract::rejection::QueryRejection>) -> Response {
    let q = match parse_query(q) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    if q.module.is_none() && q.name.is_none() && params_of(&q).is_empty() {
        return ApiError::BadRequest("give module, name or param0..param9".into()).into_response();
    }
    run(st.0, move |view| {
        let lp = list_params(&q)?;
        let (mut module, mut name) = (q.module.clone(), q.name.clone());
        if module.is_none() {
            if let Some(n) = &name {
                if let Some((m, short)) = n.rsplit_once('.') {
                    module = Some(m.to_owned());
                    name = Some(short.to_owned());
                }
            }
        }
        if name.is_some() && module.is_none() {
            return Err(ApiError::BadRequest(
                "name needs module (or a qualified name such as coin.TRANSFER)".into(),
            ));
        }
        let params = params_of(&q);
        let d = view.dict();
        // Most selective posting list available; every candidate is verified below.
        // Dictionary event names are qualified (`module.NAME`).
        let (fam, entity) = if let (Some(n), Some(m)) = (&name, &module) {
            match d.find(DictKind::EventName, &format!("{m}.{n}")) {
                Some(id) => (Family::Evt, u64::from(id)),
                None => return list_response(view, vec![], None),
            }
        } else if let Some((_, v)) = params.first().filter(|_| module.is_none()) {
            match d.account_id(v) {
                Some(id) => (Family::Acct, u64::from(id)),
                None => return list_response(view, vec![], None),
            }
        } else if let Some(m) = &module {
            match d.module_id(m) {
                Some(id) => (Family::Evt, evt_module_entity(id)),
                None => return list_response(view, vec![], None),
            }
        } else {
            return Err(ApiError::BadRequest("module or name is needed with param filters on their own".into()));
        };
        let keys = candidates(view, fam, entity, &lp)?;
        let (items, next) = page_rows(view, keys, &lp, |view, r, blocks| {
            let evs = shapes::events(&shapes::output(view, r)?);
            let Some(b) = blocks.get(view, r.key.chain(), r.key.height())? else {
                return Ok(vec![]);
            };
            let ctx = tx_context(r, b);
            let mut rows = Vec::new();
            for e in &evs {
                if module.as_deref().is_some_and(|m| e.module != m) {
                    continue;
                }
                let short = e
                    .qual_name
                    .strip_prefix(&e.module)
                    .and_then(|s| s.strip_prefix('.'))
                    .unwrap_or(&e.qual_name);
                if name.as_deref().is_some_and(|n| short != n) {
                    continue;
                }
                let p = e.params.as_array();
                if !params.iter().all(|(i, v)| {
                    p.and_then(|a| a.get(*i)).and_then(Value::as_str) == Some(*v)
                }) {
                    continue;
                }
                let Value::Object(mut row) = event_json(e) else { continue };
                let mut m = Map::new();
                for (k, v) in &ctx {
                    m.insert(k.clone(), v.clone());
                }
                m.append(&mut row);
                rows.push((e.pos as u16, Value::Object(m)));
            }
            Ok(rows)
        })?;
        list_response(view, items, next)
    })
    .await
}
