//! `GET /api/v1/txs/{request_key}` and the tx renderings shared by the list routes.

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_types::record::{BlockRecord, TxRecord};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use utoipa::IntoParams;

use super::{parse_hash, parse_path, parse_query, resource, run, tx_context, St};
use crate::compat::shapes::{self, Ev};
use crate::store::View;
use crate::{ApiError, ApiResult};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TxQuery {
    /// `proof` adds the SPV proof of a continuation tx (it can be large).
    include: Option<String>,
}

/// Event name without its module.
fn short_name(e: &Ev) -> &str {
    e.qual_name
        .strip_prefix(&e.module)
        .and_then(|s| s.strip_prefix('.'))
        .unwrap_or(&e.qual_name)
}

pub fn event_json(e: &Ev) -> Value {
    json!({
        "idx": e.pos,
        "name": short_name(e),
        "qualified_name": e.qual_name,
        "module": e.module,
        "module_hash": e.module_hash,
        "params": e.params,
    })
}

/// A transfer event with the other side of a cross-chain transfer, when known.
pub fn transfer_json(r: &TxRecord, b: &BlockRecord, evs: &[Ev], t: &shapes::Transfer) -> Value {
    let d = shapes::transfer_detail(r, b, evs, t);
    json!({
        "idx": t.pos,
        "token": t.token,
        "from": t.from,
        "to": t.to,
        "amount": t.amount,
        "cross_chain_account": d.get("crossChainAccount").cloned().unwrap_or(Value::Null),
        "cross_chain_id": d.get("crossChainId").cloned().unwrap_or(Value::Null),
    })
}

fn signer_json(s: &Value) -> Option<Value> {
    let o = s.as_object()?;
    let caps: Vec<Value> = o
        .get("clist")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|c| {
                    Some(json!({
                        "name": c.get("name")?,
                        "args": c.get("args").cloned().unwrap_or(Value::Array(vec![])),
                    }))
                })
                .collect()
        })
        .unwrap_or_default();
    Some(json!({
        "public_key": o.get("pubKey")?,
        "scheme": o.get("scheme").cloned().unwrap_or(Value::Null),
        "address": o.get("addr").cloned().unwrap_or(Value::Null),
        "capabilities": caps,
    }))
}

fn status_and_result(out: &Value) -> (&'static str, Value) {
    let ok = out.pointer("/result/status").and_then(Value::as_str) == Some("success");
    let v = if ok { out.pointer("/result/data") } else { out.pointer("/result/error") };
    (if ok { "success" } else { "failure" }, v.cloned().unwrap_or(Value::Null))
}

/// Short, list-sized view of a tx.
pub fn summary(view: &View, r: &TxRecord, b: &BlockRecord) -> ApiResult<Value> {
    let out = shapes::output(view, r)?;
    let (status, _) = status_and_result(&out);
    let mut m = tx_context(r, b);
    m.insert("status".into(), status.into());
    m.insert("gas".into(), out.get("gas").cloned().unwrap_or(Value::Null));
    if r.key.is_coinbase() {
        m.insert("kind".into(), "coinbase".into());
        return Ok(Value::Object(m));
    }
    let cmd = shapes::cmd_value(r);
    let kind = match cmd.as_ref().and_then(|c| c.pointer("/payload")) {
        Some(p) if p.get("cont").is_some() => "cont",
        Some(_) => "exec",
        None => "unknown",
    };
    m.insert("kind".into(), kind.into());
    m.insert(
        "sender".into(),
        cmd.as_ref()
            .and_then(|c| c.pointer("/meta/sender"))
            .cloned()
            .unwrap_or(Value::Null),
    );
    if let Some(code) = cmd.as_ref().and_then(shapes::exec_code) {
        const CUT: usize = 256;
        let mut end = code.len().min(CUT);
        while !code.is_char_boundary(end) {
            end -= 1;
        }
        m.insert("code".into(), code[..end].into());
        m.insert("code_truncated".into(), (end < code.len()).into());
    }
    Ok(Value::Object(m))
}

/// Full tx (coinbase or user).
pub fn detail(view: &View, r: &TxRecord, b: &BlockRecord, proof: bool) -> ApiResult<Value> {
    let out = shapes::output(view, r)?;
    let (status, result) = status_and_result(&out);
    let evs = shapes::events(&out);
    let trs = shapes::transfers(&evs);
    let mut m: Map<String, Value> = tx_context(r, b);
    m.insert("status".into(), status.into());
    m.insert("result".into(), result);
    m.insert("gas".into(), out.get("gas").cloned().unwrap_or(Value::Null));
    m.insert("pact_tx_id".into(), out.get("txId").cloned().unwrap_or(Value::Null));
    m.insert("logs".into(), out.get("logs").cloned().unwrap_or(Value::Null));
    m.insert("metadata".into(), out.get("metaData").cloned().unwrap_or(Value::Null));
    m.insert("continuation".into(), out.get("continuation").cloned().unwrap_or(Value::Null));
    m.insert("events".into(), evs.iter().map(event_json).collect::<Vec<_>>().into());
    m.insert(
        "transfers".into(),
        trs.iter().map(|t| transfer_json(r, b, &evs, t)).collect::<Vec<_>>().into(),
    );
    if r.key.is_coinbase() {
        m.insert("kind".into(), "coinbase".into());
        m.insert("command".into(), Value::Null);
        m.insert("sigs".into(), Value::Array(vec![]));
        return Ok(Value::Object(m));
    }
    let cmd = shapes::cmd_value(r);
    let meta = cmd.as_ref().and_then(|c| c.get("meta")).cloned().unwrap_or(Value::Null);
    let payload = cmd.as_ref().and_then(|c| c.get("payload")).cloned().unwrap_or(Value::Null);
    let mut c = Map::new();
    if let Some(cont) = payload.get("cont") {
        m.insert("kind".into(), "cont".into());
        c.insert("pact_id".into(), cont.get("pactId").cloned().unwrap_or(Value::Null));
        c.insert("step".into(), cont.get("step").cloned().unwrap_or(Value::Null));
        c.insert("rollback".into(), cont.get("rollback").cloned().unwrap_or(Value::Null));
        c.insert("data".into(), cont.get("data").cloned().unwrap_or(Value::Null));
        if proof {
            c.insert("proof".into(), cont.get("proof").cloned().unwrap_or(Value::Null));
        } else {
            c.insert("has_proof".into(), cont.get("proof").is_some_and(|p| !p.is_null()).into());
        }
    } else {
        m.insert("kind".into(), (if payload.get("exec").is_some() { "exec" } else { "unknown" }).into());
        let e = payload.get("exec");
        c.insert("code".into(), e.and_then(|e| e.get("code")).cloned().unwrap_or(Value::Null));
        c.insert("data".into(), e.and_then(|e| e.get("data")).cloned().unwrap_or(Value::Null));
    }
    c.insert("nonce".into(), cmd.as_ref().and_then(|c| c.get("nonce")).cloned().unwrap_or(Value::Null));
    c.insert("network_id".into(), cmd.as_ref().and_then(|c| c.get("networkId")).cloned().unwrap_or(Value::Null));
    c.insert("sender".into(), meta.get("sender").cloned().unwrap_or(Value::Null));
    c.insert("gas_limit".into(), meta.get("gasLimit").cloned().unwrap_or(Value::Null));
    c.insert("gas_price".into(), meta.get("gasPrice").cloned().unwrap_or(Value::Null));
    c.insert("ttl".into(), meta.get("ttl").cloned().unwrap_or(Value::Null));
    c.insert("creation_time".into(), meta.get("creationTime").cloned().unwrap_or(Value::Null));
    c.insert(
        "signers".into(),
        cmd.as_ref()
            .and_then(|c| c.get("signers"))
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(signer_json).collect::<Vec<_>>())
            .unwrap_or_default()
            .into(),
    );
    m.insert("command".into(), Value::Object(c));
    let sigs: Vec<Value> = kdx_decode::render::render_tx(r, &view.dict())
        .ok()
        .flatten()
        .and_then(|t| t.get("sigs").and_then(Value::as_array).cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|s| s.get("sig").cloned())
        .collect();
    m.insert("sigs".into(), sigs.into());
    Ok(Value::Object(m))
}

/// Full tx by request key, with its block.
#[utoipa::path(
    get,
    path = "/api/v1/txs/{request_key}",
    params(("request_key" = String, Path, description = "Request key (43-char base64url)"), TxQuery),
    responses(
        (status = 200, description = "Tx: command, signers with capabilities, sigs, result, events, transfers, continuation, block info"),
        (status = 400, description = "Not a request key"),
        (status = 404, description = "Unknown request key"),
    )
)]
pub async fn get_tx(
    st: St,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    q: Result<Query<TxQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let (rk, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    let proof = match q.include.as_deref() {
        None => false,
        Some("proof") => true,
        Some(_) => return ApiError::BadRequest("include must be proof".into()).into_response(),
    };
    run(st.0, move |view| {
        let rk = parse_hash(&rk, "request key")?;
        let (k, r) = view
            .tx_by_rk(&rk)?
            .ok_or_else(|| ApiError::NotFound("unknown request key".into()))?;
        let b = view
            .block(k.chain(), k.height())?
            .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("tx without its block")))?;
        let d = detail(view, &r, &b, proof)?;
        resource(view, d)
    })
    .await
}
