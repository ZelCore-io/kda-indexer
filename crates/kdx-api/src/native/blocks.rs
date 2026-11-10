//! Block routes: `/blocks`, `/blocks/{hash}`, `/chains/{c}/blocks/{height}`, `/blocks/{hash}/txs`.

use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use kdx_decode::render::{render_header, render_miner};
use kdx_hot::{Table, Txn};
use kdx_types::record::BlockRecord;
use kdx_types::{Dict, DictKind, TxKey};
use serde_json::{json, Map, Value};

use super::txs::{detail, summary};
use super::{
    list_params, list_response, parse_hash, parse_path, parse_query, resource, run, ListParams,
    Order, Q, St,
};
use crate::compat::shapes::utc_micros;
use crate::cursor::Cursor;
use crate::store::View;
use crate::{ApiError, ApiResult};

type QueryQ = Result<Query<Q>, axum::extract::rejection::QueryRejection>;

/// Most heights one `/blocks` request walks (a sparse `chains` filter can skip many).
const HEIGHT_WALK_CAP: u32 = 20_000;

/// User txs of a block (the coinbase is not counted).
fn user_tx_count(view: &View, chain: u8, height: u32) -> ApiResult<u64> {
    let n = if view.frozen(height) {
        view.seg.block_txnums(chain, height).map_or(0, |r| r.end - r.start)
    } else {
        let lo = TxKey::new(height, chain, 0).to_be_bytes();
        let hi = TxKey::new(height, chain, u16::MAX).to_be_bytes();
        view.txn.scan(Table::TailTxs, &lo, |k| k <= &hi[..])?.len() as u64
    };
    Ok(n.saturating_sub(1))
}

/// Block header, miner and counts.
pub fn block_json(view: &View, b: &BlockRecord, tips: &[(u8, u32, kdx_types::Hash32)]) -> ApiResult<Value> {
    let h = render_header(b);
    let mut m = Map::new();
    m.insert("hash".into(), b.hash.to_b64url().into());
    m.insert("chain".into(), u64::from(b.chain).into());
    m.insert("height".into(), b.height.into());
    m.insert("parent".into(), b.parent.to_b64url().into());
    m.insert("time".into(), utc_micros(b.creation_time).into());
    m.insert("creation_time".into(), b.creation_time.into());
    m.insert("adjacents".into(), h.get("adjacents").cloned().unwrap_or(Value::Null));
    for (k, v) in [("weight", "weight"), ("target", "target"), ("nonce", "nonce")] {
        m.insert(k.into(), h.get(v).cloned().unwrap_or(Value::Null));
    }
    m.insert("epoch_start".into(), b.epoch_start.into());
    m.insert("feature_flags".into(), b.feature_flags.into());
    m.insert("payload_hash".into(), b.payload_hash.to_b64url().into());
    m.insert("transactions_hash".into(), b.transactions_hash.to_b64url().into());
    m.insert("outputs_hash".into(), b.outputs_hash.to_b64url().into());
    m.insert("chainweb_version".into(), b.chainweb_version.clone().into());
    let d = view.dict();
    m.insert(
        "miner".into(),
        json!({
            "account": d.name(DictKind::Account, b.miner_account),
            "data": render_miner(b, &d).unwrap_or(Value::Null),
        }),
    );
    m.insert("tx_count".into(), user_tx_count(view, b.chain, b.height as u32)?.into());
    let tip = tips.iter().find(|t| u64::from(t.0) == u64::from(b.chain)).map(|t| t.1);
    m.insert(
        "confirmations".into(),
        tip.map_or(Value::Null, |t| (u64::from(t).saturating_sub(b.height) + 1).into()),
    );
    m.insert("merkle_verified".into(), b.merkle_verified.into());
    Ok(Value::Object(m))
}

fn block_with_coinbase(view: &View, chain: u8, height: u32, b: &BlockRecord) -> ApiResult<Response> {
    let tips = view.chain_tips()?;
    let mut v = block_json(view, b, &tips)?;
    let cb = view
        .tx(TxKey::new(height, chain, 0))?
        .map(|r| detail(view, &r, b, false))
        .transpose()?;
    if let Value::Object(m) = &mut v {
        m.insert("coinbase".into(), cb.unwrap_or(Value::Null));
    }
    resource(view, v)
}

/// Block by hash: header, miner, coinbase, tx count, confirmations.
#[utoipa::path(
    get,
    path = "/api/v1/blocks/{hash}",
    params(("hash" = String, Path, description = "Block hash (43-char base64url)")),
    responses((status = 200, description = "Block"), (status = 404, description = "Unknown block"))
)]
pub async fn by_hash(st: St, path: Result<Path<String>, axum::extract::rejection::PathRejection>) -> Response {
    let hash = match parse_path(path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| {
        let h = parse_hash(&hash, "block hash")?;
        let (c, ht, b) = view
            .block_by_hash(&h)?
            .ok_or_else(|| ApiError::NotFound("unknown block hash".into()))?;
        block_with_coinbase(view, c, ht, &b)
    })
    .await
}

/// Block at a height of a chain (the canonical one).
#[utoipa::path(
    get,
    path = "/api/v1/chains/{chain}/blocks/{height}",
    params(("chain" = u8, Path, description = "Chain 0..19"), ("height" = u32, Path, description = "Height")),
    responses((status = 200, description = "Block"), (status = 404, description = "No such block"))
)]
pub async fn by_height(st: St, path: Result<Path<(u8, u32)>, axum::extract::rejection::PathRejection>) -> Response {
    let (chain, height) = match parse_path(path) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| {
        if u32::from(chain) >= kdx_segment::SLOT_WIDTH {
            return Err(ApiError::BadRequest("chain out of range".into()));
        }
        let b = view
            .block(chain, height)?
            .ok_or_else(|| ApiError::NotFound("no block at that height".into()))?;
        block_with_coinbase(view, chain, height, &b)
    })
    .await
}

/// User txs of a block in payload order (`order=desc` reverses). The cursor is the tx position.
#[utoipa::path(
    get,
    path = "/api/v1/blocks/{hash}/txs",
    params(("hash" = String, Path, description = "Block hash"), ListParams),
    responses((status = 200, description = "`items` of tx summaries, `next_cursor`, `as_of`"))
)]
pub async fn txs(
    st: St,
    path: Result<Path<String>, axum::extract::rejection::PathRejection>,
    q: QueryQ,
) -> Response {
    let (hash, q) = match (parse_path(path), parse_query(q)) {
        (Ok(p), Ok(q)) => (p, q),
        (Err(e), _) | (_, Err(e)) => return e.into_response(),
    };
    run(st.0, move |view| {
        let mut lp = list_params(&q)?;
        // Block txs are listed in payload order unless asked otherwise.
        if q.order.is_none() {
            lp.order = Order::Asc;
        }
        let h = parse_hash(&hash, "block hash")?;
        let (c, ht, b) = view
            .block_by_hash(&h)?
            .ok_or_else(|| ApiError::NotFound("unknown block hash".into()))?;
        if let Some(cur) = &lp.cursor {
            cur.check(view)?;
        }
        let mut recs: Vec<_> = view.block_txs(c, ht)?.into_iter().filter(|r| !r.key.is_coinbase()).collect();
        if lp.order == Order::Desc {
            recs.reverse();
        }
        let skip = match &lp.cursor {
            None => 0,
            Some(cur) => recs
                .iter()
                .position(|r| match lp.order {
                    Order::Asc => r.key > cur.key,
                    Order::Desc => r.key < cur.key,
                })
                .unwrap_or(recs.len()),
        };
        let mut items = Vec::new();
        let mut next = None;
        for r in recs.iter().skip(skip) {
            if items.len() == lp.limit {
                let last = recs.iter().skip(skip).nth(lp.limit - 1).map(|r| r.key);
                next = last.map(|k| Cursor::new(view, k, 0).map(|c| c.encode())).transpose()?;
                break;
            }
            items.push(summary(view, r, &b)?);
        }
        list_response(view, items, next)
    })
    .await
}

/// Blocks by height window, newest first, ordered by (height, chain). `chains=0,3` limits the
/// chains; `minheight`/`maxheight` bound the window (default: the newest heights).
#[utoipa::path(
    get,
    path = "/api/v1/blocks",
    params(
        ListParams,
        ("chains" = Option<String>, Query, description = "Comma-separated chain ids"),
    ),
    responses((status = 200, description = "`items` of blocks (no coinbase), `next_cursor`, `as_of`"))
)]
pub async fn list(st: St, q: QueryQ) -> Response {
    let q = match parse_query(q) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    run(st.0, move |view| {
        let lp = list_params(&q)?;
        let mut chains: Vec<u8> = Vec::new();
        match (&q.chains, lp.chain) {
            (Some(s), _) => {
                for p in s.split(',') {
                    let c: u8 = p.trim().parse().map_err(|_| ApiError::BadRequest("invalid chains".into()))?;
                    if u32::from(c) >= kdx_segment::SLOT_WIDTH {
                        return Err(ApiError::BadRequest("chain out of range".into()));
                    }
                    chains.push(c);
                }
                chains.sort_unstable();
                chains.dedup();
            }
            (None, Some(c)) => chains.push(c),
            (None, None) => chains.extend(0..kdx_segment::SLOT_WIDTH as u8),
        }
        let tips = view.chain_tips()?;
        let top = view.max_indexed_height()?.unwrap_or(0);
        let hi = lp.max_height.unwrap_or(top).min(top);
        let lo = lp.min_height.unwrap_or(0);
        if let Some(cur) = &lp.cursor {
            cur.check(view)?;
        }
        // Walk (height, chain) positions in order, from the cursor.
        let desc = lp.order == Order::Desc;
        let start = match (&lp.cursor, desc) {
            (Some(c), true) => (c.key.height(), i32::from(c.key.chain()) - 1),
            (Some(c), false) => (c.key.height(), i32::from(c.key.chain()) + 1),
            (None, true) => (hi, i32::from(kdx_segment::SLOT_WIDTH as u8)),
            (None, false) => (lo, -1),
        };
        let mut items = Vec::new();
        let mut next = None;
        let (mut h, mut ci) = start;
        let mut walked = 0u32;
        let mut last: Option<(u32, u8)> = None;
        'walk: loop {
            if (desc && h < lo) || (!desc && h > hi) {
                break;
            }
            let cs: Box<dyn Iterator<Item = u8>> = if desc {
                Box::new(chains.iter().rev().copied())
            } else {
                Box::new(chains.iter().copied())
            };
            for c in cs {
                let ok = if desc { i32::from(c) < ci } else { i32::from(c) > ci };
                if !ok {
                    continue;
                }
                let Some(b) = view.block(c, h)? else { continue };
                if items.len() == lp.limit {
                    next = last.map(|(h, c)| Cursor::new(view, TxKey::new(h, c, 0), 0).map(|c| c.encode())).transpose()?;
                    break 'walk;
                }
                items.push(block_json(view, &b, &tips)?);
                last = Some((h, c));
            }
            walked += 1;
            if walked >= HEIGHT_WALK_CAP {
                if next.is_none() {
                    // Continue where the walk stopped (a sparse filter).
                    let key = TxKey::new(h, if desc { 0 } else { kdx_segment::SLOT_WIDTH as u8 - 1 }, 0);
                    next = Some(Cursor::new(view, key, 0)?.encode());
                    if desc && h == lo || !desc && h == hi {
                        next = None;
                    }
                }
                break;
            }
            ci = if desc { i32::from(kdx_segment::SLOT_WIDTH as u8) } else { -1 };
            if desc {
                if h == 0 {
                    break;
                }
                h -= 1;
            } else {
                h += 1;
            }
        }
        list_response(view, items, next)
    })
    .await
}
