//! `GET /api/v1/stats`: exact counts. Circulating supply joins with the compat `/stats` work
//! (Task 6); until then the field is absent.
//!
//! Counts come from the frozen manifest plus the hot rows. Per-chain counts of the frozen
//! ranges are summed in the background (ranges are immutable, so each is counted once) and appear
//! when ready; hot-window per-chain counts are not kept.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::response::Response;
use kdx_hot::{Table, Txn};
use kdx_segment::{RangeReader, Segments, SLOT_WIDTH};
use serde_json::{json, Value};

use super::{resource, run, St};
use crate::store::View;
use crate::ApiResult;

type PerChain = [(u64, u64); SLOT_WIDTH as usize];

struct Cache {
    done: Mutex<HashMap<(u32, u32, u64), PerChain>>,
    running: AtomicBool,
}

fn cache() -> &'static Cache {
    static C: OnceLock<Cache> = OnceLock::new();
    C.get_or_init(|| Cache {
        done: Mutex::new(HashMap::new()),
        running: AtomicBool::new(false),
    })
}

/// (records, blocks) per chain of one range.
fn count_range(r: &RangeReader) -> PerChain {
    let mut out: PerChain = Default::default();
    for s in 0..r.slots() {
        let n = r.keys.count(s);
        if n > 0 {
            let c = s % SLOT_WIDTH as usize;
            out[c].0 += n;
            out[c].1 += 1;
        }
    }
    out
}

fn key_of(r: &RangeReader) -> (u32, u32, u64) {
    (r.info.start, r.info.end, r.info.first_txnum)
}

/// Sum of the counted ranges; starts the background count when some range is missing.
fn frozen_per_chain(seg: &Arc<Segments>) -> Option<PerChain> {
    let c = cache();
    let missing = {
        let d = c.done.lock().unwrap_or_else(|e| e.into_inner());
        seg.ranges.iter().any(|r| !d.contains_key(&key_of(r)))
    };
    if missing {
        if !c.running.swap(true, Ordering::AcqRel) {
            let seg = seg.clone();
            std::thread::spawn(move || {
                for r in &seg.ranges {
                    let k = key_of(r);
                    if cache()
                        .done
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .contains_key(&k)
                    {
                        continue;
                    }
                    let v = count_range(r);
                    cache()
                        .done
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(k, v);
                }
                cache().running.store(false, Ordering::Release);
            });
        }
        return None;
    }
    let d = c.done.lock().unwrap_or_else(|e| e.into_inner());
    let mut out: PerChain = Default::default();
    for r in &seg.ranges {
        for (o, v) in out.iter_mut().zip(d[&key_of(r)].iter()) {
            o.0 += v.0;
            o.1 += v.1;
        }
    }
    Some(out)
}

pub fn totals(view: &View) -> ApiResult<(u64, u64)> {
    let m = &view.seg.manifest;
    let frozen_recs: u64 = m.entries.iter().map(|e| e.tx_count).sum();
    let frozen_blocks: u64 = m.entries.iter().map(|e| e.block_count).sum();
    // Hot rows already frozen (the gap between the manifest and the hot delete).
    let (gap_recs, gap_blocks) = m
        .entries
        .iter()
        .filter(|e| e.start >= view.hot_frontier)
        .fold((0u64, 0u64), |a, e| (a.0 + e.tx_count, a.1 + e.block_count));
    let hot_recs = (view.txn.count(Table::TailTxs)? as u64).saturating_sub(gap_recs);
    let hot_blocks = (view.txn.count(Table::Headers)? as u64).saturating_sub(gap_blocks);
    let records = frozen_recs + hot_recs;
    let blocks = frozen_blocks + hot_blocks;
    // Every block holds exactly one coinbase record.
    Ok((records.saturating_sub(blocks), blocks))
}

/// Exact user-tx and block counts, per-chain frozen counts and the indexed tips.
#[utoipa::path(
    get,
    path = "/api/v1/stats",
    responses((status = 200, description = "Counts and per-chain frozen counts"))
)]
pub async fn stats(st: St) -> Response {
    run(st.0, |view| {
        let (txs, blocks) = totals(view)?;
        let per = frozen_per_chain(&view.seg);
        let tips = view.chain_tips()?;
        let chains: Vec<Value> = tips
            .iter()
            .map(|(c, h, _)| {
                let mut v = json!({"chain": c, "tip_height": h});
                if let Some(p) = &per {
                    let (recs, bl) = p[usize::from(*c)];
                    v["frozen_blocks"] = bl.into();
                    v["frozen_user_txs"] = recs.saturating_sub(bl).into();
                }
                v
            })
            .collect();
        resource(
            view,
            json!({
                "user_tx_count": txs,
                "block_count": blocks,
                "per_chain_ready": per.is_some(),
                "chains": chains,
            }),
        )
    })
    .await
}
