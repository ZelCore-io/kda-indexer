//! Gap Q feed-back: a row the GraphQL miss read found on the node, and the store has not, is
//! marked dirty here so the reconciler reads and stores it (`kdx_graphql::miss`).
//!
//! - Only in `run` (the writer process); `serve` is read-only and drops them.
//! - The module must already be in the dictionary (the resolver reads only modules the store
//!   serves); the account is interned, under the shared dictionary lock taken before the MDBX
//!   writer (the seed pass's lock order), then marked through `balances::mark_row` at the
//!   node's read height.
//! - Refused, like the seed pass, while the backfill is incomplete
//!   (`seed_receivers::check_ready`): a backfill resume truncates names interned after its
//!   frontier.
//! - Bounded: a queue of [`QUEUE`] rows (a full queue drops: the next lookup of the row
//!   reads the node again), written in batches of at most [`BATCH`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use kdx_hot::balances;
use kdx_types::{Dict, DictKind, DictMut};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ctx::Ctx;

/// Rows waiting at most.
pub const QUEUE: usize = 4_096;
/// Rows per write txn at most.
pub const BATCH: usize = 256;

/// `(account, module, chain, node height)`.
pub type Found = (String, String, u8, u32);

/// The bounded queue: the sender side never blocks ([`mpsc::Sender::try_send`]).
pub fn channel() -> (mpsc::Sender<Found>, mpsc::Receiver<Found>) {
    mpsc::channel(QUEUE)
}

/// Drain `rx` into dirty marks until `stop`.
pub fn spawn(ctx: Arc<Ctx>, mut rx: mpsc::Receiver<Found>, stop: CancellationToken) {
    tokio::spawn(async move {
        loop {
            let mut batch = Vec::with_capacity(BATCH);
            tokio::select! {
                _ = stop.cancelled() => break,
                n = rx.recv_many(&mut batch, BATCH) => if n == 0 { break },
            }
            let c = ctx.clone();
            match tokio::task::spawn_blocking(move || mark(&c, &batch)).await {
                Ok(Ok(n)) => tracing::debug!(marked = n, "balances: gap Q node-found rows marked"),
                Ok(Err(e)) => {
                    tracing::warn!(error = format!("{e:#}"), "balances: gap Q marks failed")
                }
                Err(e) => tracing::warn!(error = %e, "balances: gap Q marks task failed"),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
}

/// Mark `rows` dirty; how many were marked.
pub fn mark(ctx: &Ctx, rows: &[Found]) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    if crate::seed_receivers::check_ready(&ctx.hot.ro()?, &ctx.segdir).is_err() {
        return Ok(0);
    }
    let mut d = ctx.dict.lock().unwrap_or_else(|e| e.into_inner());
    let r = (|| -> Result<usize> {
        let mut txn = ctx.hot.rw()?;
        let coin = balances::coin_module(&txn)?;
        let mut n = 0;
        for (a, m, c, h) in rows {
            let Some(module) = d.mem().find(DictKind::Module, m) else {
                continue;
            };
            if !kdx_decode::receivers::account_like(a) {
                continue;
            }
            let account = d.intern(DictKind::Account, a);
            if balances::mark_row(&mut txn, account, module, *c, *h, coin)? {
                n += 1;
            }
        }
        d.persist(&mut txn)?;
        txn.commit()?;
        Ok(n)
    })();
    if r.is_err() {
        // Names interned here never reached MDBX: forget them (the seed pass's recovery).
        ctx.reload_dict(&mut d)?;
    }
    r
}
