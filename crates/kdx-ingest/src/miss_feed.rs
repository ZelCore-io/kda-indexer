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
                Ok(Err(e)) if crate::ctx::stop_on_reload_failure(&e, &stop) => break,
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kdx_hot::{Geometry, HotDict, HotStore, Table};
    use kdx_types::Hash32;

    use super::*;

    /// F-52 (M-5): the gap Q feed, a real background writer, stops itself and the process
    /// after a failed dictionary reload, and the failure is recorded so the command fails.
    #[tokio::test]
    async fn the_feed_stops_the_process_after_a_failed_reload() {
        let dir = tempfile::tempdir().unwrap();
        let hot = Arc::new(HotStore::open(dir.path(), Geometry::default()).unwrap());
        std::fs::create_dir_all(dir.path().join("segments")).unwrap();
        let mut txn = hot.rw().unwrap();
        // The marks need a tail (`check_ready`).
        kdx_hot::set_checkpoint(&mut txn, kdx_hot::Stage::Tail, 0, 1, Hash32([1; 32])).unwrap();
        // A malformed `Tokens` row of module 0: the marks' write txn fails.
        txn.put(Table::Tokens, &0u32.to_be_bytes(), b"bad").unwrap();
        txn.commit().unwrap();
        let (ctx, _) = crate::reconcile::tests::client_on(
            dir.path(),
            &hot,
            Arc::new(Mutex::new(String::new())),
        );
        let mut d = HotDict::default();
        d.intern(DictKind::Module, "coin");
        *ctx.dict.lock().unwrap() = d;
        // Account ids that are not dense: the reload after the failed txn fails too.
        let mut txn = hot.rw().unwrap();
        txn.put(
            Table::DictRev(DictKind::Account),
            &7u32.to_be_bytes(),
            b"k:x",
        )
        .unwrap();
        txn.commit().unwrap();

        let ctx = Arc::new(ctx);
        let stop = CancellationToken::new();
        let (tx, rx) = channel();
        spawn(ctx.clone(), rx, stop.clone());
        let account = format!("k:{}", "ab".repeat(32));
        tx.send((account, "coin".into(), 0, 1)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), stop.cancelled())
            .await
            .expect("a failed reload stops the process");
        assert!(
            crate::ctx::reload_failure().is_some(),
            "the failure is recorded"
        );
        assert!(
            ctx.dict.lock().unwrap().is_poisoned(),
            "the drained dictionary can no longer be persisted"
        );
        // The feed task ended: its receiver is gone.
        tokio::time::timeout(Duration::from_secs(5), tx.closed())
            .await
            .expect("the feed stopped");
    }
}
