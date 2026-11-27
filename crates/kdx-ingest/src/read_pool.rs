//! The balance reconciler's read nodes (`balances.local_urls`): `details` reads spread over
//! several chainweb nodes, each with its own `/local` rate and concurrency caps
//! (`balances.local_rps` ≤ `MAX_LOCAL_RPS` per node, `balances.local_concurrency`), with
//! health and failover: a node that gives a transport or HTTP error, or answers from a tip
//! more than [`LAG_BLOCKS`] below our tail, is skipped for [`COOLDOWN`].
//!
//! Exactness does not depend on which node answered: every answer still goes through the
//! reconciler's branch check against our own store (`branch_of`), so a node on another fork
//! gives `other_branch` and a retry, never a stored value. An `Absent` answer carries no block
//! identity, so one from a node other than the primary (`node.url`) is asked again of the
//! primary before it counts (see `reconcile::read_group`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kdx_node::local::LocalClient;

/// How long a node that failed or lagged is skipped.
pub const COOLDOWN: Duration = Duration::from_secs(30);
/// An answer read more than this many blocks below our tail on its chain marks its node as
/// lagging (it is still exact, only old: the call is tried on another node first).
pub const LAG_BLOCKS: u32 = 10;

/// One read node.
pub struct ReadNode {
    /// Metrics and log label: `n0`, `n1`, … in config order (never the URL).
    pub label: String,
    pub client: Arc<LocalClient>,
    /// The primary node (`node.url`, the node the tail follows).
    pub primary: bool,
    down_until: Mutex<Option<Instant>>,
}

impl ReadNode {
    pub fn new(label: String, client: Arc<LocalClient>, primary: bool) -> ReadNode {
        kdx_metrics::get().balance_local_node_up(&label, true);
        ReadNode {
            label,
            client,
            primary,
            down_until: Mutex::new(None),
        }
    }

    fn down_until(&self) -> Option<Instant> {
        *self.down_until.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn is_up(&self, now: Instant) -> bool {
        self.down_until().is_none_or(|t| now >= t)
    }
}

/// The read nodes and the batched-read size.
pub struct ReadPool {
    nodes: Vec<ReadNode>,
    /// Accounts per batched `details` call (`balances.local_batch_keys`; 1 = single reads).
    pub batch_keys: usize,
    next: AtomicUsize,
}

impl ReadPool {
    /// `nodes` must not be empty.
    pub fn new(nodes: Vec<ReadNode>, batch_keys: usize) -> ReadPool {
        assert!(!nodes.is_empty(), "a read pool needs a node");
        ReadPool {
            nodes,
            batch_keys: batch_keys.clamp(1, kdx_node::local::MAX_BATCH_KEYS),
            next: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn node(&self, i: usize) -> &ReadNode {
        &self.nodes[i]
    }

    /// The next node for a call, round robin over the nodes that are up and not in `tried`.
    /// When every untried node is down, the one that comes back first (a call must still go
    /// somewhere: a lone node that failed is retried, as the single-node reconciler does).
    /// `None` once every node was tried.
    pub fn pick(&self, tried: &[usize]) -> Option<usize> {
        let n = self.nodes.len();
        let now = Instant::now();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let untried = (0..n)
            .map(|k| (start + k) % n)
            .filter(|i| !tried.contains(i));
        untried
            .clone()
            .find(|&i| self.nodes[i].is_up(now))
            .or_else(|| untried.min_by_key(|&i| self.nodes[i].down_until()))
    }

    /// Skip node `i` for [`COOLDOWN`].
    pub fn mark_down(&self, i: usize, why: &'static str) {
        let node = &self.nodes[i];
        let mut g = node.down_until.lock().unwrap_or_else(|e| e.into_inner());
        let was_up = g.is_none_or(|t| Instant::now() >= t);
        *g = Some(Instant::now() + COOLDOWN);
        drop(g);
        if was_up && self.nodes.len() > 1 {
            tracing::warn!(
                node = %node.label,
                reason = why,
                cooldown_secs = COOLDOWN.as_secs(),
                "balances: read node skipped for a while"
            );
        }
        kdx_metrics::get().balance_local_node_up(&node.label, false);
    }

    /// Node `i` answered: it is up again.
    pub fn mark_up(&self, i: usize) {
        let node = &self.nodes[i];
        let mut g = node.down_until.lock().unwrap_or_else(|e| e.into_inner());
        if g.take().is_some() {
            kdx_metrics::get().balance_local_node_up(&node.label, true);
        }
    }

    /// Index of the primary node, if it is in the pool.
    pub fn primary(&self) -> Option<usize> {
        self.nodes.iter().position(|n| n.primary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kdx_node::local::LocalOpts;
    use kdx_node::{NodeClient, NodeClientConfig};

    fn client() -> Arc<LocalClient> {
        let node = NodeClient::new(&NodeClientConfig {
            base: "http://127.0.0.1:9".parse().unwrap(),
            network: "mainnet01".into(),
            timeout: Duration::from_secs(1),
            max_rps: 0,
            concurrency: 1,
            p2p_url: None,
            p2p_cert_sha256: None,
            max_body_bytes: 1 << 20,
            max_sse_event_bytes: 1 << 20,
        })
        .unwrap();
        Arc::new(LocalClient::new(
            node,
            LocalOpts {
                rps: 0,
                concurrency: 1,
            },
        ))
    }

    fn pool(n: usize) -> ReadPool {
        ReadPool::new(
            (0..n)
                .map(|i| ReadNode::new(format!("t{i}"), client(), i == 0))
                .collect(),
            25,
        )
    }

    #[test]
    fn picks_round_robin_and_skips_down_nodes() {
        let p = pool(3);
        let mut seen: Vec<usize> = (0..6).filter_map(|_| p.pick(&[])).collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 0, 1, 1, 2, 2], "calls spread evenly");
        p.mark_down(1, "test");
        for _ in 0..10 {
            assert_ne!(p.pick(&[]), Some(1), "a down node is skipped");
        }
        p.mark_up(1);
        assert!(
            (0..10).any(|_| p.pick(&[]) == Some(1)),
            "back after an answer"
        );
    }

    #[test]
    fn failover_tries_every_node_once_then_stops() {
        let p = pool(3);
        let mut tried = vec![];
        while let Some(i) = p.pick(&tried) {
            assert!(!tried.contains(&i));
            tried.push(i);
        }
        tried.sort_unstable();
        assert_eq!(tried, vec![0, 1, 2]);
    }

    #[test]
    fn an_all_down_pool_still_answers_with_the_node_back_first() {
        let p = pool(2);
        p.mark_down(0, "test");
        std::thread::sleep(Duration::from_millis(5));
        p.mark_down(1, "test");
        assert_eq!(p.pick(&[]), Some(0), "the node that comes back first");
        assert_eq!(p.pick(&[0]), Some(1));
        assert_eq!(p.pick(&[0, 1]), None);
    }

    #[test]
    fn batch_keys_are_clamped() {
        let p = ReadPool::new(vec![ReadNode::new("x".into(), client(), true)], 10_000);
        assert_eq!(p.batch_keys, kdx_node::local::MAX_BATCH_KEYS);
        assert_eq!(p.primary(), Some(0));
    }
}
