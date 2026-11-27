//! The balance reconciler's read nodes (`balances.local_urls`): `details` reads spread over
//! several chainweb nodes, each with its own `/local` rate and concurrency caps
//! (`balances.local_rps` ≤ `MAX_LOCAL_RPS` per node, `balances.local_concurrency`), with
//! health and failover: a node is skipped for [`COOLDOWN`] when it gives a transport or HTTP
//! error, answers from a tip more than [`LAG_BLOCKS`] below or above our tail, or (not the
//! primary) does not know a module the primary knows, answers from another branch or names no
//! block hash. A Pact failure alone skips no node (it may be the contract's own).
//!
//! Trust: every read node is trusted for state exactly like `node.url`. The reconciler's
//! branch check (`branch_of`) binds only the block an answer *claims* (its height and
//! `prevBlockHash` against our stored header); `/local` carries no state proof. The check
//! catches a node on another fork or behind, not a node that lies about a balance or guard
//! under a real block hash. List only nodes you run.
//!
//! What a non-primary node cannot decide is asked of the primary (`node.url`): an `Absent`
//! answer (it carries no block identity), a Pact failure or an unusable answer (a lagging
//! node may not know a new module yet), and every `try` marker of a batched read (see
//! `reconcile::read_chunk`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kdx_node::local::LocalClient;

/// How long a node that failed, lagged or ran ahead is skipped.
pub const COOLDOWN: Duration = Duration::from_secs(30);
/// An answer read more than this many blocks below our tail on its chain marks its node as
/// lagging (it is still exact, only old: the call is tried on another node first); more than
/// this many above marks it as ahead (its answers cannot be branch-checked yet).
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

    /// The next node for a call, round robin over the nodes that are up and not in `tried`,
    /// preferring one with a free concurrency slot (a slow node then gets fewer calls).
    /// When every untried node is down, the one that comes back first (a call must still go
    /// somewhere: a lone node that failed is retried, as the single-node reconciler does).
    /// `None` once every node was tried.
    pub fn pick(&self, tried: &[usize]) -> Option<usize> {
        let n = self.nodes.len();
        let now = Instant::now();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let untried: Vec<usize> = (0..n)
            .map(|k| (start + k) % n)
            .filter(|i| !tried.contains(i))
            .collect();
        let mut up = untried
            .iter()
            .copied()
            .filter(|&i| self.nodes[i].is_up(now));
        up.clone()
            .find(|&i| self.nodes[i].client.free_slots() > 0)
            .or_else(|| up.next())
            .or_else(|| {
                untried
                    .iter()
                    .copied()
                    .min_by_key(|&i| self.nodes[i].down_until())
            })
    }

    /// [`Self::pick`] for a batched `details` read: a node other than the primary that is up
    /// and not in `tried`, round robin, preferring one with a free concurrency slot (a slow
    /// node then gets fewer calls), else the first such node even with no free slot (the call
    /// waits for one there). The primary's `local_rps` is the budget of the reads only it
    /// decides (`try` markers, absent answers, fungible checks, keysets); a batch it took
    /// would hold up about 25 of those. With no such node, [`Self::pick`].
    pub fn pick_batch(&self, tried: &[usize]) -> Option<usize> {
        let n = self.nodes.len();
        let now = Instant::now();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut up = (0..n)
            .map(|k| (start + k) % n)
            .filter(|&i| !tried.contains(&i) && !self.nodes[i].primary && self.nodes[i].is_up(now));
        up.clone()
            .find(|&i| self.nodes[i].client.free_slots() > 0)
            .or_else(|| up.next())
            .or_else(|| self.pick(tried))
    }

    /// Whether node `i` is up (not skipped).
    pub fn is_up(&self, i: usize) -> bool {
        self.nodes[i].is_up(Instant::now())
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

    /// Batched reads go to the nodes other than the primary while one is up; the primary
    /// takes them when they are all down or tried, and a pool of the primary alone still reads.
    #[test]
    fn batched_reads_spare_the_primary() {
        let p = pool(3);
        let seen: Vec<usize> = (0..6).filter_map(|_| p.pick_batch(&[])).collect();
        assert!(seen.iter().all(|&i| i != 0), "{seen:?}");
        assert!(seen.contains(&1) && seen.contains(&2), "spread: {seen:?}");
        assert_eq!(p.pick_batch(&[1]), Some(2));
        assert_eq!(
            p.pick_batch(&[1, 2]),
            Some(0),
            "failover reaches the primary"
        );
        assert_eq!(p.pick_batch(&[0, 1, 2]), None);
        p.mark_down(1, "test");
        p.mark_down(2, "test");
        assert_eq!(p.pick_batch(&[]), Some(0), "the others down: the primary");
        assert_eq!(pool(1).pick_batch(&[]), Some(0));
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
