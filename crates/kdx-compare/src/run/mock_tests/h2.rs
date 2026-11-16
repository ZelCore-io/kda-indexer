//! FVH-H part 2 (R-P5P9-97, R-P5P9-100): the CG-5 re-referee's leftovers M #1997 / T #2005,
//! W #567, T #332, U, Q #37 and N #178. Each case is the record's shape, live and stored, and
//! each is RED without its fix.

use super::*;
use crate::report::{DiffEntry, DiffKind};

/// A referee config over `node`, with ours and the oracle unreachable (stored bodies only).
async fn h2_cfg(node: NodeMock, dir: &std::path::Path) -> CompareConfig {
    let nu = node_server(node).await;
    let ops = c3_ops(dir);
    c3_cfg(
        &nu,
        "http://127.0.0.1:9/graphql",
        "http://127.0.0.1:9/graphql",
        dir,
        &ops,
        "",
    )
}

/// A stored QH7 record over `transfers` pages `a` (ours) and `b` (the oracle).
fn h2_rec(a: Vec<Value>, b: Vec<Value>, diff: Vec<DiffEntry>) -> crate::report::DiffRecord {
    let page = |edges: Vec<Value>| {
        json!({"data": {"transfers": {"totalCount": 40,
            "pageInfo": {"hasNextPage": false}, "edges": edges}}})
    };
    e2_stored("QH7", json!({"first": 20}), page(a), page(b), diff)
}

fn h2_missing(path: &str, b: Value) -> DiffEntry {
    DiffEntry {
        path: path.into(),
        kind: DiffKind::Missing,
        a: None,
        b: Some(b),
    }
}

// ---------------------------------------------------------------------------------------------
// M #1997 / T #2005

/// The node's `/poll` for M #1997's shape: x0 and m in block B100 at 100, or m at `mh`.
fn m_node(mh: u64) -> NodeMock {
    let mut node = NodeMock::default();
    let (k, v) = h7_xfer_poll("x0", "B100", 100, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    let (k, v) = h7_xfer_poll("m", &format!("B{mh}"), mh, "s", "r", json!(1.0));
    node.polls.insert(k, v);
    node
}

/// M #1997's rows: ours lists m once at 100 (no block hash, only a height); the oracle lists
/// it there too and again at 99 with another receiver (its orphan copy).
fn m_rows() -> (Vec<Value>, Vec<Value>) {
    let x0 = h7_xrow("x0", 100, json!(1.0), Some(("s", "r")));
    let m = h7_xrow("m", 100, json!(1.0), Some(("s", "r")));
    let orphan = h7_xrow("m", 99, json!(1.0), Some(("s", "r2")));
    (vec![x0.clone(), m.clone()], vec![x0, m, orphan])
}

/// R-P5P9-97 M #1997 (and T #2005): ours' row selects no block hash, but it lists the key at
/// the node's height on the node's chain. The oracle's height-only copy at another height is
/// an orphan by height. When the node places the key at another height than ours' row, the
/// copy stays a diff.
#[tokio::test]
async fn h2_m_ours_listed_at_the_nodes_height_without_a_hash() {
    let (a, b) = m_rows();
    for (mh, explained) in [(100, true), (101, false)] {
        let dir = tempfile::tempdir().unwrap();
        let ctx = gql_ctx(m_node(mh), dir.path()).await;
        let (d, note) = d1_settle(&ctx, a.clone(), b.clone(), (false, false)).await;
        assert_eq!(
            only_hits(&d) && !d.is_empty(),
            explained,
            "live {mh}: {d:?} {note:?}"
        );
        if explained {
            assert!(format!("{d:?}").contains("orphan by height"), "{d:?}");
        }
        // Stored: the same record from the diff log, refereed.
        let cfg = h2_cfg(m_node(mh), dir.path()).await;
        let rec = h2_rec(
            a.clone(),
            b.clone(),
            vec![h2_missing("/data/transfers/edges/2", b[2].clone())],
        );
        let rep = e2_referee(cfg, dir.path(), &[rec]).await;
        let want = if explained { (1, 0) } else { (0, 1) };
        assert_eq!(
            (rep.explained, rep.unexplained),
            want,
            "stored {mh}: {rep:?}"
        );
    }
}
