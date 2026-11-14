//! `Block` and the block lists (Task 3): `block`, `blocksFromHeight`, `blocksFromDepth`,
//! `completedBlockHeights`, `lastBlockHeight`. Header-derived fields come from the stored
//! `BlockRecord` (GQ-12): `powHash` is recomputed from the binary header.

use std::sync::Arc;

use async_graphql::{Context, Object, Result, SimpleObject, ID};
use kdx_api::store::View;
use kdx_api::ApiResult;
use kdx_decode::header;
use kdx_types::dict::{Dict, DictKind};
use kdx_types::record::BlockRecord;

use super::{
    BlockEventsConnection, BlockEventsConnectionEdge, BlockTransactionsConnection,
    BlockTransactionsConnectionEdge, Event, FungibleChainAccount, Transaction,
};
use crate::connection::{finish, BlockCursor, Dir, Page, PageArgs, PageCursor, PageInfo};
use crate::error::{bad_input, err, from_api, Code};
use crate::gql;
use crate::ids::{encode, NodeKey};
use crate::scalars::{BigInt, DateTime, Decimal};
use crate::txlist::{block_list, chain_arg, hash_arg, Item, KeyFilter, ListSpec, Rows, SourceSpec};

/// Most heights `completedBlockHeights` takes (`heightCount`).
pub const MAX_HEIGHT_COUNT: i32 = 100;
const CHECK_EVERY: usize = 64;

#[derive(Debug, Clone)]
pub struct Block {
    pub b: Arc<BlockRecord>,
}

impl Block {
    pub fn new(b: Arc<BlockRecord>) -> Block {
        Block { b }
    }
    fn hash_text(&self) -> String {
        self.b.hash.to_b64url()
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct BlockNeighbor {
    pub chain_id: String,
    pub hash: String,
}

#[Object]
impl Block {
    async fn chain_id(&self) -> BigInt {
        BigInt::new(i128::from(self.b.chain))
    }
    async fn creation_time(&self) -> DateTime {
        DateTime(self.b.creation_time as i64)
    }
    /// (2^256 − 1) / target (chainweb-node's definition).
    async fn difficulty(&self) -> BigInt {
        BigInt::number(header::difficulty(&self.b.target).min(i128::MAX as u128) as i128)
    }
    async fn epoch(&self) -> DateTime {
        DateTime(self.b.epoch_start as i64)
    }
    async fn events(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<BlockEventsConnection> {
        let spec = ListSpec {
            source: SourceSpec::Block(self.b.hash),
            filter: KeyFilter::default(),
            rows: Rows::Events {
                qual: None,
                module: None,
                order_index: None,
                params: None,
            },
        };
        let p = block_list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(BlockEventsConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| match it {
                    Item::Event(d, i) => Some(BlockEventsConnectionEdge {
                        cursor,
                        node: Event { d, i },
                    }),
                    _ => None,
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
    /// The oracle's string, as `nonce` (R-P5P9-60 D2, rule (b): kadscan requests it).
    async fn flags(&self) -> Decimal {
        decimal(self.b.feature_flags).into_text()
    }
    async fn hash(&self) -> String {
        self.hash_text()
    }
    async fn height(&self) -> BigInt {
        BigInt::new(i128::from(self.b.height))
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::Block {
            hash: self.hash_text(),
        })))
    }
    /// ZelCore extension: whether this block is the stored block at its chain and height.
    /// The store keeps the canonical chain only (a reorg replaces blocks), so a block read in
    /// the same request is canonical; one held across a reorg (a subscription) may not be.
    async fn canonical(&self, ctx: &Context<'_>) -> Result<bool> {
        is_stored(ctx, &self.b).await
    }
    /// ZelCore extension: the block's coinbase output as compact JSON text (`gas`, `logs`,
    /// `txId`, `events`, `reqKey`, `result`, `metaData`, `continuation`).
    async fn coinbase(&self, ctx: &Context<'_>) -> Result<String> {
        let b = self.b.clone();
        let out = gql(ctx)
            .state()?
            .read(move |view| {
                let Ok(h) = u32::try_from(b.height) else {
                    return Ok(None);
                };
                match view.tx(kdx_types::TxKey::new(h, b.chain, 0))? {
                    Some(r) => Ok(Some(kdx_api::compat::shapes::output(view, &r)?)),
                    None => Ok(None),
                }
            })
            .await
            .map_err(from_api)?;
        out.map(|v| super::tx::json_text(&v)).ok_or_else(|| {
            err(
                Code::NotAvailable,
                "the coinbase of this block is not stored",
            )
        })
    }
    /// The miner's `coin` account on this chain (its current reconciled row).
    async fn miner_account(&self, ctx: &Context<'_>) -> Result<FungibleChainAccount> {
        let b = self.b.clone();
        gql(ctx)
            .state()?
            .read(move |view| {
                let d = view.dict();
                let name = d
                    .name(DictKind::Account, b.miner_account)
                    .map(|n| n.to_string());
                match name {
                    Some(a) => super::account::chain_account_in(view, &a, b.chain, "coin"),
                    None => Ok(None),
                }
            })
            .await
            .map_err(from_api)?
            .ok_or_else(|| err(Code::NotSupported, "the miner account has no balance row"))
    }
    /// Adjacent blocks, ascending by chain id.
    async fn neighbors(&self) -> Vec<BlockNeighbor> {
        let mut a = self.b.adjacents.clone();
        a.sort_by_key(|x| x.0);
        a.into_iter()
            .map(|(c, h)| BlockNeighbor {
                chain_id: c.to_string(),
                hash: h.to_b64url(),
            })
            .collect()
    }
    async fn nonce(&self) -> Decimal {
        decimal(self.b.nonce).into_text()
    }
    async fn parent(&self, ctx: &Context<'_>) -> Result<Option<Block>> {
        let (p, c) = (self.b.parent, self.b.chain);
        let found = gql(ctx)
            .state()?
            .read(move |view| view.block_by_hash(&p))
            .await
            .map_err(from_api)?;
        Ok(found
            .filter(|(chain, _, _)| *chain == c)
            .map(|(_, _, b)| Block::new(Arc::new(b))))
    }
    async fn payload_hash(&self) -> String {
        self.b.payload_hash.to_b64url()
    }
    /// BLAKE2s-256 of the binary header without its hash, as big-endian hex (chainweb-data).
    async fn pow_hash(&self) -> Result<String> {
        header::pow_hash(&self.b)
            .map(|h| header::pow_hash_hex(&h))
            .ok_or_else(|| err(Code::NotSupported, "unknown chainweb version"))
    }
    /// The target as the node's header sends it: base64url of the 32 little-endian bytes
    /// (R-P5P9-57 F1; the oracle agrees).
    async fn target(&self) -> String {
        kdx_types::hash::b64url_encode(&self.b.target)
    }
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<BlockTransactionsConnection> {
        let spec = ListSpec {
            source: SourceSpec::Block(self.b.hash),
            filter: KeyFilter {
                user_only: true,
                ..KeyFilter::default()
            },
            rows: Rows::Txs {
                sender: None,
                nft: false,
            },
        };
        let p = block_list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(BlockTransactionsConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| match it {
                    Item::Tx(d) => Transaction::new(d)
                        .map(|node| BlockTransactionsConnectionEdge { cursor, node }),
                    _ => None,
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
    /// The chain's cumulative difficulty at this block, as the node's header sends it:
    /// base64url of the 32 little-endian bytes (R-P5P9-57 F1; the oracle agrees).
    async fn weight(&self) -> String {
        kdx_types::hash::b64url_encode(&self.b.weight)
    }
}

fn decimal(n: u64) -> Decimal {
    Decimal::new(&n.to_string()).unwrap_or_else(|| Decimal::new("0").expect("zero"))
}

// ---------------------------------------------------------------------------------------
// Block lists

/// The chains of a `chainIds` argument, ascending and distinct.
pub fn chain_set(ids: Option<&[String]>) -> Result<Vec<u8>> {
    let mut v = match ids {
        None => (0..20).collect(),
        Some(ids) => {
            if ids.len() > 100 {
                return Err(bad_input("too many chainIds"));
            }
            ids.iter()
                .map(|s| chain_arg(s))
                .collect::<Result<Vec<u8>>>()?
        }
    };
    v.sort_unstable();
    v.dedup();
    Ok(v)
}

/// Whether `b` is the stored block at its chain and height (the store keeps the canonical
/// chain only).
pub(crate) async fn is_stored(ctx: &Context<'_>, b: &BlockRecord) -> Result<bool> {
    let (chain, height, hash) = (b.chain, b.height, b.hash);
    gql(ctx)
        .state()?
        .read(move |view| {
            let Ok(h) = u32::try_from(height) else {
                return Ok(false);
            };
            Ok(view.block_hash(chain, h)? == Some(hash))
        })
        .await
        .map_err(from_api)
}

/// Which blocks a list holds: per chain, heights `lo..=hi`; listed by height descending (or
/// ascending when `ascending`), then chain ascending.
#[derive(Debug, Clone)]
pub struct BlockSet {
    pub chains: Vec<(u8, u64, u64)>,
    /// Heights ascending: `blocksFromHeight`, in the oracle's order (R-P5P9-57 M2a).
    pub ascending: bool,
}

impl BlockSet {
    fn bounds(&self) -> Option<(u64, u64)> {
        let lo = self.chains.iter().map(|c| c.1).min()?;
        let hi = self.chains.iter().map(|c| c.2).max()?;
        (lo <= hi).then_some((lo, hi))
    }
}

/// Up to `page.n + 1` blocks of `set` after (forward) or before (backward) the cursor, in
/// walk order. One read; the deadline is checked every 64 probes.
pub fn walk_blocks(
    view: &View,
    set: &BlockSet,
    page: &Page<PageCursor>,
) -> ApiResult<Vec<Arc<BlockRecord>>> {
    let want = page.n + 1;
    let mut out = Vec::new();
    // Nothing is stored below the store floor: start there, as `totalCount` does (M7).
    let floor = u64::from(kdx_api::compat::store_floor(view));
    let set = &BlockSet {
        chains: set
            .chains
            .iter()
            .filter(|c| c.2 >= floor)
            .map(|&(c, lo, hi)| (c, lo.max(floor), hi))
            .collect(),
        ascending: set.ascending,
    };
    let Some((lo, hi)) = set.bounds() else {
        return Ok(out);
    };
    let cur = match page.cursor {
        Some(PageCursor::Block(c)) => Some(c),
        _ => None,
    };
    let mut probes = 0usize;
    let mut visit = |h: u64, c: u8, out: &mut Vec<Arc<BlockRecord>>| -> ApiResult<()> {
        probes += 1;
        if probes.is_multiple_of(CHECK_EVERY) {
            view.check_deadline()?;
        }
        let Ok(h32) = u32::try_from(h) else {
            return Ok(());
        };
        if let Some(b) = view.block(c, h32)? {
            out.push(Arc::new(b));
        }
        Ok(())
    };
    let mut heights = 0usize;
    let in_set = |h: u64, c: &(u8, u64, u64)| h >= c.1 && h <= c.2;
    // List order: heights descending (or ascending, `set.ascending`), chains ascending
    // within a height. A forward page walks it from the cursor on, a backward page walks
    // it in reverse.
    let forward = page.dir == Dir::Forward;
    let up = forward == set.ascending;
    let mut h = if up {
        cur.map_or(lo, |c| c.height.max(lo))
    } else {
        let start = cur.map_or(hi, |c| c.height.min(hi));
        if start < lo {
            return Ok(out);
        }
        start
    };
    while h <= hi {
        heights += 1;
        if heights.is_multiple_of(4096) {
            view.check_deadline()?;
        }
        let row = set.chains.iter().filter(|c| in_set(h, c));
        let row: Vec<&(u8, u64, u64)> = if forward {
            row.collect()
        } else {
            row.rev().collect()
        };
        for ch in row {
            let past = |k: &BlockCursor| {
                if forward {
                    ch.0 <= k.chain
                } else {
                    ch.0 >= k.chain
                }
            };
            if cur.is_some_and(|k| h == k.height && past(&k)) {
                continue;
            }
            visit(h, ch.0, &mut out)?;
            if out.len() >= want {
                return Ok(out);
            }
        }
        if up {
            h += 1;
        } else {
            if h == lo {
                break;
            }
            h -= 1;
        }
    }
    Ok(out)
}

/// Per-chain stored tips and genesis heights.
pub(crate) fn tips_and_genesis(view: &View) -> ApiResult<Vec<(u8, u64, u64)>> {
    let genesis: Vec<(u8, u32)> = view.genesis.chains().collect();
    Ok(view
        .chain_tips()?
        .into_iter()
        .map(|(c, h, _)| {
            let g = genesis.iter().find(|x| x.0 == c).map_or(0, |x| x.1);
            (c, u64::from(g), u64::from(h))
        })
        .collect())
}

/// How a block list picks its heights from the tips.
#[derive(Debug, Clone, Copy)]
pub enum Span {
    /// `startHeight..=endHeight` (`endHeight` defaults to the tip).
    Heights { start: u64, end: Option<u64> },
    /// Every block at least `depth` below its chain's tip.
    Depth(u64),
    /// The newest `count` heights (complete ones only when `complete`).
    Completed { count: u64, complete: bool },
}

/// Edges, page info and the number of blocks in the whole list (ZelCore `totalCount` of
/// `blocksFromDepth`: arithmetic over the per-chain height ranges, from the store floor up;
/// blocks are dense, so it is exact without a walk).
pub type BlockEdges = (Vec<(String, Block)>, PageInfo, u64);

/// One page of a block list.
pub async fn block_page(
    ctx: &Context<'_>,
    chains: Vec<u8>,
    span: Span,
    args: PageArgs,
) -> Result<BlockEdges> {
    let g = gql(ctx);
    let page = args.parse(&g.settings)?;
    if matches!(page.cursor, Some(PageCursor::Tx(_))) {
        return Err(bad_input("invalid cursor"));
    }
    let p = page.clone();
    // The blocks are read only when the edges or page info are selected.
    let reads = crate::txlist::selects_direct(ctx, &["edges", "pageInfo"]);
    let (rows, total) = g
        .state()?
        .read(move |view| {
            let tips = tips_and_genesis(view)?;
            let tips: Vec<_> = tips.into_iter().filter(|t| chains.contains(&t.0)).collect();
            let set = BlockSet {
                chains: span_set(&tips, span),
                ascending: matches!(span, Span::Heights { .. }),
            };
            let floor = u64::from(kdx_api::compat::store_floor(view));
            let total: u64 = set
                .chains
                .iter()
                .map(|&(_, lo, hi)| (hi + 1).saturating_sub(lo.max(floor)))
                .sum();
            let rows = if reads {
                walk_blocks(view, &set, &p)?
            } else {
                Vec::new()
            };
            Ok((rows, total))
        })
        .await
        .map_err(from_api)?;
    let (edges, info) = finish(rows, &page, |b| {
        BlockCursor {
            height: b.height,
            chain: b.chain,
        }
        .encode()
    });
    Ok((
        edges.into_iter().map(|(c, b)| (c, Block::new(b))).collect(),
        info,
        total,
    ))
}

/// Per-chain `(chain, lo, hi)` of a span over `(chain, genesis, tip)` rows.
pub fn span_set(tips: &[(u8, u64, u64)], span: Span) -> Vec<(u8, u64, u64)> {
    match span {
        Span::Heights { start, end } => tips
            .iter()
            .map(|&(c, g, t)| (c, start.max(g), end.map_or(t, |e| e.min(t))))
            .filter(|x| x.1 <= x.2)
            .collect(),
        Span::Depth(d) => tips
            .iter()
            .filter(|t| t.2 >= d)
            .map(|&(c, g, t)| (c, g, t - d))
            .filter(|x| x.1 <= x.2)
            .collect(),
        Span::Completed { count, complete } => {
            if tips.is_empty() || count == 0 {
                return Vec::new();
            }
            let top = if complete {
                tips.iter().map(|t| t.2).min().unwrap_or(0)
            } else {
                tips.iter().map(|t| t.2).max().unwrap_or(0)
            };
            let lo = (top + 1).saturating_sub(count);
            let lo = if complete {
                lo.max(tips.iter().map(|t| t.1).max().unwrap_or(0))
            } else {
                lo
            };
            tips.iter()
                .map(|&(c, g, t)| (c, lo.max(g), top.min(t)))
                .filter(|x| x.1 <= x.2)
                .collect()
        }
    }
}

/// `block(hash)`.
pub async fn query_block(ctx: &Context<'_>, hash: &str) -> Result<Option<Block>> {
    let h = hash_arg("hash", hash)?;
    let found = gql(ctx)
        .state()?
        .read(move |view| view.block_by_hash(&h))
        .await
        .map_err(from_api)?;
    Ok(found.map(|(_, _, b)| Block::new(Arc::new(b))))
}

/// `lastBlockHeight`: the highest stored height over all chains.
pub async fn last_block_height(ctx: &Context<'_>) -> Result<Option<BigInt>> {
    let h = gql(ctx)
        .state()?
        .read_status(|view| view.max_indexed_height())
        .await
        .map_err(from_api)?;
    Ok(h.map(|h| BigInt::new(i128::from(h))))
}

/// A non-negative `Int` argument.
pub fn non_negative(name: &str, v: i32) -> Result<u64> {
    u64::try_from(v).map_err(|_| bad_input(format!("{name} must not be negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans() {
        // chain 0 from genesis 0 at tip 100; chain 10 from genesis 50 at tip 98.
        let tips = [(0u8, 0u64, 100u64), (10, 50, 98)];
        assert_eq!(
            span_set(
                &tips,
                Span::Heights {
                    start: 40,
                    end: None
                }
            ),
            vec![(0, 40, 100), (10, 50, 98)]
        );
        assert_eq!(
            span_set(
                &tips,
                Span::Heights {
                    start: 99,
                    end: Some(99)
                }
            ),
            vec![(0, 99, 99)]
        );
        assert_eq!(
            span_set(&tips, Span::Depth(3)),
            vec![(0, 0, 97), (10, 50, 95)]
        );
        assert_eq!(
            span_set(
                &tips,
                Span::Completed {
                    count: 3,
                    complete: true
                }
            ),
            vec![(0, 96, 98), (10, 96, 98)]
        );
        assert_eq!(
            span_set(
                &tips,
                Span::Completed {
                    count: 3,
                    complete: false
                }
            ),
            vec![(0, 98, 100), (10, 98, 98)]
        );
        assert!(span_set(&tips, Span::Depth(200)).is_empty());
    }

    #[test]
    fn chain_sets() {
        assert_eq!(chain_set(None).unwrap().len(), 20);
        let ids = vec!["3".to_string(), "1".into(), "3".into()];
        assert_eq!(chain_set(Some(&ids)).unwrap(), vec![1, 3]);
        assert!(chain_set(Some(&["x".to_string()])).is_err());
    }
}

#[cfg(test)]
mod walk_tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::testutil;

    /// M7: a walk from genesis on a partial store starts at the store floor instead of
    /// probing every empty height below it until the deadline.
    #[tokio::test]
    async fn block_walks_start_at_the_store_floor() {
        let f = testutil::store();
        let top: u64 = 40_000_000;
        f.set_floor(top as u32);
        f.apply(0, top, 0);
        f.apply(0, top + 1, 0);
        let set = BlockSet {
            chains: vec![(0, 0, top + 1)],
            ascending: false,
        };
        let page = Page {
            dir: Dir::Backward,
            n: 5,
            cursor: None,
        };
        let t = Instant::now();
        let got =
            f.st.read(move |view| walk_blocks(view, &set, &page))
                .await
                .expect("the walk ends before the deadline");
        let hs: Vec<u64> = got.iter().map(|b| b.height).collect();
        assert_eq!(hs, [top, top + 1]);
        assert!(t.elapsed() < Duration::from_secs(1));
    }

    /// D1 (R-P5P9-60): one HTTP request of one client over 4 heights × 5 chains gets all 20
    /// blocks' nested `transactions` and `events` (40 connections), none refused with
    /// `LIMIT`: a connection nested in one block takes no per-client heavy ticket. The same
    /// client's account-scoped lists keep H-1: 1 runs, 3 wait, the others get `LIMIT`.
    #[tokio::test]
    async fn nested_block_connections_take_no_heavy_ticket() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let f = testutil::store();
        for h in 10..14u64 {
            for c in 0..5u8 {
                f.apply(c, h, 0);
            }
        }
        let client: std::net::IpAddr = "192.0.2.7".parse().unwrap();
        let post = |q: String| {
            let app = crate::router(crate::build_schema(
                f.st.clone(),
                crate::GraphqlSettings::default(),
                None,
            ))
            .with_state(f.st.clone());
            let req = Request::post("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::json!({ "query": q }).to_string()))
                .unwrap();
            async move {
                let resp = kdx_api::server::as_client(client, app.oneshot(req))
                    .await
                    .unwrap();
                let b = axum::body::to_bytes(resp.into_body(), 8 << 20)
                    .await
                    .unwrap();
                serde_json::from_slice::<serde_json::Value>(&b).unwrap()
            }
        };
        let limits = |v: &serde_json::Value| {
            v["errors"].as_array().map_or(0, |es| {
                es.iter()
                    .filter(|e| e["extensions"]["code"] == "LIMIT")
                    .count()
            })
        };
        let v = post(
            "{ blocksFromHeight(startHeight: 10, endHeight: 13, \
               chainIds: [\"0\", \"1\", \"2\", \"3\", \"4\"], first: 20) \
               { edges { node { height transactions { totalCount } events { totalCount } } } } }"
                .into(),
        )
        .await;
        assert_eq!(limits(&v), 0, "{v}");
        assert!(v["errors"].is_null(), "{v}");
        let edges = v["blocksFromHeight"]["edges"].as_array();
        let edges = edges.or(v["data"]["blocksFromHeight"]["edges"].as_array());
        let edges = edges.expect("edges");
        assert_eq!(edges.len(), 20);
        for e in edges {
            assert_eq!(e["node"]["transactions"]["totalCount"], 0, "{e}");
            assert_eq!(e["node"]["events"]["totalCount"], 0, "{e}");
        }
        // H-1 still holds for the client's account-scoped lists.
        let q: String = (0..20)
            .map(|i| {
                format!(
                    "a{i}: transactions(accountName: \"k:a\", first: 1) {{ edges {{ cursor }} }} "
                )
            })
            .collect();
        // A read that ends during the poll pass frees its place early, so up to 16 are refused.
        let v = post(format!("{{ {q} }}")).await;
        let refused = limits(&v);
        let most = 20 - 1 - kdx_api::server::HEAVY_WAITING_PER_CLIENT;
        assert!(refused > 0 && refused <= most, "{refused}: {v}");
    }

    /// R-P5P9-57 M2a: `blocksFromHeight` lists heights ascending (the oracle's order),
    /// chains ascending within a height, and pages both ways in that order;
    /// `blocksFromDepth` stays newest first.
    #[tokio::test]
    async fn blocks_from_height_is_ascending() {
        let f = testutil::store();
        for h in 0..=3u64 {
            for c in [0u8, 1] {
                f.apply(c, h, 0);
            }
        }
        let schema = crate::build_schema(f.st.clone(), crate::GraphqlSettings::default(), None);
        let run = |q: String| {
            let schema = schema.clone();
            async move {
                let r = schema.execute(q).await;
                assert!(r.errors.is_empty(), "{:?}", r.errors);
                r.data.into_json().unwrap()
            }
        };
        let rows = |v: &serde_json::Value, f: &str| -> Vec<(u64, u64)> {
            v[f]["edges"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| {
                    let n = &e["node"];
                    (
                        n["height"].as_u64().unwrap(),
                        n["chainId"].as_u64().unwrap(),
                    )
                })
                .collect()
        };
        let sel = "pageInfo { hasNextPage hasPreviousPage endCursor startCursor } \
                   edges { node { height chainId } }";
        let v = run(format!(
            "{{ blocksFromHeight(startHeight: 1, chainIds: [\"0\", \"1\"], first: 4) {{ {sel} }} }}"
        ))
        .await;
        assert_eq!(
            rows(&v, "blocksFromHeight"),
            [(1, 0), (1, 1), (2, 0), (2, 1)]
        );
        assert_eq!(v["blocksFromHeight"]["pageInfo"]["hasNextPage"], true);
        let end = v["blocksFromHeight"]["pageInfo"]["endCursor"].clone();
        let v = run(format!(
            "{{ blocksFromHeight(startHeight: 1, chainIds: [\"0\", \"1\"], first: 4, after: {end}) {{ {sel} }} }}"
        ))
        .await;
        assert_eq!(rows(&v, "blocksFromHeight"), [(3, 0), (3, 1)]);
        assert_eq!(v["blocksFromHeight"]["pageInfo"]["hasNextPage"], false);
        assert_eq!(v["blocksFromHeight"]["pageInfo"]["hasPreviousPage"], true);
        let start = v["blocksFromHeight"]["pageInfo"]["startCursor"].clone();
        let v = run(format!(
            "{{ blocksFromHeight(startHeight: 1, chainIds: [\"0\", \"1\"], last: 3, before: {start}) {{ {sel} }} }}"
        ))
        .await;
        assert_eq!(rows(&v, "blocksFromHeight"), [(1, 1), (2, 0), (2, 1)]);
        let v = run(format!(
            "{{ blocksFromHeight(startHeight: 2, endHeight: 3, chainIds: [\"1\"], last: 1) {{ {sel} }} }}"
        ))
        .await;
        assert_eq!(rows(&v, "blocksFromHeight"), [(3, 1)]);
        let v = run(format!(
            "{{ blocksFromDepth(minimumDepth: 0, chainIds: [\"0\", \"1\"], first: 3) {{ {sel} }} }}"
        ))
        .await;
        assert_eq!(rows(&v, "blocksFromDepth"), [(3, 0), (3, 1), (2, 0)]);
    }
}
