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
use crate::txlist::{chain_arg, hash_arg, list, Item, KeyFilter, ListSpec, Rows, SourceSpec};

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
        BigInt(i128::from(self.b.chain))
    }
    async fn creation_time(&self) -> DateTime {
        DateTime(self.b.creation_time as i64)
    }
    /// (2^256 − 1) / target (chainweb-node's definition).
    async fn difficulty(&self) -> BigInt {
        BigInt(header::difficulty(&self.b.target).min(i128::MAX as u128) as i128)
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
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
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
    async fn flags(&self) -> Decimal {
        decimal(self.b.feature_flags)
    }
    async fn hash(&self) -> String {
        self.hash_text()
    }
    async fn height(&self) -> BigInt {
        BigInt(i128::from(self.b.height))
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
        let b = self.b.clone();
        gql(ctx)
            .state()?
            .read(move |view| {
                let Ok(h) = u32::try_from(b.height) else {
                    return Ok(false);
                };
                Ok(view.block(b.chain, h)?.is_some_and(|x| x.hash == b.hash))
            })
            .await
            .map_err(from_api)
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
        decimal(self.b.nonce)
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
    /// The target as a decimal integer (chainweb-data's `numeric` column).
    async fn target(&self) -> String {
        header::le256_decimal(&self.b.target)
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
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
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
    /// The chain's cumulative difficulty at this block, as a decimal integer.
    async fn weight(&self) -> String {
        header::le256_decimal(&self.b.weight)
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

/// Which blocks a list holds: per chain, heights `lo..=hi`; listed by height descending, then
/// chain ascending.
#[derive(Debug, Clone)]
pub struct BlockSet {
    pub chains: Vec<(u8, u64, u64)>,
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
    match page.dir {
        Dir::Forward => {
            let start = cur.map_or(hi, |c| c.height.min(hi));
            if start < lo {
                return Ok(out);
            }
            let mut h = start;
            loop {
                heights += 1;
                if heights.is_multiple_of(4096) {
                    view.check_deadline()?;
                }
                for ch in set.chains.iter().filter(|c| in_set(h, c)) {
                    if cur.is_some_and(|k| h == k.height && ch.0 <= k.chain) {
                        continue;
                    }
                    visit(h, ch.0, &mut out)?;
                    if out.len() >= want {
                        return Ok(out);
                    }
                }
                if h == lo {
                    break;
                }
                h -= 1;
            }
        }
        Dir::Backward => {
            let start = cur.map_or(lo, |c| c.height.max(lo));
            let mut h = start;
            while h <= hi {
                heights += 1;
                if heights.is_multiple_of(4096) {
                    view.check_deadline()?;
                }
                for ch in set.chains.iter().rev().filter(|c| in_set(h, c)) {
                    if cur.is_some_and(|k| h == k.height && ch.0 >= k.chain) {
                        continue;
                    }
                    visit(h, ch.0, &mut out)?;
                    if out.len() >= want {
                        return Ok(out);
                    }
                }
                h += 1;
            }
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
    let (rows, total) = g
        .state()?
        .read(move |view| {
            let tips = tips_and_genesis(view)?;
            let tips: Vec<_> = tips.into_iter().filter(|t| chains.contains(&t.0)).collect();
            let set = BlockSet {
                chains: span_set(&tips, span),
            };
            let floor = u64::from(kdx_api::compat::store_floor(view));
            let total: u64 = set
                .chains
                .iter()
                .map(|&(_, lo, hi)| (hi + 1).saturating_sub(lo.max(floor)))
                .sum();
            Ok((walk_blocks(view, &set, &p)?, total))
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
    Ok(h.map(|h| BigInt(i128::from(h))))
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
