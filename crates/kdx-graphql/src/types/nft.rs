//! Non-fungible accounts and token balances (GQ-15, Task 5), served from `NftBalances`
//! (balances above zero only, as native). `NonFungibleTokenBalance.guard` and `.info` come from
//! `NftDetails`, which the reconciler fills with two fixed `/local` forms; resolvers never call
//! the node. Details read before the balance row last moved are served (stale, as GQ-14 serves
//! stale balances). A holding with no usable details gets a `NOT_AVAILABLE` error on that field
//! only: `info` is `null`, and the non-null `guard` answers an empty `RawGuard` beside the
//! error, so the account and its other holdings still answer (see `docs/graphql.md`).

use async_graphql::{Context, Object, Result, SimpleObject, ID};
use kdx_api::{ApiResult, View};
use kdx_hot::nft::{self, parse_nft_key, HolderDetails, NftRow, TokenDetails};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};

use super::guard::{map_guard, RawGuard};
use super::{
    IGuard, NonFungibleAccountTransactionsConnection, NonFungibleAccountTransactionsConnectionEdge,
    NonFungibleAccountTransfersConnection, NonFungibleAccountTransfersConnectionEdge,
    NonFungibleChainAccountTransactionsConnection,
    NonFungibleChainAccountTransactionsConnectionEdge, NonFungibleChainAccountTransfersConnection,
    NonFungibleChainAccountTransfersConnectionEdge, Sel, Transaction, Transfer,
};
use crate::connection::{PageArgs, Total};
use crate::error::{err, from_api, Code};
use crate::gql;
use crate::ids::{encode, NodeKey};
use crate::txlist::{list, Item, KeyFilter, ListPage, ListSpec, Rows, SourceSpec};

/// One token holding.
#[derive(Debug, Clone)]
pub struct NftItem {
    pub token_id: String,
    pub ledger: String,
    pub chain: u8,
    pub balance: String,
    /// The holding's `NftDetails` guard row and its token's info row (GQ-15).
    pub holder: Option<HolderDetails>,
    pub info: Option<TokenDetails>,
}

/// The positive holdings of `account` (on `chain` when given), at most `cap`, in key order,
/// and whether there are more (part E, S1: a cut list is never served as complete).
pub fn nft_rows(
    view: &View,
    account: &str,
    chain: Option<u8>,
    cap: usize,
) -> ApiResult<(Vec<NftItem>, bool)> {
    let d = view.dict();
    let Some(a) = d.find(DictKind::Account, account) else {
        return Ok((vec![], false));
    };
    let prefix = a.to_be_bytes();
    let mut from = prefix.to_vec();
    let mut out = Vec::new();
    loop {
        let rows = view
            .txn
            .scan_limit(Table::NftBalances, &from, 256, |k| k.starts_with(&prefix))?;
        for (k, v) in &rows {
            view.check_deadline()?;
            let (Some((_, _, _, c)), Some(r), Ok(key)) = (
                parse_nft_key(k),
                NftRow::decode(v),
                <[u8; 25]>::try_from(k.as_slice()),
            ) else {
                continue;
            };
            if !r.positive() || chain.is_some_and(|x| x != c) {
                continue;
            }
            if out.len() == cap {
                return Ok((out, true));
            }
            let (holder, info) = nft::details(&view.txn, &key)?;
            out.push(NftItem {
                token_id: r.token_id.clone(),
                ledger: d
                    .name(DictKind::Module, r.ledger)
                    .unwrap_or_default()
                    .to_owned(),
                chain: c,
                balance: r.balance.clone(),
                holder,
                info,
            });
        }
        match rows.last() {
            Some((k, _)) if rows.len() == 256 => {
                from = k.clone();
                from.push(0);
            }
            _ => return Ok((out, false)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NonFungibleAccount {
    pub account: String,
    pub items: Vec<NftItem>,
    /// The holdings were cut at [`crate::MAX_NFT_HOLDINGS`].
    pub cut: bool,
}

#[derive(Debug, Clone)]
pub struct NonFungibleChainAccount {
    pub account: String,
    pub chain: u8,
    pub items: Vec<NftItem>,
    /// The account's holdings were cut at [`crate::MAX_NFT_HOLDINGS`].
    pub cut: bool,
}

fn nft_txs_spec(account: &str, chain: Option<u8>) -> ListSpec {
    ListSpec {
        source: SourceSpec::Acct(account.to_owned()),
        filter: KeyFilter {
            chain,
            user_only: true,
            ..KeyFilter::default()
        },
        rows: Rows::Txs {
            sender: None,
            nft: true,
            module: None,
            coinbase: false,
        },
    }
}

/// Stock's `hasTokenId` transfers of an account (`NonFungibleAccount.transfers`, on one chain
/// for `NonFungibleChainAccount.transfers`): the NFT transfers on either side, as the root
/// `transfers(accountName, isNFT: true[, chainId])` lists them (the account's `acct`
/// postings; NFT moves have no `xfer` postings).
fn nft_transfers_spec(account: &str, chain: Option<u8>) -> ListSpec {
    ListSpec {
        source: SourceSpec::Acct(account.to_owned()),
        filter: KeyFilter {
            chain,
            ..KeyFilter::default()
        },
        rows: Rows::Transfers {
            module: None,
            account: Some(account.to_owned()),
            nft: true,
        },
    }
}

fn transfer_edges<E>(p: ListPage, mk: impl Fn(String, Transfer) -> E) -> Vec<E> {
    p.edges
        .into_iter()
        .filter_map(|(c, it)| match it {
            Item::Nft(d, i) => Some(mk(c, Transfer::nft(d, i))),
            _ => None,
        })
        .collect()
}

fn tx_edges<E>(p: ListPage, mk: impl Fn(String, Transaction) -> E) -> Vec<E> {
    p.edges
        .into_iter()
        .filter_map(|(c, it)| match it {
            Item::Tx(d) => Transaction::new(d).map(|t| mk(c, t)),
            _ => None,
        })
        .collect()
}

fn balances(account: &str, items: &[NftItem]) -> Vec<NonFungibleTokenBalance> {
    items
        .iter()
        .map(|i| NonFungibleTokenBalance {
            account: account.to_owned(),
            item: i.clone(),
        })
        .collect()
}

#[Object]
impl NonFungibleAccount {
    async fn account_name(&self) -> &str {
        &self.account
    }
    async fn chain_accounts(&self) -> Vec<NonFungibleChainAccount> {
        let mut chains: Vec<u8> = self.items.iter().map(|i| i.chain).collect();
        chains.sort_unstable();
        chains.dedup();
        chains
            .into_iter()
            .map(|c| NonFungibleChainAccount {
                account: self.account.clone(),
                chain: c,
                items: self
                    .items
                    .iter()
                    .filter(|i| i.chain == c)
                    .cloned()
                    .collect(),
                cut: self.cut,
            })
            .collect()
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::NonFungibleAccount {
            account: self.account.clone(),
        })))
    }
    async fn non_fungible_token_balances(&self, ctx: &Context<'_>) -> Vec<NonFungibleTokenBalance> {
        served_balances(ctx, &self.account, &self.items, self.cut)
    }
    /// Txs touching the account with an event of an NFT ledger.
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleAccountTransactionsConnection> {
        let p = list(
            ctx,
            nft_txs_spec(&self.account, None),
            PageArgs::new(after, before, first, last),
        )
        .await?;
        let (page_info, total) = (p.info.clone(), p.total.clone());
        Ok(NonFungibleAccountTransactionsConnection {
            edges: tx_edges(p, |cursor, node| {
                NonFungibleAccountTransactionsConnectionEdge { cursor, node }
            }),
            page_info,
            total,
        })
    }
    /// The account's NFT transfers on every chain (stock's `hasTokenId` transfers), newest
    /// first, as the root `transfers(accountName, isNFT: true)`.
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleAccountTransfersConnection> {
        let spec = nft_transfers_spec(&self.account, None);
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        let (page_info, total) = (p.info.clone(), p.total.clone());
        Ok(NonFungibleAccountTransfersConnection {
            edges: transfer_edges(p, |cursor, node| {
                NonFungibleAccountTransfersConnectionEdge { cursor, node }
            }),
            page_info,
            total,
        })
    }
}

#[Object]
impl NonFungibleChainAccount {
    async fn account_name(&self) -> &str {
        &self.account
    }
    async fn chain_id(&self) -> String {
        self.chain.to_string()
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::NonFungibleChainAccount {
            chain: self.chain.to_string(),
            account: self.account.clone(),
        })))
    }
    async fn non_fungible_token_balances(&self, ctx: &Context<'_>) -> Vec<NonFungibleTokenBalance> {
        served_balances(ctx, &self.account, &self.items, self.cut)
    }
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleChainAccountTransactionsConnection> {
        let spec = nft_txs_spec(&self.account, Some(self.chain));
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        let (page_info, total): (_, Total) = (p.info.clone(), p.total.clone());
        Ok(NonFungibleChainAccountTransactionsConnection {
            edges: tx_edges(p, |cursor, node| {
                NonFungibleChainAccountTransactionsConnectionEdge { cursor, node }
            }),
            page_info,
            total,
        })
    }
    /// The account's NFT transfers on this chain, as the root
    /// `transfers(accountName, isNFT: true, chainId)`.
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleChainAccountTransfersConnection> {
        let spec = nft_transfers_spec(&self.account, Some(self.chain));
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        let (page_info, total) = (p.info.clone(), p.total.clone());
        Ok(NonFungibleChainAccountTransfersConnection {
            edges: transfer_edges(p, |cursor, node| {
                NonFungibleChainAccountTransfersConnectionEdge { cursor, node }
            }),
            page_info,
            total,
        })
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct NonFungibleToken {
    pub precision: i32,
    pub supply: i32,
    pub uri: String,
}

#[derive(Debug, Clone)]
pub struct NonFungibleTokenBalance {
    pub account: String,
    pub item: NftItem,
}

/// `version` of a ledger: `v1` for `marmalade.ledger`, `v2` for `marmalade-v2.ledger`, else
/// the ledger's name.
pub fn ledger_version(ledger: &str) -> String {
    match ledger {
        "marmalade.ledger" => "v1".into(),
        "marmalade-v2.ledger" => "v2".into(),
        other => other.to_owned(),
    }
}

#[Object]
impl NonFungibleTokenBalance {
    async fn account_name(&self) -> &str {
        &self.account
    }
    async fn balance(&self) -> i32 {
        int_of(&self.item.balance)
    }
    async fn chain_id(&self) -> String {
        self.item.chain.to_string()
    }
    /// The holding's guard from `NftDetails`. Without a usable row: an empty `RawGuard` and a
    /// `NOT_AVAILABLE` error for this field (the type is non-null, and an error alone would
    /// null the whole account).
    async fn guard(&self, ctx: &Context<'_>) -> Sel<IGuard> {
        let field = "NonFungibleTokenBalance.guard";
        let found = self
            .item
            .holder
            .as_ref()
            .filter(|h| h.status == nft::DETAIL_FOUND)
            .and_then(|h| serde_json::from_str::<serde_json::Value>(&h.guard).ok());
        if let Some(g) = found {
            return Sel(map_guard(&g, None));
        }
        let e = not_available(field, self.item.holder.as_ref().map(|h| h.status));
        ctx.add_error(ctx.set_error_path(e.into_server_error(ctx.item.pos)));
        Sel(IGuard::RawGuard(RawGuard {
            keys: vec![],
            predicate: String::new(),
            raw: String::new(),
        }))
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::NonFungibleTokenBalance {
            token_id: self.item.token_id.clone(),
            account: self.account.clone(),
            chain: self.item.chain.to_string(),
        })))
    }
    /// The token's info from `NftDetails`; `null` for a ledger without `get-token-info`
    /// (marmalade v1), `null` with `NOT_AVAILABLE` when there is no usable row.
    async fn info(&self) -> Result<Option<NonFungibleToken>> {
        match &self.item.info {
            Some(t) if t.status == nft::DETAIL_FOUND => Ok(Some(NonFungibleToken {
                precision: t.precision,
                supply: int_of(&t.supply),
                uri: t.uri.clone(),
            })),
            Some(t) if t.status == nft::DETAIL_UNDEFINED => Ok(None),
            t => Err(not_available(
                "NonFungibleTokenBalance.info",
                t.as_ref().map(|t| t.status),
            )),
        }
    }
    async fn token_id(&self) -> &str {
        &self.item.token_id
    }
    async fn version(&self) -> String {
        ledger_version(&self.item.ledger)
    }
}

/// A decimal text as `Int`: truncated toward zero, clamped to `0..=i32::MAX` (R7D-9).
fn int_of(s: &str) -> i32 {
    s.parse::<f64>()
        .map(|f| f.clamp(0.0, i32::MAX as f64) as i32)
        .unwrap_or(0)
}

/// `NOT_AVAILABLE` for a details field, saying why (`status` of the stored row, if any).
fn not_available(field: &str, status: Option<u8>) -> async_graphql::Error {
    let why = match status {
        None => "not read from the node yet",
        Some(nft::DETAIL_ABSENT) => "the ledger has no such row",
        Some(_) => "the node's answer could not be used",
    };
    err(Code::NotAvailable, format!("{field}: {why}"))
}

/// `nonFungibleAccount` / `nonFungibleChainAccount` (`chain`): `null` when the account holds
/// no token there. Every holding, as stock returns them, up to the safety cap
/// [`crate::MAX_NFT_HOLDINGS`] (FIX-3 part D, P4; part E, S1), and whether they were cut
/// there ([`holdings_cut`] says so on the list field).
pub async fn non_fungible(
    ctx: &Context<'_>,
    account: &str,
    chain: Option<u8>,
) -> Result<Option<(String, Vec<NftItem>, bool)>> {
    let account = crate::txlist::name_arg("accountName", account)?.to_owned();
    let cap = crate::MAX_NFT_HOLDINGS;
    let a = account.clone();
    let (items, cut) = gql(ctx)
        .state()?
        .read(move |view| nft_rows(view, &a, chain, cap))
        .await
        .map_err(from_api)?;
    Ok((!items.is_empty()).then_some((account, items, cut)))
}

/// The partial-data error of a holdings list cut at [`crate::MAX_NFT_HOLDINGS`] (part E,
/// S1): `LIMIT`, `extensions.limit = "nft_holdings"`.
pub fn holdings_cut() -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    kdx_metrics::get().graphql_rejected("nft_holdings");
    err(
        Code::Limit,
        format!(
            "holdings cut at {}: the account holds more",
            crate::MAX_NFT_HOLDINGS
        ),
    )
    .extend_with(|_, e| e.set("limit", "nft_holdings"))
}

/// The holdings of a list field, with [`holdings_cut`] at its path when cut.
fn served_balances(
    ctx: &Context<'_>,
    account: &str,
    items: &[NftItem],
    cut: bool,
) -> Vec<NonFungibleTokenBalance> {
    if cut {
        ctx.add_error(ctx.set_error_path(holdings_cut().into_server_error(ctx.item.pos)));
    }
    balances(account, items)
}
