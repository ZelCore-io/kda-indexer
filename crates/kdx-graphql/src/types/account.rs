//! Fungible accounts (GQ-14, Task 5), served from the reconciled `Balances`, `Guards` and
//! `KeyAccounts` rows. `fungibleAccount`, `fungibleChainAccount(s)` and
//! `FungibleAccount.chainAccounts` read the node once for an asked chain the store has no
//! row for, as stock reads every chain live (gap Q, [`crate::miss`]); other resolvers never
//! call the node.
//!
//! - Rows marked stale are served; a chain with no row (or a row the node reported absent)
//!   is missing from `chainAccounts`; with no row on any chain the account is `null`. A row
//!   that exists on chain is listed at any balance, 0 included, as stock lists it (owner
//!   ruling 2026-11-26: a row created and never funded, or funded and drained, is listed).
//! - Rows of a module marked not fungible are never served (as native).
//! - `…ByPublicKey` lists only rows whose guard holds the key, of `coin` (the default
//!   `fungibleName`, SEC-2) or of a module confirmed `fungible-v2` on the row's chain (I-6).
//!   At most [`MAX_KEY_ACCOUNTS`] matching accounts; a cut answer is flagged (L-2).

use async_graphql::{Context, Object, Result, ID};
use std::collections::{BTreeMap, BTreeSet};

use kdx_api::{ApiResult, PostingQuery, View};
use kdx_decode::index::xfer_entity;
use kdx_decode::Family;
use kdx_hot::balances::{self, key_hash, parse_bal_key, ref_hash, BalanceRow, GuardKeys};
use kdx_hot::{Table, Txn};
use kdx_types::{Dict, DictKind};
use serde_json::Value;

use super::guard::map_guard;
use super::{
    FungibleAccountTransactionsConnection, FungibleAccountTransactionsConnectionEdge,
    FungibleAccountTransfersConnection, FungibleAccountTransfersConnectionEdge,
    FungibleChainAccountTransactionsConnection, FungibleChainAccountTransactionsConnectionEdge,
    FungibleChainAccountTransfersConnection, FungibleChainAccountTransfersConnectionEdge, IGuard,
    Sel, Transaction, Transfer,
};
use crate::connection::PageArgs;
use crate::error::{bad_input, from_api};
use crate::gql;
use crate::ids::{encode, NodeKey};
use crate::scalars::Decimal;
use crate::txlist::{list, Item, KeyFilter, ListSpec, Rows, SourceSpec};

/// Accounts one `…ByPublicKey` call lists at most.
pub const MAX_KEY_ACCOUNTS: usize = 100;

/// One chain's row of an account.
#[derive(Debug, Clone)]
pub struct ChainRow {
    pub chain: u8,
    pub balance: String,
    pub guard: IGuard,
}

#[derive(Debug, Clone)]
pub struct FungibleAccount {
    pub account: String,
    pub module: String,
    pub rows: Vec<ChainRow>,
}

#[derive(Debug, Clone)]
pub struct FungibleChainAccount {
    pub account: String,
    pub module: String,
    pub row: ChainRow,
}

/// The stored guard `g` of a row on `chain`, mapped (a keyset reference is resolved from
/// its cached `describe-keyset`).
fn guard_of(view: &View, g: &[u8; 16], chain: u8) -> ApiResult<IGuard> {
    let parse = |v: Option<Vec<u8>>| v.and_then(|v| serde_json::from_slice::<Value>(&v).ok());
    let stored = parse(view.txn.get(Table::Guards, g)?).unwrap_or(Value::Null);
    let resolved = match balances::guard_keys(&stored) {
        GuardKeys::Ref(name) => parse(view.txn.get(Table::Guards, &ref_hash(chain, &name))?),
        _ => None,
    };
    Ok(map_guard(&stored, resolved.as_ref()))
}

fn hidden(view: &View, m: u32) -> ApiResult<bool> {
    Ok(balances::token_row(&view.txn, m)?.is_some_and(|t| t.kind == balances::KIND_NOT_FUNGIBLE))
}

/// The listed rows of `(account, module)`, by chain, optionally on `chains` only: every row
/// that exists on chain, at any balance (0 included, as stock); a row the node reported
/// absent is left out (one rule on every balance list, native and GraphQL).
pub fn chain_rows(
    view: &View,
    account: &str,
    module: &str,
    chains: Option<&[u8]>,
) -> ApiResult<Vec<ChainRow>> {
    let d = view.dict();
    let (Some(a), Some(m)) = (
        d.find(DictKind::Account, account),
        d.find(DictKind::Module, module),
    ) else {
        return Ok(vec![]);
    };
    if hidden(view, m)? {
        return Ok(vec![]);
    }
    let mut prefix = a.to_be_bytes().to_vec();
    prefix.extend_from_slice(&m.to_be_bytes());
    let mut out = Vec::new();
    for (k, v) in view
        .txn
        .scan_limit(Table::Balances, &prefix, 256, |k| k.starts_with(&prefix))?
    {
        let (Some((_, _, c)), Some(r)) = (parse_bal_key(&k), BalanceRow::decode(&v)) else {
            continue;
        };
        if r.absent || chains.is_some_and(|cs| !cs.contains(&c)) {
            continue;
        }
        out.push(ChainRow {
            chain: c,
            balance: r.balance.clone(),
            guard: guard_of(view, &r.guard, c)?,
        });
    }
    Ok(out)
}

/// Whether the store serves `module` at all: known to the dictionary and not marked not
/// fungible. Only such a module's missed rows are read from the node (gap Q).
fn servable(view: &View, module: &str) -> ApiResult<bool> {
    match view.dict().find(DictKind::Module, module) {
        Some(m) => Ok(!hidden(view, m)?),
        None => Ok(false),
    }
}

/// The stored rows of `(account, module)` on `chains` (all when `None`), and whether the
/// module is [`servable`].
fn stored_rows(
    view: &View,
    account: &str,
    module: &str,
    chains: Option<&[u8]>,
) -> ApiResult<(Vec<ChainRow>, bool)> {
    Ok((
        chain_rows(view, account, module, chains)?,
        servable(view, module)?,
    ))
}

/// Gap Q: add the rows of the `asked` chains the store has none for, read from the node
/// ([`crate::miss`]), in chain order. A failed or refused read adds nothing and flags the
/// response (`nodeReadIncomplete`), as stock drops a chain whose read failed.
async fn with_misses(
    ctx: &Context<'_>,
    account: &str,
    module: &str,
    servable: bool,
    asked: &[u8],
    rows: &mut Vec<ChainRow>,
) {
    let Some(miss) = gql(ctx).miss.as_ref().filter(|_| servable) else {
        return;
    };
    let missing: Vec<u8> = asked
        .iter()
        .copied()
        .filter(|c| !rows.iter().any(|r| r.chain == *c))
        .collect();
    let o = miss.read(ctx, account, module, &missing).await;
    if o.incomplete {
        crate::flags::mark(ctx, crate::flags::Flag::NodeReadIncomplete);
    }
    if o.found.is_empty() {
        return;
    }
    for (chain, balance, guard) in o.found {
        rows.push(ChainRow {
            chain,
            balance,
            guard: map_guard(&guard, None),
        });
    }
    rows.sort_by_key(|r| r.chain);
}

/// The chains `fungibleAccount` asks (stock: `0..20`).
fn all_chains(ctx: &Context<'_>) -> Vec<u8> {
    let n = gql(ctx).miss.as_ref().map_or(20, |m| m.settings().chains);
    (0..n).collect()
}

/// `Block.minerAccount`, the `FungibleChainAccount` node and other single chain-account
/// lookups inside a read (any balance, as every list).
pub fn chain_account_in(
    view: &View,
    account: &str,
    chain: u8,
    module: &str,
) -> ApiResult<Option<FungibleChainAccount>> {
    Ok(chain_rows(view, account, module, Some(&[chain]))?
        .into_iter()
        .next()
        .map(|row| FungibleChainAccount {
            account: account.to_owned(),
            module: module.to_owned(),
            row,
        }))
}

/// Accounts whose guard holds `pk`, with only those rows (on `chain` when given): at most
/// [`MAX_KEY_ACCOUNTS`] of them, and `true` when the walk stopped before the key's last
/// `KeyAccounts` row (L-2). The module filter runs on every candidate before the cut, so
/// accounts of other modules (anyone can guard a spam account with any key) never crowd
/// out the asked module's. The walk stops at the cap or after
/// [`SCAN_WINDOW`](crate::txlist::SCAN_WINDOW) and answers what it found, flagged.
fn by_key(
    view: &View,
    pk: &str,
    module: &str,
    chain: Option<u8>,
) -> ApiResult<(Vec<FungibleAccount>, bool)> {
    let d = view.dict();
    let Some(m) = d.find(DictKind::Module, module) else {
        return Ok((vec![], false));
    };
    if hidden(view, m)? {
        return Ok((vec![], false));
    }
    let coin = balances::coin_module(&view.txn)?;
    let kh = key_hash(pk);
    let stop_at = view
        .deadline
        .min(std::time::Instant::now() + crate::txlist::SCAN_WINDOW);
    let mut out = Vec::new();
    let mut truncated = false;
    let mut failed = None;
    // One account's rows of `m` (any balance, not absent), on `chain`, trusted and guarded by `pk`.
    let rows_of = |a: u32| -> ApiResult<Vec<ChainRow>> {
        let mut prefix = a.to_be_bytes().to_vec();
        prefix.extend_from_slice(&m.to_be_bytes());
        let mut rows = Vec::new();
        for (k, v) in view
            .txn
            .scan_limit(Table::Balances, &prefix, 256, |k| k.starts_with(&prefix))?
        {
            let (Some((_, _, c)), Some(r)) = (parse_bal_key(&k), BalanceRow::decode(&v)) else {
                continue;
            };
            if r.absent
                || chain.is_some_and(|x| x != c)
                || !balances::trusted(&view.txn, m, c, coin)?
                || !balances::stored_guard_keys(&view.txn, &r.guard, c)?
                    .iter()
                    .any(|x| x == pk)
            {
                continue;
            }
            rows.push(ChainRow {
                chain: c,
                balance: r.balance.clone(),
                guard: guard_of(view, &r.guard, c)?,
            });
        }
        Ok(rows)
    };
    view.txn.walk_keys(Table::KeyAccounts, &kh, |k| {
        if !k.starts_with(&kh) {
            return false;
        }
        if out.len() >= MAX_KEY_ACCOUNTS || std::time::Instant::now() >= stop_at {
            truncated = true;
            return false;
        }
        let Some(a) = k
            .get(16..20)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_be_bytes)
        else {
            return true;
        };
        let Some(name) = d.name(DictKind::Account, a) else {
            return true;
        };
        match rows_of(a) {
            Ok(rows) if rows.is_empty() => {}
            Ok(rows) => out.push(FungibleAccount {
                account: name.to_owned(),
                module: module.to_owned(),
                rows,
            }),
            Err(e) => {
                failed = Some(e);
                return false;
            }
        }
        true
    })?;
    if let Some(e) = failed {
        return Err(e);
    }
    Ok((out, truncated))
}

/// A non-negative decimal text (`12`, `0.5`, `9.666e-5`) as `(integer digits, fraction
/// digits)`; `None` for anything else or an exponent beyond ±1,000.
fn plain_decimal(x: &str) -> Option<(String, String)> {
    let (m, e) = match x.find(['e', 'E']) {
        Some(p) => (&x[..p], x[p + 1..].parse::<i32>().ok()?),
        None => (x, 0),
    };
    if !(-1000..=1000).contains(&e) {
        return None;
    }
    let (i, f) = m.split_once('.').unwrap_or((m, ""));
    if i.is_empty()
        || !i.bytes().all(|b| b.is_ascii_digit())
        || !f.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{i}{f}");
    // Position of the decimal point in `digits` after the shift.
    let point = i.len() as i64 + i64::from(e);
    Some(if point <= 0 {
        (
            "0".into(),
            format!("{}{digits}", "0".repeat((-point) as usize)),
        )
    } else if point as usize >= digits.len() {
        (
            format!("{digits}{}", "0".repeat(point as usize - digits.len())),
            String::new(),
        )
    } else {
        (
            digits[..point as usize].to_owned(),
            digits[point as usize..].to_owned(),
        )
    })
}

/// Exact sum of non-negative decimal texts (`None` if one is not a decimal).
pub fn decimal_sum<'a>(xs: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut int: Vec<u8> = vec![0];
    let mut frac: Vec<u8> = Vec::new();
    for x in xs {
        let (i, f) = plain_decimal(x)?;
        let (i, f) = (i.as_str(), f.as_str());
        if i.is_empty()
            || !i.bytes().all(|b| b.is_ascii_digit())
            || !f.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        // Fraction: add digit by digit from the right.
        if f.len() > frac.len() {
            frac.resize(f.len(), 0);
        }
        let mut carry = 0u8;
        for p in (0..frac.len()).rev() {
            let s = frac[p] + f.as_bytes().get(p).map_or(0, |b| b - b'0') + carry;
            frac[p] = s % 10;
            carry = s / 10;
        }
        // Integer part, little-endian digits.
        let digits: Vec<u8> = i.bytes().rev().map(|b| b - b'0').collect();
        if digits.len() + 1 > int.len() {
            int.resize(digits.len() + 1, 0);
        }
        for (p, slot) in int.iter_mut().enumerate() {
            let s = *slot + digits.get(p).copied().unwrap_or(0) + carry;
            *slot = s % 10;
            carry = s / 10;
        }
        if carry > 0 {
            int.push(carry);
        }
    }
    while int.len() > 1 && int.last() == Some(&0) {
        int.pop();
    }
    while frac.last() == Some(&0) {
        frac.pop();
    }
    let mut s: String = int.iter().rev().map(|d| (b'0' + d) as char).collect();
    if !frac.is_empty() {
        s.push('.');
        s.extend(frac.iter().map(|d| (b'0' + d) as char));
    }
    Some(s)
}

fn transfers_spec(account: &str, module: &str, chain: Option<u8>) -> ListSpec {
    ListSpec {
        source: SourceSpec::Xfer {
            account: account.to_owned(),
            module: module.to_owned(),
        },
        filter: KeyFilter {
            chain,
            ..KeyFilter::default()
        },
        rows: Rows::Transfers {
            module: Some(module.to_owned()),
            account: Some(account.to_owned()),
            nft: false,
        },
    }
}

fn tx_of(it: Item) -> Option<Transaction> {
    match it {
        Item::Tx(d) => Transaction::new(d),
        _ => None,
    }
}

fn transfer_of(it: Item) -> Option<Transfer> {
    match it {
        Item::Transfer(d, i, x) => Some(Transfer::new(d, i, x)),
        _ => None,
    }
}

#[Object]
impl FungibleAccount {
    async fn account_name(&self) -> &str {
        &self.account
    }
    async fn chain_accounts(&self) -> Vec<FungibleChainAccount> {
        self.rows
            .iter()
            .map(|row| FungibleChainAccount {
                account: self.account.clone(),
                module: self.module.clone(),
                row: row.clone(),
            })
            .collect()
    }
    async fn fungible_name(&self) -> &str {
        &self.module
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::FungibleAccount {
            fungible: self.module.clone(),
            account: self.account.clone(),
        })))
    }
    async fn total_balance(&self) -> Result<Decimal> {
        decimal_sum(self.rows.iter().map(|r| r.balance.as_str()))
            .and_then(|s| Decimal::new(&s))
            .ok_or_else(|| crate::error::err(crate::error::Code::Internal, "balance not summable"))
    }
    /// Txs with a transfer of this fungible on the account's side (`xfer` postings).
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleAccountTransactionsConnection> {
        let mut spec = transfers_spec(&self.account, &self.module, None);
        spec.rows = Rows::Txs {
            sender: None,
            nft: false,
            module: None,
            coinbase: false,
        };
        spec.filter.user_only = true;
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(FungibleAccountTransactionsConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| {
                    tx_of(it).map(|node| FungibleAccountTransactionsConnectionEdge { cursor, node })
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleAccountTransfersConnection> {
        let spec = transfers_spec(&self.account, &self.module, None);
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(FungibleAccountTransfersConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| {
                    transfer_of(it)
                        .map(|node| FungibleAccountTransfersConnectionEdge { cursor, node })
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
}

#[Object]
impl FungibleChainAccount {
    async fn account_name(&self) -> &str {
        &self.account
    }
    async fn balance(&self) -> f64 {
        self.row.balance.parse().unwrap_or(0.0)
    }
    async fn chain_id(&self) -> String {
        self.row.chain.to_string()
    }
    async fn fungible_name(&self) -> &str {
        &self.module
    }
    async fn guard(&self) -> Sel<&IGuard> {
        Sel(&self.row.guard)
    }
    pub async fn id(&self) -> Result<ID> {
        Ok(ID(encode(&NodeKey::FungibleChainAccount {
            chain: self.row.chain.to_string(),
            fungible: self.module.clone(),
            account: self.account.clone(),
        })))
    }
    /// Txs on this chain whose `meta.sender` is the account (schema text).
    async fn transactions(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleChainAccountTransactionsConnection> {
        let spec = ListSpec {
            // The `sndr` postings on a sender-ready view (FIX-2), else `acct` + the filter.
            source: SourceSpec::Sndr(self.account.clone()),
            filter: KeyFilter {
                chain: Some(self.row.chain),
                user_only: true,
                ..KeyFilter::default()
            },
            rows: Rows::Txs {
                sender: Some(self.account.clone()),
                nft: false,
                module: None,
                coinbase: false,
            },
        };
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(FungibleChainAccountTransactionsConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| {
                    tx_of(it)
                        .map(|node| FungibleChainAccountTransactionsConnectionEdge { cursor, node })
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
    async fn transfers(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleChainAccountTransfersConnection> {
        let spec = transfers_spec(&self.account, &self.module, Some(self.row.chain));
        let p = list(ctx, spec, PageArgs::new(after, before, first, last)).await?;
        Ok(FungibleChainAccountTransfersConnection {
            edges: p
                .edges
                .into_iter()
                .filter_map(|(cursor, it)| {
                    transfer_of(it)
                        .map(|node| FungibleChainAccountTransfersConnectionEdge { cursor, node })
                })
                .collect(),
            page_info: p.info,
            total: p.total,
        })
    }
}

// ---------------------------------------------------------------------------------------
// Query roots

fn names(account: &str, module: Option<&str>) -> Result<(String, String)> {
    let account = crate::txlist::name_arg("accountName", account)?.to_owned();
    let module = crate::txlist::fungible_arg(module.unwrap_or("coin"))?.to_owned();
    Ok((account, module))
}

fn chain_list(ids: &[String]) -> Result<Vec<u8>> {
    if ids.len() > 100 {
        return Err(bad_input("at most 100 chainIds"));
    }
    ids.iter().map(|s| crate::txlist::chain_arg(s)).collect()
}

/// `fungibleAccount` and the `FungibleAccount` node.
pub async fn fungible_account(
    ctx: &Context<'_>,
    account: &str,
    module: Option<&str>,
) -> Result<Option<FungibleAccount>> {
    let (account, module) = names(account, module)?;
    let (mut rows, ok) = gql(ctx)
        .state()?
        .read({
            let (a, m) = (account.clone(), module.clone());
            move |view| stored_rows(view, &a, &m, None)
        })
        .await
        .map_err(from_api)?;
    with_misses(ctx, &account, &module, ok, &all_chains(ctx), &mut rows).await;
    // No row on any chain (stored, or found by the node read): it does not exist on chain.
    Ok((!rows.is_empty()).then_some(FungibleAccount {
        account,
        module,
        rows,
    }))
}

/// The `FungibleChainAccount` node: the stored row of the chain, served at 0 too (an id
/// handed out before the account was drained still resolves, with its history; I-1).
pub async fn fungible_chain_account_node(
    ctx: &Context<'_>,
    account: &str,
    chain: &str,
    module: &str,
) -> Result<Option<FungibleChainAccount>> {
    let (account, module) = names(account, Some(module))?;
    let c = crate::txlist::chain_arg(chain)?;
    gql(ctx)
        .state()?
        .read(move |view| chain_account_in(view, &account, c, &module))
        .await
        .map_err(from_api)
}

/// `fungibleChainAccounts` (and `fungibleChainAccount` with one chain). `chain_ids` absent
/// or empty means every chain (FIX-4 item 1: an undefined `$chainIds` reaches the resolver
/// as absent, and stock reads `[]` as every chain too). The answer is always a list, `[]`
/// when there are no rows, as stock answers.
pub async fn fungible_chain_accounts(
    ctx: &Context<'_>,
    account: &str,
    chain_ids: Option<&[String]>,
    module: Option<&str>,
) -> Result<Vec<FungibleChainAccount>> {
    let (account, module) = names(account, module)?;
    let chains = match chain_ids {
        None | Some([]) => None,
        Some(ids) => Some(chain_list(ids)?),
    };
    let order = chains.clone();
    let asked = chains.clone().unwrap_or_else(|| all_chains(ctx));
    let (mut rows, ok) = gql(ctx)
        .state()?
        .read({
            let (a, m) = (account.clone(), module.clone());
            move |view| stored_rows(view, &a, &m, chains.as_deref())
        })
        .await
        .map_err(from_api)?;
    with_misses(ctx, &account, &module, ok, &asked, &mut rows).await;
    // The rows come in the order of the asked chains (stock's order), each chain once.
    let rows = match order {
        None => rows,
        Some(order) => {
            let mut rows: Vec<Option<ChainRow>> = rows.into_iter().map(Some).collect();
            let mut out = Vec::with_capacity(rows.len());
            for c in order {
                if let Some(row) = rows
                    .iter_mut()
                    .find(|r| r.as_ref().is_some_and(|r| r.chain == c))
                    .and_then(Option::take)
                {
                    out.push(row);
                }
            }
            out
        }
    };
    Ok(rows
        .into_iter()
        .map(|row| FungibleChainAccount {
            account: account.clone(),
            module: module.clone(),
            row,
        })
        .collect())
}

/// `fungibleAccountsByPublicKey` / `fungibleChainAccountsByPublicKey`.
pub async fn accounts_by_public_key(
    ctx: &Context<'_>,
    public_key: &str,
    module: Option<&str>,
    chain: Option<u8>,
) -> Result<Vec<FungibleAccount>> {
    let pk = crate::txlist::name_arg("publicKey", public_key)?.to_owned();
    let module = crate::txlist::fungible_arg(module.unwrap_or("coin"))?.to_owned();
    let (out, truncated) = gql(ctx)
        .state()?
        .read(move |view| by_key(view, &pk, &module, chain))
        .await
        .map_err(from_api)?;
    if truncated {
        crate::flags::mark(ctx, crate::flags::Flag::KeyAccountsTruncated);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------------
// ZelCore extension: Query.balance (R-P5P9-16)

/// Fungible rows `balance(accountName)` reads for one account at most (all modules and
/// chains); an account with more answers `LIMIT`.
pub const MAX_BALANCE_ROWS: usize = 4_096;

/// One row of `balance` (ZelCore extension): an account's balance of one module on one chain.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct BalanceNode {
    pub account_name: String,
    pub module: String,
    pub chain_id: String,
    pub balance: String,
}

/// The text `BalanceNode.balance` serves for a stored balance (the node's text, e.g.
/// `2.101e-3` or `6184.514948000000000000000000000000`): the layout of JavaScript's
/// `Number.prototype.toString`, as stock serves it, over the exact digits (never through
/// f64). No trailing fraction zeros and no `.0` (`110970.000000000000` is `110970`, `0.0` is
/// `0`); plain notation for 1e-6 <= |x| < 1e21 (`2.101e-3` is `0.002101`); exponent notation
/// outside it (`1e-12`, `2.7397e-8`, `1.5e+21`). Every digit the node gave is kept, so a value
/// with more than 17 significant digits differs from stock's float-rounded text in value, not
/// in layout. A text that is not a plain decimal is served as stored.
pub fn balance_text(stored: &str) -> String {
    crate::scalars::js_number(stored)
}

/// A `balance` cursor: the `(module, chain)` of a row, as `bal1:<chain>:<module>` in
/// base64url (rows are listed by module name, then chain).
pub fn balance_cursor(module: &str, chain: u8) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("bal1:{chain}:{module}"))
}

pub fn decode_balance_cursor(s: &str) -> Result<(String, u8)> {
    use base64::Engine;
    let bad = || bad_input("invalid cursor");
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .map_err(|_| bad())?;
    let t = String::from_utf8(raw).map_err(|_| bad())?;
    let rest = t.strip_prefix("bal1:").ok_or_else(bad)?;
    let (c, m) = rest.split_once(':').ok_or_else(bad)?;
    Ok((m.to_owned(), c.parse::<u8>().map_err(|_| bad())?))
}

/// Transfer keys read per (account, module) by `balance(...)`'s zero-row probe
/// ([`xfer_touched`]). Keys are height-major, so a probe for a chain the account rarely uses
/// can walk the whole list; this bounds it.
pub const BALANCE_TOUCH_KEYS: usize = 10_000;

/// Transfer keys one `balance(...)` request reads in all its zero-row probes, as a multiple
/// of the per-module cap (`balance_touch_keys`): an account with thousands of zero rows in
/// distinct modules does not read thousands of full per-module budgets on a normal read slot
/// (N-12). A module probed after the total is spent is capped (its unresolved rows are kept).
pub const BALANCE_TOUCH_MODULES: usize = 4;

/// Chains among `want` on which the `xfer` posting list of `(a, m)` has a key, newest first,
/// reading at most `cap` keys, and at most `*budget` (decreased by the keys read). Stops early
/// once every wanted chain is found. The flag is true when a cap ended the walk with some
/// wanted chain still unfound. `check` is the request's deadline, checked every 64 keys; a
/// passed deadline (the walk's own or the request's) is an error, never a cap (N-12).
fn xfer_touched(
    view: &View,
    (a, m): (u32, u32),
    want: &BTreeSet<u8>,
    cap: usize,
    budget: &mut usize,
    check: &dyn Fn() -> ApiResult<()>,
) -> ApiResult<(BTreeSet<u8>, bool)> {
    let mut found = BTreeSet::new();
    check()?;
    let mut it = view.postings(Family::Xfer, xfer_entity(a, m), PostingQuery::default())?;
    let cap = cap.min(*budget);
    let mut read = 0usize;
    let out = loop {
        if found.len() >= want.len() {
            break (found, false);
        }
        if read >= cap {
            // Capped only if keys remain unread; an error (the deadline) is returned.
            let more = it.next().transpose()?.is_some();
            break (found, more);
        }
        let Some(k) = it.next() else {
            break (found, false);
        };
        read += 1;
        if read.is_multiple_of(64) {
            check()?;
        }
        let c = k?.chain();
        if want.contains(&c) {
            found.insert(c);
        }
    };
    *budget -= read;
    Ok(out)
}

/// `balance(accountName, module, chainIds)`: an account's fungible rows above 0, plus its rows
/// at 0 that a stock TRANSFER touched (stock's `Balances` set: the holders table, or
/// [`xfer_touched`] while it is not ready), listed by module name then
/// chain, filtered by `module` and `chainIds`. Without `accountName`, a module's holders
/// ([`query_holders`]); without both, stock's error.
#[allow(clippy::too_many_arguments)]
pub async fn query_balance(
    ctx: &Context<'_>,
    account_name: Option<String>,
    chain_ids: Option<Vec<String>>,
    module: Option<String>,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> Result<super::QueryBalanceConnection> {
    use crate::connection::{finish, Dir, Page, PageCursor};
    // Stock's argument rule (`balance-query-resolver.ts`): an empty string counts as absent.
    let Some(account) = account_name.filter(|a| !a.is_empty()) else {
        return match module.filter(|m| !m.is_empty()) {
            Some(m) => query_holders(ctx, chain_ids, m, after, before, first, last).await,
            None => Err(either_account_or_module()),
        };
    };
    let account = crate::txlist::name_arg("accountName", &account)?.to_owned();
    let module = module
        .as_deref()
        .map(|m| crate::txlist::name_arg("module", m).map(str::to_owned))
        .transpose()?;
    // At most 100 chain ids, each once; `[]` is every chain, as on the other chain lists
    // (audit F-28).
    let chains: Option<Vec<u8>> = chain_ids
        .as_deref()
        .map(|cs| super::block::chain_set(Some(cs)))
        .transpose()?;
    let g = gql(ctx);
    // The count and cursor rules of every connection (`PageArgs::parse`, with the real
    // `after`/`before` flags; audit F-28); the cursor is our own.
    if first.is_some_and(|n| n < 0) || last.is_some_and(|n| n < 0) {
        return Err(bad_input("first and last must not be negative"));
    }
    if first.is_some_and(|n| n != 0) && last.is_some_and(|n| n != 0) {
        return Err(bad_input("pass first or last, not both"));
    }
    if after.is_some() && before.is_some() {
        return Err(bad_input("pass after or before, not both"));
    }
    let (dir, n) = crate::connection::served_page(
        first.map(i64::from),
        last.map(i64::from),
        after.is_some(),
        before.is_some(),
        g.settings.default_page as u64,
    );
    let n = n as usize;
    if n > crate::MAX_PAGE_ROWS {
        return Err(crate::error::page_limit());
    }
    match (dir, &after, &before) {
        (Dir::Forward, None, Some(_)) => return Err(bad_input("before goes with last")),
        (Dir::Backward, Some(_), None) => return Err(bad_input("after goes with first")),
        _ => {}
    }
    let cur = after
        .as_deref()
        .or(before.as_deref())
        .map(decode_balance_cursor)
        .transpose()?;
    let acct = account.clone();
    let touch_keys = g.settings.balance_touch_keys;
    // The request's deadline as well as the read slot's (N-12).
    let dl = crate::deadline(ctx);
    let check = move || -> ApiResult<()> {
        if std::time::Instant::now() >= dl {
            return Err(kdx_api::ApiError::Deadline);
        }
        Ok(())
    };
    let mut rows: Vec<(String, u8, String)> = g
        .state()?
        .read(move |view| {
            let d = view.dict();
            let Some(a) = d.find(DictKind::Account, &acct) else {
                return Ok(Vec::new());
            };
            let prefix = a.to_be_bytes().to_vec();
            let found =
                view.txn
                    .scan_limit(Table::Balances, &prefix, MAX_BALANCE_ROWS + 1, |k| {
                        k.starts_with(&prefix)
                    })?;
            if found.len() > MAX_BALANCE_ROWS {
                return Err(kdx_api::ApiError::Unavailable(format!(
                    "the account has more than {MAX_BALANCE_ROWS} balance rows"
                )));
            }
            let mut out = Vec::new();
            // Rows at 0 wait for the transfer probe of their module (stock's rule, below).
            let mut zero: BTreeMap<u32, (String, BTreeMap<u8, String>)> = BTreeMap::new();
            for (k, v) in found {
                view.check_deadline()?;
                check()?;
                let (Some((_, m, c)), Some(r)) = (parse_bal_key(&k), BalanceRow::decode(&v)) else {
                    continue;
                };
                if r.absent || chains.as_ref().is_some_and(|cs| !cs.contains(&c)) {
                    continue;
                }
                let Some(name) = d.name(DictKind::Module, m).map(str::to_owned) else {
                    continue;
                };
                if module.as_ref().is_some_and(|x| *x != name) || hidden(view, m)? {
                    continue;
                }
                if r.positive() {
                    out.push((name, c, balance_text(&r.balance)));
                } else {
                    zero.entry(m)
                        .or_insert_with(|| (name, BTreeMap::new()))
                        .1
                        .insert(c, balance_text(&r.balance));
                }
            }
            // Stock lists a row at 0 only when a TRANSFER event touched it: the account's
            // transfer list in that module has a key on the row's chain (the list
            // `transfers(accountName, fungibleName, chainId)` reads). Rows above 0 are always
            // listed. Our transfer postings come only from node events, which start at the
            // events fork, as stock's TRANSFER rows do, so no height bound is needed.
            //
            // Secpass A I-4 / B M-14: stock's rule is its `Balances` set (an event named exactly
            // `TRANSFER`, stock's amount forms), which the holders table holds exactly: one
            // `HolderFirst` read per zero row. The `xfer` postings follow chainweb-data's wider
            // rule (a name ending in `TRANSFER`, more amount forms), so they are only the
            // fallback, capped, while the holders table is not ready.
            let holders_ready = kdx_hot::holders::ready(&view.txn)?;
            let mut budget = touch_keys.saturating_mul(BALANCE_TOUCH_MODULES);
            for (m, (name, rows)) in zero {
                let want: BTreeSet<u8> = rows.keys().copied().collect();
                let (touched, capped) = if holders_ready {
                    let mut t = BTreeSet::new();
                    for &c in &want {
                        if kdx_hot::holders::first_of(&view.txn, a, m, c)?.is_some() {
                            t.insert(c);
                        }
                    }
                    (t, false)
                } else {
                    xfer_touched(view, (a, m), &want, touch_keys, &mut budget, &check)?
                };
                for (c, text) in rows {
                    if touched.contains(&c) {
                        out.push((name.clone(), c, text));
                    } else if capped {
                        // Not resolved within the cap: kept, the safe side (the row exists on
                        // chain), and counted.
                        kdx_metrics::get().graphql_balance_touch_capped.inc();
                        out.push((name.clone(), c, text));
                    }
                }
            }
            Ok(out)
        })
        .await
        .map_err(from_api)?;
    rows.sort_by(|a, b| (a.0.as_str(), a.1).cmp(&(b.0.as_str(), b.1)));
    let key = |r: &(String, u8, String)| (r.0.clone(), r.1);
    let walked: Vec<(String, u8, String)> = match (dir, &cur) {
        (Dir::Forward, _) => rows
            .into_iter()
            .filter(|r| cur.as_ref().is_none_or(|c| key(r) > *c))
            .take(n + 1)
            .collect(),
        (Dir::Backward, _) => rows
            .into_iter()
            .rev()
            .filter(|r| cur.as_ref().is_none_or(|c| key(r) < *c))
            .take(n + 1)
            .collect(),
    };
    let page: Page<PageCursor> = Page {
        dir,
        n,
        // Only its presence is read (hasPreviousPage / hasNextPage).
        cursor: cur.as_ref().map(|_| {
            PageCursor::Block(crate::connection::BlockCursor {
                height: 0,
                chain: 0,
            })
        }),
    };
    let (edges, page_info) = finish(walked, &page, |r| balance_cursor(&r.0, r.1));
    Ok(super::QueryBalanceConnection {
        edges: edges
            .into_iter()
            .map(|(cursor, (m, c, b))| super::QueryBalanceConnectionEdge {
                cursor,
                node: BalanceNode {
                    account_name: account.clone(),
                    module: m,
                    chain_id: c.to_string(),
                    balance: b,
                },
            })
            .collect(),
        page_info,
    })
}

/// Stock's message when `balance` gets neither `accountName` nor `module`
/// (`balance-query-resolver.ts`), with the code stock answers a thrown resolver error with.
pub const EITHER_ACCOUNT_OR_MODULE: &str = "Either accountName or module must be provided.";

fn either_account_or_module() -> async_graphql::Error {
    use async_graphql::ErrorExtensions;
    async_graphql::Error::new(EITHER_ACCOUNT_OR_MODULE)
        .extend_with(|_, e| e.set("code", crate::error::INTERNAL_SERVER_ERROR))
}

/// A holders cursor: the row's first transfer (TxKey) and AccountId, as
/// `hld1:<txkey>:<account>` in base64url. Stock's cursor is its own row id.
pub fn holders_cursor(first: u64, account: u32) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("hld1:{first}:{account}"))
}

pub fn decode_holders_cursor(s: &str) -> Result<(u64, u32)> {
    use base64::Engine;
    let bad = || bad_input("invalid cursor");
    let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .map_err(|_| bad())?;
    let t = String::from_utf8(raw).map_err(|_| bad())?;
    let rest = t.strip_prefix("hld1:").ok_or_else(bad)?;
    let (f, a) = rest.split_once(':').ok_or_else(bad)?;
    Ok((
        f.parse::<u64>().map_err(|_| bad())?,
        a.parse::<u32>().map_err(|_| bad())?,
    ))
}

/// One served holder: first TxKey, AccountId, account name, chain, balance text.
type HolderRow = (u64, u32, String, u8, String);

/// `balance(module, chainIds)` without `accountName`: the module's holders, stock's
/// `getAccountBalances` holders mode. Every `(account, chain)` a TRANSFER of the module named,
/// at any balance (zero included), newest first transfer first (stock: newest insert id
/// first; `kdx_hot::holders` explains the order). The balance is the stored row's
/// (stock reads the node per row); a row with no stored balance, or one the node reported
/// absent, is `"0"`, as stock's failed node read.
///
/// Cost: the index only, one bounded seek per chain (at most `first`/`last` + 1 keys each),
/// under the page cap (`MAX_PAGE_ROWS`, `LIMIT` above it) and the request deadline, checked
/// inside the walk. Stock lets any `first` through and reads the node once per row; ours never
/// reads the node here. `NOT_AVAILABLE` while the holders table is not ready (a store built
/// before it, until `kda-indexer reconcile-holders`).
#[allow(clippy::too_many_arguments)]
pub async fn query_holders(
    ctx: &Context<'_>,
    chain_ids: Option<Vec<String>>,
    module: String,
    after: Option<String>,
    before: Option<String>,
    first: Option<i32>,
    last: Option<i32>,
) -> Result<super::QueryBalanceConnection> {
    use crate::connection::{finish, Dir, Page, PageCursor};
    use kdx_hot::holders::{self, Order};
    let module = crate::txlist::name_arg("module", &module)?.to_owned();
    // At most 100 ids, each once; absent or `[]` is every chain (`chain_set`).
    let chains: Vec<u8> = super::block::chain_set(chain_ids.as_deref())?;
    let g = gql(ctx);
    if first.is_some_and(|n| n < 0) || last.is_some_and(|n| n < 0) {
        return Err(bad_input("first and last must not be negative"));
    }
    if first.is_some_and(|n| n != 0) && last.is_some_and(|n| n != 0) {
        return Err(bad_input("pass first or last, not both"));
    }
    if after.is_some() && before.is_some() {
        return Err(bad_input("pass after or before, not both"));
    }
    let (dir, n) = crate::connection::served_page(
        first.map(i64::from),
        last.map(i64::from),
        after.is_some(),
        before.is_some(),
        g.settings.default_page as u64,
    );
    let n = n as usize;
    if n > crate::MAX_PAGE_ROWS {
        return Err(crate::error::page_limit());
    }
    match (dir, &after, &before) {
        (Dir::Forward, None, Some(_)) => return Err(bad_input("before goes with last")),
        (Dir::Backward, Some(_), None) => return Err(bad_input("after goes with first")),
        _ => {}
    }
    let cur = after
        .as_deref()
        .or(before.as_deref())
        .map(decode_holders_cursor)
        .transpose()?;
    let order = match dir {
        Dir::Forward => Order::NewestFirst,
        Dir::Backward => Order::OldestFirst,
    };
    let dl = crate::deadline(ctx);
    let name = module.clone();
    // `None`: the table is not ready.
    let rows: Option<Vec<HolderRow>> = g
        .state()?
        .read(move |view| {
            if !holders::ready(&view.txn)? {
                return Ok(None);
            }
            let d = view.dict();
            let Some(m) = d.find(DictKind::Module, &name) else {
                return Ok(Some(Vec::new()));
            };
            let mut check = || -> anyhow::Result<()> {
                if std::time::Instant::now() >= dl {
                    anyhow::bail!(kdx_api::ApiError::Deadline);
                }
                Ok(())
            };
            // Review M-1 of PR #54: a row whose AccountId has no dictionary name (only on a
            // damaged dictionary) is skipped and counted, and the page keeps filling past it,
            // so it is never cut short and `hasNextPage` stays right. Each round reads at most
            // `chains × (n + 1)` keys; the deadline bounds the rounds.
            let ledger = kdx_decode::build::MARMALADE_LEDGERS.contains(&name.as_str());
            let mut out = Vec::with_capacity(n + 1);
            let mut at = cur;
            let mut skipped = 0u64;
            loop {
                let want = n + 1 - out.len();
                let page = holders::page(&view.txn, m, &chains, order, at, want, &mut check)
                    .map_err(|e| match e.downcast::<kdx_api::ApiError>() {
                        Ok(a) => a,
                        Err(e) => kdx_api::ApiError::from(e),
                    })?;
                let last = page.len() < want;
                at = page.last().map(holders::Holder::order).or(at);
                for h in page {
                    view.check_deadline()?;
                    let Some(acct) = d.name(DictKind::Account, h.account) else {
                        skipped += 1;
                        continue;
                    };
                    // A marmalade ledger has no account-wide balance: stock's `details` read
                    // fails there and it answers 0 (review M-3 of PR #54).
                    let bal = if ledger {
                        "0".to_owned()
                    } else {
                        view.txn
                            .get(Table::Balances, &balances::bal_key(h.account, m, h.chain))?
                            .and_then(|v| BalanceRow::decode(&v))
                            .filter(|r| !r.absent)
                            .map_or_else(|| "0".to_owned(), |r| balance_text(&r.balance))
                    };
                    out.push((h.first.0, h.account, acct.to_owned(), h.chain, bal));
                }
                if last || out.len() > n {
                    break;
                }
            }
            if skipped > 0 {
                tracing::warn!(
                    module = %name,
                    skipped,
                    "holders: rows whose AccountId has no dictionary name were skipped"
                );
            }
            Ok(Some(out))
        })
        .await
        .map_err(from_api)?;
    let Some(rows) = rows else {
        return Err(crate::error::err(
            crate::error::Code::NotAvailable,
            "the module holders list is being built (kda-indexer reconcile-holders)",
        ));
    };
    let page: Page<PageCursor> = Page {
        dir,
        n,
        // Only its presence is read (hasPreviousPage / hasNextPage).
        cursor: cur.map(|_| {
            PageCursor::Block(crate::connection::BlockCursor {
                height: 0,
                chain: 0,
            })
        }),
    };
    let (edges, page_info) = finish(rows, &page, |r| holders_cursor(r.0, r.1));
    Ok(super::QueryBalanceConnection {
        edges: edges
            .into_iter()
            .map(
                |(cursor, (_, _, a, c, b))| super::QueryBalanceConnectionEdge {
                    cursor,
                    node: BalanceNode {
                        account_name: a,
                        module: module.clone(),
                        chain_id: c.to_string(),
                        balance: b,
                    },
                },
            )
            .collect(),
        page_info,
    })
}

#[cfg(test)]
mod tests {
    use super::{balance_text, decimal_sum};

    #[test]
    fn balance_text_follows_stock_layout() {
        for (stored, served) in [
            // zero and whole numbers: no fraction, no `.0`
            ("0", "0"),
            ("0.0", "0"),
            ("0.000000000000", "0"),
            ("-0.0", "0"),
            ("2", "2"),
            ("2.0", "2"),
            ("110970.000000000000", "110970"),
            ("47500095.000000000000", "47500095"),
            ("20000000000", "20000000000"),
            // trailing fraction zeros dropped
            ("6184.514948000000000000000000000000", "6184.514948"),
            ("1.424282260000000000000000000000", "1.42428226"),
            ("0.000021690000000000000000000000", "0.00002169"),
            ("50075.391537750000", "50075.39153775"),
            // scientific notation expanded inside 1e-7 < x < 1e21
            ("2.101e-3", "0.002101"),
            ("2.53e-6", "0.00000253"),
            ("3.7e-5", "0.000037"),
            ("9.40000001e-3", "0.00940000001"),
            ("1e-6", "0.000001"),
            ("9.9e-7", "9.9e-7"),
            ("2.7026800518384e+7", "27026800.518384"),
            ("5.499517958445245e+9", "5499517958.445245"),
            ("1E2", "100"),
            // very small: exponent notation, as stock (`1e-7` and below)
            ("1e-7", "1e-7"),
            ("0.0000001", "1e-7"),
            ("1e-12", "1e-12"),
            ("0.000000000001", "1e-12"),
            ("0.000000000000000000000000000001", "1e-30"),
            ("2.7397e-8", "2.7397e-8"),
            ("0.000000027397000", "2.7397e-8"),
            // every digit kept (no f64): 18+ significant digits, small and large
            (
                "0.000000000000481481481481481481481",
                "4.81481481481481481481e-13",
            ),
            ("999999501272.013588926082", "999999501272.013588926082"),
            ("1534374.999999999999", "1534374.999999999999"),
            (
                "1234567890.123456789012345678901234",
                "1234567890.123456789012345678901234",
            ),
            (
                "1234567890123456789012345678901234.5",
                "1.2345678901234567890123456789012345e+33",
            ),
            ("123456789012345678901", "123456789012345678901"),
            ("1234567890123456789012", "1.234567890123456789012e+21"),
            ("1.5e21", "1.5e+21"),
            // not a plain decimal: served as stored
            ("", ""),
            ("abc", "abc"),
        ] {
            assert_eq!(balance_text(stored), served, "{stored}");
        }
    }

    /// Stored (ours, stock) text pairs of equal value from the G4 replay's `balance(...)`
    /// records: ours' stored text now renders as stock's.
    #[test]
    fn balance_text_matches_stored_stock_pairs() {
        for (ours, stock) in [
            ("2.101e-3", "0.002101"),
            ("2.277e-3", "0.002277"),
            ("5.62e-3", "0.00562"),
            ("1.1482e-3", "0.0011482"),
            ("4.0532e-3", "0.0040532"),
            ("9.743201e-2", "0.09743201"),
            ("2.05e-4", "0.000205"),
            ("6184.514948000000000000000000000000", "6184.514948"),
            ("321.291985000000000000000000000000", "321.291985"),
            ("940.129019906250000000000000000000", "940.12901990625"),
            ("0.000994370000000000000000000000", "0.00099437"),
            ("28202.257352331260", "28202.25735233126"),
            ("110843.240113370000000000000000000000", "110843.24011337"),
            ("110970.000000000000", "110970"),
            ("10310.862583436989", "10310.862583436989"),
            ("8400", "8400"),
            ("0", "0"),
            ("1e-12", "1e-12"),
            ("2.7397e-8", "2.7397e-8"),
        ] {
            assert_eq!(balance_text(ours), stock, "{ours}");
        }
    }

    #[test]
    fn exact_decimal_sums() {
        assert_eq!(decimal_sum(["1.5", "2.5"]).as_deref(), Some("4"));
        assert_eq!(
            decimal_sum(["0.000000000001", "99999999999.999999999999"]).as_deref(),
            Some("100000000000")
        );
        assert_eq!(
            decimal_sum(["12", "0.30", "7.05"]).as_deref(),
            Some("19.35")
        );
        assert_eq!(decimal_sum([]).as_deref(), Some("0"));
        assert_eq!(decimal_sum(["1e5"]).as_deref(), Some("100000"));
        assert_eq!(
            decimal_sum(["9.666e-5", "0.0001"]).as_deref(),
            Some("0.00019666")
        );
        assert_eq!(decimal_sum(["1.5E2", "0.25"]).as_deref(), Some("150.25"));
        assert_eq!(decimal_sum(["1e100000"]), None);
        assert_eq!(decimal_sum(["1.x"]), None);
        assert_eq!(decimal_sum(["-1"]), None);
        assert_eq!(decimal_sum([".5"]), None);
    }
}

#[cfg(test)]
mod balance_tests {
    use super::*;

    #[test]
    fn balance_cursors_round_trip_and_refuse_junk() {
        let c = balance_cursor("free.my:token", 19);
        assert_eq!(
            decode_balance_cursor(&c).unwrap(),
            ("free.my:token".to_owned(), 19)
        );
        for bad in ["", "!!", "YmFsMTp4", "YmFsMjoxOmNvaW4"] {
            assert!(decode_balance_cursor(bad).is_err(), "{bad}");
        }
    }
}

#[cfg(test)]
mod by_key_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kdx_api::{Api, ApiSettings, ApiState, Store, TipWatch};
    use kdx_hot::balances::{bal_key, guard_hash, key_account_key, key_hash, BalanceRow};
    use kdx_hot::{HotDict, Table};
    use kdx_types::{DictKind, DictMut};

    use super::by_key;

    /// `k1` guards 150 accounts of `free.spam` and then one `coin` account, `victim`
    /// (interned last, so its `KeyAccounts` row sorts last); `k2` guards 101 `coin` accounts.
    fn fixture() -> (tempfile::TempDir, Arc<ApiState>) {
        let d = tempfile::tempdir().unwrap();
        let g = kdx_hot::Geometry {
            max_gb: 1,
            growth_mb: 16,
        };
        let hot = Arc::new(kdx_hot::HotStore::open(&d.path().join("hot"), g).unwrap());
        let mut w = hot.rw().unwrap();
        let mut dict = HotDict::load(&w).unwrap();
        let coin = dict.intern(DictKind::Module, "coin");
        let spam = dict.intern(DictKind::Module, "free.spam");
        let mut rows = Vec::new();
        for i in 0..150 {
            rows.push(("k1", dict.intern(DictKind::Account, &format!("s{i}")), spam));
        }
        rows.push(("k1", dict.intern(DictKind::Account, "victim"), coin));
        for i in 0..101 {
            rows.push(("k2", dict.intern(DictKind::Account, &format!("c{i}")), coin));
        }
        // `k3` guards `drained` (every row at 0) and `mixed` (1 on chain 0, 0 on chain 1).
        let drained = dict.intern(DictKind::Account, "drained");
        let mixed = dict.intern(DictKind::Account, "mixed");
        dict.persist(&mut w).unwrap();
        let mut balance_rows: Vec<(&str, u32, u32, u8, &str)> = rows
            .into_iter()
            .map(|(pk, a, m)| (pk, a, m, 0, "1"))
            .collect();
        balance_rows.extend([
            ("k3", drained, coin, 0, "0.0"),
            ("k3", drained, coin, 1, "0"),
            ("k3", mixed, coin, 0, "1"),
            ("k3", mixed, coin, 1, "0.000"),
        ]);
        for (pk, acct, m, c, bal) in balance_rows {
            let guard = format!(r#"{{"keys":["{pk}"],"pred":"keys-all"}}"#);
            let gh = guard_hash(guard.as_bytes());
            w.put(Table::Guards, &gh, guard.as_bytes()).unwrap();
            let row = BalanceRow {
                absent: false,
                balance: bal.into(),
                guard: gh,
                as_of: 1,
                reconciled_at: 1,
                unverified: false,
            };
            w.put(Table::Balances, &bal_key(acct, m, c), &row.encode())
                .unwrap();
            w.put(
                Table::KeyAccounts,
                &key_account_key(&key_hash(pk), acct),
                &[],
            )
            .unwrap();
        }
        w.commit().unwrap();
        let store = Arc::new(Store::open(hot, d.path()).unwrap());
        let settings = ApiSettings {
            network: "mainnet01".into(),
            deadline: Duration::from_secs(5),
            max_in_flight: 16,
            max_body_bytes: 8 << 20,
            v1txs_max_body_bytes: 8 << 20,
            v1txs_deadline: std::time::Duration::from_secs(30),
            rate_per_ip: 20,
            rate_burst: 40,
            trusted_proxies: Vec::new(),
            read_threads: 2,
            ready_max_lag: 10,
            max_event_lists: 64,
        };
        (d, Api::new(store, settings, TipWatch::default()).state())
    }

    async fn names(st: &Arc<ApiState>, pk: &'static str) -> (Vec<String>, bool) {
        let (accts, cut) = st
            .read(move |view| by_key(view, pk, "coin", None))
            .await
            .unwrap();
        (accts.into_iter().map(|a| a.account).collect(), cut)
    }

    /// L-2: rows of other modules never crowd the asked module out of the 100-account cut,
    /// and a cut answer says so.
    #[tokio::test]
    async fn module_filter_runs_before_the_cut_and_a_cut_is_flagged() {
        let (_d, st) = fixture();
        assert_eq!(names(&st, "k1").await, (vec!["victim".to_owned()], false));
        let (k2, cut) = names(&st, "k2").await;
        assert_eq!((k2.len(), cut), (super::MAX_KEY_ACCOUNTS, true));
    }

    /// Owner ruling 2026-11-26 (stock compatibility): the `…ByPublicKey` lists hold every row
    /// that exists on chain at any balance, so an account with every row at 0 is listed too.
    #[tokio::test]
    async fn rows_at_zero_are_listed_by_key() {
        let (_d, st) = fixture();
        let accts = st
            .read(|view| by_key(view, "k3", "coin", None))
            .await
            .unwrap()
            .0;
        let got: Vec<(String, Vec<u8>)> = accts
            .into_iter()
            .map(|a| (a.account, a.rows.iter().map(|r| r.chain).collect()))
            .collect();
        let mut got = got;
        got.sort();
        assert_eq!(
            got,
            [
                ("drained".to_owned(), vec![0, 1]),
                ("mixed".to_owned(), vec![0, 1])
            ]
        );
    }
}
