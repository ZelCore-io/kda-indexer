//! Fungible accounts (GQ-14, Task 5), served from the reconciled `Balances`, `Guards` and
//! `KeyAccounts` rows. Resolvers never call the node.
//!
//! - Rows marked stale are served; a chain with no row (or a row the node reported absent)
//!   is missing from `chainAccounts`; with no row on any chain the account is `null`.
//! - Rows of a module marked not fungible are never served (as native).
//! - `…ByPublicKey` lists only rows whose guard holds the key, of `coin` (the default
//!   `fungibleName`, SEC-2) or of a module confirmed `fungible-v2` on the row's chain (I-6).
//!   At most [`MAX_KEY_ACCOUNTS`] matching accounts; a cut answer is flagged (L-2).

use async_graphql::{Context, Object, Result, ID};
use kdx_api::{ApiResult, View};
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

/// The present rows of `(account, module)`, by chain, optionally on `chains` only.
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

/// `Block.minerAccount` and other single chain-account lookups inside a read.
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
    // One account's rows of `m`, kept when present, on `chain`, trusted and guarded by `pk`.
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
            source: SourceSpec::Acct(self.account.clone()),
            filter: KeyFilter {
                chain: Some(self.row.chain),
                user_only: true,
                ..KeyFilter::default()
            },
            rows: Rows::Txs {
                sender: Some(self.account.clone()),
                nft: false,
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
        })
    }
}

// ---------------------------------------------------------------------------------------
// Query roots

fn names(account: &str, module: Option<&str>) -> Result<(String, String)> {
    let account = crate::txlist::name_arg("accountName", account)?.to_owned();
    let module = crate::txlist::name_arg("fungibleName", module.unwrap_or("coin"))?.to_owned();
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
    let rows = gql(ctx)
        .state()?
        .read({
            let (a, m) = (account.clone(), module.clone());
            move |view| chain_rows(view, &a, &m, None)
        })
        .await
        .map_err(from_api)?;
    Ok((!rows.is_empty()).then_some(FungibleAccount {
        account,
        module,
        rows,
    }))
}

/// `fungibleChainAccounts` (and `fungibleChainAccount` with one chain).
pub async fn fungible_chain_accounts(
    ctx: &Context<'_>,
    account: &str,
    chain_ids: &[String],
    module: Option<&str>,
) -> Result<Option<Vec<FungibleChainAccount>>> {
    let (account, module) = names(account, module)?;
    let chains = chain_list(chain_ids)?;
    let rows = gql(ctx)
        .state()?
        .read({
            let (a, m) = (account.clone(), module.clone());
            move |view| chain_rows(view, &a, &m, Some(&chains))
        })
        .await
        .map_err(from_api)?;
    if rows.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        rows.into_iter()
            .map(|row| FungibleChainAccount {
                account: account.clone(),
                module: module.clone(),
                row,
            })
            .collect(),
    ))
}

/// `fungibleAccountsByPublicKey` / `fungibleChainAccountsByPublicKey`.
pub async fn accounts_by_public_key(
    ctx: &Context<'_>,
    public_key: &str,
    module: Option<&str>,
    chain: Option<u8>,
) -> Result<Vec<FungibleAccount>> {
    let pk = crate::txlist::name_arg("publicKey", public_key)?.to_owned();
    let module = crate::txlist::name_arg("fungibleName", module.unwrap_or("coin"))?.to_owned();
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

/// `balance(accountName, module, chainIds)`: an account's present fungible rows (the
/// reconciled balances), listed by module name then chain, filtered by `module` and
/// `chainIds`. Without `accountName` (a module's holders) it is `NOT_SUPPORTED`: the store
/// keeps no holders-by-module index, and a scan of every balance row is not bounded (R7I-5).
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
    let Some(account) = account_name else {
        return Err(crate::error::not_supported(
            "balance without accountName (a module's holders)",
        ));
    };
    let account = crate::txlist::name_arg("accountName", &account)?.to_owned();
    let module = module
        .as_deref()
        .map(|m| crate::txlist::name_arg("module", m).map(str::to_owned))
        .transpose()?;
    let chains: Option<Vec<u8>> = chain_ids
        .as_deref()
        .map(|cs| cs.iter().map(|c| crate::txlist::chain_arg(c)).collect())
        .transpose()?;
    let g = gql(ctx);
    // Count and the cursor rules of every connection; the cursor is our own.
    if after.is_some() && before.is_some() {
        return Err(bad_input("pass after or before, not both"));
    }
    if (first.is_some() && before.is_some()) || (last.is_some() && after.is_some()) {
        return Err(bad_input("after goes with first, before with last"));
    }
    let probe = PageArgs::new(None, None, first, last)
        .wide()
        .parse(&g.settings)?;
    let dir = if last.is_some() || (first.is_none() && before.is_some()) {
        Dir::Backward
    } else {
        Dir::Forward
    };
    let cur = after
        .as_deref()
        .or(before.as_deref())
        .map(decode_balance_cursor)
        .transpose()?;
    let acct = account.clone();
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
            for (k, v) in found {
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
                out.push((name, c, r.balance.clone()));
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
            .take(probe.n + 1)
            .collect(),
        (Dir::Backward, _) => rows
            .into_iter()
            .rev()
            .filter(|r| cur.as_ref().is_none_or(|c| key(r) < *c))
            .take(probe.n + 1)
            .collect(),
    };
    let page: Page<PageCursor> = Page {
        dir,
        n: probe.n,
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

#[cfg(test)]
mod tests {
    use super::decimal_sum;

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
        dict.persist(&mut w).unwrap();
        for (pk, acct, m) in rows {
            let guard = format!(r#"{{"keys":["{pk}"],"pred":"keys-all"}}"#);
            let gh = guard_hash(guard.as_bytes());
            w.put(Table::Guards, &gh, guard.as_bytes()).unwrap();
            let row = BalanceRow {
                absent: false,
                balance: "1".into(),
                guard: gh,
                as_of: 1,
                reconciled_at: 1,
                unverified: false,
            };
            w.put(Table::Balances, &bal_key(acct, m, 0), &row.encode())
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
}
