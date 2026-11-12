//! Fungible accounts (GQ-14, Task 5), served from the reconciled `Balances`, `Guards` and
//! `KeyAccounts` rows. Resolvers never call the node.
//!
//! - Rows marked stale are served; a chain with no row (or a row the node reported absent)
//!   is missing from `chainAccounts`; with no row on any chain the account is `null`.
//! - Rows of a module marked not fungible are never served (as native).
//! - `…ByPublicKey` lists only rows whose guard holds the key, of `coin` (the default
//!   `fungibleName`, SEC-2) or of a module confirmed `fungible-v2` on the row's chain (I-6).

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
    Transaction, Transfer,
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

/// Accounts whose guard holds `pk`, with only those rows (on `chain` when given).
fn by_key(
    view: &View,
    pk: &str,
    module: &str,
    chain: Option<u8>,
) -> ApiResult<Vec<FungibleAccount>> {
    let d = view.dict();
    let Some(m) = d.find(DictKind::Module, module) else {
        return Ok(vec![]);
    };
    if hidden(view, m)? {
        return Ok(vec![]);
    }
    let coin = balances::coin_module(&view.txn)?;
    let kh = key_hash(pk);
    let mut out = Vec::new();
    for (k, _) in view
        .txn
        .scan_limit(Table::KeyAccounts, &kh, MAX_KEY_ACCOUNTS, |k| {
            k.starts_with(&kh)
        })?
    {
        view.check_deadline()?;
        let Some(a) = k
            .get(16..20)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_be_bytes)
        else {
            continue;
        };
        let Some(name) = d.name(DictKind::Account, a).map(str::to_owned) else {
            continue;
        };
        let mut rows = Vec::new();
        for (rm, c, r) in balances::account_rows(&view.txn, a)? {
            if rm != m
                || r.absent
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
        if !rows.is_empty() {
            out.push(FungibleAccount {
                account: name,
                module: module.to_owned(),
                rows,
            });
        }
    }
    Ok(out)
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
    async fn guard(&self) -> &IGuard {
        &self.row.guard
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
    gql(ctx)
        .state()?
        .read(move |view| by_key(view, &pk, &module, chain))
        .await
        .map_err(from_api)
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
