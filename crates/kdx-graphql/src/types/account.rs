//! Fungible accounts (GQ-14, Task 5).

use async_graphql::{Object, Result, ID};

use super::{
    unserved, FungibleAccountTransactionsConnection, FungibleAccountTransfersConnection,
    FungibleChainAccountTransactionsConnection, FungibleChainAccountTransfersConnection, IGuard,
};
use crate::scalars::Decimal;

#[derive(Debug, Clone, Default)]
pub struct FungibleAccount {}

#[Object]
#[allow(unused_variables)]
impl FungibleAccount {
    async fn account_name(&self) -> Result<String> {
        unserved!("FungibleAccount.accountName")
    }
    async fn chain_accounts(&self) -> Result<Vec<FungibleChainAccount>> {
        unserved!("FungibleAccount.chainAccounts")
    }
    async fn fungible_name(&self) -> Result<String> {
        unserved!("FungibleAccount.fungibleName")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("FungibleAccount.id")
    }
    async fn total_balance(&self) -> Result<Decimal> {
        unserved!("FungibleAccount.totalBalance")
    }
    async fn transactions(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleAccountTransactionsConnection> {
        unserved!("FungibleAccount.transactions")
    }
    async fn transfers(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleAccountTransfersConnection> {
        unserved!("FungibleAccount.transfers")
    }
}

#[derive(Debug, Clone, Default)]
pub struct FungibleChainAccount {}

#[Object]
#[allow(unused_variables)]
impl FungibleChainAccount {
    async fn account_name(&self) -> Result<String> {
        unserved!("FungibleChainAccount.accountName")
    }
    async fn balance(&self) -> Result<f64> {
        unserved!("FungibleChainAccount.balance")
    }
    async fn chain_id(&self) -> Result<String> {
        unserved!("FungibleChainAccount.chainId")
    }
    async fn fungible_name(&self) -> Result<String> {
        unserved!("FungibleChainAccount.fungibleName")
    }
    async fn guard(&self) -> Result<IGuard> {
        unserved!("FungibleChainAccount.guard")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("FungibleChainAccount.id")
    }
    async fn transactions(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleChainAccountTransactionsConnection> {
        unserved!("FungibleChainAccount.transactions")
    }
    async fn transfers(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<FungibleChainAccountTransfersConnection> {
        unserved!("FungibleChainAccount.transfers")
    }
}
