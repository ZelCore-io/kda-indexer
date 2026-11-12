//! Non-fungible accounts and token balances (GQ-15, Task 5).

use async_graphql::{Object, Result, SimpleObject, ID};

use super::{
    unserved, IGuard, NonFungibleAccountTransactionsConnection,
    NonFungibleChainAccountTransactionsConnection,
};

#[derive(Debug, Clone, Default)]
pub struct NonFungibleAccount {}

#[Object]
#[allow(unused_variables)]
impl NonFungibleAccount {
    async fn account_name(&self) -> Result<String> {
        unserved!("NonFungibleAccount.accountName")
    }
    async fn chain_accounts(&self) -> Result<Vec<NonFungibleChainAccount>> {
        unserved!("NonFungibleAccount.chainAccounts")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("NonFungibleAccount.id")
    }
    async fn non_fungible_token_balances(&self) -> Result<Vec<NonFungibleTokenBalance>> {
        unserved!("NonFungibleAccount.nonFungibleTokenBalances")
    }
    async fn transactions(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleAccountTransactionsConnection> {
        unserved!("NonFungibleAccount.transactions")
    }
}

#[derive(Debug, Clone, Default)]
pub struct NonFungibleChainAccount {}

#[Object]
#[allow(unused_variables)]
impl NonFungibleChainAccount {
    async fn account_name(&self) -> Result<String> {
        unserved!("NonFungibleChainAccount.accountName")
    }
    async fn chain_id(&self) -> Result<String> {
        unserved!("NonFungibleChainAccount.chainId")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("NonFungibleChainAccount.id")
    }
    async fn non_fungible_token_balances(&self) -> Result<Vec<NonFungibleTokenBalance>> {
        unserved!("NonFungibleChainAccount.nonFungibleTokenBalances")
    }
    async fn transactions(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<NonFungibleChainAccountTransactionsConnection> {
        unserved!("NonFungibleChainAccount.transactions")
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct NonFungibleToken {
    pub precision: i32,
    pub supply: i32,
    pub uri: String,
}

#[derive(Debug, Clone, Default)]
pub struct NonFungibleTokenBalance {}

#[Object]
impl NonFungibleTokenBalance {
    async fn account_name(&self) -> Result<String> {
        unserved!("NonFungibleTokenBalance.accountName")
    }
    async fn balance(&self) -> Result<i32> {
        unserved!("NonFungibleTokenBalance.balance")
    }
    async fn chain_id(&self) -> Result<String> {
        unserved!("NonFungibleTokenBalance.chainId")
    }
    async fn guard(&self) -> Result<IGuard> {
        unserved!("NonFungibleTokenBalance.guard")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("NonFungibleTokenBalance.id")
    }
    async fn info(&self) -> Result<Option<NonFungibleToken>> {
        unserved!("NonFungibleTokenBalance.info")
    }
    async fn token_id(&self) -> Result<String> {
        unserved!("NonFungibleTokenBalance.tokenId")
    }
    async fn version(&self) -> Result<String> {
        unserved!("NonFungibleTokenBalance.version")
    }
}
