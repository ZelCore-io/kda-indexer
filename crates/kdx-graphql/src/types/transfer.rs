//! `Transfer` (Task 4).

use async_graphql::{Object, Result, ID};

use super::{unserved, Block, Transaction};
use crate::scalars::{BigInt, DateTime, Decimal};

#[derive(Debug, Clone, Default)]
pub struct Transfer {}

#[Object]
impl Transfer {
    async fn amount(&self) -> Result<Decimal> {
        unserved!("Transfer.amount")
    }
    async fn block(&self) -> Result<Block> {
        unserved!("Transfer.block")
    }
    #[graphql(deprecation = "Use `block.hash` field instead.")]
    async fn block_hash(&self) -> Result<String> {
        unserved!("Transfer.blockHash")
    }
    #[graphql(deprecation = "Use `block.chainId` field instead.")]
    async fn chain_id(&self) -> Result<BigInt> {
        unserved!("Transfer.chainId")
    }
    async fn creation_time(&self) -> Result<DateTime> {
        unserved!("Transfer.creationTime")
    }
    async fn cross_chain_transfer(&self) -> Result<Option<Transfer>> {
        unserved!("Transfer.crossChainTransfer")
    }
    #[graphql(deprecation = "Use `block.height` field instead.")]
    async fn height(&self) -> Result<BigInt> {
        unserved!("Transfer.height")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("Transfer.id")
    }
    async fn module_hash(&self) -> Result<String> {
        unserved!("Transfer.moduleHash")
    }
    async fn module_name(&self) -> Result<String> {
        unserved!("Transfer.moduleName")
    }
    async fn order_index(&self) -> Result<BigInt> {
        unserved!("Transfer.orderIndex")
    }
    async fn receiver_account(&self) -> Result<String> {
        unserved!("Transfer.receiverAccount")
    }
    async fn request_key(&self) -> Result<String> {
        unserved!("Transfer.requestKey")
    }
    async fn sender_account(&self) -> Result<String> {
        unserved!("Transfer.senderAccount")
    }
    async fn transaction(&self) -> Result<Option<Transaction>> {
        unserved!("Transfer.transaction")
    }
}
