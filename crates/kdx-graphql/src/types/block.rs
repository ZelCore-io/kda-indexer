//! `Block` (Task 3).

use async_graphql::{Object, Result, SimpleObject, ID};

use super::{unserved, BlockEventsConnection, BlockTransactionsConnection, FungibleChainAccount};
use crate::scalars::{BigInt, DateTime, Decimal};

#[derive(Debug, Clone, Default)]
pub struct Block {}

#[derive(Debug, Clone, SimpleObject)]
pub struct BlockNeighbor {
    pub chain_id: String,
    pub hash: String,
}

#[Object]
#[allow(unused_variables)]
impl Block {
    async fn chain_id(&self) -> Result<BigInt> {
        unserved!("Block.chainId")
    }
    async fn creation_time(&self) -> Result<DateTime> {
        unserved!("Block.creationTime")
    }
    async fn difficulty(&self) -> Result<BigInt> {
        unserved!("Block.difficulty")
    }
    async fn epoch(&self) -> Result<DateTime> {
        unserved!("Block.epoch")
    }
    async fn events(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<BlockEventsConnection> {
        unserved!("Block.events")
    }
    async fn flags(&self) -> Result<Decimal> {
        unserved!("Block.flags")
    }
    async fn hash(&self) -> Result<String> {
        unserved!("Block.hash")
    }
    async fn height(&self) -> Result<BigInt> {
        unserved!("Block.height")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("Block.id")
    }
    async fn miner_account(&self) -> Result<FungibleChainAccount> {
        unserved!("Block.minerAccount")
    }
    async fn neighbors(&self) -> Result<Vec<BlockNeighbor>> {
        unserved!("Block.neighbors")
    }
    async fn nonce(&self) -> Result<Decimal> {
        unserved!("Block.nonce")
    }
    async fn parent(&self) -> Result<Option<Block>> {
        unserved!("Block.parent")
    }
    async fn payload_hash(&self) -> Result<String> {
        unserved!("Block.payloadHash")
    }
    async fn pow_hash(&self) -> Result<String> {
        unserved!("Block.powHash")
    }
    async fn target(&self) -> Result<String> {
        unserved!("Block.target")
    }
    async fn transactions(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<BlockTransactionsConnection> {
        unserved!("Block.transactions")
    }
    async fn weight(&self) -> Result<String> {
        unserved!("Block.weight")
    }
}
