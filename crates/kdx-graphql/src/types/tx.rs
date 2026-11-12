//! Transactions, their command, payloads, results and signers (Task 4).

use async_graphql::{Object, Result, SimpleObject, Union, ID};

use super::{
    unserved, Block, TransactionResultEventsConnection, TransactionResultTransfersConnection,
};
use crate::scalars::{BigInt, DateTime};

#[derive(Debug, Clone, Default)]
pub struct Transaction {}

#[Object]
impl Transaction {
    async fn cmd(&self) -> Result<TransactionCommand> {
        unserved!("Transaction.cmd")
    }
    async fn hash(&self) -> Result<String> {
        unserved!("Transaction.hash")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("Transaction.id")
    }
    async fn result(&self) -> Result<TransactionInfo> {
        unserved!("Transaction.result")
    }
    async fn sigs(&self) -> Result<Vec<TransactionSignature>> {
        unserved!("Transaction.sigs")
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransactionCommand {}

#[Object]
impl TransactionCommand {
    async fn meta(&self) -> Result<TransactionMeta> {
        unserved!("TransactionCommand.meta")
    }
    async fn network_id(&self) -> Result<String> {
        unserved!("TransactionCommand.networkId")
    }
    async fn nonce(&self) -> Result<String> {
        unserved!("TransactionCommand.nonce")
    }
    async fn payload(&self) -> Result<TransactionPayload> {
        unserved!("TransactionCommand.payload")
    }
    async fn signers(&self) -> Result<Vec<Signer>> {
        unserved!("TransactionCommand.signers")
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionMeta {
    pub chain_id: BigInt,
    pub creation_time: DateTime,
    pub gas_limit: BigInt,
    pub gas_price: f64,
    pub sender: String,
    pub ttl: BigInt,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ContinuationPayload {
    pub data: String,
    pub pact_id: Option<String>,
    pub proof: Option<String>,
    pub rollback: Option<bool>,
    pub step: Option<i32>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ExecutionPayload {
    pub code: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone, Union)]
pub enum TransactionPayload {
    ContinuationPayload(ContinuationPayload),
    ExecutionPayload(ExecutionPayload),
}

/// Never returned (GQ-3): the indexer has no mempool. Present for schema parity.
#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionMempoolInfo {
    pub status: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionResult {}

#[Object]
#[allow(unused_variables)]
impl TransactionResult {
    async fn bad_result(&self) -> Result<Option<String>> {
        unserved!("TransactionResult.badResult")
    }
    async fn block(&self) -> Result<Block> {
        unserved!("TransactionResult.block")
    }
    async fn continuation(&self) -> Result<Option<String>> {
        unserved!("TransactionResult.continuation")
    }
    async fn event_count(&self) -> Result<Option<BigInt>> {
        unserved!("TransactionResult.eventCount")
    }
    async fn events(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<TransactionResultEventsConnection> {
        unserved!("TransactionResult.events")
    }
    async fn gas(&self) -> Result<BigInt> {
        unserved!("TransactionResult.gas")
    }
    async fn good_result(&self) -> Result<Option<String>> {
        unserved!("TransactionResult.goodResult")
    }
    #[graphql(deprecation = "Use `block.height` instead.")]
    async fn height(&self) -> Result<BigInt> {
        unserved!("TransactionResult.height")
    }
    async fn logs(&self) -> Result<Option<String>> {
        unserved!("TransactionResult.logs")
    }
    #[graphql(deprecation = "Not used.")]
    async fn metadata(&self) -> Result<String> {
        unserved!("TransactionResult.metadata")
    }
    async fn transaction_id(&self) -> Result<Option<BigInt>> {
        unserved!("TransactionResult.transactionId")
    }
    async fn transfers(
        &self,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<TransactionResultTransfersConnection> {
        unserved!("TransactionResult.transfers")
    }
}

#[derive(Debug, Clone, Union)]
pub enum TransactionInfo {
    TransactionMempoolInfo(TransactionMempoolInfo),
    TransactionResult(TransactionResult),
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionSignature {
    pub sig: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct TransactionCapability {
    pub args: String,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct Signer {}

#[Object]
impl Signer {
    async fn address(&self) -> Result<Option<String>> {
        unserved!("Signer.address")
    }
    async fn clist(&self) -> Result<Vec<TransactionCapability>> {
        unserved!("Signer.clist")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("Signer.id")
    }
    async fn order_index(&self) -> Result<Option<i32>> {
        unserved!("Signer.orderIndex")
    }
    async fn pubkey(&self) -> Result<String> {
        unserved!("Signer.pubkey")
    }
    async fn scheme(&self) -> Result<Option<String>> {
        unserved!("Signer.scheme")
    }
}
