//! `Event` (Task 4).

use async_graphql::{Object, Result, ID};

use super::{unserved, Block, Transaction};
use crate::scalars::BigInt;

#[derive(Debug, Clone, Default)]
pub struct Event {}

#[Object]
impl Event {
    async fn block(&self) -> Result<Block> {
        unserved!("Event.block")
    }
    async fn chain_id(&self) -> Result<BigInt> {
        unserved!("Event.chainId")
    }
    async fn height(&self) -> Result<BigInt> {
        unserved!("Event.height")
    }
    pub async fn id(&self) -> Result<ID> {
        unserved!("Event.id")
    }
    async fn module_name(&self) -> Result<String> {
        unserved!("Event.moduleName")
    }
    async fn name(&self) -> Result<String> {
        unserved!("Event.name")
    }
    async fn order_index(&self) -> Result<BigInt> {
        unserved!("Event.orderIndex")
    }
    async fn parameter_text(&self) -> Result<String> {
        unserved!("Event.parameterText")
    }
    async fn parameters(&self) -> Result<Option<String>> {
        unserved!("Event.parameters")
    }
    async fn qualified_name(&self) -> Result<String> {
        unserved!("Event.qualifiedName")
    }
    async fn request_key(&self) -> Result<String> {
        unserved!("Event.requestKey")
    }
    async fn transaction(&self) -> Result<Option<Transaction>> {
        unserved!("Event.transaction")
    }
}
