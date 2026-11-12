//! The `Subscription` root (GQ-6, Task 6). Not served until the block feed lands.

use async_graphql::{Result, Subscription as SubscriptionAttr};
use futures::stream::BoxStream;

use super::{default_chain_ids, unserved, Block, Event, Transaction};

pub struct Subscription;

#[SubscriptionAttr]
#[allow(unused_variables)]
impl Subscription {
    async fn events(
        &self,
        chain_id: Option<String>,
        minimum_depth: Option<i32>,
        parameters_filter: Option<String>,
        qualified_event_name: String,
    ) -> Result<BoxStream<'static, Option<Vec<Event>>>> {
        unserved!("Subscription.events")
    }

    async fn new_blocks(
        &self,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
    ) -> Result<BoxStream<'static, Option<Vec<Block>>>> {
        unserved!("Subscription.newBlocks")
    }

    async fn new_blocks_from_depth(
        &self,
        #[graphql(default_with = "default_chain_ids()")] chain_ids: Option<Vec<String>>,
        minimum_depth: i32,
    ) -> Result<BoxStream<'static, Option<Vec<Block>>>> {
        unserved!("Subscription.newBlocksFromDepth")
    }

    async fn transaction(
        &self,
        chain_id: Option<String>,
        request_key: String,
    ) -> Result<BoxStream<'static, Option<Transaction>>> {
        unserved!("Subscription.transaction")
    }
}
