//! Object types of the Kadena schema. Names, arguments, defaults and nullability match the
//! vendored SDL exactly (`tests/parity.rs`). Field resolvers that are not served yet answer
//! `NOT_SUPPORTED`; Tasks 3–6 fill them in.

use async_graphql::{Interface, Object, SimpleObject, ID};

use crate::connection::{PageInfo, Total};

pub mod account;
pub mod block;
pub mod event;
pub mod guard;
pub mod network;
pub mod nft;
pub mod node;
pub mod subscription;
pub mod transfer;
pub mod tx;

pub use account::{FungibleAccount, FungibleChainAccount};
pub use block::{Block, BlockNeighbor};
pub use event::Event;
pub use guard::{IGuard, KeysetGuard, RawGuard, UserGuard};
pub use network::{GraphConfiguration, NetworkInfo};
pub use nft::{
    NonFungibleAccount, NonFungibleChainAccount, NonFungibleToken, NonFungibleTokenBalance,
};
pub use transfer::Transfer;
pub use tx::{Signer, Transaction};

/// `NOT_SUPPORTED` until the owning task serves the field.
macro_rules! unserved {
    ($field:literal) => {
        Err($crate::error::not_supported($field))
    };
}
pub(crate) use unserved;

/// The `["0" .. "19"]` default of every `chainIds` argument.
pub fn default_chain_ids() -> Option<Vec<String>> {
    Some((0..20).map(|c| c.to_string()).collect())
}

/// The Relay `Node` interface.
#[derive(Interface)]
#[graphql(field(name = "id", ty = "ID"))]
pub enum Node {
    Block(Block),
    Event(Event),
    FungibleAccount(FungibleAccount),
    FungibleChainAccount(FungibleChainAccount),
    NonFungibleAccount(NonFungibleAccount),
    NonFungibleChainAccount(NonFungibleChainAccount),
    NonFungibleTokenBalance(NonFungibleTokenBalance),
    Signer(Signer),
    Transaction(Transaction),
    Transfer(Transfer),
}

/// A connection type and its edge type. `edges` is `[Edge!]!` (`required`) or `[Edge]!`
/// (`nullable`); `total` adds `totalCount: Int!`.
macro_rules! connection {
    ($conn:ident, $edge:ident, $node:ty, required, total) => {
        connection!(@edge $edge, $node);
        #[derive(Default)]
        pub struct $conn {
            pub edges: Vec<$edge>,
            pub page_info: PageInfo,
            pub total: Total,
        }
        #[Object]
        impl $conn {
            async fn edges(&self) -> &Vec<$edge> {
                &self.edges
            }
            async fn page_info(&self) -> &PageInfo {
                &self.page_info
            }
            async fn total_count(&self) -> async_graphql::Result<i32> {
                self.total.resolve().await
            }
        }
    };
    ($conn:ident, $edge:ident, $node:ty, required, none) => {
        connection!(@edge $edge, $node);
        #[derive(Default)]
        pub struct $conn {
            pub edges: Vec<$edge>,
            pub page_info: PageInfo,
        }
        #[Object]
        impl $conn {
            async fn edges(&self) -> &Vec<$edge> {
                &self.edges
            }
            async fn page_info(&self) -> &PageInfo {
                &self.page_info
            }
        }
    };
    ($conn:ident, $edge:ident, $node:ty, nullable, total) => {
        connection!(@edge $edge, $node);
        #[derive(Default)]
        pub struct $conn {
            pub edges: Vec<$edge>,
            pub page_info: PageInfo,
            pub total: Total,
        }
        #[Object]
        impl $conn {
            async fn edges(&self) -> Vec<Option<&$edge>> {
                self.edges.iter().map(Some).collect()
            }
            async fn page_info(&self) -> &PageInfo {
                &self.page_info
            }
            async fn total_count(&self) -> async_graphql::Result<i32> {
                self.total.resolve().await
            }
        }
    };
    ($conn:ident, $edge:ident, $node:ty, nullable, none) => {
        connection!(@edge $edge, $node);
        #[derive(Default)]
        pub struct $conn {
            pub edges: Vec<$edge>,
            pub page_info: PageInfo,
        }
        #[Object]
        impl $conn {
            async fn edges(&self) -> Vec<Option<&$edge>> {
                self.edges.iter().map(Some).collect()
            }
            async fn page_info(&self) -> &PageInfo {
                &self.page_info
            }
        }
    };
    (@edge $edge:ident, $node:ty) => {
        #[derive(SimpleObject)]
        pub struct $edge {
            pub cursor: String,
            pub node: $node,
        }
    };
}

connection!(
    BlockEventsConnection,
    BlockEventsConnectionEdge,
    Event,
    required,
    total
);
connection!(
    BlockTransactionsConnection,
    BlockTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    FungibleAccountTransactionsConnection,
    FungibleAccountTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    FungibleAccountTransfersConnection,
    FungibleAccountTransfersConnectionEdge,
    Transfer,
    required,
    none
);
connection!(
    FungibleChainAccountTransactionsConnection,
    FungibleChainAccountTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    FungibleChainAccountTransfersConnection,
    FungibleChainAccountTransfersConnectionEdge,
    Transfer,
    required,
    none
);
connection!(
    NonFungibleAccountTransactionsConnection,
    NonFungibleAccountTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    NonFungibleChainAccountTransactionsConnection,
    NonFungibleChainAccountTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    QueryBlocksFromDepthConnection,
    QueryBlocksFromDepthConnectionEdge,
    Block,
    nullable,
    none
);
connection!(
    QueryBlocksFromHeightConnection,
    QueryBlocksFromHeightConnectionEdge,
    Block,
    nullable,
    none
);
connection!(
    QueryCompletedBlockHeightsConnection,
    QueryCompletedBlockHeightsConnectionEdge,
    Block,
    nullable,
    none
);
connection!(
    QueryEventsConnection,
    QueryEventsConnectionEdge,
    Event,
    required,
    total
);
connection!(
    QueryTransactionsByPublicKeyConnection,
    QueryTransactionsByPublicKeyConnectionEdge,
    Transaction,
    nullable,
    total
);
connection!(
    QueryTransactionsConnection,
    QueryTransactionsConnectionEdge,
    Transaction,
    required,
    total
);
connection!(
    QueryTransfersConnection,
    QueryTransfersConnectionEdge,
    Transfer,
    required,
    total
);
connection!(
    TransactionResultEventsConnection,
    TransactionResultEventsConnectionEdge,
    Event,
    nullable,
    total
);
connection!(
    TransactionResultTransfersConnection,
    TransactionResultTransfersConnectionEdge,
    Transfer,
    nullable,
    none
);
