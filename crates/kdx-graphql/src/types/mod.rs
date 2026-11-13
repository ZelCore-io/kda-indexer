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

pub use account::{BalanceNode, FungibleAccount, FungibleChainAccount};
pub use block::{Block, BlockNeighbor};
pub use event::Event;
pub use guard::{IGuard, KeysetGuard, RawGuard, UserGuard};
pub use network::{CountersOfEachChain, GraphConfiguration, NetworkInfo};
pub use nft::{
    NonFungibleAccount, NonFungibleChainAccount, NonFungibleToken, NonFungibleTokenBalance,
};
pub use transfer::Transfer;
pub use tx::{Signer, Transaction, TransactionSummary};

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

/// A union or interface value that answers `{}`, not `null`, when the query's fragments
/// select none of its fields (R-P5P9-57 rule b). graphql-js, and so the oracle that kadscan
/// reads, keeps the empty object: `payload { ... on ExecutionPayload { code } }` on a
/// continuation is `{}`. async-graphql folds an empty selection to `null`. The schema is
/// unchanged: the type, its name and `__typename` are the wrapped value's.
#[derive(Debug, Clone)]
pub struct Sel<T>(pub T);

impl<T: async_graphql::OutputType> async_graphql::OutputType for Sel<T> {
    fn type_name() -> std::borrow::Cow<'static, str> {
        T::type_name()
    }

    fn qualified_type_name() -> String {
        T::qualified_type_name()
    }

    fn introspection_type_name(&self) -> std::borrow::Cow<'static, str> {
        self.0.introspection_type_name()
    }

    fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
        T::create_type_info(registry)
    }

    async fn resolve(
        &self,
        ctx: &async_graphql::ContextSelectionSet<'_>,
        field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
    ) -> async_graphql::ServerResult<async_graphql::Value> {
        // A union or interface never resolves to `null` itself (a missing value is the
        // `Option` around this), so `null` here is an empty selection.
        match self.0.resolve(ctx, field).await? {
            async_graphql::Value::Null => Ok(async_graphql::Value::Object(Default::default())),
            v => Ok(v),
        }
    }
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
            async fn total_count(
                &self,
                ctx: &async_graphql::Context<'_>,
            ) -> async_graphql::Result<i32> {
                self.total.resolve(ctx).await
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
            async fn total_count(
                &self,
                ctx: &async_graphql::Context<'_>,
            ) -> async_graphql::Result<i32> {
                self.total.resolve(ctx).await
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
// ZelCore extension: `totalCount` (kadscan's block list).
connection!(
    QueryBlocksFromDepthConnection,
    QueryBlocksFromDepthConnectionEdge,
    Block,
    nullable,
    total
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
// ZelCore extension: `totalCount` (kadscan's tx page).
connection!(
    TransactionResultTransfersConnection,
    TransactionResultTransfersConnectionEdge,
    Transfer,
    nullable,
    total
);
// ZelCore extensions (not in Kadena's schema).
connection!(
    QueryBalanceConnection,
    QueryBalanceConnectionEdge,
    account::BalanceNode,
    required,
    none
);
connection!(
    QueryTransactionsByPactCodeConnection,
    QueryTransactionsByPactCodeConnectionEdge,
    tx::TransactionSummary,
    required,
    none
);
