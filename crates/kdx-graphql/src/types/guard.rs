//! Guards (GQ-14, Task 5): the `IGuard` interface and its three shapes.

use async_graphql::{Interface, SimpleObject};

#[derive(Debug, Clone, SimpleObject)]
pub struct KeysetGuard {
    pub keys: Vec<String>,
    pub predicate: String,
    pub raw: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct RawGuard {
    #[graphql(deprecation = "deprecated, use KeysetGuard.keys")]
    pub keys: Vec<String>,
    #[graphql(deprecation = "deprecated, use KeysetGuard.predicate")]
    pub predicate: String,
    pub raw: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UserGuard {
    pub args: Vec<String>,
    pub fun: String,
    #[graphql(deprecation = "deprecated, use KeysetGuard.keys")]
    pub keys: Vec<String>,
    #[graphql(deprecation = "deprecated, use KeysetGuard.predicate")]
    pub predicate: String,
    pub raw: String,
}

// Clippy reads the repeated `ty = ...` of the field list as duplicated attributes.
#[allow(clippy::duplicated_attributes)]
#[derive(Debug, Clone, Interface)]
#[graphql(
    name = "IGuard",
    field(
        name = "keys",
        ty = "&Vec<String>",
        deprecation = "deprecated, use KeysetGuard.keys"
    ),
    field(
        name = "predicate",
        ty = "&String",
        deprecation = "deprecated, use KeysetGuard.predicate"
    ),
    field(name = "raw", ty = "&String")
)]
pub enum IGuard {
    KeysetGuard(KeysetGuard),
    RawGuard(RawGuard),
    UserGuard(UserGuard),
}
