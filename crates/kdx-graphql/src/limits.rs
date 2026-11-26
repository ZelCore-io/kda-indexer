//! Query limits (GQ-9; FIX-3 parts C to E), run on every parsed document before validation
//! and execution (HTTP and websocket alike), as an async-graphql extension. See
//! docs/graphql.md "Limits".
//!
//! Served, two models apply:
//! - stock's depth and complexity limits ([`crate::stock_limits`]), with stock's thresholds,
//!   text, code and HTTP status: they refuse on depth and complexity, for the operation that
//!   runs;
//! - this module's read budget ([`budget`]), about the rows a request reads. It refuses only
//!   on safety bounds stock does not have: the walk itself ([`MAX_VISITS`] fields after
//!   fragment expansion, [`MAX_DIRECTIVES`]), the introspection cost cap
//!   ([`MAX_INTROSPECTION_COST`]), more than [`MAX_COUNTS`] count walks (`counts`) and the
//!   safety ceiling [`BUDGET_CEILING`] (`budget`; a batch counts the sum of its operations,
//!   see [`request_budget`]).
//!
//! The walk runs on the freshly parsed document before any other walk of it (C-1). Fragments
//! are expanded where they are spread, so neither fragments, aliases nor inline fragments get
//! around it. Every operation of the document is walked; the largest one is the budget.
//!
//! Cost model (field types from the schema, keyed on field names, never aliases): each field
//! costs 1 (`__typename` 0, and a connection's `edges` and their `node` 0). A deferred
//! `totalCount` (every one but a short list of cheap connections'; fails closed) costs
//! `max_page`. A connection or list field that returns a page multiplies the cost of its
//! selection (of a connection, its `edges` only: `totalCount` and `pageInfo` are answered once
//! per connection) by the rows it serves, sized by [`crate::connection::served_page`] as the
//! resolvers size it: as asked up to [`crate::MAX_PAGE_ROWS`]; absent, or `0` without a cursor,
//! at `default_page`. `nodes(ids)` multiplies by the number of ids, per-chain account lists by
//! the chain count (20 for an absent or empty `chainIds`, else its length), NFT holdings by
//! [`crate::MAX_NFT_HOLDINGS`] times their selection, each row at least 1 (one read per row,
//! whatever is selected; audit F-05).
//!
//! [`analyze`] runs the same walk as the legacy model, which also refuses on `max_depth`,
//! `introspection_depth` and `max_complexity`, with pages clamped at `max_page`. It is not
//! served.

use std::sync::Arc;

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextPrepareRequest, NextValidation,
};
use async_graphql::parser::types::{
    DocumentOperations, ExecutableDocument, Field, FragmentDefinition, OperationType, Selection,
    SelectionSet,
};
use async_graphql::registry::Registry;
use async_graphql::{Name, Request, ServerError, ServerResult, ValidationResult, Value, Variables};

use crate::error::{
    limit, stock_refusal, GRAPHQL_PARSE_FAILED, GRAPHQL_VALIDATION_FAILED, INTERNAL_SERVER_ERROR,
};
use crate::stock_limits;
use crate::{Gql, GraphqlSettings};

/// Parser recursion cap (nesting of selection sets and values).
pub const PARSER_RECURSION: usize = 64;
/// Directives on one field or fragment.
pub const MAX_DIRECTIVES: usize = 8;
/// Fields visited by one document's analysis (fragment expansion included).
pub const MAX_VISITS: usize = 20_000;
/// Chains a per-chain list returns at most.
const CHAINS: u64 = 20;

/// Fields that return a page of items.
const PAGED: &[&str] = &[
    "events",
    "transactions",
    "transfers",
    "blocksFromDepth",
    "blocksFromHeight",
    "completedBlockHeights",
    "transactionsByPublicKey",
    // ZelCore extensions.
    "transactionsByPactCode",
    "balance",
];

/// Lists that clamp at `wide_page` instead of `max_page`: the block lists and account
/// balances (R-P5P9-20).
const WIDE: &[&str] = &[
    "blocksFromDepth",
    "blocksFromHeight",
    "completedBlockHeights",
    "balance",
];

/// The `transactions` subscription's `quantity` when absent (its SDL default).
const SUB_TXS_QUANTITY: i64 = 20;

/// An account's NFT holdings (priced per row, part E S1).
const NFT_HOLDINGS: &str = "nonFungibleTokenBalances";

/// Deferred `totalCount`s (count walks) one operation may run, outer multipliers included.
pub const MAX_COUNTS: u64 = 2;

/// Connections whose `totalCount` is known with their page and needs no decoding (a block's
/// tx keys, a tx's own rows, a block range): bounded and cheap. Every other `totalCount`,
/// including one under a type the walker cannot resolve, is a deferred count walk (fail
/// closed, SR7-3). A block's `events` count is neither: see [`BLOCK_EVENTS`]. Only
/// connections that have a `totalCount` field are listed (M8).
const CHEAP_COUNTS: &[&str] = &[
    "BlockTransactionsConnection",
    "QueryBlocksFromDepthConnection",
    "TransactionResultEventsConnection",
    "TransactionResultTransfersConnection",
];

/// A block's `events` count decodes every tx output of the block: priced as a default page
/// of tx loads per block, not a count walk (kadscan counts the events of every block of a
/// block page) (M2).
const BLOCK_EVENTS: &str = "BlockEventsConnection";

fn cheap_count(ty: &str) -> bool {
    CHEAP_COUNTS.contains(&ty)
}

/// Whose selection a set is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wrap {
    None,
    /// A connection's.
    Conn,
    /// A connection's `edges`.
    Edges,
}

/// `[Foo!]!` → `Foo`.
fn base_type(t: &str) -> &str {
    t.trim_matches(|c| matches!(c, '[' | ']' | '!'))
}

/// The schema's output types (built once; the same types the served schema registers).
pub fn type_registry() -> &'static Registry {
    static REG: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    REG.get_or_init(|| {
        let mut r = Registry::default();
        <crate::types::dex::QueryRoot as async_graphql::OutputType>::create_type_info(&mut r);
        <crate::types::subscription::Subscription as async_graphql::SubscriptionType>::create_type_info(&mut r);
        r
    })
}

/// What a document needs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cost {
    pub depth: usize,
    pub introspection_depth: usize,
    pub complexity: u64,
    /// The largest `first`, `last` or `quantity` given a value (before any cap): above
    /// [`crate::MAX_PAGE_ROWS`] the resolver refuses it (`page`), which `check-ops` reports
    /// without running the document (part E, M4).
    pub rows_asked: u64,
}

struct Walk<'a> {
    frags: &'a std::collections::HashMap<Name, async_graphql::Positioned<FragmentDefinition>>,
    vars: &'a Variables,
    s: &'a GraphqlSettings,
    stack: Vec<&'a str>,
    visits: usize,
    cost: Cost,
    /// Defaults of the current operation's variables (SR7-1).
    defaults: std::collections::HashMap<Name, Option<Value>>,
    /// Introspection list fields on the current path (SR7-2: none may repeat).
    intro_lists: Vec<&'a str>,
    /// Output types, to tell connections from lists (R7I-8).
    reg: &'a Registry,
    /// Product of the page multipliers above the current field (how often it runs).
    outer: u64,
    /// The page multiplier of the connection whose selection is being walked.
    conn_mult: u64,
    /// Deferred `totalCount`s the current operation runs (times their outer multipliers).
    counts: u64,
    /// Introspection cost of the current operation.
    intro_cost: u64,
    /// `crossChainTransfer` fields on the current path (M-5).
    xchain: usize,
    /// Refuse on this model's depth, complexity and count limits ([`analyze`]); off for the
    /// served gate ([`budget`]), where stock's limits refuse instead (FIX-3 part C).
    enforce: bool,
}

/// A `crossChainTransfer` inside another one (M-5): priced as the per-item store read it
/// would be if not answered from the pair already loaded.
pub const NESTED_XCHAIN_COST: u64 = 10;
/// DEX (B3) costs in the read budget: `dexMetrics` walks every listable pair's history,
/// a DEX list or `Pool.charts` reads every listable pair or a pair's history, and a single
/// pool read a pair's latest rows.
pub const DEX_METRICS_COST: u64 = 100_000;
pub const DEX_LIST_COST: u64 = 20_000;
pub const DEX_READ_COST: u64 = 200;

/// The directive cap of one field, fragment or spread (C-1: counted here, inside the
/// bounded walk, instead of async-graphql's `limit_directives`, whose walk expands every
/// fragment spread with no visit cap).
fn directives(n: usize, what: &str) -> Result<(), ServerError> {
    if n > MAX_DIRECTIVES {
        return Err(limit(
            "directives",
            format!("more than {MAX_DIRECTIVES} directives on `{what}`"),
        ));
    }
    Ok(())
}

/// An argument as the analysis sees it.
enum Arg {
    Absent,
    /// Bound to a variable with no value and no default (SR7-1: costed at the maximum).
    Unbound,
    Val(Value),
}

impl Arg {
    fn val(&self) -> Option<&Value> {
        match self {
            Arg::Val(v) => Some(v),
            _ => None,
        }
    }
}

/// Introspection list fields and the multiplier each one's subtree is counted with (SR7-2):
/// the schema's averages rounded up (about 86 types, 4 fields per type, under one argument
/// per field), so the cost of a query tracks the size of its answer.
const INTRO_LISTS: &[(&str, u64)] = &[
    ("types", 90),
    ("fields", 4),
    ("args", 1),
    ("inputFields", 1),
    ("enumValues", 2),
    ("interfaces", 1),
    ("possibleTypes", 2),
    ("directives", 4),
];

/// Introspection cost cap per operation (SR7-2). The graphql-js introspection query costs
/// about 32,000 under [`INTRO_LISTS`]; three of them in one document are refused.
pub const MAX_INTROSPECTION_COST: u64 = 100_000;

impl<'a> Walk<'a> {
    /// The cost of a selection set on type `ty` (`None`: unknown), and the part of it its
    /// direct `edges` fields cost; `wrap` says whose selection it is (a connection's, or a
    /// connection's `edges`), so only those Relay wrappers are free (R7I-8).
    fn set(
        &mut self,
        set: &'a SelectionSet,
        depth: usize,
        intro: bool,
        ty: Option<&'a str>,
        wrap: Wrap,
    ) -> Result<(u64, u64), ServerError> {
        let mut total: u64 = 0;
        let mut edges: u64 = 0;
        for sel in &set.items {
            self.visits += 1;
            if self.visits > MAX_VISITS {
                return Err(limit(
                    "complexity",
                    format!("query has more than {MAX_VISITS} fields after fragment expansion"),
                ));
            }
            match &sel.node {
                Selection::Field(f) => {
                    let f = &f.node;
                    let name = f.name.node.as_str();
                    directives(f.directives.len(), name)?;
                    if name == "__typename" {
                        continue;
                    }
                    let intro = intro || (depth == 0 && (name == "__schema" || name == "__type"));
                    let d = depth + 1;
                    if intro {
                        self.cost.introspection_depth = self.cost.introspection_depth.max(d);
                        if self.enforce && d > self.s.introspection_depth {
                            return Err(limit(
                                "introspection_depth",
                                format!(
                                    "introspection depth exceeds {}",
                                    self.s.introspection_depth
                                ),
                            ));
                        }
                    } else {
                        self.cost.depth = self.cost.depth.max(d);
                        if self.enforce && d > self.s.max_depth {
                            return Err(limit(
                                "depth",
                                format!("query depth exceeds {}", self.s.max_depth),
                            ));
                        }
                    }
                    let list = if intro {
                        INTRO_LISTS.iter().find(|x| x.0 == name).map(|x| x.1)
                    } else {
                        None
                    };
                    if list.is_some() {
                        if self.enforce && self.intro_lists.contains(&name) {
                            return Err(limit(
                                "introspection_nesting",
                                format!("introspection field `{name}` repeats on one path"),
                            ));
                        }
                        self.intro_lists.push(name);
                    }
                    // The schema field (keyed on the field name, never the alias).
                    let mf = if intro {
                        None
                    } else {
                        ty.and_then(|t| self.reg.types.get(t))
                            .and_then(|t| t.field_by_name(name))
                    };
                    let fty: Option<&'a str> = mf.map(|f| base_type(&f.ty));
                    let conn = fty.is_some_and(|t| t.ends_with("Connection"));
                    // Only a connection or a list returns a page: a scalar that shares a paged
                    // field's name (`FungibleChainAccount.balance`) is not multiplied.
                    let pages = mf.is_none_or(|f| conn || f.ty.starts_with('['));
                    let mult = if intro {
                        list.unwrap_or(1)
                    } else if mf.is_none() {
                        // A field the walker cannot resolve (unknown parent type): the
                        // conservative page size, its subtree priced at the per-item floor.
                        self.multiplier(f, ty).max(self.s.max_page as u64)
                    } else if pages {
                        self.multiplier(f, ty)
                    } else {
                        1
                    };
                    let own: u64 = match name {
                        _ if intro => 1,
                        "edges" if wrap == Wrap::Conn => 0,
                        "node" if wrap == Wrap::Edges => 0,
                        "crossChainTransfer" if self.xchain > 0 => NESTED_XCHAIN_COST,
                        // The holdings are read when the account resolves, whatever is
                        // selected of it: one read per row up to the cap (part E, S1).
                        "nonFungibleAccount" | "nonFungibleChainAccount" if ty == Some("Query") => {
                            crate::MAX_NFT_HOLDINGS as u64
                        }
                        "node" | "nodes" if ty == Some("Query") => self.node_reads(f),
                        // A LIKE scan of up to `LIKE_SCAN_ROWS` swap rows (N-06).
                        "lastTokenPriceInKda" if ty == Some("Query") => {
                            kdx_hot::prices::LIKE_SCAN_ROWS as u64
                        }
                        // DEX fields (B3): what each reads, so aliases and nesting reach the
                        // ceiling (pages multiply them as usual).
                        "dexMetrics" if ty == Some("Query") => DEX_METRICS_COST,
                        "pools" | "liquidityPositions" | "tokenPrices" | "tokens"
                            if ty == Some("Query") =>
                        {
                            DEX_LIST_COST
                        }
                        "charts" if ty == Some("Pool") => DEX_LIST_COST,
                        "pool" | "poolTransactions" | "tokenPrice" if ty == Some("Query") => {
                            DEX_READ_COST
                        }
                        "transactions" if ty == Some("Pool") => DEX_READ_COST,
                        "totalCount" if wrap == Wrap::Conn && ty == Some(BLOCK_EVENTS) => {
                            self.s.default_page.min(self.s.max_page) as u64
                        }
                        // Cheap only under a known connection type listed as cheap; anywhere
                        // else (unknown or unresolved parent, interface, fragment) it is a
                        // count walk.
                        "totalCount" if !(wrap == Wrap::Conn && ty.is_some_and(cheap_count)) => {
                            // A count walk (GQ-10): a fixed cost, and few per operation.
                            self.counts = self.counts.saturating_add(self.outer);
                            // Enforced in the served budget too (FIX-3 part D, S4): a
                            // resource bound, not one of stock's limits.
                            if self.counts > MAX_COUNTS {
                                return Err(limit(
                                    "counts",
                                    format!(
                                        "more than {MAX_COUNTS} list totalCounts in one operation"
                                    ),
                                ));
                            }
                            self.s.max_page as u64
                        }
                        _ => 1,
                    };
                    let inner = if conn {
                        Wrap::Conn
                    } else if name == "edges" && wrap == Wrap::Conn {
                        Wrap::Edges
                    } else {
                        Wrap::None
                    };
                    // A connection's own fields run once; its `edges` once per item.
                    let (saved, saved_cm) = (self.outer, self.conn_mult);
                    if conn {
                        self.conn_mult = mult.max(1);
                    } else if inner == Wrap::Edges {
                        self.outer = self.outer.saturating_mul(self.conn_mult);
                    } else {
                        self.outer = self.outer.saturating_mul(mult.max(1));
                    }
                    let xc = usize::from(name == "crossChainTransfer");
                    self.xchain += xc;
                    let child = self.set(&f.selection_set.node, d, intro, fty, inner);
                    self.xchain -= xc;
                    (self.outer, self.conn_mult) = (saved, saved_cm);
                    if list.is_some() {
                        self.intro_lists.pop();
                    }
                    // A connection's page multiplies its `edges` only (`totalCount` and
                    // `pageInfo` are answered once), each item at least 1 when the page is
                    // read (`edges` or `pageInfo` selected); a list multiplies all of it, each
                    // item at least 1 (R7I-8).
                    let (all, part) = child?;
                    let c = if intro {
                        own.saturating_add(mult.saturating_mul(all))
                    } else if conn {
                        let reads = f.selection_set.node.items.iter().any(|x| match &x.node {
                            Selection::Field(g) => {
                                matches!(g.node.name.node.as_str(), "edges" | "pageInfo")
                            }
                            _ => true,
                        });
                        let per = if reads { part.max(1) } else { part };
                        own.saturating_add(all - part)
                            .saturating_add(mult.saturating_mul(per))
                    } else if name == NFT_HOLDINGS {
                        // One details read per holding, made whatever the selection (S1);
                        // the resolver work and response size grow with holdings x the
                        // selection, so each holding is priced at least 1 (audit F-05).
                        own.saturating_add(mult.saturating_mul(all.max(1)))
                    } else if mult > 1 {
                        own.saturating_add(mult.saturating_mul(all.max(1)))
                    } else {
                        own.saturating_add(all)
                    };
                    if intro && depth == 0 {
                        // `__schema`/`__type`: counted against the introspection cap.
                        self.intro_cost = self.intro_cost.saturating_add(c);
                        if self.intro_cost > MAX_INTROSPECTION_COST {
                            return Err(limit(
                                "introspection_complexity",
                                format!("introspection cost exceeds {MAX_INTROSPECTION_COST}"),
                            ));
                        }
                    } else {
                        total = total.saturating_add(c);
                        if name == "edges" && wrap == Wrap::Conn {
                            edges = edges.saturating_add(c);
                        }
                    }
                }
                Selection::InlineFragment(i) => {
                    directives(i.node.directives.len(), "inline fragment")?;
                    let on = i
                        .node
                        .type_condition
                        .as_ref()
                        .map(|c| c.node.on.node.as_str())
                        .or(ty);
                    let (t, e) = self.set(&i.node.selection_set.node, depth, intro, on, wrap)?;
                    total = total.saturating_add(t);
                    edges = edges.saturating_add(e);
                }
                Selection::FragmentSpread(fs) => {
                    let name = fs.node.fragment_name.node.as_str();
                    directives(fs.node.directives.len(), name)?;
                    // A cycle or an unknown fragment is left to validation, which refuses it.
                    if self.stack.contains(&name) {
                        continue;
                    }
                    let Some(def) = self.frags.get(name) else {
                        continue;
                    };
                    directives(def.node.directives.len(), name)?;
                    self.stack.push(name);
                    let on = Some(def.node.type_condition.node.on.node.as_str());
                    let (t, e) = self.set(&def.node.selection_set.node, depth, intro, on, wrap)?;
                    self.stack.pop();
                    total = total.saturating_add(t);
                    edges = edges.saturating_add(e);
                }
            }
        }
        Ok((total, edges))
    }

    /// An argument's value with variables substituted (`None` when absent or unbound).
    fn arg(&self, f: &'a Field, name: &str) -> Arg {
        let Some((_, v)) = f.arguments.iter().find(|(k, _)| k.node.as_str() == name) else {
            return Arg::Absent;
        };
        match v.node.clone().into_const_with(|n| {
            self.vars
                .get(&n)
                .cloned()
                .or_else(|| self.defaults.get(&n).cloned().flatten())
                .ok_or(())
        }) {
            Ok(v) => Arg::Val(v),
            Err(()) => Arg::Unbound,
        }
    }

    /// What the ids of `node(id)` / `nodes(ids)` read (security pass on part E): an NFT id
    /// (or one not known before the request runs) reads the account's holdings, up to
    /// [`crate::MAX_NFT_HOLDINGS`] rows; any other id (or one that does not decode) 1.
    fn node_reads(&self, f: &'a Field) -> u64 {
        let holdings = crate::MAX_NFT_HOLDINGS as u64;
        let one = |v: &Value| match v {
            Value::String(id) => match crate::ids::decode(id) {
                Ok(
                    crate::ids::NodeKey::NonFungibleAccount { .. }
                    | crate::ids::NodeKey::NonFungibleChainAccount { .. }
                    | crate::ids::NodeKey::NonFungibleTokenBalance { .. },
                ) => holdings,
                _ => 1,
            },
            Value::Null => 1,
            _ => holdings,
        };
        let unbound = (crate::types::node::MAX_NODES as u64).saturating_mul(holdings);
        match (self.arg(f, "id"), self.arg(f, "ids")) {
            (Arg::Val(v), _) => one(&v),
            (_, Arg::Val(Value::List(l))) => l.iter().map(one).fold(0, u64::saturating_add),
            (_, Arg::Val(v)) => one(&v),
            (Arg::Unbound, _) => holdings,
            (_, Arg::Unbound) => unbound,
            _ => 1,
        }
    }

    fn int(v: &Value) -> Option<i64> {
        match v {
            Value::Number(n) => n.as_i64(),
            _ => None,
        }
    }

    fn len(v: &Value) -> Option<u64> {
        match v {
            Value::List(l) => Some(l.len() as u64),
            Value::Null => None,
            _ => Some(1),
        }
    }

    fn multiplier(&mut self, f: &'a Field, parent: Option<&str>) -> u64 {
        let s = self.s;
        // The wide lists clamp at `wide_page` (R-P5P9-20), everything else at `max_page`.
        let name = f.name.node.as_str();
        // The served budget (FIX-3 part D, S5) prices a page as the resolver serves it: as
        // asked up to the safety cap (above it the resolver refuses), and an absent
        // `first`/`last` at `default_page`. The legacy model keeps its clamps.
        let cap = if !self.enforce {
            crate::MAX_PAGE_ROWS
        } else if WIDE.contains(&name) {
            s.max_page.saturating_mul(2)
        } else {
            s.max_page
        };
        let page = |n: Option<i64>| match n {
            Some(n) => n.clamp(0, cap as i64) as u64,
            None => s.default_page.min(cap) as u64,
        };
        let (fa, la) = (self.arg(f, "first"), self.arg(f, "last"));
        if matches!(fa, Arg::Unbound) || matches!(la, Arg::Unbound) {
            return cap as u64;
        }
        let first = fa.val().and_then(Self::int);
        let last = la.val().and_then(Self::int);
        let asked = |n: Option<i64>| n.map_or(0, |n| n.max(0) as u64);
        let q = match self.arg(f, "quantity") {
            Arg::Val(v) => Self::int(&v),
            _ => None,
        };
        self.cost.rows_asked = self
            .cost
            .rows_asked
            .max(asked(first))
            .max(asked(last))
            .max(asked(q));
        if first.is_some() || last.is_some() {
            // As served (part E, B1): `0` without a cursor is the default page, with one 0.
            // A cursor bound to an unbound variable counts as absent (the higher price).
            let cursor = |a: &str| self.arg(f, a).val().is_some_and(|v| *v != Value::Null);
            let (_, n) = crate::connection::served_page(
                first,
                last,
                cursor("after"),
                cursor("before"),
                s.default_page as u64,
            );
            return n.min(cap as u64);
        }
        // ZelCore `quantity` (subscriptions `newBlocks`, `transactions`): the list size, clamped
        // to `max_page` as the resolver clamps it. The `transactions` subscription's absent
        // `quantity` is its SDL default (M3).
        // An unbound `quantity` or `ids` is priced at the most the resolver serves (part E,
        // M5): the safety cap in the read budget, `max_page` in the legacy model.
        let unbound = if self.enforce {
            s.max_page as u64
        } else {
            crate::MAX_PAGE_ROWS as u64
        };
        match self.arg(f, "quantity") {
            Arg::Unbound => return unbound,
            Arg::Val(v) => return page(Self::int(&v)),
            Arg::Absent if parent == Some("Subscription") && name == "transactions" => {
                return page(Some(SUB_TXS_QUANTITY));
            }
            Arg::Absent => {}
        }
        let list_len = |arg: &str, absent: u64, unbound: u64| match self.arg(f, arg) {
            Arg::Absent => absent,
            Arg::Unbound => unbound,
            Arg::Val(v) => Self::len(&v).unwrap_or(absent),
        };
        match name {
            "nodes" => list_len("ids", 1, unbound),
            // `chainIds: []` serves every chain, as an absent list does (`chain_set`), so it
            // is priced at every chain, not at none (FIX-4 part C, B-1).
            "fungibleChainAccounts" | "newBlocks" | "newBlocksFromDepth" => {
                match list_len("chainIds", CHAINS, CHAINS) {
                    0 => CHAINS,
                    n => n,
                }
            }
            "chainAccounts" | "countersOfEachChain" => CHAINS,
            // Every account a key guards, up to the resolver's cap (I-3).
            "fungibleAccountsByPublicKey" | "fungibleChainAccountsByPublicKey" => {
                crate::types::account::MAX_KEY_ACCOUNTS as u64
            }
            // Every holding up to the cap (part E, S1), each one details read: priced per
            // row whatever is selected of it (see `set`).
            "nonFungibleTokenBalances" => crate::MAX_NFT_HOLDINGS as u64,
            _ if PAGED.contains(&name) => page(None),
            _ => 1,
        }
    }
}

/// The cost of every operation of `doc` (the largest), or the `LIMIT` error of the first
/// limit of this model it passes. Not the served gate: see [`budget`].
pub fn analyze(
    doc: &ExecutableDocument,
    vars: &Variables,
    s: &GraphqlSettings,
) -> Result<Cost, ServerError> {
    walk(doc, vars, s, true)
}

/// Safety ceiling of the read budget (FIX-3 part E, B1): a request (a batch: the sum of its
/// operations) the budget prices above it is refused with `LIMIT`,
/// `extensions.limit = "budget"`. Safety, not parity: stock's gate prices `first: 0` at 0
/// while 20 rows are served, so stock's limits alone leave aliased zero-cost pages
/// unbounded. Calibrated on the explorer operation documents and the compare suite's
/// documents (see docs/graphql.md "Limits"). CALIB
pub const BUDGET_CEILING: u64 = 1_100_000;

/// The read budget's refusal above [`BUDGET_CEILING`].
pub fn over_ceiling(cost: u64) -> Result<(), ServerError> {
    if cost > BUDGET_CEILING {
        return Err(limit(
            "budget",
            format!("query reads too much: budget {cost} exceeds {BUDGET_CEILING}"),
        ));
    }
    Ok(())
}

/// The read budget of one request of a batch (part E, B1: a batch's budget is the sum of its
/// operations'). `None` when the request is refused before its budget counts (sizes, parse,
/// the bounds of the walk): it is answered with its own error when it runs.
pub fn request_budget(query: &str, vars: &Variables, s: &GraphqlSettings) -> Option<u64> {
    check_sizes(query, vars, s).ok()?;
    let doc = async_graphql::parser::parse_query(query).ok()?;
    budget(&doc, vars, s).ok().map(|c| c.complexity)
}

/// The served pass of this model (FIX-3 part C): a read budget only. It refuses nothing
/// stock's limits ([`crate::stock_limits`]) would serve on depth, complexity or counts; it
/// keeps only the bounds of the walk itself ([`MAX_VISITS`] fields after fragment expansion,
/// [`MAX_DIRECTIVES`]) and the introspection cost cap ([`MAX_INTROSPECTION_COST`]).
pub fn budget(
    doc: &ExecutableDocument,
    vars: &Variables,
    s: &GraphqlSettings,
) -> Result<Cost, ServerError> {
    walk(doc, vars, s, false)
}

fn walk(
    doc: &ExecutableDocument,
    vars: &Variables,
    s: &GraphqlSettings,
    enforce: bool,
) -> Result<Cost, ServerError> {
    let mut w = Walk {
        enforce,
        frags: &doc.fragments,
        vars,
        s,
        stack: Vec::new(),
        visits: 0,
        cost: Cost::default(),
        defaults: Default::default(),
        intro_lists: Vec::new(),
        intro_cost: 0,
        reg: type_registry(),
        outer: 1,
        conn_mult: 1,
        counts: 0,
        xchain: 0,
    };
    let ops: Vec<_> = match &doc.operations {
        DocumentOperations::Single(op) => vec![op],
        DocumentOperations::Multiple(m) => m.values().collect(),
    };
    for op in ops {
        w.defaults = op
            .node
            .variable_definitions
            .iter()
            .map(|d| {
                (
                    d.node.name.node.clone(),
                    d.node.default_value.as_ref().map(|v| v.node.clone()),
                )
            })
            .collect();
        w.intro_cost = 0;
        let root = match op.node.ty {
            OperationType::Query => "Query",
            OperationType::Mutation => "Mutation",
            OperationType::Subscription => "Subscription",
        };
        w.outer = 1;
        w.counts = 0;
        let (c, _) = w.set(
            &op.node.selection_set.node,
            0,
            false,
            Some(root),
            Wrap::None,
        )?;
        w.cost.complexity = w.cost.complexity.max(c);
        if !enforce {
            over_ceiling(c)?;
        }
        if enforce && c > s.max_complexity {
            return Err(limit(
                "complexity",
                format!("query complexity {c} exceeds {}", s.max_complexity),
            ));
        }
    }
    Ok(w.cost)
}

/// Size limits of one request's text and variables.
pub fn check_sizes(query: &str, vars: &Variables, s: &GraphqlSettings) -> Result<(), ServerError> {
    if query.len() > s.max_query_bytes {
        return Err(limit(
            "query_size",
            format!("query text larger than {} bytes", s.max_query_bytes),
        ));
    }
    let vlen = serde_json::to_vec(vars).map_or(usize::MAX, |v| v.len());
    if vlen > s.max_variables_bytes {
        return Err(limit(
            "variables_size",
            format!("variables larger than {} bytes", s.max_variables_bytes),
        ));
    }
    Ok(())
}

/// The limits extension (installed by `build_schema`).
pub struct Limits;

impl ExtensionFactory for Limits {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(LimitsExt::default())
    }
}

/// Stock's verdicts on one request, made in `prepare_request` and applied after validation.
#[derive(Default)]
struct LimitsExt {
    stock: std::sync::Mutex<Option<StockVerdict>>,
}

struct StockVerdict {
    /// Over the socket (stock's HTTP codes do not apply).
    ws: bool,
    /// Positions of the arguments whose value holds a variable: async-graphql reports a
    /// variable value that does not coerce at them, during validation (FIX-4 part B item 5).
    var_args: Vec<async_graphql::Pos>,
    /// Depth-limit messages (stock's validation rule; over the socket too, audit F-07).
    depth: Vec<String>,
    /// Over stock's complexity; the message to answer.
    complexity: Option<&'static str>,
}

#[async_graphql::async_trait::async_trait]
impl Extension for LimitsExt {
    /// C-1: the sizes are checked, then the document is parsed here and analyzed (a walk
    /// capped at [`MAX_VISITS`]) before anything else sees it. async-graphql's own parse step
    /// (`check_recursive_depth`, and `limit_directives` when set) expands every fragment spread
    /// with no memo and no visit cap, synchronously on a runtime worker; a fragment bomb of
    /// 1.4 KB cost minutes of CPU there. The parsed document is kept on the request, so
    /// that step reuses it and only ever walks a document this analysis has bounded.
    async fn prepare_request(
        &self,
        ctx: &ExtensionContext<'_>,
        mut request: Request,
        next: NextPrepareRequest<'_>,
    ) -> ServerResult<Request> {
        let s = &ctx.data_unchecked::<Gql>().settings;
        check_sizes(&request.query, &request.variables, s)?;
        let vars = request.variables.clone();
        let op = request.operation_name.clone();
        let ws = ctx.data_opt::<crate::ws::ConnSubs>().is_some();
        // Over HTTP a document that does not parse is Apollo Server 4's
        // `GRAPHQL_PARSE_FAILED` (HTTP 400), as stock answers it (FIX-4 part B item 5).
        let doc = request.parsed_query().map_err(|e| {
            if ws {
                e
            } else {
                with_code(e, GRAPHQL_PARSE_FAILED)
            }
        })?;
        budget(doc, &vars, s)?;
        // Stock's limits (FIX-3 part C). Over the socket stock words the complexity refusal
        // without its HTTP prefix; over HTTP it skips the complexity check for an operation
        // named `IntrospectionQuery`. Stock runs no depth rule over the socket; this indexer
        // does (audit F-07 option B: a subscription re-runs per block, real ones are shallow).
        let reg = type_registry();
        let depth = stock_limits::depth_errors(reg, doc);
        // FIX-3 part D (S1): the operation priced is the one the executor runs (none: the
        // executor refuses the request itself), and the skip needs both stock's name and an
        // operation of introspection fields alone, so a spoofed name carrying real fields is
        // priced.
        let skip = !ws
            && op.as_deref() == Some(stock_limits::INTROSPECTION_QUERY)
            && stock_limits::selected_operation(doc, op.as_deref())
                .is_some_and(|o| stock_limits::pure_introspection(doc, o));
        let complexity = (!skip && stock_limits::over_complexity(reg, doc, op.as_deref(), &vars))
            .then_some(if ws {
                stock_limits::WS_COMPLEXITY_MESSAGE
            } else {
                stock_limits::COMPLEXITY_MESSAGE
            });
        let var_args = if ws {
            Vec::new()
        } else {
            var_arg_positions(doc)
        };
        *self.stock.lock().unwrap_or_else(|e| e.into_inner()) = Some(StockVerdict {
            ws,
            var_args,
            depth,
            complexity,
        });
        next.run(ctx, request).await
    }

    /// Stock's order: the depth rule is one of the validation rules (its errors join the
    /// others'); the complexity check runs only on a valid document.
    async fn validation(
        &self,
        ctx: &ExtensionContext<'_>,
        next: NextValidation<'_>,
    ) -> Result<ValidationResult, Vec<ServerError>> {
        let verdict = self.stock.lock().unwrap_or_else(|e| e.into_inner()).take();
        let r = next.run(ctx).await;
        let Some(v) = verdict else {
            return r;
        };
        let depth: Vec<ServerError> = v
            .depth
            .into_iter()
            .map(|m| stock_refusal(GRAPHQL_VALIDATION_FAILED, "depth", m))
            .collect();
        let r = match r {
            Err(es) if !v.ws => Err(apollo_validation(es, &v.var_args, !depth.is_empty())),
            r => r,
        };
        match r {
            Err(mut es) => {
                // `check-ops` answers a valid document with one marker error: still valid,
                // so the complexity check runs as on a served request.
                let valid = es.iter().all(|e| {
                    e.extensions.as_ref().and_then(|x| x.get("code"))
                        == Some(&Value::String(CHECK_PASSED.to_owned()))
                });
                if valid && depth.is_empty() {
                    if let Some(m) = v.complexity {
                        es.push(stock_refusal(INTERNAL_SERVER_ERROR, "complexity", m));
                    }
                }
                es.extend(depth);
                Err(es)
            }
            Ok(_) if !depth.is_empty() => Err(depth),
            Ok(_) if v.complexity.is_some() => Err(vec![stock_refusal(
                INTERNAL_SERVER_ERROR,
                "complexity",
                v.complexity.unwrap_or_default(),
            )]),
            ok => ok,
        }
    }
}

/// `e` with `extensions.code = code`.
fn with_code(mut e: ServerError, code: &'static str) -> ServerError {
    e.extensions
        .get_or_insert_with(Default::default)
        .set("code", code);
    e
}

/// Apollo Server 4's answer to async-graphql's validation errors (FIX-4 part B item 5).
/// Apollo validates the document first, with `GRAPHQL_VALIDATION_FAILED` (HTTP 400), and
/// coerces variable values only after that. async-graphql checks both in one pass: an
/// "Invalid value" error at an argument that holds a variable is a coercion error. So: when
/// the document itself has errors they are answered alone, coded; when only variable values
/// fail, those stay codeless (500 `INTERNAL_SERVER_ERROR`, as FIX-4 item 3 decided). Coded
/// errors (the depth rule, `check-ops`' marker) are left alone. A depth refusal (`deep`) is a
/// document error too: the coercion errors are dropped beside it (FIX-4 part C, M-1).
///
/// A coercion is told by async-graphql's message prefix at an argument that holds a
/// variable (pinned by a test in `tests/http.rs`). An argument mixing a variable with a bad
/// literal (`nodes(ids: [$a, 1])`) is classed as a coercion (500, where Apollo answers 400).
fn apollo_validation(
    es: Vec<ServerError>,
    var_args: &[async_graphql::Pos],
    deep: bool,
) -> Vec<ServerError> {
    let coded = |e: &ServerError| e.extensions.as_ref().and_then(|x| x.get("code")).is_some();
    let coercion = |e: &ServerError| {
        !coded(e)
            && e.message.starts_with("Invalid value for argument")
            && e.locations.first().is_some_and(|p| var_args.contains(p))
    };
    if !deep && es.iter().all(|e| coded(e) || coercion(e)) {
        return es;
    }
    es.into_iter()
        .filter(|e| !coercion(e))
        .map(|e| {
            if coded(&e) {
                e
            } else {
                with_code(e, GRAPHQL_VALIDATION_FAILED)
            }
        })
        .collect()
}

/// Positions of the field and directive arguments of `doc` whose value holds a variable.
fn var_arg_positions(doc: &ExecutableDocument) -> Vec<async_graphql::Pos> {
    use async_graphql::parser::types::Directive;
    use async_graphql::Positioned;
    // An argument value holds a variable iff it cannot be made constant without one.
    macro_rules! args {
        ($a:expr, $out:expr) => {
            for (n, v) in $a {
                if v.node.clone().into_const_with(|_| Err(())).is_err() {
                    $out.push(n.pos);
                }
            }
        };
    }
    fn dirs(d: &[Positioned<Directive>], out: &mut Vec<async_graphql::Pos>) {
        for d in d {
            args!(&d.node.arguments, out);
        }
    }
    fn set(s: &SelectionSet, out: &mut Vec<async_graphql::Pos>) {
        for sel in &s.items {
            match &sel.node {
                Selection::Field(f) => {
                    args!(&f.node.arguments, out);
                    dirs(&f.node.directives, out);
                    set(&f.node.selection_set.node, out);
                }
                Selection::FragmentSpread(f) => dirs(&f.node.directives, out),
                Selection::InlineFragment(f) => {
                    dirs(&f.node.directives, out);
                    set(&f.node.selection_set.node, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    for (_, op) in doc.operations.iter() {
        dirs(&op.node.directives, &mut out);
        set(&op.node.selection_set.node, &mut out);
    }
    for f in doc.fragments.values() {
        dirs(&f.node.directives, &mut out);
        set(&f.node.selection_set.node, &mut out);
    }
    out
}

/// `extensions.code` of the marker error `check-ops` gets for a valid document.
pub const CHECK_PASSED: &str = "CHECK_PASSED";

/// `graphql check-ops`: stop after validation; a valid document answers one marker error.
pub struct CheckOnly;

impl ExtensionFactory for CheckOnly {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(CheckOnlyExt)
    }
}

struct CheckOnlyExt;

#[async_graphql::async_trait::async_trait]
impl Extension for CheckOnlyExt {
    async fn validation(
        &self,
        ctx: &ExtensionContext<'_>,
        next: NextValidation<'_>,
    ) -> Result<ValidationResult, Vec<ServerError>> {
        next.run(ctx).await?;
        let mut e = ServerError::new("valid", None);
        let mut x = async_graphql::ErrorExtensionValues::default();
        x.set("code", CHECK_PASSED);
        e.extensions = Some(x);
        Err(vec![e])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cost(q: &str, vars: serde_json::Value) -> Result<Cost, String> {
        let doc = async_graphql::parser::parse_query(q).map_err(|e| e.to_string())?;
        let vars = crate::http::variables(vars).unwrap();
        analyze(&doc, &vars, &GraphqlSettings::default()).map_err(|e| {
            let x = e.extensions.unwrap();
            format!("{}", x.get("limit").unwrap())
        })
    }

    /// M3: the `transactions` subscription's absent `quantity` is priced at its SDL default
    /// (20, not `default_page`), and `quantity` at most `max_page`, as the resolver serves.
    #[test]
    fn subscription_quantity_is_priced_as_served() {
        let s = GraphqlSettings {
            default_page: 5,
            max_page: 50,
            ..GraphqlSettings::default()
        };
        let c = |q: &str| {
            let doc = async_graphql::parser::parse_query(q).unwrap();
            analyze(&doc, &Variables::default(), &s).map(|c| c.complexity)
        };
        assert_eq!(c("subscription { transactions { hash } }").ok(), Some(21));
        assert_eq!(
            c("subscription { newBlocks(quantity: 100) { hash } }").ok(),
            Some(51)
        );
    }

    /// M-5: a `crossChainTransfer` inside another one is priced at the per-item read cost;
    /// the first level keeps its price.
    #[test]
    fn nested_cross_chain_transfer_is_priced_per_read() {
        let s = GraphqlSettings::default();
        let c = |q: &str| {
            let doc = async_graphql::parser::parse_query(q).unwrap();
            analyze(&doc, &Variables::default(), &s).map(|c| c.complexity)
        };
        let page = |inner: &str| {
            format!("{{ transfers(accountName: \"a\", first: 10) {{ edges {{ node {{ {inner} }} }} }} }}")
        };
        let one = c(&page("crossChainTransfer { amount }")).unwrap();
        let two = c(&page(
            "crossChainTransfer { crossChainTransfer { amount } }",
        ))
        .unwrap();
        let three = c(&page(
            "crossChainTransfer { crossChainTransfer { crossChainTransfer { amount } } }",
        ))
        .unwrap();
        assert_eq!(one, 1 + 10 * 2, "{one}");
        assert_eq!(two - one, 10 * NESTED_XCHAIN_COST, "{two}");
        assert_eq!(three - two, 10 * NESTED_XCHAIN_COST, "{three}");
        // Siblings are not nested.
        let side = c(&page(
            "crossChainTransfer { amount } a: crossChainTransfer { amount }",
        ))
        .unwrap();
        assert_eq!(side, 1 + 10 * 4, "{side}");
    }

    /// M8: every cheap count names a connection that has a `totalCount` field.
    #[test]
    fn cheap_counts_are_real_count_fields() {
        let reg = type_registry();
        for t in CHEAP_COUNTS {
            let f = reg
                .types
                .get(*t)
                .and_then(|x| x.field_by_name("totalCount"));
            assert!(f.is_some(), "{t} has no totalCount");
        }
    }

    /// M2: a block's `events` count decodes the whole block: priced at a default page per
    /// block, so a wide block list of them is refused and a default page passes.
    #[test]
    fn block_event_counts_are_priced_per_block() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        assert_eq!(
            c("{ blocksFromHeight(startHeight: 0, first: 200) { edges { node { events { totalCount } } } } }"),
            Err("\"complexity\"".into())
        );
        assert_eq!(
            c("{ blocksFromHeight(startHeight: 0) { edges { node { events { totalCount } } } } }"),
            Ok(1 + 20 * 21)
        );
        // A block's tx count needs no decoding: still cheap under a list.
        assert!(c("{ blocksFromHeight(startHeight: 0, first: 200) { edges { node { transactions { totalCount } } } } }").unwrap() < 1000);
    }

    /// I-3: the by-public-key lists are priced at the accounts they can return (100).
    #[test]
    fn by_public_key_lists_are_priced_at_their_cap() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        for root in [
            "fungibleAccountsByPublicKey(publicKey: \"k\")",
            "fungibleChainAccountsByPublicKey(publicKey: \"k\", chainId: \"0\")",
        ] {
            assert_eq!(c(&format!("{{ {root} {{ accountName }} }}")), Ok(101));
            assert_eq!(
                c(&format!(
                    "{{ {root} {{ transactions(first: 98) {{ edges {{ node {{ hash }} }} }} }} }}"
                )),
                Err("\"complexity\"".into())
            );
        }
    }

    #[test]
    fn wrappers_are_free_only_inside_a_connection() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        // `edges { node }` under a connection: free.
        assert_eq!(
            c("{ transactions(first: 100) { edges { node { hash } } } }"),
            Ok(101)
        );
        // A root `node(id)` is a field like any other, aliased or not.
        let roots: String = (0..100)
            .map(|i| format!("n{i}: node(id: \"x\") {{ id }} "))
            .collect();
        assert!(c(&format!("{{ {roots} }}")).unwrap() >= 200);
        let roots: String = (0..1100)
            .map(|i| format!("n{i}: node(id: \"x\") {{ id }} "))
            .collect();
        assert_eq!(c(&format!("{{ {roots} }}")), Err("\"complexity\"".into()));
        // An alias named like a wrapper is not one.
        assert_eq!(c("{ edges: lastBlockHeight }"), Ok(1));
    }

    #[test]
    fn every_page_item_costs_at_least_one() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        assert_eq!(
            c("{ transactions(blockHash: \"x\", first: 100) { edges { node { __typename } } } }"),
            Ok(101)
        );
        // A scalar named like a paged field is not a page.
        assert_eq!(
            c("{ fungibleAccount(accountName: \"a\") { chainAccounts { balance } } }"),
            Ok(22)
        );
        let many: String = (0..20)
            .map(|i| format!("t{i}: transactions(blockHash: \"x\", first: 100) {{ edges {{ node {{ __typename }} }} }} "))
            .collect();
        assert_eq!(c(&format!("{{ {many} }}")), Err("\"complexity\"".into()));
    }

    #[test]
    fn list_counts_are_costed_and_few() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        let ten: String = (0..10)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ totalCount }} "))
            .collect();
        assert_eq!(c(&format!("{{ {ten} }}")), Err("\"counts\"".into()));
        let two: String = (0..2)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ totalCount }} "))
            .collect();
        assert_eq!(c(&format!("{{ {two} }}")), Ok(2 * 101));
        // A count walk under a per-chain list runs once per chain: refused.
        assert_eq!(
            c("{ fungibleChainAccounts(accountName: \"a\") { transactions { totalCount } } }"),
            Err("\"counts\"".into())
        );
        // FIX-4 part C (B-1): `chainIds: []` serves every chain, so it is priced like no
        // `chainIds` (20 chains), not as a list of none: refused the same way.
        assert_eq!(
            c("{ fungibleChainAccounts(accountName: \"a\", chainIds: []) { transactions { totalCount } } }"),
            Err("\"counts\"".into())
        );
        let none = c("{ fungibleChainAccounts(accountName: \"a\") { balance } }");
        let empty = c("{ fungibleChainAccounts(accountName: \"a\", chainIds: []) { balance } }");
        assert_eq!(empty, none);
        let sub = |q: &str| c(q).unwrap();
        assert_eq!(
            sub("subscription { newBlocks(chainIds: []) { height } }"),
            sub("subscription { newBlocks { height } }")
        );
        assert_eq!(
            sub("subscription { newBlocksFromDepth(minimumDepth: 1, chainIds: []) { height } }"),
            sub("subscription { newBlocksFromDepth(minimumDepth: 1) { height } }")
        );
        // A block's count is not a walk: kadscan's block list passes.
        assert!(c("{ blocksFromDepth(minimumDepth: 0, first: 25) { totalCount edges { node { transactions { totalCount edges { node { cmd { meta { gasPrice } } } } } } } } }").unwrap() < 2000);
    }

    /// Audit F-05: the selection under `nonFungibleTokenBalances` is priced per holding
    /// (resolver work and response size grow with holdings x fields), so 200 aliased
    /// `guard { raw }` per holding is over the ceiling; an explorer selection passes.
    #[test]
    fn nft_holdings_price_their_selection() {
        let b = |q: &str| {
            let doc = async_graphql::parser::parse_query(q).unwrap();
            budget(&doc, &Variables::default(), &GraphqlSettings::default())
                .and_then(|c| over_ceiling(c.complexity).map(|_| c.complexity))
                .map_err(|e| format!("{:?}", e.extensions.unwrap().get("code")))
        };
        let many: String = (0..200)
            .map(|i| format!("g{i}: guard {{ raw }} "))
            .collect();
        let q = format!(
            "{{ nonFungibleAccount(accountName: \"k:a\") {{ nonFungibleTokenBalances {{ {many} }} }} }}"
        );
        assert_eq!(b(&q), Err(format!("{:?}", Some(&Value::from("LIMIT")))));
        let normal = "{ nonFungibleAccount(accountName: \"k:a\") { accountName \
            nonFungibleTokenBalances { tokenId version balance \
              guard { raw ... on KeysetGuard { keys predicate } } \
              info { precision supply uri } } } }";
        assert!(b(normal).is_ok(), "{:?}", b(normal));
    }

    /// FIX-3 part D (S4, S5): the served read budget still refuses more than [`MAX_COUNTS`]
    /// count walks (resource protection, as before part C), and prices a page as it is
    /// served: an absent `first`/`last` at `default_page`, a given one as asked, up to the
    /// safety cap.
    #[test]
    fn the_read_budget_counts_walks_and_prices_served_pages() {
        let b = |q: &str| {
            let doc = async_graphql::parser::parse_query(q).unwrap();
            budget(&doc, &Variables::default(), &GraphqlSettings::default()).map_err(|e| {
                let x = e.extensions.unwrap();
                format!("{:?} {:?}", x.get("code"), x.get("limit"))
            })
        };
        let refused = format!(
            "{:?} {:?}",
            Some(&Value::from("LIMIT")),
            Some(&Value::from("counts"))
        );
        let three: String = (0..3)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ totalCount }} "))
            .collect();
        assert_eq!(b(&format!("{{ {three} }}")), Err(refused.clone()));
        let two: String = (0..2)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ totalCount }} "))
            .collect();
        assert!(b(&format!("{{ {two} }}")).is_ok());
        assert_eq!(
            b("{ fungibleChainAccounts(accountName: \"a\") { transactions { totalCount } } }"),
            Err(refused.clone())
        );
        // FIX-4 part C (B-1): `chainIds: []` is every chain, refused like the form without it.
        assert_eq!(
            b("{ fungibleChainAccounts(accountName: \"a\", chainIds: []) { transactions { totalCount } } }"),
            Err(refused.clone())
        );
        let edges = |ids: &str| {
            b(&format!(
                "{{ fungibleChainAccounts(accountName: \"a\"{ids}) {{ transactions(first: 5) {{ edges {{ node {{ hash }} }} }} }} }}"
            ))
            .unwrap()
        };
        assert_eq!(edges(", chainIds: []"), edges(""));
        // Pages: absent is the default page (20 items, 1 each, plus the connection).
        let page = |args: &str| {
            b(&format!(
                "{{ transactions{args} {{ edges {{ node {{ hash }} }} }} }}"
            ))
            .unwrap()
            .complexity
        };
        assert_eq!(page(""), 21);
        assert_eq!(page("(first: 500)"), 501);
        assert_eq!(page("(last: 1000)"), 1001);
        assert_eq!(page("(first: 5000)"), 1001);
        assert_eq!(page("(first: -5)"), 1);
    }

    /// The B1 document: `transactions` with nested `events`, both pages given by `args`.
    fn nested(outer: &str, inner: &str) -> String {
        format!(
            "transactions{outer} {{ edges {{ node {{ result {{ ... on TransactionResult {{ \
             events{inner} {{ edges {{ node {{ name parameters }} }} }} }} }} }} }} }}"
        )
    }

    fn served(q: &str, vars: serde_json::Value) -> Result<u64, String> {
        let doc = async_graphql::parser::parse_query(q).unwrap();
        let vars = crate::http::variables(vars).unwrap();
        budget(&doc, &vars, &GraphqlSettings::default())
            .map(|c| c.complexity)
            .map_err(|e| format!("{:?}", e.extensions.unwrap().get("limit")))
    }

    /// FIX-3 part E (B1): `first: 0` / `last: 0` without a cursor is served as the default
    /// page, so the read budget prices it so, at every nesting level; with a cursor it serves
    /// (and costs) 0 rows. Stock's gate still prices it at 0 (parity unchanged).
    #[test]
    fn zero_pages_are_priced_as_served() {
        let absent = served(&format!("{{ {} }}", nested("", "")), json!({})).unwrap();
        assert!(absent >= 20 * 20, "{absent}");
        for (o, i) in [
            ("(first: 0)", "(first: 0)"),
            ("(last: 0)", "(last: 0)"),
            ("(first: 0)", "(last: 0)"),
        ] {
            let q = format!("{{ {} }}", nested(o, i));
            assert_eq!(served(&q, json!({})).unwrap(), absent, "{q}");
            let doc = async_graphql::parser::parse_query(&q).unwrap();
            let stock =
                stock_limits::complexity(type_registry(), &doc, None, &Variables::default());
            assert_eq!(stock, 0.0, "{q}");
        }
        let q = format!(
            "query Q($n: Int) {{ {} }}",
            nested("(first: $n)", "(first: $n)")
        );
        assert_eq!(served(&q, json!({"n": 0})).unwrap(), absent);
        // With a cursor the count is taken as given: 0 rows.
        let page = |args: &str| {
            served(
                &format!("{{ transactions{args} {{ edges {{ node {{ hash }} }} }} }}"),
                json!({}),
            )
            .unwrap()
        };
        assert_eq!(page("(after: \"c\", first: 0)"), 1);
        assert_eq!(page("(before: \"c\", last: 0)"), 1);
        assert_eq!(page("(after: \"c\")"), 21);
        assert_eq!(page("(after: null, first: 0)"), 21);
    }

    /// B3: DEX aggregates cost their reads; a normal page with charts is served, aliases
    /// of the long-range aggregates reach the ceiling.
    #[test]
    fn dex_fields_cost_their_reads() {
        let one = served("{ dexMetrics { totalPools } }", json!({})).unwrap();
        assert!(one >= DEX_METRICS_COST, "cost {one}");
        let n = (BUDGET_CEILING / one + 1) as usize;
        let many: String = (0..n)
            .map(|i| format!("a{i}: dexMetrics {{ totalPools }} "))
            .collect();
        assert_eq!(
            served(&format!("{{ {many} }}"), json!({})),
            Err(format!("{:?}", Some(&Value::from("budget"))))
        );
        let page = served(
            "{ pools(first: 20) { edges { node { charts(timeFrame: ALL) { tvl { value } } transactions { edges { cursor } } } } } }",
            json!({}),
        )
        .unwrap();
        assert!(
            (20 * DEX_LIST_COST..BUDGET_CEILING).contains(&page),
            "cost {page}"
        );
        assert!(served(
            "{ pool(id: \"x\") { id } tokenPrice(tokenAddress: \"coin\") { id } }",
            json!({})
        )
        .is_ok());
    }

    /// N-06: `lastTokenPriceInKda` costs its scan's rows, so aliases of it reach the
    /// ceiling; one, or a few, are served (stock's answer for normal documents).
    #[test]
    fn last_token_price_costs_its_scan() {
        let one = served("{ lastTokenPriceInKda(moduleName: \"kdx\") }", json!({})).unwrap();
        assert!(one >= kdx_hot::prices::LIKE_SCAN_ROWS as u64, "cost {one}");
        let n = (BUDGET_CEILING / one + 1) as usize;
        let many: String = (0..n)
            .map(|i| format!("a{i}: lastTokenPriceInKda(moduleName: \"kdx\") "))
            .collect();
        assert_eq!(
            served(&format!("{{ {many} }}"), json!({})),
            Err(format!("{:?}", Some(&Value::from("budget"))))
        );
        let few: String = (0..4)
            .map(|i| format!("a{i}: lastTokenPriceInKda(moduleName: \"kdx\") "))
            .collect();
        assert!(served(&format!("{{ {few} }}"), json!({})).is_ok());
    }

    /// B1: aliases of the nested `first: 0` document past the ceiling are refused by the
    /// budget with `limit = "budget"`; the explorer's documents at real page sizes are not.
    #[test]
    fn the_budget_refuses_above_its_ceiling() {
        let one = served(
            &format!("{{ {} }}", nested("(first: 0)", "(first: 0)")),
            json!({}),
        )
        .unwrap();
        let n = (BUDGET_CEILING / one + 1) as usize;
        let many: String = (0..n)
            .map(|i| format!("a{i}: {} ", nested("(first: 0)", "(first: 0)")))
            .collect();
        assert_eq!(
            served(&format!("{{ {many} }}"), json!({})),
            Err(format!("{:?}", Some(&Value::from("budget"))))
        );
        let fewer: String = (0..n - 1)
            .map(|i| format!("a{i}: {} ", nested("(first: 0)", "(first: 0)")))
            .collect();
        assert!(served(&format!("{{ {fewer} }}"), json!({})).is_ok_and(|c| c <= BUDGET_CEILING));
        assert!(over_ceiling(BUDGET_CEILING).is_ok());
        assert!(over_ceiling(BUDGET_CEILING + 1).is_err());
    }

    #[test]
    fn sr7_3_unresolved_total_counts_fail_closed() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        let over = Err("\"counts\"".to_string());
        // Happy path: two deferred counts pass, a block's own counts are not walks (its
        // events count is priced at a default page, M2).
        assert_eq!(
            c("{ a: transactions(accountName: \"a\") { totalCount } b: events(qualifiedEventName: \"x\") { totalCount } }"),
            Ok(2 * 101)
        );
        assert!(
            c("{ block(hash: \"h\") { transactions { totalCount } events { totalCount } } }")
                .unwrap()
                < 40
        );
        // Three deferred counts through an inline fragment on an interface (`Node`).
        let q: String = (0..3)
            .map(|i| {
                format!(
                    "t{i}: transactions(accountName: \"a\") {{ ... on Node {{ totalCount }} }} "
                )
            })
            .collect();
        assert_eq!(c(&format!("{{ {q} }}")), over);
        // Through a named fragment spread whose type condition is not a connection.
        let q: String = (0..3)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ ...F }} "))
            .collect();
        assert_eq!(
            c(&format!("{{ {q} }} fragment F on Node {{ totalCount }}")),
            over
        );
        // Through an inline fragment on an unknown type, and an unresolved connection field.
        let q: String = (0..3)
            .map(|i| {
                format!(
                    "t{i}: transactions(accountName: \"a\") {{ ... on Mystery {{ totalCount }} }} "
                )
            })
            .collect();
        assert_eq!(c(&format!("{{ {q} }}")), over);
        assert_eq!(c("{ ... on Mystery { list { totalCount } } }"), over);
        assert_eq!(
            c("{ ... on Mutation { list(first: 1) { totalCount } } }"),
            over
        );
        // An unresolved field is never free: its subtree is priced per item.
        assert!(c("{ ... on Mystery { a b c } }").unwrap() >= 300);
        assert_eq!(
            c("{ ... on Mystery { a { b c } } }"),
            Err("\"complexity\"".into())
        );
    }

    #[test]
    fn wide_lists_cost_up_to_200_and_the_rest_100() {
        let c = |q: &str| cost(q, serde_json::json!({})).unwrap().complexity;
        // Block lists and balances: 200 items (+1 for the field itself).
        assert_eq!(
            c("{ blocksFromDepth(minimumDepth: 0, first: 500) { edges { node { hash } } } }"),
            201
        );
        assert_eq!(
            c("{ blocksFromHeight(startHeight: 0, first: 120) { edges { node { hash } } } }"),
            121
        );
        assert_eq!(
            c("{ balance(accountName: \"a\", first: 200) { edges { node { balance } } } }"),
            201
        );
        // Everything else stays at 100.
        assert_eq!(
            c("{ transactions(first: 200) { edges { node { hash } } } }"),
            101
        );
        assert_eq!(c("{ blocksFromDepth(minimumDepth: 0, first: 5) { edges { node { transactions(first: 200) { edges { node { hash } } } } } } }"), 1 + 5 * 101);
    }

    #[test]
    fn connections_multiply_their_edges_only() {
        let c = |q: &str| cost(q, serde_json::json!({})).unwrap().complexity;
        // 1 (transactions) + 100 (a list's totalCount walk) + 2 (pageInfo) + 100 x 1 (hash).
        assert_eq!(
            c("{ transactions(first: 100) { totalCount pageInfo { hasNextPage } edges { node { hash } } } }"),
            203
        );
        // A connection that selects only its count reads no page; a block's count is
        // known with its rows (cost 1), a list's count walk costs max_page.
        assert_eq!(
            c("{ block(hash: \"x\") { transactions { totalCount } } }"),
            3
        );
        assert_eq!(c("{ transactions { totalCount } }"), 101);
        // A list (no connection fields) is multiplied whole: `quantity` sizes it.
        assert_eq!(
            c("subscription { transactions(quantity: 50) { hash } }"),
            51
        );
        assert_eq!(c("subscription { newBlocks(quantity: 500) { hash } }"), 101);
        // An unbound quantity costs max_page.
        assert_eq!(
            cost(
                "subscription($q: Int) { transactions(quantity: $q) { hash } }",
                serde_json::json!({})
            )
            .unwrap()
            .complexity,
            101
        );
    }

    fn block_fields(n: usize) -> String {
        (0..n).map(|i| format!("h{i}: hash ")).collect()
    }

    #[test]
    fn complexity_counts_pages() {
        // 1 + 100 * n (the `edges` and `node` wrappers cost nothing)
        let q = |n| {
            format!(
                "{{ blocksFromHeight(startHeight: 0, first: 100) {{ edges {{ node {{ {} }} }} }} }}",
                block_fields(n)
            )
        };
        assert_eq!(
            cost(&q(20), serde_json::json!({})),
            Err("\"complexity\"".into())
        );
        assert_eq!(
            cost(&q(19), serde_json::json!({})).unwrap().complexity,
            1901
        );
        // A variable page size counts like a literal; 1000 is clamped to 200 on a block list.
        let qv = format!(
            "query($n: Int) {{ blocksFromHeight(startHeight: 0, first: $n) {{ edges {{ node {{ {} }} }} }} }}",
            block_fields(9)
        );
        assert_eq!(
            cost(&qv, serde_json::json!({"n": 1000}))
                .unwrap()
                .complexity,
            1801
        );
        assert_eq!(
            cost(&qv, serde_json::json!({"n": 1})).unwrap().complexity,
            10
        );
        // Omitted: the default page (20).
        let qd = "{ transactions(blockHash: \"x\") { edges { node { hash } } } }";
        assert_eq!(cost(qd, serde_json::json!({})).unwrap().complexity, 21);
    }

    #[test]
    fn aliases_and_fragments_do_not_bypass() {
        let aliased: String = (0..100)
            .map(|i| format!("t{i}: transactions(blockHash: \"x\", first: 100) {{ edges {{ node {{ hash }} }} }} "))
            .collect();
        assert_eq!(
            cost(&format!("{{ {aliased} }}"), serde_json::json!({})),
            Err("\"complexity\"".into())
        );
        // The same through a fragment spread twice per level.
        let q = "query { ...A ...A } fragment A on Query { transactions(blockHash: \"x\", first: 100) { edges { node { ...B ...B } } } } fragment B on Transaction { hash id }";
        assert_eq!(
            cost(q, serde_json::json!({})).unwrap().complexity,
            2 * (1 + 100 * 4)
        );
        // Exponential fragment fan-out hits the visit budget, quickly.
        let mut q = String::from("query { ...F0 }");
        for i in 0..40 {
            q.push_str(&format!(
                " fragment F{i} on Query {{ ...F{} ...F{} }}",
                i + 1,
                i + 1
            ));
        }
        q.push_str(" fragment F40 on Query { lastBlockHeight }");
        let t = std::time::Instant::now();
        assert_eq!(
            cost(&q, serde_json::json!({})),
            Err("\"complexity\"".into())
        );
        assert!(t.elapsed() < std::time::Duration::from_secs(1));
        // A fragment cycle does not loop (validation refuses it).
        assert!(cost(
            "query { ...A } fragment A on Query { ...A }",
            serde_json::json!({})
        )
        .is_ok());
    }

    #[test]
    fn depth_and_introspection_depth() {
        let nest = |n: usize| {
            let mut q =
                String::from("{ blocksFromHeight(startHeight: 0, first: 1) { edges { node { ");
            for _ in 0..n {
                q.push_str("parent { ");
            }
            q.push_str("hash");
            q.push_str(&" }".repeat(n + 4));
            q
        };
        assert_eq!(cost(&nest(6), serde_json::json!({})).unwrap().depth, 10);
        assert_eq!(
            cost(&nest(7), serde_json::json!({})),
            Err("\"depth\"".into())
        );
        // Inline fragments do not reset depth.
        let q = "{ block(hash: \"x\") { ... on Block { parent { ... on Block { parent { parent { parent { parent { parent { parent { parent { parent { hash } } } } } } } } } } } } }";
        assert_eq!(cost(q, serde_json::json!({})), Err("\"depth\"".into()));
        // Introspection gets its own cap.
        let mut q = String::from("{ __schema { types { fields { type { ");
        q.push_str(&"ofType { ".repeat(9));
        q.push_str("name");
        q.push_str(&" }".repeat(13));
        q.push_str(" }");
        let c = cost(&q, serde_json::json!({})).unwrap();
        assert_eq!((c.introspection_depth, c.depth), (14, 0));
        let deeper = q
            .replacen("ofType { ", "ofType { ofType { ", 1)
            .replacen(" }", " } }", 1);
        assert_eq!(
            cost(&deeper, serde_json::json!({})),
            Err("\"introspection_depth\"".into())
        );
    }

    #[test]
    fn sizes() {
        let s = GraphqlSettings::default();
        let big = format!("{{ lastBlockHeight }} #{}", "x".repeat(33 * 1024));
        assert!(check_sizes(&big, &Variables::default(), &s).is_err());
        let v = crate::http::variables(serde_json::json!({ "a": "x".repeat(65 * 1024) })).unwrap();
        assert!(check_sizes("{ lastBlockHeight }", &v, &s).is_err());
        assert!(check_sizes("{ lastBlockHeight }", &Variables::default(), &s).is_ok());
    }

    #[test]
    fn sr7_1_unbound_page_variables_cost_the_maximum() {
        let q = |n| {
            format!(
                "query($n: Int{n}) {{ blocksFromHeight(startHeight: 0, first: $n) {{ edges {{ node {{ {} }} }} }} }}",
                block_fields(20)
            )
        };
        // No variables, no default: costed at max_page (100), so 20 fields x 100 + 1 > 2,000.
        assert_eq!(
            cost(&q(""), serde_json::json!({})).unwrap_err(),
            "\"complexity\""
        );
        // The default applies when the variable is absent (clamped to 100 as at run time).
        assert_eq!(
            cost(&q(" = 1000"), serde_json::json!({})).unwrap_err(),
            "\"complexity\""
        );
        assert!(cost(&q(" = 5"), serde_json::json!({})).is_ok());
        // A given value wins over the default.
        assert!(cost(&q(" = 1000"), serde_json::json!({"n": 5})).is_ok());
        // `nodes(ids: $ids)` unbound counts max_page ids.
        let n = format!(
            "query($ids: [ID!]!) {{ nodes(ids: $ids) {{ id {} }} }}",
            "id ".repeat(25)
        );
        assert_eq!(
            cost(&n, serde_json::json!({})).unwrap_err(),
            "\"complexity\""
        );
    }

    #[test]
    fn sr7_2_introspection_is_counted_and_nesting_is_capped() {
        let std_q = include_str!("../tests/introspection.graphql");
        let doc = async_graphql::parser::parse_query(std_q).unwrap();
        let wide = GraphqlSettings {
            introspection_depth: 64,
            ..GraphqlSettings::default()
        };
        let c = analyze(&doc, &Variables::default(), &wide).unwrap();
        assert_eq!(
            c.introspection_depth,
            GraphqlSettings::default().introspection_depth
        );
        assert!(cost(std_q, serde_json::json!({})).is_ok());
        let three = std_q.replace("query IntrospectionQuery {", "query IntrospectionQuery { a: __schema { types { ...FullType } } b: __schema { types { ...FullType } } ");
        assert_eq!(
            cost(&three, serde_json::json!({})).unwrap_err(),
            "\"introspection_complexity\""
        );
        // Repeated `fields` on one path.
        let nested = "{ __schema { types { fields { type { fields { type { name } } } } } } }";
        assert_eq!(
            cost(nested, serde_json::json!({})).unwrap_err(),
            "\"introspection_nesting\""
        );
        // Aliases of a heavy introspection subtree add up against the introspection cap.
        let heavy = "fields { args { type { name } } type { name } } inputFields { type { name } } enumValues { name } possibleTypes { name } interfaces { name }";
        let many: String = (0..40)
            .map(|i| format!("a{i}: __schema {{ types {{ {heavy} }} }} "))
            .collect();
        assert_eq!(
            cost(&format!("{{ {many} }}"), serde_json::json!({})).unwrap_err(),
            "\"introspection_complexity\""
        );
        // `__type` nesting is capped too.
        let t = "{ __type(name: \"Block\") { fields { type { ofType { fields { name } } } } } }";
        assert_eq!(
            cost(t, serde_json::json!({})).unwrap_err(),
            "\"introspection_nesting\""
        );
    }
}
