//! Query limits (GQ-9), run on every parsed document before validation and execution
//! (HTTP and websocket alike), as an async-graphql extension:
//! - query text ≤ `max_query_bytes`, variables ≤ `max_variables_bytes`;
//! - depth ≤ `max_depth`; `__schema`/`__type` subtrees are exempt from it and capped at
//!   `introspection_depth` instead;
//! - complexity ≤ `max_complexity` (cost model below).
//!
//! Fragments are expanded where they are spread, so neither fragments, aliases nor inline
//! fragments get around depth or complexity. Every operation of a document is checked (not
//! only the one that runs). The walk itself is bounded by [`MAX_VISITS`].
//!
//! Cost model (field types from the schema, keyed on field names, never aliases): each field
//! costs 1 (`__typename` 0, and a connection's `edges` and their `node` 0). A
//! deferred `totalCount` (every one but a short list of cheap connections'; fails closed) costs `max_page`, and an operation may run at
//! most [`MAX_COUNTS`] of them, outer multipliers included. A connection or list field that returns a page
//! (`first`/`last` or a connection field without them) multiplies the cost of its
//! selection (of a connection, its `edges` only: `totalCount` and `pageInfo` are answered
//! once per connection) by the page size it will return (clamped like the resolver: `max_page`, or
//! `default_page` when omitted). `nodes(ids)` multiplies by the number of ids; per-chain
//! account lists by the chain count (20 or `chainIds`).

use std::sync::Arc;

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextParseQuery, NextValidation,
};
use async_graphql::parser::types::{
    DocumentOperations, ExecutableDocument, Field, FragmentDefinition, OperationType, Selection,
    SelectionSet,
};
use async_graphql::registry::Registry;
use async_graphql::{Name, ServerError, ServerResult, ValidationResult, Value, Variables};

use crate::error::limit;
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

/// Deferred `totalCount`s (count walks) one operation may run, outer multipliers included.
pub const MAX_COUNTS: u64 = 2;

/// Connections whose `totalCount` is known with their page (a block's or a tx's rows, block
/// ranges, an account's transfers): bounded and cheap. Every other `totalCount`, including
/// one under a type the walker cannot resolve, is a deferred count walk (fail closed, SR7-3).
const CHEAP_COUNTS: &[&str] = &[
    "BlockEventsConnection",
    "BlockTransactionsConnection",
    "FungibleAccountTransfersConnection",
    "FungibleChainAccountTransfersConnection",
    "QueryBalanceConnection",
    "QueryBlocksFromDepthConnection",
    "QueryBlocksFromHeightConnection",
    "QueryCompletedBlockHeightsConnection",
    "TransactionResultEventsConnection",
    "TransactionResultTransfersConnection",
];

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
        <crate::schema::Query as async_graphql::OutputType>::create_type_info(&mut r);
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
                    if name == "__typename" {
                        continue;
                    }
                    let intro = intro || (depth == 0 && (name == "__schema" || name == "__type"));
                    let d = depth + 1;
                    if intro {
                        self.cost.introspection_depth = self.cost.introspection_depth.max(d);
                        if d > self.s.introspection_depth {
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
                        if d > self.s.max_depth {
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
                        if self.intro_lists.contains(&name) {
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
                        self.multiplier(f).max(self.s.max_page as u64)
                    } else if pages {
                        self.multiplier(f)
                    } else {
                        1
                    };
                    let own: u64 = match name {
                        _ if intro => 1,
                        "edges" if wrap == Wrap::Conn => 0,
                        "node" if wrap == Wrap::Edges => 0,
                        // Cheap only under a known connection type listed as cheap; anywhere
                        // else (unknown or unresolved parent, interface, fragment) it is a
                        // count walk.
                        "totalCount" if !(wrap == Wrap::Conn && ty.is_some_and(cheap_count)) => {
                            // A count walk (GQ-10): a fixed cost, and few per operation.
                            self.counts = self.counts.saturating_add(self.outer);
                            if self.counts > MAX_COUNTS {
                                return Err(limit(
                                    "complexity",
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
                    let child = self.set(&f.selection_set.node, d, intro, fty, inner);
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
                    // A cycle or an unknown fragment is left to validation, which refuses it.
                    if self.stack.contains(&name) {
                        continue;
                    }
                    let Some(def) = self.frags.get(name) else {
                        continue;
                    };
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

    fn multiplier(&self, f: &'a Field) -> u64 {
        let s = self.s;
        // The wide lists clamp at `wide_page` (R-P5P9-20), everything else at `max_page`.
        let name = f.name.node.as_str();
        let cap = if WIDE.contains(&name) {
            s.wide_page()
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
        if first.is_some() || last.is_some() {
            return page(first.max(last));
        }
        // ZelCore `quantity` (subscriptions `newBlocks`, `transactions`): the list size.
        match self.arg(f, "quantity") {
            Arg::Unbound => return s.max_page as u64,
            Arg::Val(v) => return page(Self::int(&v)),
            Arg::Absent => {}
        }
        let list_len = |arg: &str, absent: u64, unbound: u64| match self.arg(f, arg) {
            Arg::Absent => absent,
            Arg::Unbound => unbound,
            Arg::Val(v) => Self::len(&v).unwrap_or(absent),
        };
        match name {
            "nodes" => list_len("ids", 1, s.max_page as u64),
            "fungibleChainAccounts" | "newBlocks" | "newBlocksFromDepth" => {
                list_len("chainIds", CHAINS, CHAINS)
            }
            "chainAccounts"
            | "fungibleAccountsByPublicKey"
            | "fungibleChainAccountsByPublicKey"
            | "countersOfEachChain" => CHAINS,
            "nonFungibleTokenBalances" => s.max_page as u64,
            _ if PAGED.contains(&name) => page(None),
            _ => 1,
        }
    }
}

/// The cost of every operation of `doc` (the largest), or the `LIMIT` error of the first
/// limit it passes.
pub fn analyze(
    doc: &ExecutableDocument,
    vars: &Variables,
    s: &GraphqlSettings,
) -> Result<Cost, ServerError> {
    let mut w = Walk {
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
        if c > s.max_complexity {
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
        Arc::new(LimitsExt)
    }
}

struct LimitsExt;

#[async_graphql::async_trait::async_trait]
impl Extension for LimitsExt {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let s = &ctx.data_unchecked::<Gql>().settings;
        check_sizes(query, variables, s)?;
        let doc = next.run(ctx, query, variables).await?;
        analyze(&doc, variables, s)?;
        Ok(doc)
    }
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

    fn cost(q: &str, vars: serde_json::Value) -> Result<Cost, String> {
        let doc = async_graphql::parser::parse_query(q).map_err(|e| e.to_string())?;
        let vars = crate::http::variables(vars).unwrap();
        analyze(&doc, &vars, &GraphqlSettings::default()).map_err(|e| {
            let x = e.extensions.unwrap();
            format!("{}", x.get("limit").unwrap())
        })
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
        assert_eq!(c(&format!("{{ {ten} }}")), Err("\"complexity\"".into()));
        let two: String = (0..2)
            .map(|i| format!("t{i}: transactions(accountName: \"a\") {{ totalCount }} "))
            .collect();
        assert_eq!(c(&format!("{{ {two} }}")), Ok(2 * 101));
        // A count walk under a per-chain list runs once per chain: refused.
        assert_eq!(
            c("{ fungibleChainAccounts(accountName: \"a\") { transactions { totalCount } } }"),
            Err("\"complexity\"".into())
        );
        // A block's count is not a walk: kadscan's block list passes.
        assert!(c("{ blocksFromDepth(minimumDepth: 0, first: 25) { totalCount edges { node { transactions { totalCount edges { node { cmd { meta { gasPrice } } } } } } } } }").unwrap() < 2000);
    }

    #[test]
    fn sr7_3_unresolved_total_counts_fail_closed() {
        let c = |q: &str| cost(q, serde_json::json!({})).map(|c| c.complexity);
        let over = Err("\"complexity\"".to_string());
        // Happy path: two deferred counts pass, a block's own counts are cheap.
        assert_eq!(
            c("{ a: transactions(accountName: \"a\") { totalCount } b: events(qualifiedEventName: \"x\") { totalCount } }"),
            Ok(2 * 101)
        );
        assert!(
            c("{ block(hash: \"h\") { transactions { totalCount } events { totalCount } } }")
                .unwrap()
                < 20
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
        assert_eq!(c("{ ... on Mystery { a { b c } } }"), over);
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
