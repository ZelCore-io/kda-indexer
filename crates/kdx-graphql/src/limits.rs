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
//! Cost model: each field costs 1 (`__typename` 0). A field that returns a page
//! (`first`/`last` or a connection field without them) multiplies the cost of its
//! selection by the page size it will return (clamped like the resolver: `max_page`, or
//! `default_page` when omitted). `nodes(ids)` multiplies by the number of ids; per-chain
//! account lists by the chain count (20 or `chainIds`).

use std::sync::Arc;

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextParseQuery, NextValidation,
};
use async_graphql::parser::types::{
    DocumentOperations, ExecutableDocument, Field, FragmentDefinition, Selection, SelectionSet,
};
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
];

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
}

impl<'a> Walk<'a> {
    fn set(
        &mut self,
        set: &'a SelectionSet,
        depth: usize,
        intro: bool,
    ) -> Result<u64, ServerError> {
        let mut total: u64 = 0;
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
                    let mult = if intro { 1 } else { self.multiplier(f) };
                    let child = self.set(&f.selection_set.node, d, intro)?;
                    total = total.saturating_add(1u64.saturating_add(mult.saturating_mul(child)));
                }
                Selection::InlineFragment(i) => {
                    total =
                        total.saturating_add(self.set(&i.node.selection_set.node, depth, intro)?);
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
                    let c = self.set(&def.node.selection_set.node, depth, intro)?;
                    self.stack.pop();
                    total = total.saturating_add(c);
                }
            }
        }
        Ok(total)
    }

    /// An argument's value with variables substituted (`None` when absent or unbound).
    fn arg(&self, f: &'a Field, name: &str) -> Option<Value> {
        f.arguments
            .iter()
            .find(|(k, _)| k.node.as_str() == name)
            .and_then(|(_, v)| {
                v.node
                    .clone()
                    .into_const_with(|n| self.vars.get(&n).cloned().ok_or(()))
                    .ok()
            })
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
        let page = |n: Option<i64>| match n {
            Some(n) => n.clamp(0, s.max_page as i64) as u64,
            None => s.default_page.min(s.max_page) as u64,
        };
        let first = self.arg(f, "first").and_then(|v| Self::int(&v));
        let last = self.arg(f, "last").and_then(|v| Self::int(&v));
        let name = f.name.node.as_str();
        if first.is_some() || last.is_some() {
            return page(first.max(last));
        }
        match name {
            "nodes" => self.arg(f, "ids").and_then(|v| Self::len(&v)).unwrap_or(1),
            "fungibleChainAccounts" | "newBlocks" | "newBlocksFromDepth" => self
                .arg(f, "chainIds")
                .and_then(|v| Self::len(&v))
                .unwrap_or(CHAINS),
            "chainAccounts"
            | "fungibleAccountsByPublicKey"
            | "fungibleChainAccountsByPublicKey" => CHAINS,
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
    };
    let ops: Vec<_> = match &doc.operations {
        DocumentOperations::Single(op) => vec![op],
        DocumentOperations::Multiple(m) => m.values().collect(),
    };
    for op in ops {
        let c = w.set(&op.node.selection_set.node, 0, false)?;
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

    fn block_fields(n: usize) -> String {
        (0..n).map(|i| format!("h{i}: hash ")).collect()
    }

    #[test]
    fn complexity_counts_pages() {
        // 1 + 100 * (edges 1 + node (1 + n))
        let q = |n| {
            format!(
                "{{ blocksFromHeight(startHeight: 0, first: 100) {{ edges {{ node {{ {} }} }} }} }}",
                block_fields(n)
            )
        };
        assert_eq!(
            cost(&q(18), serde_json::json!({})),
            Err("\"complexity\"".into())
        );
        assert_eq!(
            cost(&q(17), serde_json::json!({})).unwrap().complexity,
            1901
        );
        // A variable page size counts like a literal; 1000 is clamped to 100.
        let qv = format!(
            "query($n: Int) {{ blocksFromHeight(startHeight: 0, first: $n) {{ edges {{ node {{ {} }} }} }} }}",
            block_fields(17)
        );
        assert_eq!(
            cost(&qv, serde_json::json!({"n": 1000}))
                .unwrap()
                .complexity,
            1901
        );
        assert_eq!(
            cost(&qv, serde_json::json!({"n": 1})).unwrap().complexity,
            20
        );
        // Omitted: the default page (20).
        let qd = "{ transactions(blockHash: \"x\") { edges { node { hash } } } }";
        assert_eq!(cost(qd, serde_json::json!({})).unwrap().complexity, 61);
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
            2 * (1 + 100 * 6)
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
        q.push_str(&"ofType { ".repeat(10));
        q.push_str("name");
        q.push_str(&" }".repeat(14));
        q.push_str(" }");
        let c = cost(&q, serde_json::json!({})).unwrap();
        assert_eq!((c.introspection_depth, c.depth), (15, 0));
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
}
