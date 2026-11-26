//! Response members in selection-set order (GraphQL spec, "Serialized Map Ordering").
//!
//! async-graphql 7.2.1 resolves the fields of a selection set concurrently and inserts each
//! result into the response object as it completes (`do_resolve_container` drains a
//! `FuturesUnordered`). Any field that awaits (a heavy slot, a deadline, a loader) can land
//! after a later sibling, so the member order follows completion and changes from run to run.
//!
//! This extension keeps the concurrent execution and only fixes the output: after execution it
//! walks the response next to the executed operation and re-inserts each object's members in
//! the order the spec's CollectFields gives (aliases, fragment spreads, inline fragments,
//! `@skip`/`@include`, first occurrence wins). Members it cannot place (none expected) go
//! after the placed ones, so nothing is ever dropped.
//!
//! Type conditions are matched against the object's runtime type: the static field type when
//! it is an object type, else a selected `__typename`. When neither is known (an abstract type
//! with no `__typename`), every fragment counts, which can only differ from the spec when two
//! fragments for different types select the same keys in opposite orders.
//!
//! Cost: one pass over the response (already built in memory) plus one CollectFields per
//! distinct (selection set, runtime type) pair, cached for the request. Recursion follows the
//! response nesting, which the depth limit in [`crate::limits`] bounds; fragment expansion is
//! capped at [`MAX_FRAGMENT_DEPTH`] (validation already rejects fragment cycles).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextExecute, NextParseQuery,
};
use async_graphql::indexmap::IndexMap;
use async_graphql::parser::types::{
    Directive, ExecutableDocument, Field, OperationDefinition, Selection, SelectionSet,
};
use async_graphql::registry::{MetaType, Registry};
use async_graphql::{Name, PathSegment, Positioned, Response, ServerResult, Value, Variables};

/// Fragment nesting beyond this is not expanded (validation bounds it far lower in practice).
pub const MAX_FRAGMENT_DEPTH: usize = 64;

/// The extension (installed by `build_schema`).
pub struct FieldOrder;

impl ExtensionFactory for FieldOrder {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(FieldOrderExt(Mutex::default()))
    }
}

/// The parsed document and variables of this request (one extension instance per request;
/// a subscription parses once and executes once per event).
struct FieldOrderExt(Mutex<Option<Arc<(ExecutableDocument, Variables)>>>);

#[async_graphql::async_trait::async_trait]
impl Extension for FieldOrderExt {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let doc = next.run(ctx, query, variables).await?;
        if let Ok(mut g) = self.0.lock() {
            *g = Some(Arc::new((doc.clone(), variables.clone())));
        }
        Ok(doc)
    }

    async fn execute(
        &self,
        ctx: &ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: NextExecute<'_>,
    ) -> Response {
        let mut r = next.run(ctx, operation_name).await;
        let parsed = self.0.lock().ok().and_then(|g| g.clone());
        if let Some(p) = parsed {
            reorder_response(&ctx.schema_env.registry, &p.0, &p.1, operation_name, &mut r);
        }
        r
    }
}

/// Put every object of `r.data` in selection-set order.
pub fn reorder_response(
    registry: &Registry,
    doc: &ExecutableDocument,
    variables: &Variables,
    operation_name: Option<&str>,
    r: &mut Response,
) {
    let Some(op) = pick_operation(doc, operation_name) else {
        return;
    };
    let root = match op.ty {
        async_graphql::parser::types::OperationType::Query => Some(registry.query_type.as_str()),
        async_graphql::parser::types::OperationType::Mutation => registry.mutation_type.as_deref(),
        async_graphql::parser::types::OperationType::Subscription => {
            registry.subscription_type.as_deref()
        }
    };
    let mut w = Walker {
        registry,
        doc,
        variables,
        op,
        cache: HashMap::new(),
        failed: r.errors.iter().map(|e| seg_path(&e.path)).collect(),
        path: Vec::new(),
    };
    w.value(&mut r.data, &[&op.selection_set.node], root);
}

fn pick_operation<'a>(
    doc: &'a ExecutableDocument,
    name: Option<&str>,
) -> Option<&'a OperationDefinition> {
    let mut ops = doc.operations.iter();
    match name {
        Some(n) => ops
            .find(|(k, _)| k.map(|k| k.as_str()) == Some(n))
            .map(|(_, op)| &op.node),
        None => {
            let first = ops.next()?;
            ops.next().is_none().then_some(&first.1.node)
        }
    }
}

/// A response path segment that can be hashed (`PathSegment` cannot).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Seg {
    Field(Name),
    Index(usize),
}

fn seg_path(p: &[PathSegment]) -> Vec<Seg> {
    p.iter()
        .map(|s| match s {
            PathSegment::Field(f) => Seg::Field(Name::new(f)),
            PathSegment::Index(i) => Seg::Index(*i),
        })
        .collect()
}

/// The ordered response keys of one object, each with the field nodes that share it.
type Collected<'a> = Arc<Vec<(Name, Vec<&'a Field>)>>;

struct Walker<'a> {
    registry: &'a Registry,
    doc: &'a ExecutableDocument,
    variables: &'a Variables,
    op: &'a OperationDefinition,
    /// Keyed by the selection sets' addresses (stable: `doc` is borrowed for the walk) and
    /// the runtime type.
    cache: HashMap<(Vec<usize>, Option<String>), Collected<'a>>,
    /// The paths of the response's errors (empty on success).
    failed: HashSet<Vec<Seg>>,
    /// Where the walk is in the response.
    path: Vec<Seg>,
}

impl<'a> Walker<'a> {
    fn value(&mut self, v: &mut Value, sets: &[&'a SelectionSet], ty: Option<&str>) {
        match v {
            Value::List(items) => {
                for (i, it) in items.iter_mut().enumerate() {
                    self.path.push(Seg::Index(i));
                    self.value(it, sets, ty);
                    self.path.pop();
                }
            }
            Value::Object(map) => self.object(map, sets, ty),
            _ => {}
        }
    }

    fn object(
        &mut self,
        map: &mut IndexMap<Name, Value>,
        sets: &[&'a SelectionSet],
        ty: Option<&str>,
    ) {
        if sets.is_empty() {
            return;
        }
        let runtime = self.runtime_type(map, sets, ty);
        let collected = self.collect(sets, runtime.as_deref());
        let parent = runtime.as_deref().or(ty);
        let mut out = IndexMap::with_capacity(map.len());
        for (key, fields) in collected.iter() {
            let Some(mut v) = map.swap_remove(key) else {
                if self.failed_nullable(key, parent, fields[0]) {
                    out.insert(key.clone(), Value::Null);
                }
                continue;
            };
            let child_sets: Vec<&'a SelectionSet> = fields
                .iter()
                .map(|f| &f.selection_set.node)
                .filter(|s| !s.items.is_empty())
                .collect();
            if !child_sets.is_empty() {
                let child_ty = parent.and_then(|p| self.field_type(p, fields[0]));
                self.path.push(Seg::Field(key.clone()));
                self.value(&mut v, &child_sets, child_ty.as_deref());
                self.path.pop();
            }
            out.insert(key.clone(), v);
        }
        // Anything not placed (none expected) goes at the end. `swap_remove` keeps this
        // linear in the member count; it can reorder only these leftovers.
        out.extend(map.drain(..));
        *map = out;
    }

    /// The object's concrete type: the static type when it is an object type, else the value
    /// of a selected `__typename`.
    fn runtime_type(
        &mut self,
        map: &IndexMap<Name, Value>,
        sets: &[&'a SelectionSet],
        ty: Option<&str>,
    ) -> Option<String> {
        if let Some(t) = ty {
            if matches!(self.registry.types.get(t), Some(MetaType::Object { .. })) {
                return Some(t.to_owned());
            }
        }
        let all = self.collect(sets, None);
        all.iter().find_map(|(key, fields)| {
            if fields[0].name.node != "__typename" {
                return None;
            }
            match map.get(key) {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            }
        })
    }

    /// async-graphql drops the member of a field whose resolver failed; the spec answers a
    /// nullable one with `null` in place (and reports the error at its path).
    fn failed_nullable(&mut self, key: &Name, parent: Option<&str>, f: &Field) -> bool {
        if self.failed.is_empty() {
            return false;
        }
        let Some(meta) = parent
            .and_then(|p| self.registry.types.get(p))
            .and_then(|t| t.field_by_name(&f.name.node))
        else {
            return false;
        };
        if meta.ty.ends_with('!') {
            return false;
        }
        self.path.push(Seg::Field(key.clone()));
        let hit = self.failed.contains(&self.path);
        self.path.pop();
        hit
    }

    /// The named type a field of `parent` returns.
    fn field_type(&self, parent: &str, f: &Field) -> Option<String> {
        let meta = self
            .registry
            .types
            .get(parent)?
            .field_by_name(&f.name.node)?;
        Some(meta.ty.trim_matches(['[', ']', '!']).to_owned())
    }

    fn collect(&mut self, sets: &[&'a SelectionSet], runtime: Option<&str>) -> Collected<'a> {
        let key = (
            sets.iter()
                .map(|s| *s as *const SelectionSet as usize)
                .collect(),
            runtime.map(str::to_owned),
        );
        if let Some(c) = self.cache.get(&key) {
            return c.clone();
        }
        let mut acc: IndexMap<Name, Vec<&'a Field>> = IndexMap::new();
        for s in sets {
            self.collect_into(s, runtime, 0, &mut acc);
        }
        let c: Collected<'a> = Arc::new(acc.into_iter().collect());
        self.cache.insert(key, c.clone());
        c
    }

    fn collect_into(
        &self,
        set: &'a SelectionSet,
        runtime: Option<&str>,
        depth: usize,
        acc: &mut IndexMap<Name, Vec<&'a Field>>,
    ) {
        for sel in &set.items {
            match &sel.node {
                Selection::Field(f) => {
                    if self.included(&f.node.directives) {
                        acc.entry(f.node.response_key().node.clone())
                            .or_default()
                            .push(&f.node);
                    }
                }
                Selection::FragmentSpread(s) => {
                    if depth >= MAX_FRAGMENT_DEPTH || !self.included(&s.node.directives) {
                        continue;
                    }
                    let Some(frag) = self.doc.fragments.get(&s.node.fragment_name.node) else {
                        continue;
                    };
                    if self.applies(Some(&frag.node.type_condition.node.on.node), runtime) {
                        self.collect_into(&frag.node.selection_set.node, runtime, depth + 1, acc);
                    }
                }
                Selection::InlineFragment(i) => {
                    if depth >= MAX_FRAGMENT_DEPTH || !self.included(&i.node.directives) {
                        continue;
                    }
                    let cond = i.node.type_condition.as_ref().map(|c| &c.node.on.node);
                    if self.applies(cond, runtime) {
                        self.collect_into(&i.node.selection_set.node, runtime, depth + 1, acc);
                    }
                }
            }
        }
    }

    fn applies(&self, cond: Option<&Name>, runtime: Option<&str>) -> bool {
        match (cond, runtime) {
            (None, _) | (_, None) => true,
            (Some(c), Some(rt)) => {
                c.as_str() == rt
                    || self
                        .registry
                        .types
                        .get(c.as_str())
                        .is_some_and(|t| t.is_possible_type(rt))
            }
        }
    }

    /// `@skip(if:)` / `@include(if:)`, with variables and their defaults.
    fn included(&self, dirs: &[Positioned<Directive>]) -> bool {
        for d in dirs {
            let want = match d.node.name.node.as_str() {
                "skip" => false,
                "include" => true,
                _ => continue,
            };
            let cond = d
                .node
                .get_argument("if")
                .and_then(|v| v.node.clone().into_const_with(|n| self.var(&n)).ok());
            if cond == Some(Value::Boolean(!want)) {
                return false;
            }
        }
        true
    }

    fn var(&self, n: &Name) -> Result<Value, ()> {
        if let Some(v) = self.variables.get(n) {
            return Ok(v.clone());
        }
        self.op
            .variable_definitions
            .iter()
            .find(|d| d.node.name.node == *n)
            .and_then(|d| d.node.default_value.as_ref().map(|v| v.node.clone()))
            .ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema, SimpleObject, Union};

    use super::FieldOrder;

    async fn nap(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    #[derive(SimpleObject)]
    struct Cat {
        name: String,
        lives: i32,
    }
    #[derive(SimpleObject)]
    struct Dog {
        name: String,
        barks: bool,
    }
    #[derive(Union)]
    enum Pet {
        Cat(Cat),
        Dog(Dog),
    }

    struct Inner;
    #[Object]
    impl Inner {
        async fn p(&self) -> i32 {
            nap(30).await;
            1
        }
        async fn q(&self) -> i32 {
            2
        }
    }

    struct Q;
    #[Object]
    impl Q {
        async fn slow(&self) -> i32 {
            nap(40).await;
            1
        }
        async fn mid(&self) -> i32 {
            nap(15).await;
            2
        }
        async fn fast(&self) -> i32 {
            3
        }
        async fn boom(&self) -> async_graphql::Result<Option<i32>> {
            nap(20).await;
            Err("boom".into())
        }
        async fn inner(&self) -> Inner {
            nap(25).await;
            Inner
        }
        async fn pets(&self) -> Vec<Pet> {
            vec![
                Pet::Cat(Cat {
                    name: "c".into(),
                    lives: 9,
                }),
                Pet::Dog(Dog {
                    name: "d".into(),
                    barks: true,
                }),
            ]
        }
    }

    fn schema(ordered: bool) -> Schema<Q, EmptyMutation, EmptySubscription> {
        let b = Schema::build(Q, EmptyMutation, EmptySubscription);
        if ordered {
            b.extension(FieldOrder).finish()
        } else {
            b.finish()
        }
    }

    fn keys(v: &serde_json::Value) -> Vec<String> {
        v.as_object().unwrap().keys().cloned().collect()
    }

    async fn run(s: &Schema<Q, EmptyMutation, EmptySubscription>, q: &str) -> serde_json::Value {
        s.execute(q).await.data.into_json().unwrap()
    }

    const ROOT: &str = "{ a: slow mid fast b: slow c: fast z: mid }";

    /// The engine alone answers in completion order (documents the upstream behaviour the
    /// extension corrects; if this starts failing, async-graphql fixed it).
    #[tokio::test]
    async fn engine_alone_answers_in_completion_order() {
        let d = run(&schema(false), ROOT).await;
        assert_ne!(keys(&d), ["a", "mid", "fast", "b", "c", "z"]);
    }

    #[tokio::test]
    async fn root_fields_in_selection_order_every_time() {
        let s = schema(true);
        for _ in 0..20 {
            let d = run(&s, ROOT).await;
            assert_eq!(keys(&d), ["a", "mid", "fast", "b", "c", "z"]);
        }
    }

    #[tokio::test]
    async fn fragments_directives_errors_and_nesting() {
        let s = schema(true);
        let q = r#"
            query ($no: Boolean = false, $yes: Boolean!) {
                fast @skip(if: true)
                inner { p q }
                ...F
                mid @include(if: $no)
                ... on Q { x: fast }
                boom
                y: mid @include(if: $yes)
                fast
                r: inner { q p }
            }
            fragment F on Q { slow w: boom }
        "#;
        for _ in 0..20 {
            let req = async_graphql::Request::new(q).variables(
                async_graphql::Variables::from_json(serde_json::json!({"yes": true})),
            );
            let r = s.execute(req).await;
            assert_eq!(r.errors.len(), 2, "{:?}", r.errors);
            let d = r.data.into_json().unwrap();
            // `fast` is skipped at its first position, so its key sits at its second one.
            assert_eq!(
                keys(&d),
                ["inner", "slow", "w", "x", "boom", "y", "fast", "r"],
                "{d}"
            );
            assert!(d["boom"].is_null() && d["w"].is_null());
            assert_eq!(keys(&d["inner"]), ["p", "q"]);
            assert_eq!(keys(&d["r"]), ["q", "p"]);
        }
    }

    /// When every root field fails async-graphql answers `data: null`; that stays as is (the
    /// HTTP layer reads it as "nothing ran", e.g. a missing variable, as stock answers).
    #[tokio::test]
    async fn only_failed_fields_keep_null_data() {
        let s = schema(true);
        let r = s.execute("{ boom w: boom }").await;
        assert_eq!(r.errors.len(), 2);
        assert_eq!(r.data.into_json().unwrap(), serde_json::Value::Null);
    }

    #[tokio::test]
    async fn union_members_follow_their_fragment() {
        let s = schema(true);
        let q = "{ pets { __typename ... on Dog { barks name } ... on Cat { name lives } } }";
        let d = run(&s, q).await;
        let pets = d["pets"].as_array().unwrap();
        assert_eq!(keys(&pets[0]), ["__typename", "name", "lives"]);
        assert_eq!(keys(&pets[1]), ["__typename", "barks", "name"]);
    }

    /// The real schema (the probe from the DEX review: these orders varied run to run).
    #[tokio::test]
    async fn real_schema_root_order() {
        let f = crate::testutil::store();
        let s = crate::build_schema(f.st.clone(), crate::GraphqlSettings::default(), None);
        for (q, want) in [
            (
                "{ n: graphConfiguration { version minimumBlockHeight } lastBlockHeight m: lastBlockHeight networkInfo { networkId } }",
                vec!["n", "lastBlockHeight", "m", "networkInfo"],
            ),
            (
                "{ lastBlockHeight graphConfiguration { minimumBlockHeight version } }",
                vec!["lastBlockHeight", "graphConfiguration"],
            ),
            (
                "{ networkInfo { networkId } a: lastBlockHeight z: graphConfiguration { version } }",
                vec!["networkInfo", "a", "z"],
            ),
        ] {
            for _ in 0..20 {
                let r = s.execute(q).await;
                let d = r.data.into_json().unwrap();
                assert_eq!(keys(&d), want, "{q} -> {d} {:?}", r.errors);
            }
        }
    }

    #[tokio::test]
    async fn named_operation_is_picked() {
        let s = schema(true);
        let q = "query A { fast slow } query B { slow fast mid }";
        let r = s
            .execute(async_graphql::Request::new(q).operation_name("B"))
            .await;
        assert_eq!(keys(&r.data.into_json().unwrap()), ["slow", "fast", "mid"]);
    }
}
