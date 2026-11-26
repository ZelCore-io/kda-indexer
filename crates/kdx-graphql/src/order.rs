//! Response members in selection-set order (GraphQL spec, "Serialized Map Ordering"), and
//! field errors completed the way the spec and graphql-js (stock) complete them.
//!
//! async-graphql 7.2.1 resolves the fields of a selection set concurrently and inserts each
//! result into the response object as it completes (`do_resolve_container` drains a
//! `FuturesUnordered`). Any field that awaits (a heavy slot, a deadline, a loader) can land
//! after a later sibling, so the member order follows completion and changes from run to run.
//!
//! This extension keeps the concurrent execution and only fixes the output: after execution it
//! walks the response next to the executed operation and puts each object's members in the
//! order the spec's CollectFields gives (aliases, fragment spreads, inline fragments,
//! `@skip`/`@include`, first occurrence wins). Members it cannot place (none expected) go
//! after the placed ones, so nothing is ever dropped.
//!
//! Field errors: async-graphql drops the member of a failed field (and answers an object whose
//! members all failed as `null`). The spec ("Handling Field Errors") and graphql-js answer a
//! failed nullable field with `null` in place, and a failed non-null field by nulling its
//! nearest nullable parent (an object, a list item, or `data` itself). The walk does both,
//! only for a member that is in the collected set *and* has an error at exactly its path, so
//! it never invents a key and never nulls a value no error points at. An object async-graphql
//! answered `null` because every member failed is rebuilt when an error points below it.
//! `data` itself is rebuilt only when every error has a path and an `extensions.code` (all
//! resolver errors do, through [`crate::error::err`]); a request that never ran (a missing
//! variable: path but no code) keeps `data: null`.
//!
//! Type conditions are matched against the object's runtime type: the static field type when
//! it is an object type, else a selected `__typename`, else the only possible type (at most
//! [`MAX_INFER_TYPES`] are tried) whose collected keys cover the object's keys. When none is
//! known, every fragment counts and no `null` is restored inside that object; that can only
//! differ from the spec when two fragments for different types select the same keys in
//! opposite orders, or for a failed field there.
//!
//! Cost: one pass over the response (already built in memory) plus one CollectFields per
//! distinct (selection set, runtime type) pair, cached for the request as a plan that also
//! holds each member's declared type and child selection. An object already in order is not
//! rebuilt, so the common case allocates nothing per object. Recursion follows the response
//! nesting, which the depth limit in [`crate::limits`] bounds; fragment expansion is capped at
//! [`MAX_FRAGMENT_DEPTH`] (validation already rejects fragment cycles).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
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

/// An abstract type with more possible types than this is not inferred (see the module doc).
pub const MAX_INFER_TYPES: usize = 16;

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

/// Put every object of `r.data` in selection-set order and complete its field errors.
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
    let mut failed = HashSet::new();
    let mut under = HashSet::new();
    for e in r.errors.iter().filter(|e| !e.path.is_empty()) {
        let p = seg_path(&e.path);
        for n in 0..p.len() {
            under.insert(p[..n].to_vec());
        }
        failed.insert(p);
    }
    let mut w = Walker {
        registry,
        doc,
        variables,
        op,
        sets: Vec::new(),
        set_ids: HashMap::new(),
        plans: Vec::new(),
        failed,
        under,
        path: Vec::new(),
        last_object: None,
    };
    let root_sid = w.intern(vec![&op.selection_set.node]);
    // async-graphql answers `data: null` when every root field failed; stock answers the
    // failed nullable roots as `null` members. Only when each error is a resolver's (a path
    // and a code): a request-level error (no code) means nothing ran.
    let rebuild = r.data == Value::Null
        && !r.errors.is_empty()
        && r.errors.iter().all(|e| {
            !e.path.is_empty()
                && e.extensions
                    .as_ref()
                    .is_some_and(|x| x.get("code").is_some())
        });
    if rebuild {
        r.data = Value::Object(IndexMap::new());
    }
    let Value::Object(map) = &mut r.data else {
        return;
    };
    if w.object(map, root_sid, root) || map.is_empty() {
        r.data = Value::Null;
    }
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

/// Whether a declared type (`[Pet!]!`) admits `null`; an unknown type is treated as nullable
/// (no bubbling happens without a known type anyway).
fn nullable(ty: Option<&str>) -> bool {
    !ty.is_some_and(|t| t.ends_with('!'))
}

/// The item type of a declared list type (`[Pet!]!` -> `Pet!`), `None` when not a list.
fn item_type(ty: &str) -> Option<&str> {
    ty.strip_suffix('!')
        .unwrap_or(ty)
        .strip_prefix('[')?
        .strip_suffix(']')
}

/// One response key of an object: the first field node for it, its declared type on the
/// parent, and the merged child selection (an interned id) with the child's named type.
struct Entry<'a> {
    key: Name,
    ty: Option<&'a str>,
    child: Option<(usize, Option<&'a str>)>,
}

/// The ordered members of one (selection set, runtime type), built once per request.
struct Plan<'a> {
    entries: Vec<Entry<'a>>,
    /// The entries' keys, for the abstract-type inference's cover check (linear, not
    /// quadratic, in a wide aliased selection).
    keys: HashSet<Name>,
    /// The response key of a selected `__typename`.
    typename: Option<Name>,
}

/// A cached plan: (runtime type, parent type) -> plan, per interned selection.
type Plans<'a> = Vec<Vec<(Option<&'a str>, Option<&'a str>, Rc<Plan<'a>>)>>;

struct Walker<'a> {
    registry: &'a Registry,
    doc: &'a ExecutableDocument,
    variables: &'a Variables,
    op: &'a OperationDefinition,
    /// Interned selection-set lists (the merged selections of one response key); the map is
    /// keyed by the sets' addresses (stable: `doc` is borrowed for the walk).
    sets: Vec<Vec<&'a SelectionSet>>,
    set_ids: HashMap<Vec<usize>, usize>,
    /// Indexed by interned selection id.
    plans: Plans<'a>,
    /// The paths of the response's errors (empty on success).
    failed: HashSet<Vec<Seg>>,
    /// Every strict prefix of an error path.
    under: HashSet<Vec<Seg>>,
    /// Where the walk is in the response (kept only when there are errors).
    path: Vec<Seg>,
    /// The last named type found to be an object type (saves a registry lookup per row).
    last_object: Option<&'a str>,
}

impl<'a> Walker<'a> {
    fn intern(&mut self, sets: Vec<&'a SelectionSet>) -> usize {
        let key: Vec<usize> = sets
            .iter()
            .map(|s| *s as *const SelectionSet as usize)
            .collect();
        if let Some(&id) = self.set_ids.get(&key) {
            return id;
        }
        let id = self.sets.len();
        self.sets.push(sets);
        self.plans.push(Vec::new());
        self.set_ids.insert(key, id);
        id
    }

    /// `ty` is the declared type of `v` (with list and non-null wrappers), `named` its named
    /// type. Returns whether `v` must become `null` (the caller applies `ty`'s nullability).
    fn value(
        &mut self,
        v: &mut Value,
        sid: usize,
        ty: Option<&'a str>,
        named: Option<&'a str>,
    ) -> bool {
        match v {
            Value::List(items) => {
                let item = ty.and_then(item_type);
                let track = !self.failed.is_empty();
                for (i, it) in items.iter_mut().enumerate() {
                    if track {
                        self.path.push(Seg::Index(i));
                    }
                    let null_me = self.value(it, sid, item, named);
                    if track {
                        self.path.pop();
                    }
                    if null_me {
                        if !nullable(item) {
                            return true;
                        }
                        *it = Value::Null;
                    }
                }
                false
            }
            Value::Object(map) => self.object(map, sid, named),
            // An object whose members all failed (async-graphql answers it `null`).
            Value::Null
                if !self.failed.is_empty()
                    && ty.is_some_and(|t| item_type(t).is_none())
                    && self.under.contains(&self.path) =>
            {
                let mut map = IndexMap::new();
                if self.object(&mut map, sid, named) {
                    return true;
                }
                if !map.is_empty() {
                    *v = Value::Object(map);
                }
                false
            }
            _ => false,
        }
    }

    /// Orders `map` and completes its failed members; returns whether the object itself
    /// must become `null` (a failed non-null member).
    fn object(&mut self, map: &mut IndexMap<Name, Value>, sid: usize, ty: Option<&'a str>) -> bool {
        let runtime = self.runtime_type(map, sid, ty);
        let plan = self.plan(sid, runtime, runtime.or(ty));
        let mut in_order = true;
        let mut nulls: Vec<usize> = Vec::new();
        let mut pos = 0;
        let track = !self.failed.is_empty();
        for (n, e) in plan.entries.iter().enumerate() {
            // Already in place (the common case) needs no hashing.
            let at = match map.get_index(pos) {
                Some((k, _)) if *k == e.key => Some(pos),
                _ => map.get_index_of(&e.key),
            };
            let Some(idx) = at else {
                if self.failed_here(&e.key) && e.ty.is_some() {
                    if !nullable(e.ty) {
                        return true;
                    }
                    nulls.push(n);
                    in_order = false;
                }
                continue;
            };
            if let Some((child, named)) = e.child {
                if track {
                    self.path.push(Seg::Field(e.key.clone()));
                }
                let null_me = self.value(&mut map[idx], child, e.ty, named);
                if track {
                    self.path.pop();
                }
                if null_me {
                    if !nullable(e.ty) {
                        return true;
                    }
                    map[idx] = Value::Null;
                }
            }
            in_order &= idx == pos;
            pos += 1;
        }
        if in_order {
            return false;
        }
        let mut out = IndexMap::with_capacity(map.len() + nulls.len());
        let mut nulls = nulls.into_iter().peekable();
        for (n, e) in plan.entries.iter().enumerate() {
            if let Some(v) = map.swap_remove(&e.key) {
                out.insert(e.key.clone(), v);
            } else if nulls.next_if_eq(&n).is_some() {
                out.insert(e.key.clone(), Value::Null);
            }
        }
        // Anything not placed (none expected) goes at the end. `swap_remove` keeps this
        // linear in the member count; it can reorder only these leftovers.
        out.extend(map.drain(..));
        *map = out;
        false
    }

    /// Whether an error sits at exactly `self.path` + `key`.
    fn failed_here(&mut self, key: &Name) -> bool {
        if self.failed.is_empty() {
            return false;
        }
        self.path.push(Seg::Field(key.clone()));
        let hit = self.failed.contains(&self.path);
        self.path.pop();
        hit
    }

    /// The object's concrete type: the static type when it is an object type, else the value
    /// of a selected `__typename`, else the only possible type covering the object's keys.
    fn runtime_type(
        &mut self,
        map: &IndexMap<Name, Value>,
        sid: usize,
        ty: Option<&'a str>,
    ) -> Option<&'a str> {
        let registry = self.registry;
        let t = ty?;
        if self.last_object == Some(t) {
            return ty;
        }
        let meta = registry.types.get(t)?;
        if matches!(meta, MetaType::Object { .. }) {
            self.last_object = ty;
            return ty;
        }
        let any = self.plan(sid, None, ty);
        if let Some(Value::String(s)) = any.typename.as_ref().and_then(|k| map.get(k)) {
            return registry
                .types
                .get_key_value(s.as_str())
                .map(|(k, _)| k.as_str());
        }
        let possible = meta.possible_types()?;
        if possible.len() > MAX_INFER_TYPES {
            return None;
        }
        let mut found = None;
        for t in possible {
            let p = self.plan(sid, Some(t.as_str()), Some(t.as_str()));
            if map.len() <= p.keys.len() && map.keys().all(|k| p.keys.contains(k)) {
                if found.is_some() {
                    return None;
                }
                found = Some(t.as_str());
            }
        }
        found
    }

    fn plan(
        &mut self,
        sid: usize,
        runtime: Option<&'a str>,
        parent: Option<&'a str>,
    ) -> Rc<Plan<'a>> {
        if let Some((_, _, p)) = self.plans[sid]
            .iter()
            .find(|(r, t, _)| *r == runtime && *t == parent)
        {
            return p.clone();
        }
        let mut acc: IndexMap<Name, Vec<&'a Field>> = IndexMap::new();
        for i in 0..self.sets[sid].len() {
            let s = self.sets[sid][i];
            self.collect_into(s, runtime, 0, &mut acc);
        }
        let parent_meta = parent.and_then(|p| self.registry.types.get(p));
        let mut typename = None;
        let mut entries = Vec::with_capacity(acc.len());
        for (key, fields) in acc {
            if fields[0].name.node == "__typename" && typename.is_none() {
                typename = Some(key.clone());
            }
            let ty = parent_meta
                .and_then(|m| m.field_by_name(&fields[0].name.node))
                .map(|f| f.ty.as_str());
            let child_sets: Vec<&'a SelectionSet> = fields
                .iter()
                .map(|f| &f.selection_set.node)
                .filter(|s| !s.items.is_empty())
                .collect();
            let child = (!child_sets.is_empty()).then(|| {
                let named = ty.map(|t| t.trim_matches(['[', ']', '!']));
                (self.intern(child_sets), named)
            });
            entries.push(Entry { key, ty, child });
        }
        let keys = entries.iter().map(|e| e.key.clone()).collect();
        let p = Rc::new(Plan {
            entries,
            keys,
            typename,
        });
        self.plans[sid].push((runtime, parent, p.clone()));
        p
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

    fn coded(m: &str) -> async_graphql::Error {
        use async_graphql::ErrorExtensions;
        async_graphql::Error::new(m).extend_with(|_, e| e.set("code", "BAD_USER_INPUT"))
    }

    async fn nap(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    struct Cat {
        name: String,
        lives: i32,
    }
    #[Object]
    impl Cat {
        async fn name(&self) -> &str {
            &self.name
        }
        async fn lives(&self) -> i32 {
            self.lives
        }
        async fn mood(&self) -> async_graphql::Result<Option<i32>> {
            Err("grumpy".into())
        }
    }

    /// A nullable failing field `p`, a non-null failing field `q`, and a good `ok`.
    struct Bad;
    #[Object]
    impl Bad {
        async fn ok(&self) -> i32 {
            1
        }
        async fn p(&self) -> async_graphql::Result<Option<i32>> {
            nap(5).await;
            Err("p".into())
        }
        async fn q(&self) -> async_graphql::Result<i32> {
            Err("q".into())
        }
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
            Err(coded("boom"))
        }
        /// Fails without a code, as async-graphql's own variable errors do.
        async fn nocode(&self) -> async_graphql::Result<Option<i32>> {
            Err("nocode".into())
        }
        async fn bang(&self) -> async_graphql::Result<i32> {
            Err(coded("bang"))
        }
        async fn nul(&self) -> Option<Bad> {
            Some(Bad)
        }
        async fn req(&self) -> Bad {
            Bad
        }
        async fn list(&self) -> Vec<Bad> {
            vec![Bad, Bad]
        }
        async fn optlist(&self) -> Option<Vec<Option<Bad>>> {
            Some(vec![Some(Bad), Some(Bad)])
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

    /// When every root field fails in a resolver, stock (graphql-js) answers an object with
    /// each failed nullable root set to `null`; async-graphql alone answers `data: null`.
    #[tokio::test]
    async fn only_failed_fields_answer_an_object_of_nulls() {
        let s = schema(true);
        let r = s.execute("{ boom w: boom }").await;
        assert_eq!(r.errors.len(), 2);
        assert_eq!(
            r.data.into_json().unwrap(),
            serde_json::json!({"boom": null, "w": null})
        );
        // A failed non-null root nulls `data` itself (spec, graphql-js).
        let r = s.execute("{ boom bang }").await;
        assert_eq!(r.errors.len(), 2);
        assert_eq!(r.data.into_json().unwrap(), serde_json::Value::Null);
        // An error without a code (nothing ran, as for a missing variable) keeps `data: null`.
        let r = s.execute("{ boom nocode }").await;
        assert_eq!(r.errors.len(), 2);
        assert_eq!(r.data.into_json().unwrap(), serde_json::Value::Null);
    }

    async fn data(s: &Schema<Q, EmptyMutation, EmptySubscription>, q: &str) -> serde_json::Value {
        let r = s.execute(q).await;
        assert!(!r.errors.is_empty(), "{q}: expected errors");
        r.data.into_json().unwrap()
    }

    /// A failed non-null field nulls its nearest nullable parent (spec "Handling Field
    /// Errors", graphql-js `completeValue`); a nullable one is `null` in place.
    #[tokio::test]
    async fn non_null_failures_bubble_to_the_nearest_nullable_parent() {
        use serde_json::json;
        let s = schema(true);
        for _ in 0..5 {
            assert_eq!(
                data(&s, "{ nul { ok p q } fast }").await,
                json!({"nul": null, "fast": 3})
            );
            assert_eq!(
                data(&s, "{ nul { ok p } }").await,
                json!({"nul": {"ok": 1, "p": null}})
            );
            // Every member failed: the object is rebuilt, not left `null`.
            assert_eq!(data(&s, "{ nul { p } }").await, json!({"nul": {"p": null}}));
            // Through a non-null parent to the root.
            assert_eq!(
                data(&s, "{ req { q } fast }").await,
                serde_json::Value::Null
            );
            // `[Bad!]!` item -> list -> root; `[Bad]` item alone.
            assert_eq!(
                data(&s, "{ list { q } fast }").await,
                serde_json::Value::Null
            );
            assert_eq!(
                data(&s, "{ optlist { ok q } fast }").await,
                json!({"optlist": [null, null], "fast": 3})
            );
            // A failed nullable field inside list items keeps the item.
            assert_eq!(
                data(&s, "{ list { ok p } }").await,
                json!({"list": [{"ok": 1, "p": null}, {"ok": 1, "p": null}]})
            );
            // A non-null failure in one alias does not touch a sibling alias.
            assert_eq!(
                data(&s, "{ a: nul { q } b: nul { ok } }").await,
                json!({"a": null, "b": {"ok": 1}})
            );
        }
    }

    /// An abstract type without `__typename`: the runtime type is inferred when only one
    /// possible type's fields cover the object's keys (#57 M4).
    #[tokio::test]
    async fn union_without_typename_restores_failed_nullable() {
        let s = schema(true);
        let d = data(
            &s,
            "{ pets { ... on Cat { name mood } ... on Dog { barks } } }",
        )
        .await;
        assert_eq!(
            d,
            serde_json::json!({"pets": [{"name": "c", "mood": null}, {"barks": true}]})
        );
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
