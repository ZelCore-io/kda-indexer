//! The stock indexer's query limits, ported so that ours accepts and refuses exactly the
//! documents stock does (FIX-3 part C). Stock (hack-a-chain-software/indexer-kadena @
//! 90a4be1, `indexer/src/kadena-server/server.ts`) runs two checks:
//!
//! - **Depth** (HTTP only, a validation rule): `@graphile/depth-limit` 0.3.1 with
//!   `maxDepth: 9` and its other defaults. [`depth_errors`] ports its counting: only a field
//!   with a selection set adds to the depth, fragments never do, introspection is counted
//!   apart (12), list wrappers are summed (4, introspection 3), and a field coordinate may
//!   nest at most twice on one path (introspection coordinates have their own table).
//! - **Complexity** (HTTP after validation, and the websocket's `onSubscribe`):
//!   `graphql-query-complexity` 1.0.0 with the field-extensions, `@complexity` directive and
//!   `simpleEstimator({defaultComplexity: 0})` estimators, refused above 4000. A field with
//!   the directive costs `(value + children) × Π multipliers`, a multiplier being the named
//!   argument's number or list length, ignored when absent or null (no page-size default);
//!   every other field costs its children. Over an interface or union, the most expensive
//!   possible type counts. HTTP skips it for an operation named `IntrospectionQuery`; ours
//!   only when that operation selects introspection fields alone (FIX-3 part D, S1).
//!
//! The directive values are stock's, from [`crate::stock_schema`]. A field stock does not
//! have (a ZelCore extension) is priced as if it carried `@complexity(value: 1,
//! multipliers: ["first", "last"])`, the directive stock puts on its own lists, so the gate
//! bounds its pages too.

use std::collections::HashMap;

use async_graphql::parser::types::{
    Directive, DocumentOperations, ExecutableDocument, Field, FragmentDefinition,
    OperationDefinition, OperationType, Selection, SelectionSet,
};
use async_graphql::registry::{MetaType, Registry};
use async_graphql::{Name, Positioned, Value, Variables};

use crate::stock_schema::{self, Directive as CxDirective, StockField};

/// Stock's `MAX_COMPLEXITY`.
pub const MAX_COMPLEXITY: f64 = 4000.0;
/// Stock's `depthLimit({ maxDepth: 9 })`.
pub const MAX_DEPTH: i64 = 9;
/// `@graphile/depth-limit` 0.3.1 defaults stock keeps.
pub const MAX_LIST_DEPTH: i64 = 4;
pub const MAX_SELF_REFERENTIAL_DEPTH: i64 = 2;
pub const MAX_INTROSPECTION_DEPTH: i64 = 12;
pub const MAX_INTROSPECTION_LIST_DEPTH: i64 = 3;
pub const MAX_INTROSPECTION_SELF_REFERENTIAL_DEPTH: i64 = 2;

/// Stock's HTTP refusal text (a plain `Error` thrown in `didResolveOperation`).
pub const COMPLEXITY_MESSAGE: &str = "[ERROR][GRAPHQL][VALID_RANGE] Sorry, too complicated query! Exceeded the maximum allowed complexity.";
/// Stock's websocket refusal text (a `GraphQLError` returned from `onSubscribe`).
pub const WS_COMPLEXITY_MESSAGE: &str =
    "Sorry, too complicated query! Exceeded the maximum allowed complexity.";
/// The operation name stock's HTTP complexity check skips.
pub const INTROSPECTION_QUERY: &str = "IntrospectionQuery";

/// The directive a field stock does not have is priced with.
const EXTENSION_DIRECTIVE: CxDirective = CxDirective {
    value: 1,
    multipliers: &["first", "last"],
};

fn stock_fields() -> &'static HashMap<&'static str, &'static StockField> {
    static M: std::sync::OnceLock<HashMap<&'static str, &'static StockField>> =
        std::sync::OnceLock::new();
    M.get_or_init(|| stock_schema::FIELDS.iter().map(|f| (f.coord, f)).collect())
}

/// `[Foo!]!` → `Foo`.
fn named(t: &str) -> &str {
    t.trim_matches(|c| matches!(c, '[' | ']' | '!'))
}

fn list_depth(t: &str) -> i64 {
    t.matches('[').count() as i64
}

/// The `if` of a `@skip`/`@include`, when it is a boolean.
fn directive_if(d: &Directive, vars: &dyn Fn(&Name) -> Option<Value>) -> Option<bool> {
    let (_, v) = d.arguments.iter().find(|(k, _)| k.node.as_str() == "if")?;
    match v.node.clone().into_const_with(|n| vars(&n).ok_or(())) {
        Ok(Value::Boolean(b)) => Some(b),
        _ => None,
    }
}

/// Whether `graphql-query-complexity` counts a selection with these directives.
fn included(ds: &[Positioned<Directive>], vars: &dyn Fn(&Name) -> Option<Value>) -> bool {
    let (mut include, mut skip) = (true, false);
    for d in ds {
        match d.node.name.node.as_str() {
            "include" => {
                if let Some(b) = directive_if(&d.node, vars) {
                    include = b;
                }
            }
            "skip" => {
                if let Some(b) = directive_if(&d.node, vars) {
                    skip = b;
                }
            }
            _ => {}
        }
    }
    include && !skip
}

struct Cx<'a> {
    reg: &'a Registry,
    doc: &'a ExecutableDocument,
    vars: &'a Variables,
    /// Default values of the current operation's variables.
    defaults: HashMap<Name, Value>,
    /// Fragments being expanded (validation refuses cycles; this only keeps the walk finite).
    stack: Vec<&'a str>,
}

impl<'a> Cx<'a> {
    /// A coerced variable: the request's value, else the operation's default.
    fn var(&self, n: &Name) -> Option<Value> {
        self.vars
            .get(n)
            .cloned()
            .or_else(|| self.defaults.get(n).cloned())
    }

    /// An argument as `getArgumentValues` gives it: the literal or variable value, else the
    /// schema default, else absent.
    fn arg(&self, f: &Field, name: &str, sf: Option<&StockField>) -> Option<Value> {
        let default = || {
            sf.and_then(|s| s.int_defaults.iter().find(|d| d.0 == name))
                .map(|d| Value::from(d.1))
        };
        let Some((_, v)) = f.arguments.iter().find(|(k, _)| k.node.as_str() == name) else {
            return default();
        };
        // A variable the request leaves out (and the operation gives no default) takes the
        // argument's schema default; one inside a list is null.
        let mut missing = false;
        let c = v
            .node
            .clone()
            .into_const_with(|n| {
                Ok::<_, ()>(self.var(&n).unwrap_or_else(|| {
                    missing = true;
                    Value::Null
                }))
            })
            .ok()?;
        if missing && c == Value::Null {
            return default();
        }
        Some(c)
    }

    /// The directive estimator; `None` when it gives no number (then `simpleEstimator`).
    fn directive(&self, d: &CxDirective, f: &Field, sf: Option<&StockField>, child: f64) -> f64 {
        // FIX-3 part D (S2): a negative multiplier counts as 0 and the field's cost is never
        // below 0, so a negative `first` cannot cancel an expensive sibling (stock's library
        // multiplies the negative in). A number that is not finite falls back to the
        // children, as the library's NaN check does.
        //
        // Audit F-04 (option A): a zero multiplier is ignored when a sibling multiplier of
        // the same directive is non-zero, so `first: 1000, last: 0` is priced as the 1000
        // rows it serves (stock gives 0). Zero alone, or all zero, still gives 0, as stock.
        let factors: Vec<f64> = d
            .multipliers
            .iter()
            .filter_map(|m| match self.arg(f, m, sf) {
                Some(Value::Number(n)) => Some(n.as_f64().unwrap_or(f64::NAN).max(0.0)),
                Some(Value::List(l)) => Some(l.len() as f64),
                _ => None,
            })
            .collect();
        let any_non_zero = factors.iter().any(|x| *x != 0.0);
        let mult: f64 = factors
            .iter()
            .filter(|x| !any_non_zero || **x != 0.0)
            .product();
        let c = (d.value as f64 + child) * mult;
        if c.is_nan() {
            child
        } else {
            c.max(0.0)
        }
    }

    fn possible(&self, t: &'a MetaType) -> Vec<&'a str> {
        match t.possible_types() {
            Some(p) if t.is_abstract() => p.iter().map(String::as_str).collect(),
            _ => vec![t.name()],
        }
    }

    fn composite(&self, name: &str) -> Option<&'a MetaType> {
        self.reg.types.get(name).filter(|t| t.is_composite())
    }

    /// `QueryComplexity.nodeComplexity`.
    fn node(&mut self, set: &'a SelectionSet, ty: &'a MetaType) -> f64 {
        let possible = self.possible(ty);
        let mut by_type: HashMap<&'a str, f64> = HashMap::new();
        let add = |m: &mut HashMap<&'a str, f64>, c: f64, ts: &[&'a str]| {
            for t in ts {
                *m.entry(t).or_insert(0.0) += c;
            }
        };
        for sel in &set.items {
            let vars = |n: &Name| self.var(n);
            let ds = match &sel.node {
                Selection::Field(f) => &f.node.directives,
                Selection::FragmentSpread(s) => &s.node.directives,
                Selection::InlineFragment(i) => &i.node.directives,
            };
            if !included(ds, &vars) {
                continue;
            }
            match &sel.node {
                Selection::Field(f) => {
                    let f = &f.node;
                    let name = f.name.node.as_str();
                    if name.starts_with("__") {
                        // `__typename`, `__schema`, `__type`: no directive, and every
                        // introspection field below them costs 0.
                        if matches!(name, "__typename" | "__schema" | "__type") {
                            add(&mut by_type, 0.0, &possible);
                        }
                        continue;
                    }
                    let Some(mf) = ty.field_by_name(name) else {
                        continue;
                    };
                    let child = match self.composite(named(&mf.ty)) {
                        Some(t) => self.node(&f.selection_set.node, t),
                        None => 0.0,
                    };
                    let coord = format!("{}.{name}", ty.name());
                    let sf = stock_fields().get(coord.as_str()).copied();
                    let c = match sf {
                        Some(s) => match &s.complexity {
                            Some(d) => self.directive(d, f, sf, child),
                            None => child,
                        },
                        None => self.directive(&EXTENSION_DIRECTIVE, f, None, child),
                    };
                    add(&mut by_type, c, &possible);
                }
                Selection::FragmentSpread(s) => {
                    let name = s.node.fragment_name.node.as_str();
                    let Some(def) = self.doc.fragments.get(name) else {
                        continue;
                    };
                    let Some(ft) = self.composite(def.node.type_condition.node.on.node.as_str())
                    else {
                        continue;
                    };
                    if self.stack.contains(&name) {
                        continue;
                    }
                    self.stack.push(name);
                    let c = self.node(&def.node.selection_set.node, ft);
                    self.stack.pop();
                    let ts = self.possible(ft);
                    add(&mut by_type, c, &ts);
                }
                Selection::InlineFragment(i) => {
                    let ft = match &i.node.type_condition {
                        Some(tc) => match self.composite(tc.node.on.node.as_str()) {
                            Some(t) => t,
                            None => continue,
                        },
                        None => ty,
                    };
                    let c = self.node(&i.node.selection_set.node, ft);
                    let ts = self.possible(ft);
                    add(&mut by_type, c, &ts);
                }
            }
        }
        by_type.values().copied().fold(0.0, f64::max)
    }
}

fn root(op: OperationType) -> &'static str {
    match op {
        OperationType::Query => "Query",
        OperationType::Mutation => "Mutation",
        OperationType::Subscription => "Subscription",
    }
}

/// The operation the executor runs (async-graphql's choice): the one named
/// `operation_name`, or the only one when no name is given. `None` when the request names
/// no operation of the document, or names none in a document of several: the executor
/// refuses those itself, so no price is needed.
pub fn selected_operation<'a>(
    doc: &'a ExecutableDocument,
    operation_name: Option<&str>,
) -> Option<&'a Positioned<OperationDefinition>> {
    match (&doc.operations, operation_name) {
        (DocumentOperations::Single(op), None) => Some(op),
        (DocumentOperations::Single(_), Some(_)) => None,
        (DocumentOperations::Multiple(m), Some(n)) => m.get(n),
        (DocumentOperations::Multiple(m), None) if m.len() == 1 => m.values().next(),
        (DocumentOperations::Multiple(_), None) => None,
    }
}

/// Whether `op` selects introspection fields alone (`__schema`, `__type`, `__typename`),
/// through fragments too (FIX-3 part D, S1). Only such an operation may skip the complexity
/// check; stock skips any operation *named* `IntrospectionQuery`.
pub fn pure_introspection(doc: &ExecutableDocument, op: &Positioned<OperationDefinition>) -> bool {
    fn set(doc: &ExecutableDocument, s: &SelectionSet, seen: &mut Vec<String>) -> bool {
        s.items.iter().all(|sel| match &sel.node {
            Selection::Field(f) => f.node.name.node.starts_with("__"),
            Selection::InlineFragment(i) => set(doc, &i.node.selection_set.node, seen),
            Selection::FragmentSpread(sp) => {
                let name = sp.node.fragment_name.node.as_str();
                if seen.iter().any(|n| n == name) {
                    return true;
                }
                let Some(def) = doc.fragments.get(name) else {
                    return false;
                };
                seen.push(name.to_owned());
                set(doc, &def.node.selection_set.node, seen)
            }
        })
    }
    set(doc, &op.node.selection_set.node, &mut Vec::new())
}

/// Stock's `getComplexity` of the operation the executor runs ([`selected_operation`]); 0
/// when none is selected (the executor refuses the request).
pub fn complexity(
    reg: &Registry,
    doc: &ExecutableDocument,
    operation_name: Option<&str>,
    vars: &Variables,
) -> f64 {
    let Some(op) = selected_operation(doc, operation_name) else {
        return 0.0;
    };
    let mut cx = Cx {
        reg,
        doc,
        vars,
        defaults: HashMap::new(),
        stack: Vec::new(),
    };
    cx.defaults = op
        .node
        .variable_definitions
        .iter()
        .filter_map(|d| {
            d.node
                .default_value
                .as_ref()
                .map(|v| (d.node.name.node.clone(), v.node.clone()))
        })
        .collect();
    match reg.types.get(root(op.node.ty)) {
        Some(t) => cx.node(&op.node.selection_set.node, t),
        None => 0.0,
    }
}

/// Whether stock refuses `doc` on complexity.
pub fn over_complexity(
    reg: &Registry,
    doc: &ExecutableDocument,
    operation_name: Option<&str>,
    vars: &Variables,
) -> bool {
    complexity(reg, doc, operation_name, vars) > MAX_COMPLEXITY
}

// ---- depth (`@graphile/depth-limit` 0.3.1) ----

/// The non-coordinate counters (no `.` in them, as in graphile).
const DEPTH: &str = "$depth";
const INTROSPECTION_DEPTH: &str = "$introspectionDepth";
const LIST_DEPTH: &str = "$listDepth";
const INTROSPECTION_LIST_DEPTH: &str = "$introspectionListDepth";

/// graphile's built-in coordinate limits (stock passes no `maxDepthByFieldCoordinates`).
const COORDINATE_LIMITS: &[(&str, i64)] = &[
    ("Query.__schema", 1),
    ("Query.__type", 1),
    ("__Type.fields", 1),
    ("__Type.inputFields", 1),
    ("__Type.interfaces", 1),
    ("__Type.ofType", 8),
    ("__Type.possibleTypes", 1),
    ("__Field.args", 1),
    ("__Field.type", 1),
];

/// The composite fields of graphql-js 16's introspection types: (type, field, named type,
/// list depth).
const INTROSPECTION_FIELDS: &[(&str, &str, &str, i64)] = &[
    ("__Schema", "types", "__Type", 1),
    ("__Schema", "queryType", "__Type", 0),
    ("__Schema", "mutationType", "__Type", 0),
    ("__Schema", "subscriptionType", "__Type", 0),
    ("__Schema", "directives", "__Directive", 1),
    ("__Type", "fields", "__Field", 1),
    ("__Type", "interfaces", "__Type", 1),
    ("__Type", "possibleTypes", "__Type", 1),
    ("__Type", "enumValues", "__EnumValue", 1),
    ("__Type", "inputFields", "__InputValue", 1),
    ("__Type", "ofType", "__Type", 0),
    ("__Field", "args", "__InputValue", 1),
    ("__Field", "type", "__Type", 0),
    ("__InputValue", "type", "__Type", 0),
    ("__Directive", "args", "__InputValue", 1),
];

const INTROSPECTION_TYPES: &[&str] = &[
    "__Schema",
    "__Type",
    "__Field",
    "__InputValue",
    "__EnumValue",
    "__Directive",
];

type State = HashMap<String, i64>;

enum Node<'a> {
    Operation(&'a SelectionSet),
    Field(&'a Field),
    Inline(Option<&'a str>, &'a SelectionSet),
    Fragment(&'a FragmentDefinition),
    Spread(&'a str),
}

struct Depth<'a> {
    reg: &'a Registry,
    doc: &'a ExecutableDocument,
    /// `depthByFragment`: shared by every operation of the document.
    by_fragment: HashMap<&'a str, State>,
}

fn incr(st: &mut State, k: &str, by: i64) {
    *st.entry(k.to_owned()).or_insert(0) += by;
}

impl<'a> Depth<'a> {
    /// `schema.getType(name)` exists.
    fn exists(&self, name: &str) -> bool {
        INTROSPECTION_TYPES.contains(&name) || self.reg.types.contains_key(name)
    }

    fn is_object(&self, name: &str) -> bool {
        INTROSPECTION_TYPES.contains(&name)
            || matches!(self.reg.types.get(name), Some(MetaType::Object { .. }))
    }

    /// The named type and list depth of `ty.field` (meta fields included).
    fn field(&self, ty: &str, field: &str) -> Option<(String, i64)> {
        match field {
            "__schema" => return Some(("__Schema".into(), 0)),
            "__type" => return Some(("__Type".into(), 0)),
            "__typename" => return Some(("String".into(), 0)),
            _ => {}
        }
        if INTROSPECTION_TYPES.contains(&ty) {
            return INTROSPECTION_FIELDS
                .iter()
                .find(|x| x.0 == ty && x.1 == field)
                .map(|x| (x.2.to_owned(), x.3));
        }
        let f = self.reg.types.get(ty)?.field_by_name(field)?;
        Some((named(&f.ty).to_owned(), list_depth(&f.ty)))
    }

    /// `countDepthInternal`.
    fn count(&mut self, cur: &str, node: Node<'a>, visited: &[&'a str], intro: bool) -> State {
        let (set, coord, field) = match node {
            Node::Spread(name) => {
                if let Some(e) = self.by_fragment.get(name) {
                    return e.clone();
                }
                if visited.contains(&name) {
                    return State::new();
                }
                let Some(def) = self.doc.fragments.get(name) else {
                    return State::new();
                };
                let mut v = visited.to_vec();
                v.push(name);
                let d = self.count(cur, Node::Fragment(&def.node), &v, intro);
                self.by_fragment.insert(name, d.clone());
                return d;
            }
            Node::Operation(s) => (s, None, None),
            Node::Field(f) => (
                &f.selection_set.node,
                Some(format!("{cur}.{}", f.name.node)),
                Some(f),
            ),
            Node::Inline(_, s) => (s, None, None),
            Node::Fragment(d) => (&d.selection_set.node, None, None),
        };
        let mut st = State::new();
        if let Some(c) = &coord {
            incr(&mut st, c, 1);
        }
        // A leaf field has no selection set.
        if set.items.is_empty() {
            return st;
        }
        if field.is_some() {
            incr(&mut st, if intro { INTROSPECTION_DEPTH } else { DEPTH }, 1);
        }
        let ty: Option<String> = match &node {
            Node::Operation(_) => Some(cur.to_owned()),
            Node::Field(f) => {
                if self.is_object(cur) {
                    self.field(cur, f.name.node.as_str()).map(|(t, l)| {
                        let k = if intro {
                            INTROSPECTION_LIST_DEPTH
                        } else {
                            LIST_DEPTH
                        };
                        incr(&mut st, k, l);
                        t
                    })
                } else {
                    None
                }
            }
            Node::Inline(Some(on), _) => self.exists(on).then(|| (*on).to_owned()),
            Node::Inline(None, _) => Some(cur.to_owned()),
            Node::Fragment(d) => {
                let on = d.type_condition.node.on.node.as_str();
                self.exists(on).then(|| on.to_owned())
            }
            Node::Spread(_) => None,
        };
        let Some(ty) = ty else {
            return st;
        };
        let base = |k: &str, st: &State| st.get(k).copied().unwrap_or(0);
        let (b_depth, b_idepth) = (base(DEPTH, &st), base(INTROSPECTION_DEPTH, &st));
        let (b_list, b_ilist) = (base(LIST_DEPTH, &st), base(INTROSPECTION_LIST_DEPTH, &st));
        let b_field = coord.as_deref().map_or(0, |c| base(c, &st));
        for child in &set.items {
            let (n, child_field) = match &child.node {
                Selection::Field(f) => (Node::Field(&f.node), Some(f.node.name.node.as_str())),
                Selection::InlineFragment(i) => (
                    Node::Inline(
                        i.node
                            .type_condition
                            .as_ref()
                            .map(|c| c.node.on.node.as_str()),
                        &i.node.selection_set.node,
                    ),
                    None,
                ),
                Selection::FragmentSpread(s) => {
                    (Node::Spread(s.node.fragment_name.node.as_str()), None)
                }
            };
            let is_intro = child_field.is_some_and(|n| n.starts_with("__"));
            let inner = self.count(&ty, n, visited, intro || is_intro);
            if let Some(fc) = child_field.map(|n| format!("{ty}.{n}")) {
                if st.get(&fc).copied().unwrap_or(0) == 0 {
                    st.insert(fc, 1);
                }
            }
            for (k, v) in inner {
                let add = if Some(&k) == coord.as_ref() {
                    b_field
                } else {
                    match k.as_str() {
                        DEPTH => b_depth,
                        INTROSPECTION_DEPTH => b_idepth,
                        LIST_DEPTH => b_list,
                        INTROSPECTION_LIST_DEPTH => b_ilist,
                        _ => 0,
                    }
                };
                let score = v + add;
                if st.get(&k).is_none_or(|old| *old < score) {
                    st.insert(k, score);
                }
            }
        }
        st
    }
}

fn limit_of(k: &str) -> Option<i64> {
    match k {
        DEPTH => return Some(MAX_DEPTH),
        INTROSPECTION_DEPTH => return Some(MAX_INTROSPECTION_DEPTH),
        LIST_DEPTH => return Some(MAX_LIST_DEPTH),
        INTROSPECTION_LIST_DEPTH => return Some(MAX_INTROSPECTION_LIST_DEPTH),
        _ => {}
    }
    if let Some((_, m)) = COORDINATE_LIMITS.iter().find(|x| x.0 == k) {
        return Some(*m);
    }
    k.contains('.').then(|| {
        if k.starts_with("__") {
            MAX_INTROSPECTION_SELF_REFERENTIAL_DEPTH
        } else {
            MAX_SELF_REFERENTIAL_DEPTH
        }
    })
}

/// Stock's depth-limit errors for `doc`: one message per operation over a limit
/// (`revealDetails: false`, so the message names the operation only).
pub fn depth_errors(reg: &Registry, doc: &ExecutableDocument) -> Vec<String> {
    let mut d = Depth {
        reg,
        doc,
        by_fragment: HashMap::new(),
    };
    let ops: Vec<(Option<&Name>, _)> = match &doc.operations {
        DocumentOperations::Single(op) => vec![(None, op)],
        DocumentOperations::Multiple(m) => m.iter().map(|(n, op)| (Some(n), op)).collect(),
    };
    let mut out = Vec::new();
    for (name, op) in ops {
        let r = root(op.node.ty);
        if !reg.types.contains_key(r) {
            continue;
        }
        let depths = d.count(r, Node::Operation(&op.node.selection_set.node), &[], false);
        if depths
            .iter()
            .any(|(k, v)| limit_of(k).is_some_and(|m| *v > m))
        {
            let n = name.map_or("(anonymous)", Name::as_str);
            out.push(format!("'{n}' exceeds operation depth limits."));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::type_registry;

    fn doc(q: &str) -> ExecutableDocument {
        async_graphql::parser::parse_query(q).unwrap()
    }

    fn cx(q: &str, vars: serde_json::Value) -> f64 {
        let vars = crate::http::variables(vars).unwrap();
        complexity(type_registry(), &doc(q), None, &vars)
    }

    fn depth(q: &str) -> Vec<String> {
        depth_errors(type_registry(), &doc(q))
    }

    /// Every stock coordinate we serve has stock's return type and list depth, and every
    /// interface or union we share has stock's possible types: the table is read against
    /// the types it was made for.
    #[test]
    fn our_schema_matches_the_stock_table() {
        let reg = type_registry();
        let mut wrong = Vec::new();
        for f in stock_schema::FIELDS {
            let (t, field) = f.coord.split_once('.').unwrap();
            let Some(mf) = reg.types.get(t).and_then(|t| t.field_by_name(field)) else {
                continue;
            };
            // A scalar's name (`gasPrice`: our `Float`, stock's `String`) changes no limit.
            let composite = reg
                .types
                .get(named(&mf.ty))
                .is_some_and(|t| t.is_composite());
            if (composite && named(&mf.ty) != f.named)
                || list_depth(&mf.ty) != i64::from(f.list_depth)
            {
                wrong.push(format!("{} {} vs stock {}", f.coord, mf.ty, f.named));
            }
        }
        for (t, ps) in stock_schema::ABSTRACT {
            if let Some(p) = reg.types.get(*t).and_then(|t| t.possible_types()) {
                let ours: Vec<&str> = p.iter().map(String::as_str).collect();
                let missing: Vec<_> = ps.iter().filter(|x| !ours.contains(x)).collect();
                let extra: Vec<_> = ours.iter().filter(|x| !ps.contains(x)).collect();
                // Stock's DEX types (`Pool`) are not served.
                if !extra.is_empty() || missing.iter().any(|m| **m != "Pool") {
                    wrong.push(format!("{t}: missing {missing:?}, extra {extra:?}"));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Every field of a stock type we serve is served: a stock client that selects one gets
    /// an answer, not a validation error for the whole query (HTTP 400). Stock types we do
    /// not serve at all are out of scope here; the fields below are known gaps.
    #[test]
    fn every_field_of_a_served_stock_type_is_served() {
        const KNOWN_GAPS: &[&str] = &[
            // Not served (docs/comparison.md, "Stock fields not served").
            "Block.totalGasUsedInKda",
            "NetworkInfo.genesisHeights",
            "NetworkInfo.nodeBlockDelay",
            "NetworkInfo.nodeChains",
            "NetworkInfo.nodeLatestBehaviorHeight",
            "NetworkInfo.nodePackageVersion",
            "NetworkInfo.nodeServiceDate",
            "NetworkInfo.numberOfChains",
            "NonFungibleAccount.transfers",
            "NonFungibleChainAccount.transfers",
        ];
        let reg = type_registry();
        let missing: Vec<&str> = stock_schema::FIELDS
            .iter()
            .map(|f| f.coord)
            .filter(|c| {
                let (t, field) = c.split_once('.').unwrap();
                reg.types
                    .get(t)
                    .is_some_and(|t| t.field_by_name(field).is_none())
            })
            .filter(|c| !KNOWN_GAPS.contains(c))
            .collect();
        assert!(missing.is_empty(), "{missing:#?}");
    }

    /// The explorer's transactions-by-account document at `first: 100` costs (1 + 2) x 100.
    #[test]
    fn the_explorer_page_of_100_is_accepted() {
        let q = include_str!("../../../compare/graphql/QTransactionsByAccount.graphql");
        let c = cx(q, serde_json::json!({"accountName": "k:a", "first": 100}));
        assert_eq!(c, 300.0);
        assert!(c <= MAX_COMPLEXITY);
    }

    #[test]
    fn the_formula_is_stock_s() {
        let page =
            |n: u32| format!("{{ transactions(first: {n}) {{ edges {{ node {{ hash }} }} }} }}");
        // 4000 passes, 4001 is refused.
        assert_eq!(cx(&page(4000), serde_json::json!({})), 4000.0);
        assert_eq!(cx(&page(4001), serde_json::json!({})), 4001.0);
        // Absent first/last multiply by nothing (stock's estimator has no page default).
        assert_eq!(
            cx(
                "{ transactions { edges { node { hash } } } }",
                serde_json::json!({})
            ),
            1.0
        );
        // Children add before the multiplier; `last` multiplies too.
        assert_eq!(
            cx(
                "{ transactions(last: 10) { edges { node { cmd { meta { chainId } } } } } }",
                serde_json::json!({})
            ),
            30.0
        );
        // Nested pages multiply.
        assert_eq!(
            cx(
                "{ blocksFromHeight(startHeight: 0, first: 10) { edges { node { transactions(first: 5) { edges { node { hash } } } } } } }",
                serde_json::json!({})
            ),
            (1.0 + 5.0) * 10.0
        );
        // Variables, and the operation's defaults.
        let q = "query Q($n: Int = 7) { transactions(first: $n) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({})), 7.0);
        assert_eq!(cx(q, serde_json::json!({"n": 50})), 50.0);
        assert_eq!(cx(q, serde_json::json!({"n": null})), 1.0);
        // `length` multipliers name no argument: x1. `clist` costs 5.
        assert_eq!(
            cx(
                "{ transaction(requestKey: \"x\") { cmd { signers { clist { name } } } } }",
                serde_json::json!({})
            ),
            1.0 + 1.0 + 1.0 + 5.0
        );
        // The subscription `quantity` default (20) multiplies.
        assert_eq!(
            cx(
                "subscription { events(qualifiedEventName: \"coin.TRANSFER\") { name } }",
                serde_json::json!({})
            ),
            20.0
        );
        // Leaves and `__typename` are free; aliases add up.
        assert_eq!(
            cx(
                "{ a: lastBlockHeight b: lastBlockHeight __typename }",
                serde_json::json!({})
            ),
            2.0
        );
    }

    /// FIX-3 part D (S2): a negative multiplier counts as 0 and no field costs less than 0,
    /// so a negative `first` next to an expensive field cannot lower the total.
    #[test]
    fn a_negative_multiplier_cannot_lower_the_cost() {
        let q = "{ a: transactions(first: -5) { edges { node { hash } } } b: transactions(first: 4001) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({})), 4001.0);
        // A negative page with an expensive child: 0, never negative.
        let q = "{ blocksFromHeight(startHeight: 0, first: -1) { edges { node { transactions(first: 4001) { edges { node { hash } } } } } } x: transactions(first: 4001) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({})), 4001.0);
        // Through a variable, and `last` too.
        let q = "query Q($n: Int) { a: transactions(last: $n) { edges { node { hash } } } b: transactions(first: 4001) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({"n": -1000000})), 4001.0);
        // A fractional or huge number is still a number: priced as stock prices it, >= 0.
        assert!(cx(q, serde_json::json!({"n": 1e300})) > MAX_COMPLEXITY);
    }

    /// Audit F-04 (option A): a zero multiplier next to a non-zero sibling of the same
    /// directive is ignored, so `first: N, last: 0` is priced as the N rows it serves.
    /// `first: 0` alone stays 0, as stock prices it.
    #[test]
    fn a_zero_multiplier_beside_a_non_zero_one_is_ignored() {
        let nested = |a: &str| {
            format!("{{ blocksFromHeight(startHeight: 0, {a}) {{ edges {{ node {{ transactions(first: 50) {{ edges {{ node {{ hash }} }} }} }} }} }} }}")
        };
        assert!(cx(&nested("first: 1000, last: 0"), serde_json::json!({})) > MAX_COMPLEXITY);
        assert!(cx(&nested("first: 0, last: 1000"), serde_json::json!({})) > MAX_COMPLEXITY);
        assert_eq!(
            cx(&nested("first: 1000, last: 0"), serde_json::json!({})),
            51.0 * 1000.0
        );
        // Through a variable too.
        let q =
            "query Q($z: Int) { transactions(first: 4001, last: $z) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({"z": 0})), 4001.0);
        // `first: 0` alone is unchanged (stock: 0), and so are both zero.
        assert_eq!(cx(&nested("first: 0"), serde_json::json!({})), 0.0);
        assert_eq!(cx(&nested("first: 0, last: 0"), serde_json::json!({})), 0.0);
        // Two non-zero multipliers still multiply.
        assert_eq!(
            cx(
                "{ transactions(first: 10, last: 3) { edges { node { hash } } } }",
                serde_json::json!({})
            ),
            30.0
        );
    }

    /// FIX-3 part D (S1): the gate prices the operation the executor runs, and is skipped
    /// only for an operation of introspection fields alone.
    #[test]
    fn the_priced_operation_is_the_one_that_runs() {
        let d = doc("query A { transactions(first: 4001) { edges { node { hash } } } } query B { lastBlockHeight }");
        let v = Variables::default();
        assert!(over_complexity(type_registry(), &d, Some("A"), &v));
        assert!(!over_complexity(type_registry(), &d, Some("B"), &v));
        assert!(selected_operation(&d, None).is_none());
        assert!(selected_operation(&d, Some("C")).is_none());
        let one = doc("query A { transactions(first: 4001) { edges { node { hash } } } }");
        assert!(over_complexity(type_registry(), &one, None, &v));
        // A spoofed name carrying real fields is not introspection.
        let spoof = doc("query IntrospectionQuery { __schema { queryType { name } } transactions(first: 4001) { edges { node { hash } } } }");
        let op = selected_operation(&spoof, Some(INTROSPECTION_QUERY)).unwrap();
        assert!(!pure_introspection(&spoof, op));
        let spoof = doc("query IntrospectionQuery { ...F } fragment F on Query { transactions(first: 4001) { edges { node { hash } } } }");
        let op = selected_operation(&spoof, Some(INTROSPECTION_QUERY)).unwrap();
        assert!(!pure_introspection(&spoof, op));
        let real = doc("query IntrospectionQuery { __schema { queryType { name } } ... on Query { __type(name: \"Block\") { name } } __typename }");
        let op = selected_operation(&real, Some(INTROSPECTION_QUERY)).unwrap();
        assert!(pure_introspection(&real, op));
    }

    #[test]
    fn abstract_types_take_their_most_expensive_member_and_skip_counts() {
        // `node` (1) + max over Node's types: Block.parent (1 + 1 for its own parent).
        let q = "{ node(id: \"x\") { ... on Block { parent { parent { hash } } } ... on Transaction { cmd { meta { chainId } } } } }";
        assert_eq!(cx(q, serde_json::json!({})), 1.0 + 2.0);
        // `@skip(if: true)` and `@include(if: $v)` with v false drop the selection.
        let q = "query Q($v: Boolean!) { transactions(first: 9) @skip(if: true) { edges { node { hash } } } x: transactions(first: 3) @include(if: $v) { edges { node { hash } } } }";
        assert_eq!(cx(q, serde_json::json!({"v": false})), 0.0);
        assert_eq!(cx(q, serde_json::json!({"v": true})), 3.0);
        // Fragments count where they are spread.
        let q = "query { transactions(first: 10) { ...C } } fragment C on QueryTransactionsConnection { edges { node { cmd { meta { chainId } } } } }";
        assert_eq!(cx(q, serde_json::json!({})), 30.0);
    }

    /// Introspection costs 0, and its depth is counted apart (12). The full introspection
    /// document (graphql-js 16's `getIntrospectionQuery()` too) reaches 13 through
    /// `fields { args { type { ofType x8 } } }`, so stock refuses it over HTTP, as the real
    /// `@graphile/depth-limit` 0.3.1 does on stock's schema (checked with
    /// `scripts/stock-schema/oracle.js`).
    #[test]
    fn introspection_costs_nothing_and_its_depth_is_stock_s() {
        let q = include_str!("../tests/introspection.graphql");
        assert_eq!(cx(q, serde_json::json!({})), 0.0);
        assert_eq!(
            depth(q),
            vec!["'IntrospectionQuery' exceeds operation depth limits.".to_string()]
        );
        // Without the deepest path (args under fields) it passes.
        let shallow = q.replace("    args { ...InputValue }\n", "");
        assert_ne!(shallow, q);
        assert!(depth(&shallow).is_empty(), "{:?}", depth(&shallow));
        // `ofType` nests at most 8 times, `fields` once.
        assert_eq!(
            depth("{ __type(name: \"Block\") { fields { type { fields { name } } } } }").len(),
            1
        );
    }

    /// Depth 9 accepted, 10 refused, as graphile counts: only fields with a selection set.
    #[test]
    fn depth_boundary_is_stock_s() {
        // 1 transaction, 2 result (inline fragments add nothing), 3 block, 4 parent,
        // 5 minerAccount, 6 transactions, 7 edges, 8 node, 9 cmd, 10 meta.
        let nine = "query D { transaction(requestKey: \"x\") { result { ... on TransactionResult { block { parent { minerAccount { transactions { edges { node { cmd { nonce } } } } } } } } } } }";
        assert!(depth(nine).is_empty(), "{:?}", depth(nine));
        let ten = nine.replace("cmd { nonce }", "cmd { meta { chainId } }");
        assert_eq!(
            depth(&ten),
            vec!["'D' exceeds operation depth limits.".to_string()]
        );
        let anon = ten.replacen("query D ", "", 1);
        assert_eq!(
            depth(&anon),
            vec!["'(anonymous)' exceeds operation depth limits.".to_string()]
        );
        // Fragments neither add depth nor hide it.
        let q = "query F { transaction(requestKey: \"x\") { ...R } } fragment R on Transaction { result { ... on TransactionResult { block { parent { minerAccount { transactions { edges { node { cmd { meta { chainId } } } } } } } } } } }";
        assert_eq!(depth(q).len(), 1);
    }

    #[test]
    fn a_coordinate_nests_at_most_twice_and_lists_at_most_four_deep() {
        let two = "{ block(hash: \"x\") { parent { parent { hash } } } }";
        assert!(depth(two).is_empty());
        let three = "{ block(hash: \"x\") { parent { parent { parent { hash } } } } }";
        assert_eq!(depth(three).len(), 1);
        // Not consecutive still counts: Block.parent three times on one path.
        let apart = "{ block(hash: \"x\") { parent { minerAccount { guard { raw } } parent { transactions { edges { node { hash } } } parent { hash } } } } }";
        assert_eq!(depth(apart).len(), 1);
        // Each operation of a document is checked.
        let two_ops = format!("query A {two} query B {three}");
        assert_eq!(
            depth(&two_ops),
            vec!["'B' exceeds operation depth limits.".to_string()]
        );
    }
}

/// `scripts/stock-schema/cases.json` (FIX-3 part E, M7): the synthetic cross-check documents
/// `scripts/stock-schema/oracle.js` runs through stock's own libraries. Each case records the
/// verdict of this port (`depth`: refused by the depth rule; `cx`: stock's complexity), so
/// the oracle's output can be compared line by line, and this test keeps the file true to
/// the port.
#[cfg(test)]
mod cases {
    use super::*;
    use crate::limits::type_registry;

    #[test]
    fn the_cross_check_cases_hold() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../scripts/stock-schema/cases.json"
        );
        let cases: Vec<serde_json::Value> =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(cases.len(), 21);
        let mut got = Vec::new();
        for c in &cases {
            let q = c["q"].as_str().unwrap();
            let doc = async_graphql::parser::parse_query(q).unwrap();
            let vars = crate::http::variables(c.get("vars").cloned().unwrap_or_default())
                .unwrap_or_default();
            let op = c.get("op").and_then(serde_json::Value::as_str);
            let reg = type_registry();
            let cx = complexity(reg, &doc, op, &vars);
            let depth = !depth_errors(reg, &doc).is_empty();
            got.push(serde_json::json!({"name": c["name"], "depth": depth, "cx": cx}));
            let want = c.get("expect");
            if std::env::var_os("KDX_PRINT_CASES").is_none() {
                assert_eq!(
                    want,
                    Some(&serde_json::json!({"depth": depth, "cx": cx})),
                    "{}",
                    c["name"]
                );
            }
        }
        if std::env::var_os("KDX_PRINT_CASES").is_some() {
            println!("{}", serde_json::Value::Array(got));
        }
    }
}
