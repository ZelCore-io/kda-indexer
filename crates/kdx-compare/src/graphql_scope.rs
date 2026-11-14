//! R-P5P9-60 H2: lists bounded by one block or one transaction compare as sets.
//!
//! The lists in scope are a Block's `transactions` and `events` (under `block`,
//! `blocksFromHeight` or `blocksFromDepth`, or a transaction's `result.block`), a
//! TransactionResult's `transfers` and `events`, and a root connection scoped to one block or
//! one transaction by a non-null `blockHash` or `requestKey` variable. Their order is a
//! contract choice (M2c, R3E-8), so their edges compare as sets. When either side's page of
//! such a list is cut, the harness pages both sides to the end of that list, each at its own
//! target's rate and for at most [`MAX_PAGES`] pages, and compares the full sets. A list it
//! cannot finish (no parent identity in the answer, a selection it cannot re-issue, an error,
//! more than [`MAX_PAGES`] pages, or a first page that is not a part of that side's own full
//! list) is left as it was, so its difference stays a diff.

use serde_json::{json, Map, Value};

/// Visible hit of a finished cut list (allowlist id `graphql-r60-scoped-list`).
pub const SCOPED_FINISHED_PATH: &str = "/~graphql-r60-scoped-list-finished";
/// Pages fetched per list and side at most (one block's rows).
pub const MAX_PAGES: usize = 20;
/// Page size of the harness's own list reads.
pub const PAGE_SIZE: u32 = 50;
/// Cut lists finished per compare at most; the rest stay as they are.
pub const MAX_LISTS: usize = 4;

const BLOCK_PARENTS: &[&str] = &["block", "blocksFromHeight", "blocksFromDepth"];
const BLOCK_LISTS: &[&str] = &["transactions", "events"];
const TX_LISTS: &[&str] = &["transfers", "events"];
const ROOT_LISTS: &[&str] = &["transactions", "transfers", "events"];
const PAGE_ARGS: &[&str] = &["first", "last", "after", "before"];

/// The object a nested list belongs to, by the identity the harness re-reads it with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parent {
    /// A Block, by its hash.
    Block(String),
    /// A transaction's result, by the transaction's request key.
    Tx(String),
}

/// One nested list in scope, in one answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedList {
    /// JSON pointer of the connection object (from the body root, `/data/...`).
    pub pointer: String,
    /// The response keys from `data` to the connection, list indexes left out (the document
    /// path, `edges` and `node` included).
    pub doc_path: Vec<String>,
    /// `None` when the answer does not select the parent's identity (`hash`).
    pub parent: Option<Parent>,
}

fn esc(k: &str) -> String {
    k.replace('~', "~0").replace('/', "~1")
}

/// The nested lists in scope of one body, outermost first.
pub fn scoped_lists(body: &Value) -> Vec<ScopedList> {
    struct Walk {
        out: Vec<ScopedList>,
    }
    // `field`: response key whose value is `v` (for an edge's node, its connection's key);
    // `outer_hash`: the `hash` of the object holding `v` (a transaction above its `result`).
    fn walk(
        w: &mut Walk,
        v: &Value,
        ptr: &str,
        path: &mut Vec<String>,
        field: &str,
        outer_hash: Option<&str>,
    ) {
        match v {
            Value::Object(m) => {
                let hash = m.get("hash").and_then(Value::as_str);
                for (k, x) in m {
                    let p = format!("{ptr}/{}", esc(k));
                    let is_list = x.get("edges").is_some_and(Value::is_array);
                    if is_list && !path.is_empty() {
                        let parent = if BLOCK_LISTS.contains(&k.as_str())
                            && BLOCK_PARENTS.contains(&field)
                        {
                            Some(hash.map(|h| Parent::Block(h.to_string())))
                        } else if TX_LISTS.contains(&k.as_str()) && field == "result" {
                            Some(outer_hash.map(|h| Parent::Tx(h.to_string())))
                        } else {
                            None
                        };
                        if let Some(parent) = parent {
                            let mut doc_path = path.clone();
                            doc_path.push(k.clone());
                            w.out.push(ScopedList {
                                pointer: p.clone(),
                                doc_path,
                                parent,
                            });
                        }
                    }
                    path.push(k.clone());
                    // An edge's node keeps its connection's key as its field name.
                    let next_field = if k == "edges" || k == "node" {
                        field
                    } else {
                        k.as_str()
                    };
                    walk(w, x, &p, path, next_field, hash);
                    path.pop();
                }
            }
            Value::Array(xs) => {
                for (i, x) in xs.iter().enumerate() {
                    walk(w, x, &format!("{ptr}/{i}"), path, field, outer_hash);
                }
            }
            _ => {}
        }
    }
    let mut w = Walk { out: Vec::new() };
    if let Some(d) = body.get("data") {
        walk(&mut w, d, "/data", &mut Vec::new(), "", None);
    }
    w.out
}

/// Whether the op's root list is scoped to one block or one transaction: a non-null
/// `blockHash` or `requestKey` variable on a root `transactions`, `transfers` or `events`.
pub fn root_scoped(roots: &[String], vars: &Map<String, Value>) -> bool {
    let set = |k: &str| {
        vars.get(k)
            .is_some_and(|v| v.as_str().is_some_and(|s| !s.is_empty()))
    };
    (set("blockHash") || set("requestKey"))
        && roots.iter().any(|r| ROOT_LISTS.contains(&r.as_str()))
}

/// Whether a page of a list is cut: `hasNextPage`, or a `totalCount` above its edge count.
pub fn is_cut(conn: &Value) -> bool {
    let n = conn
        .get("edges")
        .and_then(Value::as_array)
        .map_or(0, Vec::len) as u64;
    conn.pointer("/pageInfo/hasNextPage") == Some(&Value::Bool(true))
        || conn
            .get("totalCount")
            .and_then(Value::as_u64)
            .is_some_and(|t| t > n)
}

/// Orders a list's edges by their canonical text: the list compares as a set.
pub fn sort_as_set(conn: &mut Value) {
    if let Some(e) = conn.get_mut("edges").and_then(Value::as_array_mut) {
        e.sort_by_cached_key(Value::to_string);
    }
}

/// The node texts of a list's edges, sorted (a multiset).
fn node_set(edges: &[Value]) -> Vec<String> {
    let mut s: Vec<String> = edges
        .iter()
        .map(|e| e.get("node").unwrap_or(&Value::Null).to_string())
        .collect();
    s.sort();
    s
}

/// Whether `full` (one side's whole list, as edges) may stand for `orig` (that side's page in
/// the answer): the page is a part of the full list, and a page that was not cut is all of
/// it. Anything else means the side changed or is inconsistent, and the list is left as is.
pub fn finish_ok(orig: &Value, full: &[Value]) -> bool {
    let Some(page) = orig.get("edges").and_then(Value::as_array) else {
        return false;
    };
    let (p, f) = (node_set(page), node_set(full));
    if !is_cut(orig) {
        return p == f;
    }
    let mut it = f.iter();
    p.len() < f.len() && p.iter().all(|x| it.any(|y| y == x))
}

// ---------------------------------------------------------------------------------------------
// The document: just enough of a GraphQL lexer to find a field's arguments and selection.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok<'a> {
    Name(&'a str),
    Punct(u8),
    Spread,
    Other,
}

fn lex(q: &str) -> Option<Vec<(Tok<'_>, usize, usize)>> {
    let b = q.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        match c {
            b' ' | b'\t' | b'\n' | b'\r' | b',' => i += 1,
            0xEF if b[i..].starts_with(&[0xEF, 0xBB, 0xBF]) => i += 3,
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'"' if b[i..].starts_with(b"\"\"\"") => {
                i += 3;
                loop {
                    if i + 3 > b.len() {
                        return None;
                    }
                    if b[i..].starts_with(b"\\\"\"\"") {
                        i += 4;
                    } else if b[i..].starts_with(b"\"\"\"") {
                        i += 3;
                        break;
                    } else {
                        i += 1;
                    }
                }
                out.push((Tok::Other, start, i));
            }
            b'"' => {
                i += 1;
                loop {
                    match b.get(i)? {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        b'\n' => return None,
                        _ => i += 1,
                    }
                }
                out.push((Tok::Other, start, i));
            }
            b'.' if b[i..].starts_with(b"...") => {
                i += 3;
                out.push((Tok::Spread, start, i));
            }
            b'{' | b'}' | b'(' | b')' | b'[' | b']' | b':' | b'=' | b'@' | b'$' | b'!' | b'|'
            | b'&' => {
                i += 1;
                out.push((Tok::Punct(c), start, i));
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                out.push((Tok::Name(&q[start..i]), start, i));
            }
            c if c == b'-' || c.is_ascii_digit() => {
                i += 1;
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric() || matches!(b[i], b'.' | b'-' | b'+'))
                {
                    i += 1;
                }
                out.push((Tok::Other, start, i));
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Index of the token closing the group opened at `i` (`{`, `(` or `[`).
fn closing(t: &[(Tok<'_>, usize, usize)], i: usize) -> Option<usize> {
    let (open, close) = match t.get(i)?.0 {
        Tok::Punct(b'{') => (b'{', b'}'),
        Tok::Punct(b'(') => (b'(', b')'),
        Tok::Punct(b'[') => (b'[', b']'),
        _ => return None,
    };
    let mut depth = 0usize;
    for (j, (k, _, _)) in t.iter().enumerate().skip(i) {
        match k {
            Tok::Punct(c) if *c == open => depth += 1,
            Tok::Punct(c) if *c == close => {
                depth -= 1;
                if depth == 0 {
                    return Some(j);
                }
            }
            _ => {}
        }
    }
    None
}

/// Skips directives (`@name(args)`) from `j`; the next index.
fn skip_directives(t: &[(Tok<'_>, usize, usize)], mut j: usize) -> Option<usize> {
    while matches!(t.get(j), Some((Tok::Punct(b'@'), _, _))) {
        j += 2;
        if matches!(t.get(j), Some((Tok::Punct(b'('), _, _))) {
            j = closing(t, j)? + 1;
        }
    }
    Some(j)
}

/// A field found in the document: its argument group and its selection set, as token
/// index ranges (the opening and closing tokens).
#[derive(Debug, Clone, Copy)]
struct Found {
    args: Option<(usize, usize)>,
    set: Option<(usize, usize)>,
}

/// The field with response key `key` in the selection set opened at `open`, looking through
/// inline fragments. A named fragment spread is not followed (the caller then gives up).
fn find_in_set(t: &[(Tok<'_>, usize, usize)], open: usize, key: &str) -> Option<Found> {
    let end = closing(t, open)?;
    let mut j = open + 1;
    while j < end {
        match t[j].0 {
            Tok::Spread => {
                j += 1;
                if matches!(t[j].0, Tok::Name("on")) {
                    j += 2;
                }
                if let Tok::Name(_) = t[j].0 {
                    // A named fragment spread: not followed.
                    j = skip_directives(t, j + 1)?;
                    continue;
                }
                j = skip_directives(t, j)?;
                if let Some(f) = find_in_set(t, j, key) {
                    return Some(f);
                }
                j = closing(t, j)? + 1;
            }
            Tok::Name(n) => {
                let (resp, mut k) = match t.get(j + 1) {
                    Some((Tok::Punct(b':'), _, _)) => (n, j + 3),
                    _ => (n, j + 1),
                };
                let mut f = Found {
                    args: None,
                    set: None,
                };
                if matches!(t.get(k), Some((Tok::Punct(b'('), _, _))) {
                    let c = closing(t, k)?;
                    f.args = Some((k, c));
                    k = c + 1;
                }
                k = skip_directives(t, k)?;
                if matches!(t.get(k), Some((Tok::Punct(b'{'), _, _))) {
                    let c = closing(t, k)?;
                    f.set = Some((k, c));
                    k = c + 1;
                }
                if resp == key {
                    return Some(f);
                }
                j = k;
            }
            _ => return None,
        }
    }
    None
}

/// The operation's selection set (the first `{` outside any argument group, after any
/// fragment definitions are skipped).
fn op_set(t: &[(Tok<'_>, usize, usize)]) -> Option<usize> {
    let mut j = 0;
    while j < t.len() {
        match t[j].0 {
            Tok::Name("fragment") => {
                while !matches!(t.get(j)?.0, Tok::Punct(b'{')) {
                    j += 1;
                }
                j = closing(t, j)? + 1;
            }
            Tok::Punct(b'(') => j = closing(t, j)? + 1,
            Tok::Punct(b'{') => return Some(j),
            _ => j += 1,
        }
    }
    None
}

/// The field at `path` (response keys from the operation root, `edges` and `node`
/// included) in the document `q`.
fn field_at(t: &[(Tok<'_>, usize, usize)], path: &[String]) -> Option<Found> {
    let mut set = op_set(t)?;
    let mut found = None;
    for (i, key) in path.iter().enumerate() {
        let f = find_in_set(t, set, key)?;
        if i + 1 < path.len() {
            set = f.set?.0;
        }
        found = Some(f);
    }
    found
}

/// The harness's own read of the rest of a nested list: the query, the variables without
/// `after`, and the pointer of the list in its answer. `None` when the document's selection
/// cannot be re-issued alone (a variable, a named fragment, or an argument other than the
/// page arguments).
pub fn list_query(doc: &str, list: &ScopedList) -> Option<(String, Value, &'static str)> {
    let parent = list.parent.as_ref()?;
    let field = list.doc_path.last()?;
    if !field
        .bytes()
        .all(|c| c == b'_' || c.is_ascii_alphanumeric())
    {
        return None;
    }
    let t = lex(doc)?;
    let f = field_at(&t, &list.doc_path)?;
    // Only page arguments, which the harness sets itself.
    if let Some((o, c)) = f.args {
        let mut j = o + 1;
        while j < c {
            match (t[j].0, t.get(j + 1).map(|x| x.0)) {
                (Tok::Name(n), Some(Tok::Punct(b':'))) if PAGE_ARGS.contains(&n) => {
                    j += 2;
                    while j < c
                        && !matches!(
                            (t[j].0, t.get(j + 1).map(|x| x.0)),
                            (Tok::Name(_), Some(Tok::Punct(b':')))
                        )
                    {
                        j += 1;
                    }
                }
                _ => return None,
            }
        }
    }
    // The node selection: `<field> { ... edges { ... node { SEL } } }`.
    let (so, _) = f.set?;
    let edges = find_in_set(&t, so, "edges")?;
    let node = find_in_set(&t, edges.set?.0, "node")?;
    let (no, nc) = node.set?;
    let sel_tokens = &t[no..=nc];
    let named_spread = sel_tokens
        .windows(2)
        .any(|w| w[0].0 == Tok::Spread && matches!(w[1].0, Tok::Name(n) if n != "on"));
    if named_spread || sel_tokens.iter().any(|x| x.0 == Tok::Punct(b'$')) {
        return None;
    }
    let sel = &doc[t[no].1..t[nc].2];
    let conn = format!(
        "c: {field}(first: {PAGE_SIZE}, after: $after) {{ pageInfo {{ hasNextPage endCursor }} edges {{ node {sel} }} }}"
    );
    Some(match parent {
        Parent::Block(h) => (
            format!("query($p: String!, $after: String) {{ block(hash: $p) {{ {conn} }} }}"),
            json!({ "p": h }),
            "/data/block/c",
        ),
        Parent::Tx(rk) => (
            format!(
                "query($p: String!, $after: String) {{ transaction(requestKey: $p) {{ result {{ ... on TransactionResult {{ {conn} }} }} }} }}"
            ),
            json!({ "p": rk }),
            "/data/transaction/result/c",
        ),
    })
}

/// The harness's read of the block hashes of a root block list whose answer selects none
/// (`blockTransactions`): the document's own root field and arguments, with the selection
/// `edges { node { hash } }`, under the document's variable definitions. `None` when the
/// root is not a block list or the document has a named fragment.
pub fn hash_query(doc: &str, root: &str) -> Option<String> {
    if !BLOCK_PARENTS[1..].contains(&root) {
        return None;
    }
    let t = lex(doc)?;
    if t.iter().any(|x| x.0 == Tok::Name("fragment")) {
        return None;
    }
    let set = op_set(&t)?;
    let defs = t[..set]
        .iter()
        .position(|x| x.0 == Tok::Punct(b'('))
        .and_then(|o| Some(&doc[t[o].1..t[closing(&t, o)?].2]))
        .unwrap_or("");
    let f = find_in_set(&t, set, root)?;
    let args = f.args.map_or("", |(o, c)| &doc[t[o].1..t[c].2]);
    Some(format!(
        "query{defs} {{ {root}{args} {{ edges {{ node {{ hash }} }} }} }}"
    ))
}

/// Puts the hashes of a [`hash_query`] answer (`/data/<root>/edges/<i>/node/hash`) into the
/// root's edge nodes that have none, by position; returns the pointers it set (to take them
/// out again). Nothing is set when the two lists differ in length.
pub fn put_hashes(body: &mut Value, root: &str, hashes: &Value) -> Vec<String> {
    let ptr = format!("/data/{}/edges", esc(root));
    let Some(hs) = hashes.pointer(&ptr).and_then(Value::as_array) else {
        return Vec::new();
    };
    let hs: Vec<Option<Value>> = hs
        .iter()
        .map(|e| e.pointer("/node/hash").cloned())
        .collect();
    let Some(edges) = body.pointer_mut(&ptr).and_then(Value::as_array_mut) else {
        return Vec::new();
    };
    if edges.len() != hs.len() {
        return Vec::new();
    }
    let mut set = Vec::new();
    for (i, (e, h)) in edges.iter_mut().zip(hs).enumerate() {
        if let (Some(n), Some(h)) = (e.get_mut("node").and_then(Value::as_object_mut), h) {
            if !n.contains_key("hash") && h.is_string() {
                n.insert("hash".into(), h);
                set.push(format!("{ptr}/{i}/node"));
            }
        }
    }
    set
}

/// Takes out the hashes [`put_hashes`] set.
pub fn take_hashes(body: &mut Value, set: &[String]) {
    for p in set {
        if let Some(n) = body.pointer_mut(p).and_then(Value::as_object_mut) {
            n.remove("hash");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QBLOCK: &str = "# kdx: oracle\nquery QBlock($hash: String!) { block(hash: $hash) { id hash height transactions(first: 5) { totalCount pageInfo { hasNextPage hasPreviousPage } edges { cursor node { id hash } } } events(first: 5) { totalCount edges { node { id name qualifiedName orderIndex } } } } }";
    const BY_KEY: &str = "query Q($requestKey: String!) { transaction(requestKey: $requestKey) { id hash result { ... on TransactionMempoolInfo { status } ... on TransactionResult { gas events(first: 20) { totalCount edges { node { id name parameters } } } transfers(first: $n) { totalCount edges { node { id amount } } } } } } }";

    fn edges(ids: &[&str]) -> Value {
        Value::Array(ids.iter().map(|i| json!({"node": {"id": i}})).collect())
    }

    #[test]
    fn scoped_lists_are_block_and_tx_lists_only() {
        let body = json!({"data": {
            "block": {"hash": "B", "transactions": {"edges": []}, "events": {"edges": []},
                      "parent": {"hash": "P"}},
            "fungibleAccount": {"transactions": {"edges": []}, "transfers": {"edges": []}},
            "blocksFromHeight": {"edges": [{"node": {"hash": "H1", "transactions": {"totalCount": 3, "edges": [
                {"node": {"hash": "R", "result": {"events": {"edges": []}}}}]}}},
                {"node": {"transactions": {"edges": []}}}]},
            "transaction": {"hash": "R2", "result": {"transfers": {"edges": []}}}
        }});
        let got: Vec<(String, Option<Parent>)> = scoped_lists(&body)
            .into_iter()
            .map(|l| (l.pointer, l.parent))
            .collect();
        let b = |h: &str| Some(Parent::Block(h.into()));
        let want = vec![
            ("/data/block/events".to_string(), b("B")),
            ("/data/block/transactions".to_string(), b("B")),
            (
                "/data/blocksFromHeight/edges/0/node/transactions".to_string(),
                b("H1"),
            ),
            (
                "/data/blocksFromHeight/edges/0/node/transactions/edges/0/node/result/events"
                    .to_string(),
                Some(Parent::Tx("R".into())),
            ),
            (
                "/data/blocksFromHeight/edges/1/node/transactions".to_string(),
                None,
            ),
            (
                "/data/transaction/result/transfers".to_string(),
                Some(Parent::Tx("R2".into())),
            ),
        ];
        let mut got = got;
        got.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(got, want);
        // Account lists and root lists are not nested block or tx lists.
        let l = scoped_lists(&body);
        assert!(l.iter().all(|x| !x.pointer.contains("fungibleAccount")));
        let path_of = |p: &str| l.iter().find(|x| x.pointer == p).unwrap().doc_path.clone();
        assert_eq!(path_of("/data/block/events"), ["block", "events"]);
        assert_eq!(
            path_of("/data/blocksFromHeight/edges/0/node/transactions"),
            ["blocksFromHeight", "edges", "node", "transactions"]
        );
        let vars = |k: &str, v: Value| {
            let mut m = Map::new();
            m.insert(k.into(), v);
            m
        };
        let r = |s: &str| vec![s.to_string()];
        assert!(root_scoped(
            &r("transactions"),
            &vars("blockHash", json!("B"))
        ));
        assert!(root_scoped(
            &r("transfers"),
            &vars("requestKey", json!("R"))
        ));
        assert!(!root_scoped(
            &r("transfers"),
            &vars("blockHash", Value::Null)
        ));
        assert!(!root_scoped(
            &r("transfers"),
            &vars("accountName", json!("k:a"))
        ));
        assert!(!root_scoped(
            &r("transaction"),
            &vars("requestKey", json!("R"))
        ));
    }

    #[test]
    fn cut_pages_and_finished_lists() {
        let page = json!({"totalCount": 3, "edges": edges(&["a", "c"])});
        assert!(is_cut(&page));
        assert!(is_cut(
            &json!({"pageInfo": {"hasNextPage": true}, "edges": []})
        ));
        assert!(!is_cut(
            &json!({"totalCount": 2, "pageInfo": {"hasNextPage": false}, "edges": edges(&["a", "b"])})
        ));
        let full = edges(&["c", "b", "a"]);
        let full = full.as_array().unwrap();
        // A cut page that is a part of its side's full list.
        assert!(finish_ok(&page, full));
        // A row the full list lacks, or a full list no longer than the page: left as is.
        assert!(!finish_ok(
            &json!({"totalCount": 3, "edges": edges(&["a", "x"])}),
            full
        ));
        assert!(!finish_ok(
            &json!({"totalCount": 9, "edges": edges(&["a", "b", "c"])}),
            full
        ));
        // A page that was not cut must be all of it.
        assert!(finish_ok(
            &json!({"totalCount": 3, "edges": edges(&["b", "a", "c"])}),
            full
        ));
        assert!(!finish_ok(
            &json!({"totalCount": 2, "edges": edges(&["a", "b"])}),
            full
        ));
        // Duplicates count.
        assert!(!finish_ok(
            &json!({"totalCount": 4, "edges": edges(&["a", "a"])}),
            full
        ));
        let mut c = json!({"edges": edges(&["b", "a"])});
        sort_as_set(&mut c);
        assert_eq!(c, json!({"edges": edges(&["a", "b"])}));
    }

    #[test]
    fn list_queries_reissue_the_documents_node_selection() {
        let l = |path: &[&str], parent: Parent| ScopedList {
            pointer: String::new(),
            doc_path: path.iter().map(|s| s.to_string()).collect(),
            parent: Some(parent),
        };
        let (q, v, at) = list_query(
            QBLOCK,
            &l(&["block", "transactions"], Parent::Block("B".into())),
        )
        .unwrap();
        assert_eq!(
            q,
            "query($p: String!, $after: String) { block(hash: $p) { c: transactions(first: 50, after: $after) { pageInfo { hasNextPage endCursor } edges { node { id hash } } } } }"
        );
        assert_eq!((v, at), (json!({"p": "B"}), "/data/block/c"));
        let (q, _, _) =
            list_query(QBLOCK, &l(&["block", "events"], Parent::Block("B".into()))).unwrap();
        assert!(q.contains("c: events(first: 50, after: $after) { pageInfo { hasNextPage endCursor } edges { node { id name qualifiedName orderIndex } } }"), "{q}");
        // Through an inline fragment; a `$first` page argument is the harness's own.
        let (q, v, at) = list_query(
            BY_KEY,
            &l(
                &["transaction", "result", "transfers"],
                Parent::Tx("R".into()),
            ),
        )
        .unwrap();
        assert!(q.contains("transaction(requestKey: $p) { result { ... on TransactionResult { c: transfers(first: 50, after: $after)"), "{q}");
        assert!(q.ends_with("edges { node { id amount } } } } } } }"), "{q}");
        assert_eq!((v, at), (json!({"p": "R"}), "/data/transaction/result/c"));
        // Not re-issued: an unknown field, another argument, a variable in the selection, a
        // named fragment, no parent identity.
        assert!(list_query(QBLOCK, &l(&["block", "nope"], Parent::Block("B".into()))).is_none());
        let other = QBLOCK.replace(
            "transactions(first: 5)",
            "transactions(first: 5, minimumDepth: 3)",
        );
        assert!(list_query(
            &other,
            &l(&["block", "transactions"], Parent::Block("B".into()))
        )
        .is_none());
        let var = QBLOCK.replace("node { id hash }", "node { id hash(x: $y) }");
        assert!(list_query(
            &var,
            &l(&["block", "transactions"], Parent::Block("B".into()))
        )
        .is_none());
        let frag = QBLOCK.replace("node { id hash }", "node { ...F }");
        assert!(list_query(
            &frag,
            &l(&["block", "transactions"], Parent::Block("B".into()))
        )
        .is_none());
        let mut none = l(&["block", "transactions"], Parent::Block("B".into()));
        none.parent = None;
        assert!(list_query(QBLOCK, &none).is_none());
    }

    #[test]
    fn hash_reads_reissue_the_root_block_list() {
        let doc = "query blockTransactions($startHeight: Int!, $endHeight: Int, $chainIds: [String!], $first: Int, $after: String) { blocksFromHeight(startHeight: $startHeight, endHeight: $endHeight, chainIds: $chainIds) { edges { node { transactions(first: $first, after: $after) { totalCount edges { node { result { ... on TransactionResult { gas } } } } pageInfo { endCursor hasNextPage } } } } } }";
        let q = hash_query(doc, "blocksFromHeight").unwrap();
        assert_eq!(
            q,
            "query($startHeight: Int!, $endHeight: Int, $chainIds: [String!], $first: Int, $after: String) { blocksFromHeight(startHeight: $startHeight, endHeight: $endHeight, chainIds: $chainIds) { edges { node { hash } } } }"
        );
        assert!(hash_query(doc, "transactions").is_none());
        assert!(hash_query(
            &format!("{doc} fragment F on Block {{ hash }}"),
            "blocksFromHeight"
        )
        .is_none());
        let mut body = json!({"data": {"blocksFromHeight": {"edges": [
            {"node": {"transactions": {"edges": []}}}, {"node": {"hash": "own", "transactions": {"edges": []}}}]}}});
        let keep = body.clone();
        let hashes = json!({"data": {"blocksFromHeight": {"edges": [{"node": {"hash": "H0"}}, {"node": {"hash": "H1"}}]}}});
        let set = put_hashes(&mut body, "blocksFromHeight", &hashes);
        assert_eq!(set, ["/data/blocksFromHeight/edges/0/node"]);
        let l = scoped_lists(&body);
        assert_eq!(l[0].parent, Some(Parent::Block("H0".into())));
        assert_eq!(l[1].parent, Some(Parent::Block("own".into())));
        take_hashes(&mut body, &set);
        assert_eq!(body, keep);
        // Another length: nothing is set.
        let short = json!({"data": {"blocksFromHeight": {"edges": [{"node": {"hash": "H0"}}]}}});
        assert!(put_hashes(&mut body, "blocksFromHeight", &short).is_empty());
    }
}
