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

/// Map a stored guard (the node's JSON) onto the schema's shapes (GQ-14): an inline keyset is
/// a `KeysetGuard`; a keyset reference resolved on the row's chain (`resolved`) is a
/// `KeysetGuard` whose `raw` is the stored reference; a user guard is a `UserGuard`; anything
/// else (capability, module, pact guards, an unresolved reference) is a `RawGuard`. `raw` is
/// always the compact JSON of the stored guard.
pub fn map_guard(g: &serde_json::Value, resolved: Option<&serde_json::Value>) -> IGuard {
    use serde_json::Value;
    let raw = crate::types::tx::json_text(g);
    let keyset = |k: &Value| -> Option<(Vec<String>, String)> {
        let keys = k.get("keys")?.as_array()?;
        let pred = k.get("pred")?.as_str()?;
        Some((
            keys.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            pred.to_owned(),
        ))
    };
    if let Some((keys, predicate)) = keyset(g) {
        return IGuard::KeysetGuard(KeysetGuard {
            keys,
            predicate,
            raw,
        });
    }
    if g.get("keysetref").is_some() {
        if let Some((keys, predicate)) = resolved.and_then(keyset) {
            return IGuard::KeysetGuard(KeysetGuard {
                keys,
                predicate,
                raw,
            });
        }
    }
    if let (Some(fun), Some(args)) = (
        g.get("fun").and_then(Value::as_str),
        g.get("args").and_then(Value::as_array),
    ) {
        return IGuard::UserGuard(UserGuard {
            args: args
                .iter()
                .map(|a| match a {
                    Value::String(s) => s.clone(),
                    a => crate::types::tx::json_text(a),
                })
                .collect(),
            fun: fun.to_owned(),
            keys: vec![],
            predicate: String::new(),
            raw,
        });
    }
    IGuard::RawGuard(RawGuard {
        keys: vec![],
        predicate: String::new(),
        raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn guard_shapes() {
        let g = json!({"keys": ["ab"], "pred": "keys-all"});
        match map_guard(&g, None) {
            IGuard::KeysetGuard(k) => {
                assert_eq!(
                    (k.keys, k.predicate.as_str()),
                    (vec!["ab".to_owned()], "keys-all")
                );
                assert_eq!(k.raw, r#"{"keys":["ab"],"pred":"keys-all"}"#);
            }
            g => panic!("{g:?}"),
        }
        let r = json!({"keysetref": {"ns": "free", "ksn": "ks"}});
        assert!(matches!(map_guard(&r, None), IGuard::RawGuard(_)));
        let ks = json!({"keys": ["cd"], "pred": "keys-any"});
        match map_guard(&r, Some(&ks)) {
            IGuard::KeysetGuard(k) => {
                assert_eq!(k.keys, vec!["cd".to_owned()]);
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&k.raw).unwrap(),
                    r
                );
            }
            g => panic!("{g:?}"),
        }
        let u = json!({"fun": "free.m.enforce", "args": ["k:a", 1]});
        match map_guard(&u, None) {
            IGuard::UserGuard(u) => {
                assert_eq!(u.args, vec!["k:a".to_owned(), "1".to_owned()]);
                assert_eq!(u.fun, "free.m.enforce");
            }
            g => panic!("{g:?}"),
        }
        let c = json!({"cgName": "x", "cgArgs": [], "cgPactId": null});
        assert!(matches!(map_guard(&c, None), IGuard::RawGuard(_)));
    }
}
