//! `graphql print-schema` and `graphql check-ops` (P7 Task 2). Both run without a config or
//! store, with the default `[graphql]` limits.

use std::path::{Path, PathBuf};

use anyhow::Context;
use kdx_graphql::{build_schema_check, build_schema_detached, GraphqlSettings};

/// Largest operation document read.
const MAX_DOC_BYTES: u64 = 1 << 20;

pub fn print_schema() -> String {
    build_schema_detached(GraphqlSettings::default()).sdl()
}

/// The verdict on one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    InScope,
    Invalid(Vec<String>),
    Limit(String),
}

/// Validate one document (no variables are bound; only validation and limits count).
pub async fn check_doc(schema: &kdx_graphql::KdxSchema, text: &str) -> Verdict {
    let resp = schema.execute(async_graphql::Request::new(text)).await;
    let code = |e: &async_graphql::ServerError| {
        e.extensions
            .as_ref()
            .and_then(|x| x.get("code"))
            .map(|v| v.to_string().trim_matches('"').to_owned())
    };
    let mut invalid = Vec::new();
    for e in &resp.errors {
        match code(e).as_deref() {
            Some(kdx_graphql::limits::CHECK_PASSED) => {}
            Some("LIMIT") => {
                let name = e
                    .extensions
                    .as_ref()
                    .and_then(|x| x.get("limit"))
                    .map_or_else(String::new, |v| v.to_string().trim_matches('"').to_owned());
                return Verdict::Limit(name);
            }
            _ => invalid.push(e.message.clone()),
        }
    }
    if invalid.is_empty() {
        Verdict::InScope
    } else {
        Verdict::Invalid(invalid)
    }
}

fn docs(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let p = e?.path();
        if p.extension().is_some_and(|x| x == "graphql" || x == "gql") {
            v.push(p);
        }
    }
    v.sort();
    Ok(v)
}

pub async fn check_ops(dir: &Path, json: bool) -> anyhow::Result<()> {
    let schema = build_schema_check(GraphqlSettings::default());
    let mut rows = Vec::new();
    for p in docs(dir)? {
        let len = std::fs::metadata(&p)?.len();
        let name = p
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let v = if len > MAX_DOC_BYTES {
            Verdict::Invalid(vec![format!("larger than {MAX_DOC_BYTES} bytes")])
        } else {
            let text =
                std::fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
            check_doc(&schema, &text).await
        };
        rows.push((name, v));
    }
    if json {
        let a: Vec<serde_json::Value> = rows
            .iter()
            .map(|(n, v)| match v {
                Verdict::InScope => serde_json::json!({"file": n, "status": "in-scope"}),
                Verdict::Invalid(e) => {
                    serde_json::json!({"file": n, "status": "invalid", "errors": e})
                }
                Verdict::Limit(l) => serde_json::json!({"file": n, "status": "limit", "limit": l}),
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&a)?);
    } else {
        for (n, v) in &rows {
            match v {
                Verdict::InScope => println!("{n}\tin-scope"),
                Verdict::Invalid(e) => println!("{n}\tinvalid: {}", e.join("; ")),
                Verdict::Limit(l) => println!("{n}\tlimit: {l}"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn check_ops_verdicts() {
        let s = build_schema_check(GraphqlSettings::default());
        assert_eq!(
            check_doc(&s, "query Q($h: String!) { block(hash: $h) { height powHash minerAccount { guard { raw ... on KeysetGuard { keys } } } } }").await,
            Verdict::InScope
        );
        assert_eq!(check_doc(&s, "{ lastBlockHeight }").await, Verdict::InScope);
        match check_doc(&s, "{ block(hash: \"x\") { height dexPrice } }").await {
            Verdict::Invalid(e) => assert!(e[0].contains("dexPrice"), "{e:?}"),
            v => panic!("{v:?}"),
        }
        let deep = format!(
            "{{ block(hash: \"x\") {{ {} hash {} }} }}",
            "parent { ".repeat(10),
            "}".repeat(10)
        );
        assert_eq!(check_doc(&s, &deep).await, Verdict::Limit("depth".into()));
        // Subscriptions validate too.
        assert_eq!(
            check_doc(&s, "subscription { newBlocks(chainIds: [\"0\"]) { hash } }").await,
            Verdict::InScope
        );
    }

    #[test]
    fn printed_schema_has_the_roots() {
        let sdl = print_schema();
        assert!(sdl.contains("type Query") && sdl.contains("type Subscription"));
    }
}
