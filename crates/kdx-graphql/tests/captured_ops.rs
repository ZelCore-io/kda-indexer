//! kadscan's captured operation documents (P7 exit, R-P5P9-16): every one parses, passes the
//! limits and validates against our schema (Kadena's plus the ZelCore extensions).
//!
//! The captures are not in the repo. Point `KDX_KADSCAN_CAPTURE` at a directory holding
//! `ops/*.graphql` (one document per file) and, optionally, `ops.json` (a list of
//! `{"sha8", "exampleVariables"}`: the non-string variables are applied, as they drive the
//! page multipliers of the limits). A document with no captured variables (seen in a bundle
//! only) is validated with the complexity cap lifted, since its call-time page sizes are
//! unknown (an unbound `first` costs `max_page`); its strict verdict is printed. Without the
//! variable the test is skipped.

use std::collections::HashMap;
use std::path::PathBuf;

use async_graphql::{Request, Variables};
use kdx_graphql::limits::CHECK_PASSED;
use kdx_graphql::{build_schema_check, GraphqlSettings};
use serde_json::Value;

fn example_vars(dir: &std::path::Path) -> HashMap<String, Value> {
    let Ok(text) = std::fs::read_to_string(dir.join("ops.json")) else {
        return HashMap::new();
    };
    let list: Vec<Value> = serde_json::from_str(&text).expect("ops.json is a JSON list");
    list.into_iter()
        .filter_map(|o| {
            let sha = o.get("sha8")?.as_str()?.to_owned();
            let ints: serde_json::Map<String, Value> = o
                .get("exampleVariables")?
                .as_object()?
                .iter()
                .filter(|(_, v)| !v.is_string())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Some((sha, Value::Object(ints)))
        })
        .collect()
}

#[tokio::test]
async fn every_captured_document_validates() {
    let Some(dir) = std::env::var_os("KDX_KADSCAN_CAPTURE").map(PathBuf::from) else {
        eprintln!("KDX_KADSCAN_CAPTURE is not set: skipped");
        return;
    };
    let vars = example_vars(&dir);
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("ops"))
        .expect("ops/ directory")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "graphql"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no ops/*.graphql documents");
    let schema = build_schema_check(GraphqlSettings::default());
    let lenient = build_schema_check(GraphqlSettings {
        max_complexity: u64::MAX,
        ..GraphqlSettings::default()
    });
    let mut failed = Vec::new();
    let mut unjudged = Vec::new();
    for f in &files {
        let name = f.file_stem().unwrap().to_string_lossy().into_owned();
        let sha = name.rsplit('-').next().unwrap_or_default().to_owned();
        let q = std::fs::read_to_string(f).expect("document text");
        let known = vars.get(&sha).cloned();
        let s = if known.is_some() { &schema } else { &lenient };
        if known.is_none() {
            let strict = schema.execute(Request::new(q.clone())).await;
            let verdict = strict
                .errors
                .first()
                .map_or(String::new(), |e| e.message.clone());
            unjudged.push(format!("{name}: {verdict}"));
        }
        let v = known.unwrap_or(Value::Null);
        let r = s
            .execute(Request::new(q).variables(Variables::from_json(v)))
            .await;
        let passed = r.errors.len() == 1
            && r.errors[0]
                .extensions
                .as_ref()
                .and_then(|x| x.get("code"))
                .is_some_and(|c| *c == async_graphql::Value::String(CHECK_PASSED.into()));
        if !passed {
            let msgs: Vec<String> = r.errors.iter().map(|e| e.message.clone()).collect();
            failed.push(format!("{name}: {}", msgs.join("; ")));
        }
    }
    for u in &unjudged {
        eprintln!("no captured variables, cap lifted (strict verdict: {u})");
    }
    eprintln!(
        "{} of {} captured documents validate",
        files.len() - failed.len(),
        files.len()
    );
    assert!(
        failed.is_empty(),
        "invalid documents:\n{}",
        failed.join("\n")
    );
}
