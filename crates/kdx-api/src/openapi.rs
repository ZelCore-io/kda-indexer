//! OpenAPI 3.1 documents of both REST APIs, generated from the handler annotations:
//! the native API (`kda-indexer openapi > docs/openapi.json`) and the chainweb-data
//! compatible API (`kda-indexer openapi --compat > docs/openapi-compat.json`).

use utoipa::OpenApi;

use crate::compat;
use crate::native::{accounts, balances, blocks, events, nft, search, stats, tokens, txs};

#[derive(OpenApi)]
#[openapi(
    servers((url = "/", description = "This server (same origin)")),
    info(
        title = "kda-indexer native API",
        version = "1",
        description = "Internal and unstable until a public hostname is decided. JSON in snake_case. \
List routes take limit (1..=100, default 20), an opaque cursor, order (desc default, asc), minheight, \
maxheight and chain, and answer {items, next_cursor, as_of}. A filtered page can be shorter than \
limit while next_cursor is set: follow it until it is null. A cursor whose block was reorged away \
answers 400 'cursor invalidated by reorg'. Balances and tokens come from /local reconciles: every \
balance row carries as_of_height and stale. Search routes run under a shared gate (503 when \
search.concurrency searches run, 429 when the client already runs search.per_client) and answer \
truncated: true when their deadline or a cap cut them. NFT holdings come from marmalade ledger events and /local \
(source event or local)."
    ),
    paths(
        crate::status::handler,
        stats::stats,
        blocks::list,
        blocks::by_hash,
        blocks::by_height,
        blocks::txs,
        txs::get_tx,
        txs::defpact,
        accounts::transfers,
        accounts::txs,
        accounts::key_txs,
        balances::account_balances,
        balances::key_accounts,
        tokens::list,
        tokens::get,
        nft::account,
        events::list,
        search::code,
        search::events,
    )
)]
pub struct ApiDoc;

/// The document as pretty JSON.
pub fn json() -> String {
    ApiDoc::openapi().to_pretty_json().unwrap_or_default()
}

/// The chainweb-data compatible API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "kda-indexer chainweb-data compatible API",
        version = "1",
        description = "The REST routes of chainweb-data (txs, account transfers, events, search, \
/coins, /stats) and the /v1/txs convenience route, served by the compat listener (api.compat). \
Shapes, error texts and row order follow chainweb-data; documented differences are listed \
per route. JSON uses chainweb-data's camelCase names and aeson's number and time text. \
Errors are plain text. List routes (/txs/account, /txs/events, /txs/search) take limit \
(default 10, capped at 250), offset (up to 10,000) or a next token, minheight and maxheight, \
and answer a JSON array with the next page's token in the Chainweb-Next header. Genesis \
transactions are hidden on the list routes. Every data route answers 429 (per-client rate), \
503 (too many requests in flight, or an answer over api.max_body_mb) and 504 (the 5 s \
deadline). Search routes run under a shared gate (503 when search.concurrency searches run, \
429 when the client already runs search.per_client). CORS is open and Chainweb-Next and \
X-Count-Age are exposed."
    ),
    servers((url = "/", description = "This server (same origin)")),
    components(schemas(compat::schemas::ExecutionStrategy)),
    tags(
        (name = "transactions", description = "Transactions by request key, and the newest ones"),
        (name = "transfers", description = "Transfers of an account, and the transactions behind them"),
        (name = "events", description = "Events"),
        (name = "search", description = "Code search and defpact listing"),
        (name = "network", description = "Supply and counts"),
    ),
    paths(
        compat::tx::tx_handler,
        compat::tx::txs_handler,
        compat::account::handler,
        compat::events::handler,
        compat::recent::handler,
        compat::search::handler,
        compat::coins::handler,
        compat::stats::handler,
        crate::v1txs::doc_query_form,
        crate::v1txs::doc_path_form,
    )
)]
pub struct CompatApiDoc;

/// The compat document as pretty JSON.
pub fn compat_json() -> String {
    CompatApiDoc::openapi().to_pretty_json().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use regex::Regex;
    use serde_json::Value;

    use super::*;

    const NATIVE_FILE: &str = include_str!("../../../docs/openapi.json");
    const COMPAT_FILE: &str = include_str!("../../../docs/openapi-compat.json");

    fn parse(s: &str) -> Value {
        serde_json::from_str(s).expect("valid JSON")
    }

    fn paths(doc: &Value) -> BTreeSet<String> {
        doc["paths"].as_object().unwrap().keys().cloned().collect()
    }

    #[test]
    fn native_doc_file_is_current() {
        assert_eq!(
            NATIVE_FILE.trim_end(),
            json(),
            "docs/openapi.json is stale: kda-indexer openapi > docs/openapi.json"
        );
    }

    #[test]
    fn compat_doc_file_is_current() {
        assert_eq!(
            COMPAT_FILE.trim_end(),
            compat_json(),
            "docs/openapi-compat.json is stale: kda-indexer openapi --compat > docs/openapi-compat.json"
        );
    }

    /// Route strings registered by `.route("...")` in the production part of a source file.
    fn registered(src: &str) -> Vec<String> {
        let prod = src.split("mod tests {").next().unwrap();
        let re = Regex::new(r#"\.route\(\s*"([^"]+)""#).unwrap();
        re.captures_iter(prod).map(|c| c[1].to_owned()).collect()
    }

    /// Every route the REST listeners serve, as (spec, path) once the axum spelling is
    /// mapped to the documented one. The admin listener (`/healthz`, `/readyz`, `/metrics`)
    /// and the GraphQL listener are not REST APIs and have no spec.
    fn served() -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        for r in registered(include_str!("native/mod.rs")) {
            out.push(("native", format!("/api/v1{r}")));
        }
        // The native listener also answers /status at its root: the same handler as
        // /api/v1/status, which the spec documents (server.rs, `Api::new`).
        let root = registered(include_str!("server.rs"));
        assert_eq!(
            root,
            ["/status"],
            "server.rs registers a route the spec test does not know"
        );
        out.push(("native", "/api/v1/status".into()));
        for r in registered(include_str!("compat/mod.rs")) {
            out.push(("compat", r));
        }
        for r in registered(include_str!("v1txs.rs")) {
            // `/v1/txs/` and the wildcard are spellings of the documented forms.
            let p = match r.as_str() {
                "/v1/txs" | "/v1/txs/" => "/v1/txs",
                "/v1/txs/{*rest}" => "/v1/txs/{account}/{namespace}/{limit}",
                other => panic!("v1txs registers {other}, which the spec test does not know"),
            };
            out.push(("compat", p.to_owned()));
        }
        out
    }

    #[test]
    fn every_served_route_is_in_exactly_one_spec() {
        let docs: BTreeMap<&str, BTreeSet<String>> = [
            ("native", paths(&parse(&json()))),
            ("compat", paths(&parse(&compat_json()))),
        ]
        .into();
        assert!(
            docs["native"].is_disjoint(&docs["compat"]),
            "a path is in both specs"
        );
        let served = served();
        assert!(served.len() >= 25, "route scan found only {}", served.len());
        for (spec, path) in &served {
            let hits: Vec<_> = docs.iter().filter(|(_, p)| p.contains(path)).collect();
            assert_eq!(
                hits.len(),
                1,
                "{spec} route {path} is in {} specs",
                hits.len()
            );
            assert_eq!(*hits[0].0, *spec, "{path} is documented in the wrong spec");
        }
        let all: BTreeSet<(&str, &String)> = served.iter().map(|(s, p)| (*s, p)).collect();
        for (spec, ps) in &docs {
            for p in ps {
                assert!(
                    all.contains(&(*spec, p)),
                    "{spec} spec documents {p}, which is not served"
                );
            }
        }
    }

    #[test]
    fn compat_doc_lists_every_compat_path() {
        let doc = parse(&compat_json());
        assert!(doc["openapi"].as_str().unwrap().starts_with("3.1"));
        let want: BTreeSet<String> = [
            "/txs/tx",
            "/txs/txs",
            "/txs/account/{account}",
            "/txs/events",
            "/txs/recent",
            "/txs/search",
            "/coins",
            "/stats",
            "/v1/txs",
            "/v1/txs/{account}/{namespace}/{limit}",
        ]
        .map(String::from)
        .into();
        assert_eq!(paths(&doc), want);
    }

    /// Every operation: GET only, summary, operationId, a 200 and the shared failure answers.
    #[test]
    fn compat_operations_are_complete() {
        let doc = parse(&compat_json());
        let mut ids = BTreeSet::new();
        assert_eq!(doc["paths"].as_object().unwrap().len(), 10);
        for (path, item) in doc["paths"].as_object().unwrap() {
            let ops = item.as_object().unwrap();
            assert_eq!(ops.keys().collect::<Vec<_>>(), ["get"], "{path}");
            let op = &ops["get"];
            assert!(
                op["summary"].as_str().is_some_and(|s| !s.is_empty()),
                "{path}"
            );
            assert!(
                op["description"].as_str().is_some_and(|s| s.len() > 40),
                "{path}"
            );
            let id = op["operationId"]
                .as_str()
                .unwrap_or_else(|| panic!("{path} has no operationId"));
            assert!(ids.insert(id.to_owned()), "operationId {id} repeats");
            let r = op["responses"].as_object().unwrap();
            assert!(r.contains_key("200"), "{path}");
            for code in ["429", "503", "504"] {
                assert!(r.contains_key(code), "{path} lacks {code}");
            }
            for (code, resp) in r {
                assert!(
                    resp["description"].as_str().is_some_and(|d| !d.is_empty()),
                    "{path} {code}"
                );
            }
            for p in op["parameters"].as_array().into_iter().flatten() {
                assert!(
                    p["description"].as_str().is_some_and(|d| !d.is_empty()),
                    "{path}: parameter {} has no description",
                    p["name"]
                );
            }
        }
    }

    fn op<'a>(doc: &'a Value, path: &str) -> &'a Value {
        &doc["paths"][path]["get"]
    }

    fn param_names(op: &Value) -> BTreeSet<String> {
        op["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn compat_parameters_match_the_handlers() {
        let doc = parse(&compat_json());
        let set = |v: &[&str]| -> BTreeSet<String> { v.iter().map(|s| s.to_string()).collect() };
        let paging = [
            "limit",
            "offset",
            "next",
            "minheight",
            "maxheight",
            "Chainweb-Execution-Strategy",
        ];
        let with = |extra: &[&str]| set(&[extra, &paging[..]].concat());
        assert_eq!(param_names(op(&doc, "/txs/tx")), set(&["requestkey"]));
        assert_eq!(param_names(op(&doc, "/txs/txs")), set(&["requestkey"]));
        assert_eq!(
            param_names(op(&doc, "/txs/account/{account}")),
            with(&["account", "token", "chain"])
        );
        assert_eq!(
            param_names(op(&doc, "/txs/events")),
            with(&["search", "param", "name", "modulename"])
        );
        assert_eq!(
            param_names(op(&doc, "/txs/search")),
            with(&["search", "pactid"])
        );
        assert_eq!(
            param_names(op(&doc, "/v1/txs")),
            set(&["account", "namespace", "limit"])
        );
        assert_eq!(
            param_names(op(&doc, "/v1/txs/{account}/{namespace}/{limit}")),
            set(&["account", "namespace", "limit"])
        );
        assert!(op(&doc, "/txs/recent")
            .get("parameters")
            .is_none_or(|p| p.as_array().unwrap().is_empty()));
        assert!(op(&doc, "/coins")
            .get("parameters")
            .is_none_or(|p| p.as_array().unwrap().is_empty()));
        assert!(op(&doc, "/stats")
            .get("parameters")
            .is_none_or(|p| p.as_array().unwrap().is_empty()));
    }

    #[test]
    fn compat_limits_are_the_implemented_ones() {
        let doc = parse(&compat_json());
        let param = |path: &str, name: &str| -> Value {
            op(&doc, path)["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["name"] == name)
                .unwrap()
                .clone()
        };
        for path in ["/txs/account/{account}", "/txs/events", "/txs/search"] {
            let l = param(path, "limit");
            assert_eq!(l["schema"]["default"], 10, "{path}");
            assert!(l["description"].as_str().unwrap().contains("250"), "{path}");
            assert_eq!(param(path, "offset")["schema"]["maximum"], 10_000, "{path}");
        }
        let l = param("/v1/txs", "limit");
        let d = l["description"].as_str().unwrap();
        assert!(d.contains("50") && d.contains("10,000"), "{d}");
        let s = param("/txs/account/{account}", "token");
        assert!(s["description"]
            .as_str()
            .unwrap()
            .contains("default `coin`"));
    }

    #[test]
    fn compat_headers_are_documented() {
        let doc = parse(&compat_json());
        let hdrs = |path: &str| -> BTreeSet<String> {
            op(&doc, path)["responses"]
                .as_object()
                .unwrap()
                .values()
                .flat_map(|r| {
                    r["headers"]
                        .as_object()
                        .into_iter()
                        .flat_map(|h| h.keys().cloned())
                })
                .collect()
        };
        for path in ["/txs/account/{account}", "/txs/events", "/txs/search"] {
            assert!(hdrs(path).contains("Chainweb-Next"), "{path}");
        }
        assert!(hdrs("/txs/search").contains("X-Search-Truncated"));
        assert!(hdrs("/stats").contains("X-Count-Age"));
        assert!(hdrs("/stats").contains("Retry-After"));
        assert!(
            hdrs("/txs/events").contains("Retry-After"),
            "429 and 503 carry it"
        );
    }

    #[test]
    fn compat_error_shapes_are_plain_text() {
        let doc = parse(&compat_json());
        let r = &op(&doc, "/txs/tx")["responses"];
        assert!(r["404"]["content"]["text/plain"].is_object());
        assert!(r["504"]["content"]["text/plain"].is_object());
        let stats = &op(&doc, "/stats")["responses"];
        assert!(stats["503"]["content"]["text/plain"].is_object());
        assert!(stats["404"].is_object(), "Server database is empty");
        let coins = &op(&doc, "/coins")["responses"];
        assert!(coins["200"]["content"]["text/plain"].is_object());
        assert!(coins["404"].is_object());
    }

    #[test]
    fn compat_success_bodies_are_the_documented_records() {
        let doc = parse(&compat_json());
        let item_ref = |path: &str| -> String {
            let s = &op(&doc, path)["responses"]["200"]["content"]["application/json"]["schema"];
            s["items"]["$ref"]
                .as_str()
                .unwrap_or_else(|| panic!("{path}: {s}"))
                .to_owned()
        };
        assert!(item_ref("/txs/txs").ends_with("/TxDetail"));
        assert!(item_ref("/txs/account/{account}").ends_with("/TransferDetail"));
        assert!(item_ref("/txs/events").ends_with("/EventDetail"));
        assert!(item_ref("/txs/recent").ends_with("/TxSummary"));
        assert!(item_ref("/txs/search").ends_with("/TxSummary"));
        assert!(item_ref("/v1/txs").ends_with("/TxDetail"));
        let one = &op(&doc, "/txs/tx")["responses"]["200"]["content"]["application/json"]["schema"];
        assert!(one["$ref"].as_str().unwrap().ends_with("/TxDetail"));
        let st = &op(&doc, "/stats")["responses"]["200"]["content"]["application/json"]["schema"];
        assert!(st["$ref"].as_str().unwrap().ends_with("/Stats"));
        let tx = &doc["components"]["schemas"]["TxDetail"]["properties"];
        for f in [
            "requestKey",
            "blockHash",
            "chain",
            "height",
            "success",
            "events",
            "signers",
            "sigs",
            "initialCode",
            "previousSteps",
            "creationTime",
        ] {
            assert!(tx.get(f).is_some(), "TxDetail.{f}");
        }
    }

    /// Every `$ref` of both documents points at a schema that exists.
    #[test]
    fn every_ref_resolves() {
        fn walk(v: &Value, doc: &Value, n: &mut usize) {
            match v {
                Value::Object(m) => {
                    for (k, x) in m {
                        if k == "$ref" {
                            let p = x.as_str().unwrap().strip_prefix('#').unwrap();
                            assert!(doc.pointer(p).is_some(), "dangling {p}");
                            *n += 1;
                        } else {
                            walk(x, doc, n);
                        }
                    }
                }
                Value::Array(a) => a.iter().for_each(|x| walk(x, doc, n)),
                _ => {}
            }
        }
        let mut n = 0;
        for s in [json(), compat_json()] {
            let doc = parse(&s);
            walk(&doc, &doc, &mut n);
        }
        assert!(n > 10, "only {n} refs");
    }

    /// Same origin: no server entry names a host, and no document carries a URL.
    #[test]
    fn specs_name_no_host() {
        for s in [json(), compat_json()] {
            let doc = parse(&s);
            for sv in doc["servers"].as_array().expect("servers") {
                assert_eq!(sv["url"], "/", "try it out must target the same origin");
            }
            assert!(
                !s.contains("http://") && !s.contains("https://"),
                "a spec names a URL"
            );
            for bad in [concat!("/Us", "ers/"), concat!("/Vol", "umes/"), concat!("/ho", "me/")] {
                assert!(!s.contains(bad), "{bad}");
            }
        }
    }
}
