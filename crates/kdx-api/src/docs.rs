//! Swagger UI and the two OpenAPI documents, served by the indexer itself.
//!
//! `GET /docs` is a Swagger UI page with a switcher between the native API (`/api/v1`) and
//! the chainweb-data compatible API; `/docs/openapi.json` and `/docs/openapi-compat.json`
//! are the generated documents (the same bytes as `docs/*.json`, pinned by tests). The UI is
//! `swagger-ui-dist` (Apache-2.0), vendored under `assets/swagger-ui/` and embedded with
//! `include_bytes!`: nothing is fetched from outside, and the page's CSP allows only its own
//! origin. The routes are added to the native and compat listeners by `Api::serve` unless
//! `api.docs_enabled` is false; the admin (metrics) and GraphQL listeners never serve them.
//! Read-only: no store access.

use std::sync::OnceLock;

use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::get;
use axum::Router;

/// Which API the page opens on (the listener it is served from).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Primary {
    Native,
    Compat,
}

/// Content security policy of every `/docs` response: own origin only, no inline script,
/// no eval, no framing. Swagger UI sets inline styles, hence `style-src 'unsafe-inline'`.
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; \
base-uri 'none'; form-action 'none'";

const INDEX: &str = include_str!("../assets/index.html");
const INITIALIZER: &str = include_str!("../assets/swagger-initializer.js");

const BUNDLE: &[u8] = include_bytes!("../assets/swagger-ui/swagger-ui-bundle.js");
const PRESET: &[u8] = include_bytes!("../assets/swagger-ui/swagger-ui-standalone-preset.js");
const CSS: &[u8] = include_bytes!("../assets/swagger-ui/swagger-ui.css");
const FAVICON: &[u8] = include_bytes!("../assets/swagger-ui/favicon-32x32.png");
const LICENSE: &str = include_str!("../assets/swagger-ui/LICENSE");
const NOTICE: &str = include_str!("../assets/swagger-ui/NOTICE");

/// The documentation routes, for the listener `primary` names. Merge into a data listener's
/// router.
pub fn router<S>(primary: Primary) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let page = move || async move { page(primary) };
    Router::new()
        .route("/docs", get(page))
        .route("/docs/", get(page))
        .route("/docs/{*file}", get(file))
}

fn page(primary: Primary) -> Response {
    let which = match primary {
        Primary::Native => "native",
        Primary::Compat => "compat",
    };
    reply(
        "text/html; charset=utf-8",
        "no-cache",
        INDEX.replace("{{PRIMARY}}", which).into_bytes(),
    )
}

/// A document the router serves under `/docs/{name}`: its type and bytes.
fn document(name: &str) -> Option<(&'static str, &'static [u8])> {
    Some(match name {
        "swagger-initializer.js" => ("text/javascript; charset=utf-8", INITIALIZER.as_bytes()),
        "swagger-ui-bundle.js" => ("text/javascript; charset=utf-8", BUNDLE),
        "swagger-ui-standalone-preset.js" => ("text/javascript; charset=utf-8", PRESET),
        "swagger-ui.css" => ("text/css; charset=utf-8", CSS),
        "favicon-32x32.png" => ("image/png", FAVICON),
        "swagger-ui-LICENSE.txt" => ("text/plain; charset=utf-8", LICENSE.as_bytes()),
        "swagger-ui-NOTICE.txt" => ("text/plain; charset=utf-8", NOTICE.as_bytes()),
        "openapi.json" => ("application/json", native_spec().as_bytes()),
        "openapi-compat.json" => ("application/json", compat_spec().as_bytes()),
        _ => return None,
    })
}

/// The entity tag of a document: FNV-1a over its bytes, computed once per name. The files
/// are fixed by the build, so the tag changes exactly when an upgrade changes the file.
fn etag(name: &str, bytes: &[u8]) -> String {
    static TAGS: OnceLock<std::sync::Mutex<std::collections::HashMap<String, String>>> =
        OnceLock::new();
    let mut tags = TAGS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    tags.entry(name.to_owned())
        .or_insert_with(|| {
            let h = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
            });
            format!("\"{h:016x}-{:x}\"", bytes.len())
        })
        .clone()
}

/// Whether `If-None-Match` names `tag` (or `*`).
fn matches(headers: &HeaderMap, tag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().trim_start_matches("W/"))
        .any(|t| t == "*" || t == tag)
}

async fn file(Path(name): Path<String>, headers: HeaderMap) -> Response {
    // Every document is revalidated, never cached blind: the files follow the binary, so
    // an upgrade shows at once, and an unchanged file costs a 304 and no body.
    let Some((content_type, bytes)) = document(&name) else {
        let mut r = reply("text/plain; charset=utf-8", "no-store", "not found");
        *r.status_mut() = StatusCode::NOT_FOUND;
        return r;
    };
    let tag = etag(&name, bytes);
    let mut r = if matches(&headers, &tag) {
        let mut r = reply(content_type, "no-cache", Body::empty());
        *r.status_mut() = StatusCode::NOT_MODIFIED;
        r
    } else {
        reply(content_type, "no-cache", bytes)
    };
    if let Ok(v) = HeaderValue::from_str(&tag) {
        r.headers_mut().insert(header::ETAG, v);
    }
    r
}

fn native_spec() -> &'static str {
    static S: OnceLock<String> = OnceLock::new();
    S.get_or_init(crate::openapi::json)
}

fn compat_spec() -> &'static str {
    static S: OnceLock<String> = OnceLock::new();
    S.get_or_init(crate::openapi::compat_json)
}

fn reply(content_type: &'static str, cache: &'static str, body: impl Into<Body>) -> Response {
    let mut r = Response::new(body.into());
    let h = r.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    r
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::Response;
    use regex::Regex;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;
    use crate::server::{Api, Binds, TipWatch};

    async fn get(primary: Primary, path: &str) -> Response {
        router::<()>(primary)
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body(r: Response) -> Vec<u8> {
        axum::body::to_bytes(r.into_body(), 16 << 20)
            .await
            .unwrap()
            .to_vec()
    }

    async fn text(r: Response) -> String {
        String::from_utf8(body(r).await).unwrap()
    }

    fn header_of<'a>(r: &'a Response, name: &str) -> &'a str {
        r.headers()
            .get(name)
            .unwrap_or_else(|| panic!("header {name} missing"))
            .to_str()
            .unwrap()
    }

    #[tokio::test]
    async fn docs_page_is_html_under_a_strict_csp() {
        for path in ["/docs", "/docs/"] {
            let r = get(Primary::Native, path).await;
            assert_eq!(r.status(), StatusCode::OK, "{path}");
            assert!(header_of(&r, "content-type").starts_with("text/html"));
            let csp = header_of(&r, "content-security-policy").to_owned();
            for want in [
                "default-src 'none'",
                "script-src 'self'",
                "connect-src 'self'",
                "img-src 'self' data:",
                "frame-ancestors 'none'",
                "base-uri 'none'",
                "form-action 'none'",
            ] {
                assert!(csp.contains(want), "CSP lacks {want}: {csp}");
            }
            for bad in ["unsafe-eval", "http:", "https:", "*"] {
                assert!(!csp.contains(bad), "CSP has {bad}: {csp}");
            }
            let script = csp
                .split(';')
                .find(|d| d.trim().starts_with("script-src"))
                .unwrap();
            assert!(!script.contains("unsafe-inline"), "{script}");
            assert_eq!(header_of(&r, "x-content-type-options"), "nosniff");
            assert_eq!(header_of(&r, "referrer-policy"), "no-referrer");
        }
    }

    #[tokio::test]
    async fn every_docs_response_carries_the_csp() {
        for path in [
            "/docs",
            "/docs/openapi.json",
            "/docs/openapi-compat.json",
            "/docs/swagger-initializer.js",
            "/docs/swagger-ui.css",
            "/docs/swagger-ui-bundle.js",
            "/docs/swagger-ui-standalone-preset.js",
            "/docs/favicon-32x32.png",
            "/docs/nothing-here",
        ] {
            let r = get(Primary::Native, path).await;
            assert!(
                r.headers().contains_key("content-security-policy"),
                "{path} ({}) has no CSP",
                r.status()
            );
        }
    }

    #[tokio::test]
    async fn the_page_names_its_primary_spec() {
        let n = text(get(Primary::Native, "/docs").await).await;
        let c = text(get(Primary::Compat, "/docs").await).await;
        assert!(n.contains(r#"data-primary="native""#), "{n}");
        assert!(c.contains(r#"data-primary="compat""#), "{c}");
    }

    #[tokio::test]
    async fn the_page_pulls_in_nothing_from_outside() {
        let html = text(get(Primary::Native, "/docs").await).await;
        assert!(
            !html.contains("http://") && !html.contains("https://"),
            "{html}"
        );
        let attr = Regex::new(r#"(?:src|href)\s*=\s*"([^"]*)""#).unwrap();
        let mut n = 0;
        for c in attr.captures_iter(&html) {
            n += 1;
            assert!(c[1].starts_with("/docs/"), "not served by us: {}", &c[1]);
        }
        assert!(n >= 4, "expected the css, two scripts and an icon, got {n}");
        assert!(!html.contains("<script>"), "no inline script under the CSP");
        assert!(!html.contains("onload="), "no inline handler under the CSP");
    }

    #[tokio::test]
    async fn every_asset_the_page_names_is_served() {
        let html = text(get(Primary::Native, "/docs").await).await;
        let attr = Regex::new(r#"(?:src|href)\s*=\s*"([^"]*)""#).unwrap();
        for c in attr.captures_iter(&html) {
            let r = get(Primary::Native, &c[1]).await;
            assert_eq!(r.status(), StatusCode::OK, "{}", &c[1]);
            let ct = header_of(&r, "content-type").to_owned();
            let want = match c[1].rsplit('.').next().unwrap() {
                "js" => "javascript",
                "css" => "text/css",
                "png" => "image/png",
                other => panic!("unexpected asset type {other}"),
            };
            assert!(ct.contains(want), "{}: {ct}", &c[1]);
            assert!(!body(r).await.is_empty(), "{}", &c[1]);
        }
    }

    #[tokio::test]
    async fn the_initializer_offers_both_specs_and_no_validator() {
        let js = text(get(Primary::Native, "/docs/swagger-initializer.js").await).await;
        assert!(js.contains("/docs/openapi.json"), "{js}");
        assert!(js.contains("/docs/openapi-compat.json"), "{js}");
        assert!(js.contains("validatorUrl: null"), "{js}");
        assert!(js.contains("StandaloneLayout"), "{js}");
        assert!(!js.contains("http://") && !js.contains("https://"), "{js}");
    }

    /// M4: the spec of the other listener is offered, labelled: its "Try it out" calls this
    /// origin, which does not serve those routes.
    #[tokio::test]
    async fn the_initializer_labels_the_spec_of_the_other_listener() {
        let js = text(get(Primary::Compat, "/docs/swagger-initializer.js").await).await;
        assert!(js.contains("other listener"), "{js}");
        assert!(js.contains("data-primary"), "{js}");
    }

    /// M1: assets are revalidated with an `ETag` (a 304 costs no body) instead of cached
    /// for a day, so an upgrade never leaves a browser with a mismatched bundle.
    #[tokio::test]
    async fn assets_revalidate_with_an_etag() {
        for path in [
            "/docs/swagger-ui-bundle.js",
            "/docs/swagger-ui-standalone-preset.js",
            "/docs/swagger-ui.css",
            "/docs/swagger-initializer.js",
            "/docs/favicon-32x32.png",
            "/docs/swagger-ui-LICENSE.txt",
            "/docs/openapi.json",
            "/docs/openapi-compat.json",
        ] {
            let r = get(Primary::Native, path).await;
            assert_eq!(header_of(&r, "cache-control"), "no-cache", "{path}");
            let etag = header_of(&r, "etag").to_owned();
            assert!(
                etag.starts_with('"') && etag.ends_with('"'),
                "{path}: {etag}"
            );
            let full = body(r).await;
            let cond = |tag: &str| {
                router::<()>(Primary::Native).oneshot(
                    Request::get(path)
                        .header("if-none-match", tag.to_owned())
                        .body(Body::empty())
                        .unwrap(),
                )
            };
            let r = cond(&etag).await.unwrap();
            assert_eq!(r.status(), StatusCode::NOT_MODIFIED, "{path}");
            assert_eq!(header_of(&r, "etag"), etag, "{path}");
            assert!(header_of(&r, "content-security-policy").contains("default-src"));
            assert!(body(r).await.is_empty(), "{path}");
            let r = cond("\"other\"").await.unwrap();
            assert_eq!(r.status(), StatusCode::OK, "{path}");
            assert_eq!(body(r).await, full, "{path}");
            // The same bytes, the same tag.
            let again = get(Primary::Native, path).await;
            assert_eq!(header_of(&again, "etag"), etag, "{path}");
        }
        // Unknown names carry no validator.
        let r = get(Primary::Native, "/docs/nothing-here").await;
        assert!(r.headers().get("etag").is_none());
    }

    #[tokio::test]
    async fn served_specs_are_the_generated_ones() {
        let r = get(Primary::Native, "/docs/openapi.json").await;
        assert_eq!(r.status(), StatusCode::OK);
        assert!(header_of(&r, "content-type").starts_with("application/json"));
        assert_eq!(text(r).await, crate::openapi::json());
        let r = get(Primary::Compat, "/docs/openapi-compat.json").await;
        assert_eq!(r.status(), StatusCode::OK);
        assert!(header_of(&r, "content-type").starts_with("application/json"));
        assert_eq!(text(r).await, crate::openapi::compat_json());
    }

    #[tokio::test]
    async fn both_listeners_serve_both_specs() {
        for p in [Primary::Native, Primary::Compat] {
            for path in ["/docs/openapi.json", "/docs/openapi-compat.json"] {
                assert_eq!(get(p, path).await.status(), StatusCode::OK, "{p:?} {path}");
            }
        }
    }

    #[tokio::test]
    async fn unknown_paths_and_other_methods_are_refused() {
        assert_eq!(
            get(Primary::Native, "/docs/nothing-here").await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            get(Primary::Native, "/docs/../etc/passwd").await.status(),
            StatusCode::NOT_FOUND
        );
        let r = router::<()>(Primary::Native)
            .oneshot(Request::post("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn bundled_version_is_the_pinned_one() {
        let js = body(get(Primary::Native, "/docs/swagger-ui-bundle.js").await).await;
        let js = String::from_utf8_lossy(&js);
        assert!(js.contains("5.34.1"), "swagger-ui-dist 5.34.1 expected");
        let lic = text(get(Primary::Native, "/docs/swagger-ui-LICENSE.txt").await).await;
        assert!(
            lic.contains("Apache License"),
            "{}",
            &lic[..80.min(lic.len())]
        );
        let notice = text(get(Primary::Native, "/docs/swagger-ui-NOTICE.txt").await).await;
        assert!(notice.contains("SmartBear"), "{notice}");
    }

    // ---- through the real listeners ----

    async fn get_status(addr: SocketAddr, path: &str) -> u16 {
        let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Write};
            let mut c =
                std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap();
            c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            c.write_all(req.as_bytes()).unwrap();
            let mut head = Vec::new();
            let _ = c.read_to_end(&mut head);
            String::from_utf8_lossy(&head)[9..12].parse().unwrap()
        })
        .await
        .unwrap()
    }

    /// Serve every listener on loopback and return (native, compat, graphql, admin).
    async fn serve(
        docs: Option<bool>,
    ) -> (
        tempfile::TempDir,
        CancellationToken,
        tokio::task::JoinHandle<anyhow::Result<()>>,
        [SocketAddr; 4],
    ) {
        serve_with(docs, crate::server::tests::settings(4)).await
    }

    async fn serve_with(
        docs: Option<bool>,
        settings: crate::server::ApiSettings,
    ) -> (
        tempfile::TempDir,
        CancellationToken,
        tokio::task::JoinHandle<anyhow::Result<()>>,
        [SocketAddr; 4],
    ) {
        let (d, store) = crate::testutil::store();
        let lo: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let binds = Binds {
            native: vec![lo],
            compat: vec![lo],
            graphql: vec![lo],
            admin: vec![lo],
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut api = Api::new(store, settings, TipWatch::default()).on_bound(move |a| {
            let _ = tx.send(a.to_vec());
        });
        if let Some(on) = docs {
            api = api.docs_enabled(on);
        }
        let stop = CancellationToken::new();
        let task = tokio::spawn(api.serve(binds, stop.clone()));
        let bound = rx.await.unwrap();
        let of = |n: &str| bound.iter().find(|b| b.0 == n).unwrap().1;
        let addrs = [of("native"), of("compat"), of("graphql"), of("admin")];
        (d, stop, task, addrs)
    }

    #[tokio::test]
    async fn listeners_serve_docs_by_default_but_not_graphql_or_admin() {
        let (_d, stop, task, [native, compat, graphql, admin]) = serve(None).await;
        for a in [native, compat] {
            assert_eq!(get_status(a, "/docs").await, 200, "{a}");
            assert_eq!(get_status(a, "/docs/openapi.json").await, 200, "{a}");
            assert_eq!(get_status(a, "/docs/openapi-compat.json").await, 200, "{a}");
            assert_eq!(
                get_status(a, "/docs/swagger-ui-bundle.js").await,
                200,
                "{a}"
            );
        }
        assert_eq!(get_status(graphql, "/docs").await, 404);
        assert_eq!(
            get_status(admin, "/docs").await,
            404,
            "never on the metrics port"
        );
        assert_eq!(get_status(admin, "/docs/openapi.json").await, 404);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    /// S3: with `api.max_body_mb = 1` the 1.7 MB bundle is still served on a real listener.
    #[tokio::test]
    async fn the_bundle_is_served_under_a_one_megabyte_body_cap() {
        let mut cfg = crate::server::tests::settings(4);
        cfg.max_body_bytes = 1 << 20;
        cfg.v1txs_max_body_bytes = 1 << 20;
        assert!(
            super::BUNDLE.len() > 1 << 20,
            "the bundle fits under the cap: the test proves nothing"
        );
        let (_d, stop, task, [native, compat, ..]) = serve_with(None, cfg).await;
        for a in [native, compat] {
            for p in [
                "/docs/swagger-ui-bundle.js",
                "/docs/swagger-ui-standalone-preset.js",
            ] {
                assert_eq!(get_status(a, p).await, 200, "{a} {p}");
            }
        }
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn docs_disabled_answers_404_on_every_listener() {
        let (_d, stop, task, [native, compat, graphql, admin]) = serve(Some(false)).await;
        for a in [native, compat, graphql, admin] {
            for p in [
                "/docs",
                "/docs/",
                "/docs/openapi.json",
                "/docs/swagger-ui.css",
            ] {
                assert_eq!(get_status(a, p).await, 404, "{a} {p}");
            }
        }
        // The other routes are not affected.
        assert_ne!(get_status(native, "/status").await, 404);
        assert_eq!(get_status(admin, "/healthz").await, 200);
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}
