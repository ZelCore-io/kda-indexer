//! `kda-indexer openapi` prints the committed OpenAPI documents (no config, no data dir), and
//! the package ships both next to each other.

use std::process::Command;

fn print(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_kda-indexer"))
        .args(args)
        .output()
        .expect("run kda-indexer");
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8")
}

#[test]
fn openapi_prints_the_native_document() {
    let want = include_str!("../../../docs/openapi.json");
    assert_eq!(print(&["openapi"]).trim_end(), want.trim_end());
}

#[test]
fn openapi_compat_prints_the_compat_document() {
    let want = include_str!("../../../docs/openapi-compat.json");
    let got = print(&["openapi", "--compat"]);
    assert_eq!(got.trim_end(), want.trim_end());
    assert!(got.contains("/txs/search") && got.contains("/v1/txs"));
}

#[test]
fn the_deb_ships_both_documents_and_the_swagger_licence() {
    let manifest = include_str!("../Cargo.toml");
    for f in [
        "docs/openapi.json",
        "docs/openapi-compat.json",
        "swagger-ui/LICENSE",
        "swagger-ui/NOTICE",
    ] {
        assert!(manifest.contains(f), "the deb assets lack {f}");
    }
}
