//! Property tests for the `/local` request builder and its input checks (plan Task 13, T-1,
//! rule B-1): hostile names are data, never code; the module-name check and the decimal check
//! accept exactly the documented sets (compared with regexes written from the docs).

#[macro_use]
#[path = "../../kdx-types/tests/common/prop.rs"]
mod prop;
use prop::*;

use kdx_node::local::{
    command, decimal_text, is_decimal, valid_module, GAS_LIMIT, MAX_DECIMAL_LEN,
};
use serde_json::{json, Map, Value};

/// The four fixed code forms of B-1, with the module they are built for.
fn forms(module: &str) -> [(String, &'static [&'static str]); 4] {
    [
        (format!("({module}.details (read-msg \"a\"))"), &["a"]),
        ("(describe-keyset (read-msg \"k\"))".to_string(), &["k"]),
        (
            format!("({module}.get-balance (read-msg \"t\") (read-msg \"a\"))"),
            &["t", "a"],
        ),
        (
            "(contains \"fungible-v2\" (at 'interfaces (describe-module (read-msg \"m\"))))"
                .to_string(),
            &["m"],
        ),
    ]
}

fn hostile() -> impl Strategy<Value = String> {
    prop_oneof![
        "\\PC{0,40}",
        proptest::sample::select(vec![
            "x\") (coin.transfer",
            "\"}}},\"signers\":[{\"pubKey\":\"a\"}],\"x\":{",
            "\\\"\\\\",
            "k:\u{2028}\u{0}\n\t",
            "a\"); (enforce false \"",
            "",
        ])
        .prop_map(String::from),
    ]
}

fn keys_of(v: &Value) -> Vec<&str> {
    let mut k: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    k.sort_unstable();
    k
}

props! {
    fn hostile_strings_appear_only_in_data(
        network in hostile(),
        chain in any::<u8>(),
        which in 0usize..4,
        vals in proptest::collection::vec(hostile(), 2),
        now in any::<u32>(),
    ) {
        let module = "coin";
        let (code, names) = forms(module).into_iter().nth(which).unwrap();
        let mut data = Map::new();
        for (i, n) in names.iter().enumerate() {
            data.insert((*n).to_string(), Value::String(vals[i].clone()));
        }
        let data = Value::Object(data);
        let body = command(&network, chain, &code, &data, now as u64);
        let cmd_text = body["cmd"].as_str().unwrap();
        let cmd: Value = serde_json::from_str(cmd_text).unwrap();
        // The code field is the fixed form, byte for byte.
        prop_assert_eq!(cmd.pointer("/payload/exec/code").and_then(Value::as_str), Some(code.as_str()));
        // The strings are in `data` and nowhere else: the structure is exactly the fixed one.
        prop_assert_eq!(cmd.pointer("/payload/exec/data"), Some(&data));
        prop_assert_eq!(keys_of(&cmd), vec!["meta", "networkId", "nonce", "payload", "signers"]);
        prop_assert_eq!(keys_of(&cmd["payload"]), vec!["exec"]);
        prop_assert_eq!(keys_of(&cmd["payload"]["exec"]), vec!["code", "data"]);
        prop_assert_eq!(
            keys_of(&cmd["meta"]),
            vec!["chainId", "creationTime", "gasLimit", "gasPrice", "sender", "ttl"]
        );
        prop_assert_eq!(&cmd["signers"], &json!([]));
        prop_assert_eq!(cmd["meta"]["gasLimit"].as_u64(), Some(GAS_LIMIT));
        prop_assert_eq!(cmd["meta"]["sender"].as_str(), Some(""));
        prop_assert_eq!(cmd["networkId"].as_str(), Some(network.as_str()));
        prop_assert_eq!(cmd["meta"]["chainId"].as_str().map(str::to_owned), Some(chain.to_string()));
        prop_assert_eq!(body["sigs"].as_array().map(Vec::len), Some(0));
        // The hash field is 43 base64url characters (32 bytes, no padding).
        let hash = body["hash"].as_str().unwrap();
        prop_assert_eq!(hash.len(), 43);
        prop_assert!(hash.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    }

    fn module_check_equals_the_documented_regex(m in "[A-Za-z0-9_.\\- \"()\\n\u{e9}]{0,14}") {
        // B-1: `^[A-Za-z_][A-Za-z0-9_\-]*(\.[A-Za-z_][A-Za-z0-9_\-]*)?$`.
        let re = regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_\-]*(\.[A-Za-z_][A-Za-z0-9_\-]*)?$").unwrap();
        prop_assert_eq!(valid_module(&m), re.is_match(&m), "module {:?}", m);
    }

    fn module_check_accepts_every_generated_member(
        ns in proptest::option::of("[A-Za-z_][A-Za-z0-9_\\-]{0,10}"),
        name in "[A-Za-z_][A-Za-z0-9_\\-]{0,10}",
    ) {
        let m = match ns {
            Some(ns) => [ns.as_str(), ".", name.as_str()].concat(),
            None => name,
        };
        prop_assert!(valid_module(&m), "{}", m);
    }

    fn decimal_check_equals_the_documented_grammar(s in "[0-9eE+\\-. a]{0,20}") {
        let re = regex::Regex::new(r"^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$").unwrap();
        prop_assert_eq!(is_decimal(&s), re.is_match(&s), "text {:?}", s);
        // A string is only ever read as a balance when it passes the check.
        prop_assert_eq!(decimal_text(&Value::String(s.clone())).is_some(), is_decimal(&s));
    }

    fn decimal_length_is_capped(extra in 0usize..40) {
        let ok = "9".repeat(MAX_DECIMAL_LEN);
        prop_assert!(is_decimal(&ok));
        prop_assert!(!is_decimal(&"9".repeat(MAX_DECIMAL_LEN + 1 + extra)));
    }
}
