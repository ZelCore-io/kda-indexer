//! Relay node ids (GQ-7): Kadena graph's global id format, standard base64 (with padding)
//! of `<TypeName>:<key>`. The key is the natural key of the type: the block hash for
//! `Block`, the account name for `NonFungibleAccount`, else a compact JSON array of the key
//! fields in Kadena graph's order. The deployed oracle writes the numeric elements (order
//! index, chain) as JSON strings (`["hash","1","rk"]`, R-P5P9-19), so ids are encoded that
//! way; decoding takes a number or a numeric string.

use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use base64::Engine;
use serde_json::Value;

/// Longest id accepted for decoding (base64 text).
pub const MAX_ID_LEN: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKey {
    Block {
        hash: String,
    },
    Event {
        block_hash: String,
        order_index: i64,
        request_key: String,
    },
    FungibleAccount {
        fungible: String,
        account: String,
    },
    FungibleChainAccount {
        chain: String,
        fungible: String,
        account: String,
    },
    NonFungibleAccount {
        account: String,
    },
    NonFungibleChainAccount {
        chain: String,
        account: String,
    },
    NonFungibleTokenBalance {
        token_id: String,
        account: String,
        chain: String,
    },
    Signer {
        request_key: String,
        order_index: i64,
    },
    Transaction {
        block_hash: String,
        request_key: String,
    },
    Transfer {
        block_hash: String,
        chain: i64,
        order_index: i64,
        module_hash: String,
        request_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdError;

impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid node id")
    }
}

impl std::error::Error for IdError {}

impl NodeKey {
    /// The GraphQL type name.
    pub fn type_name(&self) -> &'static str {
        match self {
            NodeKey::Block { .. } => "Block",
            NodeKey::Event { .. } => "Event",
            NodeKey::FungibleAccount { .. } => "FungibleAccount",
            NodeKey::FungibleChainAccount { .. } => "FungibleChainAccount",
            NodeKey::NonFungibleAccount { .. } => "NonFungibleAccount",
            NodeKey::NonFungibleChainAccount { .. } => "NonFungibleChainAccount",
            NodeKey::NonFungibleTokenBalance { .. } => "NonFungibleTokenBalance",
            NodeKey::Signer { .. } => "Signer",
            NodeKey::Transaction { .. } => "Transaction",
            NodeKey::Transfer { .. } => "Transfer",
        }
    }

    fn key_text(&self) -> String {
        let s = |v: &str| Value::String(v.to_owned());
        let n = |v: i64| Value::String(v.to_string());
        let arr = |v: Vec<Value>| Value::Array(v).to_string();
        match self {
            NodeKey::Block { hash } => hash.clone(),
            NodeKey::NonFungibleAccount { account } => account.clone(),
            NodeKey::Event {
                block_hash,
                order_index,
                request_key,
            } => arr(vec![s(block_hash), n(*order_index), s(request_key)]),
            NodeKey::FungibleAccount { fungible, account } => arr(vec![s(fungible), s(account)]),
            NodeKey::FungibleChainAccount {
                chain,
                fungible,
                account,
            } => arr(vec![s(chain), s(fungible), s(account)]),
            NodeKey::NonFungibleChainAccount { chain, account } => arr(vec![s(chain), s(account)]),
            NodeKey::NonFungibleTokenBalance {
                token_id,
                account,
                chain,
            } => arr(vec![s(token_id), s(account), s(chain)]),
            NodeKey::Signer {
                request_key,
                order_index,
            } => arr(vec![s(request_key), n(*order_index)]),
            NodeKey::Transaction {
                block_hash,
                request_key,
            } => arr(vec![s(block_hash), s(request_key)]),
            NodeKey::Transfer {
                block_hash,
                chain,
                order_index,
                module_hash,
                request_key,
            } => arr(vec![
                s(block_hash),
                n(*chain),
                n(*order_index),
                s(module_hash),
                s(request_key),
            ]),
        }
    }
}

/// The id of a node.
pub fn encode(key: &NodeKey) -> String {
    STANDARD.encode(format!("{}:{}", key.type_name(), key.key_text()))
}

/// Parse an id; any malformed or unknown id is an `IdError` (never a panic).
pub fn decode(id: &str) -> Result<NodeKey, IdError> {
    if id.is_empty() || id.len() > MAX_ID_LEN {
        return Err(IdError);
    }
    let raw = STANDARD
        .decode(id)
        .or_else(|_| STANDARD_NO_PAD.decode(id.trim_end_matches('=')))
        .map_err(|_| IdError)?;
    let text = String::from_utf8(raw).map_err(|_| IdError)?;
    let (ty, key) = text.split_once(':').ok_or(IdError)?;
    if key.is_empty() {
        return Err(IdError);
    }
    let parts = || -> Result<Vec<Value>, IdError> {
        match serde_json::from_str::<Value>(key) {
            Ok(Value::Array(v)) => Ok(v),
            _ => Err(IdError),
        }
    };
    let s = |v: &Value| v.as_str().map(str::to_owned).ok_or(IdError);
    // A number or a numeric string (the oracle's form).
    let n = |v: &Value| match v {
        Value::String(t) => t.parse::<i64>().map_err(|_| IdError),
        v => v.as_i64().ok_or(IdError),
    };
    let k = match ty {
        "Block" => NodeKey::Block {
            hash: key.to_owned(),
        },
        "NonFungibleAccount" => NodeKey::NonFungibleAccount {
            account: key.to_owned(),
        },
        _ => {
            let p = parts()?;
            match (ty, p.as_slice()) {
                ("Event", [a, b, c]) => NodeKey::Event {
                    block_hash: s(a)?,
                    order_index: n(b)?,
                    request_key: s(c)?,
                },
                ("FungibleAccount", [a, b]) => NodeKey::FungibleAccount {
                    fungible: s(a)?,
                    account: s(b)?,
                },
                ("FungibleChainAccount", [a, b, c]) => NodeKey::FungibleChainAccount {
                    chain: s(a)?,
                    fungible: s(b)?,
                    account: s(c)?,
                },
                ("NonFungibleChainAccount", [a, b]) => NodeKey::NonFungibleChainAccount {
                    chain: s(a)?,
                    account: s(b)?,
                },
                ("NonFungibleTokenBalance", [a, b, c]) => NodeKey::NonFungibleTokenBalance {
                    token_id: s(a)?,
                    account: s(b)?,
                    chain: s(c)?,
                },
                ("Signer", [a, b]) => NodeKey::Signer {
                    request_key: s(a)?,
                    order_index: n(b)?,
                },
                ("Transaction", [a, b]) => NodeKey::Transaction {
                    block_hash: s(a)?,
                    request_key: s(b)?,
                },
                ("Transfer", [a, b, c, d, e]) => NodeKey::Transfer {
                    block_hash: s(a)?,
                    chain: n(b)?,
                    order_index: n(c)?,
                    module_hash: s(d)?,
                    request_key: s(e)?,
                },
                _ => return Err(IdError),
            }
        }
    };
    Ok(k)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all() -> Vec<NodeKey> {
        let h = "Zm9vYmFyYmF6cXV4MDEyMzQ1Njc4OWFiY2RlZmdoaWo".to_string();
        let rk = "cmVxdWVzdGtleXJlcXVlc3RrZXlyZXF1ZXN0a2V5MDE".to_string();
        vec![
            NodeKey::Block { hash: h.clone() },
            NodeKey::Event {
                block_hash: h.clone(),
                order_index: 3,
                request_key: rk.clone(),
            },
            NodeKey::FungibleAccount {
                fungible: "coin".into(),
                account: "k:ab\"c".into(),
            },
            NodeKey::FungibleChainAccount {
                chain: "1".into(),
                fungible: "coin".into(),
                account: "alice".into(),
            },
            NodeKey::NonFungibleAccount {
                account: "a:b:c".into(),
            },
            NodeKey::NonFungibleChainAccount {
                chain: "19".into(),
                account: "bob".into(),
            },
            NodeKey::NonFungibleTokenBalance {
                token_id: "t:xyz".into(),
                account: "bob".into(),
                chain: "8".into(),
            },
            NodeKey::Signer {
                request_key: rk.clone(),
                order_index: 0,
            },
            NodeKey::Transaction {
                block_hash: h.clone(),
                request_key: rk.clone(),
            },
            NodeKey::Transfer {
                block_hash: h,
                chain: 2,
                order_index: 1,
                module_hash: "mh".into(),
                request_key: rk,
            },
        ]
    }

    #[test]
    fn ids_round_trip() {
        for k in all() {
            let id = encode(&k);
            assert_eq!(decode(&id).unwrap(), k, "{id}");
        }
    }

    #[test]
    fn known_layouts() {
        let id = encode(&NodeKey::Block { hash: "abc".into() });
        assert_eq!(STANDARD.decode(&id).unwrap(), b"Block:abc");
        let id = encode(&NodeKey::Transaction {
            block_hash: "h".into(),
            request_key: "r".into(),
        });
        assert_eq!(STANDARD.decode(&id).unwrap(), br#"Transaction:["h","r"]"#);
        let id = encode(&NodeKey::Event {
            block_hash: "h".into(),
            order_index: 2,
            request_key: "r".into(),
        });
        assert_eq!(STANDARD.decode(&id).unwrap(), br#"Event:["h","2","r"]"#);
        let id = encode(&NodeKey::Transfer {
            block_hash: "h".into(),
            chain: 1,
            order_index: 0,
            module_hash: "m".into(),
            request_key: "r".into(),
        });
        assert_eq!(
            STANDARD.decode(&id).unwrap(),
            br#"Transfer:["h","1","0","m","r"]"#
        );
        let id = encode(&NodeKey::Signer {
            request_key: "r".into(),
            order_index: 3,
        });
        assert_eq!(STANDARD.decode(&id).unwrap(), br#"Signer:["r","3"]"#);
    }

    #[test]
    fn numeric_elements_decode_from_numbers_and_strings() {
        let ev = |t: &str| decode(&STANDARD.encode(t)).unwrap();
        let want = NodeKey::Event {
            block_hash: "h".into(),
            order_index: 2,
            request_key: "r".into(),
        };
        assert_eq!(ev(r#"Event:["h","2","r"]"#), want);
        assert_eq!(ev(r#"Event:["h",2,"r"]"#), want);
        assert_eq!(
            ev(r#"Transfer:["h","1",0,"m","r"]"#),
            NodeKey::Transfer {
                block_hash: "h".into(),
                chain: 1,
                order_index: 0,
                module_hash: "m".into(),
                request_key: "r".into(),
            }
        );
        assert_eq!(
            ev(r#"Signer:["r","3"]"#),
            NodeKey::Signer {
                request_key: "r".into(),
                order_index: 3
            }
        );
    }

    #[test]
    fn bad_ids_are_errors() {
        let long = "A".repeat(MAX_ID_LEN + 1);
        for bad in [
            "",
            "!!!",
            long.as_str(),
            &STANDARD.encode("Nope:x"),
            &STANDARD.encode("Block:"),
            &STANDARD.encode("Event:[1,2]"),
            &STANDARD.encode(r#"Event:["h","x","r"]"#),
            &STANDARD.encode("Transaction:not json"),
            &STANDARD.encode([0xff, 0xfe]),
        ] {
            assert!(decode(bad).is_err(), "{bad}");
        }
    }
}
