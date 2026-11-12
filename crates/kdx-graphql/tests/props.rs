//! Property tests of the GraphQL adapter (plan Task 13, T-1): Relay ids and cursors survive
//! a round trip and never panic on a flipped or cut token, and aliases and fragments count
//! toward complexity. The complexity test compares the walker with a counter written here.

#[macro_use]
#[path = "../../kdx-types/tests/common/prop.rs"]
mod prop;
use prop::*;

use async_graphql::parser::parse_query;
use async_graphql::Variables;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use kdx_api::Cursor;
use kdx_graphql::connection::{BlockCursor, PageCursor};
use kdx_graphql::ids::{self, NodeKey};
use kdx_graphql::limits::analyze;
use kdx_graphql::types::account::{balance_cursor, decode_balance_cursor};
use kdx_graphql::GraphqlSettings;
use kdx_types::TxKey;

// ---------------------------------------------------------------- Relay ids and cursors

fn text() -> impl Strategy<Value = String> {
    prop_oneof!["[ -~]{1,24}", "\\PC{1,12}"]
}

fn node_key() -> impl Strategy<Value = NodeKey> {
    let s = text;
    prop_oneof![
        s().prop_map(|hash| NodeKey::Block { hash }),
        (s(), any::<i64>(), s()).prop_map(|(block_hash, order_index, request_key)| {
            NodeKey::Event {
                block_hash,
                order_index,
                request_key,
            }
        }),
        (s(), s()).prop_map(|(fungible, account)| NodeKey::FungibleAccount { fungible, account }),
        (s(), s(), s()).prop_map(|(chain, fungible, account)| NodeKey::FungibleChainAccount {
            chain,
            fungible,
            account
        }),
        s().prop_map(|account| NodeKey::NonFungibleAccount { account }),
        (s(), s()).prop_map(|(chain, account)| NodeKey::NonFungibleChainAccount { chain, account }),
        (s(), s(), s()).prop_map(
            |(token_id, account, chain)| NodeKey::NonFungibleTokenBalance {
                token_id,
                account,
                chain
            }
        ),
        (s(), any::<i64>()).prop_map(|(request_key, order_index)| NodeKey::Signer {
            request_key,
            order_index
        }),
        (s(), s()).prop_map(|(block_hash, request_key)| NodeKey::Transaction {
            block_hash,
            request_key
        }),
        (s(), any::<i64>(), any::<i64>(), s(), s()).prop_map(
            |(block_hash, chain, order_index, module_hash, request_key)| NodeKey::Transfer {
                block_hash,
                chain,
                order_index,
                module_hash,
                request_key
            }
        ),
    ]
}

/// A decoded id is a well-formed one: encoding it again decodes to the same key.
fn well_formed(k: &NodeKey) -> Result<(), TestCaseError> {
    let back = ids::decode(&ids::encode(k));
    prop_assert_eq!(back.as_ref(), Ok(k));
    Ok(())
}

fn is_bad_input(e: &async_graphql::Error) -> bool {
    e.extensions
        .as_ref()
        .and_then(|x| x.get("code"))
        .is_some_and(|c| *c == async_graphql::Value::from("BAD_USER_INPUT"))
}

props! {
    fn relay_id_roundtrip(k in node_key()) {
        prop_assert_eq!(ids::decode(&ids::encode(&k)), Ok(k));
    }

    fn relay_id_bit_flip_is_an_error_or_a_well_formed_id(k in node_key(), at in any::<proptest::sample::Index>(), bit in 0u8..8) {
        let mut raw = STANDARD.decode(ids::encode(&k)).unwrap();
        let i = at.index(raw.len());
        raw[i] ^= 1 << bit;
        if let Ok(d) = ids::decode(&STANDARD.encode(&raw)) {
            well_formed(&d)?;
        }
    }

    fn relay_id_char_replacement_never_panics(k in node_key(), at in any::<proptest::sample::Index>(), to in "[A-Za-z0-9+/=_-]") {
        let mut t = ids::encode(&k).into_bytes();
        let i = at.index(t.len());
        t[i] = to.as_bytes()[0];
        if let Ok(d) = ids::decode(&String::from_utf8(t).unwrap()) {
            well_formed(&d)?;
        }
    }

    fn relay_id_truncation_is_an_error_or_a_well_formed_id(k in node_key(), cut in any::<proptest::sample::Index>()) {
        let t = ids::encode(&k);
        let n = cut.index(t.len() + 1);
        if let Ok(d) = ids::decode(&t[..n]) {
            well_formed(&d)?;
        }
        // The empty id and an over-long one are always refused.
        prop_assert!(ids::decode("").is_err());
        prop_assert!(ids::decode(&"A".repeat(ids::MAX_ID_LEN + 1)).is_err());
    }

    fn relay_id_garbage_never_panics(s in "\\PC{0,200}") {
        if let Ok(d) = ids::decode(&s) {
            well_formed(&d)?;
        }
    }

    fn block_cursor_roundtrip_and_truncation(height in any::<u64>(), chain in any::<u8>(), cut in any::<proptest::sample::Index>()) {
        let c = BlockCursor { height, chain };
        let t = c.encode();
        prop_assert_eq!(BlockCursor::decode(&t).unwrap(), c);
        prop_assert_eq!(PageCursor::decode(&t).unwrap(), PageCursor::Block(c));
        let k = cut.index(t.len());
        prop_assert!(BlockCursor::decode(&t[..k]).is_err());
        let e = PageCursor::decode(&t[..k]).err();
        prop_assert!(e.as_ref().is_none_or(is_bad_input));
    }

    fn page_cursor_flips_and_cuts_are_bad_input_or_well_formed(
        raw in any::<u64>(), sub in any::<u16>(), fp in any::<u32>(),
        height in any::<u64>(), chain in any::<u8>(), block in any::<bool>(),
        at in any::<proptest::sample::Index>(), to in "[A-Za-z0-9_-]",
        cut in any::<proptest::sample::Index>(),
    ) {
        let c = if block {
            PageCursor::Block(BlockCursor { height, chain })
        } else {
            PageCursor::Tx(Cursor { key: TxKey(raw), sub, fp })
        };
        let t = c.encode();
        prop_assert_eq!(PageCursor::decode(&t).unwrap(), c);
        let mut flipped = t.clone().into_bytes();
        let i = at.index(flipped.len());
        flipped[i] = to.as_bytes()[0];
        let flipped = String::from_utf8(flipped).unwrap();
        let cut = t[..cut.index(t.len() + 1)].to_owned();
        for m in [flipped, cut] {
            match PageCursor::decode(&m) {
                // No checksum (R-P5P9-25): a flipped token may be another cursor, a well-formed one.
                Ok(d) => prop_assert_eq!(PageCursor::decode(&d.encode()).unwrap(), d),
                Err(e) => prop_assert!(is_bad_input(&e), "{:?}", e),
            }
        }
    }

    fn page_cursor_garbage_never_panics(s in "\\PC{0,100}") {
        if let Err(e) = PageCursor::decode(&s) {
            prop_assert!(is_bad_input(&e));
        }
    }

    fn balance_cursor_roundtrip_and_damage(module in "\\PC{0,24}", chain in any::<u8>(), at in any::<proptest::sample::Index>(), to in "[A-Za-z0-9_-]", cut in any::<proptest::sample::Index>()) {
        let t = balance_cursor(&module, chain);
        prop_assert_eq!(decode_balance_cursor(&t).unwrap(), (module.clone(), chain));
        let mut f = t.clone().into_bytes();
        let i = at.index(f.len());
        f[i] = to.as_bytes()[0];
        let cut = t[..cut.index(t.len() + 1)].to_owned();
        for m in [String::from_utf8(f).unwrap(), cut] {
            match decode_balance_cursor(&m) {
                Ok((md, c)) => prop_assert_eq!(decode_balance_cursor(&balance_cursor(&md, c)).unwrap(), (md, c)),
                Err(e) => prop_assert!(is_bad_input(&e)),
            }
        }
    }
}

// ---------------------------------------------------------------- complexity

/// A selection under `block(hash: ..)` (ctx Block) or under a transaction node (ctx Tx).
#[derive(Debug, Clone)]
enum Item {
    /// `hash`, a scalar of both contexts.
    Leaf,
    /// `parent { .. }`: a block, not a list.
    Parent(Vec<Item>),
    /// `transactions(first: n) { edges { node { .. } } }`, node items in Tx context.
    Txs(u8, Vec<Item>),
    /// A fragment: a named one spread `reps` times, or an inline one repeated `reps` times.
    Frag(FragKind, u8, Vec<Item>),
}

#[derive(Debug, Clone, Copy)]
enum FragKind {
    Named,
    InlineTyped,
    InlineBare,
}

fn frag_kind() -> impl Strategy<Value = FragKind> {
    prop_oneof![
        Just(FragKind::Named),
        Just(FragKind::InlineTyped),
        Just(FragKind::InlineBare)
    ]
}

fn node_items() -> BoxedStrategy<Vec<Item>> {
    let item = Just(Item::Leaf).prop_recursive(3, 12, 3, |inner| {
        (frag_kind(), 1u8..=3, proptest::collection::vec(inner, 1..4))
            .prop_map(|(k, r, b)| Item::Frag(k, r, b))
    });
    proptest::collection::vec(item, 1..4).boxed()
}

fn block_items() -> BoxedStrategy<Vec<Item>> {
    let item = Just(Item::Leaf).prop_recursive(3, 24, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 1..4).prop_map(Item::Parent),
            (1u8..=50, node_items()).prop_map(|(n, b)| Item::Txs(n, b)),
            (frag_kind(), 1u8..=3, proptest::collection::vec(inner, 1..4))
                .prop_map(|(k, r, b)| Item::Frag(k, r, b)),
        ]
    });
    proptest::collection::vec(item, 1..4).boxed()
}

/// The independent counter: every field occurrence costs 1, a list costs its page size times
/// its item (at least 1), `edges` and `node` are free, and a fragment costs its body once
/// per spread. Written from the spec text, not from the walker.
fn expected(items: &[Item]) -> u64 {
    items
        .iter()
        .map(|i| match i {
            Item::Leaf => 1,
            Item::Parent(b) => 1 + expected(b),
            Item::Txs(n, b) => 1 + u64::from(*n) * expected(b).max(1),
            Item::Frag(_, reps, b) => u64::from(*reps) * expected(b),
        })
        .sum()
}

#[derive(Default)]
struct Out {
    n: usize,
    frags: String,
    aliased: bool,
}

impl Out {
    fn field(&mut self, name: &str) -> String {
        self.n += 1;
        if self.aliased {
            format!("a{}: {name}", self.n)
        } else {
            name.to_owned()
        }
    }
}

fn render(items: &[Item], ty: &str, o: &mut Out) -> String {
    let mut s = String::new();
    for i in items {
        match i {
            Item::Leaf => s += &format!("{} ", o.field("hash")),
            Item::Parent(b) => {
                let f = o.field("parent");
                s += &format!("{f} {{ {}}} ", render(b, "Block", o));
            }
            Item::Txs(n, b) => {
                let f = o.field("transactions");
                let e = o.field("edges");
                let nd = o.field("node");
                s += &format!(
                    "{f}(first: {n}) {{ {e} {{ {nd} {{ {}}} }} }} ",
                    render(b, "Transaction", o)
                );
            }
            Item::Frag(kind, reps, b) => match kind {
                FragKind::Named => {
                    o.n += 1;
                    let name = format!("F{}", o.n);
                    let body = render(b, ty, o);
                    o.frags += &format!("fragment {name} on {ty} {{ {body}}}\n");
                    for _ in 0..*reps {
                        s += &format!("...{name} ");
                    }
                }
                FragKind::InlineTyped | FragKind::InlineBare => {
                    for _ in 0..*reps {
                        let head = match kind {
                            FragKind::InlineTyped => format!("... on {ty}"),
                            _ => "...".to_owned(),
                        };
                        s += &format!("{head} {{ {}}} ", render(b, ty, o));
                    }
                }
            },
        }
    }
    s
}

fn document(items: &[Item], aliased: bool) -> String {
    let mut o = Out {
        aliased,
        ..Out::default()
    };
    let body = render(items, "Block", &mut o);
    let root = o.field("block");
    format!("query {{ {root}(hash: \"h\") {{ {body}}} }}\n{}", o.frags)
}

fn cost_of(q: &str) -> u64 {
    let doc = parse_query(q).unwrap_or_else(|e| panic!("{e}\n{q}"));
    let s = GraphqlSettings {
        max_depth: 64,
        max_complexity: u64::MAX,
        ..GraphqlSettings::default()
    };
    analyze(&doc, &Variables::default(), &s)
        .unwrap_or_else(|e| panic!("{e:?}\n{q}"))
        .complexity
}

props! {
    fn aliases_and_fragments_count_toward_complexity(items in block_items()) {
        let want = 1 + expected(&items);
        let plain = document(&items, false);
        let aliased = document(&items, true);
        prop_assert_eq!(cost_of(&plain), want, "{}", plain);
        prop_assert_eq!(cost_of(&aliased), want, "{}", aliased);
    }
}

/// Aliases cannot disguise a field: a list aliased as another name, the same list asked twice
/// under two aliases, and an alias that copies a free wrapper name each cost what they cost.
#[test]
fn alias_tricks_do_not_lower_the_cost() {
    let one =
        cost_of(r#"{ block(hash: "h") { transactions(first: 10) { edges { node { hash } } } } }"#);
    assert_eq!(one, 1 + 1 + 10);
    let two = cost_of(
        r#"{ block(hash: "h") { a: transactions(first: 10) { edges { node { hash } } } b: transactions(first: 10) { edges { node { hash } } } } }"#,
    );
    assert_eq!(two, 1 + 2 * 11);
    // `edges: hash` is a scalar named like the free wrapper: still one unit, never free.
    let disguised = cost_of(r#"{ block(hash: "h") { edges: hash node: hash } }"#);
    assert_eq!(disguised, 1 + 2);
    // Fragments spread twice cost twice; a cycle or an unknown fragment adds nothing.
    let spread = cost_of(
        r#"{ block(hash: "h") { ...F ...F ...Unknown } } fragment F on Block { hash height }"#,
    );
    assert_eq!(spread, 1 + 4);
}
