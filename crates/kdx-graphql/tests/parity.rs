//! Schema parity (GQ-3, Review Focus 1): our SDL against the vendored Kadena schema, both
//! normalised (sorted, descriptions dropped). Names, arguments, argument defaults, types and
//! nullability, interfaces, union members and deprecations must be identical.

use std::collections::BTreeSet;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{
    ConstDirective, InputValueDefinition, TypeKind, TypeSystemDefinition,
};
use async_graphql::Positioned;
use kdx_graphql::{build_schema_detached, GraphqlSettings};

const VENDORED: &str = include_str!("../schema/kadena-graph.graphql");

fn directives(d: &[Positioned<ConstDirective>]) -> String {
    let mut v: Vec<String> = d
        .iter()
        .map(|d| {
            let args: Vec<String> = d
                .node
                .arguments
                .iter()
                .map(|(k, v)| format!("{}: {}", k.node, v.node))
                .collect();
            format!("@{}({})", d.node.name.node, args.join(", "))
        })
        .collect();
    v.sort();
    v.join(" ")
}

fn input(owner: &str, a: &InputValueDefinition) -> String {
    format!(
        "{owner}({}): {} = {} {}",
        a.name.node,
        a.ty.node,
        a.default_value
            .as_ref()
            .map_or_else(|| "-".to_string(), |v| v.node.to_string()),
        directives(&a.directives)
    )
}

/// One line per type, field, argument and union member; sorted.
fn normalise(sdl: &str) -> BTreeSet<String> {
    let doc = parse_schema(sdl).expect("SDL parses");
    let mut out = BTreeSet::new();
    for def in doc.definitions {
        let TypeSystemDefinition::Type(t) = def else {
            continue; // schema block, directive definitions
        };
        let t = t.node;
        let name = t.name.node.to_string();
        let mut fields =
            |kind: &str,
             implements: &[Positioned<async_graphql::Name>],
             fs: &[Positioned<async_graphql::parser::types::FieldDefinition>]| {
                let mut imp: Vec<String> = implements.iter().map(|i| i.node.to_string()).collect();
                imp.sort();
                out.insert(format!(
                    "{kind} {name} implements [{}] {}",
                    imp.join(","),
                    directives(&t.directives)
                ));
                for f in fs {
                    let f = &f.node;
                    out.insert(format!(
                        "field {name}.{}: {} {}",
                        f.name.node,
                        f.ty.node,
                        directives(&f.directives)
                    ));
                    for a in &f.arguments {
                        out.insert(format!(
                            "arg {}",
                            input(&format!("{name}.{}", f.name.node), &a.node)
                        ));
                    }
                }
            };
        match &t.kind {
            TypeKind::Scalar => {
                out.insert(format!("scalar {name}"));
            }
            TypeKind::Object(o) => fields("type", &o.implements, &o.fields),
            TypeKind::Interface(i) => fields("interface", &i.implements, &i.fields),
            TypeKind::Union(u) => {
                let mut m: Vec<String> = u.members.iter().map(|m| m.node.to_string()).collect();
                m.sort();
                out.insert(format!("union {name} = {}", m.join(" | ")));
            }
            TypeKind::Enum(e) => {
                for v in &e.values {
                    out.insert(format!("enum {name}.{}", v.node.value.node));
                }
            }
            TypeKind::InputObject(i) => {
                out.insert(format!("input {name}"));
                for f in &i.fields {
                    out.insert(format!("inputfield {}", input(&name, &f.node)));
                }
            }
        }
    }
    out
}

#[test]
fn our_schema_is_the_kadena_schema() {
    let ours = build_schema_detached(GraphqlSettings::default()).sdl();
    let a = normalise(VENDORED);
    let b = normalise(&ours);
    let missing: Vec<_> = a.difference(&b).collect();
    let extra: Vec<_> = b.difference(&a).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "schema differs from the vendored Kadena schema\n-- missing in ours:\n{}\n-- extra in ours:\n{}",
        missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n"),
        extra.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n"),
    );
    // Sanity: the normaliser saw the whole file.
    assert!(a.len() > 300, "{}", a.len());
}

#[test]
fn vendored_schema_is_unchanged() {
    // The file is vendored byte for byte (kadena.js commit
    // 1b05828f9c1cff7ccd8f7665c577a0a9e599aa2b, packages/apps/graph/generated-schema.graphql,
    // git blob ccfb431d723762575ef353d854f33547008e367a).
    // FNV-1a 64 of the file: any edit to the vendored copy fails here.
    let fnv = VENDORED.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3)
    });
    assert_eq!((VENDORED.len(), fnv), (21788, 0x2d687031a45f26ae));
}
