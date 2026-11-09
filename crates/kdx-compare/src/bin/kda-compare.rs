use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use kdx_compare::allowlist::Allowlist;
use kdx_compare::config::CompareConfig;
use kdx_compare::diff::{diff_route, follow_pages};
use kdx_compare::normalize::{normalize, RouteKind};
use kdx_compare::target::Target;
use serde_json::Value;

#[derive(Parser)]
#[command(
    name = "kda-compare",
    about = "Compare harness for kda-indexer (spec section 7)"
)]
struct Cli {
    #[arg(long, global = true, default_value = "compare.local.toml")]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run continuously.
    Run,
    /// One request through the pipeline.
    Once {
        #[arg(long)]
        path: String,
        /// Debug: fetch A twice and diff it against itself.
        #[arg(long, hide = true)]
        against_self: bool,
        /// Debug: change the first occurrence of this key on the B copy.
        #[arg(long, hide = true)]
        mutate: Option<String>,
    },
    /// Import an HAProxy HTTP log into a replay TSV.
    ReplayImport {
        #[arg(long)]
        haproxy: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Parse and validate the config, then print it.
    CheckConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let cfg = CompareConfig::load(&cli.config)?;
    match cli.cmd {
        Cmd::CheckConfig => {
            println!("{}", toml::to_string_pretty(&cfg)?);
            Ok(())
        }
        Cmd::Once {
            path,
            against_self,
            mutate,
        } => {
            let targets = Target::all(&cfg)?;
            let name = cfg.primary_target().context("no targets configured")?;
            let t = targets.iter().find(|t| t.name == name).context("target")?;
            if against_self {
                return self_check(&cfg, t, &path, mutate.as_deref()).await;
            }
            let cap = t.get(&path).await?;
            println!(
                "{} {} -> {} ({} bytes, {} ms, next={})",
                t.name,
                path,
                cap.status,
                cap.body.len(),
                cap.ms,
                cap.next_token().unwrap_or("-")
            );
            Ok(())
        }
        Cmd::Run | Cmd::ReplayImport { .. } => bail!("not implemented yet"),
    }
}

/// Fetches A twice (following pages), normalises, diffs and classifies.
async fn self_check(
    cfg: &CompareConfig,
    t: &Target,
    path: &str,
    mutate: Option<&str>,
) -> anyhow::Result<()> {
    let route = RouteKind::from_path(path);
    let depth = if route.paginated() {
        cfg.follow_depth
    } else {
        0
    };
    let mut a = follow_pages(t, path, depth).await?.items;
    let mut b = follow_pages(t, path, depth).await?.items;
    if let Some(key) = mutate {
        anyhow::ensure!(
            mutate_first(&mut b, key),
            "key {key:?} not found in the response"
        );
    }
    normalize(&route, &mut a);
    normalize(&route, &mut b);
    let d = diff_route(&route, &a, &b);
    let allow = Allowlist::load(&cfg.allowlist)?;
    let (unexplained, ids) = allow.classify_request(&route, path, &d);
    println!(
        "route={} entries={} unexplained={} allowlisted={:?}",
        route.as_str(),
        d.len(),
        unexplained.len(),
        ids
    );
    for e in &unexplained {
        println!("{}", serde_json::to_string(e)?);
    }
    Ok(())
}

/// Changes the first value under `key` (depth first): numbers double, strings get a suffix.
fn mutate_first(v: &mut Value, key: &str) -> bool {
    match v {
        Value::Object(m) => {
            if let Some(x) = m.get_mut(key) {
                *x = match &*x {
                    Value::Number(n) => {
                        let f: f64 = n.to_string().parse().unwrap_or(0.0);
                        serde_json::Number::from_f64(f * 2.0 + 1e-9)
                            .map(Value::Number)
                            .unwrap_or(Value::Null)
                    }
                    Value::String(s) => Value::String(format!("{s}-mutated")),
                    _ => Value::String("mutated".into()),
                };
                return true;
            }
            m.values_mut().any(|x| mutate_first(x, key))
        }
        Value::Array(a) => a.iter_mut().any(|x| mutate_first(x, key)),
        _ => false,
    }
}
