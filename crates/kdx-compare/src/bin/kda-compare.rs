use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use kdx_compare::allowlist::Allowlist;
use kdx_compare::config::CompareConfig;
use kdx_compare::diff::{diff_route, follow_pages};
use kdx_compare::gen::random::RandomGen;
use kdx_compare::gen::replay::{self, ReplayGen};
use kdx_compare::gen::seeds::SeedGen;
use kdx_compare::gen::{Generator, Mixer, Tips};
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
    Run {
        /// Stop after this many seconds (default: until SIGINT/SIGTERM).
        #[arg(long, hide = true)]
        duration_secs: Option<u64>,
    },
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
    /// P3 gate: per route and pair, requests compared, unexplained diffs, allowlisted hits,
    /// errors and p50/p99 per side over the summary files in `out_dir`. Exit 0 iff green.
    Summary {
        /// Start of the window, `YYYY-MM-DDTHH:MM:SSZ` or `YYYY-MM-DD` (ten-minute buckets
        /// that start at or after it).
        #[arg(long)]
        since: String,
        /// `must` (default), `should` (search, coins, balances) or `all`: the routes the
        /// verdict covers.
        #[arg(long, default_value = "must")]
        routes: String,
        /// `small` (R3E-13: every phase gate) or `full` (500/200/50, the final validation).
        /// Required: the default changed from full to small in P5, so a command line
        /// without it must not pass a weaker gate silently.
        #[arg(long)]
        gate: String,
    },
    /// Debug: generate requests and print them.
    #[command(hide = true)]
    Gen {
        #[arg(long, default_value_t = 50)]
        count: usize,
        #[arg(long)]
        print: bool,
        /// Only this source: random, seeds or replay (default: the configured mix).
        #[arg(long)]
        source: Option<String>,
    },
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
        Cmd::Summary {
            since,
            routes,
            gate,
        } => {
            if !kdx_compare::summary::run(&cfg.out_dir, &since, &routes, &gate)? {
                std::process::exit(1);
            }
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
            let _ = t;
            for o in kdx_compare::run::once(cfg.clone(), &path).await? {
                println!("class={}", o.class);
                if let Some(r) = o.record {
                    println!("{}", serde_json::to_string_pretty(&r)?);
                }
            }
            Ok(())
        }
        Cmd::ReplayImport { haproxy, out } => {
            let (lines, kept, distinct) = replay::import(&haproxy, &out)?;
            println!(
                "read {lines} lines, kept {kept} GET requests on compat routes, {distinct} distinct paths -> {}",
                out.display()
            );
            Ok(())
        }
        Cmd::Gen {
            count,
            print,
            source,
        } => gen_debug(&cfg, count, print, source.as_deref()).await,
        Cmd::Run { duration_secs } => kdx_compare::run::run(cfg, duration_secs).await,
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

async fn gen_debug(
    cfg: &CompareConfig,
    count: usize,
    print: bool,
    source: Option<&str>,
) -> anyhow::Result<()> {
    let node = cfg.node_client()?;
    let targets = Target::all(cfg)?;
    let a = cfg
        .primary_target()
        .and_then(|n| targets.iter().find(|t| t.name == n));
    let tips = Arc::new(Tips::default());
    tips.refresh(&node, a).await?;
    let graph = node.chain_graph().await?;
    let want = |s: &str| source.is_none_or(|x| x == s);
    let random = want("random")
        .then(|| RandomGen::new(node.clone(), graph.clone(), tips.clone(), cfg.min_depth));
    let seeds = match (&cfg.seeds, want("seeds")) {
        (Some(p), true) => Some(SeedGen::load(p)?.with_node(node.clone(), tips.clone())),
        _ => None,
    };
    let replay = match (&cfg.replay, want("replay")) {
        (Some(p), true) => Some(ReplayGen::load(p)?),
        _ => None,
    };
    let mut mixer = Mixer::new(random, seeds, replay, cfg.mix.clone());
    let mut out = Vec::new();
    while out.len() < count {
        out.extend(mixer.next().await?);
    }
    out.truncate(count);
    let mut by_route = std::collections::BTreeMap::<&str, usize>::new();
    for r in &out {
        *by_route.entry(r.route.as_str()).or_default() += 1;
        if print {
            println!(
                "{:<7} {:<8} {} oracle={}",
                r.origin.as_str(),
                r.route.as_str(),
                r.path_and_query,
                r.oracle
                    .as_ref()
                    .map(|o| serde_json::to_string(o).unwrap_or_default())
                    .unwrap_or_else(|| "-".into())
            );
        }
    }
    println!(
        "{} requests by route {:?}; empty blocks skipped {}; min tip {} (A tip {})",
        out.len(),
        by_route,
        mixer.empty_blocks(),
        tips.min_tip(),
        tips.a_tip()
    );
    Ok(())
}
