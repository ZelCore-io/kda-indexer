use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use kdx_compare::config::CompareConfig;
use kdx_compare::target::Target;

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
        Cmd::Once { path } => {
            let targets = Target::all(&cfg)?;
            let name = cfg.primary_target().context("no targets configured")?;
            let t = targets.iter().find(|t| t.name == name).context("target")?;
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
