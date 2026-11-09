use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use kda_indexer::config::{Config, LogConfig};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "kda-indexer", version, about = "Kadena chainweb indexer")]
struct Cli {
    /// Path to the TOML config file.
    #[arg(long, short, global = true, default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Configuration utilities.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Probe node routes and limits (B1, B2, B11).
    Probe,
    /// Fetch, decode and round-trip a bounded height range.
    Dump,
    /// Measure record and compression sizes.
    Measure,
    /// Benchmark request-key index layouts (B6).
    RkBench,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Parse the config and print it back as TOML.
    Check,
}

fn init_logging(log: &LogConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&log.level));
    let b = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    if log.format == "json" {
        b.json().init();
    } else {
        b.init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;
    init_logging(&cfg.log);
    match cli.command {
        Command::Config { action: ConfigAction::Check } => {
            print!("{}", toml::to_string_pretty(&cfg).context("serialising config")?);
            Ok(())
        }
        Command::Probe => kda_indexer::cmd::probe::run(&cfg).await,
        Command::Dump => anyhow::bail!("dump: not implemented yet"),
        Command::Measure => anyhow::bail!("measure: not implemented yet"),
        Command::RkBench => anyhow::bail!("rk-bench: not implemented yet"),
    }
}
