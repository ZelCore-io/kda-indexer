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
    Probe {
        /// Call each client method once and print sizes (client smoke check).
        #[arg(long)]
        smoke: bool,
        /// Report path (default test-runs/probe-<network>.json).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Fetch, decode and round-trip a bounded height range.
    Dump {
        /// First height (number or `tip-N`).
        #[arg(long)]
        from: String,
        /// Last height, inclusive (number or `tip-N`).
        #[arg(long)]
        to: String,
        /// Restrict to these chains (default: every chain that exists at each height).
        #[arg(long, value_delimiter = ',')]
        chains: Option<Vec<u8>>,
        /// Print one JSON line per tx instead of only the summary.
        #[arg(long)]
        json: bool,
        /// Write (kind, len, record) frames plus a `.dict.json` side file.
        #[arg(long)]
        records_out: Option<PathBuf>,
        /// Also write the summary as JSON.
        #[arg(long)]
        summary_out: Option<PathBuf>,
    },
    /// Measure record and compression sizes.
    Measure {
        /// Records file from `dump` (recent era).
        #[arg(long)]
        records: PathBuf,
        /// Second records file (historical era) for the cross-era dictionary check (B7).
        #[arg(long)]
        records_b: Option<PathBuf>,
        /// JSON report path.
        #[arg(long)]
        out: PathBuf,
    },
    /// Benchmark request-key index layouts (B6).
    RkBench {
        /// Records file(s) from `dump`; their real request keys are used (repeatable).
        #[arg(long, required = true)]
        records: Vec<PathBuf>,
        /// Total key count: real keys padded with blake2b-256(counter).
        #[arg(long, default_value_t = 3_000_000)]
        synthetic: usize,
        /// JSON report path.
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Parse the config and print it back as TOML.
    Check,
}

fn init_logging(log: &LogConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&log.level));
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
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
        Command::Config {
            action: ConfigAction::Check,
        } => {
            print!(
                "{}",
                toml::to_string_pretty(&cfg).context("serialising config")?
            );
            Ok(())
        }
        Command::Probe { smoke, out } => kda_indexer::cmd::probe::run(&cfg, smoke, out).await,
        Command::Dump {
            from,
            to,
            chains,
            json,
            records_out,
            summary_out,
        } => {
            kda_indexer::cmd::dump::run(
                &cfg,
                kda_indexer::cmd::dump::DumpArgs {
                    from,
                    to,
                    chains,
                    json,
                    records_out,
                    summary_out,
                },
            )
            .await
        }
        Command::Measure {
            records,
            records_b,
            out,
        } => kda_indexer::cmd::measure::run(kda_indexer::cmd::measure::MeasureArgs {
            records,
            records_b,
            out,
        }),
        Command::RkBench {
            records,
            synthetic,
            out,
        } => kda_indexer::cmd::rk_bench::run(kda_indexer::cmd::rk_bench::RkBenchArgs {
            records,
            synthetic,
            out,
        }),
    }
}
