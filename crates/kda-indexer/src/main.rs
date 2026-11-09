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
        /// First height (number or `tip-N`). `tip` is the final tip: the lowest cut height
        /// minus `ingest.finality_depth`; heights above it are refused.
        #[arg(long)]
        from: String,
        /// Last height, inclusive (number or `tip-N`, at most the final tip).
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
        /// Run index extraction (postings, defpacts, code hashes) and the adjacent-ref
        /// encoding round trip; print per-family posting counts.
        #[arg(long)]
        index: bool,
    },
    /// Fetch canonical blocks into frozen segments (resumes at the manifest frontier).
    Backfill {
        /// First height (number or `tip-N`); ignored when segments exist (resume).
        #[arg(long)]
        from: String,
        /// Last height, inclusive (number or `tip-N`); clamped to the final tip and to whole
        /// steps.
        #[arg(long)]
        to: String,
        /// Data directory (default: `data_dir` from the config).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Canonical header walk: auto (cut, switching to anchors when slow), cut or anchor.
        #[arg(long, default_value = "auto", hide = true)]
        header_mode: kdx_ingest::HeaderMode,
    },
    /// Hot store maintenance (hidden; P2 development aid).
    #[command(hide = true)]
    Hot {
        #[command(subcommand)]
        action: HotAction,
    },
    /// Frozen segments: list the manifest, check files and indexes.
    Segments {
        #[command(subcommand)]
        action: SegmentsAction,
    },
    /// Read stored records back (hot store first, then frozen segments).
    Inspect {
        /// Data directory (default: `data_dir` from the config).
        #[arg(long, global = true)]
        data_dir: Option<PathBuf>,
        #[command(subcommand)]
        what: InspectWhat,
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
}

#[derive(Subcommand)]
enum HotAction {
    /// Open (or initialise) the hot store; print Meta, table counts, dict sizes, geometry.
    Stats {
        /// Data directory (default: `data_dir` from the config).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum SegmentsAction {
    /// Manifest entries with counts, sizes, index kinds, frontier and next TxNum.
    List {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Check manifest, blake3 of every file and TxNum density; exit 1 on any error.
    Check {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Also decode every record and resolve every rk, bh and posting list.
        #[arg(long)]
        deep: bool,
    },
}

#[derive(Subcommand)]
enum InspectWhat {
    /// A tx by request key (base64url).
    Tx { reqkey: String },
    /// A block by chain and height.
    Block { chain: u8, height: u32 },
    /// A block by hash (base64url).
    BlockHash { hash: String },
    /// Posting list of an entity, newest first: family acct|xfer|pk|evt|code; name is an
    /// account, public key, qualified event name (`coin.TRANSFER`, or `coin.*` for the
    /// module), or code-hash hex.
    Postings {
        family: String,
        name: String,
        /// Token module for `xfer` (e.g. `coin`).
        #[arg(long)]
        module: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Random frozen and hot txs; with --compare-node, refetch each block and require
    /// byte-identical rendered tx, output, miner data and an equal header.
    Sample {
        #[arg(long, default_value_t = 20)]
        count: usize,
        #[arg(long)]
        compare_node: bool,
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
            index,
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
                    index,
                },
            )
            .await
        }
        Command::Backfill {
            from,
            to,
            data_dir,
            header_mode,
        } => {
            kda_indexer::cmd::backfill::run(
                &cfg,
                data_dir.as_deref().unwrap_or(&cfg.data_dir),
                &from,
                &to,
                header_mode,
            )
            .await
        }
        Command::Hot {
            action: HotAction::Stats { data_dir },
        } => kda_indexer::cmd::hot::stats(&cfg, data_dir.as_deref().unwrap_or(&cfg.data_dir)),
        Command::Segments { action } => match action {
            SegmentsAction::List { data_dir, json } => {
                kda_indexer::cmd::segments::list(data_dir.as_deref().unwrap_or(&cfg.data_dir), json)
            }
            SegmentsAction::Check { data_dir, deep } => kda_indexer::cmd::segments::check(
                data_dir.as_deref().unwrap_or(&cfg.data_dir),
                deep,
            ),
        },
        Command::Inspect { data_dir, what } => {
            use kda_indexer::cmd::inspect;
            let s = inspect::Store::open(&cfg, data_dir.as_deref().unwrap_or(&cfg.data_dir))?;
            match what {
                InspectWhat::Tx { reqkey } => inspect::tx(&s, &reqkey),
                InspectWhat::Block { chain, height } => inspect::block(&s, chain, height),
                InspectWhat::BlockHash { hash } => inspect::block_hash(&s, &hash),
                InspectWhat::Postings {
                    family,
                    name,
                    module,
                    limit,
                } => inspect::postings(&s, &family, &name, module.as_deref(), limit),
                InspectWhat::Sample {
                    count,
                    compare_node,
                } => inspect::sample(&cfg, &s, count, compare_node).await,
            }
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
    }
}
