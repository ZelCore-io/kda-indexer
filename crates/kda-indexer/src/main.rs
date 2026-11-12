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
        /// Recompute the payload Merkle roots (B8) per block from the served leaves and
        /// from the built records; print verified / mismatch per block and write a summary
        /// JSON (default `test-runs/dump-merkle-<network>-<from>-<to>.json`).
        #[arg(long)]
        merkle: bool,
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
    /// Index the network: backfill what is final, then follow the tip and freeze (until
    /// SIGTERM/SIGINT).
    Run {
        /// Data directory (default: `data_dir` from the config).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Dev: start an empty store at this height (number or `tip-N`, N below the lowest
        /// cut height; aligned down to a step) instead of backfilling history.
        #[arg(long, hide = true)]
        start_height: Option<String>,
        /// Dev: stop each chain at this height and exit when all are there.
        #[arg(long, hide = true)]
        until: Option<u32>,
        #[arg(long, default_value = "auto", hide = true)]
        header_mode: kdx_ingest::HeaderMode,
    },
    /// Serve the APIs read-only over a data dir another process writes (no ingest, no lock).
    Serve {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Dev: `run` without the freezer.
    /// Print the OpenAPI document of the native API (no config needed).
    #[command(hide = true)]
    Openapi,
    #[command(hide = true)]
    Tail {
        #[arg(long)]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        start_height: Option<String>,
        #[arg(long)]
        until: Option<u32>,
    },
    /// Undo one chain's hot blocks above a height (indexer stopped; the tail re-applies them).
    Unwind {
        #[arg(long)]
        chain: u8,
        /// Undo the chain's hot blocks above this height.
        #[arg(long)]
        to: Option<u32>,
        /// Clear the chain's halted state (after a reorg deeper than the finality depth).
        #[arg(long)]
        clear_halt: bool,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Hot store maintenance (hidden; P2 development aid).
    #[command(hide = true)]
    Hot {
        #[command(subcommand)]
        action: HotAction,
    },
    /// Read-only checks of stored data.
    Verify {
        #[command(subcommand)]
        what: VerifyWhat,
    },
    /// Write a consistent copy of the data dir (hard-linked segments, compacted hot store).
    /// Reads only; can run beside a live `run`. The result is a data dir for `serve` and `run`.
    Snapshot {
        /// New directory (must not exist or be empty).
        #[arg(long)]
        out: PathBuf,
        /// Source data directory (default: `data_dir` from the config).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Copy the segment files instead of hard-linking (another filesystem).
        #[arg(long)]
        copy: bool,
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
        /// Also print a blake3 digest per table (replay checks).
        #[arg(long)]
        digest: bool,
    },
}

#[derive(Subcommand)]
enum VerifyWhat {
    /// Recompute payload Merkle roots from stored records (leaves re-rendered) and compare
    /// them with the stored roots and the `merkle_verified` bit (B8). Read-only; a fresh read
    /// txn per chunk. Classes ok / ok_unflagged / bad_flagged / bad_legacy / bad_other;
    /// exit 1 on any bad_flagged, bad_other or render error.
    Merkle {
        /// Data directory (default: `data_dir` from the config).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// First height (default: the lowest stored height).
        #[arg(long)]
        from: Option<u32>,
        /// Last height, inclusive (default: the highest stored height).
        #[arg(long)]
        to: Option<u32>,
        /// Restrict to these chains (default: all).
        #[arg(long, value_delimiter = ',')]
        chains: Option<Vec<u8>>,
        /// Check N random (height, chain) blocks (default 1000 when --all is not given).
        #[arg(long, conflicts_with = "all")]
        sample: Option<u64>,
        /// Check every stored block in the range.
        #[arg(long)]
        all: bool,
        /// JSON output.
        #[arg(long)]
        json: bool,
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
    /// Posting list of an entity, newest first: family acct|xfer|pk|evt|code|pact; name is
    /// an account, public key, qualified event name (`coin.TRANSFER`, or `coin.*` for the
    /// module), code-hash hex or pactId (base64url).
    Postings {
        family: String,
        name: String,
        /// Token module for `xfer` (e.g. `coin`).
        #[arg(long)]
        module: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Dev: loop read views (postings walk, request-key lookups, cursors) for N seconds
    /// beside a running indexer; reports duplicates, order breaks, lookup misses, errors.
    #[command(hide = true)]
    ViewCheck {
        #[arg(long, default_value_t = 600)]
        seconds: u64,
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
    if matches!(cli.command, Command::Openapi) {
        println!("{}", kdx_api::openapi::json());
        return Ok(());
    }
    let cfg = Config::load(&cli.config)?;
    init_logging(&cfg.log);
    kdx_metrics::init(&cfg.network, env!("CARGO_PKG_VERSION"), env!("KDX_GIT_SHA"));
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
        Command::Openapi => Ok(()),
        Command::Probe { smoke, out } => kda_indexer::cmd::probe::run(&cfg, smoke, out).await,
        Command::Dump {
            from,
            to,
            chains,
            json,
            records_out,
            summary_out,
            index,
            merkle,
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
                    merkle,
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
            action: HotAction::Stats { data_dir, digest },
        } => {
            kda_indexer::cmd::hot::stats(&cfg, data_dir.as_deref().unwrap_or(&cfg.data_dir), digest)
        }
        Command::Run {
            data_dir,
            start_height,
            until,
            header_mode,
        } => {
            kda_indexer::cmd::run::run(
                &cfg,
                kda_indexer::cmd::run::RunArgs {
                    data_dir: data_dir.unwrap_or_else(|| cfg.data_dir.clone()),
                    start_height,
                    until,
                    freeze: true,
                    header_mode,
                },
            )
            .await
        }
        Command::Serve { data_dir } => {
            kda_indexer::cmd::serve::run(&cfg, data_dir.as_deref().unwrap_or(&cfg.data_dir)).await
        }
        Command::Tail {
            data_dir,
            start_height,
            until,
        } => {
            kda_indexer::cmd::run::run(
                &cfg,
                kda_indexer::cmd::run::RunArgs {
                    data_dir: data_dir.unwrap_or_else(|| cfg.data_dir.clone()),
                    start_height,
                    until,
                    freeze: false,
                    header_mode: kdx_ingest::HeaderMode::Auto,
                },
            )
            .await
        }
        Command::Unwind {
            chain,
            to,
            clear_halt,
            data_dir,
        } => kda_indexer::cmd::unwind::run(
            &cfg,
            data_dir.as_deref().unwrap_or(&cfg.data_dir),
            chain,
            to,
            clear_halt,
        ),
        Command::Verify {
            what:
                VerifyWhat::Merkle {
                    data_dir,
                    from,
                    to,
                    chains,
                    sample,
                    all,
                    json,
                },
        } => kda_indexer::cmd::verify::merkle(kda_indexer::cmd::verify::MerkleArgs {
            data_dir: data_dir.unwrap_or_else(|| cfg.data_dir.clone()),
            from,
            to,
            chains,
            sample,
            all,
            json,
        }),
        Command::Snapshot {
            out,
            data_dir,
            copy,
        } => kda_indexer::cmd::snapshot::run(kda_indexer::cmd::snapshot::SnapshotArgs {
            data_dir: data_dir.unwrap_or_else(|| cfg.data_dir.clone()),
            out,
            copy,
        }),
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
            if let InspectWhat::ViewCheck { seconds } = what {
                return kda_indexer::cmd::viewcheck::run(
                    data_dir.as_deref().unwrap_or(&cfg.data_dir),
                    seconds,
                );
            }
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
                InspectWhat::ViewCheck { .. } => unreachable!("handled above"),
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
