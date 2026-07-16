use crate::config::BenchmarkScale;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "slatedb-benchmark")]
#[command(version = concat!(
    env!("CARGO_PKG_VERSION"),
    " (slatedb ",
    env!("BENCHMARK_SLATE_VERSION"),
    " ",
    env!("BENCHMARK_SLATE_COMMIT"),
    ")"
))]
#[command(about = "Run and publish SlateDB benchmark suites")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Run(RunArgs),
    Validate(ValidateArgs),
    Catalog(CatalogArgs),
    #[command(hide = true)]
    Worker(WorkerArgs),
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[arg(long)]
    pub suite: String,
    /// Stable name used to create or resume a suite in object storage.
    #[arg(long)]
    pub session: String,
    #[arg(long)]
    pub workload: Option<String>,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value = "config")]
    pub config_dir: PathBuf,
    /// Fraction or percentage of configured data, time, caches, and probe work to execute.
    #[arg(long, default_value = "1")]
    pub scale: BenchmarkScale,
}

#[derive(Debug, Args)]
pub struct ValidateArgs {
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct CatalogArgs {
    #[arg(long)]
    pub suite: Option<String>,
    #[arg(long, default_value = "config")]
    pub config_dir: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct WorkerArgs {
    #[arg(long)]
    pub suite: String,
    #[arg(long)]
    pub workload: String,
    #[arg(long)]
    pub variant: String,
    #[arg(long)]
    pub database_path: String,
    #[arg(long)]
    pub expected_lsm_digest: String,
    #[arg(long)]
    pub object_store_cache_root: Option<PathBuf>,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value = "config")]
    pub config_dir: PathBuf,
    #[arg(long, default_value = "1")]
    pub scale: BenchmarkScale,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::Parser;

    #[test]
    fn resumable_session_can_select_a_whole_suite() {
        let cli = Cli::try_parse_from([
            "slatedb-benchmark",
            "run",
            "--suite",
            "rocksdb",
            "--session",
            "release-123",
            "--output",
            ".runs/rocksdb",
        ])
        .expect("parse suite session");

        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.suite, "rocksdb");
        assert_eq!(args.session, "release-123");
        assert!(args.workload.is_none());
        assert!(args.scale.is_full());
    }

    #[test]
    fn scale_accepts_factors_and_percentages() {
        for value in ["0.01", "1%"] {
            let cli = Cli::try_parse_from([
                "slatedb-benchmark",
                "run",
                "--suite",
                "ycsb",
                "--session",
                "scaled",
                "--output",
                ".runs/scaled",
                "--scale",
                value,
            ])
            .expect("parse scale");
            let Command::Run(args) = cli.command else {
                panic!("expected run command");
            };
            assert_eq!(args.scale.factor(), 0.01);
        }

        assert!(Cli::try_parse_from([
            "slatedb-benchmark",
            "run",
            "--suite",
            "ycsb",
            "--session",
            "scaled",
            "--output",
            ".runs/scaled",
            "--scale",
            "0",
        ])
        .is_err());
    }
}
