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
#[command(about = "Run and publish the SlateDB release benchmark suite")]
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
    pub profile: Option<String>,
    #[arg(long, requires = "profile")]
    pub workload: Option<String>,
    #[arg(long, requires = "workload")]
    pub variant: Option<String>,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, help = "Use unpublished reduced datasets and durations")]
    pub smoke: bool,
    #[arg(long, default_value = "config")]
    pub config_dir: PathBuf,
    #[arg(long, default_value = "schema")]
    pub schema_dir: PathBuf,
}

#[derive(Debug, Args)]
pub struct ValidateArgs {
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Debug, Args)]
pub struct CatalogArgs {
    #[arg(long)]
    pub smoke: bool,
}

#[derive(Debug, Clone, Args)]
pub struct WorkerArgs {
    #[arg(long)]
    pub profile: String,
    #[arg(long)]
    pub workload: String,
    #[arg(long)]
    pub variant: String,
    #[arg(long)]
    pub database_path: String,
    #[arg(long)]
    pub shared_database_bytes: u64,
    #[arg(long)]
    pub expected_lsm_digest: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub smoke: bool,
    #[arg(long, default_value = "config")]
    pub config_dir: PathBuf,
}
