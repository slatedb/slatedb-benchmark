use crate::bundle::BundleArgs;
use crate::config::{BenchmarkScale, Task};
use crate::publish::PublishArgs;
use clap::{Args, Parser, Subcommand, ValueEnum};
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
#[command(about = "Prepare, run, validate, bundle, and publish SlateDB benchmarks")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Prepare the bulk-load or compacted golden data set.
    Prepare(PrepareArgs),
    /// Run one workload.
    Run(RunArgs),
    /// Assemble validated results into a versioned run bundle.
    Bundle(BundleCommand),
    /// Validate one benchmark artifact.
    Validate(ValidateArgs),
    /// Publish one full-scale run bundle to the website checkout.
    Publish(PublishCommand),
    /// Generate JSON Schema and TypeScript contracts from Rust types.
    Generate(GenerateArgs),
    /// Print the workload catalog as a JSON array.
    Catalog,
    /// Remove all object-store data for one benchmark session.
    Cleanup(CleanupArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PreparationPhase {
    BulkLoad,
    Compaction,
}

impl From<PreparationPhase> for Task {
    fn from(value: PreparationPhase) -> Self {
        match value {
            PreparationPhase::BulkLoad => Task::BulkLoad,
            PreparationPhase::Compaction => Task::Compaction,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct PrepareArgs {
    #[arg(long, value_enum)]
    pub phase: PreparationPhase,
    #[arg(long, value_name = "GOLDEN_ID")]
    pub golden: String,
    #[arg(long, default_value = "1.0", value_name = "FACTOR")]
    pub scale: BenchmarkScale,
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
    /// Required idle time after the final compaction before recording the golden manifest.
    #[arg(long, default_value_t = 60_000)]
    pub compaction_quiet_ms: u64,
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[arg(long, value_enum)]
    pub workload: Task,
    #[arg(long, value_name = "GOLDEN_ID")]
    pub golden: String,
    #[arg(long)]
    pub session: String,
    #[arg(long, default_value = "1.0", value_name = "FACTOR")]
    pub scale: BenchmarkScale,
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct BundleCommand {
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub golden: String,
    #[arg(long)]
    pub started_at: String,
    #[arg(long, default_value = "patches/slatedb")]
    pub patches: PathBuf,
}

impl From<BundleCommand> for BundleArgs {
    fn from(value: BundleCommand) -> Self {
        Self {
            input: value.input,
            output: value.output,
            golden: value.golden,
            started_at: value.started_at,
            patches: value.patches,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Artifact {
    Golden,
    Result,
    Series,
    Run,
    TransferCapacity,
}

#[derive(Debug, Clone, Args)]
pub struct ValidateArgs {
    #[arg(long, value_enum)]
    pub artifact: Artifact,
    #[arg(long)]
    pub input: PathBuf,
    /// Paired result.json, required when validating series.json.
    #[arg(long)]
    pub result: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct PublishCommand {
    #[arg(long)]
    pub bundle: PathBuf,
    #[arg(long)]
    pub checkout: PathBuf,
}

impl From<PublishCommand> for PublishArgs {
    fn from(value: PublishCommand) -> Self {
        Self {
            bundle: value.bundle,
            checkout: value.checkout,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct GenerateArgs {
    #[arg(long, default_value = "schema")]
    pub schema_directory: PathBuf,
    #[arg(long, default_value = "website/src/generated/artifacts.ts")]
    pub typescript: PathBuf,
    #[arg(long)]
    pub check: bool,
}

#[derive(Debug, Clone, Args)]
pub struct CleanupArgs {
    #[arg(long)]
    pub session: String,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use crate::config::Task;
    use clap::Parser;

    #[test]
    fn parses_a_workload_command() {
        let cli = Cli::try_parse_from([
            "slatedb-benchmark",
            "run",
            "--workload",
            "balanced",
            "--golden",
            "golden",
            "--session",
            "github-123456",
            "--scale",
            "0.01",
            "--output",
            ".runs/balanced",
        ])
        .expect("parse command");

        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.workload, Task::Balanced);
        assert_eq!(args.session, "github-123456");
        assert_eq!(args.scale.factor(), 0.01);
    }
}
