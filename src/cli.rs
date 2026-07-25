//! Command-line interface definitions shared by the binary and tests.

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
/// Top-level command-line parser for the benchmark executable.
pub struct Cli {
    /// Operation requested by the caller.
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
/// Commands supported by the benchmark executable.
pub enum Command {
    /// Prepare the bulk-load or compacted golden data set.
    Prepare(PrepareArgs),
    /// Run one workload.
    Run(RunArgs),
    /// Assemble validated results into a versioned run bundle.
    Bundle(BundleCommand),
    /// Validate one benchmark artifact.
    Validate(ValidateArgs),
    /// Publish one run bundle to the website checkout.
    Publish(PublishCommand),
    /// Generate JSON Schema and TypeScript contracts from Rust types.
    Generate(GenerateArgs),
    /// Print the workload catalog as a JSON array.
    Catalog,
    /// Remove all object-store data for one benchmark session.
    Cleanup(CleanupArgs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
/// Golden-dataset preparation phases.
pub enum PreparationPhase {
    /// Load the source records into an uncompacted database.
    BulkLoad,
    /// Compact the bulk-loaded database and publish its checkpoint.
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
/// Arguments for a golden-dataset preparation phase.
pub struct PrepareArgs {
    /// Preparation phase to execute.
    #[arg(long, value_enum)]
    pub phase: PreparationPhase,
    /// Stable identifier for the golden dataset.
    #[arg(long, value_name = "GOLDEN_ID")]
    pub golden: String,
    /// Fraction applied to dataset size and duration for local runs.
    #[arg(long, default_value = "1.0", value_name = "FACTOR")]
    pub scale: BenchmarkScale,
    /// Directory in which to write the local artifact.
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
    /// Required idle time after the final compaction before recording the golden manifest.
    #[arg(long, default_value_t = 60_000)]
    pub compaction_quiet_ms: u64,
}

#[derive(Debug, Clone, Args)]
/// Arguments for executing one workload.
pub struct RunArgs {
    /// Workload to execute.
    #[arg(long, value_enum)]
    pub workload: Task,
    /// Golden dataset to clone for workloads that require one.
    #[arg(long, value_name = "GOLDEN_ID")]
    pub golden: String,
    /// Identifier shared by all workloads in the benchmark run.
    #[arg(long)]
    pub session: String,
    /// Fraction applied to dataset size and duration for local runs.
    #[arg(long, default_value = "1.0", value_name = "FACTOR")]
    pub scale: BenchmarkScale,
    /// Directory in which to write `result.json` and `series.json`.
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
}

#[derive(Debug, Clone, Args)]
/// Arguments for assembling individual artifacts into a run bundle.
pub struct BundleCommand {
    /// Directory containing the golden and workload artifacts.
    #[arg(long)]
    pub input: PathBuf,
    /// Root directory for the versioned bundle.
    #[arg(long)]
    pub output: PathBuf,
    /// Identifier of the golden dataset used by the run.
    #[arg(long)]
    pub golden: String,
    /// RFC 3339 timestamp captured when the run began.
    #[arg(long)]
    pub started_at: String,
    /// Directory containing the SlateDB patches applied to the run.
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
/// Artifact type accepted by the validation command.
pub enum Artifact {
    /// A compacted golden-dataset manifest.
    Golden,
    /// A summarized workload result.
    Result,
    /// A workload time-series sidecar.
    Series,
    /// A bundled-run manifest.
    Run,
}

#[derive(Debug, Clone, Args)]
/// Arguments for validating one serialized artifact.
pub struct ValidateArgs {
    /// Contract and semantic rules to apply.
    #[arg(long, value_enum)]
    pub artifact: Artifact,
    /// JSON artifact to validate.
    #[arg(long)]
    pub input: PathBuf,
    /// Paired result.json, required when validating series.json.
    #[arg(long)]
    pub result: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
/// Arguments for publishing a run bundle.
pub struct PublishCommand {
    /// Directory containing exactly one versioned run bundle.
    #[arg(long)]
    pub bundle: PathBuf,
    /// Git checkout containing the benchmark website.
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
/// Arguments for generating or checking artifact contracts.
pub struct GenerateArgs {
    /// Destination directory for generated JSON Schema documents.
    #[arg(long, default_value = "schema")]
    pub schema_directory: PathBuf,
    /// Destination file for generated TypeScript declarations.
    #[arg(long, default_value = "website/src/generated/artifacts.ts")]
    pub typescript: PathBuf,
    /// Verify checked-in files instead of rewriting them.
    #[arg(long)]
    pub check: bool,
}

#[derive(Debug, Clone, Args)]
/// Arguments for deleting one benchmark session from object storage.
pub struct CleanupArgs {
    /// Session whose workload database prefixes should be removed.
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
