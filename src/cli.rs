use crate::config::{BenchmarkScale, Task};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

const RUN_EXAMPLES: &str = r#"Examples:
  slatedb-benchmark run --task bulk-load --golden slatedb-v0.13.1-001 \
    --scale 1.0 --output .runs/bulk-load

  slatedb-benchmark run --task compaction --golden slatedb-v0.13.1-001 \
    --scale 1.0 --output .runs/compaction

  slatedb-benchmark run --task balanced --golden slatedb-v0.13.1-001 \
    --session github-123456 --scale 1.0 --output .runs/balanced"#;

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
#[command(about = "Run SlateDB benchmarks")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run one preparation phase or workload.
    Run(RunArgs),
}

#[derive(Debug, Clone, Args)]
#[command(after_help = RUN_EXAMPLES)]
pub struct RunArgs {
    /// Preparation task or workload from BENCHMARKS.md.
    #[arg(long, value_enum)]
    pub task: Task,
    /// Golden data name, for example slatedb-v0.13.1-001.
    #[arg(long, value_name = "GOLDEN_ID")]
    pub golden: String,
    /// Benchmark session name; required for workload tasks.
    #[arg(long)]
    pub session: Option<String>,
    /// Decimal scale factor greater than 0 and at most 1.0.
    #[arg(long, default_value = "1.0", value_name = "FACTOR")]
    pub scale: BenchmarkScale,
    /// Local result and diagnostic directory.
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use crate::config::Task;
    use clap::Parser;

    #[test]
    fn parses_the_documented_workload_command() {
        let cli = Cli::try_parse_from([
            "slatedb-benchmark",
            "run",
            "--task",
            "balanced",
            "--golden",
            "slatedb-v0.13.1-001",
            "--session",
            "github-123456",
            "--scale",
            "0.01",
            "--output",
            ".runs/balanced",
        ])
        .expect("parse command");

        let Command::Run(args) = cli.command;
        assert_eq!(args.task, Task::Balanced);
        assert_eq!(args.session.as_deref(), Some("github-123456"));
        assert_eq!(args.scale.factor(), 0.01);
    }
}
