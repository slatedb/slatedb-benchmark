use anyhow::{bail, Context, Result};
use clap::Parser;
use slatedb_benchmark::cli::{Artifact, Cli, Command};
use slatedb_benchmark::model::{WorkloadResult, WorkloadSeries};
use slatedb_benchmark::runner::ExecutionArgs;
use slatedb_benchmark::validation::{
    validate_golden_manifest, validate_run_manifest, validate_workload_result,
    validate_workload_series,
};
use std::fs;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Prepare(args) => {
            slatedb_benchmark::execute(ExecutionArgs {
                task: args.phase.into(),
                golden: args.golden,
                session: None,
                scale: args.scale,
                output: args.output,
                compaction_quiet: Duration::from_millis(args.compaction_quiet_ms),
            })
            .await
        }
        Command::Run(args) => {
            if args.workload.is_preparation() {
                bail!("--workload must name a workload; use `prepare` for preparation phases");
            }
            slatedb_benchmark::execute(ExecutionArgs {
                task: args.workload,
                golden: args.golden,
                session: Some(args.session),
                scale: args.scale,
                output: args.output,
                compaction_quiet: Duration::from_secs(60),
            })
            .await
        }
        Command::Bundle(args) => {
            println!(
                "{}",
                slatedb_benchmark::bundle::bundle(args.into())?.display()
            );
            Ok(())
        }
        Command::Validate(args) => validate(args),
        Command::Publish(args) => slatedb_benchmark::publish::publish(args.into()),
        Command::Generate(args) => slatedb_benchmark::contracts::generate(
            &args.schema_directory,
            &args.typescript,
            args.check,
        ),
        Command::Catalog => {
            let workloads = slatedb_benchmark::config::Task::workloads()
                .map(|task| task.as_str())
                .collect::<Vec<_>>();
            println!("{}", serde_json::to_string(&workloads)?);
            Ok(())
        }
        Command::Cleanup(args) => slatedb_benchmark::runner::cleanup_session(&args.session).await,
    }
}

fn validate(args: slatedb_benchmark::cli::ValidateArgs) -> Result<()> {
    match args.artifact {
        Artifact::Golden => validate_golden_manifest(&read(&args.input)?),
        Artifact::Result => validate_workload_result(&read(&args.input)?),
        Artifact::Run => validate_run_manifest(&read(&args.input)?),
        Artifact::Series => {
            let result_path = args
                .result
                .context("--result is required for a series artifact")?;
            let result: WorkloadResult = read(&result_path)?;
            let series: WorkloadSeries = read(&args.input)?;
            validate_workload_result(&result)?;
            validate_workload_series(&result, &series)
        }
    }
}

fn read<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T> {
    let file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parsing {}", path.display()))
}
