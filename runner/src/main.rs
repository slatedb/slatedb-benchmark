use anyhow::Result;
use clap::Parser;
use slatedb_benchmark::cli::{Cli, Command};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .without_time()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => slatedb_benchmark::execute(args).await,
        Command::Validate(args) => {
            slatedb_benchmark::validation::validate_output(&args.output)?;
            println!(
                "{{\"status\":\"ok\",\"validated\":\"{}\"}}",
                args.output.display()
            );
            Ok(())
        }
        Command::Catalog(args) => {
            let benchmark =
                slatedb_benchmark::config::BenchmarkConfig::load_from(&args.config_dir)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&benchmark.catalog(args.suite.as_deref())?)?
            );
            Ok(())
        }
        Command::Worker(args) => slatedb_benchmark::runner::execute_worker(args).await,
    }
}
