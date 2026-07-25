//! Reproducible benchmark execution, artifact validation, and publication for SlateDB.
//!
//! The crate owns the workload catalog and artifact contracts used by both the
//! command-line runner and the benchmark website. A typical run prepares a
//! golden database, executes one or more workloads, bundles their validated
//! artifacts, and publishes the bundle to the website repository.

#![warn(missing_docs)]

/// Validates and assembles individual workload artifacts into a run bundle.
pub mod bundle;
/// Command-line argument and subcommand definitions.
pub mod cli;
/// Benchmark workload, cache, and SlateDB configuration resolution.
pub mod config;
/// JSON Schema and TypeScript contract generation.
pub mod contracts;
mod database_size;
mod histogram;
mod instrumented_http;
mod instrumented_store;
/// Serializable benchmark artifact types.
pub mod model;
mod object_store;
/// Publication of validated run bundles to the website repository.
pub mod publish;
/// Benchmark preparation and workload execution.
pub mod runner;
mod system;
/// Semantic validation for all published artifact types.
pub mod validation;
mod workloads;

/// Executes one preparation phase or workload.
pub use runner::execute;
