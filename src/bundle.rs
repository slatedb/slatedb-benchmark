//! Assembly of validated workload outputs into a publishable run directory.

use crate::config::Task;
use crate::model::{
    AppliedPatch, GoldenManifest, RunManifest, SourceIdentity, WorkloadResult, WorkloadSeries,
};
use crate::validation::{
    validate_golden_manifest, validate_identifier, validate_run_manifest, validate_workload_result,
    validate_workload_series,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Inputs used to assemble a complete benchmark run bundle.
pub struct BundleArgs {
    /// Directory containing `golden.json` and per-workload result directories.
    pub input: PathBuf,
    /// Root under which the versioned run directory will be created.
    pub output: PathBuf,
    /// Identifier of the golden dataset used by the run.
    pub golden: String,
    /// RFC 3339 timestamp captured when the run began.
    pub started_at: String,
    /// Directory containing SlateDB patches applied to the run.
    pub patches: PathBuf,
}

/// Validates the supplied artifacts and writes a self-contained run bundle.
///
/// The returned path is `<output>/<slatedb-version>/<session>`.
pub fn bundle(args: BundleArgs) -> Result<PathBuf> {
    validate_identifier(&args.golden, "golden ID")?;
    let golden_path = args.input.join("golden.json");
    let golden: GoldenManifest = read_json(&golden_path)?;
    validate_golden_manifest(&golden)
        .with_context(|| format!("validating {}", golden_path.display()))?;
    ensure!(
        golden.golden_id == args.golden,
        "{} belongs to another golden data set",
        golden_path.display()
    );
    ensure!(
        golden.configuration.task.task == Task::Compaction,
        "{} is not the final compacted golden manifest",
        golden_path.display()
    );

    let tasks = discover_workloads(&args.input.join("workload"))?;
    let mut workloads = BTreeMap::new();
    let mut series = BTreeMap::new();
    for task in tasks {
        let directory = args.input.join("workload").join(task.as_str());
        let result_path = directory.join("result.json");
        let result: WorkloadResult = read_json(&result_path)?;
        validate_workload_result(&result)
            .with_context(|| format!("validating {}", result_path.display()))?;
        ensure!(
            result.task == task,
            "{} contains task {}, expected {}",
            result_path.display(),
            result.task,
            task
        );
        ensure!(
            result.golden_id == args.golden,
            "{} belongs to another golden data set",
            result_path.display()
        );
        let series_path = directory.join("series.json");
        ensure!(
            sha256_file(&series_path)? == result.series.sha256,
            "{} does not match its result digest",
            series_path.display()
        );
        let workload_series: WorkloadSeries = read_json(&series_path)?;
        validate_workload_series(&result, &workload_series)
            .with_context(|| format!("validating {}", series_path.display()))?;
        workloads.insert(task, result);
        series.insert(task, series_path);
    }

    let first = workloads
        .values()
        .next()
        .context("no workload results were provided")?;
    let source = validate_source_identities(&workloads)?;
    validate_identifier(&source.slate_version, "SlateDB version")?;
    validate_identifier(&first.session, "run ID")?;
    let scale = first.configuration.scale;
    for (task, result) in &workloads {
        ensure!(
            result.session == first.session,
            "workload results belong to different sessions"
        );
        ensure!(
            result.configuration.scale == scale,
            "{task} used a different scale"
        );
        if *task == Task::SustainedIngest {
            ensure!(
                result.initial_state.kind == "empty",
                "sustained-ingest did not start empty"
            );
        } else {
            ensure!(
                result.initial_state.checkpoint_id.as_deref()
                    == Some(golden.checkpoint.checkpoint_id.as_str())
                    && result.initial_state.manifest_id == Some(golden.checkpoint.manifest_id)
                    && result.initial_state.lsm_digest_sha256
                        == golden.checkpoint.lsm_digest_sha256,
                "{task} did not start from the golden checkpoint"
            );
        }
    }
    ensure!(
        golden.configuration.scale == scale,
        "golden manifest used a different scale"
    );

    let destination = args.output.join(&source.slate_version).join(&first.session);
    if destination.exists() {
        fs::remove_dir_all(&destination)
            .with_context(|| format!("removing {}", destination.display()))?;
    }
    fs::create_dir_all(&destination)
        .with_context(|| format!("creating {}", destination.display()))?;

    let golden_target = destination.join("golden.json");
    write_json(&golden_target, &golden)?;
    let mut results = BTreeMap::from([("golden.json".to_string(), sha256_file(&golden_target)?)]);
    let mut resolved_configuration =
        BTreeMap::from([("golden".to_string(), golden.configuration.clone())]);
    for (task, result) in &workloads {
        let relative = format!("workload/{}/result.json", task.as_str());
        let target = destination.join(&relative);
        write_json(&target, result)?;
        results.insert(relative, sha256_file(&target)?);
        resolved_configuration.insert(task.as_str().to_string(), result.configuration.clone());

        let relative = format!("workload/{}/series.json", task.as_str());
        let target = destination.join(&relative);
        fs::copy(&series[task], &target).with_context(|| {
            format!("copying {} to {}", series[task].display(), target.display())
        })?;
        results.insert(relative, sha256_file(&target)?);
    }

    let manifest = RunManifest {
        status: "ok".to_string(),
        run_id: first.session.clone(),
        golden_id: args.golden,
        started_at: args.started_at,
        finished_at: Utc::now().to_rfc3339(),
        patches: read_patches(&args.patches)?,
        source,
        golden_runner_commit: golden.source.runner_commit.clone(),
        resolved_configuration,
        max_parallel: workloads.len(),
        results,
    };
    validate_run_manifest(&manifest)?;
    write_json(&destination.join("run.json"), &manifest)?;
    Ok(destination)
}

fn discover_workloads(directory: &Path) -> Result<Vec<Task>> {
    ensure!(
        directory.is_dir(),
        "{} does not contain workload results",
        directory.display()
    );
    let allowed = Task::workloads()
        .map(|task| (task.as_str(), task))
        .collect::<BTreeMap<_, _>>();
    let mut discovered = BTreeSet::new();
    for entry in
        fs::read_dir(directory).with_context(|| format!("reading {}", directory.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.path().join("result.json").is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let task = allowed.get(name.as_ref()).copied().with_context(|| {
                format!("{} contains unknown workload {name}", directory.display())
            })?;
            discovered.insert(task);
        }
    }
    ensure!(
        !discovered.is_empty(),
        "{} does not contain any workload results",
        directory.display()
    );
    Ok(Task::workloads()
        .filter(|task| discovered.contains(task))
        .collect())
}

fn validate_source_identities(
    workloads: &BTreeMap<Task, WorkloadResult>,
) -> Result<SourceIdentity> {
    let first = workloads
        .values()
        .next()
        .context("no workload results were provided")?;
    for (task, result) in workloads {
        ensure!(
            result.source.slate_commit == first.source.slate_commit,
            "{task} used a different SlateDB commit"
        );
        ensure!(
            result.source.runner_commit == first.source.runner_commit,
            "{task} used a different benchmark runner commit"
        );
    }
    Ok(first.source.clone())
}

fn read_patches(directory: &Path) -> Result<Vec<AppliedPatch>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(directory)
        .with_context(|| format!("reading {}", directory.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file() && path.extension().is_some_and(|value| value == "patch"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            Ok(AppliedPatch {
                name: path
                    .file_name()
                    .context("patch has no file name")?
                    .to_string_lossy()
                    .into_owned(),
                sha256: sha256_file(&path)?,
            })
        })
        .collect()
}

/// Computes the lowercase SHA-256 digest of a file.
pub fn sha256_file(path: &Path) -> Result<String> {
    let contents = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(contents)))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    serde_json::from_reader(file).with_context(|| format!("parsing {}", path.display()))
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut contents = serde_json::to_vec_pretty(value)?;
    contents.push(b'\n');
    fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{discover_workloads, read_patches};
    use crate::config::Task;
    use std::fs;

    #[test]
    fn discovers_a_workload_subset_in_catalog_order() {
        let temporary = tempfile::tempdir().expect("create temporary directory");
        for name in ["sustained-ingest", "balanced"] {
            let path = temporary.path().join(name);
            fs::create_dir(&path).expect("create workload directory");
            fs::write(path.join("result.json"), "{}").expect("write workload result");
        }
        assert_eq!(
            discover_workloads(temporary.path()).expect("discover workloads"),
            vec![Task::Balanced, Task::SustainedIngest]
        );
    }

    #[test]
    fn records_patches_in_filename_order() {
        let temporary = tempfile::tempdir().expect("create temporary directory");
        fs::write(temporary.path().join("0002.patch"), "second").expect("write second patch");
        fs::write(temporary.path().join("0001.patch"), "first").expect("write first patch");
        fs::write(temporary.path().join("README.md"), "ignored").expect("write ignored file");
        let patches = read_patches(temporary.path()).expect("read patches");
        assert_eq!(
            patches
                .iter()
                .map(|patch| patch.name.as_str())
                .collect::<Vec<_>>(),
            ["0001.patch", "0002.patch"]
        );
    }
}
