use crate::bundle::sha256_file;
use crate::model::RunManifest;
use crate::validation::{validate_identifier, validate_run_manifest};
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct PublishArgs {
    pub bundle: PathBuf,
    pub checkout: PathBuf,
}

pub fn publish(args: PublishArgs) -> Result<()> {
    let manifest_path = find_manifest(&args.bundle)?;
    let manifest: RunManifest = serde_json::from_reader(
        fs::File::open(&manifest_path)
            .with_context(|| format!("opening {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parsing {}", manifest_path.display()))?;
    validate_run_manifest(&manifest)?;
    validate_identifier(&manifest.source.slate_version, "SlateDB version")?;
    validate_identifier(&manifest.run_id, "run ID")?;
    ensure!(
        manifest
            .resolved_configuration
            .values()
            .all(|configuration| configuration.scale == 1.0),
        "refusing to publish a scaled benchmark run"
    );
    let source = manifest_path
        .parent()
        .context("run manifest has no parent")?;
    ensure!(
        source.file_name().and_then(|name| name.to_str()) == Some(&manifest.run_id),
        "benchmark run directory does not match run.json"
    );
    ensure!(
        source
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(&manifest.source.slate_version),
        "benchmark version directory does not match run.json"
    );
    ensure!(
        args.checkout.join(".git").is_dir(),
        "publication checkout not found at {}",
        args.checkout.display()
    );
    validate_bundle_files(source, &manifest)?;

    let relative = PathBuf::from("results")
        .join(&manifest.source.slate_version)
        .join(&manifest.run_id);
    let destination = args.checkout.join(&relative);
    if destination.exists() {
        fs::remove_dir_all(&destination)
            .with_context(|| format!("removing {}", destination.display()))?;
    }
    copy_directory(source, &destination)?;

    git(
        &args.checkout,
        ["add", relative.to_str().context("non-UTF-8 result path")?],
    )?;
    if git_status(&args.checkout, ["diff", "--cached", "--quiet"])?.success() {
        println!(
            "SlateDB {} benchmark results are already published",
            manifest.source.slate_version
        );
        return Ok(());
    }
    git(
        &args.checkout,
        ["config", "user.name", "slatedb-benchmark[bot]"],
    )?;
    git(
        &args.checkout,
        [
            "config",
            "user.email",
            "slatedb-benchmark[bot]@users.noreply.github.com",
        ],
    )?;
    git(
        &args.checkout,
        [
            "commit",
            "-m",
            &format!(
                "Publish SlateDB {} benchmark run {}",
                manifest.source.slate_version, manifest.run_id
            ),
        ],
    )?;
    for attempt in 1..=5 {
        git(&args.checkout, ["fetch", "origin", "main"])?;
        git(&args.checkout, ["rebase", "origin/main"])?;
        if git_status(&args.checkout, ["push", "origin", "HEAD:main"])?.success() {
            return Ok(());
        }
        eprintln!("main advanced during publication attempt {attempt}; retrying");
    }
    bail!("main advanced during all publication attempts")
}

fn find_manifest(root: &Path) -> Result<PathBuf> {
    let mut manifests = Vec::new();
    visit_files(root, &mut |path| {
        if path.file_name().is_some_and(|name| name == "run.json") {
            manifests.push(path.to_path_buf());
        }
        Ok(())
    })?;
    ensure!(
        manifests.len() == 1,
        "expected one versioned benchmark run under {}",
        root.display()
    );
    Ok(manifests.remove(0))
}

fn validate_bundle_files(source: &Path, manifest: &RunManifest) -> Result<()> {
    let expected = manifest.results.keys().cloned().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    visit_files(source, &mut |path| {
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
            && path.file_name().is_none_or(|name| name != "run.json")
        {
            actual.insert(
                path.strip_prefix(source)?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
        Ok(())
    })?;
    ensure!(actual == expected, "bundle files do not match run.json");
    for (relative, expected_digest) in &manifest.results {
        ensure!(
            sha256_file(&source.join(relative))? == *expected_digest,
            "{relative} does not match run.json"
        );
    }
    Ok(())
}

fn copy_directory(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("creating {}", destination.display()))?;
    for entry in fs::read_dir(source).with_context(|| format!("reading {}", source.display()))? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target)
                .with_context(|| format!("copying {}", entry.path().display()))?;
        }
    }
    Ok(())
}

fn visit_files(root: &Path, visitor: &mut impl FnMut(&Path) -> Result<()>) -> Result<()> {
    for entry in fs::read_dir(root).with_context(|| format!("reading {}", root.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            visit_files(&entry.path(), visitor)?;
        } else {
            visitor(&entry.path())?;
        }
    }
    Ok(())
}

fn git<const N: usize>(checkout: &Path, args: [&str; N]) -> Result<()> {
    let status = git_status(checkout, args)?;
    ensure!(status.success(), "git command failed");
    Ok(())
}

fn git_status<const N: usize>(
    checkout: &Path,
    args: [&str; N],
) -> Result<std::process::ExitStatus> {
    Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(args)
        .status()
        .context("running git")
}
