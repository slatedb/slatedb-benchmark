use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-env-changed=SLATEDB_VERSION");
    println!("cargo:rerun-if-env-changed=SLATEDB_COMMIT");

    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    emit_git_rerun_paths(&root);
    let lock_hash = fs::read(root.join("Cargo.lock")).map_or_else(
        |_| "unavailable".to_string(),
        |bytes| format!("{:x}", Sha256::digest(bytes)),
    );
    println!("cargo:rustc-env=BENCHMARK_LOCK_HASH={lock_hash}");

    let runner_commit =
        git_output(&root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=BENCHMARK_RUNNER_COMMIT={runner_commit}");

    let enabled_features = slatedb_features(&root);
    println!(
        "cargo:rustc-env=BENCHMARK_ENABLED_FEATURES={}",
        enabled_features.join(",")
    );

    let version = std::env::var("SLATEDB_VERSION").unwrap_or_else(|_| "unknown".to_string());
    let commit = std::env::var("SLATEDB_COMMIT").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=BENCHMARK_SLATE_VERSION={version}");
    println!("cargo:rustc-env=BENCHMARK_SLATE_COMMIT={commit}");
}

fn slatedb_features(root: &std::path::Path) -> Vec<String> {
    let manifest_path = root.join("Cargo.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("read Cargo.toml");
    let manifest: toml::Value = toml::from_str(&manifest).expect("parse Cargo.toml");
    let dependency = manifest
        .get("dependencies")
        .and_then(|dependencies| dependencies.get("slatedb"))
        .and_then(toml::Value::as_table)
        .expect("slatedb dependency must use table form");
    assert_eq!(
        dependency
            .get("default-features")
            .and_then(toml::Value::as_bool),
        Some(false),
        "slatedb default features must be disabled so provenance is explicit"
    );
    let mut features = dependency
        .get("features")
        .and_then(toml::Value::as_array)
        .expect("slatedb dependency must declare features")
        .iter()
        .map(|feature| {
            feature
                .as_str()
                .expect("slatedb feature must be a string")
                .to_string()
        })
        .collect::<Vec<_>>();
    features.sort();
    features.dedup();
    features
}

fn emit_git_rerun_paths(root: &std::path::Path) {
    for name in ["HEAD", "packed-refs"] {
        emit_git_rerun_path(root, name);
    }
    if let Some(reference) = git_output(root, &["symbolic-ref", "-q", "HEAD"]) {
        emit_git_rerun_path(root, &reference);
    }
}

fn emit_git_rerun_path(root: &std::path::Path, name: &str) {
    if let Some(path) = git_output(root, &["rev-parse", "--git-path", name]) {
        let path = PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            root.join(path)
        };
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_output(root: &std::path::Path, args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|output| !output.is_empty())
}
