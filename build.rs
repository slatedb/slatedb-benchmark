use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-env-changed=SLATEDB_VERSION");
    println!("cargo:rerun-if-env-changed=SLATEDB_COMMIT");

    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let lock_hash = fs::read(root.join("Cargo.lock")).map_or_else(
        |_| "unavailable".to_string(),
        |bytes| format!("{:x}", Sha256::digest(bytes)),
    );
    println!("cargo:rustc-env=BENCHMARK_LOCK_HASH={lock_hash}");

    let runner_commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        );
    println!("cargo:rustc-env=BENCHMARK_RUNNER_COMMIT={runner_commit}");

    let version = std::env::var("SLATEDB_VERSION").unwrap_or_else(|_| "0.14.1".to_string());
    let commit = std::env::var("SLATEDB_COMMIT").unwrap_or_else(|_| "v0.14.1".to_string());
    println!("cargo:rustc-env=BENCHMARK_SLATE_VERSION={version}");
    println!("cargo:rustc-env=BENCHMARK_SLATE_COMMIT={commit}");
}
