#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: publish.sh <bundle-results-directory> <publish-checkout>" >&2
  exit 2
fi

bundle_root=$1
publish_checkout=$2
mapfile -t manifests < <(find "$bundle_root" -mindepth 2 -maxdepth 2 -name run.json -type f)
if [[ ${#manifests[@]} -ne 1 ]]; then
  echo "expected one versioned run.json under $bundle_root" >&2
  exit 1
fi
run_manifest=${manifests[0]}
version=$(basename "$(dirname "$run_manifest")")
if [[ -z "$version" || "$version" == "." || "$version" == ".." || "$version" =~ [^A-Za-z0-9._-] ]]; then
  echo "invalid result version $version" >&2
  exit 2
fi
if [[ ! -d "$publish_checkout/.git" ]]; then
  echo "publication checkout not found at $publish_checkout" >&2
  exit 1
fi

python3 - "$run_manifest" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest = Path(sys.argv[1])
with manifest.open(encoding="utf-8") as file:
    run = json.load(file)
preparation = {"bulk-load", "full-compaction"}
workloads = {
    "idle",
    "point-read-uniform",
    "point-read-skewed",
    "point-read-missing",
    "read-heavy",
    "balanced",
    "update-heavy",
    "range-scan",
    "sustained-ingest",
    "transaction-contention",
}
tasks = preparation | workloads
configurations = run.get("resolved_configuration", {})
if set(configurations) != tasks:
    raise SystemExit("refusing to publish an incomplete benchmark run")
if any(configuration.get("scale") != 1.0 for configuration in configurations.values()):
    raise SystemExit("refusing to publish scaled benchmark results")
expected = {
    f"{'preparation' if task in preparation else 'workload'}/{task}/result.json"
    for task in tasks
}
results = run.get("results", {})
if set(results) != expected:
    raise SystemExit("refusing to publish a run with missing results")
for relative, expected_digest in results.items():
    path = manifest.parent / relative
    contents = path.read_bytes()
    if hashlib.sha256(contents).hexdigest() != expected_digest:
        raise SystemExit(f"checksum mismatch for {relative}")
    result = json.loads(contents)
    task = path.parent.name
    if (
        result.get("status") != "ok"
        or result.get("task") != task
        or result.get("golden_id") != run.get("golden_id")
        or result.get("configuration", {}).get("scale") != 1.0
        or result.get("source", {}).get("slate_commit")
        != run.get("source", {}).get("slate_commit")
    ):
        raise SystemExit(f"invalid result metadata for {relative}")
    if task in workloads and result.get("source", {}).get("runner_commit") != run.get(
        "source", {}
    ).get("runner_commit"):
        raise SystemExit(f"runner commit mismatch for {relative}")
PY

source_directory=$(dirname "$run_manifest")
destination_directory="$publish_checkout/results/$version"
rm -rf "$destination_directory"
mkdir -p "$destination_directory"
cp -R "$source_directory/." "$destination_directory/"

git -C "$publish_checkout" add "results/$version"
if git -C "$publish_checkout" diff --cached --quiet; then
  echo "SlateDB $version benchmark results are already published"
  exit 0
fi

git -C "$publish_checkout" config user.name "slatedb-benchmark[bot]"
git -C "$publish_checkout" config user.email "slatedb-benchmark[bot]@users.noreply.github.com"
git -C "$publish_checkout" commit -m "Publish SlateDB $version benchmarks"

for attempt in 1 2 3 4 5; do
  git -C "$publish_checkout" fetch origin main
  git -C "$publish_checkout" rebase origin/main
  if git -C "$publish_checkout" push origin HEAD:main; then
    exit 0
  fi
  echo "::warning::main advanced during publication attempt $attempt; retrying"
done

echo "::error::main advanced during all publication attempts"
exit 1
