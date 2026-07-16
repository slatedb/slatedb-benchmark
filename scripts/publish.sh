#!/usr/bin/env bash
# Publishes one validated suite from a run directory to the results checkout.
# Usage: publish.sh <run-directory> <version> <suite> <publish-checkout>
# Replaces that suite's published results and retries the push when another
# publisher advances main concurrently. Website builds run in the Pages workflow.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: publish.sh <run-directory> <version> <suite> <publish-checkout>" >&2
  exit 2
fi

run_directory=$1
version=$2
suite=$3
publish_checkout=$4

if [[ -z "$version" || "$version" == "." || "$version" == ".." || "$version" =~ [^A-Za-z0-9._-] ||
      -z "$suite" || "$suite" == "." || "$suite" == ".." || "$suite" =~ [^A-Za-z0-9._-] ]]; then
  echo "version and suite must contain only letters, digits, '.', '-', or '_'" >&2
  exit 2
fi

source_directory="$run_directory/results/$version/$suite"
destination_directory="$publish_checkout/results/$version/$suite"
run_manifest="$run_directory/run.json"

if [[ ! -f "$run_manifest" ]]; then
  echo "run manifest not found at $run_manifest" >&2
  exit 1
fi
python3 - "$run_manifest" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    run = json.load(file)
if run.get("mode") != "published" or run.get("scale", 1) != 1:
    raise SystemExit("refusing to publish scaled or non-published benchmark results")
PY
if [[ ! -d "$source_directory" ]]; then
  echo "validated suite results not found at $source_directory" >&2
  exit 1
fi
if [[ ! -d "$publish_checkout/.git" ]]; then
  echo "publication checkout not found at $publish_checkout" >&2
  exit 1
fi

rm -rf "$destination_directory"
mkdir -p "$destination_directory"
cp -R "$source_directory/." "$destination_directory/"

git -C "$publish_checkout" add "results/$version/$suite"
if git -C "$publish_checkout" diff --cached --quiet; then
  echo "SlateDB $version $suite results are already published"
  exit 0
fi

git -C "$publish_checkout" config user.name "slatedb-benchmark[bot]"
git -C "$publish_checkout" config user.email "slatedb-benchmark[bot]@users.noreply.github.com"
git -C "$publish_checkout" commit -m "Publish SlateDB $version $suite benchmarks"

# Rebase before every push so concurrent suite publications are retained.
for attempt in 1 2 3 4 5; do
  git -C "$publish_checkout" fetch origin main
  git -C "$publish_checkout" rebase origin/main
  if git -C "$publish_checkout" push origin HEAD:main; then
    exit 0
  fi
  echo "::warning::main advanced during $suite publication attempt $attempt; retrying"
done

echo "::error::main advanced during all $suite publication attempts"
exit 1
