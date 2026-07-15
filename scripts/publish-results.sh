#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: publish-results.sh <run-directory> <version> <suite> <workload> <publish-checkout>" >&2
  exit 2
fi

run_directory=$1
version=$2
suite=$3
workload=$4
publish_checkout=$5

if [[ -z "$version" || "$version" == "." || "$version" == ".." || "$version" =~ [^A-Za-z0-9._-] ||
      -z "$suite" || "$suite" == "." || "$suite" == ".." || "$suite" =~ [^A-Za-z0-9._-] ||
      -z "$workload" || "$workload" == "." || "$workload" == ".." || "$workload" =~ [^A-Za-z0-9._-] ]]; then
  echo "version, suite, and workload must contain only letters, digits, '.', '-', or '_'" >&2
  exit 2
fi

source_directory="$run_directory/results/$version/$suite/$workload"
destination_directory="$publish_checkout/results/$version/$suite/$workload"

if [[ ! -d "$source_directory" ]]; then
  echo "validated workload results not found at $source_directory" >&2
  exit 1
fi
if [[ ! -d "$publish_checkout/.git" ]]; then
  echo "publication checkout not found at $publish_checkout" >&2
  exit 1
fi

rm -rf "$destination_directory"
mkdir -p "$destination_directory"
cp -R "$source_directory/." "$destination_directory/"

git -C "$publish_checkout" add "results/$version/$suite/$workload"
if git -C "$publish_checkout" diff --cached --quiet; then
  echo "SlateDB $version $suite/$workload results are already published"
  exit 0
fi

git -C "$publish_checkout" config user.name "slatedb-benchmark[bot]"
git -C "$publish_checkout" config user.email "slatedb-benchmark[bot]@users.noreply.github.com"
git -C "$publish_checkout" commit -m "Publish SlateDB $version $suite/$workload benchmarks"

for attempt in 1 2 3 4 5; do
  git -C "$publish_checkout" fetch origin main
  git -C "$publish_checkout" rebase origin/main
  (
    cd "$publish_checkout/site"
    npm ci
    npm run build
  )
  if git -C "$publish_checkout" push origin HEAD:main; then
    exit 0
  fi
  echo "::warning::main advanced during $suite/$workload publication attempt $attempt; retrying"
done

echo "::error::main advanced during all $suite/$workload publication attempts"
exit 1
