#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: publish-results.sh <run-directory> <version> <profile> <publish-checkout>" >&2
  exit 2
fi

run_directory=$1
version=$2
profile=$3
publish_checkout=$4

if [[ "$version" == */* || "$profile" == */* ]]; then
  echo "version and profile must be single path components" >&2
  exit 2
fi

source_directory="$run_directory/results/$version/$profile"
destination_directory="$publish_checkout/results/$version/$profile"

if [[ ! -d "$source_directory" ]]; then
  echo "validated profile results not found at $source_directory" >&2
  exit 1
fi
if [[ ! -d "$publish_checkout/.git" ]]; then
  echo "publication checkout not found at $publish_checkout" >&2
  exit 1
fi

mkdir -p "$destination_directory"
cp -R "$source_directory/." "$destination_directory/"

git -C "$publish_checkout" add "results/$version/$profile"
if git -C "$publish_checkout" diff --cached --quiet; then
  echo "SlateDB $version $profile results are already published"
  exit 0
fi

git -C "$publish_checkout" config user.name "slatedb-benchmark[bot]"
git -C "$publish_checkout" config user.email "slatedb-benchmark[bot]@users.noreply.github.com"
git -C "$publish_checkout" commit -m "Publish SlateDB $version $profile benchmarks"

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
  echo "::warning::main advanced during $profile publication attempt $attempt; retrying"
done

echo "::error::main advanced during all $profile publication attempts"
exit 1
