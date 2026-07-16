#!/usr/bin/env bash
# Generates local website fixtures by running every release suite at scale.
# Usage: ./scripts/generate-scaled-fixtures.sh
# Set BENCHMARK_SCALE to override the default 0.01% scale.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

scale=${BENCHMARK_SCALE:-0.0001}
working_root=.runs/scaled-fixtures
fixture_root=.scaled-results

rm -rf "$working_root" "$fixture_root"
docker compose build runner
suites=(rocksdb slatedb ycsb)
for suite in "${suites[@]}"; do
  docker compose run --rm --entrypoint slatedb-benchmark runner \
    run \
    --suite "$suite" \
    --scale "$scale" \
    --session "scaled-fixtures-$suite" \
    --output "/output/scaled-fixtures/$suite"
  docker compose run --rm --entrypoint slatedb-benchmark runner \
    validate \
    --output "/output/scaled-fixtures/$suite"
done

mkdir -p "$fixture_root/results"
for suite in "${suites[@]}"; do
  cp -R "$working_root/$suite/results/." "$fixture_root/results/"
done
rm -rf "$working_root"

result_count=$(find "$fixture_root/results" -name result.json -type f | wc -l | tr -d ' ')
echo "Generated $result_count scaled benchmark fixtures in $fixture_root"
