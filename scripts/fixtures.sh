#!/usr/bin/env bash
# Generates local website fixtures by running every release suite at scale.
# Usage: ./scripts/fixtures.sh
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
runner_user="$(id -u):$(id -g)"

run_runner() {
  docker compose run --rm --user "$runner_user" --entrypoint slatedb-benchmark runner "$@"
}

rm -rf "$fixture_root"
mkdir -p .runs
docker compose build runner
docker compose run --rm --no-deps --entrypoint rm runner -rf /output/scaled-fixtures
suites=(rocksdb slatedb ycsb)
for suite in "${suites[@]}"; do
  run_runner \
    run \
    --suite "$suite" \
    --scale "$scale" \
    --session "scaled-fixtures-$suite" \
    --output "/output/scaled-fixtures/$suite"
  run_runner \
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
