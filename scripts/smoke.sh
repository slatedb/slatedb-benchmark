#!/usr/bin/env bash
# Builds and runs every release suite at smoke scale.
# Usage: ./scripts/smoke.sh
# Requires Docker Compose. Successful runs discard their local output; failed
# runs leave it in .runs for diagnosis. Compose services are always removed.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

scale=${BENCHMARK_SCALE:-0.0001}
output_root=.runs/docker-smoke
restored_root=.runs/docker-smoke-restored
runner_user="$(id -u):$(id -g)"

run_runner() {
  docker compose run --rm --user "$runner_user" --entrypoint slatedb-benchmark runner "$@"
}

mkdir -p .runs
docker compose build runner
docker compose run --rm --no-deps --entrypoint rm runner \
  -rf /output/docker-smoke /output/docker-smoke-restored
suites=(rocksdb slatedb ycsb)
for suite in "${suites[@]}"; do
  run_runner \
    run \
    --suite "$suite" \
    --scale "$scale" \
    --session "docker-smoke-$suite" \
    --output "/output/docker-smoke/$suite"
  run_runner \
    validate \
    --output "/output/docker-smoke/$suite"
done

# Hydrate a completed sequential session into a new local output directory and
# verify that its first workload is recognized as already committed.
run_runner \
  run \
  --suite rocksdb \
  --workload bulk-load \
  --scale "$scale" \
  --session docker-smoke-rocksdb \
  --output /output/docker-smoke-restored/rocksdb
run_runner \
  validate \
  --output /output/docker-smoke-restored/rocksdb

rm -rf "$output_root" "$restored_root"
