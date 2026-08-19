#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
temporary=$(mktemp -d "${TMPDIR:-/tmp}/slatedb-benchmark-e2e.XXXXXX")
cleanup() {
  rm -rf "$temporary"
}
trap cleanup EXIT

export CLOUD_PROVIDER=local
export LOCAL_PATH="$temporary/object-store"
export SLATEDB_BENCH_PREFIX=benchmark
export SLATEDB_BENCH_REGION=local
export SLATEDB_BENCH_RUNNER_TYPE=local-e2e
mkdir -p "$LOCAL_PATH" "$temporary/input/workload/balanced"

runner="$root/target/debug/slatedb-benchmark"
"$runner" prepare \
  --phase bulk-load \
  --golden e2e \
  --scale 0.00002 \
  --output "$temporary/bulk-load"
"$runner" prepare \
  --phase compaction \
  --golden e2e \
  --scale 0.00002 \
  --compaction-quiet-ms 100 \
  --output "$temporary/compaction"
"$runner" run \
  --workload balanced \
  --golden e2e \
  --session local-e2e \
  --scale 0.00001 \
  --output "$temporary/balanced"

cp "$temporary/compaction/golden.json" "$temporary/input/golden.json"
cp "$temporary/balanced/result.json" "$temporary/input/workload/balanced/result.json"
cp "$temporary/balanced/series.json" "$temporary/input/workload/balanced/series.json"
"$runner" bundle \
  --input "$temporary/input" \
  --output "$temporary/results" \
  --golden e2e \
  --started-at 2026-07-24T00:00:00Z

BENCHMARK_RESULTS_ROOT="$temporary/results" npm run build --prefix "$root/website"
