#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

KEEP_BENCHMARK_RESULTS=true ./scripts/smoke.sh
rm -rf .scaled-results
mkdir -p .scaled-results/results
cp -R .runs/release/results/. .scaled-results/results/
rm -rf .runs/act-artifacts .runs/release .runs/bundle

result_count=$(find .scaled-results/results -name result.json -type f | wc -l | tr -d ' ')
series_count=$(find .scaled-results/results -name series.json -type f | wc -l | tr -d ' ')
echo "Generated $result_count scaled results and $series_count chart sidecars in .scaled-results"
