#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

scale=${BENCHMARK_SCALE:-0.00001}
slatedb_ref=${SLATEDB_REF:-main}
golden_id=${BENCHMARK_GOLDEN_ID:-local-smoke-${scale//./-}}
created_env=false
created_secrets=false

cleanup() {
  docker compose down --volumes --remove-orphans
  if [[ $created_env == true ]]; then rm -f .act.env; fi
  if [[ $created_secrets == true ]]; then rm -f .act.secrets; fi
  if [[ ${KEEP_BENCHMARK_RESULTS:-false} != true ]]; then
    rm -rf .runs/act-artifacts .runs/release .runs/bundle
  fi
}
trap cleanup EXIT

if [[ ! -f .act.env ]]; then
  cp .act.env.example .act.env
  created_env=true
fi
if [[ ! -f .act.secrets ]]; then
  : > .act.secrets
  if [[ -n ${GITHUB_TOKEN:-} ]]; then
    printf 'GITHUB_TOKEN=%s\n' "$GITHUB_TOKEN" > .act.secrets
  fi
  chmod 600 .act.secrets
  created_secrets=true
fi

rm -rf .runs/act-artifacts .runs/bundle .runs/release .runs/workload .runs/preparation
mkdir -p .runs/act-artifacts/1
docker compose down --volumes --remove-orphans
docker compose up -d minio
docker compose run --rm minio-init

act workflow_dispatch \
  -W .github/workflows/golden.yml \
  --concurrent-jobs 1 \
  --input "slatedb_ref=$slatedb_ref" \
  --input "golden_id=$golden_id" \
  --input "scale=$scale"

act workflow_dispatch \
  -W .github/workflows/benchmark.yml \
  --concurrent-jobs 1 \
  --input "slatedb_ref=$slatedb_ref" \
  --input "golden_id=$golden_id" \
  --input publish=false \
  --input "scale=$scale"

artifact=.runs/act-artifacts/1/benchmark-results/benchmark-results.zip
if [[ ! -f $artifact ]]; then
  echo "benchmark workflow did not produce $artifact" >&2
  exit 1
fi
mkdir -p .runs/release/results
unzip -q "$artifact" -d .runs/release/results

preparation_count=$(find .runs/release/results -path '*/preparation/*/result.json' -type f | wc -l | tr -d ' ')
if [[ $preparation_count -ne 2 ]]; then
  echo "expected 2 preparation results, found $preparation_count" >&2
  exit 1
fi
workload_count=$(find .runs/release/results -path '*/workload/*/result.json' -type f | wc -l | tr -d ' ')
if [[ $workload_count -lt 1 ]]; then
  echo "expected at least 1 workload result, found $workload_count" >&2
  exit 1
fi
series_count=$(find .runs/release/results -name series.json -type f | wc -l | tr -d ' ')
if [[ $series_count -ne $workload_count ]]; then
  echo "expected $workload_count workload series, found $series_count" >&2
  exit 1
fi
while IFS= read -r result; do
  series=$(dirname "$result")/series.json
  expected=$(jq -r '.series.sha256' "$result")
  actual=$(sha256sum "$series" | awk '{print $1}')
  if [[ "$actual" != "$expected" ]]; then
    echo "$series does not match its result digest" >&2
    exit 1
  fi
done < <(find .runs/release/results -path '*/workload/*/result.json' -type f)
find .runs/release/results -mindepth 2 -maxdepth 2 -name run.json -type f | grep -q .
