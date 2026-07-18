#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

scale=${BENCHMARK_SCALE:-0.00001}
slatedb_ref=${SLATEDB_REF:-v0.14.1}
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
  printf '%s\n' \
    'TIGRIS_ACCESS_KEY_ID=slatedb' \
    'TIGRIS_SECRET_ACCESS_KEY=slatedb-secret' > .act.secrets
  created_secrets=true
fi

rm -rf .runs/act-artifacts .runs/bundle .runs/release .runs/workload .runs/preparation
mkdir -p .runs/act-artifacts/1
docker compose down --volumes --remove-orphans
docker compose up -d minio
docker compose run --rm minio-init

act workflow_dispatch \
  -W .github/workflows/golden.yml \
  --input "slatedb_ref=$slatedb_ref" \
  --input "golden_id=$golden_id" \
  --input "scale=$scale"

act workflow_dispatch \
  -W .github/workflows/benchmark.yml \
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

result_count=$(find .runs/release/results -name result.json -type f | wc -l | tr -d ' ')
if [[ $result_count -ne 12 ]]; then
  echo "expected 12 task results, found $result_count" >&2
  exit 1
fi
find .runs/release/results -mindepth 2 -maxdepth 2 -name run.json -type f | grep -q .
