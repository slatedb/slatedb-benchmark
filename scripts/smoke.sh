#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

rm -rf .runs/docker-smoke
docker compose build runner
docker compose run --rm runner
docker compose run --rm --entrypoint slatedb-benchmark runner validate --output /output/docker-smoke
node scripts/verify-smoke.mjs .runs/docker-smoke
