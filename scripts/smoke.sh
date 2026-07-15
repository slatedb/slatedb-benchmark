#!/usr/bin/env bash
# Builds and runs the Docker smoke suite, then validates its results.
# Usage: ./scripts/smoke.sh
# Requires Docker Compose. Replaces .runs/docker-smoke and removes the Compose
# volumes on exit. The final repeated workload verifies checkpoint restoration.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cleanup() {
  docker compose down --volumes --remove-orphans
}
trap cleanup EXIT

rm -rf .runs/docker-smoke
docker compose build runner
workloads=(open-loop-read-update prefix-scan read-write transaction-contention)
for workload in "${workloads[@]}"; do
  docker compose run --rm --entrypoint slatedb-benchmark runner \
    run \
    --suite smoke \
    --workload "$workload" \
    --session docker-smoke \
    --output /output/docker-smoke
done

# A job retry starts its steps from the beginning. Verify the first completed
# workload can be restored after the entire suite has finished.
docker compose run --rm --entrypoint slatedb-benchmark runner \
  run \
  --suite smoke \
  --workload "${workloads[0]}" \
  --session docker-smoke \
  --output /output/docker-smoke
docker compose run --rm --entrypoint slatedb-benchmark runner \
  validate --output /output/docker-smoke
