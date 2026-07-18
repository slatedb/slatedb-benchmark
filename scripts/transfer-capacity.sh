#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: transfer-capacity.sh <output-json>" >&2
  exit 2
fi

output=$1
scale=${BENCHMARK_SCALE:-1.0}
endpoint=${AWS_ENDPOINT_URL_S3:-https://t3.storage.dev}
bucket=${SLATEDB_BENCH_BUCKET:?SLATEDB_BENCH_BUCKET is required}
root_prefix=${SLATEDB_BENCH_PREFIX:-benchmark}
region=${SLATEDB_BENCH_REGION:-fra}
runner_type=${SLATEDB_BENCH_RUNNER_TYPE:-unknown}
object_store=${CLOUD_PROVIDER:-aws}
if [[ $endpoint == "https://t3.storage.dev" || $endpoint == *tigris.dev* || \
  $endpoint == *tigrisdata.com* ]]; then
  object_store=Tigris
fi
parallel_objects=4
requests_per_process=8
max_concurrent_requests=$((parallel_objects * requests_per_process))
probe_id="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}"
probe_prefix="$root_prefix/probes/$probe_id"
work=$(mktemp -d)

cleanup() {
  local status=$?
  trap - EXIT
  aws s3 rm --endpoint-url "$endpoint" --only-show-errors --recursive \
    "s3://$bucket/$probe_prefix/" >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

read -r warmup_mib_per_object measured_mib_per_object < <(
  python3 - "$scale" <<'PY'
import math
import sys

scale = float(sys.argv[1])
if not math.isfinite(scale) or not 0 < scale <= 1:
    raise SystemExit("BENCHMARK_SCALE must be greater than zero and at most 1.0")
print(max(1, math.ceil(2048 * scale)), max(1, math.ceil(8192 * scale)))
PY
)

warmup_bytes=$((parallel_objects * warmup_mib_per_object * 1024 * 1024))
measured_bytes=$((parallel_objects * measured_mib_per_object * 1024 * 1024))

export AWS_CONFIG_FILE="$work/aws-config"
export AWS_EC2_METADATA_DISABLED=true
cat > "$AWS_CONFIG_FILE" <<EOF
[default]
region = ${AWS_REGION:-auto}
s3 =
    max_concurrent_requests = $requests_per_process
    multipart_threshold = 64MB
    multipart_chunksize = 64MB
EOF

create_files() {
  local directory=$1
  local mib_per_object=$2
  mkdir -p "$directory"
  for index in $(seq 1 "$parallel_objects"); do
    dd if=/dev/urandom of="$directory/part-$index.bin" \
      bs=1M count="$mib_per_object" iflag=fullblock status=none
  done
}

wait_for_transfers() {
  local failed=0
  local pid
  for pid in "$@"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  return "$failed"
}

upload_files() {
  local directory=$1
  local remote=$2
  local pids=()
  local file
  for file in "$directory"/*.bin; do
    aws s3 cp --endpoint-url "$endpoint" --only-show-errors \
      "$file" "s3://$bucket/$remote/$(basename "$file")" &
    pids+=("$!")
  done
  wait_for_transfers "${pids[@]}"
}

download_files() {
  local remote=$1
  local directory=$2
  local pids=()
  local index
  mkdir -p "$directory"
  for index in $(seq 1 "$parallel_objects"); do
    aws s3 cp --endpoint-url "$endpoint" --only-show-errors \
      "s3://$bucket/$remote/part-$index.bin" "$directory/part-$index.bin" &
    pids+=("$!")
  done
  wait_for_transfers "${pids[@]}"
}

monotonic_ns() {
  python3 -c 'import time; print(time.monotonic_ns())'
}

echo "Generating transfer-capacity probe data"
create_files "$work/source/warmup" "$warmup_mib_per_object"
create_files "$work/source/measured" "$measured_mib_per_object"

echo "Warming up $object_store uploads"
upload_files "$work/source/warmup" "$probe_prefix/warmup"
echo "Measuring parallel $object_store uploads"
upload_started=$(monotonic_ns)
upload_files "$work/source/measured" "$probe_prefix/measured"
upload_elapsed_ns=$(($(monotonic_ns) - upload_started))

rm -rf "$work/source"
echo "Warming up $object_store downloads"
download_files "$probe_prefix/warmup" "$work/download/warmup"
rm -rf "$work/download/warmup"
echo "Measuring parallel $object_store downloads"
download_started=$(monotonic_ns)
download_files "$probe_prefix/measured" "$work/download/measured"
download_elapsed_ns=$(($(monotonic_ns) - download_started))

mkdir -p "$(dirname "$output")"
python3 - \
  "$output" "$scale" "$runner_type" "$object_store" "$endpoint" "$region" \
  "$parallel_objects" "$requests_per_process" "$max_concurrent_requests" \
  "$warmup_bytes" "$measured_bytes" "$upload_elapsed_ns" \
  "$download_elapsed_ns" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    output,
    scale,
    runner_type,
    object_store,
    endpoint,
    region,
    parallel_objects,
    requests_per_process,
    max_concurrent_requests,
    warmup_bytes,
    measured_bytes,
    upload_elapsed_ns,
    download_elapsed_ns,
) = sys.argv[1:]

measured_bytes = int(measured_bytes)


def measurement(elapsed_ns):
    elapsed_seconds = int(elapsed_ns) / 1_000_000_000
    return {
        "elapsed_seconds": elapsed_seconds,
        "mib_per_second": measured_bytes / 1024 / 1024 / elapsed_seconds,
    }


result = {
    "status": "ok",
    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "scale": float(scale),
    "runner_type": runner_type,
    "object_store": object_store,
    "endpoint": endpoint,
    "region": region,
    "parallel_objects": int(parallel_objects),
    "requests_per_process": int(requests_per_process),
    "max_concurrent_requests": int(max_concurrent_requests),
    "warmup_bytes": int(warmup_bytes),
    "measured_bytes": measured_bytes,
    "upload": measurement(upload_elapsed_ns),
    "download": measurement(download_elapsed_ns),
}
Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(
    f"Upload: {result['upload']['mib_per_second']:.2f} MiB/s; "
    f"download: {result['download']['mib_per_second']:.2f} MiB/s"
)
PY
