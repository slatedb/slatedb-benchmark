#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: transfer-capacity.sh <output-json>" >&2
  exit 2
fi

output=$1
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
scale=${BENCHMARK_SCALE:-1.0}
endpoint=${AWS_ENDPOINT_URL_S3:-https://t3.storage.dev}
bucket=${SLATEDB_BENCH_BUCKET:?SLATEDB_BENCH_BUCKET is required}
root_prefix=${SLATEDB_BENCH_PREFIX:-benchmark}
region=${SLATEDB_BENCH_REGION:-fra}
runner_type=${SLATEDB_BENCH_RUNNER_TYPE:-unknown}
object_store=${CLOUD_PROVIDER:-aws}
warp_bin=${WARP_BIN:-warp}
warp_version=${WARP_VERSION:-v1.5.0}
if [[ $endpoint == "https://t3.storage.dev" || $endpoint == *tigris.dev* || \
  $endpoint == *tigrisdata.com* ]]; then
  object_store=Tigris
fi

large_object_size=4194304
large_object_size_arg=4MiB
large_concurrency=64
small_object_size=4096
small_object_size_arg=4KiB
small_concurrency=1
small_list_objects=100
attempts=3

if ! jq -en --argjson scale "$scale" \
  '$scale > 0 and $scale <= 1' >/dev/null; then
  echo "BENCHMARK_SCALE must be greater than zero and at most 1.0" >&2
  exit 2
fi
if [[ $scale == 1 || $scale == 1.0 ]]; then
  large_duration=60
  small_duration=30
else
  large_duration=10
  small_duration=10
fi

case "$endpoint" in
  https://*)
    export WARP_TLS=true
    warp_host=${endpoint#https://}
    ;;
  http://*)
    export WARP_TLS=false
    warp_host=${endpoint#http://}
    ;;
  *)
    echo "AWS_ENDPOINT_URL_S3 must begin with http:// or https://" >&2
    exit 2
    ;;
esac
warp_host=${warp_host%/}
if [[ $warp_host == */* ]]; then
  echo "AWS_ENDPOINT_URL_S3 must not contain a path" >&2
  exit 2
fi

export WARP_HOST=$warp_host
export WARP_ACCESS_KEY=${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}
export WARP_SECRET_KEY=${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}
export WARP_REGION=${AWS_REGION:-auto}
if [[ -n ${AWS_SESSION_TOKEN:-} ]]; then
  export WARP_SESSION_TOKEN=$AWS_SESSION_TOKEN
fi

if ! command -v "$warp_bin" >/dev/null 2>&1; then
  echo "Warp executable not found: $warp_bin" >&2
  exit 2
fi

probe_id="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-${GITHUB_JOB:-transfer-capacity}-$$"
probe_prefix="$root_prefix/probes/$probe_id"
artifact_dir="$(dirname "$output")/warp"
mkdir -p "$artifact_dir"

cleanup() {
  local status=$?
  trap - EXIT
  aws s3 rm --endpoint-url "$endpoint" --only-show-errors --recursive \
    "s3://$bucket/$probe_prefix/" >/dev/null 2>&1 || \
    echo "warning: failed to clean up s3://$bucket/$probe_prefix/" >&2
  exit "$status"
}
trap cleanup EXIT

analyze_warp_benchmark() {
  local raw=$1
  local analysis=$2

  "$warp_bin" analyze --json --no-color "$raw" |
    jq -e -f "$script_dir/warp-latency.jq" >"$analysis"
}

run_warp_benchmark() {
  local name=$1
  local concurrency=$2
  local duration=$3
  local command=$4
  shift 4
  local base="$artifact_dir/$name"
  local raw="$base.json.zst"
  local analysis="$base.analysis.json"
  local attempt
  local -a warp_command=(
    "$warp_bin" "$command"
    --bucket="$bucket"
    --concurrent="$concurrency"
    --duration="${duration}s"
    --benchdata="$base"
    --analyze.v
    --no-color
    --noclear
    "$@"
  )

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    rm -f "$raw" "$analysis"
    echo "Running Warp $name benchmark (attempt $attempt/$attempts)"
    printf 'Command:'
    printf ' %q' "${warp_command[@]}"
    printf '\n'
    if "${warp_command[@]}" && [[ -s $raw ]]; then
      if analyze_warp_benchmark "$raw" "$analysis"; then
        return 0
      fi
      echo "Warp $name analysis failed" >&2
      return 1
    fi
    if ((attempt == attempts)); then
      echo "Warp $name benchmark failed after $attempts attempts" >&2
      return 1
    fi
    echo "Warp $name benchmark failed; retrying in 5s" >&2
    sleep 5
  done
}

large_prefix="$probe_prefix/warp-large"
small_prefix="$probe_prefix/warp-small"
list_prefix="$probe_prefix/warp-small-list"

run_warp_benchmark \
  large-put "$large_concurrency" "$large_duration" put \
  --prefix="$large_prefix" \
  --obj.size="$large_object_size_arg" \
  --disable-multipart
run_warp_benchmark \
  large-get "$large_concurrency" "$large_duration" get \
  --prefix="$large_prefix" \
  --list-existing \
  --objects=0
run_warp_benchmark \
  small-put "$small_concurrency" "$small_duration" put \
  --prefix="$small_prefix" \
  --obj.size="$small_object_size_arg" \
  --disable-multipart
run_warp_benchmark \
  small-get "$small_concurrency" "$small_duration" get \
  --prefix="$small_prefix" \
  --list-existing \
  --objects=0
run_warp_benchmark \
  small-list "$small_concurrency" "$small_duration" list \
  --prefix="$list_prefix" \
  --obj.size="$small_object_size_arg" \
  --objects="$small_list_objects" \
  --max-keys="$small_list_objects"

mkdir -p "$(dirname "$output")"
jq -n \
  --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson scale "$scale" \
  --arg runner_type "$runner_type" \
  --arg object_store "$object_store" \
  --arg endpoint "$endpoint" \
  --arg region "$region" \
  --arg warp_version "$warp_version" \
  --argjson large_object_size "$large_object_size" \
  --argjson large_concurrency "$large_concurrency" \
  --argjson large_duration "$large_duration" \
  --argjson small_object_size "$small_object_size" \
  --argjson small_concurrency "$small_concurrency" \
  --argjson small_duration "$small_duration" \
  --slurpfile large_put_latency "$artifact_dir/large-put.analysis.json" \
  --slurpfile large_get_latency "$artifact_dir/large-get.analysis.json" \
  --slurpfile small_put_latency "$artifact_dir/small-put.analysis.json" \
  --slurpfile small_get_latency "$artifact_dir/small-get.analysis.json" \
  --slurpfile small_list_latency "$artifact_dir/small-list.analysis.json" \
  '{
    version: 3,
    status: "ok",
    timestamp: $timestamp,
    scale: $scale,
    runner_type: $runner_type,
    object_store: $object_store,
    endpoint: $endpoint,
    region: $region,
    tool: {name: "warp", version: $warp_version},
    benchmarks: [
      {
        name: "large-put", operation: "PUT",
        object_size_bytes: $large_object_size,
        concurrency: $large_concurrency,
        duration_seconds: $large_duration,
        latency_ms: $large_put_latency[0],
        benchdata: "warp/large-put.json.zst"
      },
      {
        name: "large-get", operation: "GET",
        object_size_bytes: $large_object_size,
        concurrency: $large_concurrency,
        duration_seconds: $large_duration,
        latency_ms: $large_get_latency[0],
        benchdata: "warp/large-get.json.zst"
      },
      {
        name: "small-put", operation: "PUT",
        object_size_bytes: $small_object_size,
        concurrency: $small_concurrency,
        duration_seconds: $small_duration,
        latency_ms: $small_put_latency[0],
        benchdata: "warp/small-put.json.zst"
      },
      {
        name: "small-get", operation: "GET",
        object_size_bytes: $small_object_size,
        concurrency: $small_concurrency,
        duration_seconds: $small_duration,
        latency_ms: $small_get_latency[0],
        benchdata: "warp/small-get.json.zst"
      },
      {
        name: "small-list", operation: "LIST",
        object_size_bytes: $small_object_size,
        concurrency: $small_concurrency,
        duration_seconds: $small_duration,
        latency_ms: $small_list_latency[0],
        benchdata: "warp/small-list.json.zst"
      }
    ]
  }' >"$output"
