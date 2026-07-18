#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: publish.sh <bundle-results-directory> <publish-checkout>" >&2
  exit 2
fi

bundle_root=$1
publish_checkout=$2
mapfile -t manifests < <(find "$bundle_root" -mindepth 2 -maxdepth 2 -name run.json -type f)
if [[ ${#manifests[@]} -ne 1 ]]; then
  echo "expected one versioned run.json under $bundle_root" >&2
  exit 1
fi
run_manifest=${manifests[0]}
version=$(basename "$(dirname "$run_manifest")")
if [[ -z "$version" || "$version" == "." || "$version" == ".." || "$version" =~ [^A-Za-z0-9._-] ]]; then
  echo "invalid result version $version" >&2
  exit 2
fi
if [[ ! -d "$publish_checkout/.git" ]]; then
  echo "publication checkout not found at $publish_checkout" >&2
  exit 1
fi

if ! jq -e '
  .status == "ok"
  and (.resolved_configuration | type == "object")
  and ((.resolved_configuration | length) > 0)
  and ([.resolved_configuration[].scale] | all(. == 1.0))
' "$run_manifest" >/dev/null; then
  echo "refusing to publish an unsuccessful or scaled benchmark run" >&2
  exit 1
fi

source_directory=$(dirname "$run_manifest")
mapfile -t manifest_files < <(jq -r '.results | keys[]' "$run_manifest" | sort)
mapfile -t actual_files < <(
  find "$source_directory" -mindepth 3 -maxdepth 3 -name '*.json' -type f \
    -printf '%P\n' | sort
)
if [[ "${manifest_files[*]}" != "${actual_files[*]}" ]]; then
  echo "bundle files do not match run.json" >&2
  exit 1
fi
while IFS=$'\t' read -r relative expected; do
  actual=$(sha256sum "$source_directory/$relative" | awk '{print $1}')
  if [[ "$actual" != "$expected" ]]; then
    echo "$relative does not match run.json" >&2
    exit 1
  fi
done < <(jq -r '.results | to_entries[] | [.key, .value] | @tsv' "$run_manifest")

destination_directory="$publish_checkout/results/$version"
rm -rf "$destination_directory"
mkdir -p "$destination_directory"
cp -R "$source_directory/." "$destination_directory/"

git -C "$publish_checkout" add "results/$version"
if git -C "$publish_checkout" diff --cached --quiet; then
  echo "SlateDB $version benchmark results are already published"
  exit 0
fi

git -C "$publish_checkout" config user.name "slatedb-benchmark[bot]"
git -C "$publish_checkout" config user.email "slatedb-benchmark[bot]@users.noreply.github.com"
git -C "$publish_checkout" commit -m "Publish SlateDB $version benchmarks"

for attempt in 1 2 3 4 5; do
  git -C "$publish_checkout" fetch origin main
  git -C "$publish_checkout" rebase origin/main
  if git -C "$publish_checkout" push origin HEAD:main; then
    exit 0
  fi
  echo "::warning::main advanced during publication attempt $attempt; retrying"
done

echo "::error::main advanced during all publication attempts"
exit 1
