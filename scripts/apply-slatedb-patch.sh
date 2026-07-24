#!/usr/bin/env bash
set -euo pipefail

source_dir=${1:?SlateDB source directory is required}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
patch_dir="$repo_root/patches/slatedb"

export LC_ALL=C
shopt -s nullglob
patch_paths=("$patch_dir"/*.patch)

if (( ${#patch_paths[@]} == 0 )); then
  exit 0
fi

for patch_path in "${patch_paths[@]}"; do
  echo "Applying SlateDB patch ${patch_path##*/}"
  # Later patches may depend on changes made by earlier patches.
  git -C "$source_dir" apply --check "$patch_path"
  git -C "$source_dir" apply "$patch_path"
done
