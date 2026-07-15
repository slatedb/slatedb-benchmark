#!/usr/bin/env bash
# Points this crate's SlateDB dependencies at a local SlateDB checkout.
# Usage: select-slate-source.sh <slatedb-root>
# The checkout must contain slatedb and slatedb-common packages. Running this
# script updates the root Cargo.toml and Cargo.lock through `cargo add`.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: select-slate-source.sh <slatedb-root>" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
slatedb_root="$(cd "$1" && pwd)"

for package in slatedb slatedb-common; do
  if [[ ! -f "$slatedb_root/$package/Cargo.toml" ]]; then
    echo "$slatedb_root/$package is not a Cargo package" >&2
    exit 1
  fi
done

cd "$repo_root"

configure() {
  local package="$1"
  local features="$2"
  local path="$slatedb_root/$package"

  cargo add \
    --quiet \
    --path "$path" \
    --features "$features"
  echo "configured $package from $path"
}

echo "configuring $repo_root/Cargo.toml for SlateDB at $slatedb_root"
configure slatedb aws,wal_disable,zstd
configure slatedb-common serde
