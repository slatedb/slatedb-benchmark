#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
slatedb_root="${1:-$repo_root/../slatedb}"

python3 "$repo_root/scripts/use-local-slatedb.py" "$repo_root" "$slatedb_root"
