#!/usr/bin/env bash
set -euo pipefail

slatedb_root="${1:-../slatedb}"
mkdir -p .cargo
cat > .cargo/config.toml <<EOF
[patch.crates-io]
slatedb = { path = "$slatedb_root/slatedb" }
slatedb-common = { path = "$slatedb_root/slatedb-common" }
slatedb-txn-obj = { path = "$slatedb_root/slatedb-txn-obj" }
EOF
