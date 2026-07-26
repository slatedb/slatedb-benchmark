# SlateDB Benchmark

SlateDB Benchmark is the release benchmark suite for
[SlateDB](https://github.com/slatedb/slatedb). It prepares a shared database,
runs a fixed workload catalog against a selected SlateDB revision, records
application and object-store behavior, and publishes the results at
[benchmark.slatedb.io](https://benchmark.slatedb.io).

## Workloads

The canonical dataset contains 300 million records with 20-byte keys and
400-byte values, or about 120 GiB of logical data. Golden preparation loads the
records without background compaction, then clones and compacts that database
into the checkpoint used by steady-state workloads.

Most workloads run 64 closed-loop clients after a five-minute warmup and record
15 minutes of activity. `idle` records five minutes with no client calls.
`sustained-ingest` writes to an empty database for 20 minutes.

| Workload | Operation mix | Starting state |
| --- | --- | --- |
| `idle` | No client operations | Golden checkpoint |
| `point-read-uniform` | Uniform point reads | Golden checkpoint |
| `point-read-skewed` | Zipfian point reads | Golden checkpoint |
| `point-read-missing` | Reads of absent keys | Golden checkpoint |
| `read-heavy` | 95% read, 5% update | Golden checkpoint |
| `balanced` | 50% read, 50% update | Golden checkpoint |
| `update-heavy` | 5% read, 95% update | Golden checkpoint |
| `range-scan` | Forward scans of up to 10 records | Golden checkpoint |
| `sustained-ingest` | Unique inserts | Empty database |
| `transaction-contention` | Five reads and five updates per transaction | Golden checkpoint |

### Published configuration

| Setting | Value |
| --- | --- |
| Workload runner | `warp-ubuntu-latest-arm64-32x` |
| Amazon S3 region | `us-east-1` |
| Block cache | 8 GiB |
| Metadata and index cache | 4 GiB |
| Local object-store cache | Disabled |
| Value compression target | 1.0 |

The runner records the resolved configuration, source commits, applied patches,
object store, and runner location with each result.

## Run the GitHub workflows

The release path runs on GitHub Actions against Amazon S3 or Tigris. A
configured repository needs the object-store environment and WarpBuild runners
described in [docs/SETUP.md](docs/SETUP.md).

Prepare a golden dataset first:

```console
$ gh workflow run golden.yml \
    -f slatedb_ref=v0.14.1 \
    -f golden_id=slatedb-v0.14.1-001 \
    -f object_store=s3 \
    -f scale=1.0
```

After both preparation jobs finish, run the workload matrix:

```console
$ gh workflow run benchmark.yml \
    -f slatedb_ref=main \
    -f golden_id=slatedb-v0.14.1-001 \
    -f object_store=s3 \
    -f scale=1.0
```

The two workflows must use the same golden ID, object store, and scale. They
may use different SlateDB revisions when the benchmarked revision can read the
golden checkpoint.

`scale` is a decimal factor in the range `(0, 1]`. Use `1.0` for a published
release run and a small value such as `0.00001` for an infrastructure smoke
test. Changing the golden revision, scale, patches, or preparation settings
requires a new golden ID.

The transfer-capacity workflow measures the object store without running
SlateDB:

```console
$ gh workflow run transfer-capacity.yml \
    -f object_store=s3 \
    -f scale=0.01
```

Files under `patches/slatedb/` are applied to the selected SlateDB checkout in
filename order. Published manifests record each patch name and SHA-256 digest.

## Run one workload locally

The GitHub benchmark workflow runs the full workload matrix. The runner's
`run` subcommand selects one workload for a local test.

Build the runner and create a local object-store directory:

```console
$ cargo build --locked
$ mkdir -p .runs/object-store
$ export CLOUD_PROVIDER=local
$ export LOCAL_PATH="$PWD/.runs/object-store"
$ export SLATEDB_BENCH_PREFIX=manual
$ export SLATEDB_BENCH_REGION=local
$ export SLATEDB_BENCH_RUNNER_TYPE=local
```

Prepare a small golden dataset:

```console
$ target/debug/slatedb-benchmark prepare \
    --phase bulk-load \
    --golden local \
    --scale 0.00001 \
    --output .runs/local/bulk-load

$ target/debug/slatedb-benchmark prepare \
    --phase compaction \
    --golden local \
    --scale 0.00001 \
    --compaction-quiet-ms 100 \
    --output .runs/local/compaction
```

Run one workload against it:

```console
$ target/debug/slatedb-benchmark run \
    --workload point-read-uniform \
    --golden local \
    --session local-point-read-uniform \
    --scale 0.00001 \
    --output .runs/local/point-read-uniform
```

Choose any name from the workload table for `--workload`. Reuse the golden ID,
object-store directory, and scale from preparation. Each session name must be
unique within that object-store prefix.

List the accepted workload names or inspect the command options:

```console
$ target/debug/slatedb-benchmark catalog
$ target/debug/slatedb-benchmark run --help
```

The workload output directory contains `result.json` and `series.json`. These
small local runs test the runner and workload path; their measurements are not
comparable to published release runs.

## Results and interpretation

Published results live in Git and at
[benchmark.slatedb.io](https://benchmark.slatedb.io). The site provides a
dataset page, one page per workload, and charts for each metric row.

```text
results/<version>/<run-id>/
  run.json
  golden.json
  workload/<name>/
    result.json
    series.json
```

| File | Contents |
| --- | --- |
| `run.json` | Run identity, source commits, configuration, patches, and file checksums |
| `golden.json` | Dataset and checkpoint used to start the workloads |
| `result.json` | Validated metric summaries and the sidecar digest |
| `series.json` | One-second rate samples, resource samples, and latency histograms |

Use `run.json` to check the SlateDB commit, benchmark commit, golden ID, scale,
object store, runner, resolved settings, and patches before comparing two runs.
A change to any of those inputs makes it a different experiment.

Write API latency ends when SlateDB accepts an operation. The `durable` row
measures the remaining time until SlateDB reports that write's sequence number
as durable. Rate percentiles use complete one-second client windows; resource
and latency recording continues through the final durability drain.

## Local development

CI uses Rust 1.89.0 and Node.js 22. Install the website dependencies, build the
runner, and run the local end-to-end test with:

```console
$ npm --prefix website ci --ignore-scripts
$ cargo build --locked
$ tests/e2e/local.sh
```

The end-to-end script creates a temporary local object store, prepares a scaled
golden dataset, runs `balanced`, bundles the artifacts, and builds the website.
It removes the temporary data when the script exits.

Useful checks during development:

```console
$ cargo fmt -- --check
$ cargo clippy --all-targets -- -D warnings
$ cargo test --locked
$ cargo run --locked -- generate --check
$ npm --prefix website run build
```

Run the website development server against the checked-in results:

```console
$ npm --prefix website run dev
```

## Add a workload

Workload names and defaults come from `define_tasks!` in `src/config.rs`. Adding
an entry there exposes it through the CLI and `catalog`. Add execution logic in
`src/workloads/closed.rs` when the existing operation mixes and key selectors
cannot express the new workload.

Document the workload in `docs/BENCHMARKS.md` and add
`website/src/content/workloads/<name>.md` for its result page. Regenerate the
artifact contracts after changing the task enum:

```console
$ cargo run --locked -- generate
```

Add coverage for its operation mix and validation rules, then run the checks
and local end-to-end test above.

## Repository map

| Path | Contents |
| --- | --- |
| `src/` | Rust runner, metrics, artifact validation, bundling, and publication |
| `config/` | Default and workload-specific SlateDB settings |
| `patches/slatedb/` | Patches applied before the runner builds |
| `schema/` | JSON Schema generated from the Rust artifact types |
| `results/` | Published benchmark and transfer-capacity artifacts |
| `website/` | Astro site for browsing results |
| `.github/workflows/` | CI, preparation, benchmark, publication, and Pages jobs |

## Documentation

- [Benchmark contract](docs/BENCHMARKS.md) defines the dataset, workloads,
  timing, durability rules, and metrics.
- [Design](docs/DESIGN.md) describes runner boundaries, object-store state,
  artifact recovery, workflows, and publication.
- [Setup](docs/SETUP.md) covers WarpBuild, object-store credentials, GitHub
  environments, and Pages.
- [Charts](docs/CHARTS.md) documents the workload time-series contract and
  website interaction.

The crate declares the Apache-2.0 license.
