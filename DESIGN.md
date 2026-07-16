# Design

This repository owns the SlateDB benchmark runner, its published results, and
the website at `benchmark.slatedb.io`. [`BENCHMARKS.md`](BENCHMARKS.md) defines the
workloads and fixed parameters. This document defines how the runner prepares
data, executes those workloads, records results, and publishes the website.

The suite is for users evaluating SlateDB. It reports latency, throughput,
durability, resource use, and cost for one SlateDB version at a time. Criterion
microbenchmarks, regression alerts, and cross-version comparisons remain in the
SlateDB repository.

## Repository

The repository layout is:

```text
src/                    Rust benchmark runner and workload implementations
config/                 `<suite>.suite.toml` and `<suite>.settings.toml`
schema/                 JSON schemas and price tables
results/<version>/      Published result records and histograms
website/                Static website
```

Each release result records its lockfile hash, the SlateDB revision under test,
the runner revision, and the full environment described in `BENCHMARKS.md`.
The build script reads the explicitly declared SlateDB feature list from
`Cargo.toml` and watches Git `HEAD`, its symbolic ref, and `packed-refs`, so a
cached build cannot retain revision or feature metadata from an earlier source
state.

Result files are the source of truth for the website. The website reads them at build
time, so publishing needs no database or API service. Git history records any
correction to a published result.

### SlateDB settings

The runner discovers a suite from each `config/*.suite.toml`; the filename
prefix is the suite name. Each suite declares its ordered `[[workloads]]`
array, with nested `[[workloads.variants]]` records containing a display name
and its load control. Closed-loop variants define `clients`; open-loop variants
define `target_rate`. Bulk load requires exactly one client because its
record-count-driven loader is single-threaded. Behavior is never inferred from
a variant name. The RocksDB suite uses declaration order to preserve its
stateful benchmark sequence.

SlateDB engine settings are checked-in suite TOML rather than generated from
benchmark configuration. SlateDB's settings loader resolves them in this order:

```text
SlateDB Settings::default()
  -> config/<suite>.settings.toml
```

Suite settings are required, and every workload normally uses the same
effective SlateDB settings. Bulk load is an execution-phase exception: the
runner clones the suite settings, disables the WAL and compactor, and sets
`l0_max_ssts` and `l0_max_ssts_per_key` to `u32::MAX`. After loading and
flushing the complete dataset, it reopens the database with the original
suite settings and waits for compaction to finish. Every result records the
resolved `Settings`; the ephemeral object-store cache root is omitted while its
capacity is recorded explicitly.

Dataset sizes, operation mixes, timings, durability behavior, cache capacities,
SST block size, object-store probe parameters, release eligibility, workloads,
variants, and execution model live in `suite.toml`. Block and metadata caches
are attached as `DbBuilder` components; SlateDB's metadata cache includes SST
indexes, filters, and statistics. The runner applies the object-store cache
capacity to `Settings` and supplies its local root at execution time.
Human-readable durations in TOML are resolved to milliseconds internally and
nanoseconds in result records.

The `smoke.suite.toml` file defines an ordinary suite with `release = false`.
Its few purpose-built workloads cover the important runner paths with small
datasets. Release discovery excludes non-release suites unless one is named
explicitly, so Docker runs it with `--suite smoke` and the release workflow
continues to name `rocksdb`, `ycsb`, and `slatedb`.

## Execution

A release workflow accepts a SlateDB tag or commit and builds the runner once.
It derives the result version from the selected source: a release tag such as
`v0.14.1` becomes `0.14.1`, while branches and commit hashes become
`sha-<12-character resolved commit>`. This version is used for result paths,
object-store prefixes, artifacts, and the website version selector.

The workflow then runs one benchmark job and one publisher job per release
suite. Benchmark jobs have no dependencies on one another and therefore run in
parallel. They set `timeout-minutes: 1440` instead of GitHub's 360-minute
default so the RocksDB-derived suite can finish its bulk load and twelve hours
of timed measurements in one job:

```text
                         /-> rocksdb benchmark -> artifact -> rocksdb publisher
build runner artifact --+-> slatedb benchmark -> artifact -> slatedb publisher
                         \-> ycsb benchmark    -> artifact -> ycsb publisher
```

Every benchmark job has its own Tigris bucket in the `fra` region. One runner
invocation executes the workloads in declaration order and uploads the
validated suite output as a workflow artifact. The runner preserves a commit
boundary after each workload: it validates the accumulated suite output and
commits the workload's result bundle and any database checkpoint before moving
to the next workload:

```text
restore session -> measure workload -> validate -> commit session
```

If a benchmark job is interrupted or reaches its explicit timeout, rerunning the
same workflow run preserves `github.run_id` and therefore the session name.
The runner restores and skips committed workloads; only the workload that was
in flight at interruption is measured again.

The benchmark runner owns measurement and object-store persistence. It never
modifies the repository. A separate publisher job downloads the validated
artifact on a fresh runner, copies it into a checkout of `main`, commits it, and
pushes it.

Benchmark jobs have read-only repository permissions. Tigris credentials and
object-store settings are scoped to the runner step and are absent from every
other step. Publisher jobs have repository and workflow write permissions but
do not receive the benchmark environment, Tigris credentials, or a Node.js
runtime. Non-publishing checkouts do not persist their GitHub credentials.

The workflow builds the runner in release mode on the configured WarpBuild
host. It verifies the runner type, CPU count, memory, and object-store endpoint
before starting. Workloads execute serially inside one suite invocation, so
only one measurement runs at a time within a benchmark job; parallel benchmark
jobs do not share a bucket. Dataset preparation, object-store probes, and
cleanup never overlap a measurement window within the suite. A failed suite
does not cancel the other benchmark jobs.

`.github/workflows/release.yml` is generated from the release suites and their
declared workload order. Regenerate it after changing suite configuration:

```console
$ slatedb-benchmark generate-workflow
```

`generate-workflow --check` verifies that the checked-in workflow is current.
The `benchmark` GitHub environment supplies `TIGRIS_BUCKET_PREFIX`; a suite
named `ycsb`, for example, uses `${TIGRIS_BUCKET_PREFIX}-ycsb`.

The runner uses monotonic time for durations and latency. Wall-clock time is
used only for result timestamps. Warmup data is discarded and metric counters
are reset at the start of measurement. A write workload includes its final
flush and durability drain.

### Dataset isolation

The YCSB and related SlateDB-specific workloads use a golden database for each
SlateDB version and dataset definition. The runner loads the golden database
once, flushes its memtable, and waits until the compaction queue is empty and
the manifest has stopped changing. It then stops the compactor, closes the
writer, and creates a named, non-expiring detached checkpoint of the final
manifest.

Each benchmark variant uses a new shallow clone from that checkpoint. A variant
combines a workload with one configured concurrency or target rate. SlateDB
copies the manifest and references the golden database's immutable SSTs; it
does not copy the dataset. Writes and compaction output belong to the clone.
The runner checks the clone's initial manifest against the checkpoint before
opening a fresh process and empty SlateDB caches.

In an ordinary non-resumable run, the checkpoint stays live until all clones
have closed and is then deleted. A resumable isolated suite records its golden
checkpoint in the session as soon as preparation succeeds. Later workload
processes reuse that checkpoint instead of loading the same dataset again. The
goldens remain live until the last suite workload commits. Each result records
the checkpoint ID, manifest ID, and a digest of the initial LSM state. These
fields confirm that every variant began with the same logical data and SST
layout.

The RocksDB-derived suite is different by design: its ordered workloads carry
forward the database produced by `bulk-load`. A resumable run records a
last-good detached checkpoint after every successful workload. The next
workload uses a new shallow clone of that checkpoint, so its writes cannot
modify the committed input. The runner verifies the clone's LSM digest before
measuring. If a process or workload fails, its candidate path is not recorded
in the session state; retrying clones the same last-good checkpoint.

All variant result bundles for a workload are copied to the session's
object-store prefix before the runner advances the session state. Updating that
state is the atomic workload commit point. The database paths that back golden
or sequential checkpoints remain live until the full suite completes, then the
runner deletes them. The small session state and result bundles remain so a
later process can reconstruct the complete local output without repeating
measurements. Isolated and sequential suites use the same session protocol.

Each published variant runs in a fresh worker process with newly constructed
block and metadata caches, matching `db_bench` process isolation. Its local
object-store cache uses a fresh temporary directory that survives warmup and
measurement but is not shared with another variant or included in result
artifacts. In-memory smoke runs reopen the database with new caches in the
parent process because their object store cannot cross a process boundary.
Tests that require an empty or custom dataset create their own golden database.
A cold read starts with empty local caches; the runner cannot clear caches
managed inside Tigris.

### Object-store baseline

At the start of each selected suite, before its dataset preparation or
workload execution, the runner probes the Tigris bucket and endpoint through
the object-store client without opening SlateDB. Probe parameters belong to
the suite, so the probe runs exactly once per suite. A resumed session reuses
its persisted baseline rather than probing again. The release latency probe
performs 2,000
sequential PUTs and GETs of 8 KiB objects and records their histograms and
required percentiles.

The throughput probe uploads and downloads 64 MiB incompressible objects at
concurrency 32. Each direction has a 5-second warmup and 30-second measurement.
The runner records direct upload and download MiB/s. Every workload result
references this baseline. The probes use a separate prefix and delete their
objects before dataset preparation begins.

### Load models

YCSB and the RocksDB-derived workloads use a closed loop. Each client starts
its next operation after the prior operation returns. Their latency histograms
measure return latency under fixed concurrency. A stall increases the latency
of in-flight operations and reduces the number of operations the clients can
start, so throughput must be read with the latency distribution.

The two open-loop workloads use a monotonic fixed-rate scheduler. The scheduler
creates arrivals at 1,000, 5,000, and 10,000 ops/s without waiting for earlier
requests to finish. A bounded queue holds up to one second of target traffic;
the runner counts arrivals that exceed the bound as dropped. Each shipped
variant derives one worker per operation in that one-second arrival window from
the target rate, so the harness can keep operations in flight without imposing
a hidden 256-worker ceiling or requiring a separate concurrency setting. It
records:

- response latency from scheduled arrival to completion
- return latency from SlateDB invocation to completion
- scheduling delay, offered ops/s, accepted ops/s, completed ops/s, and the scheduler's dropped operation count and rate

Open-loop results record the offered rate as `configuration.target_rate`; their
`configuration.clients` field is `null`. Accepted operations are offered
arrivals that entered the worker queue. Offered, accepted, completed, and
dropped rates use the same measured elapsed interval, including the final queue
drain, so their ratios are directly comparable. A result fails validation if
the scheduler cannot sustain the target rate while SlateDB has capacity.

### Durability

YCSB and SlateDB-specific writes use `await_durable=false` and the published
`flush_interval`. Return latency therefore ends when SlateDB accepts the write.
For each accepted write, the runner records the return timestamp and sequence
number from its `WriteHandle`. A background task subscribes to SlateDB's durable
sequence frontier. As the frontier advances, the task records durability lag
for each sequence covered by the new frontier without blocking request
generation.

After measurement, the runner calls `flush()` and waits until the durable
frontier covers the last measured write. Final drain time starts when request
generation stops. Durable ops/s divides the number of measured writes by the
time from the first measured write through that final frontier. Workloads that
set `await_durable=true` include durability in return latency and leave the
separate lag fields null where they do not apply.

## CLI

CI and manual runs use the same `slatedb-benchmark` binary. Build it from the
repository root:

```console
$ cargo build --release --locked
$ ./target/release/slatedb-benchmark --version
slatedb-benchmark <runner-version> (slatedb <version> <commit>)
```

Compatibility and release jobs run `scripts/select-slate-source.sh` first.
The script asks Cargo to replace the runner's SlateDB dependencies with
absolute paths into the selected checkout and resolves the lockfile. It
explicitly modifies `Cargo.toml` and `Cargo.lock`; CI runs it only in a
disposable checkout. Subsequent builds use `--locked`.

The binary reads S3-compatible object-store configuration from the
environment:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export AWS_ENDPOINT_URL_S3=https://t3.storage.dev
export AWS_ENDPOINT_URL_IAM=https://iam.storage.dev
export SLATEDB_BENCH_BUCKET=slatedb-benchmarks
export SLATEDB_BENCH_PREFIX="manual/$USER"
```

`--suite` runs one suite, adding `--workload` runs all of that workload's
variants, and adding `--variant` runs one variant. Workload and variant
selectors require their parent selectors. For example:

```console
$ ./target/release/slatedb-benchmark run \
    --suite ycsb \
    --workload ycsb-a \
    --variant clients-16 \
    --output .runs/ycsb-a
{"status":"ok","run":".runs/ycsb-a/run.json"}
```

Any suite accepts a stable `--session` name when a complete workload is
selected. Begin with the first configured workload and use the same name as the
suite advances:

```console
$ ./target/release/slatedb-benchmark run \
    --suite rocksdb \
    --workload bulk-load \
    --session july-0.14.1-rocksdb \
    --output .runs/rocksdb
```

Each invocation may request the next configured workload, or a workload already
completed by that session. A completed workload is restored and returns without
running any of its variants again. Completed result bundles are restored from
object storage, so the local output directory may be the original session
directory, an empty directory, or a new path. An existing directory is accepted
only when its session marker matches. Resume checks the SlateDB and runner
builds, lockfile, suite configuration, machine shape, and object-store endpoint
before continuing. Use a persistent `aws` or `local` object store to resume
across processes, and do not run two processes with the same session name
concurrently.

Omitting the selectors runs all release suites and variants. Suites with
`release = false`, including `smoke`, require an explicit `--suite`:

```console
$ ./target/release/slatedb-benchmark run --output .runs/release
{"status":"ok","run":".runs/release/run.json"}
```

The read-only `catalog` command prints the discovered release identities as
JSON without running a benchmark. Passing `--suite smoke` includes that
explicit non-release suite instead. `generate-workflow` is the corresponding
configuration-to-CI command: it emits the complete release workflow rather
than requiring a hand-maintained job or matrix.

Each ordinary `run` invocation probes the object store once per selected suite,
before preparing that suite's data. A session probes only when its state is
created. The runner writes progress to stderr and one JSON status line to
stdout. The output tree is shown below. An ordinary run's output directory must
not exist before the command starts; resumable output follows the rules above.

```text
.runs/ycsb-a/
  run.json
  object-store.json
  results/<version>/ycsb/ycsb-a/clients-16/
    result.json
    histograms.json
    timeseries.json
```

`run.json` records the selected work, source commits, resolved configuration,
per-suite object-store baselines, and result paths. The runner exits nonzero on
configuration, execution, or validation failure and does not write a success
line. Manual results use the published schema but remain under `.runs/`; only
the release workflow copies validated results into `results/` and publishes
them.

## Metrics and results

The runner uses HDR histograms for return latency, SlateDB API latency,
open-loop response latency, scheduling delay, durability lag, and direct
object-store latency. Histograms use microsecond resolution and retain three
significant digits. Each workload worker records into a private shard. The
one-second sampler swaps those shards, merges their completed histograms, and
emits application windows without a global per-operation lock. Those same
windows are merged into the aggregate histograms, so the summary and time
series describe the same observations.

The capped writer in each while-writing workload contributes counts, payload,
API latency, durability, and a `return/writer-update` histogram. Its return
latency is excluded from the aggregate `return` histogram so the workload's
headline latency describes the named read or scan operation.

Application windows also contain successful-operation counts and logical read
and write payload bytes; their sum is retained as total payload bytes. Reads
count bytes returned by SlateDB, writes count bytes submitted to SlateDB, and
read-modify-write and transaction operations contribute to both. The website
derives ops/s and MiB/s using each window's actual duration. Its payload chart
also shows SlateDB compactor read and write MiB/s alongside machine upload and
download MiB/s. Compactor read uses SlateDB's aggregate input-throughput gauge;
compactor write sums per-worker output-byte counter deltas. Machine rates come
from consecutive host-wide network counter samples. Return latency covers a
complete workload operation.
API latency covers one measured SlateDB call from invocation through return.
Multi-read records each `get()` separately, read-modify-write separates `get()`
and `put()`, and transaction methods retain distinct latency histograms. Scan
variants share a `scan()` histogram that includes iterator exhaustion. Setup,
warmup, database open, and database close are excluded.
For asynchronous writes, a separate durability series measures API return
through durable-frontier coverage. Its windows may extend through the final
flush and drain. Open-loop response latency and scheduling delay are retained
as diagnostics but are not primary website charts. Sustained-ingest results also
include the five-minute windows required by `BENCHMARKS.md`.

### SlateDB metrics

The runner wraps a `DefaultMetricsRecorder`, configures SlateDB at
`MetricLevel::Info`, and passes the wrapper to the DB, compactor, and compaction
workers. It snapshots the recorder at the start of measurement, every second,
and after the durability drain. Counter deltas exclude warmup; gauges and
histograms become time series. The raw snapshots are stored with the result.

SlateDB exposes a backpressure event counter rather than a duration. For each
workload operation, the recorder starts a task-local timer when
`backpressure_count` advances and stops it when `total_mem_size_bytes` is next
updated, which is the recheck after SlateDB's wait. Repeated waits are summed
across all workers into `backpressure_ns`; a run without backpressure reports
zero. Task-local timing keeps concurrent writers from attributing one another's
waits.

SlateDB metrics supply DB activity, WAL and L0 flush bytes, L0 stalls,
backpressure, compaction, and internal object-store operation counts, errors,
and latency. A runner-side object-store wrapper records logical operations and
payload bytes. An HTTP transport wrapper separately records physical requests,
response status classes, retries, attempted request-body bytes, and consumed
response-body bytes after range coalescing, bulk deletion, and pagination. A
host sampler collects CPU, RSS, network, and local disk statistics. Time-series
JSON stores host samples as rows and SlateDB metrics as
columnar series: metric names, descriptions, labels, types, and histogram
boundaries appear once, while an aligned value array carries each one-second
snapshot. A `null` value means that a dynamically registered metric was absent
from that sample.

Each variant produces one self-contained JSON summary plus its encoded
histograms and time series:

```text
results/<version>/<suite>/<workload>/<variant>/
  result.json
  histograms.json
  timeseries.json
```

The `variant` path component names the configured concurrency or target rate.
The repository publishes one result per variant. JSON stores durations in
nanoseconds, sizes in bytes, and counts as integers; the website converts them for
display. Fields that do not apply are present with `null`, as required by the
result schema.

The schema keeps summary values and source measurements together. Percentiles
can therefore be checked against the encoded histogram, and a future website can
render a different percentile without rerunning the workload. CI logs and
crash dumps remain workflow artifacts rather than permanent result files.

### Cost

`schema/prices.json` contains request and storage rates for comparable standard,
regional, multi-zone buckets in US East: S3 Standard, Azure Hot ZRS, and Google
Cloud Standard regional storage. The table links to each provider's source and
does not include Tigris pricing.

Results store the raw inputs rather than a price-dependent estimate: elapsed
time, logical object-store operations, physical HTTP requests by operation and
status class, transferred body bytes, and the final database size. The
website's cost section applies the selected provider's current table entry to
the exact workload and variant being viewed. Successful physical requests are
scaled from the measured elapsed time to a 30-day month; failed attempts remain
visible in the measurements but are excluded from the projection because their
billing differs by provider. Historical results without wire-level metrics do
not project request charges. Storage uses the final database size at the
provider's monthly rate. The estimate assumes that the final
footprint stays constant rather than extrapolating database growth. It excludes
dataset preparation, warmup, cloning, the direct object-store
probe, compute, free tiers, discounts, and taxes. Compute and the bucket are
assumed to share a region, so the cost section does not add transfer charges.
The request total is also broken down by operation with each operation's
projected monthly count, per-thousand rate, and monthly cost.

## Website

The website is a static Astro application built from `results/`. It defaults to
the latest stable SlateDB release and provides a version dropdown. Routes keep
the selection shareable:

```text
/<version>/<suite>/<workload>/<variant>/
```

The page displays one version and never computes a delta against another
version. Suite, workload, concurrency, and target-rate controls select result
variants within that version.

Use `~/Code/slatedb/website` as the visual reference for the website's design
aesthetic, layout, and CSS styling.

The layout favors density. A slim header contains the SlateDB wordmark. On wide
screens, a sticky context rail groups the version, suite, workload, and
variant selectors with machine, object-store, dataset, cache capacities, and
durability facts. The results canvas beside it starts with an active chart
description and a right-aligned chart dropdown. Payload MiB/s is selected by
default. Each SlateDB API exercised by the selected run gets its own p50, p95,
and p99 latency-over-time chart. A `flush()` chart appears only when at least
two calls were measured. Asynchronous write workloads also expose durability
latency.
The rail and results collapse to one column on smaller screens. A table below
the charts exposes all percentiles, durability, resource, storage, and
object-store baseline fields. Raw object-store counts appear in their own
operations subsection. A provider-selectable cost calculator follows the
measurements, and each page links to its raw result files, price table, and
source commits. Structured measurement values are flattened into ordinary
table rows, with slash-separated labels preserving their path. Latency records,
per-operation maps, and sustained-ingest windows therefore follow the same
one-value-per-row presentation rather than using JSON or nested layouts.

The header wordmark uses Marcellus, matching `slatedb.io`. Body text uses Inter
and numeric tables use JetBrains Mono. The website reuses SlateDB's ink,
off-white, and terracotta color tokens. Time-series charts use elapsed time on
the x-axis and milliseconds on latency-chart y-axes. Result charts use a
browser charting library instead of Mermaid. The website needs client
JavaScript only for selectors, chart selection, and chart rendering.

## Validation and publication

The runner validates every result against the checked-in JSON schema. It also
checks that histogram counts match operation counts, the durable frontier
covers all measured writes, resource samples span the measurement window, and
the workload used the expected initial manifest. Secrets, credentials, and
signed URLs are rejected from result files.

Every release suite has one run step and a dependent publisher job. A benchmark
failure does not invalidate the per-workload object-store commits: rerunning it
with the same run ID restores and skips every completed workload. A publisher
failure can be rerun against the already-uploaded suite artifact without
measuring the suite again. The same behavior applies to isolated and sequential
suites.

Publication replaces that suite's destination with all of its validated
workload and variant results in a fresh checkout of `main`, commits only that
result subtree, and rebases before pushing. A non-fast-forward push refetches,
rebases, and retries instead of pushing from a stale checkout. A separate Pages
workflow installs dependencies without lifecycle scripts, builds the website,
and deploys after each suite results push, so a successful suite becomes visible
without waiting for the rest of the release. Failed and interrupted benchmark
jobs keep their local output as CI artifacts. The published schema omits trial
and repetition fields.
