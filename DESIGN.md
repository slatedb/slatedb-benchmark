# Design

This repository owns the SlateDB benchmark runner, its published results, and
the site at `benchmark.slatedb.io`. [`BENCHMARKS.md`](BENCHMARKS.md) defines the
workloads and fixed parameters. This document defines how the runner prepares
data, executes those workloads, records results, and publishes the site.

The suite is for users evaluating SlateDB. It reports latency, throughput,
durability, resource use, and cost for one SlateDB version at a time. Criterion
microbenchmarks, regression alerts, and cross-version comparisons remain in the
SlateDB repository.

## Repository

The repository layout is:

```text
runner/                 Rust benchmark runner and workload implementations
config/                 Profile definitions and SlateDB settings
schema/                 Versioned JSON schemas and price tables
results/<version>/      Published result records and histograms
site/                   Static website
```

Each release result records its lockfile hash, the SlateDB revision under test,
the runner revision, the result schema version, and the full environment
described in `BENCHMARKS.md`.

Result files are the source of truth for the site. The site reads them at build
time, so publishing needs no database or API service. Git history records any
correction to a published result.

## Execution

A release workflow accepts a SlateDB tag or commit and runs this sequence:

```text
build -> inspect host -> probe object store -> prepare data -> run workloads
      -> validate results -> build site -> publish
```

The workflow builds the runner in release mode on the configured WarpBuild
host. It verifies the runner type, CPU count, memory, and object-store endpoint
before starting. One measured workload runs on a host at a time. Dataset
preparation, object-store probes, and cleanup never overlap a measurement
window.

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

The checkpoint stays live until all clones have closed. The runner then deletes
the clones and checkpoint in a cleanup phase that also runs after failure. Each
result records the checkpoint ID, manifest ID, and a digest of the initial LSM
state. These fields confirm that every variant began with the same logical data
and SST layout.

The RocksDB-derived profile is different by design: it runs its ordered
workloads against the database produced by `bulk-load`. Tests that require an
empty or custom dataset create their own golden database. A cold read clears
SlateDB's local caches; the runner cannot clear caches managed inside Tigris.

### Object-store baseline

At the start of the release run, before dataset preparation or workload
execution, the runner probes the Tigris bucket and endpoint through the
object-store client without opening SlateDB. The latency probe performs 2,000
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
the runner counts arrivals that exceed the bound as dropped. It records:

- response latency from scheduled arrival to completion
- return latency from SlateDB invocation to completion
- scheduling delay, offered ops/s, completed ops/s, and dropped ops/s

The runner uses enough asynchronous workers to keep its own queue and CPU from
limiting an otherwise healthy test. A result fails validation if the scheduler
cannot sustain the target rate while SlateDB has capacity.

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
$ cargo build --release --locked \
    --manifest-path runner/Cargo.toml \
    --target-dir target
$ ./target/release/slatedb-benchmark --version
slatedb-benchmark <runner-version> (slatedb <version> <commit>)
```

Compatibility and release jobs run `scripts/use-local-slatedb.sh` first. The
script replaces the workspace's SlateDB dependencies with absolute paths into
the selected checkout, resolves the lockfile, and checks `cargo metadata` to
ensure all SlateDB packages came from that checkout. Subsequent builds use
`--locked`, so the recorded checkout cannot silently differ from the code in
the runner binary.

The binary reads S3-compatible object-store configuration from the
environment:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
export SLATEDB_BENCH_BUCKET=slatedb-benchmarks
export SLATEDB_BENCH_PREFIX="manual/$USER"
```

`--profile` runs one profile, adding `--workload` runs all of that workload's
variants, and adding `--variant` runs one variant. Workload and variant
selectors require their parent selectors. For example:

```console
$ ./target/release/slatedb-benchmark run \
    --profile ycsb \
    --workload ycsb-a \
    --variant clients-16 \
    --output .runs/ycsb-a
{"status":"ok","run":".runs/ycsb-a/run.json"}
```

Omitting the selectors runs all profiles and variants:

```console
$ ./target/release/slatedb-benchmark run --output .runs/release
{"status":"ok","run":".runs/release/run.json"}
```

Each `run` invocation probes the object store once, before preparing data. It
writes progress to stderr and one JSON status line to stdout. The output tree
is shown below. The output directory must not exist before the command starts.

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
object-store baseline, and result paths. The runner exits nonzero on
configuration, execution, or validation failure and does not write a success
line. Manual results use the published schema but remain under `.runs/`; only
the release workflow copies validated results into `results/` and publishes
them.

## Metrics and results

The runner uses HDR histograms for operation latency, open-loop response
latency, durability lag, and direct object-store latency. Histograms use
microsecond resolution and retain three significant digits. The runner also
writes one-second counters for throughput and resource use; sustained-ingest
results include the five-minute windows required by `BENCHMARKS.md`. It retains
aggregate histograms instead of one record per operation.

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
backpressure, compaction, and internal object-store request counts, errors, and
latency. A runner-side wrapper adds transferred byte counts, which SlateDB does
not currently expose. A host sampler collects CPU, RSS, network, and local disk
statistics. Time-series JSON stores host samples as rows and SlateDB metrics as
columnar series: metric names, descriptions, labels, types, and histogram
boundaries appear once, while an aligned value array carries each one-second
snapshot. A `null` value means that a dynamically registered metric was absent
from that sample.

Each variant produces one self-contained JSON summary plus its encoded
histograms and time series:

```text
results/<version>/<profile>/<workload>/<variant>/
  result.json
  histograms.json
  timeseries.json
```

The `variant` path component names the configured concurrency or target rate.
The repository publishes one result per variant. JSON stores durations in
nanoseconds, sizes in bytes, and counts as integers; the site converts them for
display. Fields that do not apply are present with `null`, as required by the
result schema.

The schema keeps summary values and source measurements together. Percentiles
can therefore be checked against the encoded histogram, and a future site can
render a different percentile without rerunning the workload. CI logs and
crash dumps remain workflow artifacts rather than permanent result files.

### Cost

Versioned price tables under `schema/` contain compute, request, storage, and
transfer rates with their currency, effective date, and source. Each result
records the table revision used for its calculation.

Workload cost covers the measurement window and final durability drain. It
excludes dataset preparation, warmup, cloning, and the direct object-store
probe. Request and transfer cost comes from the instrumented object-store
counters. Compute cost uses elapsed host time, while storage cost integrates
sampled database size over the same interval. Cost per million operations uses
successful measured operations as the denominator.

## Website

The website is a static Astro application built from `results/`. It defaults to
the latest stable SlateDB release and provides a version dropdown. Routes keep
the selection shareable:

```text
/<version>/<profile>/<workload>/<variant>/
```

The page displays one version and never computes a delta against another
version. Profile, workload, concurrency, and target-rate controls select result
variants within that version.

Use `~/Code/slatedb/website` as the visual reference for the site's design
aesthetic, layout, and CSS styling.

The layout favors density. A slim header contains the SlateDB wordmark and
version selector. A compact metadata row shows the machine, object store,
dataset, cache, and durability settings. Summary values sit above one main
chart with a latency/throughput toggle. A table below the chart exposes all
percentiles, durability, resource, storage, cost, and object-store baseline
fields. Each page links to its raw result files and the source commits.

The header wordmark uses Marcellus, matching `slatedb.io`. Body text uses Inter
and numeric tables use JetBrains Mono. The site reuses SlateDB's ink,
off-white, and terracotta color tokens. Charts support angled categorical
labels and include a tabular fallback. Result charts use a browser charting
library instead of Mermaid. The site needs client JavaScript only for selectors
and the chart toggle.

## Validation and publication

The runner validates every result against the checked-in JSON schema. It also
checks that histogram counts match operation counts, the durable frontier
covers all measured writes, resource samples span the measurement window, and
the workload used the expected initial manifest. Secrets, credentials, and
signed URLs are rejected from result files.

The workflow publishes a version after all required variants pass validation.
The site contains complete runs; failed and interrupted runs keep their logs as
CI artifacts. A rerun replaces the unpublished files for that version. The
published schema omits trial and repetition fields.
