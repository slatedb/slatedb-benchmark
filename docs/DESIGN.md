# Design

[`BENCHMARKS.md`](BENCHMARKS.md) defines the dataset, preparation phases,
workloads, timing, validation rules, and published metrics. This document
defines the code and GitHub workflows that implement that contract.
`BENCHMARKS.md` wins if the documents disagree.

## Files and configuration

```text
config/settings.toml             Optional default SlateDB settings
config/settings.<workload>.toml  Optional workload replacement
src/                             Runner, CLI, catalog, models, validation, bundling
schema/                          Generated JSON schemas
results/<version>/<run-id>/      Published results
tests/e2e/local.sh               Deterministic local end-to-end flow
website/                         Static Astro site using generated Rust types
scripts/                         External tool adapters
```

## Runner

### Process model

One worker process runs one preparation phase or workload. A worker owns the
SlateDB instance, client tasks, recorders, and local caches. GitHub runs each
workload on a separate WarpBuild machine. Only worker samples enter task
results.

Each worker creates new block and metadata caches. Golden workloads open
separate shallow clones of the golden checkpoint. The
`sustained-ingest` initial state and all client behavior come from
[`BENCHMARKS.md`](BENCHMARKS.md).

The worker uses monotonic time for intervals and latency. Wall-clock time is
used only for timestamps and logs.

### Measurement

A measured workload follows this sequence:

```text
open database and caches
run warmup
flush warmup writes
reset recorders and take counter baselines
run measurement
stop clients
drain measured writes to the durable frontier
stop recorders
write and validate the result
```

A preparation phase follows this sequence:

```text
open database and caches
run bulk load through its final flush, or wait for compaction to settle
close database and create checkpoint
write and validate the golden manifest
```

Preparation metrics are used only to detect task errors. They are not
serialized or published.

The runner implements the metric definitions in
[`BENCHMARKS.md`](BENCHMARKS.md) at these boundaries:

```text
SlateDB API call    -> operation count, logical bytes, latency
accepted write      -> sequence number and API return time
durable frontier    -> durable latency for completed writes
S3 request attempt  -> HTTP method and request/response body bytes
Linux sample        -> process and host counters
```

The recorders remain active through the durability drain. Published totals,
latency histograms, and resource samples include that interval. Rate percentile
buckets stop at the client boundary, before the durability drain. Rate averages
use the full interval from the counter baseline through the end of the drain.
Workload results store the client interval, durability drain, and full recorded
interval separately in nanoseconds.

The `scan` API row records each iterator `next()` call separately, including a
call that returns the end of the iterator. Latency ends when that call returns.
Iterator creation and the total time to consume a scan are not recorded as
`scan` latency.

API and object-store rate percentiles use complete one-second client windows.
Resource statistics continue to use one-second buckets through the drain. API
and durability latencies use HDR histograms with microsecond precision and
three significant digits. The worker retains each window's average and
percentiles for the elapsed-time chart and the aggregate histogram for result
validation.

The S3 recorder wraps the HTTP request-attempt boundary, so retries count as
separate requests. A `404 Not Found` response still counts as a request, but not
as a task error because SlateDB probes for optional objects. Failed HTTP
attempts do not fail the task by themselves. Terminal object-store failures
surface as task errors. The Linux sampler reads process and host counters once
per second.

## Object-store state and recovery

Preparation data and benchmark sessions have different lifetimes:

```text
goldens/<golden-id>/
  bulk-load/golden.json
  golden.json

sessions/<session>/
  <workload>/series.json
  <workload>/result.json
```

Golden manifests contain checkpoint references, dataset metadata,
configuration, source commits, and environment. Workload results contain their
metric summaries, configuration, source commits, and environment. Each
workload result names and authenticates its chart sidecar.

Every result is also its task's completion signal and is created last:

```text
run task
  -> validate its database and output
  -> finish database writes
  -> create golden.json, or series.json followed by result.json
```

The workflow creates each completion file with an object-store create
precondition. A valid existing manifest or result skips the task. A missing
completion file reruns it, while an invalid file or sidecar fails and requires
cleanup. A resumed workload checks the sidecar digest and restores both files
to its local artifact. GitHub concurrency groups prevent two jobs from writing
the same golden phase or session task. The operator chooses a new golden ID
when the SlateDB commit or preparation configuration changes.

Golden checkpoints remain immutable until explicit deletion. Each workload
clone uses a session- and task-specific prefix and owns its new manifests and
SSTs. After a successful run, cleanup deletes the workload database prefixes
and retains each `result.json` completion marker. The uncompacted and golden
checkpoints remain available for later benchmark runs.

## CLI

The single crate exposes focused subcommands:

- `prepare` runs `bulk-load` or `compaction`.
- `run` accepts measured workloads only.
- `validate` applies strict Serde and semantic validation to one artifact.
- `bundle` assembles and authenticates a versioned run.
- `publish` verifies and commits one run bundle.
- `generate` writes JSON Schema and TypeScript from Rust types.
- `catalog` prints the workload names used by the workflow matrix.
- `cleanup` removes one session prefix.

Logs go to stderr. Commands intended for automation print their result to
stdout. A failure returns a nonzero status.

Compaction requires `bulk-load/golden.json`. Golden-backed workloads require
the final `golden.json`. Scaling rules remain in [`BENCHMARKS.md`](BENCHMARKS.md).

## GitHub workflows

GitHub exposes three manual workflows. `golden.yml` creates reusable
golden data, `benchmark.yml` measures workloads against it, and
`transfer-capacity.yml` measures object-store performance independently. None
of the workflows starts another. Golden preparation and benchmark sessions use
the same repository concurrency group, so they never compete for the benchmark
object store. The transfer probe has its own concurrency group.

### Inputs

`golden.yml` accepts:

| Input | Required | Example |
| --- | --- | --- |
| `slatedb_ref` | Yes | `v0.14.1` |
| `golden_id` | Yes | `slatedb-v0.14.1-001` |
| `scale` | Yes | `1.0` |

`benchmark.yml` accepts:

| Input | Required | Example |
| --- | --- | --- |
| `slatedb_ref` | Yes | `main` |
| `golden_id` | Yes | `slatedb-v0.14.1-001` |
| `scale` | Yes | `1.0` |

`transfer-capacity.yml` accepts:

| Input | Required | Example |
| --- | --- | --- |
| `object_store` | Yes | `s3` |
| `scale` | Yes | `1.0` |

`scale` is decimal. `1.0` runs the published size; `0.01` runs one percent.
The golden and benchmark workflows resolve `slatedb_ref` independently. A
benchmark can use a golden checkpoint prepared by another SlateDB commit,
provided the requested build can read it. Before building the benchmark
runner, every `*.patch` file in `patches/slatedb` is applied in filename order.
Patches do not change the SlateDB version. Each run records their filenames
and SHA-256 digests, while the benchmark commit provides their exact contents.

A published run starts with these commands:

```console
$ gh workflow run golden.yml \
    -f slatedb_ref=v0.14.1 \
    -f golden_id=slatedb-v0.14.1-001 \
    -f scale=1.0

$ gh workflow run benchmark.yml \
    -f slatedb_ref=main \
    -f golden_id=slatedb-v0.14.1-001 \
    -f scale=1.0
```

### `golden.yml`

| Job | Work |
| --- | --- |
| `build` | Resolve SlateDB and build the runner |
| `bulk-load` | Restore or create the uncompacted checkpoint |
| `compaction` | Restore or create the golden checkpoint |

Compaction waits for the bulk-load job. Both jobs use the `golden.json`
recovery rule defined above. A repeat dispatch skips phases with valid manifests.
Before rerunning a phase without a result, the workflow deletes that phase's
database prefix. Retrying compaction preserves the bulk-load checkpoint and
replaces only the incomplete clone.

The compaction job has a 24-hour GitHub timeout and no shorter runner
deadline. The workflow leaves both checkpoints in Amazon S3. Use a new golden
ID after changing the SlateDB commit or preparation configuration.

### `benchmark.yml`

| Job | Work |
| --- | --- |
| `validate-golden` | Verify and upload the final golden manifest |
| `build` | Resolve the requested SlateDB ref and build the runner against it |
| `workloads` | Run the workload matrix |
| `bundle` | Assemble and checksum all run results |
| `publish` | Commit results and deploy Pages |
| `cleanup` | Delete workload database clones after outputs are collected |

The workload matrix uses one WarpBuild machine per task and does not impose a
parallelism cap. `run.json` records the number of workloads. Each workload writes to
`sessions/<github.run_id>/<workload>/{series,result}.json`.

The bundle discovers workload artifacts and accepts any nonempty subset of the
known workload names. This lets focused runs use the same workflow without a
second task list in the bundler. Unknown workload names and incomplete
result/series pairs still fail validation.

```text
new dispatch -> new github.run_id -> run selected workloads
rerun         -> same github.run_id -> skip completed workloads
```

Every successful bundle is published and deploys Pages. This includes scaled
runs, which follow the same artifact and validation path as full-size runs.

Failed runs retain their session data. Successful cleanup keeps workload
completion markers and never deletes golden data.

### `transfer-capacity.yml`

This standalone diagnostic workflow uses MinIO Warp to measure 4 MiB PUT and
GET throughput at concurrency 64, then 4 KiB PUT, GET, and LIST latency at
concurrency 1. It also records host and network diagnostics, including a TCP
traceroute and MTR packet-loss report. Raw Warp data remains in the GitHub
artifact. The workflow publishes only the summarized throughput, request
latency, TTFB, runner location, and object-store location used by the website.
Benchmark runs do not invoke or bundle this data.

### Credentials

| Jobs | Repository | Amazon S3 |
| --- | --- | --- |
| `build` | Read | None |
| Preparation jobs | Read | Read and write |
| `validate-golden` | Read | Read |
| Standalone transfer capacity | Write | Read and write |
| `workloads` | Read | Read and write |
| `bundle` | Read | None |
| `publish` | Write | None |
| `cleanup` | None | Read and write |
| Pages | Read | None |

Jobs that access S3 assume a scoped AWS role through GitHub OIDC. The
publisher uses a fresh checkout. Website installation runs
`npm ci --ignore-scripts` without benchmark credentials.

Each `benchmark-<object-store>` GitHub environment defines the credentials,
region, bucket, and optional endpoint needed by that provider.

## Results and validation

```text
results/<version>/
  <run-id>/
    run.json
    golden.json
    workload/
      <name>/
        result.json
        series.json
results/transfer-capacity/
  <provider>/
    <run-id>/
      result.json
```

The run ID is the benchmark session, such as `github-123456`. Publishing a run
replaces only that run ID and preserves every other run for the version.
`run.json` records the run ID, timestamp, applied patches, golden ID, measured
SlateDB source, golden and benchmark runner commits, resolved configuration,
matrix concurrency, and file checksums.
`golden.json` records the source and final checkpoint that created the golden
data. Workload results record the independently selected SlateDB source,
environment, metric summaries, and initial database identity.

The website generates immutable
`/<version>/run/<run-id>/workload/<name>/` routes. Shorter
`/<version>/workload/<name>/` routes show the newest run. The Recorded selector
lists run start times in the browser's local timezone, and the Patches section
links each patch to the benchmark commit that applied it.

Transfer-capacity results use
`/transfer-capacity/<provider>/run/<run-id>/` routes. The shorter provider route
shows its newest result. Object-store names in benchmark context link to these
pages.

The worker reads each result through strict Serde models and runs one semantic
validation pass. Rust types are the artifact contract. `slatedb-benchmark generate`
derives the published JSON schemas and website TypeScript declarations from
those types. Contract tests serialize one representative value per top-level
artifact and validate it against the generated schema.

Workload sidecars contain complete rate and resource buckets plus populated HDR
histogram buckets. `result.json` stores the sidecar digest, and `run.json`
checksums both files. Failed tasks may include raw diagnostic files in their
GitHub artifact. Published files contain no credentials, signed URLs, cache
paths, or session tokens.

## Tests

Tests cover each behavior at the lowest useful layer:

1. Rust unit tests cover workload decisions, metric aggregation, durability,
   retries, settings precedence, and semantic validation.
2. Contract tests serialize one representative golden, result, series, run,
   and transfer-capacity artifact and validate each against its generated
   schema. They also fail when checked-in schemas or TypeScript declarations
   are stale.
3. `tests/e2e/local.sh` runs both preparation phases and a short balanced
   workload against a local object store, bundles the output, and builds the
   website from that bundle.
4. The manual golden and benchmark workflows are the optional real-object-store
   smoke tests. They do not run in ordinary pull-request CI.

Pull-request CI runs formatting, Clippy, Rust tests, generated-contract checks,
the local end-to-end test, and `actionlint`. It does not simulate GitHub Actions
with Docker.

## Website

The Astro website reads checked-in results during its build and deploys through
GitHub Pages. It has no database service.

```text
/<version>/dataset/
/<version>/workload/<name>/
```

The dataset page displays metadata from the final compaction checkpoint. The
Golden data value on every workload page links to it. The intermediate
bulk-load manifest has no HTML route. Workload descriptions live in
`website/src/content/workloads/<name>.md`; the build requires one file per
workload. Each page renders the description below the workload title. Workload
table rows open their charts below the row. The browser fetches one sidecar
after page load and reuses it for every row. Data-saving mode disables the
preload; a click still fetches it. Workload tables omit inapplicable rows and
keep measured zeroes visible.

The site uses the SlateDB logo, colors, and fonts: Marcellus for headings, Inter
for body text, and JetBrains Mono for numeric tables. The intended custom domain
is `benchmark.slatedb.io`.
