# Design

[`BENCHMARKS.md`](BENCHMARKS.md) defines the dataset, preparation phases,
workloads, timing, validation rules, and published metrics. This document
defines the code and GitHub workflows that implement that contract.
`BENCHMARKS.md` wins if the documents disagree.

## Files and configuration

```text
config/settings.toml    SlateDB settings
.actrc                  Local runner and artifact configuration
src/                    Fixed config, runner, workloads, metrics, validation
schema/                 Published JSON schemas
results/<version>/      Published results
website/                Static Astro website
scripts/                Smoke, fixture, and publication commands
```

## Runner

### Process model

One worker process runs one preparation phase or workload. A worker owns the
SlateDB instance, client tasks, recorders, and local caches. GitHub runs each
workload on a separate WarpBuild machine. `act` runs the same jobs in local
Docker containers. Only worker samples enter task results.

Each worker creates new block, metadata, and local object-store caches. Golden
workloads open separate shallow clones of the golden checkpoint. The
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
and durability latencies use HDR histograms with microsecond precision and three
significant digits. The worker keeps these structures in memory until result
validation finishes.

The S3 recorder wraps the HTTP request-attempt boundary, so retries count as
separate requests. A `404 Not Found` response still counts as a request, but not
as a task error because SlateDB probes for optional objects. The Linux sampler
reads process and host counters once per second. Other errors remain in
diagnostic data and fail the task.

## Object-store state and recovery

Preparation data and benchmark sessions have different lifetimes:

```text
goldens/<golden-id>/
  bulk-load/result.json
  full-compaction/result.json

sessions/<session>/
  <workload>/result.json
```

Preparation results contain checkpoint references and golden dataset metadata.
They contain no benchmark metric tables. Workload results contain their metrics,
resolved configuration, source commits, and benchmark environment.

Every result is also its task's completion signal and is created last:

```text
run task
  -> validate its database and result
  -> finish database writes
  -> create result.json
```

The workflow creates `result.json` with an object-store create precondition. A
valid existing result skips the task. A missing result reruns it, while an
invalid result fails and requires cleanup. GitHub concurrency groups prevent
two jobs from writing the same golden phase or session task. The operator
chooses a new golden ID when the SlateDB commit or preparation configuration
changes.

Golden checkpoints remain immutable until explicit deletion. Each workload
clone uses a session- and task-specific prefix and owns its new manifests and
SSTs. After a successful run, cleanup deletes the workload database prefixes
and retains each `result.json` completion marker. The uncompacted and golden
checkpoints remain available for later benchmark runs.

## CLI

```console
$ slatedb-benchmark --help
Run SlateDB benchmarks

Usage: slatedb-benchmark <COMMAND>

Commands:
  run   Run one preparation phase or workload
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version

$ slatedb-benchmark run --help
Run one preparation phase or workload

Usage: slatedb-benchmark run [OPTIONS] --task <TASK> --golden <GOLDEN_ID> \
       --output <PATH>

Options:
      --task <TASK>
          Preparation task or workload from BENCHMARKS.md
          [possible values: bulk-load, full-compaction, idle,
          point-read-uniform, point-read-skewed, point-read-missing,
          read-heavy, balanced, update-heavy, range-scan, sustained-ingest,
          transaction-contention]

      --golden <GOLDEN_ID>
          Golden data name, for example slatedb-v0.14.1-001

      --session <SESSION>
          Benchmark session name; required for workload tasks

      --scale <FACTOR>
          Decimal scale factor greater than 0 and at most 1.0
          [default: 1.0]

      --output <PATH>
          Local result and diagnostic directory

  -h, --help
          Print help

Examples:
  slatedb-benchmark run --task bulk-load --golden slatedb-v0.14.1-001 \
    --scale 1.0 --output .runs/bulk-load

  slatedb-benchmark run --task full-compaction --golden slatedb-v0.14.1-001 \
    --scale 1.0 --output .runs/full-compaction

  slatedb-benchmark run --task balanced --golden slatedb-v0.14.1-001 \
    --session github-123456 --scale 1.0 --output .runs/balanced
```

Full compaction requires `bulk-load/result.json`. A golden workload requires
`full-compaction/result.json`. The workflow passes its `scale` input to
`--scale`; the scaling rules remain in [`BENCHMARKS.md`](BENCHMARKS.md).

Logs go to stderr. Stdout contains one machine-readable status record. Failure
returns a nonzero status and prints no success record.

## GitHub workflows

GitHub exposes two manual workflows. `golden.yml` creates reusable
golden data. `benchmark.yml` measures workloads against it. Neither workflow
starts the other. Both use the same repository concurrency group, so golden
preparation and benchmark sessions never compete for the benchmark object
store.

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
| `golden_id` | Yes | `slatedb-v0.14.1-001` |
| `publish` | Yes | `true` |
| `scale` | Yes | `1.0` |

`scale` is decimal. `1.0` runs the published size; `0.01` runs one percent.

A published run starts with these commands:

```console
$ gh workflow run golden.yml \
    -f slatedb_ref=v0.14.1 \
    -f golden_id=slatedb-v0.14.1-001 \
    -f scale=1.0

$ gh workflow run benchmark.yml \
    -f golden_id=slatedb-v0.14.1-001 \
    -f publish=true \
    -f scale=1.0
```

### `golden.yml`

| Job | Work |
| --- | --- |
| `build` | Resolve SlateDB and build the runner |
| `bulk-load` | Restore or create the uncompacted checkpoint |
| `full-compaction` | Restore or create the golden checkpoint |

Full compaction waits for the bulk-load job. Both jobs use the `result.json`
recovery rule defined above. A repeat dispatch skips phases with valid results.
Before rerunning a phase without a result, the workflow deletes that phase's
database prefix. Retrying full compaction preserves the bulk-load checkpoint
and replaces only the incomplete compacted clone.

The full-compaction job has a 24-hour GitHub timeout and no shorter runner
deadline. The workflow leaves both checkpoints in Tigris. Use a new golden ID
after changing the SlateDB commit or preparation configuration.

### `benchmark.yml`

| Job | Work |
| --- | --- |
| `validate-golden` | Verify and upload both preparation results |
| `build` | Build the current runner against the recorded SlateDB commit |
| `workloads` | Run the workload matrix |
| `bundle` | Assemble and checksum the preparation and workload results |
| `publish` | Commit results when the `publish` input is `true` |
| `cleanup` | Delete workload database clones after outputs are collected |

The workload matrix uses one WarpBuild machine per task and
`max-parallel: 4`. All workload jobs share Tigris, so `run.json` records that
limit. Each workload writes to
`sessions/<github.run_id>/<workload>/result.json`.

```text
new dispatch -> new github.run_id -> run every workload
rerun         -> same github.run_id -> skip completed workloads
```

The `publish` input controls the final job:

| `publish` | Scale | Result |
| --- | --- | --- |
| `true` | Must be `1.0` | Commit results and deploy Pages |
| `false` | May be smaller | Validate artifacts only |

Failed runs retain their session data. Successful cleanup keeps workload
completion markers and never deletes golden data.

### Credentials

| Jobs | Repository | Tigris |
| --- | --- | --- |
| `build` | Read | None |
| Preparation jobs | Read | Read and write |
| `validate-golden` | Read | Read |
| `workloads` | Read | Read and write |
| `bundle` | Read | None |
| `publish` | Write | None |
| `cleanup` | None | Read and write |
| Pages | Read | None |

Tigris credentials exist only on steps that need them. The publisher uses a
fresh checkout. Website installation runs `npm ci --ignore-scripts` without
benchmark credentials.

## Results and validation

```text
results/<version>/
  run.json
  preparation/
    bulk-load/result.json
    full-compaction/result.json
  workload/
    <name>/result.json
```

`run.json` records the golden ID, preparation and benchmark runner commits,
resolved configuration, matrix concurrency, and result checksums. Preparation
results describe the golden data and checkpoints. Workload results contain the
environment, initial database identity, and summaries defined in
[`BENCHMARKS.md`](BENCHMARKS.md).

The worker reads each result through strict Serde models and runs one semantic
validation pass. That pass checks internal counts, samples, durability
coverage, database identity, and the invariants in
[`BENCHMARKS.md`](BENCHMARKS.md). JSON schemas remain the published contract;
the runner does not repeat validation through a schema engine.

Successful workloads publish summaries and discard histograms and one-second
buckets. Failed tasks may include raw diagnostic files in their GitHub artifact.
Published files contain no credentials, signed URLs, cache paths, or session
tokens.

## Smoke tests and fixtures

`act` runs the GitHub workflows locally. The repository `.actrc` supplies the
WarpBuild label mapping and artifact server:

```text
-P warp-ubuntu-latest-x64-16x=catthehacker/ubuntu:act-latest
-P ubuntu-latest=catthehacker/ubuntu:act-latest
--container-architecture=linux/amd64
--container-options=--add-host=host.docker.internal:host-gateway
--artifact-server-path=.runs/act-artifacts
--env-file=.act.env
--secret-file=.act.secrets
```

A smoke run executes both workflows against the same persistent object-store
prefix. The two act files are gitignored.

```console
$ act workflow_dispatch \
    -W .github/workflows/golden.yml \
    --input slatedb_ref=v0.14.1 \
    --input golden_id=local-smoke \
    --input scale=0.01

$ act workflow_dispatch \
    -W .github/workflows/benchmark.yml \
    --input golden_id=local-smoke \
    --input publish=false \
    --input scale=0.01
```

Both local scripts run these commands and extract the `benchmark-results`
artifact. `scripts/smoke.sh` discards the output; `scripts/fixtures.sh` copies
it into the website fixture directory. The scripts contain no task lists or
dependency logic. Act jobs install the AWS CLI because the local runner image
does not include it.

The preparation handoff uses the golden prefix because `act` cannot download
artifacts from another workflow run. Local runs validate the runner and result
bundle, not GitHub controls or performance. `act` does not enforce
[several GitHub features](https://nektosact.com/not_supported.html), including
concurrency groups, job timeouts, and permissions.

## Website

The Astro website reads checked-in results during its build and deploys through
GitHub Pages. It has no database service or charting code.

```text
/<version>/preparation/<name>/
/<version>/workload/<name>/
```

Preparation pages display golden dataset and checkpoint information. Workload
pages display the tables defined in [`BENCHMARKS.md`](BENCHMARKS.md), omit
inapplicable rows, and keep measured zeroes visible. Result files and source
commits remain linked from each page.

The site uses the SlateDB logo, colors, and fonts: Marcellus for headings, Inter
for body text, and JetBrains Mono for numeric tables. The intended custom domain
is `benchmark.slatedb.io`.
