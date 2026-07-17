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

API rates, object-store rates, and resource statistics use one-second buckets.
API and durability latencies use HDR histograms with microsecond precision and
three significant digits. The worker keeps these structures in memory until
result validation finishes.

The S3 recorder wraps the HTTP request-attempt boundary, so retries count as
separate requests. The Linux sampler reads process and host counters once per
second. Errors remain in diagnostic data and fail the task.

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
SSTs. Benchmark cleanup deletes session data only. The uncompacted and golden
checkpoints remain available for later benchmark runs.

## CLI

Workflow jobs and direct debugging use the same binary. Preparation jobs invoke
the two phases explicitly:

```console
$ ./target/release/slatedb-benchmark run \
    --task bulk-load \
    --golden slatedb-v0.14.1-001 \
    --output .runs/bulk-load

$ ./target/release/slatedb-benchmark run \
    --task full-compaction \
    --golden slatedb-v0.14.1-001 \
    --output .runs/full-compaction
```

A matrix job invokes one workload:

```console
$ ./target/release/slatedb-benchmark run \
    --task balanced \
    --golden slatedb-v0.14.1-001 \
    --session github-123456 \
    --output .runs/balanced
```

`--task` is required. Full compaction requires `bulk-load/result.json`, and a
golden workload requires `full-compaction/result.json`. The workflow passes
`--scale` from its dispatch input; the scaling rules remain in
[`BENCHMARKS.md`](BENCHMARKS.md).

Logs go to stderr. Stdout contains one machine-readable status record. Failure
returns a nonzero status and prints no success record.

## GitHub workflows

Preparation and measurement use separate `workflow_dispatch` workflows:

```text
prepare-golden inputs: slatedb_ref, golden_id, mode, scale
benchmark inputs:      golden_id, mode, scale

prepare-golden.yml (golden A)
  setup -> build -> bulk-load -> full-compaction

benchmark.yml (golden A, github.run_id)
  validate golden -> build -> workload matrix -> publish -> Pages
                                      |
                                      +-> clean up session data
```

`scale` is a decimal workflow input, where `1.0` is full scale and `0.01` is
one percent. `mode` controls the last step:

```text
published -> require scale 1.0 -> publish results
smoke     -> allow scaling   -> validate artifacts
fixtures  -> allow scaling   -> retain artifacts for the local website
```

### `prepare-golden.yml`

The caller uses a new golden ID when the SlateDB commit or preparation
configuration changes. Each job checks its result before doing work:

```text
valid result.json -> skip phase
missing result    -> run phase
invalid result    -> fail
```

The full-compaction job depends on bulk load. Its 24-hour GitHub job timeout is
the only compaction deadline. A later dispatch with the same golden ID finishes
an interrupted preparation without repeating a committed phase. Preparation
does not trigger the benchmark workflow or delete either checkpoint.

### `benchmark.yml`

The input is a completed golden ID. A validation job reads both preparation
results and uploads them as an artifact for the current workflow run.

The build uses the current benchmark runner and the SlateDB commit recorded in
the full-compaction result. The workload matrix uses one WarpBuild machine per
task and `max-parallel: 4`. Each job gets a session- and task-specific
object-store prefix.

All matrix jobs share Tigris. `run.json` records `max-parallel` because changing
object-store concurrency can affect application latency.

The session name comes from `github.run_id`:

```text
new workflow dispatch -> new session -> measure every workload
rerun same dispatch    -> same session -> restore committed workloads
```

In `published` mode, the publisher downloads all artifacts, builds `run.json`,
replaces `results/<version>/`, and pushes `main`. Pages deploys after that
commit. Other modes stop after artifact validation. Successful cleanup removes
the benchmark session's clones and candidates. Failed runs keep them for a
retry.

Tigris credentials are scoped to steps that read or write benchmark data.
Repository write access belongs only to the publisher, which receives no
Tigris credentials. Website installation uses `npm ci --ignore-scripts` and
receives neither credential.

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
--container-architecture=linux/amd64
--artifact-server-path=.runs/act-artifacts
--env-file=.act.env
--secret-file=.act.secrets
```

A smoke run executes both workflows against the same persistent object-store
prefix. The two act files are gitignored.

```console
$ act workflow_dispatch \
    -W .github/workflows/prepare-golden.yml \
    --input slatedb_ref=v0.14.1 \
    --input golden_id=local-smoke \
    --input mode=smoke \
    --input scale=0.01

$ act workflow_dispatch \
    -W .github/workflows/benchmark.yml \
    --input golden_id=local-smoke \
    --input mode=smoke \
    --input scale=0.01
```

`scripts/smoke.sh` wraps those commands. `scripts/fixtures.sh` changes the mode
to `fixtures` and copies the result artifact into the website fixture directory.
The scripts contain no task lists or dependency logic.

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

The site uses the SlateDB header, colors, and fonts: Marcellus for the wordmark,
Inter for body text, and JetBrains Mono for numeric tables. The intended custom
domain is `benchmarks.slatedb.io`.
