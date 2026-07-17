# Design

[`BENCHMARKS.md`](BENCHMARKS.md) defines the dataset, preparation phases,
workloads, timing, and published metrics. This document defines the runner,
result files, release workflow, and website that implement that specification.
`BENCHMARKS.md` takes precedence if the two documents disagree.

The repository contains one release suite named `slatedb`. `bulk-load` and
`full-compaction` prepare its database checkpoints. The remaining named tasks
are workloads. Each task has one published configuration.

## Repository

The target layout is:

```text
config/slatedb.suite.toml       Dataset, task, timing, and cache configuration
config/slatedb.settings.toml    SlateDB engine settings
src/                            Runner, workloads, metrics, and validation
schema/                         JSON schemas for published files
results/<version>/              Published release results
website/                        Static Astro website
scripts/                        Smoke, fixture, and publication commands
```

The TOML files are the executable form of `BENCHMARKS.md`. Configuration tests
pin the release values documented there. A pull request that changes a release
parameter must update the TOML, its test, and `BENCHMARKS.md` together.

The runner records the selected SlateDB commit, runner commit, lockfile hash,
resolved configuration, and benchmark environment in each result. The website
reads checked-in result files during its build. It has no database service.

## Configuration

`config/slatedb.suite.toml` declares:

- the canonical dataset
- preparation order and workload order
- warmup and measurement durations
- client count and operation mixes
- key selection parameters
- cache capacities
- scale lower bounds

Each task name identifies one configuration. Active workloads use the 64
clients specified in `BENCHMARKS.md`.

The runner constructs SlateDB settings in this order:

```text
SlateDB Settings::default()
  -> config/slatedb.settings.toml
  -> bulk-load overrides
```

The bulk loader disables background compaction while it writes the canonical
dataset. Full compaction and measured workloads use the published settings.
The runner creates block and metadata caches through `DbBuilder` and gives each
worker a new local object-store cache directory. Results contain the resolved
settings and cache capacities, without local filesystem paths.

The runner validates the complete configuration before it creates a database.
It rejects duplicate task names, unknown operations, invalid probabilities,
missing required durations, and preparation dependencies that cannot be
satisfied.

## Task dependencies

Release execution has a sequential preparation stage followed by a workload
matrix:

```text
bulk-load -> uncompacted checkpoint -> full-compaction -> golden checkpoint
                                                        -> golden workloads
empty database -> sustained-ingest
```

The preparation job runs `bulk-load` and `full-compaction` on one WarpBuild
machine. Once the golden checkpoint commits, GitHub Actions starts one matrix
job per workload. The matrix runs at most four jobs at a time, and each job gets
its own WarpBuild machine. `sustained-ingest` waits for preparation to finish
even though it creates an empty database. This keeps preparation traffic out of
workload measurements.

Workload jobs share the Tigris bucket and use task-specific prefixes. The
matrix limit bounds concurrent demand on Tigris. The run manifest records this
limit because shared service load can affect application latency.

### Workers

The preparation job starts a fresh worker process for each phase. Each matrix
job starts one worker for its workload. The worker owns the SlateDB instance,
client tasks, metric recorders, and local caches. Process statistics cover this
worker. Workflow and runner setup do not enter a task result.

Each active workload starts 64 closed-loop clients. A client chooses an
operation, waits for its SlateDB call to return, records the result, and then
chooses its next operation. `idle` starts no clients. The bulk loader uses its
record-count-driven path: bounded CPU tasks prepare keys and values, and one
writer submits the ordered batches. Full compaction starts no application
clients.

The worker uses monotonic time for intervals and latency. Wall-clock time is
limited to result timestamps and log messages.

A full manual or smoke run uses one worker at a time on its host. The release
workflow provides workload parallelism by assigning workers to separate
machines; the runner does not run measured workloads together on one machine.

### Checkpoints and clones

Bulk load writes all canonical records with background compaction disabled. It
flushes the database and creates an uncompacted checkpoint. Full compaction
starts from a shallow clone of that checkpoint, triggers SlateDB's full
compaction operation, and waits for either idle or a reported compaction
failure. It creates the golden checkpoint after the compactor reaches idle.

Every golden workload starts from its own shallow clone of the golden
checkpoint. The checkpoint and its SSTs remain immutable while workload jobs
run. Each clone uses a task-specific prefix and owns any new SSTs, manifests,
or compaction output. A workload cannot modify another workload's input.
`sustained-ingest` creates a new empty database instead.

The preparation task marker records the checkpoint ID, manifest ID, and
manifest digest. A workload checks the digest before it opens SlateDB. Each
worker also starts with new block, metadata, and local object-store caches.
Warmup may populate them.

Full compaction alone waits for ordinary compaction to become idle. Other tasks
stop after measurement and their durability drain. Any task fails as soon as
SlateDB reports a compaction failure.

## Sessions and recovery

A run has a stable session name. The release workflow derives it from
`github.run_id`, so a rerun of the same workflow restores the same session.
Manual runs supply the name through the CLI.

The session prefix contains one identity file and one commit marker per
completed task:

```text
sessions/<session>/
  identity.json
  tasks/
    bulk-load/commit.json
    full-compaction/commit.json
    <workload>/commit.json
```

`identity.json` records the source commits, lockfile, resolved configuration,
scale, runner type, CPU count, memory, region, and object-store endpoint. The
preparation job creates it with an object-store create precondition. Each later
job reads it before doing work and rejects a mismatch.

A task commit marker contains the task name, result path, result checksum, and
completion time. Preparation markers also contain their checkpoint ID,
manifest ID, and manifest digest. The worker creates the marker last:

```text
run task
  -> write candidate result
  -> read and validate candidate result
  -> upload result and checkpoint data
  -> create tasks/<task>/commit.json
```

The marker is the task commit point. A restart checks the known marker paths
from the configured task list. It restores and skips a task whose marker
exists. A candidate result without a marker is incomplete and can be replaced
by a retry.

Workload jobs write separate marker paths, so they never update one shared
session object. A GitHub concurrency group prevents two jobs from running the
same session and task. The create precondition on `commit.json` protects the
same boundary for manual runs.

A workload job that finds its marker restores the committed result and uploads
the GitHub artifact again. This lets a rerun publish a complete artifact set
without measuring completed workloads.

The uncompacted checkpoint remains until full compaction commits. The golden
checkpoint remains while any workload marker is missing. A cleanup job removes
checkpoint and candidate database data after publication succeeds. Failed
workflows retain that data for another attempt.

## Measurement lifecycle

A steady-state worker follows this order:

```text
open database and caches
run warmup
flush warmup writes
reset recorders
take baseline counters
run measurement
stop clients
drain durability
write result
```

Startup and warmup do not contribute to published measurements. The `idle` and
`sustained-ingest` workloads use the timing exceptions in `BENCHMARKS.md`.
Preparation phases define their own observation intervals.

The runner instruments SlateDB calls made during the observation interval.
Post-measurement durability tracking continues until the durable frontier
covers the last measured write. Setup and cleanup calls outside the interval do
not enter application operation rates.

The sampler creates one-second windows aligned to the start of measurement. It
uses complete windows for rate and resource percentiles. Partial windows at the
start and end do not enter those distributions. Totals cover the full
measurement interval, and average rates divide those totals by its duration.

## Application metrics

The API wrapper records operation counts, logical bytes, and a latency histogram
for each API name. It applies the naming and logical-byte rules in
`BENCHMARKS.md`. Calls with no observations do not produce rows.

The runner stores counts and logical bytes in one-second buckets. An API that
appears at least once has zero values for complete windows in which it made no
calls. The shared summary builder derives the published operation and throughput
columns defined in `BENCHMARKS.md`.

Latency timers cover invocation through return for an individual API call. The
runner records nanoseconds and builds HDR histograms with microsecond precision
and three significant digits. The summary builder derives the latency columns
defined in `BENCHMARKS.md`, and the website converts them to milliseconds.

Operation failures increment internal error counters. They remain available in
failed CI diagnostics, but the website model does not publish an outcome
table. Validation rejects a result with an application error.

### Durability latency

Writes use `await_durable = false`. The API latency timer stops when SlateDB
accepts the write. The worker then records the returned sequence number and
timestamp in the durability tracker.

A background task subscribes to SlateDB's durable frontier. Each frontier
update completes all pending writes at or below that sequence number and adds
their elapsed time to the `durable` histogram. The tracker does not block
clients.

After measurement, the worker calls `flush()` and waits until the frontier
covers the last measured write. Durability timing continues through this drain.
The result uses one `durable` row across all measured write APIs. Validation
checks that each accepted measured write reached the frontier once.

## Object-store metrics

The runner instruments the HTTP transport used by the S3 client. The recorder
sits at the request-attempt boundary, so a retry produces another request. It
groups counts by HTTP method.

The transport recorder counts request-body bytes sent and response-body bytes
consumed for each attempt. It combines both directions when it calculates body
throughput for a method.

One-second method buckets feed the shared summary builder, which derives the
request and throughput columns defined in `BENCHMARKS.md`. The website does not
show object-store latency. Transport status and retry details remain in
diagnostic data for failed runs.

## Process and machine metrics

The Linux sampler reads process and host counters once per second. It uses
counter deltas for rates and direct samples for gauges.

Process CPU is the worker's CPU time during the sample divided by elapsed wall
time. The result expresses it in cores, where `1.0` is one occupied vCPU. RSS
is the resident memory observed at the sample boundary. Its maximum is the
largest one-second sample.

Machine CPU is the host-wide busy fraction. Network and disk rows use host-wide
counter deltas. Disk results include byte rates and operation rates. The
machine table reuses the process RSS samples required by `BENCHMARKS.md`; the
runner does not collect a second RSS series.

The shared summary builder derives the process and machine columns defined in
`BENCHMARKS.md`.

## Results

The result tree separates preparation phases from workloads:

```text
results/<version>/
  run.json
  preparation/
    <name>/
      result.json
  workload/
    <name>/
      result.json
```

The publisher creates `run.json` after every workload job succeeds. Workload
jobs do not update it. The file contains release identity, resolved
configuration, matrix concurrency, and the path and checksum of each committed
result. `result.json` contains the environment, initial database identity, and
the website summaries defined in `BENCHMARKS.md`. Inapplicable sections are
absent.

Histograms and one-second buckets remain in worker memory until validation
finishes. A successful task publishes their summaries in `result.json` and
discards the raw data. A failed task may write raw diagnostics to its local run
directory; CI preserves that directory as a workflow artifact.

Published JSON uses integers for counts and bytes and nanoseconds for
durations. Display units belong to the website. Strict Serde models reject
unknown fields. Checked-in JSON schemas define the published file format.

Result files contain no credentials, signed URLs, local cache paths, or session
tokens. CI logs and crash output remain workflow artifacts.

## Validation

The worker writes a complete candidate result and reads it back through strict
Serde models. It compares the decoded summaries with the in-memory histograms
and one-second buckets during one semantic validation pass. The worker creates
the task marker after that pass. The release workflow and publisher do not
repeat it.

Validation checks:

- result path, checksum, identity, and resolved configuration
- histogram counts against API counts
- rate summaries against complete one-second buckets
- sample coverage for the measurement interval
- durability coverage for accepted measured writes
- initial checkpoint and manifest digests
- zero final application errors
- the preparation and workload invariants in `BENCHMARKS.md`
- absence of secrets and local paths

Transaction outcome counters exist for validation. Every attempted transaction
must end as committed, conflicted, or failed, and those internal counts must
sum to the attempt count. They do not appear in the website result tables.

A failed candidate remains available as a CI artifact for diagnosis. The
worker leaves its task marker absent, so a session retry runs that task again.
JSON schemas remain the publication contract; the runner does not execute a
second JSON Schema validation pass.

## CLI

CI and manual runs use the same binary:

```console
$ cargo build --release --locked
$ ./target/release/slatedb-benchmark --version
slatedb-benchmark <runner-version> (slatedb <version> <commit>)
```

The repository has one suite, so `run` needs no suite selector. A full run uses:

```console
$ ./target/release/slatedb-benchmark run \
    --session github-123456 \
    --output .runs/release
```

`--task` selects one preparation phase or workload. The runner restores or
creates its dependencies before it runs the selected task:

```console
$ ./target/release/slatedb-benchmark run \
    --task balanced \
    --session manual-balanced \
    --output .runs/balanced
```

The release preparation job invokes the two preparation tasks in order. Each
workload matrix job passes its matrix task name to `--task`, reuses the release
session, and writes to its own local output directory. Omitting `--task` keeps
manual and smoke runs sequential on one host.

A scaled run adds `--scale`:

```console
$ ./target/release/slatedb-benchmark run \
    --scale 1% \
    --session smoke \
    --output .runs/smoke
```

The binary reads S3-compatible configuration from the environment:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export AWS_ENDPOINT_URL_S3=https://t3.storage.dev
export AWS_ENDPOINT_URL_IAM=https://iam.storage.dev
export SLATEDB_BENCH_BUCKET=slatedb-benchmarks
export SLATEDB_BENCH_PREFIX="manual/$USER"
```

Progress and diagnostics go to stderr. Stdout contains one machine-readable
status record. A failure returns a nonzero status and does not print a success
record.

## Scaling, smoke, and fixtures

`--scale` overlays the release configuration after parsing. It reduces record
counts, durations, and cache capacities subject to configured lower bounds. It
preserves client count, key and value sizes, operation mixes, durability,
preparation order, workload order, and initial database choice.

A scaled run records `mode = smoke` and the requested factor. The publisher
accepts `mode = published` at scale `1`.

`scripts/smoke.sh` runs the release suite against a local object store at a
small scale and validates its output. It does not upload results or build the
website. `scripts/fixtures.sh` runs the same suite at a small scale and places
its results where the website can use them during local development. Neither
path uses synthetic result data.

## Release workflow

The release workflow accepts a SlateDB tag or commit. A build job checks out
that source, replaces the runner's SlateDB dependencies with `cargo add
--path`, records the resolved commit, and builds one release binary from the
locked dependency set.

The preparation job runs on the WarpBuild machine specified in
`BENCHMARKS.md`. It restores or runs `bulk-load`, then restores or runs
`full-compaction`. Its 24-hour job timeout is the outer limit for full
compaction; the runner places no separate deadline on that phase. The job logs
the host, filesystem, disk, and object-store connection profile before it
starts.

After preparation succeeds, a GitHub Actions matrix starts one job for each
workload. `max-parallel` is four. Every job gets its own WarpBuild machine,
uses the same session and golden checkpoint, and writes to its task-specific
object-store prefix. A workload that already has a commit marker restores its
result instead of running again. Either way, the job uploads that result as a
GitHub artifact.

The workflow has this dependency chain:

```text
build runner
    |
    v
prepare data
    |
    v
workload matrix (at most four jobs at a time)
    |
    v
publish results
    +-> deploy Pages
    +-> clean up session data
```

The preparation and workload jobs use a session derived from `github.run_id`.
A rerun restores completed tasks from Tigris. Each job uploads its local output
after success or failure so failed candidates remain available for diagnosis.

All workload jobs use the same Tigris service, so concurrent jobs can affect
one another through shared service load. Running each workload on a separate
machine prevents host CPU, memory, disk, and network counters from mixing. The
four-job limit bounds object-store concurrency and is recorded in `run.json`.
Changing it changes the benchmark environment.

Build and benchmark steps have read-only repository access. Tigris credentials
are scoped to the preparation and workload run steps. The publisher uses a
fresh checkout with repository write access and receives no Tigris
credentials. The Pages job installs website dependencies with
`npm ci --ignore-scripts` and has no benchmark credentials.

The publisher waits for preparation and every workload job. It downloads their
artifacts, verifies that every configured task has one valid result, and builds
`run.json` from those results. It replaces the destination for that SlateDB
version, commits the result files, and pushes `main`. It does not read or write
session markers.

The Pages workflow builds and deploys the static site after the result commit.
After publication succeeds, a cleanup job with Tigris credentials and no
repository write access removes the session's checkpoints, database clones,
and incomplete candidates. A failed workflow leaves them intact for a rerun.

## Website

The Astro website builds from checked-in result files and deploys through
GitHub Pages. The intended custom domain is `benchmarks.slatedb.io`.

Routes separate preparation results from workload results:

```text
/<version>/preparation/<name>/
/<version>/workload/<name>/
```

Each result page displays configuration and environment context followed by
the tables defined in `BENCHMARKS.md`, in the order specified there.

The page omits inapplicable rows and sections. A measured zero remains visible.
Preparation pages use the same table components as workload pages. Links expose
the raw result files and source commits.

The website renders no charts. Version and task navigation use normal links, so
result pages need no charting library or client-side data loader.

The header, wordmark, fonts, and colors follow the SlateDB website. Marcellus
is the wordmark font, Inter is the body font, and JetBrains Mono is used for
numeric tables. The layout keeps metric tables readable on narrow screens
without hiding columns.
