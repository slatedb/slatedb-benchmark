# SlateDB benchmark suite

This repository publishes one benchmark suite for SlateDB. We borrowed useful
workload patterns from the [YCSB core workloads][ycsb] and RocksDB's
[`benchmark.sh`][rocksdb], then defined them against SlateDB's API, durability
model, and object-store architecture. The results are not YCSB or `db_bench`
results.

[ycsb]:
  https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads
[rocksdb]:
  https://github.com/facebook/rocksdb/wiki/performance-benchmarks

## Benchmark environment

Release runs use a WarpBuild `warp-ubuntu-latest-x64-16x` runner in Hetzner's
Frankfurt, Germany data center. Tigris uses its Frankfurt (`fra`) region. Each
preparation phase and workload has one published configuration. The release
does not vary clients, values, caches, machines, object stores, or SlateDB
settings within a preparation phase or workload.

The suite uses the SlateDB release defaults unless a preparation phase or
workload says otherwise. It configures these caches:

- 4 GiB block cache
- 512 MiB metadata/index cache
- 16 GiB local object-store cache

## Dataset

The canonical dataset contains 300,000,000 records with 20-byte keys and
400-byte values. This is about 117.3 GiB of logical key-value data. Generated
values have a target compression ratio of 1.0.

Keys contain an 8-byte big-endian unsigned record ID followed by 12 ASCII `0`
bytes. This matches the `db_bench` key format, preserves numeric ordering, and
keeps the encoding stable as the keyspace grows. The bulk loader writes each ID
once, and all later workloads use the same mapping.

## Dataset preparation

`bulk-load` runs first. It loads the canonical dataset with background
compaction disabled, flushes the writes, and saves an uncompacted checkpoint.
`full-compaction` clones that checkpoint, compacts the database, and saves the
golden checkpoint. These are tool phases, not workloads. The tool records their
results separately.

### Bulk load

The loader inserts all 300,000,000 records once. It prepares keys and values in
parallel, submits ordered batches through SlateDB's write API, and does not
wait for each batch to become durable. It disables background compaction for
the load, flushes the database, and saves the uncompacted checkpoint.

Measurement starts after the database opens and ends when the final flush
returns. Each 1,024-record batch appears as a `write` API call. The final flush
appears as one `flush` call.

Progress logs include completed records, recent and average records per second,
logical MiB/s, physical HTTP upload MiB/s, L0 flush MiB/s, backpressure,
elapsed time, and ETA.

### Full compaction

The tool clones the uncompacted checkpoint and opens it with the published
SlateDB settings. It records the input manifest, triggers a full compaction,
and waits for SlateDB to report idle. This phase has no warmup or client
operations. Its output becomes the golden checkpoint. The compaction wait has
no runner-level deadline. The GitHub job's 24-hour timeout remains the outer
limit.

Measurement starts after the cloned database opens and ends when full
compaction completes. This phase has no application API rows; its activity
appears in the object-store, process, and machine tables.

The measured steady-state workloads clone the golden checkpoint and do not
inherit another workload's writes.

Each clone starts with an empty local object-store cache. Warmup may fill it.
PUT caching and startup preloading stay disabled.

`sustained-ingest` starts with an empty database. It does not use the golden
checkpoint.

## Timing and durability

Active steady-state workloads use 64 closed-loop clients. A client waits for
one operation to return before issuing the next. Each workload gets a 5-minute
warmup and a 15-minute measurement unless its definition below overrides those
durations. The runner flushes warmup writes before measurement.

Writes use `await_durable = false`. Return latency ends when SlateDB accepts the
operation. The runner records each accepted write's sequence number and tracks
SlateDB's durable frontier independently. It measures durability latency from
the API return until the frontier covers that sequence. Tracking continues
through the post-measurement flush, and the runner records the final drain time.
The suite has no per-operation synchronous-write workload.

Every preparation phase and workload fails if SlateDB reports a compaction
failure while it runs. A workload stops after measurement and any durability
drain. It does not wait for ordinary background compaction to become idle.

## Workloads

| Workload | Initial state | Operations | Key selection |
| --- | --- | --- | --- |
| `idle` | Golden | No client operations | None |
| `point-read-uniform` | Golden | 100% point read | Uniform |
| `point-read-skewed` | Golden | 100% point read | Zipfian |
| `point-read-missing` | Golden | 100% point read | Uniform absent keys |
| `read-heavy` | Golden | 95% read, 5% update | Zipfian |
| `balanced` | Golden | 50% read, 50% update | Zipfian |
| `update-heavy` | Golden | 5% read, 95% update | Zipfian |
| `range-scan` | Golden | 100% forward scan | Uniform start ID |
| `sustained-ingest` | Empty | 100% insert | Unique IDs |
| `transaction-contention` | Golden | Ten-operation transactions | Hot set |

Zipfian workloads use an exponent of 0.99 over the fixed 300,000,000-record
domain. Inserts never expand that domain, and completed operations never remap
its ranks.

### Idle

Open a clone of the golden database and finish startup before taking the initial
metrics snapshot. Keep SlateDB and its normal background services running for
five minutes, with no warmup and no client API calls. Do not request a flush or
compaction at the end of the window.

### Point reads and updates

`point-read-uniform`, `point-read-skewed`, and the mixed read/write workloads
address records that exist in the golden dataset. Their reads should all hit.

`point-read-missing` selects record IDs uniformly, encodes each normal 20-byte
key, and changes the last padding byte from ASCII `0` to ASCII `1`. These absent
keys are distributed through the loaded key range rather than beyond its end.
A miss counts as a successful read with zero payload. Any hit or database error
fails the workload.

`update-heavy` overwrites existing records without growing the logical database.
Its compaction and write-amplification measurements cover churn over a fixed
keyspace. `read-heavy` and `balanced` use the same update behavior.

### Range scan

Each scan starts at a uniformly selected ID and returns up to 10 records in
ascending key order. A start position in the last nine records can return fewer
than 10.

### Sustained ingest

Sixty-four clients insert unique keys into an empty database for 60 minutes,
with no warmup. A final flush drains all accepted writes and completes the
durability record.

### Transaction contention

Each client runs serializable snapshot transactions against 10,000 existing
records. A transaction shuffles five reads and five updates, then commits. The
API tables retain the names `transaction.get`, `transaction.put`, and
`transaction.commit`.

## Metrics

The website shows seven tables and no charts: application operations,
application throughput, application latency, object-store requests,
object-store throughput, process statistics, and machine statistics.
Workload and preparation pages use these tables. Preparation pages also show
checkpoint references and golden dataset metadata. Full compaction omits the
three empty application tables. The website omits rows with no calls and keeps
zero values in rows that have calls. Values in the examples are illustrative.

The runner counts operations and samples machine counters once per second. The
workload recorders stay active through the durability drain, so totals and
latency statistics include drain activity. Preparation recorders cover the
phase boundaries defined above. Workload rate percentiles use complete
one-second client windows and exclude the durability drain and partial windows
at the boundaries. Preparation rate percentiles also include the final partial
window. Average rates divide the total by the full recorded interval. Latency
statistics use individual calls and milliseconds.

### Application operations

Each row identifies a SlateDB API call such as `get`, `put`, `delete`, `scan`,
`write`, or `flush`. Transaction calls retain the names `transaction.get`,
`transaction.put`, and `transaction.commit`. All columns except `total` use
calls per second.

| API | total | avg/s | p50/s | p95/s | p99/s | p99.9/s | min/s | max/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 45.5M | 50.6K | 50.5K | 52.1K | 53.2K | 54.0K | 47.1K | 54.0K |
| `put` | 45.6M | 50.6K | 50.5K | 52.2K | 53.3K | 54.1K | 47.2K | 54.1K |

### Application throughput

Application throughput measures logical data passed to or returned from
SlateDB. A `get` counts the request key and returned value. A `put` or `write`
counts its keys and values, while a `delete` counts its key. A `scan` counts all
returned keys and values. Calls such as `flush` transfer no logical data and do
not appear in this table. The `total` column uses GiB; the remaining columns use
MiB/s.

| API | total | avg | p50 | p95 | p99 | p99.9 | min | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 17.8 | 20.3 | 20.2 | 20.9 | 21.3 | 21.6 | 18.9 | 21.6 |
| `put` | 17.8 | 20.3 | 20.2 | 20.9 | 21.4 | 21.7 | 18.9 | 21.7 |

### Application latency

Each row uses the same API name as the operations table. The `durable` row is
not an API call. For each accepted write, its timer starts when the write call
returns and ends when SlateDB's durable frontier reaches the write's sequence
number. The row combines durability latency for the workload's accepted
writes. All values use milliseconds.

| API | avg | p50 | p95 | p99 | p99.9 | min | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `get` | 0.82 | 0.62 | 1.98 | 5.10 | 19.42 | 0.08 | 406.7 |
| `put` | 0.41 | 0.24 | 1.04 | 3.62 | 13.91 | 0.05 | 298.6 |
| `durable` | 54.8 | 48.3 | 99.2 | 132.8 | 241.7 | 1.2 | 1,932.4 |

### Object-store requests

Rows use physical HTTP methods such as `GET`, `PUT`, `HEAD`, `DELETE`, and
`POST`. Each retry counts as another operation, so request totals match the
traffic sent to the object store. All columns except `total` use operations per
second.

| Method | total | avg/s | p50/s | p95/s | p99/s | p99.9/s | min/s | max/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `GET` | 2,840,100 | 3,155.7 | 3,141 | 3,388 | 3,510 | 3,622 | 2,801 | 3,622 |
| `PUT` | 84,200 | 93.6 | 92 | 108 | 116 | 124 | 71 | 124 |
| `HEAD` | 18,400 | 20.4 | 20 | 25 | 28 | 31 | 13 | 31 |

### Object-store throughput

Object-store throughput measures physical HTTP body bytes. Each method's row
combines request and response bodies. Methods that transfer no body remain in
the requests table and do not appear here. The `total` column uses GiB; the
remaining columns use MiB/s.

| Method | total | avg | p50 | p95 | p99 | p99.9 | min | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `GET` | 167.0 | 190.0 | 184 | 238 | 270 | 310 | 110 | 310 |
| `PUT` | 83.5 | 95.0 | 92 | 121 | 139 | 158 | 54 | 158 |

### Process statistics

Process statistics cover the benchmark process, including SlateDB, its
embedded compactor, the async runtime, and the object-store client. CPU uses
cores, where `1.0` means one fully occupied vCPU. RSS uses GiB. The runner
calculates every column from complete one-second samples.

| Metric | avg | p50 | p95 | p99 | p99.9 | min | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPU utilization (cores) | 6.8 | 6.7 | 9.1 | 10.8 | 12.4 | 2.3 | 12.4 |
| RSS (GiB) | 7.1 | 7.2 | 7.7 | 7.8 | 7.9 | 5.9 | 7.9 |

### Machine statistics

CPU, network, and disk statistics cover the whole runner. RSS covers the
benchmark process, including SlateDB and its embedded compactor. The runner
calculates each column from complete one-second samples.

| Metric | avg | p50 | p95 | p99 | p99.9 | min | max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| CPU (%) | 46.2 | 45.8 | 63.1 | 74.4 | 88.7 | 21.0 | 88.7 |
| RSS (GiB) | 7.1 | 7.2 | 7.7 | 7.8 | 7.8 | 5.9 | 7.8 |
| Network receive (MiB/s) | 46.4 | 45.9 | 61.2 | 70.5 | 82.1 | 18.4 | 82.1 |
| Network send (MiB/s) | 21.5 | 21.2 | 29.8 | 34.6 | 41.0 | 8.2 | 41.0 |
| Disk read (MiB/s) | 14.9 | 14.2 | 22.4 | 27.1 | 34.8 | 3.1 | 34.8 |
| Disk write (MiB/s) | 10.7 | 10.3 | 16.1 | 19.5 | 24.2 | 2.8 | 24.2 |
| Disk read ops/s | 478 | 462 | 655 | 738 | 811 | 190 | 811 |
| Disk write ops/s | 320 | 311 | 446 | 502 | 558 | 124 | 558 |

Result bundles record the resolved preparation or workload definition, source
commits, scale, caches, and SlateDB settings.

## Validation

The publisher rejects results with operation errors. The runner keeps the failed
bundle for diagnosis and leaves that preparation phase or workload incomplete
in the session, so a retry measures it again.

The runner also checks these preparation and workload invariants:

- `bulk-load` creates the configured number of unique records and saves the
  expected uncompacted manifest.
- `full-compaction` starts from that manifest and completes without queued work.
- Golden workloads start from the saved manifest digest.
- Idle records no client operations or logical payload.
- Hit-only reads and updates of the canonical dataset do not miss.
- Every `point-read-missing` operation returns a miss.
- Observed operation mixes fall within the suite's statistical tolerance.
- Range scans return the expected number of records for their start ID.
- Sustained-ingest keys do not collide.
- Transaction outcomes reconcile with attempted transactions.

Golden prefixes store preparation results and checkpoints. Benchmark sessions
store workload results and database clones. Successful cleanup deletes the
clones and keeps the result markers, so a retry can skip completed workloads.

## Scaling

Smoke tests and website fixtures run the release suite with `--scale`; they do
not use a separate mock catalog. Scale reduces records, durations, and cache
capacities. It preserves operation mixes, clients, key and value sizes,
durability, preparation and workload order, and initial state. The publisher
rejects scaled results.

## Out of scope

- YCSB or RocksDB `db_bench` compatibility
- RocksDB direct-I/O and `sync=1` emulation
- Open-loop request generation
- Published configuration matrices
- Criterion microbenchmarks and regression alerts in the SlateDB repository
- Automated pass/fail comparisons between SlateDB releases
