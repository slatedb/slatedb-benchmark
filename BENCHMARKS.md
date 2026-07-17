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
workload has one published configuration. The release does not vary clients,
values, caches, machines, object stores, or SlateDB settings within a workload.

The suite uses these SlateDB settings unless a workload says otherwise:

- 100 ms flush interval
- 4 GiB block cache
- 512 MiB metadata/index cache
- 16 GiB local object-store cache

## Dataset

The canonical dataset contains 300,000,000 records with 20-byte keys and
400-byte values. This is about 117.3 GiB of logical key-value data. Generated
values have a target compression ratio of 1.0. After full compaction, the live
LSM must contain at least 100 GiB of SST data.

Keys contain an 8-byte big-endian unsigned record ID followed by 12 ASCII `0`
bytes. This matches the `db_bench` key format, preserves numeric ordering, and
keeps the encoding stable as the keyspace grows. The bulk loader writes each ID
once, and all later workloads use the same mapping.

## Database setup

`bulk-load` runs first. It loads the canonical dataset with background
compaction disabled, flushes the writes, and saves an uncompacted checkpoint.
`full-compaction` clones that checkpoint, compacts the database, and saves the
golden checkpoint. The remaining steady-state workloads clone the golden
checkpoint and do not inherit another workload's writes.

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
operation. Durability lag ends when SlateDB reports the operation durable. The
runner flushes after measurement and records the drain time. The suite has no
per-operation synchronous-write workload.

The runner waits for compaction until SlateDB reports idle or reports a failed
compaction. It imposes no compaction deadline. The GitHub job's 24-hour timeout
remains the outer limit.

## Workloads

| Workload | Initial state | Operations | Key selection |
| --- | --- | --- | --- |
| `bulk-load` | Empty | 100% insert | Every ID once |
| `full-compaction` | Uncompacted | Full compaction | None |
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

### Bulk load

The loader inserts all 300,000,000 records once. It prepares keys and values in
parallel, submits ordered batches through SlateDB's write API, and does not
wait for each batch to become durable. It disables background compaction for
the load, flushes the database, and saves the uncompacted checkpoint.

The headline load rate covers the insert phase. The result records final flush
time separately. Progress logs include completed records, recent and average
records per second, logical MiB/s, physical HTTP upload MiB/s, L0 flush MiB/s,
backpressure, elapsed time, and ETA.

### Full compaction

Clone the uncompacted checkpoint and open it with the published SlateDB
settings. Record the input manifest, trigger a full compaction, and wait for
SlateDB to report idle. This workload has no warmup or client operations. Its
output becomes the golden checkpoint.

Report elapsed time, input and output SST bytes, input and output SST counts,
compactor read and write MiB/s, object-store requests and transferred bytes,
CPU, peak RSS, network activity, peak transient storage, and final live
database size. The result includes object-store cost for the full compaction
and cost per 100 GiB of input. The website does not extrapolate this workload
as continuous monthly traffic.

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
than 10. The runner reports operations per second, records per second, and the
bytes in all returned keys and values.

### Sustained ingest

Sixty-four clients insert unique keys into an empty database for 60 minutes,
with no warmup. The runner reports 5-minute windows for insert rate, logical and
physical throughput, compaction backlog, backpressure, write amplification,
live database size, and object-store requests. A final flush drains all accepted
writes and completes the durability record.

### Transaction contention

Each client runs serializable snapshot transactions against 10,000 existing
records. A transaction shuffles five reads and five updates, then commits. The
result reports transaction latency, commit latency, commits, conflicts, aborts,
and their rates. API metrics retain the names `transaction.get`,
`transaction.put`, and `transaction.commit`.

## Accounting

Payload bytes include the key and value for each successful point read or write.
Scans include every returned key and value. Misses have zero payload.

One-second application windows record completed and successful operations,
logical read and write bytes, return latency by operation, and SlateDB API
latency. Workloads with asynchronous writes also produce one-second durability
windows through the final drain.

Each result bundle contains:

- Identity and configuration, including the resolved workload definition,
  source commits, scale, caches, and SlateDB settings
- Application throughput, payload throughput, hits, misses, errors, and p50,
  p95, p99, p99.9, and maximum latency
- Durability lag, durable operations per second, final drain time, and sequence
  reconciliation
- CPU, peak RSS, network activity, and local disk activity
- Live and average database size, compaction throughput, write amplification,
  backpressure, and compaction backlog
- Logical object-store operations, physical HTTP requests, transferred bytes,
  retries, and errors
- The website's request and storage cost estimate. `full-compaction` reports
  one-run cost instead of a continuous monthly rate.

## Validation

The publisher rejects results with operation errors. The runner keeps the failed
bundle for diagnosis and leaves that workload incomplete in the session, so a
retry measures it again.

The runner also checks these workload invariants:

- Bulk load creates the configured number of unique records and saves the
  expected uncompacted manifest.
- Full compaction starts from that manifest, completes without queued work, and
  produces a live LSM of at least 100 GiB.
- Golden workloads start from the saved manifest digest.
- Idle records no client operations or logical payload.
- Hit-only reads and updates of the canonical dataset do not miss.
- Every `point-read-missing` operation returns a miss.
- Observed operation mixes fall within the suite's statistical tolerance.
- Range scans return the expected number of records for their start ID.
- Sustained-ingest keys do not collide.
- Transaction outcomes reconcile with attempted transactions.

The session stores completed workload bundles and both database checkpoints in
the object store. A retry restores them instead of rerunning completed work.

## Scaling

Smoke tests and website fixtures run the release suite with `--scale`; they do
not use a separate mock catalog. Scale reduces records, durations, and cache
capacities. It preserves operation mixes, clients, key and value sizes,
durability, workload order, and initial state. Scaled results use smoke mode and
cannot be published as release benchmarks.

## Historical results

Existing `ycsb`, `rocksdb`, and legacy `slatedb` results stay published. They
use different workload definitions and should not be compared with the unified
suite. New releases publish only the unified `slatedb` suite.

## Out of scope

- YCSB or RocksDB `db_bench` compatibility
- RocksDB direct-I/O and `sync=1` emulation
- Open-loop request generation
- Published configuration matrices
- Criterion microbenchmarks and regression alerts in the SlateDB repository
- Automated pass/fail comparisons between SlateDB releases
