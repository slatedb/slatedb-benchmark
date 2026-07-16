# Benchmark suites

Run all suites for each SlateDB release on a WarpBuild
`warp-ubuntu-latest-x64-16x` runner against Tigris in the `fra` region.

Each workload has one fixed effective configuration. SlateDB settings inherit
from the library defaults and then the suite. Do not cross product workloads
with additional value sizes, cache sizes, machines, object stores, or SlateDB
settings.

The `config/` directory contains two files per suite:
`<suite>.suite.toml` defines the suite, its ordered `[[workloads]]`, and
their nested variants; `<suite>.settings.toml` contains the suite-wide
SlateDB settings. Smoke and local website fixture generation run these same
release suites with the runner's `--scale` overlay; there is no separate reduced
catalog.

Scale reduces data volume, phase durations, cache capacities, and object-store
probe work while preserving workload semantics such as client counts, record
shape, operation mix, durability, and ordering. Scaled outputs are marked as
smoke results and cannot be published as release benchmarks.

Each measured variant starts with an empty local object-store cache. Warmup may
populate it; PUT caching and startup preloading remain disabled.

## Standard results

Every workload emits the same result record. Use `null` for fields that do not
apply.

- Identity: SlateDB version and commit, timestamp, suite, and workload
- Environment: runner type, CPU model and core count, RAM, local disk, OS and
  kernel, object store, endpoint, and region
- Object-store baseline: direct upload and download MiB/s and p50, p95, p99,
  p99.9, and maximum request latency from the runner to the benchmark bucket,
  measured outside SlateDB
- Configuration: scale, client count, duration, record count, key and value sizes,
  target value compression ratio, cache sizes, SlateDB settings, build profile,
  and enabled features
- Application performance: total operations, accepted and completed ops/s, payload MiB/s,
  errors, and p50, p95, p99, p99.9, and maximum return latency overall and per
  operation type. The same percentiles are recorded for each SlateDB API used
  during measurement. One-second application windows report return and API
  latency, successful-operation counts, and logical read, write, and total
  payload bytes over time.
- Durability performance: p50, p95, p99, p99.9, and maximum durability lag,
  final flush/drain time, and durable ops/s. Asynchronous writes also report
  one-second durability-lag windows through the final drain; awaited writes do
  not have a separate post-return lag.
- Resources: average and peak CPU, peak RSS, host-wide network bytes sent and
  received, and disk bytes and operations read and written
- Storage: compactor read and write throughput over time, aggregate compaction
  throughput, write amplification, backpressure time, and compaction backlog
- Object store operations: final and time-weighted average database size,
  logical operations and payload bytes, physical HTTP requests and body bytes,
  retries, and client, server, transport, and final operation errors
- Cost: estimated 30-day request and storage cost if the displayed workload ran
  continuously, using the selected standard regional Amazon S3, Azure Blob
  Storage, or Google Cloud Storage bucket in US East, with request costs broken
  down by operation

Published results have a zero-error policy. The runner records final operation
errors and continues the measurement long enough to produce a complete
diagnostic bundle, but validation rejects any variant whose `errors` count is
nonzero. Consequently, the `errors` field is useful in failed-run artifacts and
is always zero in published results. A workload with an operation error is not
committed to its resumable session and is measured again when that session is
retried.

## YCSB suite

- Dataset: 100 million records with 16-byte keys and 1 KiB values
- Cache: 4 GiB block cache, 512 MiB metadata/index cache, and 16 GiB local
  object-store cache
- Writes: `await_durable=false` with `flush_interval=100ms`
- Concurrency: 64 clients
- Run: 60-second warmup followed by 5 minutes of measurement

Reset each test to the same preloaded dataset. YCSB A, B, C, E, and F use YCSB's
scrambled Zipfian key distribution: exponent 0.99 over YCSB's 10-billion-rank
space, with each sampled rank mapped into the loaded keyspace by YCSB's FNV-64
hash. Logical IDs are hashed again before fixed-width encoding to preserve
YCSB's default hashed insertion order. Workload E fixes its Zipfian domain at
twice the initial record count and rejects keys above the acknowledged insert
frontier, so inserting records never remaps existing hot keys. Call `flush()`
after measurement and include the drain in the durability results.

1. `ycsb-a`: 50% reads and 50% updates.
2. `ycsb-b`: 95% reads and 5% updates.
3. `ycsb-c`: 100% reads.
4. `ycsb-d`: 95% reads of recently inserted keys and 5% inserts.
5. `ycsb-e`: 95% forward scans and 5% inserts. Scan lengths are uniformly
   distributed from 1 to 100 records.
6. `ycsb-f`: 50% reads and 50% read-modify-write operations.

## RocksDB suite

Follow RocksDB's published
[`benchmark.sh` sequence](https://github.com/facebook/rocksdb/wiki/performance-benchmarks)
and the defaults in
[`tools/benchmark.sh`](https://github.com/facebook/rocksdb/blob/main/tools/benchmark.sh)
where SlateDB has an equivalent setting.

- Keyspace: 900 million possible 20-byte keys with 400-byte values generated
  with RocksDB's default 0.5 target compression ratio. Bulk load performs 900
  million uniformly random puts with replacement, matching `fillrandom`, so it
  produces about 569 million distinct keys on average.
- Key encoding: `db_bench`'s big-endian binary integer prefix followed by ASCII
  `0` padding
- Cache: 6 GiB block cache, 128 MiB metadata/index cache, and 16 GiB local
  object-store cache
- SST block size: 8 KiB
- Compression: Zstandard
- Concurrency: 64 clients
- Write buffer and compaction output SST target: 128 MiB, mapped to SlateDB's
  L0 SST threshold and compaction-worker output limit
- Background compaction concurrency: 16 coordinator jobs and 16 embedded-worker
  jobs, with one subcompaction per job to match `benchmark.sh`
- Writes: use `await_durable=true` where `benchmark.sh` uses `sync=1`
- Duration: 90 minutes per test, except `bulk-load`
- Warmup: none

RocksDB's `max_background_jobs=16` is a shared budget for flush and compaction
threads. SlateDB has no shared background-job pool, so the suite applies 16 to
both compaction limits and leaves SlateDB's separate L0 object-store upload
parallelism unchanged. RocksDB's leveled-compaction geometry
(`max_bytes_for_level_base`, level multiplier, and number of levels) has no
SlateDB equivalent because SlateDB uses size-tiered compaction, so those settings
are intentionally not mapped.

Run the tests below in order against the same database.

1. `bulk-load`: With one client, issue 900 million uniformly random puts over
   the 900-million-key ID space, sampling with replacement to match `fillrandom`.
   Disable the WAL and compactor, and do not wait for durability during individual
   writes. Set both L0 SST limits to `u32::MAX` so the uncompacted L0 can hold the
   complete load. Flush all memtables after loading, restore the suite settings,
   and wait for compaction to finish. RocksDB also uses its vector memtable and
   an explicit full compaction; SlateDB has no exact equivalents. Bulk-load
   workloads must have an effective warmup of zero because their
   record-count-driven phase ignores duration. While inserting, report progress,
   recent and average throughput, backpressure, and ETA every 30 seconds.
2. `random-read`: Sample uniformly from the full configured key ID space. After
   bulk load, about 63.2% of IDs are expected to exist; report hits and misses.
3. `multi-random-read`: Read batches of 10 IDs sampled uniformly from the full
   configured key ID space. SlateDB has no native `MultiGet`, so issue 10 `get`
   calls concurrently and report batch latency, total key throughput, hits, and
   misses.
4. `forward-range`: Scan up to 10 records from uniformly random start keys.
5. `reverse-range`: Scan up to 10 records in descending order from uniformly
   random start keys.
6. `overwrite`: Put uniformly random key IDs and wait for durability. IDs absent
   after bulk load become new records.
7. `read-while-writing`: Run 64 random-read clients and one additional writer
   capped at 2 MiB/s. The writer waits for durability.
8. `forward-range-while-writing`: Run 64 forward-scan clients and one additional
   writer capped at 2 MiB/s. Each scan reads up to 10 records; the writer waits
   for durability.
9. `reverse-range-while-writing`: Run 64 reverse-scan clients and one additional
   writer capped at 2 MiB/s. Each scan reads up to 10 records; the writer waits
   for durability.

For the three while-writing workloads, headline return latency contains only
the reader or scanner operations. The capped writer remains visible as
`writer-update` in the per-operation return latency, API latency, throughput,
payload, and durability metrics. The writer keeps at most 1,024 durable puts in
flight, charges each key and value against the 2 MiB/s limit, and publishes both
its target and achieved logical throughput.

RocksDB publishes both buffered-I/O and direct-I/O variants. Direct I/O does not
apply to SlateDB's object-store path, so this suite omits that variant.

## SlateDB-specific suite

Use the YCSB suite settings unless a test specifies otherwise. All workloads
run with 64 clients.

1. `cold-read`: Run uniform random reads after restarting SlateDB and clearing
   its caches. Skip the warmup.
2. `sustained-ingest`: Insert unique random keys with 64 clients for 60 minutes.
   Report 5-minute windows, compaction backlog, and write amplification.
3. `transaction-contention`: Run 10-operation transactions containing 50%
   reads and 50% updates against a 10,000-key hot set. Report commit, abort,
   and conflict rates.
4. `prefix-scan`: Store 10 records under each of 10 million 8-byte prefixes,
   with an 8-byte suffix completing each key. Select prefixes uniformly and
   scan all 10 records with SlateDB's prefix-scan API.
## Out of scope

Do not add configuration variants within a suite. Do not publish comparisons
or deltas between SlateDB versions. Criterion microbenchmarks and regression
alerts remain in the SlateDB repository.
