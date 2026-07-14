# Benchmark suite

Run all profiles for each SlateDB release on a WarpBuild
`warp-ubuntu-latest-x64-16x` runner against Tigris in the `auto` region.

Each profile has one fixed configuration. Do not cross product its workloads
with additional value sizes, cache sizes, machines, object stores, or SlateDB
settings.

## Standard results

Every workload emits the same result record. Use `null` for fields that do not
apply.

- Identity: schema version, SlateDB version and commit, timestamp, profile, and
  workload
- Environment: runner type, CPU model and core count, RAM, local disk, OS and
  kernel, object store, endpoint, and region
- Configuration: client count, duration, record count, key and value sizes,
  cache sizes, SlateDB settings, build profile, and enabled features
- Application performance: total operations, accepted ops/s, payload MiB/s,
  errors, and p50, p95, p99, p99.9, and maximum return latency overall and per
  operation type
- Durability performance: p50, p95, p99, p99.9, and maximum durability lag,
  final flush/drain time, and durable ops/s
- Resources: average and peak CPU, peak RSS, network bytes sent and received,
  and disk bytes and operations read and written
- Storage: database size, object-store request counts by operation, bytes read
  and written, compaction throughput, write amplification, and backpressure time
- Cost: compute, request, storage, and transfer cost, both total and per million
  operations

## YCSB profile

- Dataset: 100 million records with 16-byte keys and 1 KiB values
- Cache: 4 GiB block cache and 512 MiB metadata cache
- Writes: `await_durable=false` with `flush_interval=100ms`
- Concurrency: 1, 16, and 64 clients
- Run: 60-second warmup followed by 5 minutes of measurement

Reset each test to the same preloaded dataset. YCSB A, B, C, and F use a
Zipfian key distribution. Call `flush()` after measurement and include the drain
in the durability results.

1. `ycsb-a`: 50% reads and 50% updates.
2. `ycsb-b`: 95% reads and 5% updates.
3. `ycsb-c`: 100% reads.
4. `ycsb-d`: 95% reads of recently inserted keys and 5% inserts.
5. `ycsb-e`: 95% forward scans and 5% inserts. Scan lengths are uniformly
   distributed from 1 to 100 records.
6. `ycsb-f`: 50% reads and 50% read-modify-write operations.

## RocksDB profile

Follow RocksDB's published
[`benchmark.sh` sequence](https://github.com/facebook/rocksdb/wiki/performance-benchmarks)
and the defaults in
[`tools/benchmark.sh`](https://github.com/facebook/rocksdb/blob/main/tools/benchmark.sh)
where SlateDB has an equivalent setting.

- Dataset: 900 million records with 20-byte keys and 400-byte values
- Cache: 6 GiB block cache; use SlateDB's default metadata cache
- SST block size: 8 KiB
- Compression: Zstandard
- Concurrency: 64 clients
- Writes: use `await_durable=true` where `benchmark.sh` uses `sync=1`
- Duration: 90 minutes per test, except `bulk-load`
- Warmup: none

Run the tests below in order against the same database.

1. `bulk-load`: With one client, insert all 900 million records in random key
   order. Disable the WAL and compactor, and do not wait for durability during
   individual writes. Flush all memtables after loading, enable compaction, and
   wait for compaction to finish. RocksDB also uses its vector memtable and an
   explicit full compaction; SlateDB has no exact equivalents.
2. `random-read`: Read uniformly random existing keys.
3. `multi-random-read`: Read batches of 10 uniformly random keys. SlateDB has no
   native `MultiGet`, so issue 10 `get` calls concurrently and report batch
   latency and total key throughput.
4. `forward-range`: Scan up to 10 records from uniformly random start keys.
5. `reverse-range`: Scan up to 10 records in descending order from uniformly
   random start keys.
6. `overwrite`: Update uniformly random existing keys and wait for durability.
7. `read-while-writing`: Run 64 random-read clients and one additional writer
   capped at 2 MiB/s. The writer waits for durability.
8. `forward-range-while-writing`: Run 64 forward-scan clients and one additional
   writer capped at 2 MiB/s. Each scan reads up to 10 records; the writer waits
   for durability.
9. `reverse-range-while-writing`: Run 64 reverse-scan clients and one additional
   writer capped at 2 MiB/s. Each scan reads up to 10 records; the writer waits
   for durability.

RocksDB publishes both buffered-I/O and direct-I/O variants. Direct I/O does not
apply to SlateDB's object-store path, so this profile omits that variant.

## SlateDB-specific profile

Use the YCSB profile settings unless a test specifies otherwise.

1. `cold-read`: Run uniform random reads after restarting SlateDB and clearing
   its caches. Skip the warmup.
2. `sustained-ingest`: Insert unique random keys with 64 clients for 60 minutes.
   Report 5-minute windows, compaction backlog, and write amplification.
3. `transaction-contention`: Run 10-operation transactions containing 50%
   reads and 50% updates against a 10,000-key hot set. Run with 16 and 64
   clients and report commit, abort, and conflict rates.
4. `prefix-scan`: Store 10 records under each of 10 million 8-byte prefixes,
   with an 8-byte suffix completing each key. Select prefixes uniformly and
   scan all 10 records with SlateDB's prefix-scan API.
5. `open-loop-read`: Run YCSB C's 100% read mix with fixed-rate arrivals
   scheduled independently of completions.
6. `open-loop-read-update`: Run YCSB A's 50% read and 50% update mix with
   fixed-rate arrivals scheduled independently of completions.

For each open-loop test, reset to the preloaded dataset and run at fixed target
rates of 1,000, 5,000, and 10,000 ops/s.

## Out of scope

Do not add configuration variants within a profile. Do not publish comparisons
or deltas between SlateDB versions. Criterion microbenchmarks and regression
alerts remain in the SlateDB repository.
