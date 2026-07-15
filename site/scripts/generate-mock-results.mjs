import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
const outputRoot = process.env.BENCHMARK_MOCK_RESULTS_ROOT
  ? path.resolve(process.cwd(), process.env.BENCHMARK_MOCK_RESULTS_ROOT)
  : path.join(repoRoot, '.mock-results');
const published = JSON.parse(await readFile(path.join(repoRoot, 'config/published.json'), 'utf8'));

const version = '0.14.1-mock';
const timestamp = new Date().toISOString();
const zeroHash = '0'.repeat(64);
const zeroCommit = '0'.repeat(40);

const objectStoreBaseline = {
  measured_at: timestamp,
  put_latency: { count: 8, p50_ns: 1_782_000, p95_ns: 13_503_000, p99_ns: 13_503_000, p999_ns: 13_503_000, max_ns: 13_503_000 },
  get_latency: { count: 8, p50_ns: 796_000, p95_ns: 1_983_000, p99_ns: 1_983_000, p999_ns: 1_983_000, max_ns: 1_983_000 },
  upload_mib_per_second: 184.97,
  download_mib_per_second: 276.76,
};

const objectStoreHistograms = {
  'object_store/put': {
    unit: 'microseconds', count: 8, min: 1597, max: 13503,
    data: 'HISTFAAAAEl4nAE+AMH/HISTEwAAABYAAAAAAAAAAwAAAAAAAAABAAAAFB3XYAA/8AAAAAAAAPkYAn0CJQLHAQKLAgKZAQLNBALFJgLmnwlm',
  },
  'object_store/get': {
    unit: 'microseconds', count: 8, min: 754, max: 1983,
    data: 'HISTFAAAAEB4nC2I0QkAEBgGf58neVE8kkxiFcPYwCSmMYAFLKD44+q6Ol+bJSKWkb/i6sIolNcbU0MjIUJBogtsgwO09wcJ',
  },
};

const latencyTemplates = {
  small: {
    summary: { count: 1024, p50_ns: 22_000, p95_ns: 40_000, p99_ns: 51_000, p999_ns: 77_000, max_ns: 272_000 },
    histogram: {
      unit: 'microseconds', count: 1024, min: 16, max: 272,
      data: 'HISTFAAAAGh4nC2IMQ5AQBRE/59dohCRrCiEaGSj2kKhUbiEA6j2IrbUuYVzuINrqNTEmsnLS17pNkVEA/mJ3/ySVedM4+VDDaSruHnngx1bO5leG9M2utNFEudpEkVShiEgZQAI0HeFReABhcAL5A==',
    },
  },
  medium: {
    summary: { count: 75, p50_ns: 9_535_000, p95_ns: 15_039_000, p99_ns: 16_431_000, p999_ns: 16_431_000, max_ns: 16_431_000 },
    histogram: {
      unit: 'microseconds', count: 75, min: 6020, max: 16431,
      data: 'HISTFAAAAJd4nC2NLQ7CQBCFp9+WbDaIDSmIBhpCKhAogmhIRVWPgAaHQhCugMPBiRCcgWvgMTwoM5nf997M5HzLzOxqnbl/TRTD4rmz5tUt7hUn1owwONCjZcCWiiORMQUNTntHYMZeOfyoEZeWTPFS1FL0mauL8pIFSzFyLolgL1INOpDLg2BT79NMou+8Edqy0qs3qRSPhA+F4BDz',
    },
  },
  large: {
    summary: { count: 142, p50_ns: 20_895_000, p95_ns: 29_023_000, p99_ns: 39_007_000, p999_ns: 39_359_000, max_ns: 39_359_000 },
    histogram: {
      unit: 'microseconds', count: 142, min: 13968, max: 39359,
      data: 'HISTFAAAALt4nDWOMQrCUAyG078p5VGKSC3yQBERh05OIk49gAdw9RJOrm6OPYJ4Jq/h5uqfPPseIclH/j9Z3IdGRL6SXv7PGWO2fF+k/yTwPCHihiHDK8MaNbsJKjRAQI4CQbUEYUDpRJgb5hWriuMOWoY4JmkxBwjMo6XYNGEkAYXS1ZgJtfDCFpjTnvKctuJBPbuOhmJFpK9oxNYPtIUbjtTufsCUP51/BAfM106sVTvO7siv6PXMFQ+l7Ad+vg/Y',
    },
  },
};

const writeWorkloads = new Set([
  'ycsb-a', 'ycsb-b', 'ycsb-d', 'ycsb-e', 'ycsb-f',
  'bulk-load', 'overwrite', 'read-while-writing', 'forward-range-while-writing',
  'reverse-range-while-writing', 'sustained-ingest', 'transaction-contention',
  'open-loop-read-update',
]);

function variantSettings(variant) {
  const target = variant.startsWith('rate-') ? Number(variant.slice(5)) : null;
  const clientsMatch = variant.match(/(?:clients|readers)-(\d+)/);
  const clients = target === null ? Number(clientsMatch?.[1] ?? 1) : null;
  const scale = target !== null ? (target >= 10_000 ? 'large' : target >= 5_000 ? 'medium' : 'small')
    : clients >= 64 ? 'large' : clients >= 16 ? 'medium' : 'small';
  return { target, clients, scale };
}

function mockThroughput(profile, workload, clients, target) {
  if (target !== null) return target * 0.96;
  const base = profile === 'rocksdb' ? 32_000 : workload === 'prefix-scan' ? 2_400 : 48_000;
  return base * Math.sqrt(clients ?? 1);
}

function sample(offset_ns, operations, throughput, databaseSize) {
  return {
    offset_ns,
    operations,
    errors: 0,
    cpu_percent: Math.min(1_600, 18 + throughput / 900),
    rss_bytes: 2_684_354_560,
    network_bytes_sent: Math.round(operations * 1024),
    network_bytes_received: Math.round(operations * 640),
    disk_bytes_read: Math.round(operations * 32),
    disk_bytes_written: Math.round(operations * 48),
    disk_read_operations: Math.round(operations / 10),
    disk_write_operations: Math.round(operations / 8),
    database_size_bytes: databaseSize,
    object_store_requests: { put: Math.round(operations / 100), get: Math.round(operations / 20), list: 4 },
    object_store_bytes_read: Math.round(operations * 640),
    object_store_bytes_written: Math.round(operations * 1024),
  };
}

function windowLatency(summary, count, index, extra = 1) {
  const wave = 0.9 + Math.sin(index / 4) * 0.08;
  const stall = index === 34 || index === 35 ? 2.4 : 1;
  const factor = wave * stall * extra;
  const scale = (value) => Math.max(1, Math.round(value * factor));
  return {
    count,
    p50_ns: scale(summary.p50_ns),
    p95_ns: scale(summary.p95_ns),
    p99_ns: scale(summary.p99_ns),
    p999_ns: scale(summary.p999_ns),
    max_ns: scale(summary.max_ns),
  };
}

function mockTimeseries({ throughput, valueBytes, databaseSize, isWrite, awaitDurable, openLoop, latency }) {
  const windowCount = 60;
  let cumulativeOperations = 0;
  const samples = [sample(0, 0, throughput, databaseSize)];
  const applicationWindows = [];
  const durabilityWindows = isWrite && !awaitDurable ? [] : null;
  for (let index = 0; index < windowCount; index += 1) {
    const traffic = 0.94 + Math.sin(index / 5) * 0.045 - (index === 34 || index === 35 ? 0.18 : 0);
    const successful = Math.max(1, Math.round(throughput * traffic));
    const offered = openLoop ? Math.round(throughput / 0.96) : null;
    const dropped = openLoop ? Math.max(0, offered - successful) : null;
    const returnLatency = windowLatency(latency.summary, successful, index);
    cumulativeOperations += successful;
    applicationWindows.push({
      start_offset_ns: index * 1_000_000_000,
      duration_ns: 1_000_000_000,
      completed_operations: successful,
      successful_operations: successful,
      errors: 0,
      payload_bytes: successful * valueBytes,
      offered_operations: offered,
      dropped_operations: dropped,
      return_latency: returnLatency,
      return_latency_by_operation: { operation: returnLatency },
      response_latency: openLoop ? windowLatency(latency.summary, successful, index, 1.08) : null,
      scheduling_delay: openLoop
        ? windowLatency({ p50_ns: 40_000, p95_ns: 90_000, p99_ns: 180_000, p999_ns: 400_000, max_ns: 700_000 }, successful, index)
        : null,
      batch_latency: null,
    });
    if (durabilityWindows) {
      durabilityWindows.push({
        start_offset_ns: index * 1_000_000_000,
        duration_ns: 1_000_000_000,
        writes_made_durable: successful,
        durability_lag: windowLatency(latency.summary, successful, index, 5.5),
      });
    }
    samples.push(sample(
      (index + 1) * 1_000_000_000,
      cumulativeOperations,
      successful,
      databaseSize + (isWrite ? cumulativeOperations * valueBytes : 0),
    ));
  }
  return { samples, application_windows: applicationWindows, durability_windows: durabilityWindows };
}

await rm(outputRoot, { recursive: true, force: true });
const resultPaths = [];

for (const profile of published.profiles) {
  for (const workload of profile.workloads) {
    for (const variant of workload.variants) {
      const { clients, target, scale } = variantSettings(variant);
      const latency = latencyTemplates[scale];
      const operations = latency.summary.count;
      const throughput = mockThroughput(profile.name, workload.name, clients, target);
      const isWrite = writeWorkloads.has(workload.name);
      const awaitDurable = Boolean(workload.await_durable);
      const measurementMs = workload.measurement_ms ?? profile.measurement_ms;
      const measurementNs = measurementMs * 1_000_000;
      const recordCount = workload.record_count ?? profile.record_count;
      const keyBytes = workload.key_bytes ?? profile.key_bytes;
      const valueBytes = workload.value_bytes ?? profile.value_bytes;
      const databaseSize = Math.max(16 * 1024 * 1024, Math.round(recordCount * (keyBytes + valueBytes) * 0.42));
      const compute = Math.max(0.00001, measurementMs / 60_000 * 0.032);
      const requests = operations * 0.0000006;
      const storage = databaseSize / 1_073_741_824 * (measurementMs / 2_592_000_000) * 0.02;
      const totalCost = compute + requests + storage;
      const perMillion = 1_000_000 / operations;
      const openLoop = target !== null;
      const transaction = workload.name === 'transaction-contention';
      const durabilityLag = isWrite && !awaitDurable ? latency.summary : null;

      const result = {
        schema_version: 1,
        identity: {
          slate_version: version,
          slate_commit: zeroCommit,
          runner_version: '0.1.0-mock',
          runner_commit: zeroCommit,
          lockfile_sha256: zeroHash,
          timestamp,
          profile: profile.name,
          workload: workload.name,
          variant,
          mode: 'smoke',
        },
        environment: {
          runner_type: 'mock-warp-x64-16x',
          hostname: 'mock-runner',
          cpu_model: 'Mock 16-core CPU',
          cpu_cores: 16,
          ram_bytes: 68_719_476_736,
          local_disk: 'mock-nvme:/',
          os: 'Ubuntu 24.04 (mock)',
          kernel: '6.8.0-mock',
          object_store: 'Tigris (mock)',
          endpoint: 'https://fly.storage.tigris.dev',
          region: 'fra',
        },
        object_store_baseline: objectStoreBaseline,
        configuration: {
          clients,
          target_rate: target,
          warmup_ns: (workload.warmup_ms ?? profile.warmup_ms) * 1_000_000,
          measurement_ns: measurementNs,
          record_count: recordCount,
          key_bytes: keyBytes,
          value_bytes: valueBytes,
          block_cache_bytes: profile.block_cache_bytes,
          metadata_cache_bytes: profile.metadata_cache_bytes,
          sst_block_bytes: profile.sst_block_bytes ?? null,
          slate_settings: {
            flush_interval_ms: profile.flush_interval_ms,
            await_durable: awaitDurable,
            compression: profile.compression,
          },
          build_profile: 'release',
          enabled_features: ['aws', 'foyer', 'wal_disable', 'zstd'],
        },
        application: {
          total_operations: operations,
          successful_operations: operations,
          accepted_ops_per_second: throughput,
          completed_ops_per_second: throughput,
          offered_ops_per_second: openLoop ? target : null,
          dropped_operations: openLoop ? 0 : null,
          dropped_ops_per_second: openLoop ? 0 : null,
          payload_mib_per_second: throughput * valueBytes / 1_048_576,
          errors: 0,
          return_latency: latency.summary,
          return_latency_by_operation: { operation: latency.summary },
          response_latency: openLoop ? latency.summary : null,
          scheduling_delay: openLoop ? latency.summary : null,
          batch_latency: null,
          key_throughput_per_second: workload.name === 'multi-random-read' ? throughput * 10 : null,
          transaction_commits: transaction ? operations : null,
          transaction_aborts: transaction ? 0 : null,
          transaction_conflicts: transaction ? 0 : null,
          transaction_commit_rate: transaction ? 1 : null,
          transaction_abort_rate: transaction ? 0 : null,
          transaction_conflict_rate: transaction ? 0 : null,
        },
        durability: {
          lag: durabilityLag,
          final_flush_drain_ns: isWrite ? 8_000_000 : null,
          durable_ops_per_second: isWrite ? throughput * 0.98 : null,
          last_measured_sequence: isWrite ? operations : null,
          final_durable_sequence: isWrite ? operations : null,
        },
        resources: {
          average_cpu_percent: Math.min(1_600, 12 + throughput / 1_100),
          peak_cpu_percent: Math.min(1_600, 28 + throughput / 850),
          peak_rss_bytes: 2_684_354_560 + (clients ?? 1) * 8_388_608,
          network_bytes_sent: operations * valueBytes,
          network_bytes_received: Math.round(operations * valueBytes * 0.64),
          disk_bytes_read: operations * 32,
          disk_bytes_written: operations * 48,
          disk_read_operations: Math.round(operations / 10),
          disk_write_operations: Math.round(operations / 8),
        },
        storage: {
          database_size_bytes: databaseSize,
          object_store_requests: { put: Math.round(operations / 100), get: Math.round(operations / 20), head: 2, list: 4, delete: 0 },
          object_store_errors: 0,
          bytes_read: Math.round(operations * valueBytes * 0.64),
          bytes_written: isWrite ? operations * valueBytes : 0,
          compaction_throughput_bytes_per_second: isWrite ? throughput * valueBytes * 0.31 : null,
          write_amplification: isWrite ? 1.18 : null,
          backpressure_ns: 0,
          compaction_backlog_bytes: isWrite ? Math.round(databaseSize * 0.03) : 0,
          five_minute_windows: workload.name === 'sustained-ingest'
            ? Array.from({ length: 12 }, (_, index) => ({
              start_offset_ns: index * 300_000_000_000,
              operations: Math.round(throughput * 300),
              ops_per_second: throughput * (1 - index * 0.005),
              compaction_backlog_bytes: Math.round(databaseSize * index / 120),
              write_amplification: 1.12 + index * 0.01,
            }))
            : [],
        },
        cost: {
          price_table_revision: 'tigris-standard-2026-07-14',
          currency: 'USD',
          compute,
          requests,
          storage,
          transfer: 0,
          total: totalCost,
          compute_per_million_operations: compute * perMillion,
          requests_per_million_operations: requests * perMillion,
          storage_per_million_operations: storage * perMillion,
          transfer_per_million_operations: 0,
          total_per_million_operations: totalCost * perMillion,
        },
        initial_state: {
          checkpoint_id: profile.name === 'rocksdb' ? null : '00000000-0000-4000-8000-000000000000',
          manifest_id: 1,
          lsm_digest_sha256: zeroHash,
        },
        source_files: { histograms: 'histograms.json', timeseries: 'timeseries.json' },
      };

      const histograms = {
        schema_version: 1,
        encoding: 'hdrhistogram-v2-deflate-base64',
        significant_digits: 3,
        histograms: {
          return: latency.histogram,
          'return/operation': latency.histogram,
          ...objectStoreHistograms,
          ...(openLoop ? { response: latency.histogram, scheduling_delay: latency.histogram } : {}),
          ...(durabilityLag ? { durability_lag: latency.histogram } : {}),
        },
      };
      const windowed = mockTimeseries({
        throughput, valueBytes, databaseSize, isWrite, awaitDurable, openLoop, latency,
      });
      const timeseries = {
        schema_version: 1,
        interval_ns: 1_000_000_000,
        ...windowed,
        slatedb_metrics: [],
      };

      const relativeDirectory = path.join('results', version, profile.name, workload.name, variant);
      const directory = path.join(outputRoot, relativeDirectory);
      await mkdir(directory, { recursive: true });
      await Promise.all([
        writeJson(path.join(directory, 'result.json'), result),
        writeJson(path.join(directory, 'histograms.json'), histograms),
        writeJson(path.join(directory, 'timeseries.json'), timeseries),
      ]);
      resultPaths.push(path.join(relativeDirectory, 'result.json').split(path.sep).join('/'));
    }
  }
}

const run = {
  schema_version: 1,
  status: 'ok',
  started_at: timestamp,
  finished_at: timestamp,
  mode: 'smoke',
  slate_version: version,
  slate_commit: zeroCommit,
  runner_version: '0.1.0-mock',
  runner_commit: zeroCommit,
  lockfile_sha256: zeroHash,
  resolved_configuration: published,
  object_store_baseline: objectStoreBaseline,
  results: resultPaths,
};
await Promise.all([
  writeJson(path.join(outputRoot, 'run.json'), run),
  writeJson(path.join(outputRoot, 'object-store.json'), objectStoreBaseline),
]);

console.log(`Generated ${resultPaths.length} mock benchmark variants in ${outputRoot}`);

async function writeJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, `${JSON.stringify(value, null, 2)}\n`);
}
