import { mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseToml } from 'smol-toml';

const repoRoot = fileURLToPath(new URL('../..', import.meta.url));
const outputRoot = process.env.BENCHMARK_MOCK_RESULTS_ROOT
  ? path.resolve(process.cwd(), process.env.BENCHMARK_MOCK_RESULTS_ROOT)
  : path.join(repoRoot, '.mock-results');
const published = await loadReleaseConfiguration(path.join(repoRoot, 'config'));

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
  const target = variant.target_rate ?? null;
  const clients = variant.clients ?? null;
  const scale = target !== null ? (target >= 10_000 ? 'large' : target >= 5_000 ? 'medium' : 'small')
    : clients >= 64 ? 'large' : clients >= 16 ? 'medium' : 'small';
  return { target, clients, scale };
}

function mockThroughput(suite, workload, clients, target) {
  if (target !== null) return target * 0.96;
  const base = suite === 'rocksdb' ? 32_000 : workload === 'prefix-scan' ? 2_400 : 48_000;
  return base * Math.sqrt(clients ?? 1);
}

function sample(offset_ns, operations, throughput, databaseSize, networkBytesSent, networkBytesReceived) {
  const objectStoreRequests = {
    put: Math.round(operations / 100),
    get: Math.round(operations / 20),
    list: 4,
  };
  return {
    offset_ns,
    operations,
    errors: 0,
    cpu_percent: Math.min(1_600, 18 + throughput / 900),
    rss_bytes: 2_684_354_560,
    network_bytes_sent: networkBytesSent,
    network_bytes_received: networkBytesReceived,
    disk_bytes_read: Math.round(operations * 32),
    disk_bytes_written: Math.round(operations * 48),
    disk_read_operations: Math.round(operations / 10),
    disk_write_operations: Math.round(operations / 8),
    database_size_bytes: databaseSize,
    object_store_operations: objectStoreRequests,
    object_store_requests: objectStoreRequests,
    object_store_successful_requests: objectStoreRequests,
    object_store_request_errors: {},
    object_store_client_errors: {},
    object_store_server_errors: {},
    object_store_transport_errors: {},
    object_store_bytes_read: Math.round(operations * 640),
    object_store_bytes_written: Math.round(operations * 1024),
    object_store_operation_bytes_read: Math.round(operations * 576),
    object_store_operation_bytes_written: Math.round(operations * 1024),
  };
}

function payloadPerOperation(workload, keyBytes, valueBytes, throughput) {
  const scan = 10 * (keyBytes + valueBytes);
  const cappedWriter = 2 * 1024 * 1024 / throughput;
  switch (workload) {
    case 'ycsb-a': return { read: 0.5 * valueBytes, write: 0.5 * valueBytes };
    case 'ycsb-b':
    case 'ycsb-d': return { read: 0.95 * valueBytes, write: 0.05 * valueBytes };
    case 'ycsb-e': return { read: 0.95 * 50.5 * (keyBytes + valueBytes), write: 0.05 * valueBytes };
    case 'ycsb-f': return { read: valueBytes, write: 0.5 * valueBytes };
    case 'open-loop-read-update': return { read: 0.5 * valueBytes, write: 0.5 * valueBytes };
    case 'transaction-contention': return { read: 5 * valueBytes, write: 5 * valueBytes };
    case 'multi-random-read': return { read: 10 * valueBytes, write: 0 };
    case 'forward-range':
    case 'reverse-range':
    case 'prefix-scan': return { read: scan, write: 0 };
    case 'read-while-writing': return { read: valueBytes, write: cappedWriter };
    case 'forward-range-while-writing':
    case 'reverse-range-while-writing': return { read: scan, write: cappedWriter };
    case 'bulk-load':
    case 'overwrite':
    case 'sustained-ingest': return { read: 0, write: valueBytes };
    default: return { read: valueBytes, write: 0 };
  }
}

function apiCallsForWorkload(workload) {
  switch (workload) {
    case 'ycsb-a': return { get: 0.5, put: 0.5 };
    case 'ycsb-b':
    case 'ycsb-d': return { get: 0.95, put: 0.05 };
    case 'ycsb-e': return { scan: 0.95, put: 0.05 };
    case 'ycsb-f': return { get: 1, put: 0.5 };
    case 'open-loop-read-update': return { get: 0.5, put: 0.5 };
    case 'multi-random-read': return { get: 10 };
    case 'forward-range':
    case 'reverse-range':
    case 'prefix-scan': return { scan: 1 };
    case 'read-while-writing': return { get: 1, put: 0.1 };
    case 'forward-range-while-writing':
    case 'reverse-range-while-writing': return { scan: 1, put: 0.1 };
    case 'bulk-load':
    case 'overwrite':
    case 'sustained-ingest': return { put: 1 };
    case 'transaction-contention': return {
      'transaction.begin': 1,
      'transaction.get': 5,
      'transaction.put': 5,
      'transaction.commit': 1,
    };
    default: return { get: 1 };
  }
}

function apiLatencyFactor(api) {
  return {
    get: 1,
    put: 1.4,
    scan: 8,
    'transaction.begin': 0.8,
    'transaction.get': 0.7,
    'transaction.put': 0.1,
    'transaction.commit': 2.2,
    flush: 20,
  }[api] ?? 1;
}

function latencySummary(summary, count, factor = 1) {
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

function machineTraffic(readBytes, writeBytes) {
  return {
    upload: Math.round(writeBytes * 1.25 + readBytes * 0.025),
    download: Math.round(readBytes * 0.72 + writeBytes * 0.015),
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

function mockCompactorMetrics(samples, throughput, payload, isWrite) {
  let readRate = 0;
  let writtenBytes = 0;
  const readValues = [];
  const writeValues = [];
  for (let index = 0; index < samples.length; index += 1) {
    if (isWrite && index > 0) {
      const wave = 0.88 + Math.sin(index / 6) * 0.1;
      if (index % 10 === 0) {
        readRate = Math.round(throughput * payload.write * 0.52 * wave);
      }
      writtenBytes += Math.round(throughput * payload.write * 0.39 * wave);
    }
    readValues.push(readRate);
    writeValues.push(writtenBytes);
  }
  return [
    {
      name: 'slatedb.compactor.total_throughput_bytes_per_sec',
      description: 'Aggregate compaction input throughput',
      labels: {},
      value_type: 'gauge',
      boundaries: null,
      values: readValues,
    },
    {
      name: 'slatedb.compactor.bytes_compacted',
      description: 'Bytes written to compaction output SSTs',
      labels: { worker_id: 'mock-worker' },
      value_type: 'counter',
      boundaries: null,
      values: writeValues,
    },
  ];
}

function mockTimeseries({ throughput, payload, databaseSize, isWrite, awaitDurable, openLoop, latency, apiCalls }) {
  const windowCount = 60;
  let cumulativeOperations = 0;
  let networkBytesSent = 0;
  let networkBytesReceived = 0;
  const samples = [sample(0, 0, throughput, databaseSize, 0, 0)];
  const applicationWindows = [];
  const durabilityWindows = isWrite && !awaitDurable ? [] : null;
  for (let index = 0; index < windowCount; index += 1) {
    const traffic = 0.94 + Math.sin(index / 5) * 0.045 - (index === 34 || index === 35 ? 0.18 : 0);
    const successful = Math.max(1, Math.round(throughput * traffic));
    const offered = openLoop ? Math.round(throughput / 0.96) : null;
    const dropped = openLoop ? Math.max(0, offered - successful) : null;
    const returnLatency = windowLatency(latency.summary, successful, index);
    const apiLatency = Object.fromEntries(Object.entries(apiCalls).map(([api, multiplier]) => [
      api,
      windowLatency(
        latency.summary,
        Math.max(1, Math.round(successful * multiplier)),
        index,
        apiLatencyFactor(api),
      ),
    ]));
    if (isWrite && index === windowCount - 1) {
      apiLatency.flush = windowLatency(latency.summary, 1, index, apiLatencyFactor('flush'));
    }
    const readPayloadBytes = Math.round(successful * payload.read);
    const writePayloadBytes = Math.round(successful * payload.write);
    const network = machineTraffic(readPayloadBytes, writePayloadBytes);
    cumulativeOperations += successful;
    networkBytesSent += network.upload;
    networkBytesReceived += network.download;
    applicationWindows.push({
      start_offset_ns: index * 1_000_000_000,
      duration_ns: 1_000_000_000,
      completed_operations: successful,
      successful_operations: successful,
      errors: 0,
      read_payload_bytes: readPayloadBytes,
      write_payload_bytes: writePayloadBytes,
      payload_bytes: readPayloadBytes + writePayloadBytes,
      offered_operations: offered,
      dropped_operations: dropped,
      return_latency: returnLatency,
      return_latency_by_operation: { operation: returnLatency },
      api_latency: apiLatency,
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
      databaseSize + (isWrite ? Math.round(cumulativeOperations * payload.write) : 0),
      networkBytesSent,
      networkBytesReceived,
    ));
  }
  return { samples, application_windows: applicationWindows, durability_windows: durabilityWindows };
}

await rm(outputRoot, { recursive: true, force: true });
const resultPaths = [];

for (const suite of published.suites) {
  for (const workload of suite.workloads) {
    for (const variantDefinition of workload.variants) {
      const variant = variantDefinition.name;
      const { clients, target, scale } = variantSettings(variantDefinition);
      const latency = latencyTemplates[scale];
      const operations = latency.summary.count;
      const throughput = mockThroughput(suite.name, workload.name, clients, target);
      const isWrite = writeWorkloads.has(workload.name);
      const awaitDurable = Boolean(workload.await_durable);
      const measurementMs = workload.measurement_ms ?? suite.measurement_ms;
      const measurementNs = measurementMs * 1_000_000;
      const elapsedNs = measurementNs || Math.max(1, Math.round(operations / throughput * 1_000_000_000));
      const recordCount = workload.record_count ?? suite.record_count;
      const keyBytes = workload.key_bytes ?? suite.key_bytes;
      const valueBytes = workload.value_bytes ?? suite.value_bytes;
      const payload = payloadPerOperation(workload.name, keyBytes, valueBytes, throughput);
      const apiCalls = apiCallsForWorkload(workload.name);
      const aggregateMachineTraffic = machineTraffic(
        operations * payload.read,
        operations * payload.write,
      );
      const databaseSize = Math.max(16 * 1024 * 1024, Math.round(recordCount * (keyBytes + valueBytes) * 0.42));
      const openLoop = target !== null;
      const transaction = workload.name === 'transaction-contention';
      const durabilityLag = isWrite && !awaitDurable ? latency.summary : null;

      const result = {
        identity: {
          slate_version: version,
          slate_commit: zeroCommit,
          runner_version: '0.1.0-mock',
          runner_commit: zeroCommit,
          lockfile_sha256: zeroHash,
          timestamp,
          suite: suite.name,
          workload: workload.name,
          variant,
          mode: 'smoke',
        },
        elapsed_ns: elapsedNs,
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
          endpoint: 'https://t3.storage.dev',
          region: 'fra',
        },
        object_store_baseline: objectStoreBaseline,
        configuration: {
          clients,
          target_rate: target,
          warmup_ns: (workload.warmup_ms ?? suite.warmup_ms) * 1_000_000,
          measurement_ns: measurementNs,
          record_count: recordCount,
          key_bytes: keyBytes,
          value_bytes: valueBytes,
          block_cache_bytes: suite.block_cache_bytes,
          metadata_cache_bytes: suite.metadata_cache_bytes,
          object_store_cache_bytes: suite.object_store_cache_bytes ?? null,
          sst_block_bytes: suite.sst_block_bytes ?? null,
          slate_settings: {
            flush_interval: '100ms',
            wal_enabled: workload.kind !== 'bulk-load',
            manifest_poll_interval: '20ms',
            manifest_update_timeout: '300s',
            min_filter_keys: 1000,
            l0_sst_size_bytes: 65536,
            max_wal_flushes_before_l0_flush: 4096,
            l0_max_ssts: workload.kind === 'bulk-load' ? 4_294_967_295 : 8,
            l0_max_ssts_per_key: workload.kind === 'bulk-load' ? 4_294_967_295 : 8,
            l0_flush_parallelism: 4,
            max_unflushed_bytes: 8388608,
            compactor_options: workload.kind === 'bulk-load' ? null : {},
            compression_codec: suite.name === 'rocksdb' ? 'Zstd' : null,
            object_store_cache_options: {
              root_folder: null,
              max_cache_size_bytes: suite.object_store_cache_bytes ?? null,
            },
            garbage_collector_options: {},
            metric_level: 'Info',
            default_ttl: null,
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
          payload_mib_per_second: throughput * (payload.read + payload.write) / 1_048_576,
          errors: 0,
          return_latency: latency.summary,
          return_latency_by_operation: { operation: latency.summary },
          api_latency: {
            ...Object.fromEntries(Object.entries(apiCalls).map(([api, multiplier]) => [
              api,
              latencySummary(
                latency.summary,
                Math.max(1, Math.round(operations * multiplier)),
                apiLatencyFactor(api),
              ),
            ])),
            ...(isWrite ? { flush: latencySummary(latency.summary, 1, apiLatencyFactor('flush')) } : {}),
          },
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
          network_bytes_sent: aggregateMachineTraffic.upload,
          network_bytes_received: aggregateMachineTraffic.download,
          disk_bytes_read: operations * 32,
          disk_bytes_written: operations * 48,
          disk_read_operations: Math.round(operations / 10),
          disk_write_operations: Math.round(operations / 8),
        },
        storage: {
          database_size_bytes: databaseSize,
          average_database_size_bytes: Math.round(databaseSize * (isWrite ? 1.05 : 1)),
          object_store_operations: {
            put: Math.round(operations / 100),
            get: Math.round(operations / 20),
            head: 2,
            list: 4,
            delete: 0,
            copy: 0,
            create_multipart: 0,
            complete_multipart: 0,
            abort_multipart: 0,
          },
          object_store_requests: {
            put: Math.round(operations / 100),
            get: Math.round(operations / 20),
            head: 2,
            list: 4,
            delete: 0,
            copy: 0,
            create_multipart: 0,
            complete_multipart: 0,
            abort_multipart: 0,
          },
          object_store_successful_requests: {
            put: Math.round(operations / 100),
            get: Math.round(operations / 20),
            head: 2,
            list: 4,
            delete: 0,
            copy: 0,
            create_multipart: 0,
            complete_multipart: 0,
            abort_multipart: 0,
          },
          object_store_request_errors: {},
          object_store_client_errors: {},
          object_store_server_errors: {},
          object_store_transport_errors: {},
          object_store_errors: 0,
          bytes_read: Math.round(operations * valueBytes * 0.64),
          bytes_written: isWrite ? operations * valueBytes : 0,
          object_store_operation_bytes_read: Math.round(operations * valueBytes * 0.58),
          object_store_operation_bytes_written: isWrite ? operations * valueBytes : 0,
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
        initial_state: {
          checkpoint_id: suite.name === 'rocksdb' ? null : '00000000-0000-4000-8000-000000000000',
          manifest_id: 1,
          lsm_digest_sha256: zeroHash,
        },
        source_files: { histograms: 'histograms.json', timeseries: 'timeseries.json' },
      };

      const histograms = {
        encoding: 'hdrhistogram-v2-deflate-base64',
        significant_digits: 3,
        histograms: {
          return: latency.histogram,
          'return/operation': latency.histogram,
          ...Object.fromEntries(Object.entries(apiCalls).map(([api, multiplier]) => [
            `api/${api}`,
            { ...latency.histogram, count: Math.max(1, Math.round(operations * multiplier)) },
          ])),
          ...(isWrite ? { 'api/flush': { ...latency.histogram, count: 1 } } : {}),
          ...objectStoreHistograms,
          ...(openLoop ? { response: latency.histogram, scheduling_delay: latency.histogram } : {}),
          ...(durabilityLag ? { durability_lag: latency.histogram } : {}),
        },
      };
      const windowed = mockTimeseries({
        throughput, payload, databaseSize, isWrite, awaitDurable, openLoop, latency, apiCalls,
      });
      const timeseries = {
        interval_ns: 1_000_000_000,
        ...windowed,
        slatedb_metrics: mockCompactorMetrics(windowed.samples, throughput, payload, isWrite),
      };

      const relativeDirectory = path.join('results', version, suite.name, workload.name, variant);
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
  object_store_baselines: Object.fromEntries(
    published.suites.map((suite) => [suite.name, objectStoreBaseline]),
  ),
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

async function loadReleaseConfiguration(configRoot) {
  const entries = await readdir(configRoot, { withFileTypes: true });
  const suites = [];
  const suiteSuffix = '.suite.toml';
  const suiteEntries = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(suiteSuffix))
    .sort((a, b) => a.name.localeCompare(b.name));
  for (const entry of suiteEntries) {
    const suiteName = entry.name.slice(0, -suiteSuffix.length);
    const suitePath = path.join(configRoot, entry.name);
    const suite = parseToml(await readFile(suitePath, 'utf8'));
    if (!suite.release) continue;

    const workloads = suite.workloads.map((workload) => ({
      ...workload,
      warmup_ms: workload.warmup === undefined ? undefined : durationMs(workload.warmup),
      measurement_ms: workload.measurement === undefined ? undefined : durationMs(workload.measurement),
    }));
    suites.push({
      ...suite,
      name: suiteName,
      warmup_ms: durationMs(suite.warmup),
      measurement_ms: durationMs(suite.measurement),
      compaction_quiet_ms: durationMs(suite.compaction_quiet),
      compaction_timeout_ms: durationMs(suite.compaction_timeout),
      workloads,
    });
  }
  return { suites };
}

function durationMs(value) {
  const match = /^(\d+)(ms|s|m|h)$/.exec(value);
  if (!match) throw new Error(`unsupported duration ${value}`);
  const multiplier = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000 }[match[2]];
  return Number(match[1]) * multiplier;
}
