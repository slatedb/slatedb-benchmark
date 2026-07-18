import { promises as fs } from 'node:fs';
import type { Dirent } from 'node:fs';
import path from 'node:path';

export type SourceIdentity = {
  slate_version: string;
  slate_commit: string;
  runner_version: string;
  runner_commit: string;
  lockfile_sha256: string;
};

export type Environment = {
  runner_type: string;
  hostname: string;
  cpu_model: string;
  cpu_cores: number;
  ram_bytes: number;
  local_disk: string;
  os: string;
  kernel: string;
  object_store: string;
  endpoint: string;
  region: string;
};

export type CheckpointReference = {
  database_path: string;
  checkpoint_id: string;
  manifest_id: number;
  lsm_digest_sha256: string;
  live_sst_bytes: number;
};

export type ResolvedConfiguration = {
  scale: number;
  dataset: {
    record_count: number;
    key_bytes: number;
    value_bytes: number;
    value_compression_ratio: number;
  };
  caches: {
    block_bytes: number;
    metadata_bytes: number;
    object_store_bytes: number;
  };
  task: {
    task: string;
    clients: number;
    warmup_ms: number;
    measurement_ms: number;
    initial_state: string;
    key_selection: string;
    operation_mix: Record<string, number>;
    scan_limit: number | null;
    transaction_hot_keys: number | null;
    transaction_reads: number | null;
    transaction_updates: number | null;
  };
  slate_settings: Record<string, unknown>;
  build_profile: string;
  enabled_features: string[];
};

export type PreparationResult = {
  status: 'ok';
  task: 'bulk-load' | 'compaction';
  golden_id: string;
  timestamp: string;
  source: SourceIdentity;
  environment: Environment;
  configuration: ResolvedConfiguration;
  source_checkpoint: CheckpointReference | null;
  checkpoint: CheckpointReference;
  dataset: {
    record_count: number;
    key_bytes: number;
    value_bytes: number;
    logical_bytes: number;
    live_sst_bytes: number;
  };
} & RecordedMetrics;

export type RateSummary = {
  total: number;
  avg_per_second: number;
  p50_per_second: number;
  p95_per_second: number;
  p99_per_second: number;
  p999_per_second: number;
  min_per_second: number;
  max_per_second: number;
};

export type ThroughputSummary = {
  total_bytes: number;
  avg_bytes_per_second: number;
  p50_bytes_per_second: number;
  p95_bytes_per_second: number;
  p99_bytes_per_second: number;
  p999_bytes_per_second: number;
  min_bytes_per_second: number;
  max_bytes_per_second: number;
};

export type LatencySummary = {
  count: number;
  avg_ns: number;
  p50_ns: number;
  p95_ns: number;
  p99_ns: number;
  p999_ns: number;
  min_ns: number;
  max_ns: number;
};

export type DistributionSummary = {
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  p999: number;
  min: number;
  max: number;
};

export type RecordedMetrics = {
  recorded_interval_ns: number;
  application: {
    operations: Record<string, RateSummary>;
    throughput: Record<string, ThroughputSummary>;
    latency: Record<string, LatencySummary>;
  };
  object_store: {
    requests: Record<string, RateSummary>;
    throughput: Record<string, ThroughputSummary>;
  };
  process: {
    cpu_cores: DistributionSummary;
    rss_bytes: DistributionSummary;
  };
  machine: {
    cpu_percent: DistributionSummary;
    rss_bytes: DistributionSummary;
    network_receive_bytes_per_second: DistributionSummary;
    network_send_bytes_per_second: DistributionSummary;
    disk_read_bytes_per_second: DistributionSummary;
    disk_write_bytes_per_second: DistributionSummary;
    disk_read_operations_per_second: DistributionSummary;
    disk_write_operations_per_second: DistributionSummary;
  };
};

export type WorkloadResult = {
  status: 'ok';
  task: string;
  golden_id: string;
  session: string;
  timestamp: string;
  source: SourceIdentity;
  environment: Environment;
  configuration: ResolvedConfiguration;
  initial_state: {
    kind: 'golden' | 'empty';
    checkpoint_id: string | null;
    manifest_id: number | null;
    lsm_digest_sha256: string;
  };
  client_measurement_ns: number;
  durability_drain_ns: number;
  series: {
    file: 'series.json';
    sha256: string;
  };
} & RecordedMetrics;

export type HistogramSeries = {
  upper_bound_ns: number[];
  counts: number[];
};

export type WorkloadSeries = {
  rate_elapsed_ns: number[];
  rate_duration_ns: number[];
  latency_elapsed_ns: number[];
  latency_duration_ns: number[];
  resource_elapsed_ns: number[];
  resource_duration_ns: number[];
  application: {
    operations_per_second: Record<string, number[]>;
    bytes_per_second: Record<string, number[]>;
    latency_ns: Record<string, {
      avg: (number | null)[];
      p50: (number | null)[];
      p95: (number | null)[];
      p99: (number | null)[];
      p999: (number | null)[];
    }>;
    latency_histograms: Record<string, HistogramSeries>;
  };
  object_store: {
    requests_per_second: Record<string, number[]>;
    bytes_per_second: Record<string, number[]>;
  };
  process: {
    cpu_cores: number[];
    rss_bytes: number[];
  };
  machine: {
    cpu_percent: number[];
    rss_bytes: number[];
    network_receive_bytes_per_second: number[];
    network_send_bytes_per_second: number[];
    disk_read_bytes_per_second: number[];
    disk_write_bytes_per_second: number[];
    disk_read_operations_per_second: number[];
    disk_write_operations_per_second: number[];
  };
};

export type ResultRoute<T> = {
  version: string;
  kind: 'preparation' | 'workload';
  name: string;
  result: T;
};

const repoRoot = path.resolve(process.cwd(), '..');
export const resultsRoot = process.env.BENCHMARK_RESULTS_ROOT
  ? path.resolve(process.env.BENCHMARK_RESULTS_ROOT)
  : path.join(repoRoot, 'results');

async function walk(directory: string): Promise<string[]> {
  let entries: Dirent<string>[];
  try {
    entries = await fs.readdir(directory, { withFileTypes: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
  const values = await Promise.all(
    entries.map((entry) => {
      const child = path.join(directory, entry.name);
      return entry.isDirectory() ? walk(child) : Promise.resolve([child]);
    }),
  );
  return values.flat();
}

export async function loadPreparationResults(): Promise<ResultRoute<PreparationResult>[]> {
  return loadTaskResults<PreparationResult>('preparation');
}

export async function loadWorkloadResults(): Promise<ResultRoute<WorkloadResult>[]> {
  return loadTaskResults<WorkloadResult>('workload');
}

async function loadTaskResults<T>(kind: 'preparation' | 'workload'): Promise<ResultRoute<T>[]> {
  const files = (await walk(resultsRoot)).filter((file) => {
    const relative = path.relative(resultsRoot, file).split(path.sep);
    return relative.length === 4 && relative[1] === kind && relative[3] === 'result.json';
  });
  const routes = await Promise.all(
    files.map(async (file) => {
      const [version, , name] = path.relative(resultsRoot, file).split(path.sep);
      return {
        version,
        kind,
        name,
        result: JSON.parse(await fs.readFile(file, 'utf8')) as T,
      };
    }),
  );
  return routes.sort(compareRoutes);
}

export async function rawResultFiles() {
  const files = (await walk(resultsRoot)).filter((file) =>
    ['result.json', 'run.json', 'series.json'].includes(path.basename(file)),
  );
  return Promise.all(
    files.map(async (file) => ({
      path: path.relative(resultsRoot, file).split(path.sep).join('/'),
      body: await fs.readFile(file),
    })),
  );
}

export function latestStable<T>(routes: ResultRoute<T>[]): ResultRoute<T> | undefined {
  return routes.find((route) => /^\d+\.\d+\.\d+$/.test(route.version)) ?? routes[0];
}

export function routeHref(route: Pick<ResultRoute<unknown>, 'version' | 'kind' | 'name'>) {
  return `/${route.version}/${route.kind}/${route.name}/`;
}

function compareRoutes(left: ResultRoute<unknown>, right: ResultRoute<unknown>) {
  return compareVersion(right.version, left.version) || compareTask(left.name, right.name);
}

export const workloadNames = [
  'idle',
  'point-read-uniform',
  'point-read-skewed',
  'point-read-missing',
  'read-heavy',
  'balanced',
  'update-heavy',
  'range-scan',
  'sustained-ingest',
  'transaction-contention',
] as const;

const taskOrder = [
  'bulk-load',
  'compaction',
  ...workloadNames,
];

function compareTask(left: string, right: string) {
  const leftIndex = taskOrder.indexOf(left);
  const rightIndex = taskOrder.indexOf(right);
  if (leftIndex === -1 && rightIndex === -1) return left.localeCompare(right);
  if (leftIndex === -1) return 1;
  if (rightIndex === -1) return -1;
  return leftIndex - rightIndex;
}

function compareVersion(left: string, right: string) {
  const a = left.split(/[.-]/).map((part) => Number(part) || 0);
  const b = right.split(/[.-]/).map((part) => Number(part) || 0);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const difference = (a[index] || 0) - (b[index] || 0);
    if (difference !== 0) return difference;
  }
  return left.localeCompare(right);
}
