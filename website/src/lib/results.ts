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

export type AppliedPatch = {
  name: string;
  sha256: string;
};

export type RunManifest = {
  status: 'ok';
  run_id: string;
  golden_id: string;
  started_at: string;
  finished_at: string;
  patches: AppliedPatch[];
  source: SourceIdentity;
  results: Record<string, string>;
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
  slate_default_settings?: Record<string, unknown>;
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
  p001_per_second: number;
  p01_per_second: number;
  p50_per_second: number;
  p99_per_second: number;
  p999_per_second: number;
};

export type ThroughputSummary = {
  total_bytes: number;
  avg_bytes_per_second: number;
  p001_bytes_per_second: number;
  p01_bytes_per_second: number;
  p50_bytes_per_second: number;
  p99_bytes_per_second: number;
  p999_bytes_per_second: number;
};

export type LatencySummary = {
  count: number;
  avg_ns: number;
  p001_ns: number;
  p01_ns: number;
  p50_ns: number;
  p99_ns: number;
  p999_ns: number;
};

export type DistributionSummary = {
  avg: number;
  p001: number;
  p01: number;
  p50: number;
  p99: number;
  p999: number;
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
  actions_log_url?: string;
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
      p001: (number | null)[];
      p01: (number | null)[];
      p50: (number | null)[];
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
  run: BenchmarkRun;
  rawBase: string;
  kind: 'preparation' | 'workload';
  name: string;
  result: T;
};

export type BenchmarkRun = {
  id: string;
  startedAt: string;
  finishedAt: string;
  patches: AppliedPatch[];
};

type StoredRun = {
  version: string;
  run: BenchmarkRun;
  rawBase: string;
  root: string;
  manifest: RunManifest;
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

export async function loadDatasetResults(): Promise<ResultRoute<PreparationResult>[]> {
  return (await loadPreparationResults()).filter((route) => route.name === 'compaction');
}

export async function loadWorkloadResults(): Promise<ResultRoute<WorkloadResult>[]> {
  return loadTaskResults<WorkloadResult>('workload');
}

async function loadTaskResults<T>(kind: 'preparation' | 'workload'): Promise<ResultRoute<T>[]> {
  const runs = await loadRuns();
  const routes = await Promise.all(runs.flatMap((stored) =>
    Object.keys(stored.manifest.results)
      .map((relative) => relative.split('/'))
      .filter((parts) =>
        parts.length === 3
        && parts[0] === kind
        && parts[2] === 'result.json'
      )
      .map(async ([, name]) => {
        const file = path.join(stored.root, kind, name, 'result.json');
        const { root: _root, manifest: _manifest, ...routeRun } = stored;
        return {
          ...routeRun,
          kind,
          name,
          result: JSON.parse(await fs.readFile(file, 'utf8')) as T,
        };
      }),
  ));
  return routes.sort(compareRoutes);
}

async function loadRuns(): Promise<StoredRun[]> {
  const manifests = (await walk(resultsRoot)).filter((file) => {
    if (path.basename(file) !== 'run.json') return false;
    const relative = path.relative(resultsRoot, file).split(path.sep);
    return relative.length === 3;
  });
  const runs = await Promise.all(manifests.map(async (file) => {
    const relative = path.relative(resultsRoot, file).split(path.sep);
    const version = relative[0];
    const root = path.dirname(file);
    const rawBase = path.relative(resultsRoot, root).split(path.sep).join('/');
    const manifest = JSON.parse(await fs.readFile(file, 'utf8')) as RunManifest;
    const nestedRunId = relative[1];
    if (manifest.run_id !== nestedRunId) {
      throw new Error(`${file} run_id does not match its directory`);
    }
    return {
      version,
      rawBase,
      root,
      manifest,
      run: {
        id: manifest.run_id,
        startedAt: manifest.started_at,
        finishedAt: manifest.finished_at,
        patches: manifest.patches,
      },
    };
  }));
  const identities = new Set<string>();
  for (const stored of runs) {
    const identity = `${stored.version}\0${stored.run.id}`;
    if (identities.has(identity)) {
      throw new Error(`duplicate benchmark run ${stored.version}/${stored.run.id}`);
    }
    identities.add(identity);
  }
  return runs;
}

export function latestRoutes<T>(
  routes: ResultRoute<T>[],
  key: (route: ResultRoute<T>) => string,
) {
  const selected = new Map<string, ResultRoute<T>>();
  for (const route of routes) {
    const candidate = selected.get(key(route));
    if (!candidate || compareRunDates(route, candidate) < 0) {
      selected.set(key(route), route);
    }
  }
  return [...selected.values()].sort(compareRoutes);
}

function compareRunDates(left: ResultRoute<unknown>, right: ResultRoute<unknown>) {
  return Date.parse(right.run.startedAt) - Date.parse(left.run.startedAt)
    || right.run.id.localeCompare(left.run.id);
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

export function routeHref(
  route: Pick<ResultRoute<unknown>, 'version' | 'run' | 'kind' | 'name'>,
) {
  if (route.kind === 'preparation') {
    return datasetHref(route.version, route.run.id);
  }
  return `/${route.version}/run/${route.run.id}/workload/${route.name}/`;
}

export function latestRouteHref(
  route: Pick<ResultRoute<unknown>, 'version' | 'kind' | 'name'>,
) {
  if (route.kind === 'preparation') return datasetHref(route.version);
  return `/${route.version}/workload/${route.name}/`;
}

export function datasetHref(version: string, runId?: string) {
  return runId
    ? `/${version}/run/${runId}/dataset/`
    : `/${version}/dataset/`;
}

function compareRoutes(left: ResultRoute<unknown>, right: ResultRoute<unknown>) {
  return compareVersions(right.version, left.version)
    || compareRunDates(left, right)
    || compareTask(left.name, right.name);
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

const versionPattern = /^(\d+)\.(\d+)\.(\d+)(?:[.-](.+))?$/;

export function versionLabel(version: string) {
  return versionPattern.test(version) ? version : `0.0.0-${version}`;
}

export function compareVersions(left: string, right: string) {
  const a = versionLabel(left).match(versionPattern)!;
  const b = versionLabel(right).match(versionPattern)!;
  for (let index = 1; index <= 3; index += 1) {
    const difference = Number(a[index]) - Number(b[index]);
    if (difference !== 0) return difference;
  }
  const prerelease = comparePrerelease(a[4], b[4]);
  if (prerelease !== 0) return prerelease;
  return left.localeCompare(right);
}

function comparePrerelease(left: string | undefined, right: string | undefined) {
  if (left === right) return 0;
  if (left === undefined) return 1;
  if (right === undefined) return -1;
  const a = left.split('.');
  const b = right.split('.');
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    if (a[index] === undefined) return -1;
    if (b[index] === undefined) return 1;
    const aNumber = /^\d+$/.test(a[index]);
    const bNumber = /^\d+$/.test(b[index]);
    if (aNumber && bNumber) {
      const difference = Number(a[index]) - Number(b[index]);
      if (difference !== 0) return difference;
    } else if (aNumber !== bNumber) {
      return aNumber ? -1 : 1;
    } else {
      const difference = a[index].localeCompare(b[index]);
      if (difference !== 0) return difference;
    }
  }
  return 0;
}
