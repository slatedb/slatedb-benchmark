import { promises as fs } from 'node:fs';
import type { Dirent } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type BenchmarkResult = {
  schema_version: number;
  identity: {
    slate_version: string;
    slate_commit: string;
    runner_version: string;
    runner_commit: string;
    timestamp: string;
    suite: string;
    workload: string;
    variant: string;
    mode: string;
  };
  environment: Record<string, string | number>;
  object_store_baseline: {
    upload_mib_per_second: number;
    download_mib_per_second: number;
    put_latency: Latency;
    get_latency: Latency;
  };
  configuration: Record<string, unknown> & {
    clients: number | null;
    target_rate: number | null;
    record_count: number;
    key_bytes: number;
    value_bytes: number;
    block_cache_bytes: number | null;
    metadata_cache_bytes: number | null;
  };
  application: Record<string, unknown> & {
    accepted_ops_per_second: number;
    payload_mib_per_second: number;
    errors: number;
    return_latency: Latency;
    return_latency_by_operation: Record<string, Latency>;
  };
  durability: Record<string, unknown> & {
    lag: Latency | null;
    durable_ops_per_second: number | null;
  };
  resources: Record<string, unknown>;
  storage: Record<string, unknown>;
  cost: Record<string, unknown> & { total: number; total_per_million_operations: number | null; currency: string };
  initial_state: Record<string, unknown>;
  source_files: { histograms: string; timeseries: string };
};

export type Latency = {
  count: number;
  p50_ns: number;
  p95_ns: number;
  p99_ns: number;
  p999_ns: number;
  max_ns: number;
};

export type ApplicationWindow = {
  start_offset_ns: number;
  duration_ns: number;
  completed_operations: number;
  successful_operations: number;
  errors: number;
  payload_bytes: number;
  offered_operations: number | null;
  dropped_operations: number | null;
  return_latency: Latency | null;
  return_latency_by_operation: Record<string, Latency>;
  response_latency: Latency | null;
  scheduling_delay: Latency | null;
  batch_latency: Latency | null;
};

export type DurabilityWindow = {
  start_offset_ns: number;
  duration_ns: number;
  writes_made_durable: number;
  durability_lag: Latency | null;
};

export type BenchmarkTimeseries = {
  schema_version: number;
  interval_ns: number;
  application_windows: ApplicationWindow[];
  durability_windows: DurabilityWindow[] | null;
};

export type ResultRoute = {
  version: string;
  suite: string;
  workload: string;
  variant: string;
  result: BenchmarkResult;
  timeseries: BenchmarkTimeseries;
  directory: string;
};

const resultsRoot = process.env.BENCHMARK_RESULTS_ROOT
  ? path.resolve(process.env.BENCHMARK_RESULTS_ROOT)
  : fileURLToPath(new URL('../../../results', import.meta.url));

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

export async function loadResults(): Promise<ResultRoute[]> {
  const files = (await walk(resultsRoot)).filter((file) => path.basename(file) === 'result.json');
  const routes = await Promise.all(
    files.map(async (file) => {
      const result = JSON.parse(await fs.readFile(file, 'utf8')) as BenchmarkResult;
      const directory = path.dirname(file);
      const timeseries = JSON.parse(
        await fs.readFile(path.join(directory, result.source_files.timeseries), 'utf8'),
      ) as BenchmarkTimeseries;
      return {
        version: result.identity.slate_version,
        suite: result.identity.suite,
        workload: result.identity.workload,
        variant: result.identity.variant,
        result,
        timeseries,
        directory,
      };
    }),
  );
  return routes.sort(compareRoutes);
}

export async function rawResultFiles() {
  const files = (await walk(resultsRoot)).filter((file) =>
    ['result.json', 'histograms.json', 'timeseries.json'].includes(path.basename(file)),
  );
  return Promise.all(
    files.map(async (file) => ({
      path: path.relative(resultsRoot, file).split(path.sep).join('/'),
      body: await fs.readFile(file),
    })),
  );
}

export function latestStable(routes: ResultRoute[]): ResultRoute | undefined {
  const stable = routes
    .filter((route) => /^\d+\.\d+\.\d+$/.test(route.version))
    .sort((left, right) => compareVersion(right.version, left.version))[0];
  return stable || routes[0];
}

export function routeHref(route: Pick<ResultRoute, 'version' | 'suite' | 'workload' | 'variant'>) {
  return `/${route.version}/${route.suite}/${route.workload}/${route.variant}/`;
}

function compareRoutes(left: ResultRoute, right: ResultRoute) {
  return (
    compareVersion(right.version, left.version) ||
    left.suite.localeCompare(right.suite) ||
    left.workload.localeCompare(right.workload) ||
    left.variant.localeCompare(right.variant, undefined, { numeric: true })
  );
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
