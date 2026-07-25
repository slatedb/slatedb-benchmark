import { promises as fs } from 'node:fs';
import type { Dirent } from 'node:fs';
import path from 'node:path';

import type {
  AppliedPatch,
  DistributionSummary,
  Environment,
  GoldenManifest,
  HistogramSeries,
  LatencySummary,
  RateSummary,
  ResultConfiguration,
  RunManifest,
  SourceIdentity,
  ThroughputSummary,
  WorkloadResult,
  WorkloadSeries,
} from "../generated/artifacts";
import { workloadNames } from "../generated/artifacts";

export type {
  AppliedPatch,
  DistributionSummary,
  Environment,
  GoldenManifest,
  HistogramSeries,
  LatencySummary,
  RateSummary,
  ResultConfiguration,
  RunManifest,
  SourceIdentity,
  ThroughputSummary,
  WorkloadResult,
  WorkloadSeries,
};
export { workloadNames };

export type ResolvedConfiguration = ResultConfiguration;
export type RecordedMetrics = Pick<
  WorkloadResult,
  "recorded_interval_ns" | "application" | "object_store" | "process" | "machine"
>;

export type ResultRoute<T> = {
  version: string;
  run: BenchmarkRun;
  rawBase: string;
  kind: 'golden' | 'workload';
  name: string;
  result: T;
};

export type BenchmarkRun = {
  id: string;
  startedAt: string;
  finishedAt: string;
  runnerCommit: string;
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
let archiveFilesPromise: Promise<string[]> | undefined;
let runsPromise: Promise<StoredRun[]> | undefined;
let datasetResultsPromise: Promise<ResultRoute<GoldenManifest>[]> | undefined;
let workloadResultsPromise: Promise<ResultRoute<WorkloadResult>[]> | undefined;

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

function loadArchiveFiles() {
  archiveFilesPromise ??= walk(resultsRoot);
  return archiveFilesPromise;
}

export function loadDatasetResults(): Promise<ResultRoute<GoldenManifest>[]> {
  datasetResultsPromise ??= readDatasetResults();
  return datasetResultsPromise;
}

async function readDatasetResults(): Promise<ResultRoute<GoldenManifest>[]> {
  const runs = await loadRuns();
  const routes = await Promise.all(runs
    .filter((stored) => Object.hasOwn(stored.manifest.results, 'golden.json'))
    .map(async (stored) => {
      const { root: _root, manifest: _manifest, ...routeRun } = stored;
      return {
        ...routeRun,
        kind: 'golden' as const,
        name: 'dataset',
        result: JSON.parse(
          await fs.readFile(path.join(stored.root, 'golden.json'), 'utf8'),
        ) as GoldenManifest,
      };
    }));
  return routes.sort(compareRoutes);
}

export function loadWorkloadResults(): Promise<ResultRoute<WorkloadResult>[]> {
  workloadResultsPromise ??= loadTaskResults<WorkloadResult>();
  return workloadResultsPromise;
}

async function loadTaskResults<T>(): Promise<ResultRoute<T>[]> {
  const runs = await loadRuns();
  const routes = await Promise.all(runs.flatMap((stored) =>
    Object.keys(stored.manifest.results)
      .map((relative) => relative.split('/'))
      .filter((parts) =>
        parts.length === 3
        && parts[0] === 'workload'
        && parts[2] === 'result.json'
      )
      .map(async ([, name]) => {
        const file = path.join(stored.root, 'workload', name, 'result.json');
        const { root: _root, manifest: _manifest, ...routeRun } = stored;
        return {
          ...routeRun,
          kind: 'workload' as const,
          name,
          result: JSON.parse(await fs.readFile(file, 'utf8')) as T,
        };
      }),
  ));
  return routes.sort(compareRoutes);
}

function loadRuns(): Promise<StoredRun[]> {
  runsPromise ??= readRuns();
  return runsPromise;
}

async function readRuns(): Promise<StoredRun[]> {
  const manifests = (await loadArchiveFiles()).filter((file) => {
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
        runnerCommit: manifest.source.runner_commit,
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

export async function rawResultPaths() {
  const files = (await loadArchiveFiles()).filter((file) =>
    ['golden.json', 'result.json', 'run.json', 'series.json'].includes(path.basename(file)),
  );
  return files.map((file) => ({
    path: path.relative(resultsRoot, file).split(path.sep).join('/'),
    file,
  }));
}

export function latestStable<T>(routes: ResultRoute<T>[]): ResultRoute<T> | undefined {
  return routes.find((route) => /^\d+\.\d+\.\d+$/.test(route.version)) ?? routes[0];
}

export function routeHref(
  route: Pick<ResultRoute<unknown>, 'version' | 'run' | 'kind' | 'name'>,
) {
  if (route.kind === 'golden') {
    return datasetHref(route.version, route.run.id);
  }
  return `/${route.version}/run/${route.run.id}/workload/${route.name}/`;
}

export function latestRouteHref(
  route: Pick<ResultRoute<unknown>, 'version' | 'kind' | 'name'>,
) {
  if (route.kind === 'golden') return datasetHref(route.version);
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

const taskOrder = [
  'dataset',
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
