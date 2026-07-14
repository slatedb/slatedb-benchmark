import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';

const root = process.argv[2];
if (!root) throw new Error('usage: verify-smoke.mjs <run-directory>');
const run = JSON.parse(await readFile(path.join(root, 'run.json'), 'utf8'));
const catalog = JSON.parse(
  await new Promise((resolve, reject) => {
    import('node:child_process').then(({ execFile }) =>
      execFile('cargo', ['run', '--quiet', '--', 'catalog', '--smoke'], { cwd: process.cwd() }, (error, stdout) =>
        error ? reject(error) : resolve(stdout),
      ),
    );
  }),
);
if (run.results.length !== catalog.length) {
  throw new Error(`expected ${catalog.length} results, found ${run.results.length}`);
}
const expected = new Set(catalog.map((entry) => `${entry.profile}/${entry.workload}/${entry.variant}`));
for (const relative of run.results) {
  const result = JSON.parse(await readFile(path.join(root, relative), 'utf8'));
  expected.delete(`${result.identity.profile}/${result.identity.workload}/${result.identity.variant}`);
  const directory = path.dirname(path.join(root, relative));
  const files = await readdir(directory);
  for (const required of ['result.json', 'histograms.json', 'timeseries.json']) {
    if (!files.includes(required)) throw new Error(`${directory} is missing ${required}`);
  }
  const timeseries = JSON.parse(await readFile(path.join(directory, 'timeseries.json'), 'utf8'));
  if (!Array.isArray(timeseries.slatedb_metrics)) {
    throw new Error(`${directory} does not contain columnar SlateDB metrics`);
  }
  if (timeseries.samples.some((sample) => Object.hasOwn(sample, 'slatedb_metrics'))) {
    throw new Error(`${directory} repeats SlateDB metric metadata in each sample`);
  }
  for (const metric of timeseries.slatedb_metrics) {
    if (metric.values.length !== timeseries.samples.length) {
      throw new Error(`${directory} metric ${metric.name} is not aligned with its samples`);
    }
  }
  const fixed = {
    ...timeseries,
    samples: [],
    slatedb_metrics: timeseries.slatedb_metrics.map((metric) => ({ ...metric, values: [] })),
  };
  const bytes = (value) => Buffer.byteLength(JSON.stringify(value));
  const hostSampleBytes = Math.max(...timeseries.samples.map(bytes));
  const metricValueBytes = timeseries.slatedb_metrics.reduce(
    (total, metric) => total + Math.max(...metric.values.map(bytes)) + 1,
    0,
  );
  const projectedNinetyMinuteBytes =
    bytes(fixed) + 5_402 * (hostSampleBytes + metricValueBytes + 1);
  if (projectedNinetyMinuteBytes >= 90 * 1024 * 1024) {
    throw new Error(
      `${directory} projects to ${(projectedNinetyMinuteBytes / 1024 / 1024).toFixed(1)} MiB for 90 minutes`,
    );
  }
}
if (expected.size) throw new Error(`missing smoke variants: ${[...expected].join(', ')}`);
console.log(`verified ${catalog.length} Docker smoke variants`);
