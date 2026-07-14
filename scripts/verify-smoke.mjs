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
}
if (expected.size) throw new Error(`missing smoke variants: ${[...expected].join(', ')}`);
console.log(`verified ${catalog.length} Docker smoke variants`);
