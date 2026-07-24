import { getCollection } from 'astro:content';
import { loadWorkloadResults, workloadNames } from './results';

export async function loadWorkloadPageData() {
  const workloads = await loadWorkloadResults();
  const content = await getCollection('workloads');
  const descriptions = new Map(content.map((entry) => [entry.id, entry]));
  const expected = new Set<string>(workloadNames);
  const missing = workloadNames.filter((name) => !descriptions.has(name));
  const unexpected = content
    .filter((entry) => !expected.has(entry.id))
    .map((entry) => entry.id);
  const undocumented = [...new Set(
    workloads
      .filter((route) => !descriptions.has(route.name))
      .map((route) => route.name),
  )];
  if (missing.length > 0 || unexpected.length > 0 || undocumented.length > 0) {
    throw new Error(
      [
        'workload descriptions do not match the catalog',
        `missing=${missing.join(',') || 'none'}`,
        `unexpected=${unexpected.join(',') || 'none'}`,
        `undocumented results=${undocumented.join(',') || 'none'}`,
      ].join('; '),
    );
  }
  return { workloads, descriptions };
}
