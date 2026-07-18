import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';

const workloads = defineCollection({
  loader: glob({ pattern: '*.md', base: './src/content/workloads' }),
});

export const collections = { workloads };
