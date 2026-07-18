import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

const workloads = defineCollection({
  loader: glob({ pattern: '*.md', base: './src/content/workloads' }),
  schema: z.object({
    source: z.string().regex(/^src\/[A-Za-z0-9_./-]+\.rs$/),
  }),
});

export const collections = { workloads };
