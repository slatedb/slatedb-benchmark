import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://benchmark.slatedb.io',
  output: 'static',
  build: { format: 'directory' },
});
