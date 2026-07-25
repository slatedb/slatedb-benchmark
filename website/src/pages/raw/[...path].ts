import type { APIRoute, GetStaticPaths } from 'astro';
import { promises as fs } from 'node:fs';
import { rawResultPaths } from '../../lib/results';

export const getStaticPaths = (async () => {
  const files = await rawResultPaths();
  return files.map((file) => ({ params: { path: file.path }, props: { file: file.file } }));
}) satisfies GetStaticPaths;

export const GET: APIRoute = async ({ props }) =>
  new Response(await fs.readFile(props.file), {
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'public, max-age=300',
    },
  });
