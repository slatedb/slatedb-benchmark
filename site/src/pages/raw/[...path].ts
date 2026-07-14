import type { APIRoute, GetStaticPaths } from 'astro';
import { rawResultFiles } from '../../lib/results';

export const getStaticPaths = (async () => {
  const files = await rawResultFiles();
  return files.map((file) => ({ params: { path: file.path }, props: { body: file.body } }));
}) satisfies GetStaticPaths;

export const GET: APIRoute = ({ props }) =>
  new Response(props.body, { headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'public, max-age=300' } });
