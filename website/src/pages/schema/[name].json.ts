import { promises as fs } from 'node:fs';
import path from 'node:path';
import type { APIRoute, GetStaticPaths } from 'astro';

const names = ['result', 'run'];
const schemaRoot = path.resolve(process.cwd(), '..', 'schema');

export const getStaticPaths = (async () =>
  Promise.all(names.map(async (name) => ({
    params: { name },
    props: { body: await fs.readFile(path.join(schemaRoot, `${name}.json`)) },
  })))) satisfies GetStaticPaths;

export const GET: APIRoute = ({ props }) =>
  new Response(props.body, {
    headers: {
      'content-type': 'application/schema+json; charset=utf-8',
      'cache-control': 'public, max-age=300',
    },
  });
