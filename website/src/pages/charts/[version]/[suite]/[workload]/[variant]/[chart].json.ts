import type { APIRoute, GetStaticPaths } from 'astro';
import { buildCharts, compactChart } from '../../../../../../lib/charts';
import { loadResults } from '../../../../../../lib/results';

export const getStaticPaths = (async () => {
  const routes = await loadResults();
  return routes.flatMap((route) =>
    buildCharts(route.result, route.timeseries).map((chart) => ({
      params: {
        version: route.version,
        suite: route.suite,
        workload: route.workload,
        variant: route.variant,
        chart: chart.key,
      },
      props: { body: JSON.stringify(compactChart(chart)) },
    }))
  );
}) satisfies GetStaticPaths;

export const GET: APIRoute = ({ props }) =>
  new Response(props.body, {
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'public, max-age=300',
    },
  });
