import type { BenchmarkResult, BenchmarkTimeseries, Latency, ResultRoute } from './results';

export type Point = { x: number; y: number };

export type ChartDefinition = {
  key: string;
  title: string;
  description: string;
  unit: string;
  digits: number;
  showPoints?: boolean;
  datasets: ChartDataset[];
};

export type ChartDataset = {
  label: string;
  color: string;
  points: Point[];
  dash?: number[];
  width?: number;
};

export type ChartManifest = Pick<ChartDefinition, 'key' | 'title' | 'description'> & {
  href: string;
};

export type CompactChart = Omit<ChartDefinition, 'datasets'> & {
  x: number[][];
  datasets: Array<Omit<ChartDataset, 'points'> & {
    x: number;
    y: number[];
  }>;
};

type RouteIdentity = Pick<ResultRoute, 'version' | 'suite' | 'workload' | 'variant'>;

const apiOrder = [
  'get',
  'put',
  'scan',
  'transaction.begin',
  'transaction.get',
  'transaction.put',
  'transaction.commit',
  'flush',
];

export function buildCharts(
  result: BenchmarkResult,
  timeseries: BenchmarkTimeseries,
): ChartDefinition[] {
  const application = timeseries.application_windows;
  const atEnd = (window: { start_offset_ns: number; duration_ns: number }) =>
    (window.start_offset_ns + window.duration_ns) / 1e9;
  const seconds = (window: { duration_ns: number }) => window.duration_ns / 1e9;
  const mibPerSecond = (bytes: number, durationSeconds: number) =>
    bytes / 1_048_576 / Math.max(durationSeconds, Number.EPSILON);
  const bytesPerSecondToMib = (bytes: number) => bytes / 1_048_576;
  const machineRate = (field: 'network_bytes_sent' | 'network_bytes_received') =>
    timeseries.samples.slice(1).flatMap((sample, index) => {
      const previous = timeseries.samples[index];
      const duration = (sample.offset_ns - previous.offset_ns) / 1e9;
      if (duration <= 0) return [];
      return [{
        x: sample.offset_ns / 1e9,
        y: mibPerSecond(Math.max(0, sample[field] - previous[field]), duration),
      }];
    });
  const scalarMetricSeries = (name: string, valueType: 'counter' | 'gauge') =>
    timeseries.slatedb_metrics.filter((series) =>
      series.name === name && series.value_type === valueType
    );
  const scalarValue = (value: number | Record<string, unknown> | null | undefined) =>
    typeof value === 'number' ? value : null;
  const compactorReadPoints = (() => {
    const series = scalarMetricSeries('slatedb.compactor.total_throughput_bytes_per_sec', 'gauge');
    return timeseries.samples.flatMap((sample, index) => {
      const values = series.flatMap((metric) => {
        const value = scalarValue(metric.values[index]);
        return value === null ? [] : [value];
      });
      return values.length === 0 ? [] : [{
        x: sample.offset_ns / 1e9,
        y: bytesPerSecondToMib(Math.max(0, values.reduce((sum, value) => sum + value, 0))),
      }];
    });
  })();
  const compactorWritePoints = (() => {
    const series = scalarMetricSeries('slatedb.compactor.bytes_compacted', 'counter');
    return timeseries.samples.slice(1).flatMap((sample, index) => {
      const duration = (sample.offset_ns - timeseries.samples[index].offset_ns) / 1e9;
      if (duration <= 0) return [];
      const deltas = series.flatMap((metric) => {
        const previous = scalarValue(metric.values[index]);
        const current = scalarValue(metric.values[index + 1]);
        return previous === null || current === null ? [] : [Math.max(0, current - previous)];
      });
      return deltas.length === 0 ? [] : [{
        x: sample.offset_ns / 1e9,
        y: mibPerSecond(deltas.reduce((sum, value) => sum + value, 0), duration),
      }];
    });
  })();
  const latencyDatasets = (rows: Array<{ x: number; latency: Latency }>): ChartDataset[] => [
    { label: 'p50', color: '#8b94a3', value: (latency: Latency) => latency.p50_ns },
    { label: 'p95', color: '#5e6878', value: (latency: Latency) => latency.p95_ns },
    { label: 'p99', color: '#b26844', value: (latency: Latency) => latency.p99_ns },
  ].map(({ label, color, value }) => ({
    label,
    color,
    points: rows.map(({ x, latency }) => ({ x, y: value(latency) / 1e6 })),
  }));
  const awaitsDurability = result.durability.final_flush_drain_ns !== null
    && result.durability.lag === null;

  const throughputChart: ChartDefinition = {
    key: 'payload-throughput',
    title: 'Throughput',
    description: 'Logical application payload, SlateDB compaction, and host-wide network traffic',
    unit: 'MiB / second',
    digits: 2,
    datasets: [
      {
        label: 'Application read',
        color: '#3f6f8f',
        width: 2.2,
        points: application.map((window) => ({
          x: atEnd(window),
          y: mibPerSecond(window.read_payload_bytes, seconds(window)),
        })),
      },
      {
        label: 'Application write',
        color: '#b26844',
        width: 2.2,
        points: application.map((window) => ({
          x: atEnd(window),
          y: mibPerSecond(window.write_payload_bytes, seconds(window)),
        })),
      },
      {
        label: 'Compactor read',
        color: '#3f6f8f',
        dash: [2, 4],
        points: compactorReadPoints,
      },
      {
        label: 'Compactor write',
        color: '#b26844',
        dash: [2, 4],
        points: compactorWritePoints,
      },
      {
        label: 'Machine download',
        color: '#3f6f8f',
        dash: [7, 5],
        points: machineRate('network_bytes_received'),
      },
      {
        label: 'Machine upload',
        color: '#b26844',
        dash: [7, 5],
        points: machineRate('network_bytes_sent'),
      },
    ],
  };

  const apiLabel = (api: string) => `${api}()`;
  const apiChartLabel = (api: string) => `${api.replace(/^transaction\./, '')}()`;
  const apiDescription = (api: string) => {
    if (api === 'scan') return 'SlateDB scan invocation through iterator exhaustion';
    if (api === 'flush') return 'SlateDB flush() invocation through durable completion';
    const durability = ['put', 'transaction.commit'].includes(api) && awaitsDurability
      ? ' · includes durability'
      : '';
    return `SlateDB ${apiLabel(api)} invocation to API return${durability}`;
  };
  const apiCharts: ChartDefinition[] = [...new Set(application.flatMap((window) =>
    Object.keys(window.api_latency),
  ))]
    .sort((left, right) => {
      const leftIndex = apiOrder.indexOf(left);
      const rightIndex = apiOrder.indexOf(right);
      return (leftIndex < 0 ? apiOrder.length : leftIndex)
        - (rightIndex < 0 ? apiOrder.length : rightIndex)
        || left.localeCompare(right);
    })
    .flatMap((api) => {
      const rows = application.flatMap((window) => {
        const latency = window.api_latency[api];
        return latency ? [{ x: atEnd(window), latency }] : [];
      });
      const calls = rows.reduce((count, row) => count + row.latency.count, 0);
      if (api === 'flush' && calls < 2) return [];
      return [{
        key: `api-${api.replaceAll('.', '-')}`,
        title: `${apiChartLabel(api)} latency`,
        description: apiDescription(api),
        unit: 'Milliseconds',
        digits: 3,
        showPoints: rows.length < 3,
        datasets: latencyDatasets(rows),
      }];
    });

  const durabilityRows = (timeseries.durability_windows || [])
    .filter((window) => window.durability_lag !== null)
    .map((window) => ({ x: atEnd(window), latency: window.durability_lag! }));
  const durabilityChart: ChartDefinition | null = durabilityRows.length > 0 ? {
    key: 'durability-latency',
    title: 'Durability latency',
    description: 'API return to durable frontier coverage',
    unit: 'Milliseconds',
    digits: 3,
    datasets: latencyDatasets(durabilityRows),
  } : null;

  return [
    throughputChart,
    ...apiCharts,
    durabilityChart,
  ]
    .filter((chart): chart is ChartDefinition => chart !== null);
}

export function compactChart(chart: ChartDefinition): CompactChart {
  const { datasets, ...metadata } = chart;
  const x: number[][] = [];
  const compactDatasets = datasets.map(({ points, ...dataset }) => {
    const values = points.map((point) => point.x);
    let axis = x.findIndex((candidate) =>
      candidate.length === values.length
      && candidate.every((value, index) => value === values[index])
    );
    if (axis < 0) {
      axis = x.length;
      x.push(values);
    }
    return {
      ...dataset,
      x: axis,
      y: points.map((point) => point.y),
    };
  });
  return { ...metadata, x, datasets: compactDatasets };
}

export function chartManifest(
  route: RouteIdentity,
  chart: ChartDefinition,
): ChartManifest {
  return {
    key: chart.key,
    title: chart.title,
    description: chart.description,
    href: chartDataHref(route, chart.key),
  };
}

export function chartDataHref(route: RouteIdentity, chartKey: string): string {
  const segments = [
    route.version,
    route.suite,
    route.workload,
    route.variant,
    `${chartKey}.json`,
  ].map(encodeURIComponent);
  return `../../../../charts/${segments.join('/')}`;
}
