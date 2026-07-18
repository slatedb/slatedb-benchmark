import type { HistogramSeries, WorkloadSeries } from '../lib/results';

type ChartOptions = {
  kind: 'time' | 'cdf';
  source: string;
  key?: string;
  unit: string;
  divisor?: number;
  average?: number;
  label: string;
};

type DestroyChart = () => void;

const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 });

export function initializeMetricCharts() {
  document.querySelectorAll<HTMLElement>('[data-metric-charts]').forEach((root) => {
    if (root.dataset.chartsReady === 'true') return;
    root.dataset.chartsReady = 'true';
    const seriesUrl = root.dataset.seriesUrl;
    if (!seriesUrl) return;

    let seriesPromise: Promise<WorkloadSeries> | undefined;
    const loadSeries = () => {
      seriesPromise ??= fetch(seriesUrl)
        .then((response) => {
          if (!response.ok) throw new Error(`Chart data returned ${response.status}`);
          return response.json() as Promise<WorkloadSeries>;
        })
        .catch((error) => {
          seriesPromise = undefined;
          throw error;
        });
      return seriesPromise;
    };

    schedulePreload(loadSeries);
    const destroyers = new WeakMap<HTMLElement, DestroyChart>();
    const closeTimers = new WeakMap<HTMLTableRowElement, number>();

    root.querySelectorAll<HTMLTableElement>('[data-chart-table]').forEach((table) => {
      table.querySelectorAll<HTMLTableRowElement>('[data-chart-row]').forEach((row) => {
        const button = row.querySelector<HTMLButtonElement>('[data-chart]');
        const detail = row.nextElementSibling as HTMLTableRowElement | null;
        const panel = detail?.querySelector<HTMLElement>('[data-chart-panel]');
        if (!button || !detail || !panel) return;

        row.addEventListener('click', () => {
          if (button.getAttribute('aria-expanded') === 'true') {
            closeRow(row, button, detail, panel, destroyers, closeTimers);
            return;
          }
          const open = table.querySelector<HTMLButtonElement>('[data-chart][aria-expanded="true"]');
          if (open) {
            const openRow = open.closest<HTMLTableRowElement>('[data-chart-row]');
            const openDetail = openRow?.nextElementSibling as HTMLTableRowElement | null;
            const openPanel = openDetail?.querySelector<HTMLElement>('[data-chart-panel]');
            if (openRow && openDetail && openPanel) {
              closeRow(openRow, open, openDetail, openPanel, destroyers, closeTimers);
            }
          }
          openRow(row, button, detail, panel, closeTimers);
          renderRow(
            panel,
            JSON.parse(button.dataset.chart ?? '{}') as ChartOptions,
            loadSeries,
            Number(root.dataset.clientMeasurementNs ?? 0),
            destroyers,
          );
        });
      });
    });
  });
}

function schedulePreload(loadSeries: () => Promise<WorkloadSeries>) {
  const connection = (navigator as Navigator & { connection?: { saveData?: boolean } }).connection;
  if (connection?.saveData) return;
  const start = () => window.setTimeout(() => void loadSeries().catch(() => undefined), 1_000);
  if (document.readyState === 'complete') start();
  else window.addEventListener('load', start, { once: true });
}

function openRow(
  row: HTMLTableRowElement,
  button: HTMLButtonElement,
  detail: HTMLTableRowElement,
  panel: HTMLElement,
  closeTimers: WeakMap<HTMLTableRowElement, number>,
) {
  const timer = closeTimers.get(detail);
  if (timer !== undefined) window.clearTimeout(timer);
  row.classList.add('chart-row-open');
  button.setAttribute('aria-expanded', 'true');
  detail.hidden = false;
  panel.dataset.open = 'true';
}

function closeRow(
  row: HTMLTableRowElement,
  button: HTMLButtonElement,
  detail: HTMLTableRowElement,
  panel: HTMLElement,
  destroyers: WeakMap<HTMLElement, DestroyChart>,
  closeTimers: WeakMap<HTMLTableRowElement, number>,
) {
  row.classList.remove('chart-row-open');
  button.setAttribute('aria-expanded', 'false');
  panel.dataset.open = 'false';
  destroyers.get(panel)?.();
  destroyers.delete(panel);
  const timer = window.setTimeout(() => {
    detail.hidden = true;
    panel.replaceChildren(status('Loading chart…'));
  }, prefersReducedMotion() ? 0 : 220);
  closeTimers.set(detail, timer);
}

async function renderRow(
  panel: HTMLElement,
  options: ChartOptions,
  loadSeries: () => Promise<WorkloadSeries>,
  clientMeasurementNs: number,
  destroyers: WeakMap<HTMLElement, DestroyChart>,
) {
  panel.replaceChildren(status('Loading chart…'));
  try {
    const series = await loadSeries();
    if (panel.dataset.open !== 'true') return;
    const destroy = options.kind === 'cdf'
      ? renderCdf(panel, options, series)
      : renderTimeSeries(panel, options, series, clientMeasurementNs);
    destroyers.set(panel, destroy);
  } catch (error) {
    if (panel.dataset.open !== 'true') return;
    const message = status(error instanceof Error ? error.message : 'Could not load chart data.');
    const retry = document.createElement('button');
    retry.type = 'button';
    retry.className = 'chart-retry';
    retry.textContent = 'Retry';
    retry.addEventListener('click', (event) => {
      event.stopPropagation();
      void renderRow(panel, options, loadSeries, clientMeasurementNs, destroyers);
    });
    panel.replaceChildren(message, retry);
  }
}

function renderTimeSeries(
  panel: HTMLElement,
  options: ChartOptions,
  series: WorkloadSeries,
  clientMeasurementNs: number,
): DestroyChart {
  const resource = options.source.startsWith('process.') || options.source.startsWith('machine.');
  const elapsed = resource ? series.resource_elapsed_ns : series.rate_elapsed_ns;
  const raw = resolveSource(series, options.source, options.key);
  if (!Array.isArray(raw) || raw.length !== elapsed.length || raw.length === 0) {
    throw new Error(`No chart data was recorded for ${options.label}.`);
  }
  const divisor = options.divisor ?? 1;
  const values = raw.map((value) => Number(value) / divisor);
  const average = options.average === undefined ? undefined : options.average / divisor;
  const points = elapsed.map((value, index) => ({ x: value / 1e9, y: values[index] }));
  return renderCanvas(panel, {
    label: options.label,
    xLabel: 'Elapsed seconds',
    yLabel: options.unit,
    points,
    average,
    boundary: resource && clientMeasurementNs > 0 ? clientMeasurementNs / 1e9 : undefined,
    logarithmicX: false,
    tooltip: (point) => {
      const interval = resource && clientMeasurementNs > 0 && point.x * 1e9 > clientMeasurementNs
        ? 'drain'
        : 'measurement';
      return `${formatElapsed(point.x)} · ${number.format(point.y)} ${options.unit} · ${interval}`;
    },
  });
}

function renderCdf(panel: HTMLElement, options: ChartOptions, series: WorkloadSeries): DestroyChart {
  const histogram = resolveSource(series, options.source, options.key) as HistogramSeries | undefined;
  if (!histogram || histogram.counts.length === 0) {
    throw new Error(`No latency data was recorded for ${options.label}.`);
  }
  const total = histogram.counts.reduce((sum, count) => sum + count, 0);
  let cumulative = 0;
  const points = histogram.counts.map((count, index) => {
    cumulative += count;
    return { x: histogram.upper_bound_ns[index] / 1e6, y: cumulative / total * 100 };
  });
  return renderCanvas(panel, {
    label: options.label,
    xLabel: 'Latency (ms, logarithmic)',
    yLabel: 'Cumulative calls (%)',
    points,
    logarithmicX: true,
    tooltip: (point) => `${number.format(point.y)}% at ${number.format(point.x)} ms`,
  });
}

function resolveSource(series: WorkloadSeries, source: string, key?: string): unknown {
  let value: unknown = series;
  for (const part of source.split('.')) {
    if (!value || typeof value !== 'object') return undefined;
    value = (value as Record<string, unknown>)[part];
  }
  if (key === undefined) return value;
  if (!value || typeof value !== 'object') return undefined;
  return (value as Record<string, unknown>)[key];
}

type Point = { x: number; y: number };
type CanvasOptions = {
  label: string;
  xLabel: string;
  yLabel: string;
  points: Point[];
  average?: number;
  boundary?: number;
  logarithmicX: boolean;
  tooltip: (point: Point) => string;
};

function renderCanvas(panel: HTMLElement, options: CanvasOptions): DestroyChart {
  const heading = document.createElement('div');
  heading.className = 'chart-heading';
  const title = document.createElement('strong');
  title.textContent = options.label;
  heading.append(title);
  if (options.average !== undefined) {
    const average = document.createElement('span');
    average.textContent = `Published average ${number.format(options.average)} ${options.yLabel}`;
    heading.append(average);
  }
  const frame = document.createElement('div');
  frame.className = 'chart-frame';
  const canvas = document.createElement('canvas');
  canvas.setAttribute('role', 'img');
  canvas.setAttribute('aria-label', `${options.label}: ${options.yLabel} by ${options.xLabel}`);
  const tooltip = document.createElement('div');
  tooltip.className = 'chart-tooltip';
  tooltip.hidden = true;
  const xLabel = document.createElement('div');
  xLabel.className = 'chart-x-label';
  xLabel.textContent = options.xLabel;
  const yLabel = document.createElement('div');
  yLabel.className = 'chart-y-label';
  yLabel.textContent = options.yLabel;
  frame.append(canvas, tooltip, xLabel, yLabel);
  panel.replaceChildren(heading, frame);

  let plot = { left: 64, top: 16, width: 1, height: 1 };
  let xValues: number[] = [];
  const draw = () => {
    const width = Math.max(frame.clientWidth, 320);
    const height = Math.max(frame.clientHeight, 300);
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const context = canvas.getContext('2d');
    if (!context) return;
    context.scale(ratio, ratio);
    const right = 18;
    const bottom = 43;
    plot = { left: 64, top: 16, width: width - 64 - right, height: height - 16 - bottom };
    const transformedX = options.points.map((point) => options.logarithmicX ? Math.log10(Math.max(point.x, 1e-6)) : point.x);
    const minX = options.logarithmicX ? Math.min(...transformedX) : 0;
    const maxX = Math.max(...transformedX);
    const maxY = Math.max(...options.points.map((point) => point.y), options.average ?? 0, 1);
    const xScale = (value: number) => plot.left + (value - minX) / Math.max(maxX - minX, Number.EPSILON) * plot.width;
    const yScale = (value: number) => plot.top + plot.height - value / maxY * plot.height;
    xValues = transformedX.map(xScale);

    context.clearRect(0, 0, width, height);
    context.font = '10px JetBrains Mono, monospace';
    context.textAlign = 'right';
    context.textBaseline = 'middle';
    for (let index = 0; index <= 4; index += 1) {
      const value = maxY * index / 4;
      const y = yScale(value);
      context.strokeStyle = 'rgba(15, 22, 32, .10)';
      context.beginPath();
      context.moveTo(plot.left, y);
      context.lineTo(plot.left + plot.width, y);
      context.stroke();
      context.fillStyle = '#626d7c';
      context.fillText(number.format(value), plot.left - 9, y);
    }
    context.textAlign = 'center';
    context.textBaseline = 'top';
    for (let index = 0; index <= 4; index += 1) {
      const transformed = minX + (maxX - minX) * index / 4;
      const label = options.logarithmicX ? 10 ** transformed : transformed;
      context.fillStyle = '#626d7c';
      context.fillText(number.format(label), xScale(transformed), plot.top + plot.height + 10);
    }
    if (options.average !== undefined) {
      context.setLineDash([5, 5]);
      context.strokeStyle = '#8b94a3';
      context.beginPath();
      context.moveTo(plot.left, yScale(options.average));
      context.lineTo(plot.left + plot.width, yScale(options.average));
      context.stroke();
      context.setLineDash([]);
    }
    if (options.boundary !== undefined && options.boundary < options.points.at(-1)!.x) {
      const transformed = options.logarithmicX ? Math.log10(options.boundary) : options.boundary;
      const x = xScale(transformed);
      context.setLineDash([2, 4]);
      context.strokeStyle = '#a45632';
      context.beginPath();
      context.moveTo(x, plot.top);
      context.lineTo(x, plot.top + plot.height);
      context.stroke();
      context.setLineDash([]);
      context.fillStyle = '#a45632';
      context.textAlign = 'right';
      context.textBaseline = 'top';
      context.fillText('drain', x - 5, plot.top + 4);
    }
    context.strokeStyle = '#a45632';
    context.lineWidth = 1.75;
    context.beginPath();
    options.points.forEach((point, index) => {
      const x = xValues[index];
      const y = yScale(point.y);
      if (index === 0) context.moveTo(x, y);
      else context.lineTo(x, y);
    });
    context.stroke();
  };

  const move = (event: PointerEvent) => {
    const bounds = canvas.getBoundingClientRect();
    const x = event.clientX - bounds.left;
    if (x < plot.left || x > plot.left + plot.width) {
      tooltip.hidden = true;
      return;
    }
    let closest = 0;
    for (let index = 1; index < xValues.length; index += 1) {
      if (Math.abs(xValues[index] - x) < Math.abs(xValues[closest] - x)) closest = index;
    }
    tooltip.textContent = options.tooltip(options.points[closest]);
    tooltip.hidden = false;
    tooltip.style.left = `${Math.min(Math.max(xValues[closest], 90), frame.clientWidth - 90)}px`;
    tooltip.style.top = `${Math.max(event.clientY - bounds.top - 38, 4)}px`;
  };
  const leave = () => tooltip.hidden = true;
  canvas.addEventListener('pointermove', move);
  canvas.addEventListener('pointerleave', leave);
  const observer = new ResizeObserver(draw);
  observer.observe(frame);
  draw();
  return () => {
    observer.disconnect();
    canvas.removeEventListener('pointermove', move);
    canvas.removeEventListener('pointerleave', leave);
  };
}

function status(message: string) {
  const element = document.createElement('p');
  element.className = 'chart-status';
  element.setAttribute('role', 'status');
  element.textContent = message;
  return element;
}

function formatElapsed(seconds: number) {
  if (seconds < 60) return `${number.format(seconds)}s`;
  return `${Math.floor(seconds / 60)}m ${number.format(seconds % 60)}s`;
}

function prefersReducedMotion() {
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}
