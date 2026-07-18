# Workload Row Charts

Each workload metric row opens a chart in a detail cell directly below it.
The tables remain the primary result, and chart data stays out of the initial
HTML. Preparation pages are outside this feature.

## Interaction

Put a button in the row header and let clicks anywhere in the row activate it.
The button supports Enter and Space and uses `aria-expanded` and
`aria-controls`.

The chart belongs in a second `<tr>` whose only cell spans the table. Opening
another row in the same table closes the first. Clicking the open row closes
it. The cell expands while its panel slides in from the right. Disable the
animation under `prefers-reduced-motion`.

Show loading and retry states inside the detail cell. If JavaScript fails, the
summary table remains intact.

## Axes

| Table or row | X-axis | Y-axis |
| --- | --- | --- |
| Application operations | Elapsed seconds | Calls/s |
| Application throughput | Elapsed seconds | MiB/s |
| Application latency | Elapsed seconds | Latency (ms) |
| Object-store requests | Elapsed seconds | Requests/s |
| Object-store throughput | Elapsed seconds | MiB/s |
| Process CPU utilization | Elapsed seconds | CPU cores |
| Process RSS | Elapsed seconds | GiB |
| Machine CPU | Elapsed seconds | CPU (%) |
| Machine RSS | Elapsed seconds | GiB |
| Machine network receive/send | Elapsed seconds | MiB/s |
| Machine disk read/write | Elapsed seconds | MiB/s |
| Machine disk read/write operations | Elapsed seconds | Operations/s |

Rate charts use complete client buckets and stop before durability drain.
Latency, process, and machine charts continue through drain and mark its
start. Draw the row's published average as a reference line.

The average may differ from the arithmetic mean of plotted points. The table
divides totals by the full recorded interval, including partial boundary
intervals, while rate percentiles and charts use complete buckets.

Application latency plots the average latency of calls completed in each
sampling window. Windows with no calls for that API appear as gaps. The
aggregate HDR histogram remains in the sidecar so the published table can be
validated. Time-chart tooltips show elapsed time, value, and whether the point
falls in measurement or drain.

## Data contract

Publish one sidecar per workload:

```text
results/<version>/workload/<name>/
  result.json
  series.json
```

`result.json` authenticates the sidecar:

```json
{
  "series": {
    "file": "series.json",
    "sha256": "0123456789abcdef..."
  }
}
```

`series.json` uses aligned arrays:

```json
{
  "rate_elapsed_ns": [1000123000, 2000876000],
  "rate_duration_ns": [1000123000, 1000753000],
  "latency_elapsed_ns": [1000123000, 2000876000, 2260000000],
  "latency_duration_ns": [1000123000, 1000753000, 259124000],
  "resource_elapsed_ns": [1000123000, 2000876000, 3001142000],
  "resource_duration_ns": [1000123000, 1000753000, 1000266000],
  "application": {
    "operations_per_second": {
      "get": [50120.8, 49881.4]
    },
    "bytes_per_second": {
      "get": [22053152.0, 21947716.0]
    },
    "latency_ns": {
      "get": [72000.0, 68000.0, null]
    },
    "latency_histograms": {
      "get": {
        "upper_bound_ns": [80000, 81000, 82000],
        "counts": [18, 42, 31]
      }
    }
  }
}
```

The complete sidecar contains these series:

| Area | Series |
| --- | --- |
| Application | Calls/s and logical bytes/s by API |
| Application latency | Window averages and aggregate HDR data by API |
| Object store | Requests/s and combined body bytes/s by HTTP method |
| Process | CPU cores and RSS bytes |
| Machine | CPU percent, RSS bytes, network bytes/s, disk bytes/s, disk ops/s |

Application rate and object-store arrays match `rate_elapsed_ns`. Application
latency arrays match `latency_elapsed_ns`. Process and machine arrays match
`resource_elapsed_ns`. Missing APIs and HTTP methods contribute zero to rate
arrays. A latency window with no completed call uses `null`. Store nanoseconds
and bytes; the website converts display units.

Keep every complete sampling bucket and populated HDR bucket. Do not reduce the
sample count. GitHub Pages compression and caching limit transfer cost, and the
browser fetches the sidecar after page load or when a row first opens.

## Runner

The sampler already keeps the required windows until it creates the summaries.
Before dropping them:

1. Add serializable series types in `src/model.rs`.
2. Record cumulative elapsed time for each sample in `src/system.rs`.
3. Export aligned, zero-filled arrays from application, object-store, process,
   and machine windows.
4. Use one rate helper for the summaries and sidecar.
5. Export per-window average latency and populated aggregate HDR bounds and
   counts from `src/histogram.rs`.
6. Add the sidecar reference to `WorkloadResult`.
7. Validate and write `series.json` before creating `result.json`.

`result.json` remains the completion marker:

```text
run workload
  -> validate summaries and series
  -> write sessions/<session>/<task>/series.json
  -> create sessions/<session>/<task>/result.json
```

On a resumed job, download the referenced sidecar, verify its digest, and write
both files to the local output directory. A missing or invalid sidecar makes
the stored result invalid; the runner must not emit a partial artifact.

## Validation

Add `schema/series.json` and publish it with the existing schemas. Semantic
validation checks:

- Elapsed offsets increase and describe complete samples.
- Each elapsed array matches its duration array and has at least one sample.
- Each series matches its timeline and contains finite, nonnegative values.
- Series keys match the rows in `result.json`.
- Throughput exists only for rows that transferred bytes.
- Latency arrays match their timeline and contain a value for every published
  latency row.
- Histogram bounds increase, counts are positive, and counts sum to latency
  totals.
- Rate summaries match the complete sample arrays.
- Latency summaries match the exported histogram within HDR precision.
- The sidecar digest matches `result.json`.

Keep the existing average-rate check against total operations and the full
recorded interval.

## Bundling and publication

The workload artifact already uploads its output directory. Complete the
pipeline by changing:

- `scripts/bundle.py` to copy sidecars and add their digests to `run.json`.
- `schema/run.json` to require ten workload sidecars.
- `scripts/publish.sh` to reject missing or modified sidecars.
- `scripts/smoke.sh` to require one sidecar per workload.
- `scripts/fixtures.sh` to retain sidecars with scaled results.

Preparation results do not receive sidecars.

## Website

Give each row a stable chart selector:

```ts
{
  label,
  values,
  chart: {
    source: 'application.operations_per_second',
    key: label,
    yUnit: 'calls/s',
  },
}
```

`MetricTable.astro` renders the button and detail row. A page-level controller
keeps one shared promise for `series.json`. It starts the request about one
second after the window `load` event unless the user has enabled reduced data
usage. A click starts the same request immediately or waits for the in-flight
request. Later rows reuse the parsed result.

Use one canvas time-series renderer. Keep labels and tooltips in HTML, resize
with `ResizeObserver`, and destroy the chart when its row closes.

Expose the sidecar through:

```text
/raw/<version>/workload/<name>/series.json
```

Update the website result types, `rawResultFiles()`, and the static raw route.
Return `application/json` with the existing public cache policy. Never put the
sidecar in Astro props, inline scripts, or HTML attributes.

CSS must cover focus, open rows, loading and errors, animation, chart size,
narrow screens, and reduced motion. Keep the detail cell inside the table's
horizontal scroll container.

## Tests

Rust tests cover zero filling, variable sample durations, missed ticks,
histogram export, digest checks, and summary reconciliation. Bundle and
publication tests reject missing, modified, or unexpected sidecars. Scaled
fixtures pass the result, run, and series schemas.

A browser test verifies:

1. Without a click, the page waits for the `load` event and delay before
   requesting `series.json`.
2. Reduced-data mode skips the background request.
3. Mouse, Enter, and Space open the correct chart.
4. Opening another row closes the first.
5. The page fetches the sidecar once.
6. Time charts mark measurement and drain.
7. Latency charts use elapsed time and include drain samples.
8. Desktop and mobile layouts work.
9. Tables survive chart-loading failure.

## Implementation order

Define and export the series first. Add recovery and publication next, then the
raw route and row UI. Update `BENCHMARKS.md` and `DESIGN.md` after the code
matches this contract.
