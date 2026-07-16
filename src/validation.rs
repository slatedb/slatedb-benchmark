use crate::model::{
    ApplicationPerformance, HistogramsFile, LatencySummary, MetricSeriesValue, MetricValueType,
    ResultRecord, RunManifest, TimeseriesFile,
};
use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hdrhistogram::serialization::Deserializer;
use hdrhistogram::Histogram;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

pub(crate) fn validate_result(
    result: &ResultRecord,
    histograms: &HistogramsFile,
    timeseries: &TimeseriesFile,
) -> Result<()> {
    validate_contract_values(result, histograms, timeseries)?;
    validate_invariants(result, histograms, timeseries)?;
    reject_secrets(&serde_json::to_value(result)?, "result")?;
    reject_secrets(&serde_json::to_value(histograms)?, "histograms")?;
    reject_secrets(&serde_json::to_value(timeseries)?, "timeseries")?;
    Ok(())
}

pub(crate) fn validate_run(run: &RunManifest) -> Result<()> {
    if run.status != "ok" {
        bail!("run status must be ok");
    }
    validate_mode(&run.mode)?;
    validate_timestamp(&run.started_at, "run start")?;
    validate_timestamp(&run.finished_at, "run finish")?;
    if run.object_store_baselines.is_empty() {
        bail!("run contains no object-store baselines");
    }
    if run.results.is_empty() {
        bail!("run contains no results");
    }
    let value = serde_json::to_value(run)?;
    reject_secrets(&value, "run")
}

pub fn validate_output(output: &Path) -> Result<()> {
    let run_path = output.join("run.json");
    let run: RunManifest = read_json(&run_path)?;
    validate_run(&run)?;
    let manifest_paths = run.results.iter().collect::<BTreeSet<_>>();
    if manifest_paths.len() != run.results.len() {
        bail!("run manifest contains duplicate result paths");
    }
    let result_paths = find_named(output, "result.json")?;
    if result_paths.len() != run.results.len() {
        bail!(
            "run manifest lists {} results but output contains {}",
            run.results.len(),
            result_paths.len()
        );
    }
    validate_result_bundle(output, &run.results)
}

pub(crate) fn validate_result_bundle(output: &Path, results: &[String]) -> Result<()> {
    for relative in results {
        let result_path = output.join(relative);
        if !result_path.is_file() {
            bail!(
                "run manifest result {} does not exist",
                result_path.display()
            );
        }
        let directory = result_path.parent().context("result has no parent")?;
        let result: ResultRecord = read_json(&result_path)?;
        let expected_path = PathBuf::from("results")
            .join(&result.identity.slate_version)
            .join(&result.identity.suite)
            .join(&result.identity.workload)
            .join(&result.identity.variant)
            .join("result.json");
        if Path::new(relative) != expected_path {
            bail!(
                "result identity resolves to {} but manifest lists {}",
                expected_path.display(),
                relative
            );
        }
        let histograms: HistogramsFile = read_json(&directory.join("histograms.json"))?;
        let timeseries: TimeseriesFile = read_json(&directory.join("timeseries.json"))?;
        validate_result(&result, &histograms, &timeseries)
            .with_context(|| format!("validating {}", result_path.display()))?;
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path).with_context(|| format!("reading {}", path.display()))?)
        .with_context(|| format!("parsing {}", path.display()))
}

fn validate_contract_values(
    result: &ResultRecord,
    histograms: &HistogramsFile,
    timeseries: &TimeseriesFile,
) -> Result<()> {
    validate_mode(&result.identity.mode)?;
    validate_timestamp(&result.identity.timestamp, "result timestamp")?;
    validate_timestamp(
        &result.object_store_baseline.measured_at,
        "object-store baseline timestamp",
    )?;
    if result.identity.suite.is_empty()
        || result.identity.workload.is_empty()
        || result.identity.variant.is_empty()
    {
        bail!("result identity contains an empty suite, workload, or variant");
    }
    if result.environment.cpu_cores == 0 || result.environment.ram_bytes == 0 {
        bail!("result environment has no CPU cores or memory");
    }
    if result.configuration.clients == 0
        || result.configuration.key_bytes == 0
        || result.configuration.value_bytes == 0
    {
        bail!("result configuration has zero clients, key bytes, or value bytes");
    }
    if !(result.configuration.value_compression_ratio.is_finite()
        && 0.0 < result.configuration.value_compression_ratio
        && result.configuration.value_compression_ratio <= 1.0)
    {
        bail!("result configuration has an invalid value compression ratio");
    }
    for (name, value) in [
        (
            "object-store upload throughput",
            result.object_store_baseline.upload_mib_per_second,
        ),
        (
            "object-store download throughput",
            result.object_store_baseline.download_mib_per_second,
        ),
        (
            "application payload throughput",
            result.application.payload_mib_per_second,
        ),
        ("average CPU", result.resources.average_cpu_percent),
        ("peak CPU", result.resources.peak_cpu_percent),
    ] {
        validate_nonnegative_finite(value, name)?;
    }
    if result.source_files.histograms != "histograms.json"
        || result.source_files.timeseries != "timeseries.json"
    {
        bail!("result source file names do not match the workload bundle");
    }
    if histograms.encoding != "hdrhistogram-v2-deflate-base64" || histograms.significant_digits != 3
    {
        bail!("histogram encoding metadata is unsupported");
    }
    for (name, histogram) in &histograms.histograms {
        if histogram.unit != "microseconds" {
            bail!("histogram {name} has unsupported unit {}", histogram.unit);
        }
    }
    if timeseries.interval_ns != 1_000_000_000 {
        bail!("time-series interval must be one second");
    }
    for sample in &timeseries.samples {
        validate_nonnegative_finite(sample.cpu_percent, "time-series CPU")?;
    }
    Ok(())
}

fn validate_mode(mode: &str) -> Result<()> {
    if !matches!(mode, "published" | "smoke") {
        bail!("unsupported benchmark mode {mode}");
    }
    Ok(())
}

fn validate_timestamp(timestamp: &str, name: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .with_context(|| format!("{name} is not an RFC 3339 timestamp"))?;
    Ok(())
}

fn validate_nonnegative_finite(value: f64, name: &str) -> Result<()> {
    if !value.is_finite() || value < 0.0 {
        bail!("{name} must be a finite nonnegative number");
    }
    Ok(())
}

fn validate_invariants(
    result: &ResultRecord,
    histograms: &HistogramsFile,
    timeseries: &TimeseriesFile,
) -> Result<()> {
    let return_count = histograms
        .histograms
        .get("return")
        .map(|histogram| histogram.count)
        .unwrap_or(0);
    let background_returns =
        background_return_count(&result.application.return_latency_by_operation);
    let expected_returns =
        validate_return_histogram_count(&result.application, return_count, background_returns)?;
    let expected_headline_returns = expected_returns
        .checked_sub(background_returns)
        .context("background operations exceed returned operations")?;
    if result.application.return_latency.count != return_count {
        bail!("return latency summary does not match encoded histogram count");
    }
    validate_histogram_summary(histograms, "return", &result.application.return_latency)?;
    validate_histogram_summary(
        histograms,
        "object_store/put",
        &result.object_store_baseline.put_latency,
    )?;
    validate_histogram_summary(
        histograms,
        "object_store/get",
        &result.object_store_baseline.get_latency,
    )?;
    for (operation, summary) in &result.application.return_latency_by_operation {
        validate_histogram_summary(histograms, &format!("return/{operation}"), summary)?;
    }
    let operation_returns = result
        .application
        .return_latency_by_operation
        .values()
        .map(|summary| summary.count)
        .sum::<u64>();
    if operation_returns != expected_returns {
        bail!(
            "per-operation return histograms contain {operation_returns} observations for {expected_returns} returned operations"
        );
    }
    let histogram_apis = histograms
        .histograms
        .keys()
        .filter_map(|name| name.strip_prefix("api/"))
        .collect::<BTreeSet<_>>();
    let result_apis = result
        .application
        .api_latency
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if histogram_apis != result_apis {
        bail!("API latency summaries do not match encoded histograms");
    }
    for (api, summary) in &result.application.api_latency {
        validate_histogram_summary(histograms, &format!("api/{api}"), summary)?;
    }
    for (name, summary) in [
        ("batch", result.application.batch_latency.as_ref()),
        ("durability_lag", result.durability.lag.as_ref()),
    ] {
        if let Some(summary) = summary {
            validate_histogram_summary(histograms, name, summary)?;
        }
    }
    validate_application_windows(
        result,
        histograms,
        timeseries,
        expected_returns,
        expected_headline_returns,
    )?;
    validate_durability_windows(result, timeseries)?;
    if result.application.errors != 0 {
        bail!(
            "result contains {} operation errors",
            result.application.errors
        );
    }
    if let Some(last_write) = result.durability.last_measured_sequence {
        let final_durable = result
            .durability
            .final_durable_sequence
            .context("write workload has no final durable sequence")?;
        if final_durable < last_write {
            bail!("durable frontier {final_durable} does not cover write {last_write}");
        }
    }
    if timeseries.samples.len() < 2 {
        bail!("resource samples do not span the measurement window");
    }
    let mut metric_identities = BTreeSet::new();
    for metric in &timeseries.slatedb_metrics {
        if !metric_identities.insert((metric.name.as_str(), serde_json::to_string(&metric.labels)?))
        {
            bail!("duplicate SlateDB metric series {}", metric.name);
        }
        if metric.values.len() != timeseries.samples.len() {
            bail!(
                "SlateDB metric {} has {} values for {} samples",
                metric.name,
                metric.values.len(),
                timeseries.samples.len()
            );
        }
        match metric.value_type {
            MetricValueType::Histogram => {
                let boundaries = metric
                    .boundaries
                    .as_ref()
                    .context("histogram metric has no bucket boundaries")?;
                for value in metric.values.iter().flatten() {
                    let MetricSeriesValue::Histogram(value) = value else {
                        bail!("histogram metric {} contains a scalar value", metric.name);
                    };
                    if value.bucket_counts.len() != boundaries.len().saturating_add(1) {
                        bail!(
                            "histogram metric {} has the wrong number of bucket counts",
                            metric.name
                        );
                    }
                }
            }
            MetricValueType::Counter | MetricValueType::Gauge | MetricValueType::UpDownCounter => {
                if metric.boundaries.is_some() {
                    bail!("scalar metric {} has histogram boundaries", metric.name);
                }
                for value in metric.values.iter().flatten() {
                    let MetricSeriesValue::Scalar(value) = value else {
                        bail!("scalar metric {} contains a histogram value", metric.name);
                    };
                    if metric.value_type == MetricValueType::Counter && value.as_u64().is_none() {
                        bail!("counter metric {} contains a negative value", metric.name);
                    }
                    if metric.value_type != MetricValueType::Counter
                        && value.as_i64().is_none()
                        && value.as_u64().is_none()
                    {
                        bail!("scalar metric {} contains a non-integer value", metric.name);
                    }
                }
            }
        }
    }
    let last_offset = timeseries
        .samples
        .last()
        .map(|sample| sample.offset_ns)
        .unwrap_or(0);
    if last_offset < result.configuration.measurement_ns {
        bail!("resource samples end before the measurement window");
    }
    if result.initial_state.lsm_digest_sha256.is_empty() {
        bail!("initial LSM digest is missing");
    }
    if result.initial_state.lsm_digest_sha256.len() != 64
        || !result
            .initial_state
            .lsm_digest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("initial LSM digest is not a SHA-256 hex digest");
    }
    Ok(())
}

fn validate_application_windows(
    result: &ResultRecord,
    histograms: &HistogramsFile,
    timeseries: &TimeseriesFile,
    expected_returns: u64,
    expected_headline_returns: u64,
) -> Result<()> {
    if timeseries.application_windows.is_empty() {
        bail!("application time series contains no windows");
    }
    validate_window_layout(
        timeseries
            .application_windows
            .iter()
            .map(|window| (window.start_offset_ns, window.duration_ns)),
        "application",
    )?;
    let completed = timeseries
        .application_windows
        .iter()
        .map(|window| window.completed_operations)
        .sum::<u64>();
    let successful = timeseries
        .application_windows
        .iter()
        .map(|window| window.successful_operations)
        .sum::<u64>();
    let errors = timeseries
        .application_windows
        .iter()
        .map(|window| window.errors)
        .sum::<u64>();
    let return_windows = timeseries
        .application_windows
        .iter()
        .filter_map(|window| window.return_latency.as_ref())
        .map(|latency| latency.count)
        .sum::<u64>();
    if completed != expected_returns || return_windows != expected_headline_returns {
        bail!(
            "application windows contain {completed} completions and {return_windows} headline return latencies for {expected_returns} returned operations and {expected_headline_returns} headline operations"
        );
    }
    if successful != result.application.successful_operations {
        bail!(
            "application windows contain {successful} successful operations but result reports {}",
            result.application.successful_operations
        );
    }
    if errors != result.application.errors {
        bail!(
            "application windows contain {errors} errors but result reports {}",
            result.application.errors
        );
    }

    for window in &timeseries.application_windows {
        let returns = window
            .return_latency
            .as_ref()
            .map(|latency| latency.count)
            .unwrap_or(0);
        let operation_returns = window
            .return_latency_by_operation
            .values()
            .map(|latency| latency.count)
            .sum::<u64>();
        if operation_returns != window.completed_operations {
            bail!(
                "application window contains {operation_returns} per-operation return latencies for {} completed operations",
                window.completed_operations
            );
        }
        let background_returns = background_return_count(&window.return_latency_by_operation);
        let expected_headline = window
            .completed_operations
            .checked_sub(background_returns)
            .context("application window has more background returns than completions")?;
        if returns != expected_headline {
            bail!(
                "application window contains {returns} headline return latencies for {expected_headline} foreground operations"
            );
        }
        if window.successful_operations > window.completed_operations {
            bail!("application window has more successful operations than completions");
        }
    }

    for name in ["return", "batch"] {
        let expected = histograms
            .histograms
            .get(name)
            .map(|histogram| histogram.count)
            .unwrap_or(0);
        let actual = timeseries
            .application_windows
            .iter()
            .filter_map(|window| match name {
                "return" => window.return_latency.as_ref(),
                "batch" => window.batch_latency.as_ref(),
                _ => None,
            })
            .map(|latency| latency.count)
            .sum::<u64>();
        if actual != expected {
            bail!("application window {name} count {actual} does not match histogram count {expected}");
        }
    }
    for (api, summary) in &result.application.api_latency {
        let actual = timeseries
            .application_windows
            .iter()
            .filter_map(|window| window.api_latency.get(api))
            .map(|latency| latency.count)
            .sum::<u64>();
        if actual != summary.count {
            bail!(
                "application window api/{api} count {actual} does not match histogram count {}",
                summary.count
            );
        }
    }
    for api in timeseries
        .application_windows
        .iter()
        .flat_map(|window| window.api_latency.keys())
    {
        if !result.application.api_latency.contains_key(api) {
            bail!("application window contains unknown API latency {api}");
        }
    }
    Ok(())
}

fn validate_durability_windows(result: &ResultRecord, timeseries: &TimeseriesFile) -> Result<()> {
    let Some(windows) = &timeseries.durability_windows else {
        if result.durability.lag.is_some() {
            bail!("durability lag has no time-series windows");
        }
        return Ok(());
    };
    validate_window_layout(
        windows
            .iter()
            .map(|window| (window.start_offset_ns, window.duration_ns)),
        "durability",
    )?;
    let count = windows
        .iter()
        .filter_map(|window| window.durability_lag.as_ref())
        .map(|latency| latency.count)
        .sum::<u64>();
    let expected = result
        .durability
        .lag
        .as_ref()
        .map(|latency| latency.count)
        .unwrap_or(0);
    for window in windows {
        let latency_count = window
            .durability_lag
            .as_ref()
            .map(|latency| latency.count)
            .unwrap_or(0);
        if latency_count != window.writes_made_durable {
            bail!(
                "durability window contains {latency_count} lag observations for {} durable writes",
                window.writes_made_durable
            );
        }
    }
    if count != expected {
        bail!("durability windows contain {count} writes but result reports {expected}");
    }
    Ok(())
}

fn validate_window_layout(windows: impl Iterator<Item = (u64, u64)>, name: &str) -> Result<()> {
    let mut expected_start = 0_u64;
    for (start, duration) in windows {
        if duration == 0 {
            bail!("{name} time-series window has zero duration");
        }
        if start != expected_start {
            bail!("{name} time-series window starts at {start}, expected {expected_start}");
        }
        expected_start = start.saturating_add(duration);
    }
    Ok(())
}

fn validate_return_histogram_count(
    application: &ApplicationPerformance,
    return_count: u64,
    background_return_count: u64,
) -> Result<u64> {
    let expected_returns = application.total_operations;
    let expected_headline_returns = expected_returns
        .checked_sub(background_return_count)
        .context("background operations exceed returned operations")?;
    if return_count != expected_headline_returns {
        bail!(
            "return histogram count {return_count} does not match headline operation count {expected_headline_returns}"
        );
    }
    Ok(expected_returns)
}

fn background_return_count(summaries: &BTreeMap<String, LatencySummary>) -> u64 {
    summaries
        .get("writer-update")
        .map(|summary| summary.count)
        .unwrap_or(0)
}

fn validate_histogram_summary(
    histograms: &HistogramsFile,
    name: &str,
    expected: &crate::model::LatencySummary,
) -> Result<()> {
    let encoded = histograms
        .histograms
        .get(name)
        .with_context(|| format!("missing encoded {name} histogram"))?;
    let bytes = STANDARD
        .decode(&encoded.data)
        .with_context(|| format!("decoding {name} histogram"))?;
    let histogram: Histogram<u64> = Deserializer::new()
        .deserialize(&mut Cursor::new(bytes))
        .with_context(|| format!("deserializing {name} histogram"))?;
    let actual = crate::model::LatencySummary {
        count: histogram.len(),
        p50_ns: histogram.value_at_quantile(0.50).saturating_mul(1_000),
        p95_ns: histogram.value_at_quantile(0.95).saturating_mul(1_000),
        p99_ns: histogram.value_at_quantile(0.99).saturating_mul(1_000),
        p999_ns: histogram.value_at_quantile(0.999).saturating_mul(1_000),
        max_ns: histogram.max().saturating_mul(1_000),
    };
    if &actual != expected {
        bail!("{name} summary does not match its encoded histogram");
    }
    Ok(())
}

fn reject_secrets(value: &Value, path: &str) -> Result<()> {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let lower = key.to_ascii_lowercase();
                if ["secret", "credential", "access_key", "signed_url", "token"]
                    .iter()
                    .any(|needle| lower.contains(needle))
                {
                    bail!("secret-like field rejected at {path}.{key}");
                }
                reject_secrets(value, &format!("{path}.{key}"))?;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                reject_secrets(value, &format!("{path}[{index}]"))?;
            }
        }
        Value::String(value) => {
            let lower = value.to_ascii_lowercase();
            if lower.contains("x-amz-signature=") || lower.contains("aws_secret_access_key") {
                bail!("secret-like value rejected at {path}");
            }
        }
        _ => {}
    }
    Ok(())
}

fn find_named(root: &Path, name: &str) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).with_context(|| format!("reading {}", path.display()))? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if entry.file_name() == name {
                found.push(entry.path());
            }
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::validate_return_histogram_count;
    use crate::model::{ApplicationPerformance, LatencySummary};

    #[test]
    fn background_operations_are_excluded_from_headline_return_count() {
        let application = ApplicationPerformance {
            total_operations: 10,
            ..Default::default()
        };

        assert_eq!(
            validate_return_histogram_count(&application, 8, 2)
                .expect("valid background return count"),
            10
        );
    }

    #[test]
    fn artifact_models_reject_unknown_fields() {
        let error = serde_json::from_value::<LatencySummary>(serde_json::json!({
            "count": 1,
            "p50_ns": 1,
            "p95_ns": 1,
            "p99_ns": 1,
            "p999_ns": 1,
            "max_ns": 1,
            "unexpected": true
        }))
        .expect_err("unknown artifact field should fail");

        assert!(error.to_string().contains("unknown field"));
    }
}
