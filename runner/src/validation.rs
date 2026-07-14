use crate::model::{
    HistogramsFile, MetricSeriesValue, MetricValueType, ResultRecord, RunManifest, TimeseriesFile,
};
use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hdrhistogram::serialization::Deserializer;
use hdrhistogram::Histogram;
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

pub fn validate_result(
    result: &ResultRecord,
    histograms: &HistogramsFile,
    timeseries: &TimeseriesFile,
    schema_dir: &Path,
) -> Result<()> {
    validate_schema(
        &serde_json::to_value(result)?,
        &schema_dir.join("result-v1.json"),
    )?;
    validate_schema(
        &serde_json::to_value(histograms)?,
        &schema_dir.join("histograms-v1.json"),
    )?;
    validate_schema(
        &serde_json::to_value(timeseries)?,
        &schema_dir.join("timeseries-v1.json"),
    )?;
    validate_invariants(result, histograms, timeseries)?;
    reject_secrets(&serde_json::to_value(result)?, "result")?;
    reject_secrets(&serde_json::to_value(histograms)?, "histograms")?;
    reject_secrets(&serde_json::to_value(timeseries)?, "timeseries")?;
    Ok(())
}

pub fn validate_run(run: &RunManifest, schema_dir: &Path) -> Result<()> {
    let value = serde_json::to_value(run)?;
    validate_schema(&value, &schema_dir.join("run-v1.json"))?;
    reject_secrets(&value, "run")
}

pub fn validate_output(output: &Path) -> Result<()> {
    let schema_dir = Path::new("schema");
    let run_path = output.join("run.json");
    let run: RunManifest = serde_json::from_slice(
        &fs::read(&run_path).with_context(|| format!("reading {}", run_path.display()))?,
    )?;
    validate_run(&run, schema_dir)?;
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
    for relative in &run.results {
        let result_path = output.join(relative);
        if !result_path.is_file() {
            bail!(
                "run manifest result {} does not exist",
                result_path.display()
            );
        }
        let directory = result_path.parent().context("result has no parent")?;
        let result: ResultRecord = serde_json::from_slice(&fs::read(&result_path)?)?;
        let histograms: HistogramsFile =
            serde_json::from_slice(&fs::read(directory.join("histograms.json"))?)?;
        let timeseries: TimeseriesFile =
            serde_json::from_slice(&fs::read(directory.join("timeseries.json"))?)?;
        validate_result(&result, &histograms, &timeseries, schema_dir)
            .with_context(|| format!("validating {}", result_path.display()))?;
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
    let dropped = if result.application.dropped_ops_per_second.is_some() {
        result
            .application
            .total_operations
            .saturating_sub(result.application.return_latency.count)
    } else {
        0
    };
    let expected_returns = result.application.total_operations.saturating_sub(dropped);
    if return_count != expected_returns {
        bail!(
            "return histogram count {return_count} does not match returned operation count {expected_returns}"
        );
    }
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
    for (name, summary) in [
        ("response", result.application.response_latency.as_ref()),
        (
            "scheduling_delay",
            result.application.scheduling_delay.as_ref(),
        ),
        ("batch", result.application.batch_latency.as_ref()),
        ("durability_lag", result.durability.lag.as_ref()),
    ] {
        if let Some(summary) = summary {
            validate_histogram_summary(histograms, name, summary)?;
        }
    }
    if result.application.offered_ops_per_second.is_some() {
        for name in ["response", "scheduling_delay"] {
            let count = histograms
                .histograms
                .get(name)
                .map(|histogram| histogram.count)
                .unwrap_or(0);
            if count != expected_returns {
                bail!("{name} histogram count {count} does not match returned operation count {expected_returns}");
            }
        }
    }
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
    if let (Some(target), Some(offered)) = (
        result.configuration.target_rate,
        result.application.offered_ops_per_second,
    ) {
        if offered < target as f64 * 0.95 {
            bail!("scheduler offered only {offered:.2} ops/s for target {target}");
        }
        if result
            .application
            .scheduling_delay
            .as_ref()
            .is_none_or(|latency| latency.p99_ns > 1_000_000_000)
        {
            bail!("open-loop scheduler p99 delay exceeds its one-second queue bound");
        }
    }
    Ok(())
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

fn validate_schema(instance: &Value, schema_path: &Path) -> Result<()> {
    let schema: Value = serde_json::from_slice(
        &fs::read(schema_path)
            .with_context(|| format!("reading schema {}", schema_path.display()))?,
    )?;
    let validator = jsonschema::validator_for(&schema)
        .with_context(|| format!("compiling schema {}", schema_path.display()))?;
    let errors = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        bail!("schema validation failed: {}", errors.join("; "));
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
