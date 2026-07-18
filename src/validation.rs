use crate::model::{
    ApplicationMetrics, DistributionSummary, Environment, LatencySummary, MachineStatistics,
    ObjectStoreMetrics, PreparationResult, ProcessStatistics, RateSummary, ResultConfiguration,
    SourceIdentity, ThroughputSummary, WorkloadResult, WorkloadSeries,
};
use anyhow::{bail, ensure, Result};
use std::collections::BTreeSet;

pub fn validate_preparation_result(result: &PreparationResult) -> Result<()> {
    ensure!(result.status == "ok", "preparation status must be ok");
    ensure!(
        result.task.is_preparation(),
        "preparation result has a workload task"
    );
    ensure!(!result.golden_id.is_empty(), "golden ID is empty");
    validate_timestamp(&result.timestamp)?;
    validate_source(&result.source)?;
    validate_configuration(&result.configuration, result.task)?;
    validate_environment(&result.environment)?;
    validate_recorded_metrics(
        result.recorded_interval_ns,
        &result.application,
        &result.object_store,
        &result.process,
        &result.machine,
    )?;
    ensure!(
        result.dataset.record_count > 0,
        "prepared dataset has no records"
    );
    ensure!(
        result.dataset.key_bytes > 0,
        "prepared keys have zero bytes"
    );
    ensure!(
        result.dataset.value_bytes > 0,
        "prepared values have zero bytes"
    );
    ensure!(
        result.dataset.logical_bytes
            == result.dataset.record_count.saturating_mul(
                u64::try_from(
                    result
                        .dataset
                        .key_bytes
                        .saturating_add(result.dataset.value_bytes)
                )
                .unwrap_or(u64::MAX)
            ),
        "prepared logical byte count is inconsistent"
    );
    validate_checkpoint(&result.checkpoint)?;
    ensure!(
        result.checkpoint.live_sst_bytes > 0,
        "prepared checkpoint has no SST data"
    );
    ensure!(
        result.dataset.live_sst_bytes == result.checkpoint.live_sst_bytes,
        "prepared dataset size does not match its checkpoint"
    );
    match result.task {
        crate::config::Task::BulkLoad => {
            ensure!(
                result.source_checkpoint.is_none(),
                "bulk load must not have a source checkpoint"
            );
            validate_preparation_application_rows(result)?;
        }
        crate::config::Task::FullCompaction => {
            let source = result
                .source_checkpoint
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("full compaction has no source checkpoint"))?;
            validate_checkpoint(source)?;
            ensure!(
                source.checkpoint_id != result.checkpoint.checkpoint_id,
                "full compaction reused its source checkpoint"
            );
            validate_preparation_application_rows(result)?;
        }
        _ => unreachable!("checked preparation task"),
    }
    ensure!(
        result.configuration.dataset.record_count == result.dataset.record_count
            && result.configuration.dataset.key_bytes == result.dataset.key_bytes
            && result.configuration.dataset.value_bytes == result.dataset.value_bytes,
        "preparation dataset metadata differs from its configuration"
    );
    Ok(())
}

pub fn validate_workload_result(result: &WorkloadResult) -> Result<()> {
    ensure!(result.status == "ok", "workload status must be ok");
    ensure!(
        !result.task.is_preparation(),
        "workload result has a preparation task"
    );
    ensure!(!result.golden_id.is_empty(), "golden ID is empty");
    ensure!(!result.session.is_empty(), "session is empty");
    validate_timestamp(&result.timestamp)?;
    validate_source(&result.source)?;
    validate_configuration(&result.configuration, result.task)?;
    validate_environment(&result.environment)?;
    ensure!(
        result.series.file == "series.json",
        "workload series file must be series.json"
    );
    ensure!(
        is_sha256(&result.series.sha256),
        "workload series digest is invalid"
    );
    ensure!(
        result.recorded_interval_ns
            >= result
                .client_measurement_ns
                .saturating_add(result.durability_drain_ns),
        "recorded interval does not cover client measurement and durability drain"
    );
    ensure!(
        result.recorded_interval_ns > 0,
        "recorded interval is empty"
    );
    if result.task.may_write() {
        ensure!(
            result.durability_drain_ns > 0,
            "write workload has no durability drain"
        );
    } else {
        ensure!(
            result.durability_drain_ns == 0,
            "read-only workload has a durability drain"
        );
    }
    validate_initial_state(result)?;
    validate_recorded_metrics(
        result.recorded_interval_ns,
        &result.application,
        &result.object_store,
        &result.process,
        &result.machine,
    )?;
    validate_application_rows(result)?;
    Ok(())
}

pub fn validate_workload_series(result: &WorkloadResult, series: &WorkloadSeries) -> Result<()> {
    validate_timeline("rate", &series.rate_elapsed_ns, &series.rate_duration_ns)?;
    validate_timeline(
        "latency",
        &series.latency_elapsed_ns,
        &series.latency_duration_ns,
    )?;
    validate_timeline(
        "resource",
        &series.resource_elapsed_ns,
        &series.resource_duration_ns,
    )?;
    ensure!(
        series.rate_elapsed_ns.last().copied().unwrap_or_default() <= result.recorded_interval_ns,
        "rate series extends past the recorded interval"
    );
    ensure!(
        series
            .latency_elapsed_ns
            .last()
            .copied()
            .unwrap_or_default()
            <= result.recorded_interval_ns,
        "latency series extends past the recorded interval"
    );
    ensure!(
        series
            .resource_elapsed_ns
            .last()
            .copied()
            .unwrap_or_default()
            <= result.recorded_interval_ns,
        "resource series extends past the recorded interval"
    );

    let expected = result
        .application
        .operations
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = series
        .application
        .operations_per_second
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "application operation series keys differ"
    );
    let expected = result
        .application
        .throughput
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = series
        .application
        .bytes_per_second
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "application throughput series keys differ"
    );
    let expected = result
        .application
        .latency
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = series
        .application
        .latency_ns
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "application latency time-series keys differ"
    );
    let actual = series
        .application
        .latency_histograms
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(actual == expected, "application latency series keys differ");

    for (name, values) in &series.application.operations_per_second {
        validate_values(name, values, series.rate_elapsed_ns.len())?;
        validate_rate_distribution(values, &result.application.operations[name])?;
    }
    for (name, values) in &series.application.bytes_per_second {
        validate_values(name, values, series.rate_elapsed_ns.len())?;
        validate_throughput_distribution(values, &result.application.throughput[name])?;
    }
    for (name, values) in &series.application.latency_ns {
        for (statistic, samples) in [
            ("avg", values.avg.as_slice()),
            ("p50", values.p50.as_slice()),
            ("p95", values.p95.as_slice()),
            ("p99", values.p99.as_slice()),
            ("p99.9", values.p999.as_slice()),
        ] {
            validate_optional_values(
                &format!("{name} {statistic}"),
                samples,
                series.latency_elapsed_ns.len(),
            )?;
        }
        for index in 0..series.latency_elapsed_ns.len() {
            match (
                values.avg[index],
                values.p50[index],
                values.p95[index],
                values.p99[index],
                values.p999[index],
            ) {
                (None, None, None, None, None) => {}
                (Some(_), Some(p50), Some(p95), Some(p99), Some(p999)) => ensure!(
                    p50 <= p95 && p95 <= p99 && p99 <= p999,
                    "latency series {name} percentiles are out of order"
                ),
                _ => bail!("latency series {name} has misaligned samples"),
            }
        }
    }
    for (name, histogram) in &series.application.latency_histograms {
        let summary = &result.application.latency[name];
        ensure!(
            !histogram.counts.is_empty()
                && histogram.counts.len() == histogram.upper_bound_ns.len(),
            "latency series {name} is empty or misaligned"
        );
        ensure!(
            histogram
                .upper_bound_ns
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
            "latency series {name} bounds do not increase"
        );
        ensure!(
            histogram.counts.iter().all(|count| *count > 0),
            "latency series {name} contains an empty bucket"
        );
        ensure!(
            histogram.counts.iter().sum::<u64>() == summary.count,
            "latency series {name} count differs from result.json"
        );
        for (quantile, published) in [
            (0.50, summary.p50_ns),
            (0.95, summary.p95_ns),
            (0.99, summary.p99_ns),
            (0.999, summary.p999_ns),
        ] {
            ensure!(
                histogram_quantile(histogram, quantile) == published,
                "latency series {name} percentile differs from result.json"
            );
        }
        ensure!(
            summary.min_ns <= histogram.upper_bound_ns[0]
                && summary.max_ns == *histogram.upper_bound_ns.last().unwrap_or(&0),
            "latency series {name} bounds differ from result.json"
        );
        let upper_bound_mean = histogram
            .upper_bound_ns
            .iter()
            .zip(&histogram.counts)
            .map(|(&bound, &count)| bound as f64 * count as f64)
            .sum::<f64>()
            / summary.count as f64;
        let hdr_tolerance = summary.avg_ns.abs().max(1_000.0) * 0.002;
        ensure!(
            (upper_bound_mean - summary.avg_ns).abs() <= hdr_tolerance,
            "latency series {name} average differs from result.json"
        );
    }

    let expected = result
        .object_store
        .requests
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = series
        .object_store
        .requests_per_second
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "object-store request series keys differ"
    );
    let expected = result
        .object_store
        .throughput
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = series
        .object_store
        .bytes_per_second
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "object-store throughput series keys differ"
    );
    for (method, values) in &series.object_store.requests_per_second {
        validate_values(method, values, series.rate_elapsed_ns.len())?;
        validate_rate_distribution(values, &result.object_store.requests[method])?;
    }
    for (method, values) in &series.object_store.bytes_per_second {
        validate_values(method, values, series.rate_elapsed_ns.len())?;
        validate_throughput_distribution(values, &result.object_store.throughput[method])?;
    }

    let resource_length = series.resource_elapsed_ns.len();
    for (name, values, summary) in [
        (
            "process CPU",
            series.process.cpu_cores.as_slice(),
            &result.process.cpu_cores,
        ),
        (
            "process RSS",
            series.process.rss_bytes.as_slice(),
            &result.process.rss_bytes,
        ),
        (
            "machine CPU",
            series.machine.cpu_percent.as_slice(),
            &result.machine.cpu_percent,
        ),
        (
            "machine RSS",
            series.machine.rss_bytes.as_slice(),
            &result.machine.rss_bytes,
        ),
        (
            "network receive",
            series.machine.network_receive_bytes_per_second.as_slice(),
            &result.machine.network_receive_bytes_per_second,
        ),
        (
            "network send",
            series.machine.network_send_bytes_per_second.as_slice(),
            &result.machine.network_send_bytes_per_second,
        ),
        (
            "disk read bytes",
            series.machine.disk_read_bytes_per_second.as_slice(),
            &result.machine.disk_read_bytes_per_second,
        ),
        (
            "disk write bytes",
            series.machine.disk_write_bytes_per_second.as_slice(),
            &result.machine.disk_write_bytes_per_second,
        ),
        (
            "disk read operations",
            series.machine.disk_read_operations_per_second.as_slice(),
            &result.machine.disk_read_operations_per_second,
        ),
        (
            "disk write operations",
            series.machine.disk_write_operations_per_second.as_slice(),
            &result.machine.disk_write_operations_per_second,
        ),
    ] {
        validate_values(name, values, resource_length)?;
        ensure_distribution_matches(name, values, summary)?;
    }
    Ok(())
}

fn validate_timeline(name: &str, elapsed: &[u64], duration: &[u64]) -> Result<()> {
    ensure!(
        !elapsed.is_empty() && elapsed.len() == duration.len(),
        "{name} timeline is empty or misaligned"
    );
    ensure!(
        duration.iter().all(|value| *value > 0),
        "{name} timeline has an empty window"
    );
    ensure!(
        elapsed.windows(2).all(|pair| pair[0] < pair[1]),
        "{name} timeline does not increase"
    );
    let mut previous = 0_u64;
    for (&end, &length) in elapsed.iter().zip(duration) {
        ensure!(
            end == previous.saturating_add(length),
            "{name} timeline has a gap"
        );
        previous = end;
    }
    Ok(())
}

fn validate_values(name: &str, values: &[f64], expected_len: usize) -> Result<()> {
    ensure!(
        values.len() == expected_len,
        "series {name} has the wrong length"
    );
    ensure!(
        values
            .iter()
            .all(|value| value.is_finite() && *value >= 0.0),
        "series {name} contains a negative or non-finite value"
    );
    Ok(())
}

fn validate_optional_values(name: &str, values: &[Option<f64>], expected_len: usize) -> Result<()> {
    ensure!(
        values.len() == expected_len,
        "series {name} has the wrong length"
    );
    ensure!(
        values
            .iter()
            .flatten()
            .all(|value| value.is_finite() && *value >= 0.0),
        "series {name} contains a negative or non-finite value"
    );
    ensure!(
        values.iter().any(Option::is_some),
        "series {name} contains no samples"
    );
    Ok(())
}

fn validate_rate_distribution(values: &[f64], summary: &RateSummary) -> Result<()> {
    let distribution = distribution_from_values(values);
    ensure_summary_values_match(
        &distribution,
        [
            summary.p50_per_second,
            summary.p95_per_second,
            summary.p99_per_second,
            summary.p999_per_second,
            summary.min_per_second,
            summary.max_per_second,
        ],
    )
}

fn validate_throughput_distribution(values: &[f64], summary: &ThroughputSummary) -> Result<()> {
    let distribution = distribution_from_values(values);
    ensure_summary_values_match(
        &distribution,
        [
            summary.p50_bytes_per_second,
            summary.p95_bytes_per_second,
            summary.p99_bytes_per_second,
            summary.p999_bytes_per_second,
            summary.min_bytes_per_second,
            summary.max_bytes_per_second,
        ],
    )
}

fn ensure_distribution_matches(
    name: &str,
    values: &[f64],
    summary: &DistributionSummary,
) -> Result<()> {
    let distribution = distribution_from_values(values);
    let expected = [
        summary.p50,
        summary.p95,
        summary.p99,
        summary.p999,
        summary.min,
        summary.max,
    ];
    ensure_summary_values_match(&distribution, expected)
        .map_err(|error| anyhow::anyhow!("series {name} differs from result.json: {error}"))?;
    ensure!(
        approximately_equal(distribution[6], summary.avg),
        "series {name} average differs from result.json"
    );
    Ok(())
}

fn distribution_from_values(values: &[f64]) -> [f64; 7] {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    let percentile = |quantile: f64| {
        let index = (quantile * (values.len().saturating_sub(1)) as f64).round() as usize;
        values[index]
    };
    [
        percentile(0.50),
        percentile(0.95),
        percentile(0.99),
        percentile(0.999),
        values[0],
        values[values.len() - 1],
        values.iter().sum::<f64>() / values.len() as f64,
    ]
}

fn ensure_summary_values_match(actual: &[f64; 7], expected: [f64; 6]) -> Result<()> {
    ensure!(
        actual[..6]
            .iter()
            .zip(expected)
            .all(|(left, right)| approximately_equal(*left, right)),
        "series distribution differs from result.json"
    );
    Ok(())
}

fn approximately_equal(left: f64, right: f64) -> bool {
    (left - right).abs() <= left.abs().max(right.abs()).max(1.0) * 1e-9
}

fn histogram_quantile(histogram: &crate::model::HistogramSeries, quantile: f64) -> u64 {
    let total = histogram.counts.iter().sum::<u64>();
    let target = (quantile * total as f64).ceil().max(1.0) as u64;
    let mut cumulative = 0_u64;
    for (&bound, &count) in histogram.upper_bound_ns.iter().zip(&histogram.counts) {
        cumulative = cumulative.saturating_add(count);
        if cumulative >= target {
            return bound;
        }
    }
    0
}

fn validate_recorded_metrics(
    recorded_interval_ns: u64,
    application: &ApplicationMetrics,
    object_store: &ObjectStoreMetrics,
    process: &ProcessStatistics,
    machine: &MachineStatistics,
) -> Result<()> {
    ensure!(recorded_interval_ns > 0, "recorded interval is empty");
    for (api, operations) in &application.operations {
        ensure!(!api.is_empty(), "application API name is empty");
        validate_rate(operations)?;
        let latency = application
            .latency
            .get(api)
            .ok_or_else(|| anyhow::anyhow!("application API {api} has no latency row"))?;
        validate_latency(latency)?;
        ensure!(
            latency.count == operations.total,
            "application API {api} count differs between operations and latency"
        );
        validate_average_rate(
            operations.total,
            operations.avg_per_second,
            recorded_interval_ns,
        )?;
    }
    for (api, throughput) in &application.throughput {
        ensure!(
            application.operations.contains_key(api),
            "application throughput {api} has no operations row"
        );
        validate_throughput(throughput)?;
        validate_average_rate(
            throughput.total_bytes,
            throughput.avg_bytes_per_second,
            recorded_interval_ns,
        )?;
    }
    for (name, latency) in &application.latency {
        validate_latency(latency)?;
        ensure!(
            name == "durable" || application.operations.contains_key(name),
            "application latency {name} has no operations row"
        );
    }
    for (method, requests) in &object_store.requests {
        ensure!(
            matches!(
                method.as_str(),
                "GET" | "PUT" | "HEAD" | "DELETE" | "POST" | "OTHER"
            ),
            "unknown HTTP method {method}"
        );
        validate_rate(requests)?;
        validate_average_rate(
            requests.total,
            requests.avg_per_second,
            recorded_interval_ns,
        )?;
    }
    for (method, throughput) in &object_store.throughput {
        ensure!(
            object_store.requests.contains_key(method),
            "object-store throughput {method} has no request row"
        );
        validate_throughput(throughput)?;
        validate_average_rate(
            throughput.total_bytes,
            throughput.avg_bytes_per_second,
            recorded_interval_ns,
        )?;
    }
    validate_distribution(&process.cpu_cores)?;
    validate_distribution(&process.rss_bytes)?;
    validate_distribution(&machine.cpu_percent)?;
    validate_distribution(&machine.rss_bytes)?;
    validate_distribution(&machine.network_receive_bytes_per_second)?;
    validate_distribution(&machine.network_send_bytes_per_second)?;
    validate_distribution(&machine.disk_read_bytes_per_second)?;
    validate_distribution(&machine.disk_write_bytes_per_second)?;
    validate_distribution(&machine.disk_read_operations_per_second)?;
    validate_distribution(&machine.disk_write_operations_per_second)?;
    Ok(())
}

fn validate_source(source: &SourceIdentity) -> Result<()> {
    for (name, value) in [
        ("SlateDB version", source.slate_version.as_str()),
        ("SlateDB commit", source.slate_commit.as_str()),
        ("runner version", source.runner_version.as_str()),
        ("runner commit", source.runner_commit.as_str()),
        ("lockfile digest", source.lockfile_sha256.as_str()),
    ] {
        ensure!(!value.is_empty(), "{name} is empty");
    }
    Ok(())
}

fn validate_timestamp(timestamp: &str) -> Result<()> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|_| ())
        .map_err(anyhow::Error::from)
}

fn validate_configuration(
    configuration: &ResultConfiguration,
    task: crate::config::Task,
) -> Result<()> {
    ensure!(
        configuration.task.task == task,
        "configuration belongs to another task"
    );
    configuration.task.validate()?;
    ensure!(
        configuration.scale.is_finite() && configuration.scale > 0.0 && configuration.scale <= 1.0,
        "configuration scale is invalid"
    );
    ensure!(
        configuration.dataset.record_count > 0,
        "configuration has no records"
    );
    ensure!(
        configuration.dataset.key_bytes > 0,
        "configuration has zero-byte keys"
    );
    ensure!(
        configuration.dataset.value_bytes > 0,
        "configuration has zero-byte values"
    );
    if let Some(hot_keys) = configuration.task.transaction_hot_keys {
        ensure!(
            hot_keys <= configuration.dataset.record_count,
            "transaction hot set exceeds the dataset"
        );
    }
    ensure!(configuration.caches.block_bytes > 0, "block cache is empty");
    ensure!(
        configuration.caches.metadata_bytes > 0,
        "metadata cache is empty"
    );
    ensure!(
        configuration.caches.object_store_bytes > 0,
        "object-store cache is empty"
    );
    ensure!(
        configuration.slate_settings.is_object(),
        "SlateDB settings are not an object"
    );
    ensure!(
        matches!(configuration.build_profile.as_str(), "debug" | "release"),
        "build profile is invalid"
    );
    let unique_features = configuration
        .enabled_features
        .iter()
        .collect::<BTreeSet<_>>();
    ensure!(
        unique_features.len() == configuration.enabled_features.len(),
        "enabled features contain duplicates"
    );
    Ok(())
}

fn validate_environment(environment: &Environment) -> Result<()> {
    ensure!(environment.cpu_cores > 0, "environment has no CPU cores");
    ensure!(environment.ram_bytes > 0, "environment has no RAM");
    for (name, value) in [
        ("runner type", environment.runner_type.as_str()),
        ("hostname", environment.hostname.as_str()),
        ("CPU model", environment.cpu_model.as_str()),
        ("operating system", environment.os.as_str()),
        ("kernel", environment.kernel.as_str()),
        ("object store", environment.object_store.as_str()),
        ("object-store endpoint", environment.endpoint.as_str()),
        ("object-store region", environment.region.as_str()),
    ] {
        ensure!(!value.is_empty(), "environment {name} is empty");
    }
    Ok(())
}

fn validate_initial_state(result: &WorkloadResult) -> Result<()> {
    let should_be_empty = result.task == crate::config::Task::SustainedIngest;
    if should_be_empty {
        ensure!(
            result.initial_state.kind == "empty",
            "sustained ingest did not start empty"
        );
        ensure!(
            result.initial_state.checkpoint_id.is_none()
                && result.initial_state.manifest_id.is_none(),
            "empty initial state contains a checkpoint"
        );
    } else {
        ensure!(
            result.initial_state.kind == "golden",
            "golden workload did not start from golden data"
        );
        ensure!(
            result.initial_state.checkpoint_id.is_some(),
            "golden initial state has no checkpoint ID"
        );
        ensure!(
            result.initial_state.manifest_id.is_some(),
            "golden initial state has no manifest ID"
        );
    }
    ensure!(
        is_sha256(&result.initial_state.lsm_digest_sha256),
        "initial LSM digest is invalid"
    );
    Ok(())
}

fn validate_preparation_application_rows(result: &PreparationResult) -> Result<()> {
    use crate::config::Task;

    match result.task {
        Task::BulkLoad => {
            let expected = ["flush", "write"].into_iter().collect::<BTreeSet<_>>();
            let operations = result
                .application
                .operations
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            let latency = result
                .application
                .latency
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            let throughput = result
                .application
                .throughput
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            ensure!(
                operations == expected && latency == expected,
                "bulk load must record write and flush calls"
            );
            ensure!(
                throughput == ["write"].into_iter().collect(),
                "bulk load must record write throughput"
            );
            ensure!(
                result.application.operations["write"].total
                    == result
                        .dataset
                        .record_count
                        .div_ceil(crate::workloads::DATASET_BATCH_RECORDS),
                "bulk-load write count does not match its batches"
            );
            ensure!(
                result.application.operations["flush"].total == 1,
                "bulk load must record exactly one flush"
            );
            ensure!(
                result.application.throughput["write"].total_bytes == result.dataset.logical_bytes,
                "bulk-load write throughput differs from logical dataset bytes"
            );
        }
        Task::FullCompaction => ensure!(
            result.application.operations.is_empty()
                && result.application.throughput.is_empty()
                && result.application.latency.is_empty(),
            "full compaction must not record application calls"
        ),
        _ => unreachable!("checked preparation task"),
    }
    Ok(())
}

fn validate_application_rows(result: &WorkloadResult) -> Result<()> {
    let mut expected = BTreeSet::new();
    for operation in result.configuration.task.operation_mix.keys() {
        match operation.as_str() {
            "get" | "put" | "scan" => {
                expected.insert(operation.as_str());
            }
            "transaction" => {
                expected.extend(["transaction.get", "transaction.put", "transaction.commit"]);
            }
            _ => unreachable!("validated operation mix"),
        }
    }
    if result.task.may_write() {
        expected.insert("flush");
    }
    let actual = result
        .application
        .operations
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        actual == expected,
        "application operation rows do not match the workload"
    );

    let latency = result
        .application
        .latency
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut expected_latency = expected;
    if result.task.may_write() {
        expected_latency.insert("durable");
    }
    ensure!(
        latency == expected_latency,
        "application latency rows do not match the workload"
    );
    if result.task.may_write() {
        ensure!(
            result.application.operations["flush"].total == 1,
            "write workload must record exactly one final flush"
        );
        let durable = result.application.latency["durable"].count;
        if result.configuration.task.operation_mix.contains_key("put") {
            ensure!(
                durable == result.application.operations["put"].total,
                "durability count does not match accepted puts"
            );
        } else if result
            .configuration
            .task
            .operation_mix
            .contains_key("transaction")
        {
            let commits = result.application.operations["transaction.commit"].total;
            let reads = u64::try_from(
                result
                    .configuration
                    .task
                    .transaction_reads
                    .expect("validated transaction read count"),
            )
            .unwrap_or(u64::MAX);
            let updates = u64::try_from(
                result
                    .configuration
                    .task
                    .transaction_updates
                    .expect("validated transaction update count"),
            )
            .unwrap_or(u64::MAX);
            ensure!(
                result.application.operations["transaction.get"].total
                    == commits.saturating_mul(reads)
                    && result.application.operations["transaction.put"].total
                        == commits.saturating_mul(updates),
                "transaction API counts do not match the configured reads and updates"
            );
            ensure!(
                durable <= commits,
                "durability count exceeds transaction commits"
            );
        } else {
            unreachable!("validated write workload");
        }
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &crate::model::CheckpointReference) -> Result<()> {
    ensure!(
        !checkpoint.database_path.is_empty(),
        "checkpoint path is empty"
    );
    ensure!(
        !checkpoint.checkpoint_id.is_empty(),
        "checkpoint ID is empty"
    );
    checkpoint
        .checkpoint_id
        .parse::<uuid::Uuid>()
        .map_err(anyhow::Error::from)?;
    ensure!(
        is_sha256(&checkpoint.lsm_digest_sha256),
        "checkpoint LSM digest is invalid"
    );
    Ok(())
}

fn validate_rate(summary: &RateSummary) -> Result<()> {
    ensure!(summary.total > 0, "rate row has no operations");
    validate_ordered([
        summary.min_per_second,
        summary.p50_per_second,
        summary.p95_per_second,
        summary.p99_per_second,
        summary.p999_per_second,
        summary.max_per_second,
    ])?;
    validate_nonnegative([
        summary.avg_per_second,
        summary.p50_per_second,
        summary.p95_per_second,
        summary.p99_per_second,
        summary.p999_per_second,
        summary.min_per_second,
        summary.max_per_second,
    ])
}

fn validate_average_rate(total: u64, average: f64, elapsed_ns: u64) -> Result<()> {
    let expected = total as f64 / (elapsed_ns as f64 / 1_000_000_000.0);
    let tolerance = expected.abs().max(1.0) * 1e-9;
    ensure!(
        (average - expected).abs() <= tolerance,
        "summary average does not match its total and interval"
    );
    Ok(())
}

fn validate_throughput(summary: &ThroughputSummary) -> Result<()> {
    ensure!(summary.total_bytes > 0, "throughput row has zero bytes");
    validate_ordered([
        summary.min_bytes_per_second,
        summary.p50_bytes_per_second,
        summary.p95_bytes_per_second,
        summary.p99_bytes_per_second,
        summary.p999_bytes_per_second,
        summary.max_bytes_per_second,
    ])?;
    validate_nonnegative([
        summary.avg_bytes_per_second,
        summary.p50_bytes_per_second,
        summary.p95_bytes_per_second,
        summary.p99_bytes_per_second,
        summary.p999_bytes_per_second,
        summary.min_bytes_per_second,
        summary.max_bytes_per_second,
    ])
}

fn validate_latency(summary: &LatencySummary) -> Result<()> {
    ensure!(summary.count > 0, "latency row has no calls");
    ensure!(
        summary.avg_ns.is_finite() && summary.avg_ns >= 0.0,
        "latency average is invalid"
    );
    ensure!(
        summary.min_ns <= summary.max_ns,
        "latency bounds are reversed"
    );
    ensure!(
        summary.min_ns <= summary.p50_ns
            && summary.p50_ns <= summary.p95_ns
            && summary.p95_ns <= summary.p99_ns
            && summary.p99_ns <= summary.p999_ns
            && summary.p999_ns <= summary.max_ns,
        "latency percentiles are not ordered"
    );
    Ok(())
}

fn validate_distribution(summary: &DistributionSummary) -> Result<()> {
    validate_nonnegative([
        summary.avg,
        summary.p50,
        summary.p95,
        summary.p99,
        summary.p999,
        summary.min,
        summary.max,
    ])?;
    ensure!(
        summary.min <= summary.max,
        "distribution bounds are reversed"
    );
    validate_ordered([
        summary.min,
        summary.p50,
        summary.p95,
        summary.p99,
        summary.p999,
        summary.max,
    ])?;
    Ok(())
}

fn validate_ordered<const N: usize>(values: [f64; N]) -> Result<()> {
    validate_nonnegative(values)?;
    ensure!(
        values.windows(2).all(|pair| pair[0] <= pair[1]),
        "summary percentiles are not ordered"
    );
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_nonnegative<const N: usize>(values: [f64; N]) -> Result<()> {
    if values
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        bail!("summary contains a negative or non-finite value");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_distribution;
    use crate::model::DistributionSummary;

    #[test]
    fn rejects_non_finite_published_values() {
        let summary = DistributionSummary {
            avg: f64::NAN,
            ..Default::default()
        };
        assert!(validate_distribution(&summary).is_err());
    }
}
