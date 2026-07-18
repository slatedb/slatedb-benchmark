use crate::model::{
    ApplicationMetrics, DistributionSummary, Environment, LatencySummary, MachineStatistics,
    ObjectStoreMetrics, PreparationResult, ProcessStatistics, RateSummary, ResultConfiguration,
    SourceIdentity, ThroughputSummary, WorkloadResult,
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
    use crate::config::Task;

    let expected: &[&str] = match result.task {
        Task::Idle => &[],
        Task::PointReadUniform | Task::PointReadSkewed | Task::PointReadMissing => &["get"],
        Task::ReadHeavy | Task::Balanced | Task::UpdateHeavy => &["get", "put", "flush"],
        Task::RangeScan => &["scan"],
        Task::SustainedIngest => &["put", "flush"],
        Task::TransactionContention => &[
            "transaction.get",
            "transaction.put",
            "transaction.commit",
            "flush",
        ],
        Task::BulkLoad | Task::FullCompaction => unreachable!("checked workload task"),
    };
    let actual = result
        .application
        .operations
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
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
        match result.task {
            Task::ReadHeavy | Task::Balanced | Task::UpdateHeavy | Task::SustainedIngest => {
                ensure!(
                    durable == result.application.operations["put"].total,
                    "durability count does not match accepted puts"
                );
            }
            Task::TransactionContention => {
                let commits = result.application.operations["transaction.commit"].total;
                ensure!(
                    result.application.operations["transaction.get"].total
                        == commits.saturating_mul(5)
                        && result.application.operations["transaction.put"].total
                            == commits.saturating_mul(5),
                    "transaction API counts do not match five reads and five updates"
                );
                ensure!(
                    durable <= commits,
                    "durability count exceeds transaction commits"
                );
            }
            _ => unreachable!("checked write workload"),
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
