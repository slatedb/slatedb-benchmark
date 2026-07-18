use super::durability::DurabilitySender;
use super::stats::{record_error, record_success, WorkerStats};
use super::util::{key_for_id, missing_key_for_id, KeySelector, ValueGenerator};
use crate::config::{ResolvedConfig, Task};
use crate::instrumented_store::StoreMetrics;
use crate::system::{
    counter_value, duration_ns, measure_backpressure, ApplicationRecorder, ApplicationRegistry,
    BenchmarkMetricsRecorder,
};
use anyhow::{Context, Result};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::{Db, ErrorKind, IsolationLevel, WriteBatch};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub async fn run_phase(
    db: Arc<Db>,
    config: &ResolvedConfig,
    duration: Duration,
    registry: Option<Arc<ApplicationRegistry>>,
    durability: Option<DurabilitySender>,
) -> Result<WorkerStats> {
    if config.task.task == Task::Idle {
        tokio::time::sleep(duration).await;
        return Ok(WorkerStats::default());
    }
    let next_insert = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + duration;
    let mut tasks = JoinSet::new();
    for _ in 0..config.task.clients {
        let db = Arc::clone(&db);
        let config = config.clone();
        let recorder = registry.as_ref().map(|registry| registry.recorder());
        let durability = durability.clone();
        let next_insert = Arc::clone(&next_insert);
        tasks.spawn(async move {
            worker_loop(db, config, deadline, recorder, durability, next_insert).await
        });
    }
    let mut merged = WorkerStats::default();
    while let Some(result) = tasks.join_next().await {
        merged.merge(&result.context("joining benchmark client")??);
    }
    Ok(merged)
}

async fn worker_loop(
    db: Arc<Db>,
    config: ResolvedConfig,
    deadline: Instant,
    recorder: Option<ApplicationRecorder>,
    durability: Option<DurabilitySender>,
    next_insert: Arc<AtomicU64>,
) -> Result<WorkerStats> {
    let mut rng = StdRng::from_os_rng();
    let selector = if matches!(
        config.task.task,
        Task::PointReadSkewed | Task::ReadHeavy | Task::Balanced | Task::UpdateHeavy
    ) {
        KeySelector::zipfian(config.dataset.record_count)
    } else {
        KeySelector::uniform(config.dataset.record_count)
    };
    let mut values = ValueGenerator::new();
    let mut stats = WorkerStats::default();
    while Instant::now() < deadline {
        match config.task.task {
            Task::PointReadUniform | Task::PointReadSkewed => {
                get(
                    &db,
                    &config,
                    selector.sample(&mut rng),
                    false,
                    recorder.as_ref(),
                    &mut stats,
                )
                .await;
            }
            Task::PointReadMissing => {
                get(
                    &db,
                    &config,
                    selector.sample(&mut rng),
                    true,
                    recorder.as_ref(),
                    &mut stats,
                )
                .await;
            }
            Task::ReadHeavy => {
                if rng.random_bool(0.95) {
                    get(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        false,
                        recorder.as_ref(),
                        &mut stats,
                    )
                    .await;
                } else {
                    put(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        recorder.as_ref(),
                        durability.as_ref(),
                        &mut rng,
                        &mut values,
                        &mut stats,
                    )
                    .await;
                }
            }
            Task::Balanced => {
                if rng.random_bool(0.5) {
                    get(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        false,
                        recorder.as_ref(),
                        &mut stats,
                    )
                    .await;
                } else {
                    put(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        recorder.as_ref(),
                        durability.as_ref(),
                        &mut rng,
                        &mut values,
                        &mut stats,
                    )
                    .await;
                }
            }
            Task::UpdateHeavy => {
                if rng.random_bool(0.05) {
                    get(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        false,
                        recorder.as_ref(),
                        &mut stats,
                    )
                    .await;
                } else {
                    put(
                        &db,
                        &config,
                        selector.sample(&mut rng),
                        recorder.as_ref(),
                        durability.as_ref(),
                        &mut rng,
                        &mut values,
                        &mut stats,
                    )
                    .await;
                }
            }
            Task::RangeScan => {
                scan(
                    &db,
                    &config,
                    selector.sample(&mut rng),
                    recorder.as_ref(),
                    &mut stats,
                )
                .await;
            }
            Task::SustainedIngest => {
                let id = next_insert.fetch_add(1, Ordering::Relaxed);
                put(
                    &db,
                    &config,
                    id,
                    recorder.as_ref(),
                    durability.as_ref(),
                    &mut rng,
                    &mut values,
                    &mut stats,
                )
                .await;
            }
            Task::TransactionContention => {
                transaction(
                    &db,
                    &config,
                    recorder.as_ref(),
                    durability.as_ref(),
                    &mut rng,
                    &mut values,
                    &mut stats,
                )
                .await;
            }
            Task::BulkLoad | Task::FullCompaction | Task::Idle => {
                anyhow::bail!("{} is not an active workload", config.task.task);
            }
        }
    }
    Ok(stats)
}

async fn get(
    db: &Db,
    config: &ResolvedConfig,
    id: u64,
    missing: bool,
    recorder: Option<&ApplicationRecorder>,
    stats: &mut WorkerStats,
) {
    let key = if missing {
        missing_key_for_id(id, config.dataset.key_bytes)
    } else {
        key_for_id(id, config.dataset.key_bytes)
    };
    let started = Instant::now();
    match db.get(key.clone()).await {
        Ok(value) => {
            let bytes = u64::try_from(key.len()).unwrap_or(u64::MAX).saturating_add(
                value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            );
            record_success(recorder, "get", started.elapsed(), bytes);
            if value.is_some() {
                stats.read_hits = stats.read_hits.saturating_add(1);
            } else {
                stats.read_misses = stats.read_misses.saturating_add(1);
            }
        }
        Err(error) => {
            record_error(stats, recorder, "get", started.elapsed());
            tracing::debug!(%error, "get failed");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn put(
    db: &Db,
    config: &ResolvedConfig,
    id: u64,
    recorder: Option<&ApplicationRecorder>,
    durability: Option<&DurabilitySender>,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) {
    let key = key_for_id(id, config.dataset.key_bytes);
    let value = values.generate(config.dataset.value_bytes, rng);
    let logical_bytes = u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX);
    let options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let started = Instant::now();
    match db
        .put_with_options(key, value, &PutOptions::default(), &options)
        .await
    {
        Ok(handle) => {
            let returned_at = Instant::now();
            record_success(recorder, "put", started.elapsed(), logical_bytes);
            stats.record_write(&handle, returned_at, durability);
        }
        Err(error) => {
            record_error(stats, recorder, "put", started.elapsed());
            tracing::debug!(%error, "put failed");
        }
    }
}

async fn scan(
    db: &Db,
    config: &ResolvedConfig,
    start_id: u64,
    recorder: Option<&ApplicationRecorder>,
    stats: &mut WorkerStats,
) {
    let key = key_for_id(start_id, config.dataset.key_bytes);
    let Ok(mut iterator) = db.scan(key..).await else {
        record_error(stats, recorder, "scan", Duration::ZERO);
        return;
    };
    let limit = config.task.scan_limit.unwrap_or(10);
    let expected = usize::try_from(config.dataset.record_count.saturating_sub(start_id))
        .unwrap_or(usize::MAX)
        .min(limit);
    let mut returned = 0_usize;
    while returned < limit {
        let started = Instant::now();
        match iterator.next().await {
            Ok(Some(entry)) => {
                let bytes = u64::try_from(entry.key.len().saturating_add(entry.value.len()))
                    .unwrap_or(u64::MAX);
                record_success(recorder, "scan", started.elapsed(), bytes);
                returned += 1;
                stats.scan_records = stats.scan_records.saturating_add(1);
            }
            Ok(None) => {
                record_success(recorder, "scan", started.elapsed(), 0);
                stats.scan_end_calls = stats.scan_end_calls.saturating_add(1);
                break;
            }
            Err(error) => {
                record_error(stats, recorder, "scan", started.elapsed());
                tracing::debug!(%error, "scan next failed");
                return;
            }
        }
    }
    if returned != expected {
        stats.errors = stats.errors.saturating_add(1);
        tracing::debug!(
            start_id,
            expected,
            returned,
            "scan returned an unexpected record count"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn transaction(
    db: &Db,
    config: &ResolvedConfig,
    recorder: Option<&ApplicationRecorder>,
    durability: Option<&DurabilitySender>,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) {
    stats.transaction_attempts = stats.transaction_attempts.saturating_add(1);
    let transaction = match db.begin(IsolationLevel::SerializableSnapshot).await {
        Ok(transaction) => transaction,
        Err(error) => {
            stats.errors = stats.errors.saturating_add(1);
            tracing::debug!(%error, "transaction begin failed");
            return;
        }
    };
    let hot_keys = config.task.transaction_hot_keys.unwrap_or(10_000).max(1);
    let mut operations = [
        true, true, true, true, true, false, false, false, false, false,
    ];
    operations.shuffle(rng);
    for read in operations {
        let id = rng.random_range(0..hot_keys);
        let key = key_for_id(id, config.dataset.key_bytes);
        if read {
            let started = Instant::now();
            match transaction.get(key.clone()).await {
                Ok(Some(value)) => {
                    let bytes =
                        u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX);
                    record_success(recorder, "transaction.get", started.elapsed(), bytes);
                }
                Ok(None) => {
                    record_success(recorder, "transaction.get", started.elapsed(), 0);
                    stats.errors = stats.errors.saturating_add(1);
                }
                Err(error) => {
                    record_error(stats, recorder, "transaction.get", started.elapsed());
                    tracing::debug!(%error, "transaction get failed");
                    return;
                }
            }
        } else {
            let value = values.generate(config.dataset.value_bytes, rng);
            let bytes = u64::try_from(key.len().saturating_add(value.len())).unwrap_or(u64::MAX);
            let started = Instant::now();
            match transaction.put(key, value) {
                Ok(()) => record_success(recorder, "transaction.put", started.elapsed(), bytes),
                Err(error) => {
                    record_error(stats, recorder, "transaction.put", started.elapsed());
                    tracing::debug!(%error, "transaction put failed");
                    return;
                }
            }
        }
    }
    let options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let started = Instant::now();
    match transaction.commit_with_options(&options).await {
        Ok(Some(handle)) => {
            let returned_at = Instant::now();
            record_success(recorder, "transaction.commit", started.elapsed(), 0);
            stats.transaction_commits = stats.transaction_commits.saturating_add(1);
            stats.record_write(&handle, returned_at, durability);
        }
        Ok(None) => {
            record_error(stats, recorder, "transaction.commit", started.elapsed());
            tracing::debug!("write transaction committed without a write handle");
        }
        Err(error) if error.kind() == ErrorKind::Transaction => {
            record_success(recorder, "transaction.commit", started.elapsed(), 0);
            stats.transaction_conflicts = stats.transaction_conflicts.saturating_add(1);
        }
        Err(error) => {
            record_error(stats, recorder, "transaction.commit", started.elapsed());
            tracing::debug!(%error, "transaction commit failed");
        }
    }
}

const DATASET_BATCH_RECORDS: u64 = 1_024;
const DATASET_BATCH_QUEUE_DEPTH: usize = 16;

pub struct DatasetLoadMetrics {
    store: Arc<StoreMetrics>,
    slate: Arc<BenchmarkMetricsRecorder>,
}

impl DatasetLoadMetrics {
    pub fn new(store: Arc<StoreMetrics>, slate: Arc<BenchmarkMetricsRecorder>) -> Self {
        Self { store, slate }
    }
}

struct DatasetBatch {
    start: u64,
    end: u64,
    batch: WriteBatch,
}

pub async fn populate_dataset(
    db: Arc<Db>,
    config: &ResolvedConfig,
    metrics: DatasetLoadMetrics,
) -> Result<()> {
    const HEARTBEAT: Duration = Duration::from_secs(30);
    let record_count = config.dataset.record_count;
    let producer_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(2)
        .clamp(2, 8);
    let (mut batch_receivers, producers) = spawn_dataset_producers(config, producer_count);
    let options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let started = Instant::now();
    let mut last_report = started;
    let mut last_records = 0_u64;
    let mut last_upload = put_bytes(&metrics.store.snapshot());
    let mut last_l0 = counter_value(&metrics.slate.snapshot(), slatedb::db_stats::L0_FLUSH_BYTES);
    let mut loaded = 0_u64;
    let mut backpressure_ns = 0_u64;
    let mut last_backpressure_ns = 0_u64;
    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    tracing::info!(
        producer_count,
        queue_depth = DATASET_BATCH_QUEUE_DEPTH,
        records_per_batch = DATASET_BATCH_RECORDS,
        "starting bulk load"
    );

    while loaded < record_count {
        let producer = usize::try_from(loaded / DATASET_BATCH_RECORDS)
            .context("dataset batch index exceeds the platform limit")?
            % producer_count;
        let batch = batch_receivers[producer]
            .recv()
            .await
            .context("dataset producer stopped before completing the load")??;
        anyhow::ensure!(
            batch.start == loaded,
            "dataset producer returned a batch out of order"
        );
        let mut write = Box::pin(measure_backpressure(
            db.write_with_options(batch.batch, &options),
        ));
        let (result, backpressure) = loop {
            tokio::select! {
                result = &mut write => break result,
                _ = heartbeat.tick() => {
                    report_load_progress(
                        loaded,
                        record_count,
                        backpressure_ns,
                        config,
                        &metrics,
                        started,
                        &mut last_report,
                        &mut last_records,
                        &mut last_upload,
                        &mut last_l0,
                        &mut last_backpressure_ns,
                    );
                }
            }
        };
        result.with_context(|| format!("loading records {}..{}", batch.start, batch.end))?;
        backpressure_ns = backpressure_ns.saturating_add(duration_ns(backpressure));
        loaded = batch.end;
    }
    for producer in producers {
        producer.await.context("joining dataset producer")??;
    }
    let mut flush = Box::pin(db.flush());
    loop {
        tokio::select! {
            result = &mut flush => {
                result.context("flushing bulk-loaded dataset")?;
                break;
            }
            _ = heartbeat.tick() => {
                report_load_progress(
                    loaded,
                    record_count,
                    backpressure_ns,
                    config,
                    &metrics,
                    started,
                    &mut last_report,
                    &mut last_records,
                    &mut last_upload,
                    &mut last_l0,
                    &mut last_backpressure_ns,
                );
            }
        }
    }
    report_load_progress(
        loaded,
        record_count,
        backpressure_ns,
        config,
        &metrics,
        started,
        &mut last_report,
        &mut last_records,
        &mut last_upload,
        &mut last_l0,
        &mut last_backpressure_ns,
    );
    tracing::info!(
        elapsed_seconds = truncate(started.elapsed().as_secs_f64()),
        "bulk load complete"
    );
    Ok(())
}

type DatasetProducer = tokio::task::JoinHandle<Result<()>>;

fn spawn_dataset_producers(
    config: &ResolvedConfig,
    producer_count: usize,
) -> (
    Vec<mpsc::Receiver<Result<DatasetBatch>>>,
    Vec<DatasetProducer>,
) {
    let queue_per_producer = (DATASET_BATCH_QUEUE_DEPTH / producer_count).max(1);
    let mut receivers = Vec::with_capacity(producer_count);
    let mut producers = Vec::new();
    for producer in 0..producer_count {
        let (sender, receiver) = mpsc::channel(queue_per_producer);
        receivers.push(receiver);
        let record_count = config.dataset.record_count;
        let key_bytes = config.dataset.key_bytes;
        let value_bytes = config.dataset.value_bytes;
        producers.push(tokio::task::spawn_blocking(move || -> Result<()> {
            let mut rng = StdRng::from_os_rng();
            let mut values = ValueGenerator::new();
            let stride = DATASET_BATCH_RECORDS.saturating_mul(producer_count as u64);
            let mut start = DATASET_BATCH_RECORDS.saturating_mul(producer as u64);
            while start < record_count {
                let end = start
                    .saturating_add(DATASET_BATCH_RECORDS)
                    .min(record_count);
                let mut batch = WriteBatch::new();
                for id in start..end {
                    batch.put_bytes_with_options(
                        key_for_id(id, key_bytes),
                        values.generate(value_bytes, &mut rng),
                        &PutOptions::default(),
                    );
                }
                if sender
                    .blocking_send(Ok(DatasetBatch { start, end, batch }))
                    .is_err()
                {
                    break;
                }
                start = start.saturating_add(stride);
            }
            Ok(())
        }));
    }
    (receivers, producers)
}

#[allow(clippy::too_many_arguments)]
fn report_load_progress(
    completed: u64,
    total: u64,
    backpressure_ns: u64,
    config: &ResolvedConfig,
    metrics: &DatasetLoadMetrics,
    started: Instant,
    last_report: &mut Instant,
    last_records: &mut u64,
    last_upload: &mut u64,
    last_l0: &mut u64,
    last_backpressure_ns: &mut u64,
) {
    let now = Instant::now();
    let interval = now.saturating_duration_since(*last_report);
    let recent_records = completed.saturating_sub(*last_records);
    let recent_rate = recent_records as f64 / interval.as_secs_f64().max(f64::EPSILON);
    let upload = put_bytes(&metrics.store.snapshot());
    let l0 = counter_value(&metrics.slate.snapshot(), slatedb::db_stats::L0_FLUSH_BYTES);
    let logical_bytes = config
        .dataset
        .key_bytes
        .saturating_add(config.dataset.value_bytes) as f64;
    tracing::info!(
        records_completed = completed,
        successful_records = completed,
        total_records = total,
        errors = 0_u64,
        progress_percent = truncate(completed as f64 / total.max(1) as f64 * 100.0),
        recent_puts_per_second = truncate(recent_rate),
        average_puts_per_second = truncate(
            completed as f64
                / now
                    .saturating_duration_since(started)
                    .as_secs_f64()
                    .max(f64::EPSILON)
        ),
        logical_mib_per_second = truncate(recent_rate * logical_bytes / (1024.0 * 1024.0)),
        physical_upload_mib_per_second = truncate(
            upload.saturating_sub(*last_upload) as f64
                / interval.as_secs_f64().max(f64::EPSILON)
                / (1024.0 * 1024.0)
        ),
        l0_flush_mib_per_second = truncate(
            l0.saturating_sub(*last_l0) as f64
                / interval.as_secs_f64().max(f64::EPSILON)
                / (1024.0 * 1024.0)
        ),
        backpressure_percent = truncate(
            backpressure_ns.saturating_sub(*last_backpressure_ns) as f64
                / 1e9
                / interval.as_secs_f64().max(f64::EPSILON)
                * 100.0
        ),
        eta_seconds =
            truncate(total.saturating_sub(completed) as f64 / recent_rate.max(f64::EPSILON)),
        "bulk-load progress"
    );
    *last_report = now;
    *last_records = completed;
    *last_upload = upload;
    *last_l0 = l0;
    *last_backpressure_ns = backpressure_ns;
}

fn put_bytes(snapshot: &crate::instrumented_store::StoreSnapshot) -> u64 {
    snapshot.request_bytes.get("PUT").copied().unwrap_or(0)
}

fn truncate(value: f64) -> f64 {
    (value * 100.0).trunc() / 100.0
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn progress_values_have_two_decimal_places() {
        assert_eq!(truncate(1.3359), 1.33);
        assert_eq!(truncate(121_718.117), 121_718.11);
    }
}
