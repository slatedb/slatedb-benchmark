use super::durability::DurabilitySender;
use super::stats::{Payload, WorkerStats};
use super::util::{
    ordered_key_for_id, prefix_key, random_unique_key, rocksdb_key_for_id, ycsb_key_for_id,
    KeySelector, ValueGenerator, YcsbLatestSelector,
};
use crate::config::{KeyDistribution, VariantConfig, WorkloadKind};
use crate::system::{measure_backpressure, ApplicationWindowRegistry};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, ScanOptions, WriteOptions};
use slatedb::{Db, ErrorKind, IsolationLevel, IterationOrder, WriteBatch, WriteHandle};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

fn key_for_suite(suite: &str, id: u64, size: usize) -> bytes::Bytes {
    match suite {
        "ycsb" => ycsb_key_for_id(id, size),
        "rocksdb" => rocksdb_key_for_id(id, size),
        _ => ordered_key_for_id(id, size),
    }
}

fn key_for_variant(variant: &VariantConfig, id: u64) -> bytes::Bytes {
    key_for_suite(&variant.suite.name, id, variant.key_bytes())
}

fn point_payload_bytes(variant: &VariantConfig, key_bytes: usize, value_bytes: usize) -> u64 {
    let key_bytes = if variant.suite.name == "rocksdb" {
        key_bytes
    } else {
        0
    };
    key_bytes.saturating_add(value_bytes) as u64
}

#[derive(Clone)]
pub struct ClosedLoopState {
    next_insert: Arc<AtomicU64>,
    acknowledged_insert: Arc<AtomicU64>,
    completed_inserts: Arc<Mutex<BTreeSet<u64>>>,
}

impl ClosedLoopState {
    pub fn new(next_insert: u64) -> Self {
        Self {
            next_insert: Arc::new(AtomicU64::new(next_insert)),
            acknowledged_insert: Arc::new(AtomicU64::new(next_insert)),
            completed_inserts: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn allocate_insert(&self) -> u64 {
        self.next_insert.fetch_add(1, Ordering::Relaxed)
    }

    fn acknowledge_insert(&self, id: u64) {
        let mut completed = self
            .completed_inserts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut frontier = self.acknowledged_insert.load(Ordering::Relaxed);
        if id < frontier {
            return;
        }
        completed.insert(id);
        while completed.remove(&frontier) {
            frontier = frontier.saturating_add(1);
        }
        self.acknowledged_insert.store(frontier, Ordering::Release);
    }

    fn acknowledged_insert(&self) -> u64 {
        self.acknowledged_insert.load(Ordering::Acquire)
    }
}

pub async fn run_closed_phase(
    db: Arc<Db>,
    variant: &VariantConfig,
    duration: Duration,
    durability: Option<DurabilitySender>,
    windows: Option<Arc<ApplicationWindowRegistry>>,
    state: &ClosedLoopState,
) -> Result<WorkerStats> {
    if variant.workload.kind.while_writing_read_kind().is_some() {
        return run_with_capped_writer(db, variant, duration, durability, windows, state).await;
    }

    let clients = variant.clients;
    let deadline = Instant::now() + duration;
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let db = Arc::clone(&db);
        let variant = variant.clone();
        let durability = durability.clone();
        let windows = windows.clone();
        let state = state.clone();
        tasks.spawn(
            async move { worker_loop(db, variant, deadline, durability, windows, state).await },
        );
    }
    merge_tasks(tasks).await
}

async fn worker_loop(
    db: Arc<Db>,
    variant: VariantConfig,
    deadline: Instant,
    durability: Option<DurabilitySender>,
    windows: Option<Arc<ApplicationWindowRegistry>>,
    state: ClosedLoopState,
) -> Result<WorkerStats> {
    let mut rng = StdRng::from_os_rng();
    let selector = match variant.workload.kind.key_distribution() {
        KeyDistribution::Uniform => KeySelector::uniform(variant.record_count()),
        KeyDistribution::Zipfian if variant.workload.kind == WorkloadKind::YcsbE => {
            KeySelector::ycsb_insert_aware(variant.record_count())
        }
        KeyDistribution::Zipfian => KeySelector::zipfian(variant.record_count()),
    };
    let mut latest_selector = (variant.workload.kind == WorkloadKind::YcsbD)
        .then(|| YcsbLatestSelector::new(variant.record_count()));
    let write_options = WriteOptions {
        await_durable: variant.workload.await_durable,
        ..Default::default()
    };
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let mut stats = WorkerStats::with_window_recorder(
        windows
            .as_ref()
            .map(|windows| windows.register_window_recorder()),
    );
    let operation_context = OperationContext {
        db: &db,
        variant: &variant,
        write_options: &write_options,
    };
    while Instant::now() < deadline {
        let (completion, backpressure) = measure_backpressure(execute_operation(
            &operation_context,
            &selector,
            &mut latest_selector,
            &state,
            &mut rng,
            &mut values,
            &mut stats,
        ))
        .await;
        stats.record_backpressure(backpressure);
        match completion.result {
            Ok(outcome) => {
                let returned_at = Instant::now();
                stats.record_success(outcome.name, completion.latency, outcome.payload);
                stats.batch_keys = stats.batch_keys.saturating_add(outcome.batch_keys);
                if outcome.batch_keys > 0 {
                    stats.record_batch_latency(completion.latency);
                }
                if outcome.transaction_commit {
                    stats.transaction_commits += 1;
                }
                if let Some(handle) = outcome.write_handle {
                    stats.record_write(returned_at, handle.seqnum());
                    if let Some(tracker) = &durability {
                        tracker.accepted(handle.seqnum(), returned_at);
                    }
                }
            }
            Err(OperationError::TransactionConflict) => {
                stats.record_transaction_conflict(completion.latency);
            }
            Err(OperationError::Other(error)) => {
                stats.record_error(
                    variant.workload.kind.default_operation_name(),
                    completion.latency,
                );
                tracing::debug!(%error, "benchmark operation failed");
            }
        }
    }
    Ok(stats)
}

struct OperationOutcome {
    name: &'static str,
    payload: Payload,
    batch_keys: u64,
    write_handle: Option<WriteHandle>,
    transaction_commit: bool,
}

struct OperationCompletion {
    result: std::result::Result<OperationOutcome, OperationError>,
    latency: Duration,
}

enum OperationError {
    TransactionConflict,
    Other(anyhow::Error),
}

impl From<anyhow::Error> for OperationError {
    fn from(value: anyhow::Error) -> Self {
        Self::Other(value)
    }
}

struct OperationContext<'a> {
    db: &'a Db,
    variant: &'a VariantConfig,
    write_options: &'a WriteOptions,
}

async fn execute_operation(
    context: &OperationContext<'_>,
    selector: &KeySelector,
    latest_selector: &mut Option<YcsbLatestSelector>,
    state: &ClosedLoopState,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) -> OperationCompletion {
    let started = Instant::now();
    let db = context.db;
    let variant = context.variant;
    let write_options = context.write_options;
    let kind = variant.workload.kind;
    let result = match kind {
        WorkloadKind::YcsbA => {
            if rng.random_bool(0.5) {
                read(db, variant, selector.sample(rng), "read", stats).await
            } else {
                update(context, selector.sample(rng), rng, values, "update", stats).await
            }
        }
        WorkloadKind::YcsbB => {
            if rng.random_bool(0.95) {
                read(db, variant, selector.sample(rng), "read", stats).await
            } else {
                update(context, selector.sample(rng), rng, values, "update", stats).await
            }
        }
        WorkloadKind::YcsbC | WorkloadKind::ColdRead | WorkloadKind::RandomRead => {
            read(db, variant, selector.sample(rng), "read", stats).await
        }
        WorkloadKind::YcsbD => {
            if rng.random_bool(0.95) {
                let latest = state.acknowledged_insert().max(1);
                let id = latest_selector
                    .as_mut()
                    .expect("YCSB D latest selector")
                    .sample(latest, rng);
                read(db, variant, id, "read", stats).await
            } else {
                return ycsb_insert(context, state, rng, values, stats).await;
            }
        }
        WorkloadKind::YcsbE => {
            if rng.random_bool(0.95) {
                let length = rng.random_range(1..=100);
                scan(
                    db,
                    variant,
                    selector.sample_existing(state.acknowledged_insert(), rng),
                    length,
                    false,
                    "scan",
                    stats,
                )
                .await
            } else {
                return ycsb_insert(context, state, rng, values, stats).await;
            }
        }
        WorkloadKind::YcsbF => {
            async {
                if rng.random_bool(0.5) {
                    read(db, variant, selector.sample(rng), "read", stats).await
                } else {
                    let id = selector.sample(rng);
                    let key = key_for_variant(variant, id);
                    let previous = stats
                        .measure_api("get", db.get(key.clone()))
                        .await
                        .map_err(|error| OperationError::Other(error.into()))?
                        .context("read-modify-write key not found")
                        .map_err(OperationError::Other)?;
                    let value = values.generate(variant.value_bytes(), rng);
                    let handle = stats
                        .measure_api(
                            "put",
                            db.put_with_options(
                                key,
                                value.clone(),
                                &PutOptions::default(),
                                write_options,
                            ),
                        )
                        .await
                        .map_err(|error| OperationError::Other(error.into()))?;
                    Ok(OperationOutcome {
                        name: "read-modify-write",
                        payload: Payload::read_write(previous.len() as u64, value.len() as u64),
                        batch_keys: 0,
                        write_handle: Some(handle),
                        transaction_commit: false,
                    })
                }
            }
            .await
        }
        WorkloadKind::MultiRandomRead => multi_read(db, variant, selector, rng, stats).await,
        WorkloadKind::ForwardRange => {
            scan(db, variant, selector.sample(rng), 10, false, "scan", stats).await
        }
        WorkloadKind::ReverseRange => {
            scan(db, variant, selector.sample(rng), 10, true, "scan", stats).await
        }
        WorkloadKind::Overwrite => {
            update(context, selector.sample(rng), rng, values, "update", stats).await
        }
        WorkloadKind::SustainedIngest => {
            async {
                let id = state.next_insert.fetch_add(1, Ordering::Relaxed);
                let key = random_unique_key(id, variant.key_bytes(), rng);
                let value = values.generate(variant.value_bytes(), rng);
                let handle = stats
                    .measure_api(
                        "put",
                        db.put_with_options(
                            key,
                            value.clone(),
                            &PutOptions::default(),
                            write_options,
                        ),
                    )
                    .await
                    .map_err(|error| OperationError::Other(error.into()))?;
                Ok(OperationOutcome {
                    name: "insert",
                    payload: Payload::write(value.len() as u64),
                    batch_keys: 0,
                    write_handle: Some(handle),
                    transaction_commit: false,
                })
            }
            .await
        }
        WorkloadKind::TransactionContention => {
            transaction(db, variant, write_options, rng, values, stats).await
        }
        WorkloadKind::PrefixScan => prefix_scan(db, variant, rng, stats).await,
        other => Err(OperationError::Other(anyhow::anyhow!(
            "unsupported closed-loop operation {other:?}"
        ))),
    };
    OperationCompletion {
        result,
        latency: started.elapsed(),
    }
}

async fn ycsb_insert(
    context: &OperationContext<'_>,
    state: &ClosedLoopState,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) -> OperationCompletion {
    let started = Instant::now();
    let id = state.allocate_insert();
    let result = update(context, id, rng, values, "insert", stats).await;
    state.acknowledge_insert(id);
    OperationCompletion {
        result,
        latency: started.elapsed(),
    }
}

async fn read(
    db: &Db,
    variant: &VariantConfig,
    id: u64,
    name: &'static str,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let key = key_for_variant(variant, id);
    let key_bytes = key.len();
    let value = stats
        .measure_api("get", db.get(key))
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    let payload = match value {
        Some(value) => Payload::read_hit(point_payload_bytes(variant, key_bytes, value.len())),
        None => Payload::read_miss(),
    };
    Ok(OperationOutcome {
        name,
        payload,
        batch_keys: 0,
        write_handle: None,
        transaction_commit: false,
    })
}

async fn update(
    context: &OperationContext<'_>,
    id: u64,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    name: &'static str,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let db = context.db;
    let variant = context.variant;
    let key = key_for_variant(variant, id);
    let value = values.generate(variant.value_bytes(), rng);
    let payload_bytes = point_payload_bytes(variant, key.len(), value.len());
    let handle = stats
        .measure_api(
            "put",
            db.put_with_options(
                key,
                value.clone(),
                &PutOptions::default(),
                context.write_options,
            ),
        )
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    Ok(OperationOutcome {
        name,
        payload: Payload::write(payload_bytes),
        batch_keys: 0,
        write_handle: Some(handle),
        transaction_commit: false,
    })
}

async fn multi_read(
    db: &Db,
    variant: &VariantConfig,
    selector: &KeySelector,
    rng: &mut StdRng,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let keys = (0..10)
        .map(|_| key_for_variant(variant, selector.sample(rng)))
        .collect::<Vec<_>>();
    let values = join_all(keys.into_iter().map(|key| async {
        let key_bytes = key.len();
        let started = Instant::now();
        let result = db.get(key).await;
        (result, started.elapsed(), key_bytes)
    }))
    .await;
    let mut bytes = 0_u64;
    let mut hits = 0_u64;
    let mut misses = 0_u64;
    for (value, latency, key_bytes) in values {
        stats.record_api_latency("get", latency);
        match value.map_err(|error| OperationError::Other(error.into()))? {
            Some(value) => {
                hits = hits.saturating_add(1);
                bytes = bytes.saturating_add(point_payload_bytes(variant, key_bytes, value.len()));
            }
            None => misses = misses.saturating_add(1),
        }
    }
    Ok(OperationOutcome {
        name: "batch-read",
        payload: Payload::read_batch(bytes, hits, misses),
        batch_keys: 10,
        write_handle: None,
        transaction_commit: false,
    })
}

async fn scan(
    db: &Db,
    variant: &VariantConfig,
    start_id: u64,
    limit: usize,
    reverse: bool,
    name: &'static str,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let started = Instant::now();
    let result = async {
        let key = key_for_variant(variant, start_id);
        let mut iterator = if reverse {
            db.scan_with_options(
                ..=key,
                &ScanOptions::default().with_order(IterationOrder::Descending),
            )
            .await
        } else {
            db.scan(key..).await
        }
        .map_err(|error| OperationError::Other(error.into()))?;
        let mut bytes = 0_u64;
        let mut keys = 0_u64;
        while keys < limit as u64 {
            match iterator
                .next()
                .await
                .map_err(|error| OperationError::Other(error.into()))?
            {
                Some(value) => {
                    bytes = bytes.saturating_add((value.key.len() + value.value.len()) as u64);
                    keys += 1;
                }
                None => break,
            }
        }
        Ok(OperationOutcome {
            name,
            payload: Payload::read(bytes),
            batch_keys: keys,
            write_handle: None,
            transaction_commit: false,
        })
    }
    .await;
    stats.record_api_latency("scan", started.elapsed());
    result
}

async fn prefix_scan(
    db: &Db,
    variant: &VariantConfig,
    rng: &mut StdRng,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let started = Instant::now();
    let result = async {
        let prefix_count = (variant.record_count() / 10).max(1);
        let prefix = rng.random_range(0..prefix_count).to_be_bytes();
        let mut iterator = db
            .scan_prefix(prefix, ..)
            .await
            .map_err(|error| OperationError::Other(error.into()))?;
        let mut keys = 0_u64;
        let mut bytes = 0_u64;
        while let Some(value) = iterator
            .next()
            .await
            .map_err(|error| OperationError::Other(error.into()))?
        {
            keys += 1;
            bytes = bytes.saturating_add((value.key.len() + value.value.len()) as u64);
        }
        if keys != 10 {
            return Err(OperationError::Other(anyhow::anyhow!(
                "prefix scan returned {keys} records instead of 10"
            )));
        }
        Ok(OperationOutcome {
            name: "prefix-scan",
            payload: Payload::read(bytes),
            batch_keys: keys,
            write_handle: None,
            transaction_commit: false,
        })
    }
    .await;
    stats.record_api_latency("scan", started.elapsed());
    result
}

async fn transaction(
    db: &Db,
    variant: &VariantConfig,
    write_options: &WriteOptions,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let transaction = stats
        .measure_api(
            "transaction.begin",
            db.begin(IsolationLevel::SerializableSnapshot),
        )
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    let mut read_payload_bytes = 0_u64;
    let mut write_payload_bytes = 0_u64;
    let mut operations = [
        true, true, true, true, true, false, false, false, false, false,
    ];
    operations.shuffle(rng);
    for read_operation in operations {
        let id = rng.random_range(0..variant.record_count().max(1));
        let key = key_for_variant(variant, id);
        if read_operation {
            let value = stats
                .measure_api("transaction.get", transaction.get(key))
                .await
                .map_err(|error| OperationError::Other(error.into()))?
                .context("transaction read key not found")
                .map_err(OperationError::Other)?;
            read_payload_bytes = read_payload_bytes.saturating_add(value.len() as u64);
        } else {
            let value = values.generate(variant.value_bytes(), rng);
            write_payload_bytes = write_payload_bytes.saturating_add(value.len() as u64);
            stats
                .measure_api_sync("transaction.put", || transaction.put(key, value))
                .map_err(|error| OperationError::Other(error.into()))?;
        }
    }
    match stats
        .measure_api(
            "transaction.commit",
            transaction.commit_with_options(write_options),
        )
        .await
    {
        Ok(handle) => Ok(OperationOutcome {
            name: "transaction",
            payload: Payload::read_write(read_payload_bytes, write_payload_bytes),
            batch_keys: 10,
            write_handle: handle,
            transaction_commit: true,
        }),
        Err(error) if error.kind() == ErrorKind::Transaction => {
            Err(OperationError::TransactionConflict)
        }
        Err(error) => Err(OperationError::Other(error.into())),
    }
}

pub(super) async fn run_bulk_load(
    db: Arc<Db>,
    variant: &VariantConfig,
    durability: Option<DurabilitySender>,
    windows: Option<Arc<ApplicationWindowRegistry>>,
) -> Result<WorkerStats> {
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
    // db_bench's fillrandom issues individual puts. SlateDB's async call overhead
    // dominates this 900M-record setup, so batch independent puts while preserving
    // fillrandom sampling and record-level accounting.
    const BATCH_RECORDS: u64 = 1_024;

    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let count = variant.record_count();
    let write_options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let put_options = PutOptions::default();
    let mut stats = WorkerStats::with_window_recorder(
        windows
            .as_ref()
            .map(|windows| windows.register_window_recorder()),
    );
    let load_started = Instant::now();
    let mut last_reported_at = load_started;
    let mut last_reported_records = 0_u64;
    let mut last_reported_backpressure_ns = 0_u64;
    let logical_bytes_per_record = variant.key_bytes().saturating_add(variant.value_bytes()) as f64;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    let mut completed_records = 0_u64;
    while completed_records < count {
        let batch_records = count.saturating_sub(completed_records).min(BATCH_RECORDS);
        let mut batch = WriteBatch::new();
        let mut payload_bytes = 0_u64;
        for _ in 0..batch_records {
            let id = rng.random_range(0..count);
            let key = key_for_variant(variant, id);
            let value = values.generate(variant.value_bytes(), &mut rng);
            payload_bytes =
                payload_bytes.saturating_add(point_payload_bytes(variant, key.len(), value.len()));
            batch.put_bytes_with_options(key, value, &put_options);
        }
        let started = Instant::now();
        let mut write = Box::pin(measure_backpressure(
            db.write_with_options(batch, &write_options),
        ));
        let (result, backpressure) = loop {
            tokio::select! {
                result = &mut write => break result,
                _ = heartbeat.tick() => {
                    let now = Instant::now();
                    let interval = now.saturating_duration_since(last_reported_at);
                    let elapsed = now.saturating_duration_since(load_started);
                    let records = completed_records;
                    let recent_records = records.saturating_sub(last_reported_records);
                    let recent_puts_per_second =
                        recent_records as f64 / interval.as_secs_f64().max(f64::EPSILON);
                    let average_puts_per_second =
                        records as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
                    let recent_backpressure_ns = stats
                        .backpressure_ns
                        .saturating_sub(last_reported_backpressure_ns);
                    let backpressure_percent = (recent_backpressure_ns as f64 / 1e9
                        / interval.as_secs_f64().max(f64::EPSILON)
                        * 100.0)
                        .min(100.0);
                    let progress_percent = if count == 0 {
                        100.0
                    } else {
                        records as f64 / count as f64 * 100.0
                    };
                    let eta_seconds = if recent_puts_per_second > 0.0 {
                        count.saturating_sub(records) as f64 / recent_puts_per_second
                    } else {
                        -1.0
                    };
                    tracing::info!(
                        records_completed = records,
                        successful_records = stats.successful,
                        total_records = count,
                        errors = stats.errors,
                        progress_percent,
                        recent_puts_per_second,
                        average_puts_per_second,
                        logical_mib_per_second = recent_puts_per_second
                            * logical_bytes_per_record
                            / (1024.0 * 1024.0),
                        backpressure_percent,
                        elapsed_seconds = elapsed.as_secs_f64(),
                        eta_seconds,
                        "bulk-load progress"
                    );
                    last_reported_at = now;
                    last_reported_records = records;
                    last_reported_backpressure_ns = stats.backpressure_ns;
                }
            }
        };
        let return_latency = started.elapsed();
        stats.record_api_latency("write", return_latency);
        stats.record_batch_latency(return_latency);
        stats.record_backpressure(backpressure);
        match result {
            Ok(handle) => {
                let returned_at = Instant::now();
                stats.record_success_n(
                    "insert",
                    return_latency,
                    Payload::write(payload_bytes),
                    batch_records,
                );
                stats.record_write(returned_at, handle.seqnum());
                if let Some(tracker) = &durability {
                    tracker.accepted(handle.seqnum(), returned_at);
                }
            }
            Err(error) => {
                stats.record_error_n("insert", return_latency, batch_records);
                tracing::debug!(%error, batch_records, "bulk-load batch write failed");
            }
        }
        completed_records = completed_records.saturating_add(batch_records);
    }
    let elapsed = load_started.elapsed();
    let average_puts_per_second = count as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    tracing::info!(
        records_completed = count,
        successful_records = stats.successful,
        total_records = count,
        errors = stats.errors,
        progress_percent = 100.0,
        average_puts_per_second,
        logical_mib_per_second =
            average_puts_per_second * logical_bytes_per_record / (1024.0 * 1024.0),
        backpressure_percent =
            (stats.backpressure_ns as f64 / 1e9 / elapsed.as_secs_f64().max(f64::EPSILON) * 100.0)
                .min(100.0),
        elapsed_seconds = elapsed.as_secs_f64(),
        "bulk-load insert phase complete"
    );
    Ok(stats)
}

async fn run_with_capped_writer(
    db: Arc<Db>,
    variant: &VariantConfig,
    duration: Duration,
    durability: Option<DurabilitySender>,
    windows: Option<Arc<ApplicationWindowRegistry>>,
    state: &ClosedLoopState,
) -> Result<WorkerStats> {
    let clients = variant.clients;
    let deadline = Instant::now() + duration;
    let read_kind = variant
        .workload
        .kind
        .while_writing_read_kind()
        .context("invalid while-writing workload")?;
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let db = Arc::clone(&db);
        let mut reader_variant = variant.clone();
        reader_variant.workload.kind = read_kind;
        reader_variant.workload.await_durable = false;
        let windows = windows.clone();
        let state = state.clone();
        tasks.spawn(async move {
            worker_loop(db, reader_variant, deadline, None, windows, state).await
        });
    }
    let writer_db = db;
    let writer_variant = variant.clone();
    tasks.spawn(async move {
        capped_writer(writer_db, writer_variant, deadline, durability, windows).await
    });
    merge_tasks(tasks).await
}

async fn capped_writer(
    db: Arc<Db>,
    variant: VariantConfig,
    deadline: Instant,
    durability: Option<DurabilitySender>,
    windows: Option<Arc<ApplicationWindowRegistry>>,
) -> Result<WorkerStats> {
    const TARGET_BYTES_PER_SECOND: u64 = 2 * 1024 * 1024;
    const MAX_IN_FLIGHT: usize = 1024;

    let logical_bytes_per_write = variant.key_bytes().saturating_add(variant.value_bytes()) as u64;
    let period =
        Duration::from_secs_f64(logical_bytes_per_write as f64 / TARGET_BYTES_PER_SECOND as f64);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let selector = KeySelector::uniform(variant.record_count());
    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let mut stats = WorkerStats::with_window_recorder(
        windows
            .as_ref()
            .map(|windows| windows.register_window_recorder()),
    );
    stats.background_writer_target_bytes_per_second = Some(TARGET_BYTES_PER_SECOND);
    let mut in_flight = FuturesUnordered::new();
    let tokio_deadline = tokio::time::Instant::from_std(deadline);

    loop {
        if Instant::now() >= deadline {
            break;
        }
        if in_flight.len() >= MAX_IN_FLIGHT {
            let completion = in_flight
                .next()
                .await
                .context("capped writer pipeline unexpectedly became empty")?;
            record_capped_write(&mut stats, completion, durability.as_ref());
            continue;
        }

        tokio::select! {
            _ = ticker.tick() => {
                let key = key_for_variant(&variant, selector.sample(&mut rng));
                let value = values.generate(variant.value_bytes(), &mut rng);
                in_flight.push(execute_capped_write(
                    Arc::clone(&db),
                    key,
                    value,
                    Instant::now(),
                ));
            }
            completion = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(completion) = completion {
                    record_capped_write(&mut stats, completion, durability.as_ref());
                }
            }
            _ = tokio::time::sleep_until(tokio_deadline) => break,
        }
    }

    while let Some(completion) = in_flight.next().await {
        record_capped_write(&mut stats, completion, durability.as_ref());
    }
    Ok(stats)
}

struct CappedWriteCompletion {
    result: std::result::Result<WriteHandle, slatedb::Error>,
    return_latency: Duration,
    api_latency: Duration,
    backpressure: Duration,
    value_bytes: u64,
    logical_bytes: u64,
}

async fn execute_capped_write(
    db: Arc<Db>,
    key: Bytes,
    value: Bytes,
    started: Instant,
) -> CappedWriteCompletion {
    let value_bytes = value.len() as u64;
    let logical_bytes = key.len().saturating_add(value.len()) as u64;
    let options = WriteOptions {
        await_durable: true,
        ..Default::default()
    };
    let api_started = Instant::now();
    let (result, backpressure) =
        measure_backpressure(db.put_with_options(key, value, &PutOptions::default(), &options))
            .await;
    CappedWriteCompletion {
        result,
        return_latency: started.elapsed(),
        api_latency: api_started.elapsed(),
        backpressure,
        value_bytes,
        logical_bytes,
    }
}

fn record_capped_write(
    stats: &mut WorkerStats,
    completion: CappedWriteCompletion,
    durability: Option<&DurabilitySender>,
) {
    stats.record_api_latency("put", completion.api_latency);
    stats.record_backpressure(completion.backpressure);
    match completion.result {
        Ok(handle) => {
            let returned_at = Instant::now();
            stats.record_background_writer_success(
                "writer-update",
                completion.return_latency,
                Payload::write(completion.value_bytes),
                completion.logical_bytes,
            );
            stats.record_write(returned_at, handle.seqnum());
            if let Some(tracker) = durability {
                tracker.accepted(handle.seqnum(), returned_at);
            }
        }
        Err(error) => {
            stats.record_background_error("writer-update", completion.return_latency);
            tracing::debug!(%error, "capped writer failed");
        }
    }
}

async fn merge_tasks(mut tasks: JoinSet<Result<WorkerStats>>) -> Result<WorkerStats> {
    let mut merged = WorkerStats::default();
    while let Some(result) = tasks.join_next().await {
        merged.merge(&result.context("joining benchmark client")??)?;
    }
    Ok(merged)
}

pub async fn populate_dataset(
    db: Arc<Db>,
    suite: &str,
    record_count: u64,
    key_bytes: usize,
    value_bytes: usize,
    value_compression_ratio: f64,
    prefix_layout: bool,
) -> Result<()> {
    // Golden-database creation is unmeasured setup, so batch independent records
    // to avoid making one asynchronous database call per record.
    const BATCH_RECORDS: u64 = 1_024;

    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(value_compression_ratio);
    let options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let put_options = PutOptions::default();
    let mut loaded_records = 0_u64;
    while loaded_records < record_count {
        let batch_end = loaded_records
            .saturating_add(BATCH_RECORDS)
            .min(record_count);
        let mut batch = WriteBatch::new();
        for id in loaded_records..batch_end {
            let key = if prefix_layout {
                prefix_key(id / 10, id % 10)
            } else {
                key_for_suite(suite, id, key_bytes)
            };
            batch.put_bytes_with_options(key, values.generate(value_bytes, &mut rng), &put_options);
        }
        db.write_with_options(batch, &options)
            .await
            .with_context(|| format!("loading records {loaded_records}..{batch_end}"))?;
        loaded_records = batch_end;
    }
    db.flush().await.context("flushing loaded dataset")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        capped_writer, key_for_suite, point_payload_bytes, populate_dataset, run_bulk_load,
        ClosedLoopState,
    };
    use crate::config::BenchmarkConfig;
    use anyhow::Result;
    use object_store::memory::InMemory;
    use slatedb::config::Settings;
    use slatedb::Db;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn rocksdb_point_payload_includes_keys_without_changing_ycsb_payload() -> Result<()> {
        let benchmark = BenchmarkConfig::load_from(Path::new("config"))?;
        let rocksdb = benchmark
            .select(Some("rocksdb"), Some("random-read"), None)?
            .into_iter()
            .next()
            .expect("RocksDB random-read variant");
        let ycsb = benchmark
            .select(Some("ycsb"), Some("ycsb-c"), None)?
            .into_iter()
            .next()
            .expect("YCSB C variant");

        assert_eq!(point_payload_bytes(&rocksdb, 20, 400), 420);
        assert_eq!(point_payload_bytes(&ycsb, 16, 1024), 1024);
        Ok(())
    }

    #[test]
    fn insert_frontier_advances_after_out_of_order_completions() {
        let state = ClosedLoopState::new(10);
        let first = state.allocate_insert();
        let second = state.allocate_insert();
        let third = state.allocate_insert();

        assert_eq!((first, second, third), (10, 11, 12));
        state.acknowledge_insert(second);
        assert_eq!(state.acknowledged_insert(), 10);
        state.acknowledge_insert(first);
        assert_eq!(state.acknowledged_insert(), 12);
        state.acknowledge_insert(third);
        assert_eq!(state.acknowledged_insert(), 13);
    }

    #[tokio::test]
    async fn bulk_load_batches_writes_but_counts_records() -> Result<()> {
        let benchmark = BenchmarkConfig::load_from(Path::new("config"))?;
        let mut variant = benchmark
            .select(Some("rocksdb"), Some("bulk-load"), None)?
            .pop()
            .expect("configured bulk-load variant");
        variant.workload.record_count = Some(2_050);
        let db = Arc::new(
            Db::builder("bulk-load-batch-test", Arc::new(InMemory::new()))
                .build()
                .await?,
        );

        let stats = run_bulk_load(Arc::clone(&db), &variant, None, None).await?;

        assert_eq!(stats.total, 2_050);
        assert_eq!(stats.successful, 2_050);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.writes, 3);
        assert_eq!(stats.write_payload_bytes, 2_050 * 420);
        assert_eq!(
            stats
                .histograms
                .get("return/insert")
                .expect("insert return histogram")
                .len(),
            2_050
        );
        assert_eq!(
            stats
                .histograms
                .get("api/write")
                .expect("write API histogram")
                .len(),
            3
        );
        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn populate_dataset_writes_across_batch_boundaries() -> Result<()> {
        let db = Arc::new(
            Db::builder("populate-dataset-batch-test", Arc::new(InMemory::new()))
                .build()
                .await?,
        );

        populate_dataset(Arc::clone(&db), "ycsb", 1_025, 16, 64, 1.0, false).await?;

        for id in [0, 1_023, 1_024] {
            let value = db
                .get(key_for_suite("ycsb", id, 16))
                .await?
                .expect("populated record");
            assert_eq!(value.len(), 64);
        }
        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn capped_writer_pipelines_durable_writes() -> Result<()> {
        let benchmark = BenchmarkConfig::load_from(Path::new("config"))?;
        let variant = benchmark
            .select(Some("rocksdb"), Some("read-while-writing"), None)?
            .pop()
            .expect("configured while-writing variant");
        let settings = Settings {
            flush_interval: Some(Duration::from_millis(100)),
            compactor_options: None,
            ..Default::default()
        };
        let db = Arc::new(
            Db::builder("capped-writer-test", Arc::new(InMemory::new()))
                .with_settings(settings)
                .build()
                .await?,
        );

        let started = Instant::now();
        let stats = capped_writer(
            Arc::clone(&db),
            variant,
            started + Duration::from_millis(350),
            None,
            None,
        )
        .await?;
        let elapsed = started.elapsed();

        let writer_operations = stats
            .histograms
            .get("return/writer-update")
            .expect("writer return histogram")
            .len();
        assert!(
            writer_operations > 100,
            "expected pipelined writes, got {writer_operations}"
        );
        assert_eq!(stats.successful, 0);
        assert_eq!(stats.errors, 0);
        assert_eq!(
            stats.background_writer_target_bytes_per_second,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(
            stats.background_writer_logical_bytes,
            writer_operations * 420
        );
        let achieved = stats
            .application(elapsed)
            .background_writer_achieved_mib_per_second
            .expect("achieved writer throughput");
        assert!(
            (1.0..=2.05).contains(&achieved),
            "expected writer close to its 2 MiB/s cap, got {achieved} MiB/s"
        );
        db.close().await?;
        Ok(())
    }
}
