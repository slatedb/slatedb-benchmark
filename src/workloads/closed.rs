use super::durability::DurabilitySender;
use super::stats::{Payload, WorkerStats};
use super::util::{
    choose_coprime_multiplier, key_for_id, prefix_key, random_unique_key, KeySelector,
    ValueGenerator,
};
use crate::config::{VariantConfig, WorkloadKind};
use crate::system::{measure_backpressure, ApplicationCounters};
use anyhow::{bail, Context, Result};
use futures::future::join_all;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, ScanOptions, WriteOptions};
use slatedb::{Db, ErrorKind, IsolationLevel, IterationOrder, WriteHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Clone)]
pub struct ClosedLoopState {
    next_insert: Arc<AtomicU64>,
    insert_lock: Arc<Mutex<()>>,
}

impl ClosedLoopState {
    pub fn new(next_insert: u64) -> Self {
        Self {
            next_insert: Arc::new(AtomicU64::new(next_insert)),
            insert_lock: Arc::new(Mutex::new(())),
        }
    }
}

pub async fn run_closed_phase(
    db: Arc<Db>,
    variant: &VariantConfig,
    duration: Duration,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
    state: &ClosedLoopState,
) -> Result<WorkerStats> {
    if variant.workload.kind == WorkloadKind::BulkLoad {
        return run_bulk_load(db, variant, durability, counters).await;
    }
    if matches!(
        variant.workload.kind,
        WorkloadKind::ReadWhileWriting
            | WorkloadKind::ForwardRangeWhileWriting
            | WorkloadKind::ReverseRangeWhileWriting
    ) {
        return run_with_capped_writer(db, variant, duration, durability, counters, state).await;
    }

    let clients = variant.clients;
    let deadline = Instant::now() + duration;
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let db = Arc::clone(&db);
        let variant = variant.clone();
        let durability = durability.clone();
        let counters = counters.clone();
        let state = state.clone();
        tasks.spawn(async move {
            worker_loop(db, variant, deadline, durability, counters, state).await
        });
    }
    merge_tasks(tasks).await
}

async fn worker_loop(
    db: Arc<Db>,
    variant: VariantConfig,
    deadline: Instant,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
    state: ClosedLoopState,
) -> Result<WorkerStats> {
    let mut rng = StdRng::from_os_rng();
    let selector = if matches!(
        variant.workload.kind,
        WorkloadKind::YcsbA | WorkloadKind::YcsbB | WorkloadKind::YcsbC | WorkloadKind::YcsbF
    ) {
        KeySelector::zipfian(variant.record_count())
    } else {
        KeySelector::uniform(variant.record_count())
    };
    let write_options = WriteOptions {
        await_durable: variant.workload.await_durable,
        ..Default::default()
    };
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let mut stats = WorkerStats::with_window_recorder(
        counters
            .as_ref()
            .map(|counters| counters.register_window_recorder()),
    );
    while Instant::now() < deadline {
        let (completion, backpressure) = measure_backpressure(execute_operation(
            &db,
            &variant,
            &selector,
            &write_options,
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
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(OperationError::TransactionConflict) => {
                stats.record_transaction_conflict(completion.latency);
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(OperationError::Other(error)) => {
                stats.record_error(operation_name(variant.workload.kind), completion.latency);
                if let Some(counters) = &counters {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                }
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

async fn execute_operation(
    db: &Db,
    variant: &VariantConfig,
    selector: &KeySelector,
    write_options: &WriteOptions,
    state: &ClosedLoopState,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) -> OperationCompletion {
    let started = Instant::now();
    let kind = variant.workload.kind;
    let result = match kind {
        WorkloadKind::YcsbA => {
            if rng.random_bool(0.5) {
                read(db, variant, selector.sample(rng), "read", stats).await
            } else {
                update(
                    db,
                    variant,
                    selector.sample(rng),
                    write_options,
                    rng,
                    values,
                    "update",
                    stats,
                )
                .await
            }
        }
        WorkloadKind::YcsbB => {
            if rng.random_bool(0.95) {
                read(db, variant, selector.sample(rng), "read", stats).await
            } else {
                update(
                    db,
                    variant,
                    selector.sample(rng),
                    write_options,
                    rng,
                    values,
                    "update",
                    stats,
                )
                .await
            }
        }
        WorkloadKind::YcsbC | WorkloadKind::ColdRead | WorkloadKind::RandomRead => {
            read(db, variant, selector.sample(rng), "read", stats).await
        }
        WorkloadKind::YcsbD => {
            if rng.random_bool(0.95) {
                let latest = state.next_insert.load(Ordering::Acquire).max(1);
                let window = latest.min(10_000);
                let id = latest.saturating_sub(1 + rng.random_range(0..window));
                read(db, variant, id, "read", stats).await
            } else {
                return ycsb_d_insert(db, variant, write_options, state, rng, values, stats).await;
            }
        }
        WorkloadKind::YcsbE => {
            if rng.random_bool(0.95) {
                let length = rng.random_range(1..=100);
                scan(
                    db,
                    variant,
                    selector.sample(rng),
                    length,
                    false,
                    "scan",
                    stats,
                )
                .await
            } else {
                let id = state.next_insert.fetch_add(1, Ordering::Relaxed);
                update(db, variant, id, write_options, rng, values, "insert", stats).await
            }
        }
        WorkloadKind::YcsbF => {
            async {
                if rng.random_bool(0.5) {
                    read(db, variant, selector.sample(rng), "read", stats).await
                } else {
                    let id = selector.sample(rng);
                    let key = key_for_id(id, variant.key_bytes());
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
            update(
                db,
                variant,
                selector.sample(rng),
                write_options,
                rng,
                values,
                "update",
                stats,
            )
            .await
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

async fn ycsb_d_insert(
    db: &Db,
    variant: &VariantConfig,
    write_options: &WriteOptions,
    state: &ClosedLoopState,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    stats: &mut WorkerStats,
) -> OperationCompletion {
    let _guard = state.insert_lock.lock().await;
    let started = Instant::now();
    let id = state.next_insert.load(Ordering::Relaxed);
    let result = update(db, variant, id, write_options, rng, values, "insert", stats).await;
    if result.is_ok() {
        state
            .next_insert
            .store(id.saturating_add(1), Ordering::Release);
    }
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
    let value = stats
        .measure_api("get", db.get(key_for_id(id, variant.key_bytes())))
        .await
        .map_err(|error| OperationError::Other(error.into()))?
        .context("benchmark read key not found")
        .map_err(OperationError::Other)?;
    Ok(OperationOutcome {
        name,
        payload: Payload::read(value.len() as u64),
        batch_keys: 0,
        write_handle: None,
        transaction_commit: false,
    })
}

async fn update(
    db: &Db,
    variant: &VariantConfig,
    id: u64,
    write_options: &WriteOptions,
    rng: &mut StdRng,
    values: &mut ValueGenerator,
    name: &'static str,
    stats: &mut WorkerStats,
) -> std::result::Result<OperationOutcome, OperationError> {
    let value = values.generate(variant.value_bytes(), rng);
    let handle = stats
        .measure_api(
            "put",
            db.put_with_options(
                key_for_id(id, variant.key_bytes()),
                value.clone(),
                &PutOptions::default(),
                write_options,
            ),
        )
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    Ok(OperationOutcome {
        name,
        payload: Payload::write(value.len() as u64),
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
        .map(|_| key_for_id(selector.sample(rng), variant.key_bytes()))
        .collect::<Vec<_>>();
    let values = join_all(keys.into_iter().map(|key| async {
        let started = Instant::now();
        let result = db.get(key).await;
        (result, started.elapsed())
    }))
    .await;
    let mut bytes = 0_u64;
    for (value, latency) in values {
        stats.record_api_latency("get", latency);
        bytes = bytes.saturating_add(
            value
                .map_err(|error| OperationError::Other(error.into()))?
                .context("multi-random-read key not found")
                .map_err(OperationError::Other)?
                .len() as u64,
        );
    }
    Ok(OperationOutcome {
        name: "batch-read",
        payload: Payload::read(bytes),
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
        let key = key_for_id(start_id, variant.key_bytes());
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
        let key = key_for_id(id, variant.key_bytes());
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

async fn run_bulk_load(
    db: Arc<Db>,
    variant: &VariantConfig,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
) -> Result<WorkerStats> {
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let count = variant.record_count();
    let multiplier = choose_coprime_multiplier(count, &mut rng);
    let offset = if count > 0 {
        rng.random_range(0..count)
    } else {
        0
    };
    let write_options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    let put_options = PutOptions::default();
    let mut stats = WorkerStats::with_window_recorder(
        counters
            .as_ref()
            .map(|counters| counters.register_window_recorder()),
    );
    let load_started = Instant::now();
    let mut last_reported_at = load_started;
    let mut last_reported_records = 0_u64;
    let mut last_reported_backpressure_ns = 0_u64;
    let logical_bytes_per_record = variant.key_bytes().saturating_add(variant.value_bytes()) as f64;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    for sequence in 0..count {
        let id = if count > 0 {
            (sequence.wrapping_mul(multiplier).wrapping_add(offset)) % count
        } else {
            0
        };
        let key = key_for_id(id, variant.key_bytes());
        let value = values.generate(variant.value_bytes(), &mut rng);
        let started = Instant::now();
        let mut put = Box::pin(measure_backpressure(db.put_with_options(
            key,
            value.clone(),
            &put_options,
            &write_options,
        )));
        let (result, backpressure) = loop {
            tokio::select! {
                result = &mut put => break result,
                _ = heartbeat.tick() => {
                    let now = Instant::now();
                    let interval = now.saturating_duration_since(last_reported_at);
                    let elapsed = now.saturating_duration_since(load_started);
                    let records = sequence;
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
        stats.record_api_latency("put", started.elapsed());
        stats.record_backpressure(backpressure);
        match result {
            Ok(handle) => {
                let returned_at = Instant::now();
                stats.record_success(
                    "insert",
                    started.elapsed(),
                    Payload::write(value.len() as u64),
                );
                stats.record_write(returned_at, handle.seqnum());
                if let Some(tracker) = &durability {
                    tracker.accepted(handle.seqnum(), returned_at);
                }
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(error) => {
                stats.record_error("insert", started.elapsed());
                if let Some(counters) = &counters {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                }
                tracing::debug!(%error, "bulk-load write failed");
            }
        }
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
    counters: Option<Arc<ApplicationCounters>>,
    state: &ClosedLoopState,
) -> Result<WorkerStats> {
    let clients = variant.clients;
    let deadline = Instant::now() + duration;
    let read_kind = match variant.workload.kind {
        WorkloadKind::ReadWhileWriting => WorkloadKind::RandomRead,
        WorkloadKind::ForwardRangeWhileWriting => WorkloadKind::ForwardRange,
        WorkloadKind::ReverseRangeWhileWriting => WorkloadKind::ReverseRange,
        _ => bail!("invalid while-writing workload"),
    };
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let db = Arc::clone(&db);
        let mut reader_variant = variant.clone();
        reader_variant.workload.kind = read_kind;
        reader_variant.workload.await_durable = false;
        let counters = counters.clone();
        let state = state.clone();
        tasks.spawn(async move {
            worker_loop(db, reader_variant, deadline, None, counters, state).await
        });
    }
    let writer_db = db;
    let writer_variant = variant.clone();
    tasks.spawn(async move {
        capped_writer(writer_db, writer_variant, deadline, durability, counters).await
    });
    merge_tasks(tasks).await
}

async fn capped_writer(
    db: Arc<Db>,
    variant: VariantConfig,
    deadline: Instant,
    durability: Option<DurabilitySender>,
    counters: Option<Arc<ApplicationCounters>>,
) -> Result<WorkerStats> {
    let bytes_per_second = 2 * 1024 * 1024_u64;
    let ops_per_second = (bytes_per_second / variant.value_bytes() as u64).max(1);
    let period = Duration::from_secs_f64(1.0 / ops_per_second as f64);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let selector = KeySelector::uniform(variant.record_count());
    let options = WriteOptions {
        await_durable: true,
        ..Default::default()
    };
    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(variant.value_compression_ratio());
    let mut stats = WorkerStats::with_window_recorder(
        counters
            .as_ref()
            .map(|counters| counters.register_window_recorder()),
    );
    while Instant::now() < deadline {
        ticker.tick().await;
        let started = Instant::now();
        let value = values.generate(variant.value_bytes(), &mut rng);
        let (result, backpressure) = measure_backpressure(stats.measure_api(
            "put",
            db.put_with_options(
                key_for_id(selector.sample(&mut rng), variant.key_bytes()),
                value.clone(),
                &PutOptions::default(),
                &options,
            ),
        ))
        .await;
        stats.record_backpressure(backpressure);
        match result {
            Ok(handle) => {
                let returned_at = Instant::now();
                stats.record_background_success(
                    "writer-update",
                    started.elapsed(),
                    Payload::write(value.len() as u64),
                );
                stats.record_write(returned_at, handle.seqnum());
                if let Some(tracker) = &durability {
                    tracker.accepted(handle.seqnum(), returned_at);
                }
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(error) => {
                stats.record_background_error("writer-update", started.elapsed());
                if let Some(counters) = &counters {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                }
                tracing::debug!(%error, "capped writer failed");
            }
        }
    }
    Ok(stats)
}

async fn merge_tasks(mut tasks: JoinSet<Result<WorkerStats>>) -> Result<WorkerStats> {
    let mut merged = WorkerStats::default();
    while let Some(result) = tasks.join_next().await {
        merged.merge(&result.context("joining benchmark client")??)?;
    }
    Ok(merged)
}

fn operation_name(kind: WorkloadKind) -> &'static str {
    match kind {
        WorkloadKind::TransactionContention => "transaction",
        WorkloadKind::PrefixScan => "prefix-scan",
        WorkloadKind::MultiRandomRead => "batch-read",
        WorkloadKind::ForwardRange | WorkloadKind::ReverseRange | WorkloadKind::YcsbE => "scan",
        WorkloadKind::SustainedIngest | WorkloadKind::BulkLoad | WorkloadKind::YcsbD => "insert",
        WorkloadKind::Overwrite => "update",
        _ => "operation",
    }
}

pub async fn populate_dataset(
    db: Arc<Db>,
    record_count: u64,
    key_bytes: usize,
    value_bytes: usize,
    value_compression_ratio: f64,
    prefix_layout: bool,
) -> Result<()> {
    let mut rng = StdRng::from_os_rng();
    let mut values = ValueGenerator::new(value_compression_ratio);
    let options = WriteOptions {
        await_durable: false,
        ..Default::default()
    };
    for id in 0..record_count {
        let key = if prefix_layout {
            prefix_key(id / 10, id % 10)
        } else {
            key_for_id(id, key_bytes)
        };
        db.put_with_options(
            key,
            values.generate(value_bytes, &mut rng),
            &PutOptions::default(),
            &options,
        )
        .await
        .with_context(|| format!("loading record {id}"))?;
    }
    db.flush().await.context("flushing loaded dataset")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ycsb_d_insert, ClosedLoopState, WorkerStats};
    use crate::config::BenchmarkConfig;
    use crate::workloads::util::ValueGenerator;
    use anyhow::{Context, Result};
    use object_store::memory::InMemory;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use slatedb::config::WriteOptions;
    use slatedb::Db;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn ycsb_d_insert_latency_excludes_insert_lock_wait() -> Result<()> {
        let benchmark = BenchmarkConfig::load_from(std::path::Path::new("config"))?;
        let variant = benchmark
            .select(Some("ycsb"), Some("ycsb-d"), Some("clients-64"))?
            .pop()
            .context("missing YCSB-D test variant")?;
        let db = Db::open("ycsb-d-insert-latency-test", Arc::new(InMemory::new())).await?;
        let state = ClosedLoopState::new(1);
        let held_lock = Arc::clone(&state.insert_lock).lock_owned().await;
        let lock_delay = Duration::from_millis(50);
        let release = tokio::spawn(async move {
            tokio::time::sleep(lock_delay).await;
            drop(held_lock);
        });
        let mut rng = StdRng::seed_from_u64(1);
        let mut values = ValueGenerator::new(variant.value_compression_ratio());
        let mut stats = WorkerStats::default();
        let write_options = WriteOptions {
            await_durable: false,
            ..Default::default()
        };

        let wall_started = Instant::now();
        let completion = ycsb_d_insert(
            &db,
            &variant,
            &write_options,
            &state,
            &mut rng,
            &mut values,
            &mut stats,
        )
        .await;
        let wall_elapsed = wall_started.elapsed();
        release.await.context("joining insert-lock release task")?;

        assert!(completion.result.is_ok());
        assert_eq!(
            state.next_insert.load(std::sync::atomic::Ordering::Acquire),
            2
        );
        assert!(
            wall_elapsed.saturating_sub(completion.latency) >= lock_delay / 2,
            "reported latency {:?} did not exclude lock wait from wall time {:?}",
            completion.latency,
            wall_elapsed
        );
        db.close().await?;
        Ok(())
    }
}
