use super::durability::DurabilitySender;
use super::stats::WorkerStats;
use super::util::{
    choose_coprime_multiplier, key_for_id, prefix_key, random_unique_key, random_value, KeySelector,
};
use crate::config::{VariantConfig, WorkloadKind};
use crate::system::ApplicationCounters;
use anyhow::{bail, Context, Result};
use futures::future::try_join_all;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use slatedb::config::{PutOptions, ScanOptions, WriteOptions};
use slatedb::{Db, IsolationLevel, IterationOrder, WriteHandle};
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

    let clients = variant
        .clients
        .context("closed-loop variant has no clients")?;
    let deadline = Instant::now() + duration;
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let db = Arc::clone(&db);
        let variant = variant.clone();
        let durability = durability.clone();
        let counters = counters.clone();
        let next_insert = Arc::clone(&state.next_insert);
        let insert_lock = Arc::clone(&state.insert_lock);
        tasks.spawn(async move {
            worker_loop(
                db,
                variant,
                deadline,
                durability,
                counters,
                next_insert,
                insert_lock,
            )
            .await
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
    next_insert: Arc<AtomicU64>,
    insert_lock: Arc<Mutex<()>>,
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
    let mut stats = WorkerStats::default();
    while Instant::now() < deadline {
        let started = Instant::now();
        match execute_operation(
            &db,
            &variant,
            &selector,
            &write_options,
            &next_insert,
            &insert_lock,
            &mut rng,
        )
        .await
        {
            Ok(outcome) => {
                let returned_at = Instant::now();
                stats.record_success(outcome.name, started.elapsed(), outcome.payload_bytes);
                stats.batch_keys = stats.batch_keys.saturating_add(outcome.batch_keys);
                if outcome.batch_keys > 0 {
                    stats.histograms.record("batch", started.elapsed());
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
                stats.total += 1;
                stats.transaction_aborts += 1;
                stats.transaction_conflicts += 1;
                stats.histograms.record("return", started.elapsed());
                stats
                    .histograms
                    .record("return/transaction", started.elapsed());
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(OperationError::Other(error)) => {
                stats.record_error(operation_name(variant.workload.kind), started.elapsed());
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
    payload_bytes: u64,
    batch_keys: u64,
    write_handle: Option<WriteHandle>,
    transaction_commit: bool,
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
    next_insert: &AtomicU64,
    insert_lock: &Mutex<()>,
    rng: &mut StdRng,
) -> std::result::Result<OperationOutcome, OperationError> {
    let kind = variant.workload.kind;
    match kind {
        WorkloadKind::YcsbA => {
            if rng.random_bool(0.5) {
                read(db, variant, selector.sample(rng), "read").await
            } else {
                update(
                    db,
                    variant,
                    selector.sample(rng),
                    write_options,
                    rng,
                    "update",
                )
                .await
            }
        }
        WorkloadKind::YcsbB => {
            if rng.random_bool(0.95) {
                read(db, variant, selector.sample(rng), "read").await
            } else {
                update(
                    db,
                    variant,
                    selector.sample(rng),
                    write_options,
                    rng,
                    "update",
                )
                .await
            }
        }
        WorkloadKind::YcsbC | WorkloadKind::ColdRead | WorkloadKind::RandomRead => {
            read(db, variant, selector.sample(rng), "read").await
        }
        WorkloadKind::YcsbD => {
            if rng.random_bool(0.95) {
                let latest = next_insert.load(Ordering::Acquire).max(1);
                let window = latest.min(10_000);
                let id = latest.saturating_sub(1 + rng.random_range(0..window));
                read(db, variant, id, "read").await
            } else {
                let _guard = insert_lock.lock().await;
                let id = next_insert.load(Ordering::Relaxed);
                let outcome = update(db, variant, id, write_options, rng, "insert").await?;
                next_insert.store(id.saturating_add(1), Ordering::Release);
                Ok(outcome)
            }
        }
        WorkloadKind::YcsbE => {
            if rng.random_bool(0.95) {
                let length = rng.random_range(1..=100);
                scan(db, variant, selector.sample(rng), length, false, "scan").await
            } else {
                let id = next_insert.fetch_add(1, Ordering::Relaxed);
                update(db, variant, id, write_options, rng, "insert").await
            }
        }
        WorkloadKind::YcsbF => {
            if rng.random_bool(0.5) {
                read(db, variant, selector.sample(rng), "read").await
            } else {
                let id = selector.sample(rng);
                let key = key_for_id(id, variant.key_bytes());
                let previous = db
                    .get(key.clone())
                    .await
                    .map_err(|error| OperationError::Other(error.into()))?
                    .context("read-modify-write key not found")
                    .map_err(OperationError::Other)?;
                let value = random_value(variant.value_bytes(), rng);
                let handle = db
                    .put_with_options(key, value.clone(), &PutOptions::default(), write_options)
                    .await
                    .map_err(|error| OperationError::Other(error.into()))?;
                Ok(OperationOutcome {
                    name: "read-modify-write",
                    payload_bytes: (previous.len() + value.len()) as u64,
                    batch_keys: 0,
                    write_handle: Some(handle),
                    transaction_commit: false,
                })
            }
        }
        WorkloadKind::MultiRandomRead => multi_read(db, variant, selector, rng).await,
        WorkloadKind::ForwardRange => {
            scan(db, variant, selector.sample(rng), 10, false, "scan").await
        }
        WorkloadKind::ReverseRange => {
            scan(db, variant, selector.sample(rng), 10, true, "scan").await
        }
        WorkloadKind::Overwrite => {
            update(
                db,
                variant,
                selector.sample(rng),
                write_options,
                rng,
                "update",
            )
            .await
        }
        WorkloadKind::SustainedIngest => {
            let id = next_insert.fetch_add(1, Ordering::Relaxed);
            let key = random_unique_key(id, variant.key_bytes(), rng);
            let value = random_value(variant.value_bytes(), rng);
            let handle = db
                .put_with_options(key, value.clone(), &PutOptions::default(), write_options)
                .await
                .map_err(|error| OperationError::Other(error.into()))?;
            Ok(OperationOutcome {
                name: "insert",
                payload_bytes: value.len() as u64,
                batch_keys: 0,
                write_handle: Some(handle),
                transaction_commit: false,
            })
        }
        WorkloadKind::TransactionContention => transaction(db, variant, write_options, rng).await,
        WorkloadKind::PrefixScan => prefix_scan(db, variant, rng).await,
        other => Err(OperationError::Other(anyhow::anyhow!(
            "unsupported closed-loop operation {other:?}"
        ))),
    }
}

async fn read(
    db: &Db,
    variant: &VariantConfig,
    id: u64,
    name: &'static str,
) -> std::result::Result<OperationOutcome, OperationError> {
    let value = db
        .get(key_for_id(id, variant.key_bytes()))
        .await
        .map_err(|error| OperationError::Other(error.into()))?
        .context("benchmark read key not found")
        .map_err(OperationError::Other)?;
    Ok(OperationOutcome {
        name,
        payload_bytes: value.len() as u64,
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
    name: &'static str,
) -> std::result::Result<OperationOutcome, OperationError> {
    let value = random_value(variant.value_bytes(), rng);
    let handle = db
        .put_with_options(
            key_for_id(id, variant.key_bytes()),
            value.clone(),
            &PutOptions::default(),
            write_options,
        )
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    Ok(OperationOutcome {
        name,
        payload_bytes: value.len() as u64,
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
) -> std::result::Result<OperationOutcome, OperationError> {
    let keys = (0..10)
        .map(|_| key_for_id(selector.sample(rng), variant.key_bytes()))
        .collect::<Vec<_>>();
    let values = try_join_all(keys.into_iter().map(|key| db.get(key)))
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    let mut bytes = 0_u64;
    for value in values {
        bytes = bytes.saturating_add(
            value
                .context("multi-random-read key not found")
                .map_err(OperationError::Other)?
                .len() as u64,
        );
    }
    Ok(OperationOutcome {
        name: "batch-read",
        payload_bytes: bytes,
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
) -> std::result::Result<OperationOutcome, OperationError> {
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
        payload_bytes: bytes,
        batch_keys: keys,
        write_handle: None,
        transaction_commit: false,
    })
}

async fn prefix_scan(
    db: &Db,
    variant: &VariantConfig,
    rng: &mut StdRng,
) -> std::result::Result<OperationOutcome, OperationError> {
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
        payload_bytes: bytes,
        batch_keys: keys,
        write_handle: None,
        transaction_commit: false,
    })
}

async fn transaction(
    db: &Db,
    variant: &VariantConfig,
    write_options: &WriteOptions,
    rng: &mut StdRng,
) -> std::result::Result<OperationOutcome, OperationError> {
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .map_err(|error| OperationError::Other(error.into()))?;
    let mut payload = 0_u64;
    let mut operations = [
        true, true, true, true, true, false, false, false, false, false,
    ];
    operations.shuffle(rng);
    for read_operation in operations {
        let id = rng.random_range(0..variant.record_count().max(1));
        let key = key_for_id(id, variant.key_bytes());
        if read_operation {
            let value = transaction
                .get(key)
                .await
                .map_err(|error| OperationError::Other(error.into()))?
                .context("transaction read key not found")
                .map_err(OperationError::Other)?;
            payload = payload.saturating_add(value.len() as u64);
        } else {
            let value = random_value(variant.value_bytes(), rng);
            payload = payload.saturating_add(value.len() as u64);
            transaction
                .put(key, value)
                .map_err(|error| OperationError::Other(error.into()))?;
        }
    }
    match transaction.commit_with_options(write_options).await {
        Ok(handle) => Ok(OperationOutcome {
            name: "transaction",
            payload_bytes: payload,
            batch_keys: 10,
            write_handle: handle,
            transaction_commit: true,
        }),
        Err(error) if error.to_string().to_ascii_lowercase().contains("conflict") => {
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
    let mut rng = StdRng::from_os_rng();
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
    let mut stats = WorkerStats::default();
    for sequence in 0..count {
        let id = if count > 0 {
            (sequence.wrapping_mul(multiplier).wrapping_add(offset)) % count
        } else {
            0
        };
        let key = key_for_id(id, variant.key_bytes());
        let value = random_value(variant.value_bytes(), &mut rng);
        let started = Instant::now();
        match db
            .put_with_options(key, value.clone(), &PutOptions::default(), &write_options)
            .await
        {
            Ok(handle) => {
                let returned_at = Instant::now();
                stats.record_success("insert", started.elapsed(), value.len() as u64);
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
    let clients = variant.clients.context("reader count is missing")?;
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
        let next_insert = Arc::clone(&state.next_insert);
        let insert_lock = Arc::clone(&state.insert_lock);
        tasks.spawn(async move {
            worker_loop(
                db,
                reader_variant,
                deadline,
                None,
                counters,
                next_insert,
                insert_lock,
            )
            .await
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
    let mut stats = WorkerStats::default();
    while Instant::now() < deadline {
        ticker.tick().await;
        let started = Instant::now();
        let value = random_value(variant.value_bytes(), &mut rng);
        let result = db
            .put_with_options(
                key_for_id(selector.sample(&mut rng), variant.key_bytes()),
                value.clone(),
                &PutOptions::default(),
                &options,
            )
            .await;
        match result {
            Ok(handle) => {
                let returned_at = Instant::now();
                stats.record_success("writer-update", started.elapsed(), value.len() as u64);
                stats.record_write(returned_at, handle.seqnum());
                if let Some(tracker) = &durability {
                    tracker.accepted(handle.seqnum(), returned_at);
                }
                if let Some(counters) = &counters {
                    counters.operations.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(error) => {
                stats.record_error("writer-update", started.elapsed());
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
    prefix_layout: bool,
) -> Result<()> {
    let mut rng = StdRng::from_os_rng();
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
            random_value(value_bytes, &mut rng),
            &PutOptions::default(),
            &options,
        )
        .await
        .with_context(|| format!("loading record {id}"))?;
    }
    db.flush().await.context("flushing loaded dataset")?;
    Ok(())
}
