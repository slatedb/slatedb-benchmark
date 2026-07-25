//! Lightweight correctness counters collected independently of published metrics.

use super::durability::DurabilitySender;
use crate::system::ApplicationRecorder;
use slatedb::WriteHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
/// Per-client counters used to validate workload behavior after a phase.
pub struct WorkerStats {
    /// API failures or unexpected missing values.
    pub errors: u64,
    /// Reads that returned values.
    pub read_hits: u64,
    /// Reads that returned no value.
    pub read_misses: u64,
    /// Writes accepted for durability tracking.
    pub writes: u64,
    /// Greatest sequence returned by an accepted write.
    pub last_write_sequence: Option<u64>,
    /// Transactions that reached commit.
    pub transaction_attempts: u64,
    /// Transactions committed successfully.
    pub transaction_commits: u64,
    /// Transactions rejected due to an expected conflict.
    pub transaction_conflicts: u64,
}

impl WorkerStats {
    /// Saturating-merges another client's counters into this aggregate.
    pub fn merge(&mut self, other: &Self) {
        self.errors = self.errors.saturating_add(other.errors);
        self.read_hits = self.read_hits.saturating_add(other.read_hits);
        self.read_misses = self.read_misses.saturating_add(other.read_misses);
        self.writes = self.writes.saturating_add(other.writes);
        self.last_write_sequence = match (self.last_write_sequence, other.last_write_sequence) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.transaction_attempts = self
            .transaction_attempts
            .saturating_add(other.transaction_attempts);
        self.transaction_commits = self
            .transaction_commits
            .saturating_add(other.transaction_commits);
        self.transaction_conflicts = self
            .transaction_conflicts
            .saturating_add(other.transaction_conflicts);
    }

    /// Records an accepted write and forwards it to durability tracking.
    pub fn record_write(
        &mut self,
        handle: &WriteHandle,
        returned_at: Instant,
        durability: Option<&DurabilitySender>,
    ) {
        let sequence = handle.seqnum();
        self.writes = self.writes.saturating_add(1);
        self.last_write_sequence = Some(
            self.last_write_sequence
                .map_or(sequence, |current| current.max(sequence)),
        );
        if let Some(durability) = durability {
            durability.accepted(sequence, returned_at);
        }
    }
}

/// Records a successful application operation when measurement is enabled.
pub fn record_success(
    recorder: Option<&ApplicationRecorder>,
    api: &str,
    latency: Duration,
    logical_bytes: u64,
) {
    if let Some(recorder) = recorder {
        recorder.record_success(api, latency, logical_bytes);
    }
}

/// Records an application error and increments the correctness error count.
pub fn record_error(
    stats: &mut WorkerStats,
    recorder: Option<&ApplicationRecorder>,
    api: &str,
    latency: Duration,
) {
    stats.errors = stats.errors.saturating_add(1);
    if let Some(recorder) = recorder {
        recorder.record_error(api, latency);
    }
}
