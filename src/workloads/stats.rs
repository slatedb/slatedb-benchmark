use super::durability::DurabilitySender;
use crate::system::ApplicationRecorder;
use slatedb::WriteHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub errors: u64,
    pub read_hits: u64,
    pub read_misses: u64,
    pub writes: u64,
    pub last_write_sequence: Option<u64>,
    pub transaction_attempts: u64,
    pub transaction_commits: u64,
    pub transaction_conflicts: u64,
    pub scan_records: u64,
    pub scan_end_calls: u64,
}

impl WorkerStats {
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
        self.scan_records = self.scan_records.saturating_add(other.scan_records);
        self.scan_end_calls = self.scan_end_calls.saturating_add(other.scan_end_calls);
    }

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
