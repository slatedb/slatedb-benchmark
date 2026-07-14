use crate::histogram::HistogramSet;
use crate::model::ApplicationPerformance;
use anyhow::Result;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub total: u64,
    pub successful: u64,
    pub errors: u64,
    pub payload_bytes: u64,
    pub writes: u64,
    pub offered: u64,
    pub dropped: u64,
    pub batch_keys: u64,
    pub transaction_commits: u64,
    pub transaction_aborts: u64,
    pub transaction_conflicts: u64,
    pub backpressure_ns: u64,
    pub first_write_return: Option<Instant>,
    pub last_write_sequence: Option<u64>,
    pub histograms: HistogramSet,
}

impl WorkerStats {
    pub fn record_success(&mut self, operation: &str, latency: Duration, payload_bytes: u64) {
        self.total += 1;
        self.successful += 1;
        self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes);
        self.histograms.record("return", latency);
        self.histograms
            .record(format!("return/{operation}"), latency);
    }

    pub fn record_error(&mut self, operation: &str, latency: Duration) {
        self.total += 1;
        self.errors += 1;
        self.histograms.record("return", latency);
        self.histograms
            .record(format!("return/{operation}"), latency);
    }

    pub fn record_write(&mut self, returned_at: Instant, sequence: u64) {
        self.writes += 1;
        self.first_write_return = Some(
            self.first_write_return
                .map(|current| current.min(returned_at))
                .unwrap_or(returned_at),
        );
        self.last_write_sequence = Some(
            self.last_write_sequence
                .map(|current| current.max(sequence))
                .unwrap_or(sequence),
        );
    }

    pub fn record_backpressure(&mut self, elapsed: Duration) {
        self.backpressure_ns = self
            .backpressure_ns
            .saturating_add(elapsed.as_nanos().min(u64::MAX as u128) as u64);
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        self.total = self.total.saturating_add(other.total);
        self.successful = self.successful.saturating_add(other.successful);
        self.errors = self.errors.saturating_add(other.errors);
        self.payload_bytes = self.payload_bytes.saturating_add(other.payload_bytes);
        self.writes = self.writes.saturating_add(other.writes);
        self.offered = self.offered.saturating_add(other.offered);
        self.dropped = self.dropped.saturating_add(other.dropped);
        self.batch_keys = self.batch_keys.saturating_add(other.batch_keys);
        self.transaction_commits = self
            .transaction_commits
            .saturating_add(other.transaction_commits);
        self.transaction_aborts = self
            .transaction_aborts
            .saturating_add(other.transaction_aborts);
        self.transaction_conflicts = self
            .transaction_conflicts
            .saturating_add(other.transaction_conflicts);
        self.backpressure_ns = self.backpressure_ns.saturating_add(other.backpressure_ns);
        self.first_write_return = match (self.first_write_return, other.first_write_return) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.last_write_sequence = match (self.last_write_sequence, other.last_write_sequence) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        self.histograms.merge(&other.histograms)
    }

    pub fn application(&self, elapsed: Duration, open_loop: bool) -> ApplicationPerformance {
        let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
        let return_latency = self
            .histograms
            .get("return")
            .map(|histogram| histogram.summary())
            .unwrap_or_default();
        let response_latency = open_loop
            .then(|| self.histograms.get("response").map(|h| h.summary()))
            .flatten();
        let scheduling_delay = open_loop
            .then(|| self.histograms.get("scheduling_delay").map(|h| h.summary()))
            .flatten();
        let batch_latency = self.histograms.get("batch").map(|h| h.summary());
        let transactions = self.transaction_commits + self.transaction_aborts;
        let transaction_rate =
            |count: u64| (transactions > 0).then_some(count as f64 / transactions as f64);
        ApplicationPerformance {
            total_operations: self.total,
            successful_operations: self.successful,
            accepted_ops_per_second: self.successful as f64 / seconds,
            completed_ops_per_second: self.successful as f64 / seconds,
            offered_ops_per_second: open_loop.then_some(self.offered as f64 / seconds),
            dropped_ops_per_second: open_loop.then_some(self.dropped as f64 / seconds),
            payload_mib_per_second: self.payload_bytes as f64 / (1024.0 * 1024.0) / seconds,
            errors: self.errors,
            return_latency,
            return_latency_by_operation: self.histograms.summaries_with_prefix("return/"),
            response_latency,
            scheduling_delay,
            batch_latency,
            key_throughput_per_second: (self.batch_keys > 0)
                .then_some(self.batch_keys as f64 / seconds),
            transaction_commits: (self.transaction_commits + self.transaction_aborts > 0)
                .then_some(self.transaction_commits),
            transaction_aborts: (self.transaction_commits + self.transaction_aborts > 0)
                .then_some(self.transaction_aborts),
            transaction_conflicts: (self.transaction_commits + self.transaction_aborts > 0)
                .then_some(self.transaction_conflicts),
            transaction_commit_rate: transaction_rate(self.transaction_commits),
            transaction_abort_rate: transaction_rate(self.transaction_aborts),
            transaction_conflict_rate: transaction_rate(self.transaction_conflicts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerStats;
    use std::time::Duration;

    #[test]
    fn accumulates_backpressure_duration() {
        let mut left = WorkerStats::default();
        left.record_backpressure(Duration::from_nanos(7));
        left.record_backpressure(Duration::from_nanos(11));

        let mut right = WorkerStats::default();
        right.record_backpressure(Duration::from_nanos(13));
        left.merge(&right).expect("merge worker stats");

        assert_eq!(left.backpressure_ns, 31);
    }
}
