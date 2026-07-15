use crate::histogram::HistogramSet;
use crate::model::ApplicationPerformance;
use crate::system::{duration_ns, ApplicationWindowRecorder};
use anyhow::Result;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub total: u64,
    pub successful: u64,
    pub errors: u64,
    pub read_payload_bytes: u64,
    pub write_payload_bytes: u64,
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
    window_recorder: Option<ApplicationWindowRecorder>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Payload {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl Payload {
    pub const fn read(bytes: u64) -> Self {
        Self {
            read_bytes: bytes,
            write_bytes: 0,
        }
    }

    pub const fn write(bytes: u64) -> Self {
        Self {
            read_bytes: 0,
            write_bytes: bytes,
        }
    }

    pub const fn read_write(read_bytes: u64, write_bytes: u64) -> Self {
        Self {
            read_bytes,
            write_bytes,
        }
    }

    const fn total(self) -> u64 {
        self.read_bytes.saturating_add(self.write_bytes)
    }
}

impl WorkerStats {
    pub fn with_window_recorder(window_recorder: Option<ApplicationWindowRecorder>) -> Self {
        Self {
            window_recorder,
            ..Self::default()
        }
    }

    pub fn record_success(&mut self, operation: &str, latency: Duration, payload: Payload) {
        self.total += 1;
        self.successful += 1;
        self.read_payload_bytes = self.read_payload_bytes.saturating_add(payload.read_bytes);
        self.write_payload_bytes = self.write_payload_bytes.saturating_add(payload.write_bytes);
        if let Some(recorder) = &self.window_recorder {
            recorder.record_success(operation, latency, payload.read_bytes, payload.write_bytes);
        } else {
            self.histograms.record("return", latency);
            self.histograms
                .record(format!("return/{operation}"), latency);
        }
    }

    pub fn record_error(&mut self, operation: &str, latency: Duration) {
        self.total += 1;
        self.errors += 1;
        if let Some(recorder) = &self.window_recorder {
            recorder.record_error(operation, latency);
        } else {
            self.histograms.record("return", latency);
            self.histograms
                .record(format!("return/{operation}"), latency);
        }
    }

    pub fn record_transaction_conflict(&mut self, latency: Duration) {
        self.total += 1;
        self.transaction_aborts += 1;
        self.transaction_conflicts += 1;
        if let Some(recorder) = &self.window_recorder {
            recorder.record_completion("transaction", latency);
        } else {
            self.histograms.record("return", latency);
            self.histograms.record("return/transaction", latency);
        }
    }

    pub fn record_open_loop_timing(&mut self, response: Duration, scheduling_delay: Duration) {
        if let Some(recorder) = &self.window_recorder {
            recorder.record_open_loop_timing(response, scheduling_delay);
        } else {
            self.histograms.record("response", response);
            self.histograms.record("scheduling_delay", scheduling_delay);
        }
    }

    pub fn record_batch_latency(&mut self, latency: Duration) {
        if let Some(recorder) = &self.window_recorder {
            recorder.record_batch_latency(latency);
        } else {
            self.histograms.record("batch", latency);
        }
    }

    pub fn record_write(&mut self, returned_at: Instant, sequence: u64) {
        self.writes += 1;
        self.first_write_return = Some(
            self.first_write_return
                .map_or(returned_at, |current| current.min(returned_at)),
        );
        self.last_write_sequence = Some(
            self.last_write_sequence
                .map_or(sequence, |current| current.max(sequence)),
        );
    }

    pub fn record_backpressure(&mut self, elapsed: Duration) {
        self.backpressure_ns = self.backpressure_ns.saturating_add(duration_ns(elapsed));
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        self.total = self.total.saturating_add(other.total);
        self.successful = self.successful.saturating_add(other.successful);
        self.errors = self.errors.saturating_add(other.errors);
        self.read_payload_bytes = self
            .read_payload_bytes
            .saturating_add(other.read_payload_bytes);
        self.write_payload_bytes = self
            .write_payload_bytes
            .saturating_add(other.write_payload_bytes);
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
        let histogram_summary = |name| {
            self.histograms
                .get(name)
                .map(|histogram| histogram.summary())
        };
        let return_latency = histogram_summary("return").unwrap_or_default();
        let response_latency = if open_loop {
            histogram_summary("response")
        } else {
            None
        };
        let scheduling_delay = if open_loop {
            histogram_summary("scheduling_delay")
        } else {
            None
        };
        let batch_latency = histogram_summary("batch");
        let transactions = self.transaction_commits + self.transaction_aborts;
        let transaction_rate =
            |count: u64| (transactions > 0).then_some(count as f64 / transactions as f64);
        ApplicationPerformance {
            total_operations: self.total,
            successful_operations: self.successful,
            accepted_ops_per_second: self.successful as f64 / seconds,
            completed_ops_per_second: self.successful as f64 / seconds,
            offered_ops_per_second: open_loop.then_some(self.offered as f64 / seconds),
            dropped_operations: open_loop.then_some(self.dropped),
            dropped_ops_per_second: open_loop.then_some(self.dropped as f64 / seconds),
            payload_mib_per_second: Payload::read_write(
                self.read_payload_bytes,
                self.write_payload_bytes,
            )
            .total() as f64
                / (1024.0 * 1024.0)
                / seconds,
            errors: self.errors,
            return_latency,
            return_latency_by_operation: self.histograms.summaries_with_prefix("return/"),
            response_latency,
            scheduling_delay,
            batch_latency,
            key_throughput_per_second: (self.batch_keys > 0)
                .then_some(self.batch_keys as f64 / seconds),
            transaction_commits: (transactions > 0).then_some(self.transaction_commits),
            transaction_aborts: (transactions > 0).then_some(self.transaction_aborts),
            transaction_conflicts: (transactions > 0).then_some(self.transaction_conflicts),
            transaction_commit_rate: transaction_rate(self.transaction_commits),
            transaction_abort_rate: transaction_rate(self.transaction_aborts),
            transaction_conflict_rate: transaction_rate(self.transaction_conflicts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Payload, WorkerStats};
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

    #[test]
    fn application_exposes_scheduler_drop_count() {
        let stats = WorkerStats {
            total: 10,
            offered: 10,
            dropped: 2,
            ..Default::default()
        };

        let application = stats.application(Duration::from_secs(1), true);

        assert_eq!(application.dropped_operations, Some(2));
    }

    #[test]
    fn application_payload_combines_read_and_write_bytes() {
        let mut stats = WorkerStats::default();
        stats.record_success(
            "read-modify-write",
            Duration::from_millis(1),
            Payload::read_write(1024 * 1024, 2 * 1024 * 1024),
        );

        let application = stats.application(Duration::from_secs(1), false);

        assert_eq!(application.payload_mib_per_second, 3.0);
    }
}
