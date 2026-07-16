use crate::histogram::HistogramSet;
use crate::model::ApplicationPerformance;
use crate::system::{duration_ns, ApplicationWindowRecorder};
use anyhow::Result;
use std::future::Future;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub total: u64,
    pub successful: u64,
    pub errors: u64,
    pub read_payload_bytes: u64,
    pub write_payload_bytes: u64,
    pub read_hits: u64,
    pub read_misses: u64,
    pub background_writer_target_bytes_per_second: Option<u64>,
    pub background_writer_logical_bytes: u64,
    pub writes: u64,
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
    pub read_hits: u64,
    pub read_misses: u64,
}

impl Payload {
    pub const fn read(bytes: u64) -> Self {
        Self {
            read_bytes: bytes,
            write_bytes: 0,
            read_hits: 0,
            read_misses: 0,
        }
    }

    pub const fn read_hit(bytes: u64) -> Self {
        Self {
            read_bytes: bytes,
            write_bytes: 0,
            read_hits: 1,
            read_misses: 0,
        }
    }

    pub const fn read_miss() -> Self {
        Self {
            read_bytes: 0,
            write_bytes: 0,
            read_hits: 0,
            read_misses: 1,
        }
    }

    pub const fn read_batch(bytes: u64, hits: u64, misses: u64) -> Self {
        Self {
            read_bytes: bytes,
            write_bytes: 0,
            read_hits: hits,
            read_misses: misses,
        }
    }

    pub const fn write(bytes: u64) -> Self {
        Self {
            read_bytes: 0,
            write_bytes: bytes,
            read_hits: 0,
            read_misses: 0,
        }
    }

    pub const fn read_write(read_bytes: u64, write_bytes: u64) -> Self {
        Self {
            read_bytes,
            write_bytes,
            read_hits: 0,
            read_misses: 0,
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
        self.record_success_internal(operation, latency, payload, true);
    }

    pub fn record_success_n(
        &mut self,
        operation: &str,
        latency: Duration,
        payload: Payload,
        count: u64,
    ) {
        self.total = self.total.saturating_add(count);
        self.successful = self.successful.saturating_add(count);
        self.read_payload_bytes = self.read_payload_bytes.saturating_add(payload.read_bytes);
        self.write_payload_bytes = self.write_payload_bytes.saturating_add(payload.write_bytes);
        self.read_hits = self.read_hits.saturating_add(payload.read_hits);
        self.read_misses = self.read_misses.saturating_add(payload.read_misses);
        if let Some(recorder) = &self.window_recorder {
            recorder.record_success_n(
                operation,
                latency,
                payload.read_bytes,
                payload.write_bytes,
                payload.read_hits,
                payload.read_misses,
                count,
            );
        } else {
            self.histograms.record_n("return", latency, count);
            self.histograms
                .record_n(format!("return/{operation}"), latency, count);
        }
    }

    pub fn record_background_success(
        &mut self,
        operation: &str,
        latency: Duration,
        payload: Payload,
    ) {
        self.record_success_internal(operation, latency, payload, false);
    }

    pub fn record_background_writer_success(
        &mut self,
        operation: &str,
        latency: Duration,
        payload: Payload,
        logical_bytes: u64,
    ) {
        self.record_background_success(operation, latency, payload);
        self.background_writer_logical_bytes = self
            .background_writer_logical_bytes
            .saturating_add(logical_bytes);
    }

    fn record_success_internal(
        &mut self,
        operation: &str,
        latency: Duration,
        payload: Payload,
        include_in_headline: bool,
    ) {
        if include_in_headline {
            self.total += 1;
            self.successful += 1;
            self.read_payload_bytes = self.read_payload_bytes.saturating_add(payload.read_bytes);
            self.write_payload_bytes = self.write_payload_bytes.saturating_add(payload.write_bytes);
            self.read_hits = self.read_hits.saturating_add(payload.read_hits);
            self.read_misses = self.read_misses.saturating_add(payload.read_misses);
        }
        if let Some(recorder) = &self.window_recorder {
            if include_in_headline {
                recorder.record_success(
                    operation,
                    latency,
                    payload.read_bytes,
                    payload.write_bytes,
                    payload.read_hits,
                    payload.read_misses,
                );
            } else {
                recorder.record_background_success(
                    operation,
                    latency,
                    payload.read_bytes,
                    payload.write_bytes,
                    payload.read_hits,
                    payload.read_misses,
                );
            }
        } else {
            if include_in_headline {
                self.histograms.record("return", latency);
            }
            self.histograms
                .record(format!("return/{operation}"), latency);
        }
    }

    pub fn record_error(&mut self, operation: &str, latency: Duration) {
        self.record_error_internal(operation, latency, true);
    }

    pub fn record_error_n(&mut self, operation: &str, latency: Duration, count: u64) {
        self.total = self.total.saturating_add(count);
        self.errors = self.errors.saturating_add(count);
        if let Some(recorder) = &self.window_recorder {
            recorder.record_error_n(operation, latency, count);
        } else {
            self.histograms.record_n("return", latency, count);
            self.histograms
                .record_n(format!("return/{operation}"), latency, count);
        }
    }

    pub fn record_background_error(&mut self, operation: &str, latency: Duration) {
        self.record_error_internal(operation, latency, false);
    }

    fn record_error_internal(
        &mut self,
        operation: &str,
        latency: Duration,
        include_in_headline: bool,
    ) {
        self.total += 1;
        self.errors += 1;
        if let Some(recorder) = &self.window_recorder {
            if include_in_headline {
                recorder.record_error(operation, latency);
            } else {
                recorder.record_background_error(operation, latency);
            }
        } else {
            if include_in_headline {
                self.histograms.record("return", latency);
            }
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

    pub fn record_batch_latency(&mut self, latency: Duration) {
        if let Some(recorder) = &self.window_recorder {
            recorder.record_batch_latency(latency);
        } else {
            self.histograms.record("batch", latency);
        }
    }

    pub async fn measure_api<T>(&mut self, api: &str, future: impl Future<Output = T>) -> T {
        let started = Instant::now();
        let result = future.await;
        self.record_api_latency(api, started.elapsed());
        result
    }

    pub fn measure_api_sync<T>(&mut self, api: &str, call: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let result = call();
        self.record_api_latency(api, started.elapsed());
        result
    }

    pub fn record_api_latency(&mut self, api: &str, latency: Duration) {
        if let Some(recorder) = &self.window_recorder {
            recorder.record_api_latency(api, latency);
        } else {
            self.histograms.record(format!("api/{api}"), latency);
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
        self.read_hits = self.read_hits.saturating_add(other.read_hits);
        self.read_misses = self.read_misses.saturating_add(other.read_misses);
        self.background_writer_target_bytes_per_second = self
            .background_writer_target_bytes_per_second
            .or(other.background_writer_target_bytes_per_second);
        self.background_writer_logical_bytes = self
            .background_writer_logical_bytes
            .saturating_add(other.background_writer_logical_bytes);
        self.writes = self.writes.saturating_add(other.writes);
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

    pub fn application(&self, elapsed: Duration) -> ApplicationPerformance {
        let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
        let histogram_summary = |name| {
            self.histograms
                .get(name)
                .map(|histogram| histogram.summary())
        };
        let return_latency = histogram_summary("return").unwrap_or_default();
        let batch_latency = histogram_summary("batch");
        let transactions = self.transaction_commits + self.transaction_aborts;
        let transaction_rate =
            |count: u64| (transactions > 0).then_some(count as f64 / transactions as f64);
        ApplicationPerformance {
            total_operations: self.total,
            successful_operations: self.successful,
            read_hits: (self.read_hits.saturating_add(self.read_misses) > 0)
                .then_some(self.read_hits),
            read_misses: (self.read_hits.saturating_add(self.read_misses) > 0)
                .then_some(self.read_misses),
            background_writer_target_mib_per_second: self
                .background_writer_target_bytes_per_second
                .map(|bytes| bytes as f64 / (1024.0 * 1024.0)),
            background_writer_achieved_mib_per_second: self
                .background_writer_target_bytes_per_second
                .map(|_| self.background_writer_logical_bytes as f64 / (1024.0 * 1024.0) / seconds),
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
            api_latency: self.histograms.summaries_with_prefix("api/"),
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
    fn application_counts_transaction_conflicts_as_completed() {
        let mut stats = WorkerStats::default();
        stats.record_transaction_conflict(Duration::from_millis(1));

        let application = stats.application(Duration::from_secs(1));

        assert_eq!(application.total_operations, 1);
        assert_eq!(application.successful_operations, 0);
    }

    #[test]
    fn application_payload_combines_read_and_write_bytes() {
        let mut stats = WorkerStats::default();
        stats.record_success(
            "read-modify-write",
            Duration::from_millis(1),
            Payload::read_write(1024 * 1024, 2 * 1024 * 1024),
        );

        let application = stats.application(Duration::from_secs(1));

        assert_eq!(application.payload_mib_per_second, 3.0);
    }

    #[test]
    fn application_reports_point_read_hits_and_misses() {
        let mut stats = WorkerStats::default();
        stats.record_success("read", Duration::from_millis(1), Payload::read_hit(400));
        stats.record_success("read", Duration::from_millis(1), Payload::read_miss());
        stats.record_success(
            "batch-read",
            Duration::from_millis(1),
            Payload::read_batch(1_200, 3, 7),
        );

        let application = stats.application(Duration::from_secs(1));

        assert_eq!(application.read_hits, Some(4));
        assert_eq!(application.read_misses, Some(8));
        assert_eq!(stats.read_payload_bytes, 1_600);
    }

    #[test]
    fn background_operation_is_excluded_from_headline_totals_and_payload() {
        let mut stats = WorkerStats::default();
        stats.record_success("read", Duration::from_millis(1), Payload::read(1));
        stats.record_background_success("writer-update", Duration::from_secs(1), Payload::write(1));

        let application = stats.application(Duration::from_secs(1));

        assert_eq!(application.total_operations, 1);
        assert_eq!(application.successful_operations, 1);
        assert_eq!(application.payload_mib_per_second, 1.0 / (1024.0 * 1024.0));
        assert_eq!(application.return_latency.count, 1);
        assert_eq!(application.return_latency.max_ns, 1_000_000);
        assert_eq!(application.return_latency_by_operation["read"].count, 1);
        assert_eq!(
            application.return_latency_by_operation["writer-update"].count,
            1
        );
    }

    #[test]
    fn application_reports_background_writer_target_and_achieved_rate() {
        let mut stats = WorkerStats {
            background_writer_target_bytes_per_second: Some(2 * 1024 * 1024),
            ..Default::default()
        };
        stats.record_background_writer_success(
            "writer-update",
            Duration::from_millis(1),
            Payload::write(400),
            420,
        );

        let application = stats.application(Duration::from_secs(1));

        assert_eq!(
            application.background_writer_target_mib_per_second,
            Some(2.0)
        );
        assert_eq!(
            application.background_writer_achieved_mib_per_second,
            Some(420.0 / (1024.0 * 1024.0))
        );
        assert_eq!(application.payload_mib_per_second, 0.0);
    }
}
