//! HDR histogram recording and conversion to published latency artifacts.

use crate::model::{HistogramSeries, LatencySummary};
use anyhow::{Context, Result};
use hdrhistogram::Histogram;
use std::collections::BTreeMap;
use std::time::Duration;

/// Number of significant decimal digits retained by latency histograms.
pub const SIGNIFICANT_DIGITS: u8 = 3;
const MAX_MICROSECONDS: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone)]
/// HDR histogram that records clamped microsecond values and exports nanoseconds.
pub struct LatencyHistogram {
    inner: Histogram<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    /// Creates an empty histogram covering one microsecond through 24 hours.
    pub fn new() -> Self {
        let inner = Histogram::new_with_bounds(1, MAX_MICROSECONDS, SIGNIFICANT_DIGITS)
            .unwrap_or_else(|error| panic!("valid histogram bounds: {error}"));
        Self { inner }
    }

    /// Records one latency observation.
    pub fn record(&mut self, duration: Duration) {
        self.record_n(duration, 1);
    }

    /// Records `count` observations with the same latency.
    pub fn record_n(&mut self, duration: Duration, count: u64) {
        let micros = duration.as_micros().clamp(1, MAX_MICROSECONDS as u128) as u64;
        let _ = self.inner.record_n(micros, count);
    }

    /// Merges another histogram into this one.
    pub fn add(&mut self, other: &Self) -> Result<()> {
        self.inner
            .add(&other.inner)
            .context("merging HDR histograms")
    }

    /// Removes all observations while retaining the allocation.
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Returns the number of recorded observations.
    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// Returns whether the histogram contains no observations.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Summarizes the histogram using the published percentile set.
    pub fn summary(&self) -> LatencySummary {
        if self.is_empty() {
            return LatencySummary::default();
        }
        LatencySummary {
            count: self.len(),
            avg_ns: self.inner.mean() * 1_000.0,
            p001_ns: self.inner.value_at_quantile(0.001) * 1_000,
            p01_ns: self.inner.value_at_quantile(0.01) * 1_000,
            p50_ns: self.inner.value_at_quantile(0.50) * 1_000,
            p99_ns: self.inner.value_at_quantile(0.99) * 1_000,
            p999_ns: self.inner.value_at_quantile(0.999) * 1_000,
            min_ns: self.inner.min() * 1_000,
            max_ns: self.inner.max() * 1_000,
        }
    }

    /// Exports non-empty histogram buckets as a sparse series.
    pub fn series(&self) -> HistogramSeries {
        let mut upper_bound_ns = Vec::new();
        let mut counts = Vec::new();
        for value in self.inner.iter_recorded() {
            upper_bound_ns.push(value.value_iterated_to().saturating_mul(1_000));
            counts.push(value.count_at_value());
        }
        HistogramSeries {
            upper_bound_ns,
            counts,
        }
    }
}

#[derive(Debug, Clone, Default)]
/// Named collection of latency histograms.
pub struct HistogramSet {
    values: BTreeMap<String, LatencyHistogram>,
}

impl HistogramSet {
    /// Records one observation in the named histogram.
    pub fn record(&mut self, name: impl Into<String>, duration: Duration) {
        self.values.entry(name.into()).or_default().record(duration);
    }

    /// Merges populated histograms from another set.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        for (name, histogram) in &other.values {
            if histogram.is_empty() {
                continue;
            }
            self.values
                .entry(name.clone())
                .or_default()
                .add(histogram)?;
        }
        Ok(())
    }

    /// Resets all histograms while retaining names and allocations.
    pub fn reset(&mut self) {
        for histogram in self.values.values_mut() {
            histogram.reset();
        }
    }

    /// Returns a summary when the named histogram has observations.
    pub fn summary(&self, name: &str) -> Option<LatencySummary> {
        self.values
            .get(name)
            .filter(|histogram| !histogram.is_empty())
            .map(LatencyHistogram::summary)
    }

    /// Summarizes histograms whose names begin with `prefix`.
    ///
    /// Keys in the returned map have the prefix removed.
    pub fn summaries_with_prefix(&self, prefix: &str) -> BTreeMap<String, LatencySummary> {
        self.values
            .iter()
            .filter_map(|(name, histogram)| {
                name.strip_prefix(prefix)
                    .map(|short| (short.to_string(), histogram.summary()))
            })
            .collect()
    }

    /// Exports sparse series for histograms whose names begin with `prefix`.
    pub fn series_with_prefix(&self, prefix: &str) -> BTreeMap<String, HistogramSeries> {
        self.values
            .iter()
            .filter_map(|(name, histogram)| {
                name.strip_prefix(prefix)
                    .map(|short| (short.to_string(), histogram.series()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::LatencyHistogram;
    use std::time::Duration;

    #[test]
    fn exports_only_populated_histogram_buckets() {
        let mut histogram = LatencyHistogram::new();
        histogram.record_n(Duration::from_micros(10), 2);
        histogram.record(Duration::from_micros(20));

        let series = histogram.series();
        assert_eq!(series.counts.iter().sum::<u64>(), 3);
        assert_eq!(series.upper_bound_ns.len(), series.counts.len());
        assert!(series
            .upper_bound_ns
            .windows(2)
            .all(|pair| pair[0] < pair[1]));
        assert!(series.counts.iter().all(|count| *count > 0));
    }
}
