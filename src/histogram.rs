use crate::model::{EncodedHistogram, HistogramsFile, LatencySummary};
use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::Histogram;
use std::collections::BTreeMap;
use std::time::Duration;

pub const SIGNIFICANT_DIGITS: u8 = 3;
const MAX_MICROSECONDS: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    inner: Histogram<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        let inner = Histogram::new_with_bounds(1, MAX_MICROSECONDS, SIGNIFICANT_DIGITS)
            .unwrap_or_else(|error| panic!("valid histogram bounds: {error}"));
        Self { inner }
    }

    pub fn record(&mut self, duration: Duration) {
        self.record_n(duration, 1);
    }

    pub fn record_n(&mut self, duration: Duration, count: u64) {
        let micros = duration.as_micros().clamp(1, MAX_MICROSECONDS as u128) as u64;
        let _ = self.inner.record_n(micros, count);
    }

    pub fn add(&mut self, other: &Self) -> Result<()> {
        self.inner
            .add(&other.inner)
            .context("merging HDR histograms")
    }

    pub fn reset(&mut self) {
        self.inner.reset();
    }

    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn summary(&self) -> LatencySummary {
        if self.is_empty() {
            return LatencySummary::default();
        }
        LatencySummary {
            count: self.len(),
            p50_ns: self.inner.value_at_quantile(0.50) * 1_000,
            p95_ns: self.inner.value_at_quantile(0.95) * 1_000,
            p99_ns: self.inner.value_at_quantile(0.99) * 1_000,
            p999_ns: self.inner.value_at_quantile(0.999) * 1_000,
            max_ns: self.inner.max() * 1_000,
        }
    }

    pub fn encode(&self) -> Result<EncodedHistogram> {
        let mut bytes = Vec::new();
        V2DeflateSerializer::new()
            .serialize(&self.inner, &mut bytes)
            .context("encoding HDR histogram")?;
        Ok(EncodedHistogram {
            unit: "microseconds".to_string(),
            count: self.len(),
            min: if self.is_empty() { 0 } else { self.inner.min() },
            max: self.inner.max(),
            data: STANDARD.encode(bytes),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct HistogramSet {
    values: BTreeMap<String, LatencyHistogram>,
}

impl HistogramSet {
    pub fn record(&mut self, name: impl Into<String>, duration: Duration) {
        self.values.entry(name.into()).or_default().record(duration);
    }

    pub fn record_n(&mut self, name: impl Into<String>, duration: Duration, count: u64) {
        self.values
            .entry(name.into())
            .or_default()
            .record_n(duration, count);
    }

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

    pub fn reset(&mut self) {
        for histogram in self.values.values_mut() {
            histogram.reset();
        }
    }

    pub fn insert(&mut self, name: impl Into<String>, histogram: LatencyHistogram) {
        self.values.insert(name.into(), histogram);
    }

    pub fn get(&self, name: &str) -> Option<&LatencyHistogram> {
        self.values.get(name)
    }

    pub fn summaries_with_prefix(&self, prefix: &str) -> BTreeMap<String, LatencySummary> {
        self.values
            .iter()
            .filter_map(|(name, histogram)| {
                name.strip_prefix(prefix)
                    .map(|short| (short.to_string(), histogram.summary()))
            })
            .collect()
    }

    pub fn to_file(&self) -> Result<HistogramsFile> {
        let mut histograms = BTreeMap::new();
        for (name, histogram) in &self.values {
            histograms.insert(name.clone(), histogram.encode()?);
        }
        Ok(HistogramsFile {
            encoding: "hdrhistogram-v2-deflate-base64".to_string(),
            significant_digits: SIGNIFICANT_DIGITS,
            histograms,
        })
    }
}
