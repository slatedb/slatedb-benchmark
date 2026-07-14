use crate::model::{CostEstimate, StoragePerformance};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct PriceTable {
    pub revision: String,
    pub currency: String,
    pub units: Units,
    pub rates: Rates,
    pub request_classes: RequestClasses,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Units {
    pub gib_bytes: u64,
    pub month_days: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rates {
    pub compute_per_minute: f64,
    pub storage_per_gib_month: f64,
    pub class_a_per_1000: f64,
    pub class_b_per_1000: f64,
    pub delete_per_1000: f64,
    pub retrieval_per_gib: f64,
    pub transfer_per_gib: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RequestClasses {
    pub class_a: Vec<String>,
    pub class_b: Vec<String>,
    pub free: Vec<String>,
}

impl PriceTable {
    pub fn load(schema_dir: &Path) -> Result<Self> {
        let path = schema_dir.join("prices-tigris-2026-07-14.json");
        serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("parsing {}", path.display()))
    }

    pub fn estimate(
        &self,
        elapsed_ns: u64,
        average_database_bytes: u64,
        storage: &StoragePerformance,
        successful_operations: u64,
    ) -> CostEstimate {
        let elapsed_seconds = elapsed_ns as f64 / 1_000_000_000.0;
        let compute = elapsed_seconds / 60.0 * self.rates.compute_per_minute;
        let requests = request_cost(
            &storage.object_store_requests,
            &self.request_classes,
            &self.rates,
        );
        let gib = average_database_bytes as f64 / self.units.gib_bytes as f64;
        let month_seconds = self.units.month_days as f64 * 86_400.0;
        let storage_cost =
            gib * (elapsed_seconds / month_seconds) * self.rates.storage_per_gib_month;
        let transfer_gib =
            (storage.bytes_read + storage.bytes_written) as f64 / self.units.gib_bytes as f64;
        let transfer = transfer_gib * self.rates.transfer_per_gib
            + storage.bytes_read as f64 / self.units.gib_bytes as f64
                * self.rates.retrieval_per_gib;
        let total = compute + requests + storage_cost + transfer;
        let per_million_factor =
            (successful_operations > 0).then(|| 1_000_000.0 / successful_operations as f64);
        CostEstimate {
            price_table_revision: self.revision.clone(),
            currency: self.currency.clone(),
            compute,
            requests,
            storage: storage_cost,
            transfer,
            total,
            compute_per_million_operations: per_million_factor.map(|factor| compute * factor),
            requests_per_million_operations: per_million_factor.map(|factor| requests * factor),
            storage_per_million_operations: per_million_factor.map(|factor| storage_cost * factor),
            transfer_per_million_operations: per_million_factor.map(|factor| transfer * factor),
            total_per_million_operations: per_million_factor.map(|factor| total * factor),
        }
    }
}

fn request_cost(counts: &BTreeMap<String, u64>, classes: &RequestClasses, rates: &Rates) -> f64 {
    counts
        .iter()
        .map(|(operation, count)| {
            let rate = if classes.class_a.contains(operation) {
                rates.class_a_per_1000
            } else if classes.class_b.contains(operation) {
                rates.class_b_per_1000
            } else if classes.free.contains(operation) {
                rates.delete_per_1000
            } else {
                0.0
            };
            *count as f64 / 1_000.0 * rate
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::PriceTable;
    use crate::model::StoragePerformance;
    use std::collections::BTreeMap;
    use std::path::Path;

    #[test]
    fn estimates_tigris_list_prices_for_the_measured_window() {
        let prices = PriceTable::load(Path::new("../schema")).expect("price table");
        let storage = StoragePerformance {
            object_store_requests: BTreeMap::from([
                ("put".to_string(), 1_000),
                ("get".to_string(), 1_000),
                ("delete".to_string(), 1_000),
            ]),
            bytes_read: 1_073_741_824,
            ..Default::default()
        };
        let estimate = prices.estimate(60_000_000_000, 1_073_741_824, &storage, 1_000_000);
        assert!((estimate.compute - 0.032).abs() < 1e-12);
        assert!((estimate.requests - 0.0055).abs() < 1e-12);
        assert!((estimate.storage - (0.02 / 43_200.0)).abs() < 1e-12);
        assert_eq!(estimate.transfer, 0.0);
        assert!(
            (estimate.total_per_million_operations.expect("per million") - estimate.total).abs()
                < 1e-12
        );
        assert_eq!(estimate.compute_per_million_operations, Some(0.032));
        assert_eq!(estimate.requests_per_million_operations, Some(0.0055));
        assert_eq!(
            estimate.storage_per_million_operations,
            Some(0.02 / 43_200.0)
        );
        assert_eq!(estimate.transfer_per_million_operations, Some(0.0));
    }
}
