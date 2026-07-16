use bytes::Bytes;
use rand::distr::Distribution;
use rand::{Rng, RngCore};
use rand_distr::Zipf;

// Match YCSB's ScrambledZipfianGenerator and Utils.fnvhash64.
const YCSB_ZIPFIAN_ITEM_COUNT: u64 = 10_000_000_000;
const YCSB_ZIPFIAN_EXPONENT: f64 = 0.99;
const YCSB_FNV_OFFSET_BASIS_64: u64 = 0xcbf2_9ce4_8422_2325;
const YCSB_FNV_PRIME_64: u64 = 1_099_511_628_211;
const VALUE_CORPUS_BYTES: usize = 1_048_576;
const COMPRESSIBLE_FRAGMENT_BYTES: usize = 100;

pub fn key_for_id(id: u64, size: usize) -> Bytes {
    let mut key = vec![0_u8; size];
    let encoded = id.to_be_bytes();
    if size >= encoded.len() {
        key[size - encoded.len()..].copy_from_slice(&encoded);
    } else {
        key.copy_from_slice(&encoded[encoded.len() - size..]);
    }
    Bytes::from(key)
}

pub fn prefix_key(prefix: u64, suffix: u64) -> Bytes {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&prefix.to_be_bytes());
    key.extend_from_slice(&suffix.to_be_bytes());
    Bytes::from(key)
}

pub fn random_unique_key(id: u64, size: usize, rng: &mut impl RngCore) -> Bytes {
    let mut key = vec![0_u8; size];
    rng.fill_bytes(&mut key);
    let encoded = id.to_be_bytes();
    let start = size.saturating_sub(encoded.len());
    key[start..].copy_from_slice(&encoded[encoded.len().saturating_sub(size)..]);
    Bytes::from(key)
}

pub struct ValueGenerator {
    compression_ratio: f64,
    corpus: Option<Bytes>,
    position: usize,
}

impl ValueGenerator {
    pub fn new(compression_ratio: f64) -> Self {
        Self {
            compression_ratio,
            corpus: None,
            position: 0,
        }
    }

    pub fn generate(&mut self, size: usize, rng: &mut impl RngCore) -> Bytes {
        if size == 0 {
            return Bytes::new();
        }
        let compression_ratio = self.compression_ratio;
        let corpus = self.corpus.get_or_insert_with(|| {
            Bytes::from(compressible_data(
                VALUE_CORPUS_BYTES,
                compression_ratio,
                rng,
            ))
        });
        if size > corpus.len() {
            return Bytes::from(compressible_data(size, self.compression_ratio, rng));
        }
        if self.position + size > corpus.len() {
            self.position = 0;
        }
        let value = corpus.slice(self.position..self.position + size);
        self.position += size;
        value
    }
}

fn compressible_data(size: usize, compression_ratio: f64, rng: &mut impl RngCore) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let random_bytes = ((COMPRESSIBLE_FRAGMENT_BYTES as f64 * compression_ratio) as usize)
        .clamp(1, COMPRESSIBLE_FRAGMENT_BYTES);
    while data.len() < size {
        let fragment_size = COMPRESSIBLE_FRAGMENT_BYTES.min(size - data.len());
        let mut seed = [0_u8; COMPRESSIBLE_FRAGMENT_BYTES];
        rng.fill_bytes(&mut seed[..random_bytes]);
        let fragment_start = data.len();
        while data.len() - fragment_start < fragment_size {
            let remaining = fragment_size - (data.len() - fragment_start);
            data.extend_from_slice(&seed[..remaining.min(random_bytes)]);
        }
    }
    data
}

pub struct KeySelector {
    zipf: Option<Zipf<f64>>,
    record_count: u64,
}

impl KeySelector {
    pub fn uniform(record_count: u64) -> Self {
        Self {
            zipf: None,
            record_count,
        }
    }

    pub fn zipfian(record_count: u64) -> Self {
        Self {
            zipf: Zipf::new(YCSB_ZIPFIAN_ITEM_COUNT as f64, YCSB_ZIPFIAN_EXPONENT).ok(),
            record_count,
        }
    }

    pub fn sample(&self, rng: &mut impl Rng) -> u64 {
        self.sample_with_record_count(self.record_count, rng)
    }

    pub fn sample_with_record_count(&self, record_count: u64, rng: &mut impl Rng) -> u64 {
        if record_count == 0 {
            return 0;
        }
        match &self.zipf {
            Some(zipf) => {
                let rank = (zipf.sample(rng) as u64).saturating_sub(1);
                ycsb_scramble(rank) % record_count
            }
            None => rng.random_range(0..record_count),
        }
    }
}

pub struct YcsbLatestSelector {
    item_count: u64,
    zipf: Zipf<f64>,
}

impl YcsbLatestSelector {
    pub fn new(item_count: u64) -> Self {
        let item_count = item_count.max(1);
        Self {
            item_count,
            zipf: Zipf::new(item_count as f64, YCSB_ZIPFIAN_EXPONENT)
                .expect("positive YCSB latest item count"),
        }
    }

    pub fn sample(&mut self, item_count: u64, rng: &mut impl Rng) -> u64 {
        let item_count = item_count.max(1);
        if item_count != self.item_count {
            self.item_count = item_count;
            self.zipf = Zipf::new(item_count as f64, YCSB_ZIPFIAN_EXPONENT)
                .expect("positive YCSB latest item count");
        }
        let rank = (self.zipf.sample(rng) as u64)
            .saturating_sub(1)
            .min(item_count - 1);
        item_count - 1 - rank
    }
}

fn ycsb_scramble(mut value: u64) -> u64 {
    let mut hash = YCSB_FNV_OFFSET_BASIS_64;
    for _ in 0..8 {
        hash ^= value & 0xff;
        value >>= 8;
        hash = hash.wrapping_mul(YCSB_FNV_PRIME_64);
    }
    (hash as i64).wrapping_abs() as u64
}

#[cfg(test)]
mod tests {
    use super::{
        key_for_id, prefix_key, random_unique_key, ycsb_scramble, KeySelector, ValueGenerator,
        YcsbLatestSelector,
    };
    use rand::SeedableRng;
    use std::collections::BTreeSet;

    #[test]
    fn benchmark_values_repeat_the_configured_random_fraction() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut values = ValueGenerator::new(0.5);
        let value = values.generate(400, &mut rng);

        assert_eq!(value.len(), 400);
        for fragment in value.chunks_exact(100) {
            assert_eq!(&fragment[..50], &fragment[50..]);
        }
    }

    #[test]
    fn incompressible_benchmark_values_keep_every_byte_random() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut values = ValueGenerator::new(1.0);
        let value = values.generate(400, &mut rng);

        assert_eq!(value.len(), 400);
        assert_ne!(&value[..200], &value[200..]);
    }

    #[test]
    fn numeric_keys_preserve_lexicographic_order() {
        let keys = (0..256).map(|id| key_for_id(id, 16)).collect::<Vec<_>>();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn prefix_layout_has_eight_byte_prefix_and_suffix() {
        let key = prefix_key(7, 9);
        assert_eq!(key.len(), 16);
        assert_eq!(&key[..8], &7_u64.to_be_bytes());
        assert_eq!(&key[8..], &9_u64.to_be_bytes());
    }

    #[test]
    fn ingest_keys_are_unique_without_relying_on_random_bytes() {
        let mut rng = rand::rng();
        let keys = (0..1_000)
            .map(|id| random_unique_key(id, 16, &mut rng))
            .collect::<BTreeSet<_>>();
        assert_eq!(keys.len(), 1_000);
    }

    #[test]
    fn ycsb_scramble_matches_reference_fnv_hashes() {
        assert_eq!(ycsb_scramble(0), 6_284_781_860_667_377_211);
        assert_eq!(ycsb_scramble(1), 8_517_097_267_634_966_620);
        assert_eq!(ycsb_scramble(2), 1_820_151_046_732_198_393);
        assert_eq!(ycsb_scramble(9_999_999_999), 3_605_131_173_811_637_474);
    }

    #[test]
    fn hottest_ycsb_ranks_are_scattered_across_loaded_keys() {
        let record_count = 100_000_000;
        let ids = (0..5)
            .map(|rank| ycsb_scramble(rank) % record_count)
            .collect::<Vec<_>>();
        let first = *ids.iter().min().expect("hot key");
        let last = *ids.iter().max().expect("hot key");

        assert_eq!(
            ids,
            [67_377_211, 34_966_620, 32_198_393, 99_787_802, 71_816_769]
        );
        assert!(last - first > 50_000_000);
    }

    #[test]
    fn scrambled_zipfian_samples_stay_within_loaded_keyspace() {
        let selector = KeySelector::zipfian(1_000);
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);

        assert!((0..10_000).all(|_| selector.sample(&mut rng) < 1_000));
    }

    #[test]
    fn scrambled_zipfian_domain_expands_with_inserted_keys() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(23);
        let selector = KeySelector::zipfian(100);
        let samples = (0..10_000)
            .map(|_| selector.sample_with_record_count(200, &mut rng))
            .collect::<Vec<_>>();

        assert!(samples.iter().all(|sample| *sample < 200));
        assert!(samples.iter().any(|sample| *sample >= 100));
    }

    #[test]
    fn latest_selector_biases_reads_recently_across_the_full_keyspace() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(19);
        let mut selector = YcsbLatestSelector::new(100_000);
        let samples = (0..10_000)
            .map(|_| selector.sample(100_000, &mut rng))
            .collect::<Vec<_>>();

        assert!(samples.iter().all(|sample| *sample < 100_000));
        assert!(samples.iter().any(|sample| *sample < 90_000));
        assert!(samples.iter().filter(|sample| **sample >= 99_000).count() > 5_000);
    }
}
