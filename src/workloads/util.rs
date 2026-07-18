use bytes::Bytes;
use rand::distr::Distribution;
use rand::{Rng, RngCore};
use rand_distr::Zipf;

const ZIPFIAN_EXPONENT: f64 = 0.99;
const FNV_OFFSET_BASIS_64: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME_64: u64 = 1_099_511_628_211;
const VALUE_CORPUS_BYTES: usize = 1_048_576;

pub fn key_for_id(id: u64, size: usize) -> Bytes {
    let mut key = vec![b'0'; size];
    let bytes_to_fill = size.min(8);
    key[..bytes_to_fill].copy_from_slice(&id.to_be_bytes()[8 - bytes_to_fill..]);
    Bytes::from(key)
}

pub fn missing_key_for_id(id: u64, size: usize) -> Bytes {
    let mut key = key_for_id(id, size).to_vec();
    if let Some(last) = key.last_mut() {
        *last = b'1';
    }
    Bytes::from(key)
}

pub struct ValueGenerator {
    corpus: Option<Bytes>,
    position: usize,
}

impl ValueGenerator {
    pub fn new() -> Self {
        Self {
            corpus: None,
            position: 0,
        }
    }

    pub fn generate(&mut self, size: usize, rng: &mut impl RngCore) -> Bytes {
        if size == 0 {
            return Bytes::new();
        }
        let corpus = self.corpus.get_or_insert_with(|| {
            let mut corpus = vec![0_u8; VALUE_CORPUS_BYTES.max(size)];
            rng.fill_bytes(&mut corpus);
            Bytes::from(corpus)
        });
        if self.position.saturating_add(size) > corpus.len() {
            self.position = 0;
        }
        let value = corpus.slice(self.position..self.position + size);
        self.position += size;
        value
    }
}

pub enum KeySelector {
    Uniform { record_count: u64 },
    ScrambledZipfian { record_count: u64, zipf: Zipf<f64> },
}

impl KeySelector {
    pub fn uniform(record_count: u64) -> Self {
        Self::Uniform { record_count }
    }

    pub fn zipfian(record_count: u64) -> Self {
        let record_count = record_count.max(1);
        Self::ScrambledZipfian {
            record_count,
            zipf: Zipf::new(record_count as f64, ZIPFIAN_EXPONENT)
                .expect("positive Zipfian domain"),
        }
    }

    pub fn sample(&self, rng: &mut impl Rng) -> u64 {
        match self {
            Self::Uniform { record_count } => {
                if *record_count == 0 {
                    0
                } else {
                    rng.random_range(0..*record_count)
                }
            }
            Self::ScrambledZipfian { record_count, zipf } => {
                let rank = (zipf.sample(rng) as u64).saturating_sub(1);
                fnv_hash(rank) % record_count
            }
        }
    }
}

fn fnv_hash(mut value: u64) -> u64 {
    let mut hash = FNV_OFFSET_BASIS_64;
    for _ in 0..8 {
        hash ^= value & 0xff;
        value >>= 8;
        hash = hash.wrapping_mul(FNV_PRIME_64);
    }
    (hash as i64).wrapping_abs() as u64
}

#[cfg(test)]
mod tests {
    use super::{key_for_id, missing_key_for_id, KeySelector, ValueGenerator};
    use rand::SeedableRng;

    #[test]
    fn keys_use_the_documented_binary_prefix_and_ascii_padding() {
        let key = key_for_id(0x0102_0304_0506_0708, 20);
        assert_eq!(&key[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(&key[8..], &[b'0'; 12]);

        let missing = missing_key_for_id(0x0102_0304_0506_0708, 20);
        assert_eq!(&missing[..19], &key[..19]);
        assert_eq!(missing[19], b'1');
    }

    #[test]
    fn values_are_incompressible_and_have_the_requested_size() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut values = ValueGenerator::new();
        let value = values.generate(400, &mut rng);
        assert_eq!(value.len(), 400);
        assert_ne!(&value[..200], &value[200..]);
    }

    #[test]
    fn scrambled_zipfian_samples_stay_in_the_fixed_domain() {
        let selector = KeySelector::zipfian(1_000);
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        assert!((0..10_000).all(|_| selector.sample(&mut rng) < 1_000));
    }
}
