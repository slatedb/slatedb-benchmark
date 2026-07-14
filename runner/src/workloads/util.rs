use bytes::Bytes;
use rand::distr::Distribution;
use rand::{Rng, RngCore};
use rand_distr::Zipf;

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

pub fn random_value(size: usize, rng: &mut impl RngCore) -> Bytes {
    let mut value = vec![0_u8; size];
    rng.fill_bytes(&mut value);
    Bytes::from(value)
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
            zipf: Zipf::new(record_count.max(1) as f64, 0.99).ok(),
            record_count,
        }
    }

    pub fn sample(&self, rng: &mut impl Rng) -> u64 {
        if self.record_count == 0 {
            return 0;
        }
        self.zipf
            .as_ref()
            .map(|zipf| zipf.sample(rng) as u64 - 1)
            .unwrap_or_else(|| rng.random_range(0..self.record_count))
    }
}

pub fn choose_coprime_multiplier(record_count: u64, rng: &mut impl Rng) -> u64 {
    if record_count <= 2 {
        return 1;
    }
    loop {
        let candidate = rng.random_range(1..record_count);
        if gcd(candidate, record_count) == 1 {
            return candidate;
        }
    }
}

fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[cfg(test)]
mod tests {
    use super::{key_for_id, prefix_key, random_unique_key};
    use std::collections::BTreeSet;

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
}
