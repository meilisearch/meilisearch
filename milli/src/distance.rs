use serde::{Deserialize, Serialize};
use space::Metric;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct DotProduct;

impl Metric<Vec<f32>> for DotProduct {
    type Unit = u32;

    // Following <https://docs.rs/space/0.17.0/space/trait.Metric.html>.
    //
    // Here is a playground that validate the ordering of the bit representation of floats in range 0.0..=1.0:
    // <https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=6c59e31a3cc5036b32edf51e8937b56e>
    fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> Self::Unit {
        let dist = 1.0 - dot_product_similarity(a, b);
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}

/// Returns the dot product similarity score that will between 0.0 and 1.0
/// if both vectors are normalized. The higher the more similar the vectors are.
pub fn dot_product_similarity(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(a, b)| a * b).sum()
}
