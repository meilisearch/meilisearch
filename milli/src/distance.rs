use serde::{Deserialize, Serialize};
use space::Metric;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct DotProduct;

impl Metric<Vec<f32>> for DotProduct {
    type Unit = u32;

    // TODO explain me this function, I don't understand why f32.to_bits is ordered.
    // I tried to do this and it wasn't OK <https://stackoverflow.com/a/43305015/1941280>
    //
    // Following <https://docs.rs/space/0.17.0/space/trait.Metric.html>.
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

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Euclidean;

impl Metric<Vec<f32>> for Euclidean {
    type Unit = u32;

    fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> Self::Unit {
        let dist = euclidean_squared_distance(a, b).sqrt();
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}

/// Return the squared euclidean distance between both vectors that will
/// between 0.0 and +inf. The smaller the nearer the vectors are.
pub fn euclidean_squared_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(a, b)| (a - b).powi(2)).sum()
}
