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
        let dist: f32 = a.iter().zip(b).map(|(a, b)| a * b).sum();
        let dist = 1.0 - dist;
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct Euclidean;

impl Metric<Vec<f32>> for Euclidean {
    type Unit = u32;

    fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> Self::Unit {
        let squared: f32 = a.iter().zip(b).map(|(a, b)| (a - b).powi(2)).sum();
        let dist = squared.sqrt();
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}
