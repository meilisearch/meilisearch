use serde::{Deserialize, Serialize};
use space::Metric;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub struct DotProduct;

impl Metric<Vec<f32>> for DotProduct {
    type Unit = u32;

    // Following <https://docs.rs/space/0.17.0/space/trait.Metric.html>.
    fn distance(&self, a: &Vec<f32>, b: &Vec<f32>) -> Self::Unit {
        let dist: f32 = a.iter().zip(b).map(|(a, b)| a * b).sum();
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}
