use std::ops;

use instant_distance::Point;
use serde::{Deserialize, Serialize};

use crate::normalize_vector;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NDotProductPoint(Vec<f32>);

impl NDotProductPoint {
    pub fn new(point: Vec<f32>) -> Self {
        NDotProductPoint(normalize_vector(point))
    }

    pub fn into_inner(self) -> Vec<f32> {
        self.0
    }
}

impl ops::Deref for NDotProductPoint {
    type Target = [f32];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl Point for NDotProductPoint {
    fn distance(&self, other: &Self) -> f32 {
        let dist = 1.0 - dot_product_similarity(&self.0, &other.0);
        debug_assert!(!dist.is_nan());
        dist
    }
}

/// Returns the dot product similarity score that will between 0.0 and 1.0
/// if both vectors are normalized. The higher the more similar the vectors are.
pub fn dot_product_similarity(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(a, b)| a * b).sum()
}
