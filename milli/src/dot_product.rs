use serde::{Deserialize, Serialize};
use space::Metric;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
const MIN_DIM_SIZE_SIMD: usize = 16;

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
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        {
            if std::arch::is_aarch64_feature_detected!("neon") && a.len() >= MIN_DIM_SIZE_SIMD {
                let squared = unsafe { squared_euclid_neon(&a, &b) };
                let dist = squared.sqrt();
                debug_assert!(!dist.is_nan());
                return dist.to_bits();
            }
        }

        let squared: f32 = a.iter().zip(b).map(|(a, b)| (a - b).powi(2)).sum();
        let dist = squared.sqrt();
        debug_assert!(!dist.is_nan());
        dist.to_bits()
    }
}

#[cfg(target_feature = "neon")]
use std::arch::aarch64::*;

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn squared_euclid_neon(v1: &[f32], v2: &[f32]) -> f32 {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum1 = vfmaq_f32(sum1, sub1, sub1);

        let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum2 = vfmaq_f32(sum2, sub2, sub2);

        let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum3 = vfmaq_f32(sum3, sub3, sub3);

        let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        sum4 = vfmaq_f32(sum4, sub4, sub4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
    }
    result
}
