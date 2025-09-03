use deserr::{DeserializeError, Deserr};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Describes the mean and sigma of distribution of embedding similarity in the embedding space.
///
/// The intended use is to make the similarity score more comparable to the regular ranking score.
/// This allows to correct effects where results are too "packed" around a certain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize, ToSchema)]
#[serde(from = "DistributionShiftSerializable")]
#[serde(into = "DistributionShiftSerializable")]
pub struct DistributionShift {
    /// Value where the results are "packed".
    ///
    /// Similarity scores are translated so that they are packed around 0.5 instead
    #[schema(value_type = f32)]
    pub current_mean: OrderedFloat<f32>,

    /// standard deviation of a similarity score.
    ///
    /// Set below 0.4 to make the results less packed around the mean, and above 0.4 to make them more packed.
    #[schema(value_type = f32)]
    pub current_sigma: OrderedFloat<f32>,
}

impl<E> Deserr<E> for DistributionShift
where
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        let value = DistributionShiftSerializable::deserialize_from_value(value, location)?;
        if value.mean < 0. || value.mean > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution mean must be in the range [0, 1], got {}",
                        value.mean
                    ),
                },
                location,
            )));
        }
        if value.sigma <= 0. || value.sigma > 1. {
            return Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                None,
                deserr::ErrorKind::Unexpected {
                    msg: format!(
                        "the distribution sigma must be in the range ]0, 1], got {}",
                        value.sigma
                    ),
                },
                location,
            )));
        }

        Ok(value.into())
    }
}

#[derive(Serialize, Deserialize, Deserr)]
#[serde(deny_unknown_fields)]
#[deserr(deny_unknown_fields)]
struct DistributionShiftSerializable {
    mean: f32,
    sigma: f32,
}

impl From<DistributionShift> for DistributionShiftSerializable {
    fn from(
        DistributionShift {
            current_mean: OrderedFloat(current_mean),
            current_sigma: OrderedFloat(current_sigma),
        }: DistributionShift,
    ) -> Self {
        Self { mean: current_mean, sigma: current_sigma }
    }
}

impl From<DistributionShiftSerializable> for DistributionShift {
    fn from(DistributionShiftSerializable { mean, sigma }: DistributionShiftSerializable) -> Self {
        Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) }
    }
}

impl DistributionShift {
    /// `None` if sigma <= 0.
    pub fn new(mean: f32, sigma: f32) -> Option<Self> {
        if sigma <= 0.0 {
            None
        } else {
            Some(Self { current_mean: OrderedFloat(mean), current_sigma: OrderedFloat(sigma) })
        }
    }

    pub fn shift(&self, score: f32) -> f32 {
        let current_mean = self.current_mean.0;
        let current_sigma = self.current_sigma.0;
        // <https://math.stackexchange.com/a/2894689>
        // We're somewhat abusively mapping the distribution of distances to a gaussian.
        // The parameters we're given is the mean and sigma of the native result distribution.
        // We're using them to retarget the distribution to a gaussian centered on 0.5 with a sigma of 0.4.

        let target_mean = 0.5;
        let target_sigma = 0.4;

        // a^2 sig1^2 = sig2^2 => a^2 = sig2^2 / sig1^2 => a = sig2 / sig1, assuming a, sig1, and sig2 positive.
        let factor = target_sigma / current_sigma;
        // a*mu1 + b = mu2 => b = mu2 - a*mu1
        let offset = target_mean - (factor * current_mean);

        let mut score = factor * score + offset;

        // clamp the final score in the ]0, 1] interval.
        if score <= 0.0 {
            score = f32::EPSILON;
        }
        if score > 1.0 {
            score = 1.0;
        }

        score
    }
}
