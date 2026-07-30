use std::cmp::Ordering;

use meilisearch_types::milli::score_details::WeightedScoreValue;

pub fn compare(
    left_it: impl Iterator<Item = WeightedScoreValue>,
    left_weighted_global_score: f64,
    right_it: impl Iterator<Item = WeightedScoreValue>,
    right_weighted_global_score: f64,
) -> Ordering {
    WeightedScoreValue::compare_partial(left_it, right_it).unwrap_or_else(|| {
        left_weighted_global_score.partial_cmp(&right_weighted_global_score).unwrap()
    })
}
