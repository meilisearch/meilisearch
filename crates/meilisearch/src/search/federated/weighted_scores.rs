use std::cmp::Ordering;

use meilisearch_types::milli::score_details::{self, WeightedScoreValue};

pub fn compare(
    mut left_it: impl Iterator<Item = WeightedScoreValue>,
    left_weighted_global_score: f64,
    mut right_it: impl Iterator<Item = WeightedScoreValue>,
    right_weighted_global_score: f64,
) -> Ordering {
    loop {
        let left = left_it.next();
        let right = right_it.next();

        match (left, right) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (
                Some(
                    WeightedScoreValue::WeightedScore(left) | WeightedScoreValue::VectorSort(left),
                ),
                Some(
                    WeightedScoreValue::WeightedScore(right)
                    | WeightedScoreValue::VectorSort(right),
                ),
            ) => {
                if (left - right).abs() <= f64::EPSILON {
                    continue;
                }
                return left.partial_cmp(&right).unwrap();
            }
            (
                Some(WeightedScoreValue::Sort { asc: left_asc, value: left }),
                Some(WeightedScoreValue::Sort { asc: right_asc, value: right }),
            ) => {
                if left_asc != right_asc {
                    return left_weighted_global_score
                        .partial_cmp(&right_weighted_global_score)
                        .unwrap();
                }
                match score_details::compare_sort_values(left_asc, &left, &right) {
                    Ordering::Equal => continue,
                    order => return order,
                }
            }
            (
                Some(WeightedScoreValue::GeoSort { asc: left_asc, distance: left }),
                Some(WeightedScoreValue::GeoSort { asc: right_asc, distance: right }),
            ) => {
                if left_asc != right_asc {
                    continue;
                }
                match (left, right) {
                    (None, None) => continue,
                    (None, Some(_)) => return Ordering::Less,
                    (Some(_), None) => return Ordering::Greater,
                    (Some(left), Some(right)) => {
                        if (left - right).abs() <= f64::EPSILON {
                            continue;
                        }
                        return left.partial_cmp(&right).unwrap();
                    }
                }
            }
            // not comparable details, use global
            (Some(WeightedScoreValue::WeightedScore(_)), Some(_))
            | (Some(_), Some(WeightedScoreValue::WeightedScore(_)))
            | (Some(WeightedScoreValue::VectorSort(_)), Some(_))
            | (Some(_), Some(WeightedScoreValue::VectorSort(_)))
            | (Some(WeightedScoreValue::GeoSort { .. }), Some(WeightedScoreValue::Sort { .. }))
            | (Some(WeightedScoreValue::Sort { .. }), Some(WeightedScoreValue::GeoSort { .. })) => {
                let left_count = left_it.count();
                let right_count = right_it.count();
                // compare how many remaining groups of rules each side has.
                // the group with the most remaining groups wins.
                return left_count
                    .cmp(&right_count)
                    // breaks ties with the global ranking score
                    .then_with(|| {
                        left_weighted_global_score
                            .partial_cmp(&right_weighted_global_score)
                            .unwrap()
                    });
            }
        }
    }
}
