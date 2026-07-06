use std::cmp::Ordering;

use meilisearch_types::milli::score_details::WeightedScoreValue;

pub fn compare(
    left_it: impl Iterator<Item = WeightedScoreValue>,
    left_weighted_global_score: f64,
    right_it: impl Iterator<Item = WeightedScoreValue>,
    right_weighted_global_score: f64,
) -> Ordering {
    compare_partial(left_it, right_it).unwrap_or_else(|| {
        left_weighted_global_score.partial_cmp(&right_weighted_global_score).unwrap()
    })
}

pub fn compare_partial(
    mut left_it: impl Iterator<Item = WeightedScoreValue>,
    mut right_it: impl Iterator<Item = WeightedScoreValue>,
) -> Option<Ordering> {
    loop {
        let left = left_it.next();
        let right = right_it.next();

        match (left, right) {
            (None, None) => return Some(Ordering::Equal),
            (None, Some(_)) => return Some(Ordering::Less),
            (Some(_), None) => return Some(Ordering::Greater),
            (Some(left), Some(right)) => match left.partial_cmp(&right) {
                Some(Ordering::Equal) => continue,
                Some(order) => return Some(order),
                None => {
                    let left_count = left_it.count();
                    let right_count = right_it.count();
                    // compare how many remaining groups of rules each side has.
                    // the group with the most remaining groups wins.
                    let count_nb = left_count.cmp(&right_count);
                    if count_nb.is_eq() {
                        return None;
                    } else {
                        return Some(count_nb);
                    }
                }
            },
        }
    }
}
