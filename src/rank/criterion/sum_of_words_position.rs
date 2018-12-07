use std::cmp::Ordering;

use group_by::GroupBy;

use crate::database::DatabaseView;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::Match;

#[inline]
fn sum_matches_attribute_index(matches: &[Match]) -> u32 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).attribute_index
    }).sum()
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsPosition;

impl Criterion for SumOfWordsPosition {
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView) -> Ordering {
        let lhs = sum_matches_attribute_index(&lhs.matches);
        let rhs = sum_matches_attribute_index(&rhs.matches);

        lhs.cmp(&rhs)
    }
}
