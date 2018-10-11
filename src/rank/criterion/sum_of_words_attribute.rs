use std::cmp::Ordering;
use group_by::GroupBy;
use crate::Match;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;

#[inline]
fn sum_matches_attributes(matches: &[Match]) -> u8 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).attribute
    }).sum()
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsAttribute;

impl Criterion for SumOfWordsAttribute {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        let lhs = sum_matches_attributes(&lhs.matches);
        let rhs = sum_matches_attributes(&rhs.matches);

        lhs.cmp(&rhs)
    }
}
