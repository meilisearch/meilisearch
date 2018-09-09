use std::cmp::Ordering;
use group_by::GroupBy;
use crate::Match;
use crate::rank::{match_query_index, Document};

#[inline]
fn sum_matches_attributes(matches: &[Match]) -> u8 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).attribute
    }).sum()
}

#[inline]
pub fn sum_of_words_attribute(lhs: &Document, rhs: &Document) -> Ordering {
    let lhs = sum_matches_attributes(&lhs.matches);
    let rhs = sum_matches_attributes(&rhs.matches);

    lhs.cmp(&rhs)
}
