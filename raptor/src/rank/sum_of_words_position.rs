use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

#[inline]
fn sum_matches_attribute_index(matches: &[Match]) -> u32 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).attribute_index
    }).sum()
}

#[inline]
pub fn sum_of_words_position(lhs: &Document, rhs: &Document) -> Ordering {
    let lhs = sum_matches_attribute_index(&lhs.matches);
    let rhs = sum_matches_attribute_index(&rhs.matches);

    lhs.cmp(&rhs)
}
