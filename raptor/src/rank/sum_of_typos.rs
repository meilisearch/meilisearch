use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

#[inline]
fn sum_matches_typos(matches: &[Match]) -> u8 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).distance
    }).sum()
}

#[inline]
pub fn sum_of_typos(lhs: &Document, rhs: &Document) -> Ordering {
    let lhs = sum_matches_typos(&lhs.matches);
    let rhs = sum_matches_typos(&rhs.matches);

    lhs.cmp(&rhs)
}
