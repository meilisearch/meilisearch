use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

#[inline]
fn number_of_query_words(matches: &[Match]) -> usize {
    GroupBy::new(matches, match_query_index).count()
}

#[inline]
pub fn number_of_words(lhs: &Document, rhs: &Document) -> Ordering {
    let lhs = number_of_query_words(&lhs.matches);
    let rhs = number_of_query_words(&rhs.matches);

    lhs.cmp(&rhs).reverse()
}
