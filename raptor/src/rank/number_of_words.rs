use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

pub fn number_of_words(lhs: &Document, rhs: &Document) -> Ordering {
    let key = |matches: &[Match]| -> usize {
        GroupBy::new(matches, match_query_index).count()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches)).reverse()
}
