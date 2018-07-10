use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

fn key(matches: &[Match]) -> u32 {
    GroupBy::new(matches, match_query_index).map(|m| m[0].attribute_index).sum()
}

pub fn sum_of_words_position(lhs: &Document, rhs: &Document) -> Ordering {
    key(&lhs.matches).cmp(&key(&rhs.matches))
}
