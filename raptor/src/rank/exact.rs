use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

#[inline]
fn contains_exact(matches: &[Match]) -> bool {
    matches.iter().any(|m| m.is_exact)
}

#[inline]
fn number_exact_matches(matches: &[Match]) -> usize {
    GroupBy::new(matches, match_query_index).map(contains_exact).count()
}

#[inline]
pub fn exact(lhs: &Document, rhs: &Document) -> Ordering {
    let lhs = number_exact_matches(&lhs.matches);
    let rhs = number_exact_matches(&rhs.matches);

    lhs.cmp(&rhs)
}
