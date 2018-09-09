use std::cmp::Ordering;
use group_by::GroupBy;
use crate::Match;
use crate::rank::{match_query_index, Document};

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
