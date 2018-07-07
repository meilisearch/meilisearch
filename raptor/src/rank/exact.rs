use std::cmp::Ordering;
use Match;
use rank::{match_query_index, Document};
use group_by::GroupBy;

pub fn exact(lhs: &Document, rhs: &Document) -> Ordering {
    let contains_exact = |matches: &[Match]| matches.iter().any(|m| m.is_exact);
    let key = |matches: &[Match]| -> usize {
        GroupBy::new(matches, match_query_index).map(contains_exact).filter(Clone::clone).count()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches))
}
