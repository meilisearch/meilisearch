use std::cmp::Ordering;
use group_by::GroupBy;
use crate::Match;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;

#[inline]
fn number_of_query_words(matches: &[Match]) -> usize {
    GroupBy::new(matches, match_query_index).count()
}

#[derive(Debug, Clone, Copy)]
pub struct NumberOfWords;

impl Criterion for NumberOfWords {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        let lhs = number_of_query_words(&lhs.matches);
        let rhs = number_of_query_words(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}
