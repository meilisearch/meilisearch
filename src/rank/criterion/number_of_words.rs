use std::cmp::Ordering;

use slice_group_by::GroupBy;

use crate::rank::criterion::Criterion;
use crate::rank::RawDocument;

#[inline]
fn number_of_query_words(query_index: &[u32]) -> usize {
    query_index.linear_group().count()
}

#[derive(Debug, Clone, Copy)]
pub struct NumberOfWords;

impl Criterion for NumberOfWords {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            number_of_query_words(query_index)
        };
        let rhs = {
            let query_index = rhs.query_index();
            number_of_query_words(query_index)
        };

        lhs.cmp(&rhs).reverse()
    }
}
