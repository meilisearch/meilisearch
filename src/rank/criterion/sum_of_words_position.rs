use std::cmp::Ordering;

use slice_group_by::GroupBy;

use crate::rank::criterion::Criterion;
use crate::rank::RawDocument;

#[inline]
fn sum_matches_attribute_index(query_index: &[u32], word_index: &[u16]) -> usize {
    let mut sum_word_index = 0;
    let mut index = 0;

    for group in query_index.linear_group() {
        sum_word_index += word_index[index] as usize;
        index += group.len();
    }

    sum_word_index
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsPosition;

impl Criterion for SumOfWordsPosition {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            let word_index = lhs.word_index();
            sum_matches_attribute_index(query_index, word_index)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let word_index = rhs.word_index();
            sum_matches_attribute_index(query_index, word_index)
        };

        lhs.cmp(&rhs)
    }
}
