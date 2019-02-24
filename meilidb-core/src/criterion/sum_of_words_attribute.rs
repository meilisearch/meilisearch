use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::criterion::Criterion;
use crate::RawDocument;

#[inline]
fn sum_matches_attributes(query_index: &[u32], attribute: &[u16]) -> usize {
    let mut sum_attributes = 0;
    let mut index = 0;

    for group in query_index.linear_group() {
        sum_attributes += attribute[index] as usize;
        index += group.len();
    }

    sum_attributes
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsAttribute;

impl Criterion for SumOfWordsAttribute {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            let attribute = lhs.attribute();
            sum_matches_attributes(query_index, attribute)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let attribute = rhs.attribute();
            sum_matches_attributes(query_index, attribute)
        };

        lhs.cmp(&rhs)
    }
}
