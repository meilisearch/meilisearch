use crate::criterion::Criterion;
use crate::RawDocument;
use slice_group_by::GroupBy;
use std::cmp::Ordering;

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

    fn name(&self) -> &str {
        "SumOfWordsAttribute"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // typing: "soulier"
    //
    // doc0: { 0. "Soulier bleu", 1. "bla bla bla" }
    // doc1: { 0. "Botte rouge", 1. "Soulier en cuir" }
    #[test]
    fn title_vs_description() {
        let query_index0 = &[0];
        let attribute0 = &[0];

        let query_index1 = &[0];
        let attribute1 = &[1];

        let doc0 = sum_matches_attributes(query_index0, attribute0);
        let doc1 = sum_matches_attributes(query_index1, attribute1);
        assert_eq!(doc0.cmp(&doc1), Ordering::Less);
    }
}
