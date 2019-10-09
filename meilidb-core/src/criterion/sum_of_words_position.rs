use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::criterion::Criterion;
use crate::RawDocument;

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

    fn name(&self) -> &str {
        "SumOfWordsPosition"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // typing: "soulier"
    //
    // doc0: "Soulier bleu"
    // doc1: "Botte rouge et soulier noir"
    #[test]
    fn easy_case() {
        let query_index0 = &[0];
        let word_index0 = &[0];

        let query_index1 = &[0];
        let word_index1 = &[3];

        let doc0 = sum_matches_attribute_index(query_index0, word_index0);
        let doc1 = sum_matches_attribute_index(query_index1, word_index1);
        assert_eq!(doc0.cmp(&doc1), Ordering::Less);
    }
}
