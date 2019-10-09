use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::criterion::Criterion;
use crate::RawDocument;

#[inline]
fn number_exact_matches(query_index: &[u32], is_exact: &[bool]) -> usize {
    let mut count = 0;
    let mut index = 0;

    for group in query_index.linear_group() {
        let len = group.len();
        count += is_exact[index..index + len].contains(&true) as usize;
        index += len;
    }

    count
}

#[derive(Debug, Clone, Copy)]
pub struct Exact;

impl Criterion for Exact {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            let is_exact = lhs.is_exact();
            number_exact_matches(query_index, is_exact)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let is_exact = rhs.is_exact();
            number_exact_matches(query_index, is_exact)
        };

        lhs.cmp(&rhs).reverse()
    }

    fn name(&self) -> &str {
        "Exact"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // typing: "soulier"
    //
    // doc0: "Soulier bleu"
    // doc1: "souliereres rouge"
    #[test]
    fn easy_case() {
        let query_index0 = &[0];
        let is_exact0 = &[true];

        let query_index1 = &[0];
        let is_exact1 = &[false];

        let doc0 = number_exact_matches(query_index0, is_exact0);
        let doc1 = number_exact_matches(query_index1, is_exact1);
        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }
}
