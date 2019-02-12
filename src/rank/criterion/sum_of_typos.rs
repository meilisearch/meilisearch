use std::cmp::Ordering;

use slice_group_by::GroupBy;

use crate::rank::criterion::Criterion;
use crate::rank::RawDocument;

#[inline]
fn sum_matches_typos(query_index: &[u32], distance: &[u8]) -> isize {
    let mut number_words = 0.0;
    let mut sum_typos = 0.0;
    let mut index = 0;

    for group in query_index.linear_group_by(PartialEq::eq) {
        let typo = distance[index] as f32;
        sum_typos += (typo + 1.0).log10();
        number_words += 1.0_f32;
        index += group.len();
    }

    let out = number_words / (sum_typos + 1.0);

    (out * 1000.0) as isize
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfTypos;

impl Criterion for SumOfTypos {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = {
            let query_index = lhs.query_index();
            let distance = lhs.distance();
            sum_matches_typos(query_index, distance)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let distance = rhs.distance();
            sum_matches_typos(query_index, distance)
        };

        lhs.cmp(&rhs).reverse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // typing: "Geox CEO"
    //
    // doc0: "Geox SpA: CEO and Executive"
    // doc1: "Mt. Gox CEO Resigns From Bitcoin Foundation"
    #[test]
    fn one_typo_reference() {
        let query_index0 = &[0, 1];
        let distance0 = &[0, 0];

        let query_index1 = &[0, 1];
        let distance1 = &[1, 0];

        let doc0 = sum_matches_typos(query_index0, distance0);
        let doc1 = sum_matches_typos(query_index1, distance1);
        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }

    // typing: "bouton manchette"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn no_typo() {
        let query_index0 = &[0, 1];
        let distance0 = &[0, 0];

        let query_index1 = &[0];
        let distance1 = &[0];

        let doc0 = sum_matches_typos(query_index0, distance0);
        let doc1 = sum_matches_typos(query_index1, distance1);
        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }

    // typing: "bouton manchztte"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn one_typo() {
        let query_index0 = &[0, 1];
        let distance0 = &[0, 1];

        let query_index1 = &[0];
        let distance1 = &[0];

        let doc0 = sum_matches_typos(query_index0, distance0);
        let doc1 = sum_matches_typos(query_index1, distance1);
        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }
}
