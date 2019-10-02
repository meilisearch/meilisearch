use std::cmp::Ordering;

use slice_group_by::GroupBy;

use crate::criterion::Criterion;
use crate::RawDocument;

// This function is a wrong logarithmic 10 function.
// It is safe to panic on input number higher than 3,
// the number of typos is never bigger than that.
#[inline]
fn custom_log10(n: u8) -> f32 {
    match n {
        0 => 0.0,       // log(1)
        1 => 0.30102,   // log(2)
        2 => 0.47712,   // log(3)
        3 => 0.60205,   // log(4)
        _ => panic!("invalid number"),
    }
}

#[inline]
fn sum_matches_typos(query_index: &[u32], distance: &[u8]) -> usize {
    let mut number_words: usize = 0;
    let mut sum_typos = 0.0;
    let mut index = 0;

    for group in query_index.linear_group() {
        sum_typos += custom_log10(distance[index]);
        number_words += 1;
        index += group.len();
    }

    (number_words as f32 / (sum_typos + 1.0) * 1000.0) as usize
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

    fn name(&self) -> &'static str {
        "SumOfTypos"
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
