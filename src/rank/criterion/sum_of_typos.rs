use std::cmp::Ordering;
use group_by::GroupBy;
use crate::Match;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;

#[inline]
fn sum_matches_typos(matches: &[Match]) -> i8 {
    let mut sum_typos = 0;
    let mut number_words = 0;

    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    for group in GroupBy::new(matches, match_query_index) {
        sum_typos += unsafe { group.get_unchecked(0).distance } as i8;
        number_words += 1;
    }

    sum_typos - number_words
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfTypos;

impl Criterion for SumOfTypos {
    fn evaluate(&self, lhs: &Document, rhs: &Document) -> Ordering {
        let lhs = sum_matches_typos(&lhs.matches);
        let rhs = sum_matches_typos(&rhs.matches);

        lhs.cmp(&rhs)
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
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: 0, attribute_index: 0, is_exact: false },
                Match { query_index: 1, distance: 0, attribute: 0, attribute_index: 2, is_exact: false },
            ];
            Document {
                id: 0,
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 1, attribute: 0, attribute_index: 0, is_exact: false },
                Match { query_index: 1, distance: 0, attribute: 0, attribute_index: 2, is_exact: false },
            ];
            Document {
                id: 1,
                matches: matches,
            }
        };

        assert_eq!(SumOfTypos.evaluate(&doc0, &doc1), Ordering::Less);
    }

    // typing: "bouton manchette"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn no_typo() {
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: 0, attribute_index: 0, is_exact: false },
                Match { query_index: 1, distance: 0, attribute: 0, attribute_index: 1, is_exact: false },
            ];
            Document {
                id: 0,
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: 0, attribute_index: 0, is_exact: false },
            ];
            Document {
                id: 1,
                matches: matches,
            }
        };

        assert_eq!(SumOfTypos.evaluate(&doc0, &doc1), Ordering::Less);
    }

    // typing: "bouton manchztte"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn one_typo() {
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: 0, attribute_index: 0, is_exact: false },
                Match { query_index: 1, distance: 1, attribute: 0, attribute_index: 1, is_exact: false },
            ];
            Document {
                id: 0,
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: 0, attribute_index: 0, is_exact: false },
            ];
            Document {
                id: 1,
                matches: matches,
            }
        };

        assert_eq!(SumOfTypos.evaluate(&doc0, &doc1), Ordering::Equal);
    }
}
