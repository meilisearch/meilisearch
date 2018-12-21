use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use group_by::GroupBy;

use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::Match;

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

/// A criterion that do the sum of all typos of all matches in documents
/// and sort them by this sum, a document having more typos is less important.
#[derive(Debug, Clone, Copy)]
pub struct SumOfTypos;

impl<D> Criterion<D> for SumOfTypos
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = sum_matches_typos(&lhs.matches);
        let rhs = sum_matches_typos(&rhs.matches);

        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{DocumentId, Attribute, WordArea};

    // typing: "Geox CEO"
    //
    // doc0: "Geox SpA: CEO and Executive"
    // doc1: "Mt. Gox CEO Resigns From Bitcoin Foundation"
    #[test]
    fn one_typo_reference() {
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
                Match { query_index: 1, distance: 0, attribute: Attribute::new(0, 2), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(0),
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 1, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
                Match { query_index: 1, distance: 0, attribute: Attribute::new(0, 2), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(1),
                matches: matches,
            }
        };

        let lhs = sum_matches_typos(&doc0.matches);
        let rhs = sum_matches_typos(&doc1.matches);
        assert_eq!(lhs.cmp(&rhs), Ordering::Less);
    }

    // typing: "bouton manchette"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn no_typo() {
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
                Match { query_index: 1, distance: 0, attribute: Attribute::new(0, 1), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(0),
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(1),
                matches: matches,
            }
        };

        let lhs = sum_matches_typos(&doc0.matches);
        let rhs = sum_matches_typos(&doc1.matches);
        assert_eq!(lhs.cmp(&rhs), Ordering::Less);
    }

    // typing: "bouton manchztte"
    //
    // doc0: "bouton manchette"
    // doc1: "bouton"
    #[test]
    fn one_typo() {
        let doc0 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
                Match { query_index: 1, distance: 1, attribute: Attribute::new(0, 1), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(0),
                matches: matches,
            }
        };

        let doc1 = {
            let matches = vec![
                Match { query_index: 0, distance: 0, attribute: Attribute::new(0, 0), is_exact: false, word_area: WordArea::new(0, 6) },
            ];
            Document {
                id: DocumentId(1),
                matches: matches,
            }
        };

        let lhs = sum_matches_typos(&doc0.matches);
        let rhs = sum_matches_typos(&doc1.matches);
        assert_eq!(lhs.cmp(&rhs), Ordering::Equal);
    }
}
