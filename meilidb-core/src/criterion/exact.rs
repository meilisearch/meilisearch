use std::cmp::Ordering;

use sdset::Set;
use slice_group_by::GroupBy;
use meilidb_schema::SchemaAttr;

use crate::criterion::Criterion;
use crate::RawDocument;

#[inline]
fn number_exact_matches(
    query_index: &[u32],
    attribute: &[u16],
    is_exact: &[bool],
    fields_counts: &Set<(SchemaAttr, u64)>,
) -> usize
{
    let mut count = 0;
    let mut index = 0;

    for group in query_index.linear_group() {
        let len = group.len();

        let mut found_exact = false;
        for (pos, _) in is_exact[index..index + len].iter().filter(|x| **x).enumerate() {
            found_exact = true;
            if let Ok(pos) = fields_counts.binary_search_by_key(&attribute[pos], |(a, _)| a.0) {
                let (_, count) = fields_counts[pos];
                if count == 1 {
                    return usize::max_value()
                }
            }
        }

        count += found_exact as usize;
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
            let attribute = lhs.attribute();
            let fields_counts = &lhs.fields_counts;

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
        };

        let rhs = {
            let query_index = rhs.query_index();
            let is_exact = rhs.is_exact();
            let attribute = rhs.attribute();
            let fields_counts = &rhs.fields_counts;

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
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
        let doc0 = {
            let query_index   = &[0];
            let attribute     = &[0];
            let is_exact      = &[true];
            let fields_counts = Set::new(&[(SchemaAttr(0), 2)]).unwrap();

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
        };

        let doc1 = {
            let query_index   = &[0];
            let attribute     = &[0];
            let is_exact      = &[false];
            let fields_counts = Set::new(&[(SchemaAttr(0), 2)]).unwrap();

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
        };

        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }

    // typing: "soulier"
    //
    // doc0: { 0. "soulier" }
    // doc1: { 0. "soulier bleu et blanc" }
    #[test]
    fn basic() {
        let doc0 = {
            let query_index   = &[0];
            let attribute     = &[0];
            let is_exact      = &[true];
            let fields_counts = Set::new(&[(SchemaAttr(0), 1)]).unwrap();

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
        };

        let doc1 = {
            let query_index   = &[0];
            let attribute     = &[0];
            let is_exact      = &[true];
            let fields_counts = Set::new(&[(SchemaAttr(0), 4)]).unwrap();

            number_exact_matches(query_index, attribute, is_exact, fields_counts)
        };

        assert_eq!(doc0.cmp(&doc1).reverse(), Ordering::Less);
    }
}
