use std::cmp::{Ordering, Reverse};

use compact_arena::SmallArena;
use slice_group_by::GroupBy;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{PostingsListView, BareMatch, QueryWordAutomaton};
use crate::RawDocument;
use super::Criterion;

pub struct Exact;

impl Criterion for Exact {
    fn name(&self) -> &str { "exact" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut SmallArena<PostingsListView>,
        query_enhancer: &QueryEnhancer,
        automatons: &[QueryWordAutomaton],
    ) {
        for document in documents {
            document.raw_matches.sort_unstable_by_key(|bm| (bm.query_index, Reverse(bm.is_exact)));
        }
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &SmallArena<PostingsListView>,
    ) -> Ordering
    {
        #[inline]
        fn sum_exact_query_words(matches: &[BareMatch]) -> usize {
            let mut sum_exact_query_words = 0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_exact_query_words += group[0].is_exact as usize;
            }

            sum_exact_query_words
        }

        let lhs = sum_exact_query_words(&lhs.raw_matches);
        let rhs = sum_exact_query_words(&rhs.raw_matches);

        lhs.cmp(&rhs).reverse()
    }
}
