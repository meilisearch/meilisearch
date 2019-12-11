use std::cmp::Ordering;

use compact_arena::SmallArena;
use slice_group_by::GroupBy;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{PostingsListView, SimpleMatch, QueryWordAutomaton};
use crate::RawDocument;

use super::{Criterion, prepare_raw_matches};

pub struct WordsPosition;

impl Criterion for WordsPosition {
    fn name(&self) -> &str { "words position" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
        query_enhancer: &QueryEnhancer,
        automatons: &[QueryWordAutomaton],
    ) {
        prepare_raw_matches(documents, postings_lists, query_enhancer, automatons);
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &SmallArena<'tag, PostingsListView<'txn>>,
    ) -> Ordering
    {
        #[inline]
        fn sum_words_position(matches: &[SimpleMatch]) -> usize {
            let mut sum_words_position = 0;
            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_words_position += group[0].word_index as usize;
            }
            sum_words_position
        }

        let lhs = sum_words_position(&lhs.processed_matches);
        let rhs = sum_words_position(&rhs.processed_matches);

        lhs.cmp(&rhs)
    }
}
