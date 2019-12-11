use std::cmp::Ordering;

use compact_arena::SmallArena;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{PostingsListView, QueryWordAutomaton};
use crate::RawDocument;

use super::{Criterion, prepare_query_distances};

pub struct Words;

impl Criterion for Words {
    fn name(&self) -> &str { "words" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
        query_enhancer: &QueryEnhancer,
        automatons: &[QueryWordAutomaton],
    ) {
        prepare_query_distances(documents, query_enhancer, automatons, postings_lists);
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &SmallArena<PostingsListView>,
    ) -> Ordering
    {
        #[inline]
        fn number_of_query_words(distances: &[Option<u8>]) -> usize {
            distances.iter().cloned().filter(Option::is_some).count()
        }

        let lhs = number_of_query_words(&lhs.processed_distances);
        let rhs = number_of_query_words(&rhs.processed_distances);

        lhs.cmp(&rhs).reverse()
    }
}
