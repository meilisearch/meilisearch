use std::cmp::{self, Ordering};

use compact_arena::SmallArena;
use slice_group_by::GroupBy;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{SimpleMatch, PostingsListView, QueryWordAutomaton};
use crate::RawDocument;

use super::{Criterion, prepare_raw_matches};

pub struct Attribute;

impl Criterion for Attribute {
    fn name(&self) -> &str { "attribute" }

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
        fn best_attribute(matches: &[SimpleMatch]) -> u16 {
            let mut best_attribute = u16::max_value();
            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                best_attribute = cmp::min(best_attribute, group[0].attribute);
            }
            best_attribute
        }

        let lhs = best_attribute(&lhs.processed_matches);
        let rhs = best_attribute(&rhs.processed_matches);

        lhs.cmp(&rhs)
    }
}
