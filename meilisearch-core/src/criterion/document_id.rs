use std::cmp::Ordering;

use compact_arena::SmallArena;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{PostingsListView, QueryWordAutomaton};
use crate::RawDocument;
use super::Criterion;

pub struct DocumentId;

impl Criterion for DocumentId {
    fn name(&self) -> &str { "stable document id" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut SmallArena<PostingsListView>,
        query_enhancer: &QueryEnhancer,
        automatons: &[QueryWordAutomaton],
    ) {
        // ...
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &SmallArena<PostingsListView>,
    ) -> Ordering
    {
        let lhs = &lhs.id;
        let rhs = &rhs.id;

        lhs.cmp(rhs)
    }
}
