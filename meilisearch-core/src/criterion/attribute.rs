use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::RawDocument;
use crate::bucket_sort::SimpleMatch;
use super::{Criterion, Context, ContextMut, prepare_raw_matches};

pub struct Attribute;

impl Criterion for Attribute {
    fn name(&self) -> &str { "attribute" }

    fn prepare<'p, 'tag, 'txn, 'q, 'a, 'r>(
        &self,
        ctx: ContextMut<'p, 'tag, 'txn, 'q, 'a>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) {
        prepare_raw_matches(documents, ctx.postings_lists, ctx.query_enhancer, ctx.automatons);
    }

    fn evaluate(&self, _ctx: &Context, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        #[inline]
        fn sum_of_attribute(matches: &[SimpleMatch]) -> usize {
            let mut sum_of_attribute = 0;
            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_of_attribute += group[0].attribute as usize;
            }
            sum_of_attribute
        }

        let lhs = sum_of_attribute(&lhs.processed_matches);
        let rhs = sum_of_attribute(&rhs.processed_matches);

        lhs.cmp(&rhs)
    }
}
