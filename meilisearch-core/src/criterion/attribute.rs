use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::{RawDocument, MResult};
use crate::bucket_sort::SimpleMatch;
use super::{Criterion, Context, ContextMut, prepare_bare_matches};

pub struct Attribute;

impl Criterion for Attribute {
    fn name(&self) -> &str { "attribute" }

    fn prepare<'h, 'p, 'tag, 'txn, 'q, 'r>(
        &self,
        ctx: ContextMut<'h, 'p, 'tag, 'txn, 'q>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) -> MResult<()>
    {
        prepare_bare_matches(documents, ctx.postings_lists, ctx.query_mapping);
        Ok(())
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
