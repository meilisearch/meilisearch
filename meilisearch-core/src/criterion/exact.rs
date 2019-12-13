use std::cmp::{Ordering, Reverse};
use slice_group_by::GroupBy;
use crate::{RawDocument, MResult};
use crate::bucket_sort::BareMatch;
use super::{Criterion, Context, ContextMut};

pub struct Exact;

impl Criterion for Exact {
    fn name(&self) -> &str { "exact" }

    fn prepare<'h, 'p, 'tag, 'txn, 'q, 'a, 'r>(
        &self,
        _ctx: ContextMut<'h, 'p, 'tag, 'txn, 'q, 'a>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) -> MResult<()>
    {
        for document in documents {
            document.raw_matches.sort_unstable_by_key(|bm| (bm.query_index, Reverse(bm.is_exact)));
        }
        Ok(())
    }

    fn evaluate(&self, _ctx: &Context, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
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
