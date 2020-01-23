use std::cmp::Ordering;
use slice_group_by::GroupBy;
use crate::bucket_sort::SimpleMatch;
use crate::{RawDocument, MResult};
use super::{Criterion, Context, ContextMut, prepare_bare_matches};

pub struct WordsPosition;

impl Criterion for WordsPosition {
    fn name(&self) -> &str { "words position" }

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
