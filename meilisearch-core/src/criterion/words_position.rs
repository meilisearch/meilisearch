use std::cmp::Ordering;

use slice_group_by::GroupBy;

use crate::RawDocument;
use crate::bucket_sort::SimpleMatch;
use super::{Criterion, Context, ContextMut, prepare_raw_matches};

pub struct WordsPosition;

impl Criterion for WordsPosition {
    fn name(&self) -> &str { "words position" }

    fn prepare<'p, 'tag, 'txn, 'q, 'a, 'r>(
        &self,
        ctx: ContextMut<'p, 'tag, 'txn, 'q, 'a>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) {
        prepare_raw_matches(documents, ctx.postings_lists, ctx.query_enhancer, ctx.automatons);
    }

    fn evaluate<'p, 'tag, 'txn, 'q, 'a, 'r>(
        &self,
        ctx: &Context<'p, 'tag, 'txn, 'q, 'a>,
        lhs: &RawDocument<'r, 'tag>,
        rhs: &RawDocument<'r, 'tag>,
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
