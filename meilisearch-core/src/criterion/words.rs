use std::cmp::Ordering;
use crate::RawDocument;
use super::{Criterion, Context, ContextMut, prepare_query_distances};

pub struct Words;

impl Criterion for Words {
    fn name(&self) -> &str { "words" }

    fn prepare<'p, 'tag, 'txn, 'q, 'a, 'r>(
        &self,
        ctx: ContextMut<'p, 'tag, 'txn, 'q, 'a>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) {
        prepare_query_distances(documents, ctx.query_enhancer, ctx.automatons, ctx.postings_lists);
    }

    fn evaluate(&self, _ctx: &Context, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        #[inline]
        fn number_of_query_words(distances: &[Option<u8>]) -> usize {
            distances.iter().cloned().filter(Option::is_some).count()
        }

        let lhs = number_of_query_words(&lhs.processed_distances);
        let rhs = number_of_query_words(&rhs.processed_distances);

        lhs.cmp(&rhs).reverse()
    }
}
