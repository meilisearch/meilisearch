use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::{Document, Matches};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;

#[inline]
fn number_of_query_words(matches: &Matches) -> usize {
    matches.query_index_groups().count()
}

#[derive(Debug, Clone, Copy)]
pub struct NumberOfWords;

impl<D> Criterion<D> for NumberOfWords
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = number_of_query_words(&lhs.matches);
        let rhs = number_of_query_words(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}
