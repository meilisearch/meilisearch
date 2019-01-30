use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;
use slice_group_by::GroupBy;

use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::Match;

#[inline]
fn number_of_query_words(matches: &[Match]) -> usize {
    matches.linear_group_by(match_query_index).count()
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
