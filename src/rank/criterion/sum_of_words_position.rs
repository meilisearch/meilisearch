use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;
use slice_group_by::GroupBy;

use crate::database::DatabaseView;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::Match;

#[inline]
fn sum_matches_attribute_index(matches: &[Match]) -> usize {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    matches.linear_group_by(match_query_index).map(|group| {
        unsafe { group.get_unchecked(0).attribute.word_index() as usize }
    }).sum()
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsPosition;

impl<D> Criterion<D> for SumOfWordsPosition
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = sum_matches_attribute_index(&lhs.matches);
        let rhs = sum_matches_attribute_index(&rhs.matches);

        lhs.cmp(&rhs)
    }
}
