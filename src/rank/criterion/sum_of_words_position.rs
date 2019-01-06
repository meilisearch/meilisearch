use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::{Document, Matches};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;

#[inline]
fn sum_matches_attribute_index(matches: &Matches) -> u32 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    matches.query_index_groups().map(|group| {
        unsafe { group.get_unchecked(0).attribute.word_index() }
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
