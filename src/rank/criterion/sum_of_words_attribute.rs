use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::{Document, Matches};
use crate::database::DatabaseView;
use crate::rank::criterion::Criterion;

#[inline]
fn sum_matches_attributes(matches: &Matches) -> u16 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    matches.query_index_groups().map(|group| {
        unsafe { group.get_unchecked(0).attribute.attribute() }
    }).sum()
}

#[derive(Debug, Clone, Copy)]
pub struct SumOfWordsAttribute;

impl<D> Criterion<D> for SumOfWordsAttribute
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = sum_matches_attributes(&lhs.matches);
        let rhs = sum_matches_attributes(&rhs.matches);

        lhs.cmp(&rhs)
    }
}
