use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;
use group_by::GroupBy;

use crate::database::DatabaseView;
use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::Match;

#[inline]
fn sum_matches_attributes(matches: &[Match]) -> u16 {
    // note that GroupBy will never return an empty group
    // so we can do this assumption safely
    GroupBy::new(matches, match_query_index).map(|group| unsafe {
        group.get_unchecked(0).attribute.attribute()
    }).sum()
}

#[derive(Default, Debug, Clone, Copy)]
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
