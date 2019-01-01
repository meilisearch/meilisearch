use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;
use group_by::GroupBy;

use crate::rank::{match_query_index, Document};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::Match;

#[inline]
fn contains_exact(matches: &[Match]) -> bool {
    matches.iter().any(|m| m.is_exact)
}

#[inline]
fn number_exact_matches(matches: &[Match]) -> usize {
    GroupBy::new(matches, match_query_index).map(contains_exact).count()
}

#[derive(Default, Debug, Clone, Copy)]
pub struct Exact;

impl<D> Criterion<D> for Exact
where D: Deref<Target=DB>
{
    fn evaluate(&mut self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = number_exact_matches(&lhs.matches);
        let rhs = number_exact_matches(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}
