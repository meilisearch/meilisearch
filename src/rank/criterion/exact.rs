use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::{Document, Matches};
use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::Match;

#[inline]
fn contains_exact(matches: &[Match]) -> bool {
    matches.iter().any(|m| m.is_exact)
}

#[inline]
fn number_exact_matches(matches: &Matches) -> usize {
    matches.query_index_groups().map(contains_exact).count()
}

#[derive(Debug, Clone, Copy)]
pub struct Exact;

impl<D> Criterion<D> for Exact
where D: Deref<Target=DB>
{
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        let lhs = number_exact_matches(&lhs.matches);
        let rhs = number_exact_matches(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}
