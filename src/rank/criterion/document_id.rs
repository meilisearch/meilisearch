use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::rank::Document;

#[derive(Default, Debug, Clone, Copy)]
pub struct DocumentId;

impl<D> Criterion<D> for DocumentId
where D: Deref<Target=DB>
{
    fn evaluate(&mut self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }
}
