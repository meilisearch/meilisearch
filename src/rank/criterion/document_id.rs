use std::cmp::Ordering;
use std::ops::Deref;

use rocksdb::DB;

use crate::rank::criterion::Criterion;
use crate::database::DatabaseView;
use crate::rank::Document;

/// A criterion mostly used to keep a constantly sorted output
/// between two calls and avoid the unordered maps downside.
///
/// If two documents are in the same bucket this criterion will
/// order them by their document id.
///
/// [1]: https://doc.rust-lang.org/std/collections/struct.HashMap.html
#[derive(Debug, Clone, Copy)]
pub struct DocumentId;

impl<D> Criterion<D> for DocumentId
where D: Deref<Target=DB>
{
    #[inline]
    fn evaluate(&self, lhs: &Document, rhs: &Document, _: &DatabaseView<D>) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }
}
