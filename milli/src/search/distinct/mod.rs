mod facet_distinct;
mod map_distinct;
mod noop_distinct;

use roaring::RoaringBitmap;

pub use facet_distinct::FacetDistinct;
pub use map_distinct::MapDistinct;
pub use noop_distinct::NoopDistinct;
use crate::DocumentId;

/// A trait implemented by document interators that are returned by calls to `Distinct::distinct`.
/// It provides a way to get back the ownership to the excluded set.
pub trait DocIter: Iterator<Item=anyhow::Result<DocumentId>> {
    /// Returns ownership on the internal exluded set.
    fn into_excluded(self) -> RoaringBitmap;
}

/// A trait that is implemented by structs that perform a distinct on `candidates`. Calling distinct
/// must return an iterator containing only distinct documents, and add the discarded documents to
/// the excluded set. The excluded set can later be retrieved by calling `DocIter::excluded` on the
/// returned iterator.
pub trait Distinct<'a> {
    type Iter: DocIter;

    fn distinct(&'a mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter;
}
