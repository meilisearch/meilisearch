mod facet_distinct;
mod map_distinct;
mod noop_distinct;

use roaring::RoaringBitmap;

pub use facet_distinct::FacetDistinct;
pub use map_distinct::MapDistinct;
pub use noop_distinct::NoopDistinct;
use crate::DocumentId;

pub trait DocIter: Iterator<Item=anyhow::Result<DocumentId>> {
    /// Returns ownership on the internal RoaringBitmaps: (candidates, excluded)
    fn into_excluded(self) -> RoaringBitmap;
}

pub trait Distinct<'a> {
    type Iter: DocIter;

    fn distinct(&'a mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter;
}
