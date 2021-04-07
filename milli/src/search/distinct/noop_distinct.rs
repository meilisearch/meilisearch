use roaring::RoaringBitmap;

use crate::DocumentId;
use super::{DocIter, Distinct};

pub struct NoopDistinct;

pub struct NoopDistinctIter {
    candidates: roaring::bitmap::IntoIter,
    excluded: RoaringBitmap,
}

impl Iterator for NoopDistinctIter {
    type Item = anyhow::Result<DocumentId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.candidates.next().map(Result::Ok)
    }
}

impl DocIter for NoopDistinctIter {
    fn into_excluded(self) -> RoaringBitmap {
        self.excluded
    }
}

impl Distinct<'_> for NoopDistinct {
    type Iter = NoopDistinctIter;

    fn distinct(&mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter {
        NoopDistinctIter {
            candidates: candidates.into_iter(),
            excluded,
        }
    }
}
