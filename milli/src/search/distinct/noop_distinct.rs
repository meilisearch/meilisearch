use roaring::bitmap::IntoIter;
use roaring::RoaringBitmap;

use super::{Distinct, DocIter};
use crate::{DocumentId, Result};

/// A distinct implementer that does not perform any distinct,
/// and simply returns an iterator to the candidates.
pub struct NoopDistinct;

pub struct NoopDistinctIter {
    candidates: IntoIter,
    excluded: RoaringBitmap,
}

impl Iterator for NoopDistinctIter {
    type Item = Result<DocumentId>;

    fn next(&mut self) -> Option<Self::Item> {
        self.candidates.next().map(Ok)
    }
}

impl DocIter for NoopDistinctIter {
    fn into_excluded(self) -> RoaringBitmap {
        self.excluded
    }
}

impl Distinct for NoopDistinct {
    type Iter = NoopDistinctIter;

    fn distinct(&mut self, candidates: RoaringBitmap, excluded: RoaringBitmap) -> Self::Iter {
        NoopDistinctIter { candidates: candidates.into_iter(), excluded }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_noop() {
        let candidates = (1..10).collect();
        let excluded = RoaringBitmap::new();
        let mut iter = NoopDistinct.distinct(candidates, excluded);
        assert_eq!(
            iter.by_ref().map(Result::unwrap).collect::<Vec<_>>(),
            (1..10).collect::<Vec<_>>()
        );

        let excluded = iter.into_excluded();
        assert!(excluded.is_empty());
    }
}
