use crate::Index;

use roaring::RoaringBitmap;

use super::query_tree::Operation;

pub mod typo;

pub trait Criterion {
    fn next(&mut self) -> anyhow::Result<Option<(Option<Operation>, RoaringBitmap)>>;
}

/// Either a set of candidates that defines the candidates
/// that are allowed to be returned,
/// or the candidates that must never be returned.
enum Candidates {
    Allowed(RoaringBitmap),
    Forbidden(RoaringBitmap)
}

impl Candidates {
    fn into_inner(self) -> RoaringBitmap {
        match self {
            Self::Allowed(inner) => inner,
            Self::Forbidden(inner) => inner,
        }
    }
}

impl Default for Candidates {
    fn default() -> Self {
        Self::Forbidden(RoaringBitmap::new())
    }
}
