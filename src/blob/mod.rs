mod merge;
mod ops;
mod ops_indexed_value;
mod positive_blob;
mod negative_blob;

pub use self::merge::Merge;
pub use self::positive_blob::{PositiveBlob, PositiveBlobBuilder};
pub use self::negative_blob::{NegativeBlob, NegativeBlobBuilder};

use fst::Map;

use crate::data::DocIndexes;

pub enum Blob {
    Positive(PositiveBlob),
    Negative(NegativeBlob),
}

impl Blob {
    pub fn sign(&self) -> Sign {
        match self {
            Blob::Positive(_) => Sign::Positive,
            Blob::Negative(_) => Sign::Negative,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sign {
    Positive,
    Negative,
}

impl Sign {
    pub fn alternate(self) -> Sign {
        match self {
            Sign::Positive => Sign::Negative,
            Sign::Negative => Sign::Positive,
        }
    }
}
