mod doc_ids;
mod doc_indexes;

use std::ops::Deref;
use std::sync::Arc;

use fst::raw::MmapReadOnly;

pub use self::doc_ids::{DocIds, DocIdsBuilder};
pub use self::doc_indexes::{DocIndexes, DocIndexesBuilder};

#[derive(Clone)]
enum Data {
    Shared {
        vec: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
    Mmap(MmapReadOnly),
}

impl Deref for Data {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Data::Shared { vec, offset, len } => {
                &vec[*offset..offset + len]
            },
            Data::Mmap(m) => m.as_slice(),
        }
    }
}
