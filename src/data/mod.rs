mod doc_ids;
mod doc_indexes;

use std::ops::Deref;
use std::sync::Arc;

use fst::raw::MmapReadOnly;

pub use self::doc_ids::{DocIds, DocIdsBuilder};
pub use self::doc_indexes::{DocIndexes, DocIndexesBuilder, RawDocIndexesBuilder};

#[derive(Clone)]
enum Data {
    Shared {
        bytes: Arc<Vec<u8>>,
        offset: usize,
        len: usize,
    },
    Mmap(MmapReadOnly),
}

impl Data {
    pub fn range(&self, off: usize, l: usize) -> Data {
        match self {
            Data::Shared { bytes, offset, len } => {
                assert!(off + l <= *len);
                Data::Shared {
                    bytes: bytes.clone(),
                    offset: offset + off,
                    len: l,
                }
            },
            Data::Mmap(mmap) => Data::Mmap(mmap.range(off, l)),
        }
    }
}

impl Default for Data {
    fn default() -> Data {
        Data::Shared {
            bytes: Arc::default(),
            offset: 0,
            len: 0,
        }
    }
}

impl Deref for Data {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Data {
    fn as_ref(&self) -> &[u8] {
        match self {
            Data::Shared { bytes, offset, len } => {
                &bytes[*offset..offset + len]
            },
            Data::Mmap(m) => m.as_slice(),
        }
    }
}
