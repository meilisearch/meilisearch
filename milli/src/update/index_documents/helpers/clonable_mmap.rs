use std::sync::Arc;

use memmap2::Mmap;

/// Wrapper around Mmap allowing to virtualy clone grenad-chunks
/// in a parallel process like the indexing.
#[derive(Debug, Clone)]
pub struct ClonableMmap {
    inner: Arc<Mmap>,
}

impl AsRef<[u8]> for ClonableMmap {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl From<Mmap> for ClonableMmap {
    fn from(inner: Mmap) -> ClonableMmap {
        ClonableMmap { inner: Arc::new(inner) }
    }
}

pub type CursorClonableMmap = std::io::Cursor<ClonableMmap>;
