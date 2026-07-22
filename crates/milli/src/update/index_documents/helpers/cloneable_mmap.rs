use std::sync::Arc;

use memmap2::Mmap;

/// Wrapper around Mmap allowing to virtually clone grenad-chunks
/// in a parallel process like the indexing.
#[derive(Debug, Clone)]
pub struct CloneableMmap {
    inner: Arc<Mmap>,
}

impl AsRef<[u8]> for CloneableMmap {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl From<Mmap> for CloneableMmap {
    fn from(inner: Mmap) -> CloneableMmap {
        CloneableMmap { inner: Arc::new(inner) }
    }
}

pub type CursorCloneableMmap = std::io::Cursor<CloneableMmap>;
