use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobName;

impl BlobName {
    pub fn new() -> BlobName {
        unimplemented!()
    }
}

impl fmt::Display for BlobName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}
