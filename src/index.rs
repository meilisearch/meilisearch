use std::path::{Path, PathBuf};
use std::error::Error;

use crate::rank::Document;
use crate::blob::Blob;

pub struct Index {
    path: PathBuf,
    blobs: Vec<Blob>,
}

impl Index {
    pub fn open(path: &Path) -> Result<Self, Box<Error>> {
        unimplemented!()
    }

    pub fn create(path: &Path) -> Result<Self, Box<Error>> {
        unimplemented!()
    }

    pub fn blobs(&self) -> &[Blob] {
        &self.blobs
    }
}
