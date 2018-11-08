use std::path::{Path, PathBuf};
use std::error::Error;
use std::fs::{self, File};

use fs2::FileExt;

use crate::rank::Document;
use crate::blob::Blob;

pub struct Index {
    path: PathBuf,
    lock_file: File,
    blobs: Vec<Blob>,
}

impl Index {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self, Box<Error>> {
        let path = path.into();

        let lock_file = File::create(path.join(".lock"))?;
        lock_file.try_lock_exclusive()?;

        let blobs = Vec::new();

        Ok(Self { path, lock_file, blobs })
    }

    pub fn create<P: Into<PathBuf>>(path: P) -> Result<Self, Box<Error>> {
        let path = path.into();

        fs::create_dir_all(&path)?;
        File::create(path.join(".lock"))?;

        Self::open(path)
    }

    pub fn blobs(&self) -> &[Blob] {
        &self.blobs
    }
}
