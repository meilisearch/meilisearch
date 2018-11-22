use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::blob::{BlobName, Sign};

mod negative_update;
mod positive_update;

pub use self::negative_update::{NegativeUpdateBuilder};
pub use self::positive_update::{PositiveUpdateBuilder, NewState};

pub struct Update {
    path: PathBuf,
}

impl Update {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        Ok(Update { path: path.into() })
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.path
    }

    pub fn info(&self) -> UpdateInfo {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpdateInfo {
    pub sign: Sign,
    pub id: BlobName,
}
