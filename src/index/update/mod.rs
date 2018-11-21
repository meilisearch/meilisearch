use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::index::blob_name::BlobName;
use crate::blob::Sign;

mod negative_update;
mod positive_update;

pub use self::negative_update::{NegativeUpdateBuilder};
pub use self::positive_update::{PositiveUpdateBuilder, NewState};

pub struct Update {
    path: PathBuf,
}

impl Update {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Update, Box<Error>> {
        let path = path.into();

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&path.to_string_lossy())?;
        let infos = file_writer.finish()?;

        // FIXME check if the update contains a blobs-order entry

        Ok(Update { path })
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
