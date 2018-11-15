use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::index::blob_name::BlobName;
use crate::blob::Sign;

mod negative_update;
mod positive_update;

pub use self::negative_update::{NegativeUpdateBuilder};
pub use self::positive_update::{PositiveUpdateBuilder, NewState};

// These prefixes are here to make sure the documents fields
// and the internal data doesn't collide and the internal data are
// at the top of the sst file.
const FIELD_BLOBS_ORDER: &str = "00-blobs-order";

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

        if infos.smallest_key() != FIELD_BLOBS_ORDER.as_bytes() {
            // FIXME return a nice error
            panic!("Invalid update file: the blobs-order field is not the smallest key")
        }

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
