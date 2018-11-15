use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::index::update::{FIELD_BLOBS_ORDER, Update};
use crate::index::blob_name::BlobName;
use crate::data::DocIdsBuilder;
use crate::DocumentId;

pub struct NegativeUpdateBuilder {
    path: PathBuf,
    doc_ids: DocIdsBuilder<Vec<u8>>,
}

impl NegativeUpdateBuilder {
    pub fn new<P: Into<PathBuf>>(path: P) -> NegativeUpdateBuilder {
        NegativeUpdateBuilder {
            path: path.into(),
            doc_ids: DocIdsBuilder::new(Vec::new()),
        }
    }

    pub fn remove(&mut self, id: DocumentId) -> bool {
        self.doc_ids.insert(id)
    }

    pub fn build(self) -> Result<Update, Box<Error>> {
        let blob_name = BlobName::new();

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);

        file_writer.open(&self.path.to_string_lossy())?;

        // TODO the blob-name must be written in bytes (16 bytes)
        //      along with the sign
        unimplemented!("write the blob sign and name");

        // write the blob name to be merged
        let blob_name = blob_name.to_string();
        file_writer.merge(FIELD_BLOBS_ORDER.as_bytes(), blob_name.as_bytes())?;

        // write the doc ids
        let blob_key = format!("0b-{}-doc-ids", blob_name);
        let blob_doc_ids = self.doc_ids.into_inner()?;
        file_writer.put(blob_key.as_bytes(), &blob_doc_ids)?;

        for id in blob_doc_ids {
            let start = format!("5d-{}", id);
            let end = format!("5d-{}", id + 1);
            file_writer.delete_range(start.as_bytes(), end.as_bytes())?;
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
