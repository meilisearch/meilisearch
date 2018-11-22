use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::blob::BlobInfo;
use crate::index::DATA_BLOBS_ORDER;
use crate::index::update::Update;
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
        let blob_info = BlobInfo::new_negative();

        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.path.to_string_lossy())?;

        // write the doc ids
        let blob_key = format!("blob-{}-doc-ids", blob_info.name);
        let blob_doc_ids = self.doc_ids.into_inner()?;
        file_writer.put(blob_key.as_bytes(), &blob_doc_ids)?;

        {
            // write the blob name to be merged
            let mut buffer = Vec::new();
            blob_info.write_into(&mut buffer);
            file_writer.merge(DATA_BLOBS_ORDER.as_bytes(), &buffer)?;
        }

        for id in blob_doc_ids {
            let start = format!("docu-{}", id);
            let end = format!("docu-{}", id + 1);
            file_writer.delete_range(start.as_bytes(), end.as_bytes())?;
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
