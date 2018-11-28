use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

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
        let env_options = rocksdb_options::EnvOptions::new();
        let column_family_options = rocksdb_options::ColumnFamilyOptions::new();
        let mut file_writer = rocksdb::SstFileWriter::new(env_options, column_family_options);
        file_writer.open(&self.path.to_string_lossy())?;

        // // write the doc ids
        // let blob_key = Identifier::blob(blob_info.name).document_ids().build();
        // let blob_doc_ids = self.doc_ids.into_inner()?;
        // file_writer.put(&blob_key, &blob_doc_ids)?;

        // {
        //     // write the blob name to be merged
        //     let mut buffer = Vec::new();
        //     blob_info.write_into(&mut buffer);
        //     let data_key = Identifier::data().blobs_order().build();
        //     file_writer.merge(&data_key, &buffer)?;
        // }

        // let blob_doc_ids = DocIds::from_bytes(blob_doc_ids)?;
        // for id in blob_doc_ids.doc_ids().iter().cloned() {
        //     let start = Identifier::document(id).build();
        //     let end = Identifier::document(id + 1).build();
        //     file_writer.delete_range(&start, &end)?;
        // }

        // file_writer.finish()?;
        // Update::open(self.path)

        unimplemented!()
    }
}
