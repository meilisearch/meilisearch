use std::path::PathBuf;
use std::error::Error;

use ::rocksdb::rocksdb_options;

use crate::database::update::negative::unordered_builder::UnorderedNegativeBlobBuilder;
use crate::database::blob::{Blob, NegativeBlob};
use crate::database::update::Update;
use crate::database::DocumentKey;
use crate::database::DATA_INDEX;
use crate::DocumentId;

pub struct NegativeUpdateBuilder {
    path: PathBuf,
    doc_ids: UnorderedNegativeBlobBuilder<Vec<u8>>,
}

impl NegativeUpdateBuilder {
    pub fn new<P: Into<PathBuf>>(path: P) -> NegativeUpdateBuilder {
        NegativeUpdateBuilder {
            path: path.into(),
            doc_ids: UnorderedNegativeBlobBuilder::memory(),
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

        let bytes = self.doc_ids.into_inner()?;
        let negative_blob = NegativeBlob::from_bytes(bytes)?;
        let blob = Blob::Negative(negative_blob);

        // write the data-index aka negative blob
        let mut bytes = Vec::new();
        blob.write_to_bytes(&mut bytes);
        file_writer.merge(DATA_INDEX, &bytes)?;

        // FIXME remove this ugly thing !
        // let Blob::Negative(negative_blob) = blob;
        let negative_blob = match blob {
            Blob::Negative(blob) => blob,
            Blob::Positive(_) => unreachable!(),
        };

        for &document_id in negative_blob.as_ref().as_slice() {
            let start = DocumentKey::new(document_id);
            let end = start.with_attribute_max();
            file_writer.delete_range(start.as_ref(), end.as_ref())?;
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
