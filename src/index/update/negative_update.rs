use std::path::PathBuf;
use std::error::Error;
use std::io::{Cursor, Write};

use byteorder::{NetworkEndian, WriteBytesExt};
use ::rocksdb::rocksdb_options;

use crate::data::{DocIds, DocIdsBuilder};
use crate::blob::{Blob, NegativeBlob};
use crate::index::update::Update;
use crate::index::DATA_INDEX;
use crate::DocumentId;

const DOC_KEY_LEN: usize = 4 + std::mem::size_of::<u64>();

// "doc-ID_8_BYTES"
fn raw_document_key(id: DocumentId) -> [u8; DOC_KEY_LEN] {
    let mut key = [0; DOC_KEY_LEN];
    let mut rdr = Cursor::new(&mut key[..]);
    rdr.write_all(b"doc-").unwrap();
    rdr.write_u64::<NetworkEndian>(id).unwrap();
    key
}

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

        // write the data-index aka negative blob
        let bytes = self.doc_ids.into_inner()?;
        let doc_ids = DocIds::from_bytes(bytes)?;
        let blob = Blob::Negative(NegativeBlob::from_raw(doc_ids));
        let bytes = bincode::serialize(&blob)?;
        file_writer.merge(DATA_INDEX, &bytes);

        // FIXME remove this ugly thing !
        // let Blob::Negative(negative_blob) = blob;
        let negative_blob = match blob {
            Blob::Negative(blob) => blob,
            Blob::Positive(_) => unreachable!(),
        };

        for &document_id in negative_blob.as_ref() {
            let start = raw_document_key(document_id);
            let end = raw_document_key(document_id + 1);
            file_writer.delete_range(&start, &end)?;
        }

        file_writer.finish()?;
        Update::open(self.path)
    }
}
