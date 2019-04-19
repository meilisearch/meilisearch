use std::error::Error;

use byteorder::{ReadBytesExt, WriteBytesExt};

use meilidb_core::{Index as WordIndex};
use meilidb_core::data::DocIds;
use meilidb_core::write_to_bytes::WriteToBytes;
use meilidb_core::shared_data_cursor::{SharedDataCursor, FromSharedDataCursor};

enum NewIndexEvent<'a, S> {
    RemovedDocuments(&'a DocIds),
    UpdatedDocuments(&'a WordIndex<S>),
}

impl<'a, S> WriteToBytes for NewIndexEvent<'a, S> {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        match self {
            NewIndexEvent::RemovedDocuments(doc_ids) => {
                let _ = bytes.write_u8(0);
                doc_ids.write_to_bytes(bytes);
            },
            NewIndexEvent::UpdatedDocuments(index) => {
                let _ = bytes.write_u8(1);
                // index.write_to_bytes(bytes);
            }
        }
    }
}

enum IndexEvent<S> {
    RemovedDocuments(DocIds),
    UpdatedDocuments(WordIndex<S>),
}

impl<S> FromSharedDataCursor for IndexEvent<S> {
    type Error = Box<Error>;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<Self, Self::Error> {
        match cursor.read_u8()? {
            0 => DocIds::from_shared_data_cursor(cursor).map(IndexEvent::RemovedDocuments),
            // 1 => WordIndex::from_shared_data_cursor(cursor).map(IndexEvent::UpdatedDocuments),
            _ => Err("invalid index event type".into()),
        }
    }
}
