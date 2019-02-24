use std::error::Error;

use byteorder::{ReadBytesExt, WriteBytesExt};
use meilidb_core::shared_data_cursor::{SharedDataCursor, FromSharedDataCursor};
use meilidb_core::write_to_bytes::WriteToBytes;
use meilidb_core::data::DocIds;

use crate::database::Index;

pub enum WriteIndexEvent<'a> {
    RemovedDocuments(&'a DocIds),
    UpdatedDocuments(&'a Index),
}

impl<'a> WriteToBytes for WriteIndexEvent<'a> {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        match self {
            WriteIndexEvent::RemovedDocuments(doc_ids) => {
                let _ = bytes.write_u8(0);
                doc_ids.write_to_bytes(bytes);
            },
            WriteIndexEvent::UpdatedDocuments(index) => {
                let _ = bytes.write_u8(1);
                index.write_to_bytes(bytes);
            }
        }
    }
}

pub enum ReadIndexEvent {
    RemovedDocuments(DocIds),
    UpdatedDocuments(Index),
}

impl ReadIndexEvent {
    pub fn updated_documents(self) -> Option<Index> {
        use ReadIndexEvent::*;
        match self {
            RemovedDocuments(_) => None,
            UpdatedDocuments(index) => Some(index),
        }
    }
}

impl FromSharedDataCursor for ReadIndexEvent {
    type Error = Box<Error>;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<Self, Self::Error> {
        match cursor.read_u8()? {
            0 => DocIds::from_shared_data_cursor(cursor).map(ReadIndexEvent::RemovedDocuments),
            1 => Index::from_shared_data_cursor(cursor).map(ReadIndexEvent::UpdatedDocuments),
            _ => unreachable!(),
        }
    }
}
