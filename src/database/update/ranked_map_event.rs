use std::error::Error;

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::shared_data_cursor::{SharedDataCursor, FromSharedDataCursor};
use crate::write_to_bytes::WriteToBytes;
use crate::database::RankedMap;
use crate::data::DocIds;

pub enum WriteRankedMapEvent<'a> {
    RemovedDocuments(&'a DocIds),
    UpdatedDocuments(&'a RankedMap),
}

impl<'a> WriteToBytes for WriteRankedMapEvent<'a> {
    fn write_to_bytes(&self, bytes: &mut Vec<u8>) {
        match self {
            WriteRankedMapEvent::RemovedDocuments(doc_ids) => {
                let _ = bytes.write_u8(0);
                doc_ids.write_to_bytes(bytes);
            },
            WriteRankedMapEvent::UpdatedDocuments(ranked_map) => {
                let _ = bytes.write_u8(1);
                bincode::serialize_into(bytes, ranked_map).unwrap()
            }
        }
    }
}

pub enum ReadRankedMapEvent {
    RemovedDocuments(DocIds),
    UpdatedDocuments(RankedMap),
}

impl FromSharedDataCursor for ReadRankedMapEvent {
    type Error = Box<Error>;

    fn from_shared_data_cursor(cursor: &mut SharedDataCursor) -> Result<Self, Self::Error> {
        match cursor.read_u8()? {
            0 => DocIds::from_shared_data_cursor(cursor).map(ReadRankedMapEvent::RemovedDocuments),
            1 => {
                let ranked_map = bincode::deserialize_from(cursor)?;
                Ok(ReadRankedMapEvent::UpdatedDocuments(ranked_map))
            },
            _ => unreachable!(),
        }
    }
}
