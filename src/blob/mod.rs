mod merge;
mod ops;
mod ops_indexed_value;
mod positive_blob;
mod negative_blob;

pub use self::merge::Merge;
pub use self::positive_blob::{PositiveBlob, PositiveBlobBuilder};
pub use self::negative_blob::{NegativeBlob, NegativeBlobBuilder};

use std::error::Error;
use std::io::{Write, Read};
use std::{io, fmt, mem};

use fst::Map;
use uuid::Uuid;
use rocksdb::rocksdb::{DB, Snapshot};

use crate::index::identifier::Identifier;
use crate::data::DocIndexes;

pub enum Blob {
    Positive(PositiveBlob),
    Negative(NegativeBlob),
}

impl Blob {
    pub fn sign(&self) -> Sign {
        match self {
            Blob::Positive(_) => Sign::Positive,
            Blob::Negative(_) => Sign::Negative,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Sign {
    Positive,
    Negative,
}

impl Sign {
    pub fn invert(self) -> Sign {
        match self {
            Sign::Positive => Sign::Negative,
            Sign::Negative => Sign::Positive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobName(Uuid);

impl BlobName {
    pub fn new() -> BlobName {
        BlobName(Uuid::new_v4())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl fmt::Display for BlobName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("BlobName")
            .field(&self.0.to_hyphenated().to_string())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobInfo {
    pub sign: Sign,
    pub name: BlobName,
}

impl BlobInfo {
    pub fn new_positive() -> BlobInfo {
        BlobInfo {
            sign: Sign::Positive,
            name: BlobName::new(),
        }
    }

    pub fn new_negative() -> BlobInfo {
        BlobInfo {
            sign: Sign::Negative,
            name: BlobName::new(),
        }
    }

    pub fn read_from<R: Read>(reader: R) -> bincode::Result<BlobInfo> {
        bincode::deserialize_from(reader)
    }

    pub fn read_from_slice(slice: &[u8]) -> bincode::Result<Vec<BlobInfo>> {
        let len = slice.len() / mem::size_of::<BlobInfo>();
        let mut blob_infos = Vec::with_capacity(len);

        let mut cursor = io::Cursor::new(slice);
        while blob_infos.len() != len {
            let blob_info = BlobInfo::read_from(&mut cursor)?;
            blob_infos.push(blob_info);
        }

        Ok(blob_infos)
    }

    pub fn write_into<W: Write>(&self, writer: W) -> bincode::Result<()> {
        bincode::serialize_into(writer, self)
    }
}

pub fn blobs_from_blob_infos(infos: &[BlobInfo], snapshot: &Snapshot<&DB>) -> Result<Vec<Blob>, Box<Error>> {
    let mut blobs = Vec::with_capacity(infos.len());

    for info in infos {
        let blob = match info.sign {
            Sign::Positive => {
                let blob_key = Identifier::blob(info.name).fst_map().build();
                let map = match snapshot.get(&blob_key)? {
                    Some(value) => value.to_vec(),
                    None => return Err(format!("No fst entry found for blob {}", info.name).into()),
                };
                let blob_key = Identifier::blob(info.name).document_indexes().build();
                let doc_idx = match snapshot.get(&blob_key)? {
                    Some(value) => value.to_vec(),
                    None => return Err(format!("No doc-idx entry found for blob {}", info.name).into()),
                };
                PositiveBlob::from_bytes(map, doc_idx).map(Blob::Positive)?
            },
            Sign::Negative => {
                let blob_key = Identifier::blob(info.name).document_ids().build();
                let doc_ids = match snapshot.get(&blob_key)? {
                    Some(value) => value.to_vec(),
                    None => return Err(format!("No doc-ids entry found for blob {}", info.name).into()),
                };
                NegativeBlob::from_bytes(doc_ids).map(Blob::Negative)?
            },
        };
        blobs.push(blob);
    }

    Ok(blobs)
}
