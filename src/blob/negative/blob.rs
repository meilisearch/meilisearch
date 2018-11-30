use std::path::Path;
use std::error::Error;

use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use crate::data::DocIds;
use crate::DocumentId;

pub struct NegativeBlob {
    doc_ids: DocIds,
}

impl NegativeBlob {
    pub unsafe fn from_path<P>(doc_ids: P) -> Result<Self, Box<Error>>
    where P: AsRef<Path>,
    {
        let doc_ids = DocIds::from_path(doc_ids)?;
        Ok(NegativeBlob { doc_ids })
    }

    pub fn from_bytes(doc_ids: Vec<u8>) -> Result<Self, Box<Error>> {
        let doc_ids = DocIds::from_bytes(doc_ids)?;
        Ok(NegativeBlob { doc_ids })
    }

    pub fn from_raw(doc_ids: DocIds) -> Self {
        NegativeBlob { doc_ids }
    }

    pub fn as_ids(&self) -> &DocIds {
        &self.doc_ids
    }

    pub fn into_doc_ids(self) -> DocIds {
        self.doc_ids
    }
}

impl AsRef<[DocumentId]> for NegativeBlob {
    fn as_ref(&self) -> &[DocumentId] {
        self.as_ids().doc_ids()
    }
}

impl Serialize for NegativeBlob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.doc_ids.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NegativeBlob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<NegativeBlob, D::Error> {
        let bytes = Vec::deserialize(deserializer)?;
        NegativeBlob::from_bytes(bytes).map_err(de::Error::custom)
    }
}
