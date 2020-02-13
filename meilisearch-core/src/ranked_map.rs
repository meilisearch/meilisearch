use std::io::{Read, Write};

use hashbrown::HashMap;
use meilisearch_schema::FieldId;
use serde::{Deserialize, Serialize};

use crate::{DocumentId, Number};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RankedMap(HashMap<(DocumentId, FieldId), Number>);

impl RankedMap {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, document: DocumentId, field: FieldId, number: Number) {
        self.0.insert((document, field), number);
    }

    pub fn remove(&mut self, document: DocumentId, field: FieldId) {
        self.0.remove(&(document, field));
    }

    pub fn get(&self, document: DocumentId, field: FieldId) -> Option<Number> {
        self.0.get(&(document, field)).cloned()
    }

    pub fn read_from_bin<R: Read>(reader: R) -> bincode::Result<RankedMap> {
        bincode::deserialize_from(reader).map(RankedMap)
    }

    pub fn write_to_bin<W: Write>(&self, writer: W) -> bincode::Result<()> {
        bincode::serialize_into(writer, &self.0)
    }
}
