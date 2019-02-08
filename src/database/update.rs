use std::collections::{HashSet, BTreeMap};
use std::error::Error;

use rocksdb::rocksdb::{Writable, WriteBatch};
use hashbrown::hash_map::HashMap;
use serde::Serialize;
use fst::map::Map;
use sdset::Set;

use crate::database::index::{Positive, PositiveBuilder, Negative};
use crate::database::document_key::{DocumentKey, DocumentKeyAttr};
use crate::database::serde::serializer::Serializer;
use crate::database::serde::SerializerError;
use crate::database::schema::SchemaAttr;
use crate::tokenizer::TokenizerBuilder;
use crate::data::{DocIds, DocIndexes};
use crate::database::schema::Schema;
use crate::database::index::Index;
use crate::{DocumentId, DocIndex};
use crate::database::{DATA_INDEX, DATA_RANKED_MAP};

pub type Token = Vec<u8>; // TODO could be replaced by a SmallVec

pub struct Update {
    schema: Schema,
    raw_builder: RawUpdateBuilder,
}

impl Update {
    pub(crate) fn new(schema: Schema) -> Update {
        Update { schema, raw_builder: RawUpdateBuilder::new() }
    }

    pub fn update_document<T, B>(
        &mut self,
        document: T,
        tokenizer_builder: &B,
        stop_words: &HashSet<String>,
    ) -> Result<DocumentId, SerializerError>
    where T: Serialize,
          B: TokenizerBuilder,
    {
        let document_id = self.schema.document_id(&document)?;

        let serializer = Serializer {
            schema: &self.schema,
            document_id: document_id,
            tokenizer_builder: tokenizer_builder,
            update: &mut self.raw_builder.document_update(document_id)?,
            stop_words: stop_words,
        };

        document.serialize(serializer)?;

        Ok(document_id)
    }

    pub fn remove_document<T>(&mut self, document: T) -> Result<DocumentId, SerializerError>
    where T: Serialize,
    {
        let document_id = self.schema.document_id(&document)?;
        self.raw_builder.document_update(document_id)?.remove()?;
        Ok(document_id)
    }

    pub(crate) fn build(self) -> Result<WriteBatch, Box<Error>> {
        self.raw_builder.build()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum UpdateType {
    Updated,
    Deleted,
}

use UpdateType::{Updated, Deleted};

pub struct RawUpdateBuilder {
    documents_update: HashMap<DocumentId, UpdateType>,
    documents_ranked_fields: HashMap<(DocumentId, SchemaAttr), i64>,
    indexed_words: BTreeMap<Token, Vec<DocIndex>>,
    batch: WriteBatch,
}

impl RawUpdateBuilder {
    pub fn new() -> RawUpdateBuilder {
        RawUpdateBuilder {
            documents_update: HashMap::new(),
            documents_ranked_fields: HashMap::new(),
            indexed_words: BTreeMap::new(),
            batch: WriteBatch::new(),
        }
    }

    pub fn document_update(&mut self, document_id: DocumentId) -> Result<DocumentUpdate, SerializerError> {
        use serde::ser::Error;

        match self.documents_update.get(&document_id) {
            Some(Deleted) | None => Ok(DocumentUpdate { document_id, inner: self }),
            Some(Updated) => Err(SerializerError::custom(
                "This document has already been removed and cannot be updated in the same update"
            )),
        }
    }

    pub fn build(self) -> Result<WriteBatch, Box<Error>> {
        let negative = {
            let mut removed_document_ids = Vec::new();
            for (id, update_type) in self.documents_update {
                if update_type == Deleted {
                    removed_document_ids.push(id);
                }
            }

            removed_document_ids.sort_unstable();
            let removed_document_ids = Set::new_unchecked(&removed_document_ids);
            let doc_ids = DocIds::new(removed_document_ids);

            Negative::new(doc_ids)
        };

        let positive = {
            let mut positive_builder = PositiveBuilder::memory();

            for (key, mut indexes) in self.indexed_words {
                indexes.sort_unstable();
                let indexes = Set::new_unchecked(&indexes);
                positive_builder.insert(key, indexes)?;
            }

            let (map, indexes) = positive_builder.into_inner()?;
            let map = Map::from_bytes(map)?;
            let indexes = DocIndexes::from_bytes(indexes)?;

            Positive::new(map, indexes)
        };

        let index = Index { negative, positive };

        // write the data-index
        let mut bytes_index = Vec::new();
        index.write_to_bytes(&mut bytes_index);
        self.batch.merge(DATA_INDEX, &bytes_index)?;

        let bytes_ranked_map = bincode::serialize(&self.documents_ranked_fields).unwrap();
        self.batch.merge(DATA_RANKED_MAP, &bytes_ranked_map)?;

        Ok(self.batch)
    }
}

pub struct DocumentUpdate<'a> {
    document_id: DocumentId,
    inner: &'a mut RawUpdateBuilder,
}

impl<'a> DocumentUpdate<'a> {
    pub fn remove(&mut self) -> Result<(), SerializerError> {
        use serde::ser::Error;

        if let Updated = self.inner.documents_update.entry(self.document_id).or_insert(Deleted) {
            return Err(SerializerError::custom(
                "This document has already been updated and cannot be removed in the same update"
            ));
        }

        let start = DocumentKey::new(self.document_id).with_attribute_min();
        let end = DocumentKey::new(self.document_id).with_attribute_max(); // FIXME max + 1
        self.inner.batch.delete_range(start.as_ref(), end.as_ref())?;

        Ok(())
    }

    pub fn insert_attribute_value(&mut self, attr: SchemaAttr, value: &[u8]) -> Result<(), SerializerError> {
        use serde::ser::Error;

        if let Deleted = self.inner.documents_update.entry(self.document_id).or_insert(Updated) {
            return Err(SerializerError::custom(
                "This document has already been deleted and cannot be updated in the same update"
            ));
        }

        let key = DocumentKeyAttr::new(self.document_id, attr);
        self.inner.batch.put(key.as_ref(), &value)?;

        Ok(())
    }

    pub fn insert_doc_index(&mut self, token: Token, doc_index: DocIndex) -> Result<(), SerializerError> {
        use serde::ser::Error;

        if let Deleted = self.inner.documents_update.entry(self.document_id).or_insert(Updated) {
            return Err(SerializerError::custom(
                "This document has already been deleted and cannot be updated in the same update"
            ));
        }

        self.inner.indexed_words.entry(token).or_insert_with(Vec::new).push(doc_index);

        Ok(())
    }

    pub fn register_ranked_attribute(
        &mut self,
        attr: SchemaAttr,
        integer: i64,
    ) -> Result<(), SerializerError>
    {
        use serde::ser::Error;

        if let Deleted = self.inner.documents_update.entry(self.document_id).or_insert(Updated) {
            return Err(SerializerError::custom(
                "This document has already been deleted, ranked attributes cannot be added in the same update"
            ));
        }

        self.inner.documents_ranked_fields.insert((self.document_id, attr), integer);

        Ok(())
    }
}
